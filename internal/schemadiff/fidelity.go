package schemadiff

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/johndauphine/smt/internal/ddl"
	"github.com/johndauphine/smt/internal/driver"
	"github.com/johndauphine/smt/internal/expr"
	"github.com/johndauphine/smt/internal/fidelity"
)

// FidelityEvolutionError refuses an opt-in plan before it can execute partially.
// Existing-table changes require an ownership/transition protocol; a prefix is
// never sufficient evidence that a target check belongs to SMT.
type FidelityEvolutionError struct{ Reason string }

func (e *FidelityEvolutionError) Error() string { return "unsupported fidelity evolution: " + e.Reason }

type fidelityComparison struct {
	desired, existing []driver.Table
	assessment        fidelity.Assessment
	checkDrift        map[string][]string
	columnDrift       map[string][]string
}

func prepareFidelityComparison(desired, existing []driver.Table, source, target string, opts DriftOptions, profile fidelity.Profile) (fidelityComparison, error) {
	renderer, err := ddl.NewRenderer(target, "", "fail")
	if err != nil {
		return fidelityComparison{}, err
	}
	renderer = renderer.WithSource(source)
	result := fidelityComparison{desired: make([]driver.Table, len(desired)), existing: make([]driver.Table, len(existing)), checkDrift: make(map[string][]string), columnDrift: make(map[string][]string)}
	if len(desired) == 0 {
		assessment, err := fidelity.AssessTable(&driver.Table{}, source, target)
		result.assessment = assessment
		if err != nil {
			return result, err
		}
		if err := fidelity.Gate(profile, assessment); err != nil {
			return result, err
		}
	}
	for i, table := range existing {
		result.existing[i] = deepCopyTable(table)
	}
	existingIndex := make(map[string]int, len(existing))
	for i, table := range existing {
		existingIndex[strings.ToLower(table.Name)] = i
	}
	for i, table := range desired {
		result.desired[i] = deepCopyTable(table)
		checks, assessment, err := renderer.FidelityChecks(&table, profile)
		result.assessment.Requirements = append(result.assessment.Requirements, assessment.Requirements...)
		result.assessment.Findings = append(result.assessment.Findings, assessment.Findings...)
		if err != nil {
			return result, err
		}
		index, found := existingIndex[strings.ToLower(table.Name)]
		if !found {
			continue
		}
		have := &result.existing[index]
		if strings.TrimSpace(source) != "" {
			result.columnDrift[table.Name] = normalizeFidelityBinaryDefaults(&result.desired[i], have, source)
			if len(result.columnDrift[table.Name]) == 0 {
				delete(result.columnDrift, table.Name)
			}
		}
		matched := make(map[int]bool)
		for _, expected := range checks {
			found := false
			for j, actual := range have.CheckConstraints {
				if actual.Name != expected.Name {
					continue
				}
				found = true
				if !equalFidelityExpression(expected.Expression, actual.Definition) {
					result.checkDrift[table.Name] = append(result.checkDrift[table.Name], "generated restriction "+expected.Name+" has a different predicate")
				} else if actual.Validated == nil || !*actual.Validated {
					result.checkDrift[table.Name] = append(result.checkDrift[table.Name], "generated restriction "+expected.Name+" is unvalidated or lacks validation evidence")
				} else {
					matched[j] = true
				}
			}
			if !found {
				result.checkDrift[table.Name] = append(result.checkDrift[table.Name], "missing generated restriction "+expected.Name)
			}
		}
		var remaining []driver.CheckConstraint
		for j, check := range have.CheckConstraints {
			if !matched[j] {
				remaining = append(remaining, check)
			}
		}
		have.CheckConstraints = remaining
		// Opting out of source CHECK emission never establishes ownership of
		// unexplained target restrictions. They may be user constraints or
		// restrictions from an older profile/type; report rather than drop.
		if !opts.CompareChecks && len(remaining) > 0 {
			result.checkDrift[table.Name] = append(result.checkDrift[table.Name], "target contains unexplained restrictions outside the current assessed requirements")
		}
	}
	return result, nil
}

// ComputeDriftWithFidelity compares exact current generated checks in addition
// to the legacy schema dimensions. Desired identifiers must already be target
// normalized, while columns retain original source types and metadata.
func ComputeDriftWithFidelity(desired, existing []driver.Table, source, target string, opts DriftOptions, profile fidelity.Profile) (Drift, fidelity.Assessment, error) {
	profile, err := fidelity.ParseProfile(string(profile))
	if err != nil {
		return Drift{}, fidelity.Assessment{}, err
	}
	if profile == fidelity.Legacy {
		return ComputeDrift(desired, existing, source, target, opts), fidelity.Assessment{}, nil
	}
	comparison, err := prepareFidelityComparison(desired, existing, source, target, opts, profile)
	if err != nil {
		return Drift{}, comparison.assessment, err
	}
	drift := ComputeDrift(comparison.desired, comparison.existing, source, target, opts)
	for table, reasons := range comparison.checkDrift {
		sort.Strings(reasons)
		found := false
		for i := range drift.ChangedTables {
			if drift.ChangedTables[i].Name == table {
				drift.ChangedTables[i].CheckDrift = strings.Join(reasons, "; ")
				found = true
			}
		}
		if !found {
			drift.ChangedTables = append(drift.ChangedTables, TableDrift{Name: table, CheckDrift: strings.Join(reasons, "; ")})
		}
	}
	for table, deltas := range comparison.columnDrift {
		found := false
		for i := range drift.ChangedTables {
			if drift.ChangedTables[i].Name == table {
				drift.ChangedTables[i].ColumnDeltas = append(drift.ChangedTables[i].ColumnDeltas, deltas...)
				sort.Strings(drift.ChangedTables[i].ColumnDeltas)
				found = true
			}
		}
		if !found {
			drift.ChangedTables = append(drift.ChangedTables, TableDrift{Name: table, ColumnDeltas: deltas})
		}
	}
	sort.Slice(drift.ChangedTables, func(i, j int) bool { return drift.ChangedTables[i].Name < drift.ChangedTables[j].Name })
	return drift, comparison.assessment, nil
}

// ComputeLiveDiffWithFidelity currently supports new tables and exact no-op
// comparisons. Existing-table evolution and profile transitions fail explicitly
// before rendering, including bound changes invisible in a bytea type alone.
func ComputeLiveDiffWithFidelity(desired, existing []driver.Table, source, target string, opts DriftOptions, profile fidelity.Profile) (Diff, fidelity.Assessment, error) {
	profile, err := fidelity.ParseProfile(string(profile))
	if err != nil {
		return Diff{}, fidelity.Assessment{}, err
	}
	if profile == fidelity.Legacy {
		return ComputeLiveDiff(desired, existing, source, target, opts), fidelity.Assessment{}, nil
	}
	comparison, err := prepareFidelityComparison(desired, existing, source, target, opts, profile)
	if err != nil {
		return Diff{}, comparison.assessment, err
	}
	if len(comparison.checkDrift) > 0 || len(comparison.columnDrift) > 0 {
		var reasons []string
		for table, changes := range comparison.checkDrift {
			for _, change := range changes {
				reasons = append(reasons, table+": "+change)
			}
		}
		for table, deltas := range comparison.columnDrift {
			for _, delta := range deltas {
				reasons = append(reasons, table+": "+delta)
			}
		}
		sort.Strings(reasons)
		return Diff{}, comparison.assessment, &FidelityEvolutionError{Reason: strings.Join(reasons, "; ") + "; validating or replacing restrictions on existing rows requires an explicit supported transition"}
	}
	diff := ComputeLiveDiff(comparison.desired, comparison.existing, source, target, opts)
	if len(diff.ChangedTables) != 0 || len(diff.Unsupported) != 0 || len(diff.RemovedTables) != 0 {
		return Diff{}, comparison.assessment, &FidelityEvolutionError{Reason: "existing-table changes and removals require source-semantics and restriction ownership evidence; only new tables and verified no-op comparisons are supported"}
	}
	// Restore complete source namespace metadata for new-table rendering;
	// CompareChecks/Indexes/ForeignKeys must not alter generated check names.
	byName := indexByLowerName(desired)
	for i := range diff.AddedTables {
		diff.AddedTables[i] = deepCopyTable(byName[strings.ToLower(diff.AddedTables[i].Name)])
	}
	return diff, comparison.assessment, nil
}

// RenderLiveDiffWithFidelity uses the same typed whole-table renderer as create.
// Source side objects remain separately gated and no generated check is emitted
// twice. It returns no executable partial plan for unsupported evolution.
func RenderLiveDiffWithFidelity(diff Diff, opts RenderOptions, managed DriftOptions, profile fidelity.Profile) (Plan, error) {
	profile, err := fidelity.ParseProfile(string(profile))
	if err != nil {
		return Plan{}, err
	}
	if profile == fidelity.Legacy {
		return RenderDeterministicWithOptions(diff, opts)
	}
	if len(diff.ChangedTables) != 0 || len(diff.RemovedTables) != 0 || len(diff.Unsupported) != 0 {
		return Plan{}, &FidelityEvolutionError{Reason: "existing-table changes and removals are not yet supported"}
	}
	renderer, err := newDeterministicRenderer(opts)
	if err != nil {
		return Plan{}, err
	}
	assessment, err := fidelity.AssessTable(&driver.Table{}, opts.SourceDialect, renderer.renderer.Target())
	if err != nil {
		return Plan{}, err
	}
	if err := fidelity.Gate(profile, assessment); err != nil {
		return Plan{}, err
	}
	var plan Plan
	sideTables := make([]driver.Table, 0, len(diff.AddedTables))
	for _, table := range diff.AddedTables {
		sql, _, err := renderer.renderer.CreateTableWithFidelity(&table, profile)
		if err != nil {
			return Plan{}, err
		}
		plan.Statements = append(plan.Statements, Statement{Table: table.Name, Description: "create table " + table.Name, SQL: sql, Risk: RiskSafe, Warnings: renderer.tableMappingWarnings(table)})
		sideTables = append(sideTables, addedTableForOptions(table, managed))
	}
	if err := renderer.renderAddedTableNonForeignKeySideObjects(&plan, sideTables); err != nil {
		return Plan{}, err
	}
	if err := renderer.renderAddedTableForeignKeys(&plan, sideTables); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

// Matching byte defaults are removed only from comparator copies after parsing
// both dialects into exact bytes. Mismatches remain explicit even if the legacy
// default classifier would put two different literals in the same category.
func normalizeFidelityBinaryDefaults(want, have *driver.Table, source string) []string {
	var deltas []string
	byName := make(map[string]int, len(have.Columns))
	for i, column := range have.Columns {
		byName[strings.ToLower(column.Name)] = i
	}
	for i, column := range want.Columns {
		kind := strings.ToLower(strings.TrimSpace(column.DataType))
		if kind != "binary" && kind != "varbinary" {
			continue
		}
		index, found := byName[strings.ToLower(column.Name)]
		if !found {
			continue
		}
		actual := have.Columns[index]
		if !driver.ColumnHasDefault(column) && !driver.ColumnHasDefault(actual) {
			continue
		}
		sourceLiteral, sourceErr := expr.ParseBinaryLiteral(column.DefaultExpression, source)
		targetLiteral, targetErr := expr.ParseBinaryLiteral(actual.DefaultExpression, "postgres")
		if driver.ColumnHasDefault(column) && driver.ColumnHasDefault(actual) && sourceErr == nil && targetErr == nil && sourceLiteral.Null == targetLiteral.Null && bytes.Equal(sourceLiteral.Bytes, targetLiteral.Bytes) {
			want.Columns[i].DefaultExpression, want.Columns[i].HasDefault = "", false
			have.Columns[index].DefaultExpression, have.Columns[index].HasDefault = "", false
			continue
		}
		deltas = append(deltas, fmt.Sprintf("%s default: source binary literal and target default differ or cannot be assessed", column.Name))
	}
	sort.Strings(deltas)
	return deltas
}

// equalFidelityExpression handles only catalog decoration of our two typed
// predicates. It does not reuse the permissive legacy comparator, which strips
// arbitrary casts and classifies some different expressions as equivalent.
func equalFidelityExpression(expected, actual string) bool {
	a, ok := fidelityExpressionTokens(expected)
	if !ok {
		return false
	}
	b, ok := fidelityExpressionTokens(actual)
	return ok && strings.Join(a, "\x00") == strings.Join(b, "\x00")
}

func fidelityExpressionTokens(input string) ([]string, bool) {
	var tokens []string
	depth := 0
	for len(input) > 0 {
		r, width := utf8.DecodeRuneInString(input)
		if unicode.IsSpace(r) {
			input = input[width:]
			continue
		}
		if r == '(' || r == ')' {
			if r == '(' {
				depth++
			} else {
				depth--
				if depth < 0 {
					return nil, false
				}
			}
			input = input[width:]
			continue
		}
		if strings.HasPrefix(input, "::") {
			if len(tokens) == 0 || !strings.HasPrefix(tokens[len(tokens)-1], "number:") {
				return nil, false
			}
			input = strings.TrimLeftFunc(input[2:], unicode.IsSpace)
			end := 0
			for end < len(input) && ((input[end] >= 'a' && input[end] <= 'z') || (input[end] >= 'A' && input[end] <= 'Z') || (input[end] >= '0' && input[end] <= '9')) {
				end++
			}
			switch strings.ToLower(input[:end]) {
			case "smallint", "int2", "integer", "int4", "bigint", "int8":
			default:
				return nil, false
			}
			input = input[end:]
			continue
		}
		if r == '"' {
			input = input[1:]
			var name strings.Builder
			closed := false
			for len(input) > 0 {
				if input[0] == '"' {
					if len(input) > 1 && input[1] == '"' {
						name.WriteByte('"')
						input = input[2:]
						continue
					}
					input, closed = input[1:], true
					break
				}
				name.WriteByte(input[0])
				input = input[1:]
			}
			if !closed {
				return nil, false
			}
			tokens = append(tokens, "identifier:"+name.String())
			continue
		}
		if unicode.IsLetter(r) || r == '_' {
			end := width
			for end < len(input) {
				r, width = utf8.DecodeRuneInString(input[end:])
				if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
					break
				}
				end += width
			}
			word := strings.ToLower(input[:end])
			isFunction := word == "octet_length" && strings.HasPrefix(strings.TrimLeftFunc(input[end:], unicode.IsSpace), "(")
			if word == "and" || isFunction {
				tokens = append(tokens, word)
			} else {
				tokens = append(tokens, "identifier:"+word)
			}
			input = input[end:]
			continue
		}
		if r >= '0' && r <= '9' {
			end := 1
			for end < len(input) && input[end] >= '0' && input[end] <= '9' {
				end++
			}
			tokens = append(tokens, "number:"+input[:end])
			input = input[end:]
			continue
		}
		if strings.HasPrefix(input, ">=") || strings.HasPrefix(input, "<=") {
			tokens = append(tokens, input[:2])
			input = input[2:]
			continue
		}
		return nil, false
	}
	return tokens, depth == 0 && len(tokens) > 0
}
