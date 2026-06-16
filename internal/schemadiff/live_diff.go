package schemadiff

import (
	"fmt"
	"sort"
	"strings"

	"smt/internal/driver"
)

// ComputeLiveDiff compares the desired target schema derived from the current
// source against the existing live target schema. Desired tables should already
// have target-normalized identifiers and target schema references; existing
// tables should come from target introspection.
func ComputeLiveDiff(desired, existing []driver.Table, sourceDialect, targetDialect string, opts DriftOptions) Diff {
	desiredByName := indexByLowerName(desired)
	existingByName := indexByLowerName(existing)

	var d Diff
	for _, name := range sortedLowerKeys(desiredByName) {
		want := desiredByName[name]
		have, ok := existingByName[name]
		if !ok {
			d.AddedTables = append(d.AddedTables, addedTableForOptions(want, opts))
			continue
		}
		td, unsupported := liveTableDiff(want, have, sourceDialect, targetDialect, opts)
		d.Unsupported = append(d.Unsupported, unsupported...)
		if !td.IsEmpty() {
			d.ChangedTables = append(d.ChangedTables, td)
		}
	}
	for _, name := range sortedLowerKeys(existingByName) {
		if _, ok := desiredByName[name]; !ok {
			d.RemovedTables = append(d.RemovedTables, existingByName[name])
		}
	}
	return d
}

func liveTableDiff(want, have driver.Table, sourceDialect, targetDialect string, opts DriftOptions) (TableDiff, []UnsupportedChange) {
	td := TableDiff{Schema: want.Schema, Name: want.Name, Curr: want}
	var unsupported []UnsupportedChange

	wantByName := columnByLowerName(want.Columns)
	haveByName := columnByLowerName(have.Columns)

	for _, wc := range want.Columns {
		hc, ok := haveByName[strings.ToLower(wc.Name)]
		if !ok {
			td.AddedColumns = append(td.AddedColumns, wc)
			continue
		}
		criteria := columnDeltaCriteria(driver.CompareColumns([]driver.Column{wc}, []driver.Column{hc}, sourceDialect, targetDialect))
		if wc.IsComputed != hc.IsComputed {
			criteria = appendIfMissing(criteria, "computed")
		} else if wc.IsComputed && wc.ComputedPersisted != hc.ComputedPersisted && targetSupportsVirtualComputed(targetDialect) {
			criteria = appendIfMissing(criteria, "computed_storage")
		}
		criteria = append(criteria, unsupportedColumnMetadataCriteria(wc, hc, sourceDialect, targetDialect)...)
		if len(criteria) == 0 {
			continue
		}
		if reason := liveColumnChangeUnsupportedReason(criteria); reason != "" {
			unsupported = append(unsupported, UnsupportedChange{
				Table:       want.Name,
				Description: fmt.Sprintf("change column %s", wc.Name),
				Reason:      reason,
			})
			continue
		}
		td.ChangedColumns = append(td.ChangedColumns, ColumnChange{Name: wc.Name, Old: hc, New: wc, Criteria: criteria})
	}
	for _, hc := range have.Columns {
		if _, ok := wantByName[strings.ToLower(hc.Name)]; !ok {
			td.RemovedColumns = append(td.RemovedColumns, hc)
		}
	}

	if !sameColSetCI(want.PrimaryKey, have.PrimaryKey) {
		unsupported = append(unsupported, UnsupportedChange{
			Table:       want.Name,
			Description: "change primary key",
			Reason:      "primary key add/drop/re-key is not supported by deterministic sync",
		})
	}

	if opts.CompareIndexes {
		td.AddedIndexes, td.RemovedIndexes = liveIndexDiff(want.Indexes, have.Indexes)
	}
	if opts.CompareForeignKeys {
		td.AddedForeignKeys, td.RemovedForeignKeys = liveForeignKeyDiff(want.ForeignKeys, have.ForeignKeys, want.Schema, have.Schema)
	}
	if opts.CompareChecks {
		switch {
		case len(want.CheckConstraints) > len(have.CheckConstraints):
			unsupported = append(unsupported, UnsupportedChange{
				Table:       want.Name,
				Description: "reconcile check constraints",
				Reason:      "live target check predicates are dialect-rewritten; deterministic sync compares counts but cannot safely identify which check to add",
			})
		case len(want.CheckConstraints) < len(have.CheckConstraints):
			unsupported = append(unsupported, UnsupportedChange{
				Table:       want.Name,
				Description: "reconcile check constraints",
				Reason:      "dropping target-only check constraints is not supported by deterministic sync",
			})
		case sameDialect(sourceDialect, targetDialect) && !sameCheckDefinitions(want.CheckConstraints, have.CheckConstraints):
			unsupported = append(unsupported, UnsupportedChange{
				Table:       want.Name,
				Description: "reconcile check constraints",
				Reason:      "same-dialect check predicate changes are detected but not rendered by deterministic sync",
			})
		}
	}

	sort.Slice(td.AddedColumns, func(i, j int) bool {
		return strings.ToLower(td.AddedColumns[i].Name) < strings.ToLower(td.AddedColumns[j].Name)
	})
	sort.Slice(td.RemovedColumns, func(i, j int) bool {
		return strings.ToLower(td.RemovedColumns[i].Name) < strings.ToLower(td.RemovedColumns[j].Name)
	})
	sort.Slice(td.ChangedColumns, func(i, j int) bool {
		return strings.ToLower(td.ChangedColumns[i].Name) < strings.ToLower(td.ChangedColumns[j].Name)
	})
	return td, unsupported
}

func addedTableForOptions(t driver.Table, opts DriftOptions) driver.Table {
	out := deepCopyTable(t)
	if !opts.CompareIndexes {
		out.Indexes = nil
	}
	if !opts.CompareForeignKeys {
		out.ForeignKeys = nil
	}
	if !opts.CompareChecks {
		out.CheckConstraints = nil
	}
	return out
}

func columnByLowerName(cols []driver.Column) map[string]driver.Column {
	out := make(map[string]driver.Column, len(cols))
	for _, c := range cols {
		out[strings.ToLower(c.Name)] = c
	}
	return out
}

func columnDeltaCriteria(deltas []driver.ColumnDelta) []string {
	seen := make(map[string]bool, len(deltas))
	var out []string
	for _, d := range deltas {
		if d.Criterion == "missing" || seen[d.Criterion] {
			continue
		}
		seen[d.Criterion] = true
		out = append(out, d.Criterion)
	}
	sort.Strings(out)
	return out
}

func appendIfMissing(in []string, criterion string) []string {
	for _, c := range in {
		if c == criterion {
			return in
		}
	}
	return append(in, criterion)
}

func unsupportedColumnMetadataCriteria(want, have driver.Column, sourceDialect, targetDialect string) []string {
	var criteria []string
	if strings.TrimSpace(want.OnUpdateExpression) != strings.TrimSpace(have.OnUpdateExpression) {
		criteria = append(criteria, "on_update")
	}
	if want.SRID != have.SRID {
		criteria = append(criteria, "srid")
	}
	if sameDialect(sourceDialect, targetDialect) {
		if !stringSlicesEqual(want.EnumValues, have.EnumValues) {
			criteria = append(criteria, "enum_values")
		}
		if want.DisplayWidth != have.DisplayWidth {
			criteria = append(criteria, "display_width")
		}
		if want.IsComputed && have.IsComputed && normalizedExpression(want.ComputedExpression) != normalizedExpression(have.ComputedExpression) {
			criteria = append(criteria, "computed_expression")
		}
	}
	return criteria
}

func liveColumnChangeUnsupportedReason(criteria []string) string {
	reasons := make(map[string]bool)
	for _, c := range criteria {
		switch c {
		case "identity":
			reasons["identity changes are not supported by deterministic sync"] = true
		case "computed", "computed_storage", "computed_expression":
			reasons["computed-column changes are not supported by deterministic sync"] = true
		case "on_update":
			reasons["ON UPDATE clause changes are detected but not rendered by deterministic sync"] = true
		case "enum_values":
			reasons["ENUM/SET value-list changes are detected but not rendered by deterministic sync"] = true
		case "srid":
			reasons["spatial SRID changes are detected but not rendered by deterministic sync"] = true
		case "display_width":
			reasons["display-width changes are detected but not rendered by deterministic sync"] = true
		}
	}
	if len(reasons) == 0 {
		return ""
	}
	out := make([]string, 0, len(reasons))
	for reason := range reasons {
		out = append(out, reason)
	}
	sort.Strings(out)
	return strings.Join(out, "; ")
}

func sameDialect(a, b string) bool {
	return driver.Canonicalize(a) == driver.Canonicalize(b)
}

func sameCheckDefinitions(want, have []driver.CheckConstraint) bool {
	if len(want) != len(have) {
		return false
	}
	wantDefs := normalizedCheckDefinitions(want)
	haveDefs := normalizedCheckDefinitions(have)
	for i := range wantDefs {
		if wantDefs[i] != haveDefs[i] {
			return false
		}
	}
	return true
}

func normalizedCheckDefinitions(checks []driver.CheckConstraint) []string {
	out := make([]string, len(checks))
	for i, chk := range checks {
		out[i] = normalizedExpression(chk.Definition)
	}
	sort.Strings(out)
	return out
}

func normalizedExpression(expr string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(expr)), " "))
}

func liveIndexDiff(want, have []driver.Index) (added, removed []driver.Index) {
	wantByKey := make(map[string]driver.Index, len(want))
	for _, idx := range want {
		wantByKey[indexKey(idx)] = idx
	}
	haveByKey := make(map[string]driver.Index, len(have))
	for _, idx := range have {
		haveByKey[indexKey(idx)] = idx
	}
	for _, key := range sortedIndexKeys(wantByKey) {
		if _, ok := haveByKey[key]; !ok {
			added = append(added, wantByKey[key])
		}
	}
	for _, key := range sortedIndexKeys(haveByKey) {
		if _, ok := wantByKey[key]; !ok {
			removed = append(removed, haveByKey[key])
		}
	}
	return added, removed
}

func indexKey(idx driver.Index) string {
	key := orderedCols(idx.Columns)
	if len(idx.IncludeCols) > 0 {
		key += "/include:" + orderedCols(idx.IncludeCols)
	}
	if strings.TrimSpace(idx.Filter) != "" {
		key += "/filtered"
	}
	if idx.IsUnique {
		key = "unique:" + key
	}
	return key
}

func sortedIndexKeys(m map[string]driver.Index) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func liveForeignKeyDiff(want, have []driver.ForeignKey, wantSchema, haveSchema string) (added, removed []driver.ForeignKey) {
	wantByKey := make(map[string]driver.ForeignKey, len(want))
	for _, fk := range want {
		wantByKey[foreignKeyKey(fk, wantSchema)] = fk
	}
	haveByKey := make(map[string]driver.ForeignKey, len(have))
	for _, fk := range have {
		haveByKey[foreignKeyKey(fk, haveSchema)] = fk
	}
	for _, key := range sortedForeignKeyKeys(wantByKey) {
		if _, ok := haveByKey[key]; !ok {
			added = append(added, wantByKey[key])
		}
	}
	for _, key := range sortedForeignKeyKeys(haveByKey) {
		if _, ok := wantByKey[key]; !ok {
			removed = append(removed, haveByKey[key])
		}
	}
	return added, removed
}

func foreignKeyKey(fk driver.ForeignKey, ownerSchema string) string {
	pairs := make([]string, len(fk.Columns))
	for i, c := range fk.Columns {
		ref := ""
		if i < len(fk.RefColumns) {
			ref = strings.ToLower(fk.RefColumns[i])
		}
		pairs[i] = strings.ToLower(c) + ":" + ref
	}
	schema := "self"
	if s := strings.TrimSpace(fk.RefSchema); s != "" && !strings.EqualFold(s, ownerSchema) {
		schema = strings.ToLower(s)
	}
	return strings.Join(pairs, ",") + "->" + schema + "." + strings.ToLower(fk.RefTable) +
		"|" + normAction(fk.OnDelete) + "|" + normAction(fk.OnUpdate)
}

func sortedForeignKeyKeys(m map[string]driver.ForeignKey) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
