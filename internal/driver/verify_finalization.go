package driver

import (
	"fmt"
	"strings"

	exprir "smt/internal/expr"
)

// VerifyFinalizationDDLDeterministic parses SMT-rendered side-object DDL and
// compares it mechanically with the source metadata. It replaces the legacy
// free-text auditor for CREATE INDEX, FOREIGN KEY, and CHECK finalization.
func VerifyFinalizationDDLDeterministic(req VerifyFinalizationDDLRequest) (*VerifyResult, error) {
	if req.Table == nil {
		return nil, fmt.Errorf("Table is required")
	}
	if req.ProposedDDL == "" {
		return nil, fmt.Errorf("ProposedDDL is required")
	}

	var (
		deltas []FinalizationDelta
		err    error
	)
	switch req.Type {
	case DDLTypeIndex:
		if req.Index == nil {
			return nil, fmt.Errorf("Index is required for DDLTypeIndex verify")
		}
		deltas, err = compareIndexDDL(req)
	case DDLTypeForeignKey:
		if req.ForeignKey == nil {
			return nil, fmt.Errorf("ForeignKey is required for DDLTypeForeignKey verify")
		}
		deltas, err = compareForeignKeyDDL(req)
	case DDLTypeCheckConstraint:
		if req.CheckConstraint == nil {
			return nil, fmt.Errorf("CheckConstraint is required for DDLTypeCheckConstraint verify")
		}
		deltas, err = compareCheckDDL(req)
	default:
		return nil, fmt.Errorf("unsupported DDL type for verify: %s", req.Type)
	}
	if err != nil {
		return &VerifyResult{
			OK:     false,
			Issues: []string{fmt.Sprintf("verifier could not parse target %s DDL: %v", req.Type, err)},
		}, nil
	}
	if len(deltas) == 0 {
		return &VerifyResult{OK: true}, nil
	}
	issues := make([]string, len(deltas))
	for i, d := range deltas {
		issues[i] = d.String()
	}
	return &VerifyResult{OK: false, Issues: issues}, nil
}

// FinalizationDelta records a single side-object mismatch.
type FinalizationDelta struct {
	Object    string
	Criterion string
	SourceVal string
	TargetVal string
}

func (d FinalizationDelta) String() string {
	return fmt.Sprintf("%s: %s - %s vs %s", d.Object, d.Criterion, d.SourceVal, d.TargetVal)
}

type parsedIndexDDL struct {
	Name        string
	Table       string
	Columns     []string
	Unique      bool
	IncludeCols []string
	Filter      string
}

type parsedForeignKeyDDL struct {
	Name       string
	Table      string
	Columns    []string
	RefSchema  string
	RefTable   string
	RefColumns []string
	OnDelete   string
	OnUpdate   string
}

type parsedCheckDDL struct {
	Name       string
	Table      string
	Definition string
}

func compareIndexDDL(req VerifyFinalizationDDLRequest) ([]FinalizationDelta, error) {
	got, err := parseIndexDDL(req.ProposedDDL)
	if err != nil {
		return nil, err
	}
	target := req.TargetDBType
	wantName := finalizationNameKey(target, req.Index.Name)
	obj := "index " + req.Index.Name
	var deltas []FinalizationDelta
	if gotName := finalizationNameKey(target, got.Name); gotName != wantName {
		deltas = append(deltas, finalizationDelta(obj, "name", wantName, gotName))
	}
	if gotTable := finalizationNameKey(target, got.Table); gotTable != finalizationNameKey(target, req.Table.Name) {
		deltas = append(deltas, finalizationDelta(obj, "table", finalizationNameKey(target, req.Table.Name), gotTable))
	}
	if !stringSlicesEqualOrdered(finalizationNameKeys(target, req.Index.Columns), finalizationNameKeys(target, got.Columns)) {
		deltas = append(deltas, finalizationDelta(obj, "columns", strings.Join(finalizationNameKeys(target, req.Index.Columns), ","), strings.Join(finalizationNameKeys(target, got.Columns), ",")))
	}
	if req.Index.IsUnique != got.Unique {
		deltas = append(deltas, finalizationDelta(obj, "unique", boolText(req.Index.IsUnique), boolText(got.Unique)))
	}
	// MySQL has no INCLUDE columns, and the renderer intentionally drops them
	// there. PostgreSQL and SQL Server should preserve the ordered list.
	if targetSupportsIncludeColumns(target) &&
		!stringSlicesEqualOrdered(finalizationNameKeys(target, req.Index.IncludeCols), finalizationNameKeys(target, got.IncludeCols)) {
		deltas = append(deltas, finalizationDelta(obj, "include_cols", strings.Join(finalizationNameKeys(target, req.Index.IncludeCols), ","), strings.Join(finalizationNameKeys(target, got.IncludeCols), ",")))
	}
	if !finalizationPredicatesEquivalent(req.Index.Filter, req.SourceDBType, got.Filter, req.TargetDBType) {
		deltas = append(deltas, finalizationDelta(obj, "filter", finalizationPredicateLabel(req.Index.Filter, req.SourceDBType), finalizationPredicateLabel(got.Filter, req.TargetDBType)))
	}
	return deltas, nil
}

func compareForeignKeyDDL(req VerifyFinalizationDDLRequest) ([]FinalizationDelta, error) {
	got, err := parseForeignKeyDDL(req.ProposedDDL)
	if err != nil {
		return nil, err
	}
	target := req.TargetDBType
	obj := "foreign key " + req.ForeignKey.Name
	var deltas []FinalizationDelta
	if gotName := finalizationNameKey(target, got.Name); gotName != finalizationNameKey(target, req.ForeignKey.Name) {
		deltas = append(deltas, finalizationDelta(obj, "name", finalizationNameKey(target, req.ForeignKey.Name), gotName))
	}
	if gotTable := finalizationNameKey(target, got.Table); gotTable != finalizationNameKey(target, req.Table.Name) {
		deltas = append(deltas, finalizationDelta(obj, "table", finalizationNameKey(target, req.Table.Name), gotTable))
	}
	if !stringSlicesEqualOrdered(finalizationNameKeys(target, req.ForeignKey.Columns), finalizationNameKeys(target, got.Columns)) {
		deltas = append(deltas, finalizationDelta(obj, "columns", strings.Join(finalizationNameKeys(target, req.ForeignKey.Columns), ","), strings.Join(finalizationNameKeys(target, got.Columns), ",")))
	}
	if gotRefTable := finalizationNameKey(target, got.RefTable); gotRefTable != finalizationNameKey(target, req.ForeignKey.RefTable) {
		deltas = append(deltas, finalizationDelta(obj, "ref_table", finalizationNameKey(target, req.ForeignKey.RefTable), gotRefTable))
	}
	if !stringSlicesEqualOrdered(finalizationNameKeys(target, req.ForeignKey.RefColumns), finalizationNameKeys(target, got.RefColumns)) {
		deltas = append(deltas, finalizationDelta(obj, "ref_columns", strings.Join(finalizationNameKeys(target, req.ForeignKey.RefColumns), ","), strings.Join(finalizationNameKeys(target, got.RefColumns), ",")))
	}
	if finalizationActionKey(req.ForeignKey.OnDelete) != finalizationActionKey(got.OnDelete) {
		deltas = append(deltas, finalizationDelta(obj, "on_delete", finalizationActionKey(req.ForeignKey.OnDelete), finalizationActionKey(got.OnDelete)))
	}
	if finalizationActionKey(req.ForeignKey.OnUpdate) != finalizationActionKey(got.OnUpdate) {
		deltas = append(deltas, finalizationDelta(obj, "on_update", finalizationActionKey(req.ForeignKey.OnUpdate), finalizationActionKey(got.OnUpdate)))
	}
	return deltas, nil
}

func compareCheckDDL(req VerifyFinalizationDDLRequest) ([]FinalizationDelta, error) {
	got, err := parseCheckDDL(req.ProposedDDL)
	if err != nil {
		return nil, err
	}
	target := req.TargetDBType
	obj := "check constraint " + req.CheckConstraint.Name
	var deltas []FinalizationDelta
	if gotName := finalizationNameKey(target, got.Name); gotName != finalizationNameKey(target, req.CheckConstraint.Name) {
		deltas = append(deltas, finalizationDelta(obj, "name", finalizationNameKey(target, req.CheckConstraint.Name), gotName))
	}
	if gotTable := finalizationNameKey(target, got.Table); gotTable != finalizationNameKey(target, req.Table.Name) {
		deltas = append(deltas, finalizationDelta(obj, "table", finalizationNameKey(target, req.Table.Name), gotTable))
	}
	if !finalizationPredicatesEquivalent(req.CheckConstraint.Definition, req.SourceDBType, got.Definition, req.TargetDBType) {
		deltas = append(deltas, finalizationDelta(obj, "predicate", finalizationPredicateLabel(req.CheckConstraint.Definition, req.SourceDBType), finalizationPredicateLabel(got.Definition, req.TargetDBType)))
	}
	return deltas, nil
}

func finalizationDelta(object, criterion, sourceVal, targetVal string) FinalizationDelta {
	if sourceVal == "" {
		sourceVal = "<empty>"
	}
	if targetVal == "" {
		targetVal = "<empty>"
	}
	return FinalizationDelta{Object: object, Criterion: criterion, SourceVal: sourceVal, TargetVal: targetVal}
}

func finalizationPredicatesEquivalent(src, srcDialect, tgt, tgtDialect string) bool {
	src = strings.TrimSpace(src)
	tgt = strings.TrimSpace(tgt)
	if src == "" || tgt == "" {
		return src == "" && tgt == ""
	}
	srcNode := exprir.ParseCheck(src, finalizationExprDialect(srcDialect))
	tgtNode := exprir.ParseCheck(tgt, finalizationExprDialect(tgtDialect))
	if exprir.Equal(srcNode, tgtNode) {
		return true
	}
	return normalizePredicateText(src) == normalizePredicateText(tgt)
}

func finalizationPredicateLabel(raw, dialect string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	return exprir.ClassLabel(exprir.ParseCheck(raw, finalizationExprDialect(dialect)))
}

func finalizationExprDialect(dialect string) string {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "postgres", "postgresql", "pg":
		return exprir.Postgres
	case "mssql", "sqlserver", "sql_server", "sql-server":
		return exprir.MSSQL
	case "mysql", "mariadb", "maria":
		return exprir.MySQL
	default:
		return ""
	}
}

func normalizePredicateText(s string) string {
	s = strings.TrimSpace(s)
	s = stripBalancedOuterParensText(s)
	var b strings.Builder
	inSingle := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' {
			b.WriteByte(ch)
			if inSingle && i+1 < len(s) && s[i+1] == '\'' {
				i++
				b.WriteByte(s[i])
				continue
			}
			inSingle = !inSingle
			continue
		}
		if inSingle {
			b.WriteByte(ch)
			continue
		}
		switch ch {
		case '[', ']', '`', '"':
			continue
		default:
			if ch == '\t' || ch == '\n' || ch == '\r' {
				ch = ' '
			}
			b.WriteByte(ch)
		}
	}
	return strings.Join(strings.Fields(strings.ToLower(b.String())), " ")
}

func parseIndexDDL(ddl string) (parsedIndexDDL, error) {
	s := trimSQLStatement(ddl)
	rest, ok := consumeKeyword(s, "CREATE")
	if !ok {
		return parsedIndexDDL{}, fmt.Errorf("expected CREATE")
	}
	unique := false
	if next, ok := consumeKeyword(rest, "UNIQUE"); ok {
		unique = true
		rest = next
	}
	rest, ok = consumeKeyword(rest, "INDEX")
	if !ok {
		return parsedIndexDDL{}, fmt.Errorf("expected INDEX")
	}
	onIdx := findKeywordOutside(rest, "ON")
	if onIdx < 0 {
		return parsedIndexDDL{}, fmt.Errorf("expected ON")
	}
	namePart := strings.TrimSpace(rest[:onIdx])
	rest = strings.TrimSpace(rest[onIdx+len("ON"):])
	openIdx := findByteOutside(rest, '(')
	if openIdx < 0 {
		return parsedIndexDDL{}, fmt.Errorf("expected index column list")
	}
	tablePart := strings.TrimSpace(rest[:openIdx])
	colText, closeIdx, ok := readBalancedParen(rest, openIdx)
	if !ok {
		return parsedIndexDDL{}, fmt.Errorf("unbalanced index column list")
	}
	after := strings.TrimSpace(rest[closeIdx+1:])
	out := parsedIndexDDL{
		Name:    lastQualifiedIdentifierPart(namePart),
		Table:   lastQualifiedIdentifierPart(tablePart),
		Columns: parseIdentifierList(colText),
		Unique:  unique,
	}

	if next, ok := consumeKeyword(after, "INCLUDE"); ok {
		open := findByteOutside(next, '(')
		if open < 0 {
			return parsedIndexDDL{}, fmt.Errorf("expected INCLUDE column list")
		}
		includeText, includeClose, ok := readBalancedParen(next, open)
		if !ok {
			return parsedIndexDDL{}, fmt.Errorf("unbalanced INCLUDE column list")
		}
		out.IncludeCols = parseIdentifierList(includeText)
		after = strings.TrimSpace(next[includeClose+1:])
	}
	if next, ok := consumeKeyword(after, "WHERE"); ok {
		out.Filter = strings.TrimSpace(next)
		after = ""
	}
	if strings.TrimSpace(after) != "" {
		return parsedIndexDDL{}, fmt.Errorf("unexpected trailing CREATE INDEX text %q", after)
	}
	return out, nil
}

func parseForeignKeyDDL(ddl string) (parsedForeignKeyDDL, error) {
	parts, err := parseAlterTableAddConstraint(ddl)
	if err != nil {
		return parsedForeignKeyDDL{}, err
	}
	rest, ok := consumeKeyword(parts.afterConstraint, "FOREIGN")
	if !ok {
		return parsedForeignKeyDDL{}, fmt.Errorf("expected FOREIGN KEY")
	}
	rest, ok = consumeKeyword(rest, "KEY")
	if !ok {
		return parsedForeignKeyDDL{}, fmt.Errorf("expected FOREIGN KEY")
	}
	openIdx := findByteOutside(rest, '(')
	if openIdx < 0 {
		return parsedForeignKeyDDL{}, fmt.Errorf("expected local column list")
	}
	localText, localClose, ok := readBalancedParen(rest, openIdx)
	if !ok {
		return parsedForeignKeyDDL{}, fmt.Errorf("unbalanced local column list")
	}
	rest = strings.TrimSpace(rest[localClose+1:])
	rest, ok = consumeKeyword(rest, "REFERENCES")
	if !ok {
		return parsedForeignKeyDDL{}, fmt.Errorf("expected REFERENCES")
	}
	refOpen := findByteOutside(rest, '(')
	if refOpen < 0 {
		return parsedForeignKeyDDL{}, fmt.Errorf("expected referenced column list")
	}
	refTablePart := strings.TrimSpace(rest[:refOpen])
	refText, refClose, ok := readBalancedParen(rest, refOpen)
	if !ok {
		return parsedForeignKeyDDL{}, fmt.Errorf("unbalanced referenced column list")
	}
	after := strings.TrimSpace(rest[refClose+1:])
	refParts := splitQualifiedIdentifier(refTablePart)
	out := parsedForeignKeyDDL{
		Name:       parts.name,
		Table:      parts.table,
		Columns:    parseIdentifierList(localText),
		RefTable:   lastQualifiedIdentifierPart(refTablePart),
		RefColumns: parseIdentifierList(refText),
	}
	if len(refParts) > 1 {
		out.RefSchema = refParts[len(refParts)-2]
	}
	for strings.TrimSpace(after) != "" {
		if next, ok := consumeKeyword(after, "ON"); ok {
			if rest, ok := consumeKeyword(next, "DELETE"); ok {
				out.OnDelete, after = readReferentialAction(rest)
				continue
			}
			if rest, ok := consumeKeyword(next, "UPDATE"); ok {
				out.OnUpdate, after = readReferentialAction(rest)
				continue
			}
		}
		return parsedForeignKeyDDL{}, fmt.Errorf("unexpected trailing FOREIGN KEY text %q", after)
	}
	return out, nil
}

func parseCheckDDL(ddl string) (parsedCheckDDL, error) {
	parts, err := parseAlterTableAddConstraint(ddl)
	if err != nil {
		return parsedCheckDDL{}, err
	}
	rest, ok := consumeKeyword(parts.afterConstraint, "CHECK")
	if !ok {
		return parsedCheckDDL{}, fmt.Errorf("expected CHECK")
	}
	def := strings.TrimSpace(rest)
	if def == "" {
		return parsedCheckDDL{}, fmt.Errorf("expected CHECK predicate")
	}
	return parsedCheckDDL{Name: parts.name, Table: parts.table, Definition: def}, nil
}

type alterTableConstraintParts struct {
	table           string
	name            string
	afterConstraint string
}

func parseAlterTableAddConstraint(ddl string) (alterTableConstraintParts, error) {
	s := trimSQLStatement(ddl)
	rest, ok := consumeKeyword(s, "ALTER")
	if !ok {
		return alterTableConstraintParts{}, fmt.Errorf("expected ALTER TABLE")
	}
	rest, ok = consumeKeyword(rest, "TABLE")
	if !ok {
		return alterTableConstraintParts{}, fmt.Errorf("expected ALTER TABLE")
	}
	addIdx := findKeywordOutside(rest, "ADD")
	if addIdx < 0 {
		return alterTableConstraintParts{}, fmt.Errorf("expected ADD CONSTRAINT")
	}
	tablePart := strings.TrimSpace(rest[:addIdx])
	rest = strings.TrimSpace(rest[addIdx+len("ADD"):])
	rest, ok = consumeKeyword(rest, "CONSTRAINT")
	if !ok {
		return alterTableConstraintParts{}, fmt.Errorf("expected CONSTRAINT")
	}
	kindIdx := findFirstKeywordOutside(rest, "FOREIGN", "CHECK")
	if kindIdx < 0 {
		return alterTableConstraintParts{}, fmt.Errorf("expected FOREIGN KEY or CHECK")
	}
	namePart := strings.TrimSpace(rest[:kindIdx])
	after := strings.TrimSpace(rest[kindIdx:])
	return alterTableConstraintParts{
		table:           lastQualifiedIdentifierPart(tablePart),
		name:            lastQualifiedIdentifierPart(namePart),
		afterConstraint: after,
	}, nil
}

func readReferentialAction(s string) (action, rest string) {
	s = strings.TrimSpace(s)
	next := findKeywordOutside(s, "ON")
	if next < 0 {
		return strings.TrimSpace(s), ""
	}
	return strings.TrimSpace(s[:next]), strings.TrimSpace(s[next:])
}

func trimSQLStatement(s string) string {
	s = strings.TrimSpace(s)
	for strings.HasSuffix(s, ";") {
		s = strings.TrimSpace(strings.TrimSuffix(s, ";"))
	}
	return s
}

func consumeKeyword(s, keyword string) (string, bool) {
	s = strings.TrimSpace(s)
	if len(s) < len(keyword) || !strings.EqualFold(s[:len(keyword)], keyword) {
		return "", false
	}
	if len(s) > len(keyword) && isIdentByte(s[len(keyword)]) {
		return "", false
	}
	return strings.TrimSpace(s[len(keyword):]), true
}

func findFirstKeywordOutside(s string, keywords ...string) int {
	best := -1
	for _, keyword := range keywords {
		idx := findKeywordOutside(s, keyword)
		if idx >= 0 && (best < 0 || idx < best) {
			best = idx
		}
	}
	return best
}

func findKeywordOutside(s, keyword string) int {
	kwLen := len(keyword)
	inSingle := false
	inDouble := false
	inBacktick := false
	inBracket := false
	depth := 0
	for i := 0; i+kwLen <= len(s); i++ {
		ch := s[i]
		if inSingle {
			if ch == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '"' {
				if i+1 < len(s) && s[i+1] == '"' {
					i++
					continue
				}
				inDouble = false
			}
			continue
		}
		if inBacktick {
			if ch == '`' {
				if i+1 < len(s) && s[i+1] == '`' {
					i++
					continue
				}
				inBacktick = false
			}
			continue
		}
		if inBracket {
			if ch == ']' {
				if i+1 < len(s) && s[i+1] == ']' {
					i++
					continue
				}
				inBracket = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
			continue
		case '"':
			inDouble = true
			continue
		case '`':
			inBacktick = true
			continue
		case '[':
			inBracket = true
			continue
		case '(':
			depth++
			continue
		case ')':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth == 0 && strings.EqualFold(s[i:i+kwLen], keyword) &&
			(i == 0 || !isIdentByte(s[i-1])) &&
			(i+kwLen == len(s) || !isIdentByte(s[i+kwLen])) {
			return i
		}
	}
	return -1
}

func findByteOutside(s string, want byte) int {
	inSingle := false
	inDouble := false
	inBacktick := false
	inBracket := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inSingle {
			if ch == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '"' {
				if i+1 < len(s) && s[i+1] == '"' {
					i++
					continue
				}
				inDouble = false
			}
			continue
		}
		if inBacktick {
			if ch == '`' {
				if i+1 < len(s) && s[i+1] == '`' {
					i++
					continue
				}
				inBacktick = false
			}
			continue
		}
		if inBracket {
			if ch == ']' {
				if i+1 < len(s) && s[i+1] == ']' {
					i++
					continue
				}
				inBracket = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case '[':
			inBracket = true
		default:
			if ch == want {
				return i
			}
		}
	}
	return -1
}

func readBalancedParen(s string, openIdx int) (content string, closeIdx int, ok bool) {
	if openIdx < 0 || openIdx >= len(s) || s[openIdx] != '(' {
		return "", -1, false
	}
	inSingle := false
	inDouble := false
	inBacktick := false
	inBracket := false
	depth := 0
	for i := openIdx; i < len(s); i++ {
		ch := s[i]
		if inSingle {
			if ch == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '"' {
				if i+1 < len(s) && s[i+1] == '"' {
					i++
					continue
				}
				inDouble = false
			}
			continue
		}
		if inBacktick {
			if ch == '`' {
				if i+1 < len(s) && s[i+1] == '`' {
					i++
					continue
				}
				inBacktick = false
			}
			continue
		}
		if inBracket {
			if ch == ']' {
				if i+1 < len(s) && s[i+1] == ']' {
					i++
					continue
				}
				inBracket = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case '[':
			inBracket = true
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return s[openIdx+1 : i], i, true
			}
		}
	}
	return "", -1, false
}

func parseIdentifierList(s string) []string {
	parts := splitCommaTopLevel(s)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if ident := lastQualifiedIdentifierPart(p); ident != "" {
			out = append(out, ident)
		}
	}
	return out
}

func splitCommaTopLevel(s string) []string {
	var parts []string
	start := 0
	inSingle := false
	inDouble := false
	inBacktick := false
	inBracket := false
	depth := 0
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inSingle {
			if ch == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '"' {
				if i+1 < len(s) && s[i+1] == '"' {
					i++
					continue
				}
				inDouble = false
			}
			continue
		}
		if inBacktick {
			if ch == '`' {
				if i+1 < len(s) && s[i+1] == '`' {
					i++
					continue
				}
				inBacktick = false
			}
			continue
		}
		if inBracket {
			if ch == ']' {
				if i+1 < len(s) && s[i+1] == ']' {
					i++
					continue
				}
				inBracket = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case '[':
			inBracket = true
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(s[start:]))
	return parts
}

func splitQualifiedIdentifier(s string) []string {
	var parts []string
	start := 0
	inDouble := false
	inBacktick := false
	inBracket := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inDouble {
			if ch == '"' {
				if i+1 < len(s) && s[i+1] == '"' {
					i++
					continue
				}
				inDouble = false
			}
			continue
		}
		if inBacktick {
			if ch == '`' {
				if i+1 < len(s) && s[i+1] == '`' {
					i++
					continue
				}
				inBacktick = false
			}
			continue
		}
		if inBracket {
			if ch == ']' {
				if i+1 < len(s) && s[i+1] == ']' {
					i++
					continue
				}
				inBracket = false
			}
			continue
		}
		switch ch {
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case '[':
			inBracket = true
		case '.':
			parts = append(parts, unquoteIdentifier(strings.TrimSpace(s[start:i])))
			start = i + 1
		}
	}
	parts = append(parts, unquoteIdentifier(strings.TrimSpace(s[start:])))
	return parts
}

func lastQualifiedIdentifierPart(s string) string {
	parts := splitQualifiedIdentifier(s)
	for i := len(parts) - 1; i >= 0; i-- {
		if parts[i] != "" {
			return parts[i]
		}
	}
	return ""
}

func unquoteIdentifier(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 {
		switch {
		case s[0] == '[' && s[len(s)-1] == ']':
			return strings.ReplaceAll(s[1:len(s)-1], "]]", "]")
		case s[0] == '`' && s[len(s)-1] == '`':
			return strings.ReplaceAll(s[1:len(s)-1], "``", "`")
		case s[0] == '"' && s[len(s)-1] == '"':
			return strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
		}
	}
	return s
}

func finalizationNameKeys(target string, names []string) []string {
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = finalizationNameKey(target, name)
	}
	return out
}

func finalizationNameKey(target, name string) string {
	return strings.ToLower(NormalizeIdentifier(canonicalFinalizationDialect(target), unquoteIdentifier(strings.TrimSpace(name))))
}

func canonicalFinalizationDialect(dialect string) string {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "postgres", "postgresql", "pg":
		return "postgres"
	case "mssql", "sqlserver", "sql_server", "sql-server":
		return "mssql"
	case "mysql", "mariadb", "maria":
		return "mysql"
	default:
		return strings.ToLower(strings.TrimSpace(dialect))
	}
}

func finalizationActionKey(action string) string {
	switch strings.ToUpper(strings.Join(strings.Fields(strings.TrimSpace(action)), " ")) {
	case "", "NO ACTION", "RESTRICT":
		return "noaction"
	case "CASCADE":
		return "cascade"
	case "SET NULL":
		return "set null"
	case "SET DEFAULT":
		return "set default"
	default:
		return strings.ToLower(strings.Join(strings.Fields(action), " "))
	}
}

func targetSupportsIncludeColumns(target string) bool {
	switch canonicalFinalizationDialect(target) {
	case "postgres", "mssql":
		return true
	default:
		return false
	}
}

func stringSlicesEqualOrdered(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func boolText(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func isIdentByte(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' || b == '$'
}

func stripBalancedOuterParensText(s string) string {
	for {
		s = strings.TrimSpace(s)
		if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
			return s
		}
		_, closeIdx, ok := readBalancedParen(s, 0)
		if !ok || closeIdx != len(s)-1 {
			return s
		}
		s = s[1 : len(s)-1]
	}
}
