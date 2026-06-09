package postgres

import (
	"fmt"
	"regexp"
	"strings"

	"smt/internal/driver"
	"smt/internal/logging"
)

type deterministicDDL struct {
	dialect           Dialect
	unknownTypePolicy string
}

func newDeterministicDDL(policy ...string) deterministicDDL {
	unknownTypePolicy := "fail"
	if len(policy) > 0 && strings.TrimSpace(policy[0]) != "" {
		unknownTypePolicy = strings.TrimSpace(policy[0])
	}
	return deterministicDDL{dialect: Dialect{}, unknownTypePolicy: unknownTypePolicy}
}

func RenderCreateTableDDL(t *driver.Table, targetSchema string, unlogged bool) (string, map[string]string, error) {
	return RenderCreateTableDDLWithPolicy(t, targetSchema, unlogged, "")
}

func RenderCreateTableDDLWithPolicy(t *driver.Table, targetSchema string, unlogged bool, unknownTypePolicy string) (string, map[string]string, error) {
	return newDeterministicDDL(unknownTypePolicy).createTable(t, targetSchema, unlogged)
}

func RenderColumnDefinition(col driver.Column) (string, error) {
	return RenderColumnDefinitionWithPolicy(col, "")
}

func RenderColumnDefinitionWithPolicy(col driver.Column, unknownTypePolicy string) (string, error) {
	def, _, err := newDeterministicDDL(unknownTypePolicy).columnDefinition(col)
	return def, err
}

func RenderColumnDefinitionWithContextAndPolicy(col driver.Column, tableColumns []driver.Column, unknownTypePolicy string) (string, error) {
	def, _, err := newDeterministicDDL(unknownTypePolicy).columnDefinition(col, tableColumns)
	return def, err
}

func RenderColumnType(col driver.Column) (string, error) {
	return RenderColumnTypeWithPolicy(col, "")
}

func RenderColumnTypeWithPolicy(col driver.Column, unknownTypePolicy string) (string, error) {
	return newDeterministicDDL(unknownTypePolicy).columnType(col)
}

func RenderColumnDefaultDDL(col driver.Column) (string, error) {
	return RenderColumnDefaultDDLWithPolicy(col, "")
}

func RenderColumnDefaultDDLWithPolicy(col driver.Column, unknownTypePolicy string) (string, error) {
	return newDeterministicDDL(unknownTypePolicy).defaultExpression(col)
}

func RenderCreateIndexDDL(t *driver.Table, idx *driver.Index, targetSchema string) (string, error) {
	return newDeterministicDDL().createIndex(t, idx, targetSchema)
}

func RenderCreateForeignKeyDDL(t *driver.Table, fk *driver.ForeignKey, targetSchema string) (string, error) {
	return newDeterministicDDL().createForeignKey(t, fk, targetSchema)
}

func RenderCreateCheckConstraintDDL(t *driver.Table, chk *driver.CheckConstraint, targetSchema string) (string, error) {
	return newDeterministicDDL().createCheckConstraint(t, chk, targetSchema)
}

func (r deterministicDDL) createTable(t *driver.Table, targetSchema string, unlogged bool) (string, map[string]string, error) {
	tableName := sanitizePGTableName(t.Name)
	columnTypes := make(map[string]string, len(t.Columns))
	lines := make([]string, 0, len(t.Columns)+1)

	for _, col := range t.Columns {
		def, colType, err := r.columnDefinition(col, t.Columns)
		if err != nil {
			return "", nil, fmt.Errorf("mapping column %s.%s: %w", t.Name, col.Name, err)
		}
		columnTypes[col.Name] = colType
		columnTypes[sanitizePGIdentifier(col.Name)] = colType
		lines = append(lines, "    "+def)
	}

	if len(t.PrimaryKey) > 0 {
		cols := make([]string, len(t.PrimaryKey))
		for i, c := range t.PrimaryKey {
			cols[i] = r.dialect.QuoteIdentifier(sanitizePGIdentifier(c))
		}
		pkName := "pk_" + tableName
		lines = append(lines, fmt.Sprintf("    CONSTRAINT %s PRIMARY KEY (%s)",
			r.dialect.QuoteIdentifier(pkName), strings.Join(cols, ", ")))
	}

	create := "CREATE TABLE"
	if unlogged {
		create = "CREATE UNLOGGED TABLE"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%s %s (\n", create, r.dialect.QualifyTable(targetSchema, tableName))
	b.WriteString(strings.Join(lines, ",\n"))
	b.WriteString("\n)")
	return b.String(), columnTypes, nil
}

func (r deterministicDDL) columnDefinition(col driver.Column, tableColumns ...[]driver.Column) (string, string, error) {
	colName := sanitizePGIdentifier(col.Name)
	colType, err := r.columnType(col)
	if err != nil {
		return "", "", err
	}
	contextColumns := []driver.Column(nil)
	if len(tableColumns) > 0 {
		contextColumns = tableColumns[0]
	}
	if col.IsComputed {
		expr, err := r.sqlServerExpression(col.ComputedExpression)
		if err != nil {
			return "", "", fmt.Errorf("mapping computed column %s: %w", col.Name, err)
		}
		if strings.TrimSpace(expr) == "" {
			return "", "", fmt.Errorf("computed column %s has no expression", col.Name)
		}
		if isTextualPGType(colType) {
			expr = rewriteSQLServerStringConcat(expr)
		}
		expr = rewriteSQLServerBitComparisons(expr, contextColumns)
		if strings.EqualFold(strings.TrimSpace(col.DataType), "bit") {
			expr = rewriteSQLServerBooleanResultLiterals(expr)
		}
		var b strings.Builder
		b.WriteString(r.dialect.QuoteIdentifier(colName))
		b.WriteString(" ")
		b.WriteString(colType)
		b.WriteString(" GENERATED ALWAYS AS (")
		b.WriteString(expr)
		b.WriteString(") STORED")
		if !col.IsNullable {
			b.WriteString(" NOT NULL")
		}
		return b.String(), colType, nil
	}

	var b strings.Builder
	b.WriteString(r.dialect.QuoteIdentifier(colName))
	b.WriteString(" ")
	b.WriteString(colType)

	if !col.IsIdentity && !col.IsNullable {
		b.WriteString(" NOT NULL")
	}
	if !col.IsIdentity && strings.TrimSpace(col.DefaultExpression) != "" {
		def, err := r.defaultExpression(col)
		if err != nil {
			return "", "", err
		}
		if def != "" {
			b.WriteString(" DEFAULT ")
			b.WriteString(def)
		}
	}
	return b.String(), colType, nil
}

func (r deterministicDDL) createIndex(t *driver.Table, idx *driver.Index, targetSchema string) (string, error) {
	tableName := sanitizePGTableName(t.Name)
	indexName := sanitizePGIdentifier(idx.Name)
	if len(idx.Columns) == 0 {
		return "", fmt.Errorf("index %s has no columns", idx.Name)
	}

	cols := make([]string, len(idx.Columns))
	for i, c := range idx.Columns {
		cols[i] = r.dialect.QuoteIdentifier(sanitizePGIdentifier(c))
	}

	var b strings.Builder
	b.WriteString("CREATE ")
	if idx.IsUnique {
		b.WriteString("UNIQUE ")
	}
	fmt.Fprintf(&b, "INDEX %s ON %s (%s)",
		r.dialect.QuoteIdentifier(indexName),
		r.dialect.QualifyTable(targetSchema, tableName),
		strings.Join(cols, ", "))

	if len(idx.IncludeCols) > 0 {
		includeCols := make([]string, len(idx.IncludeCols))
		for i, c := range idx.IncludeCols {
			includeCols[i] = r.dialect.QuoteIdentifier(sanitizePGIdentifier(c))
		}
		fmt.Fprintf(&b, " INCLUDE (%s)", strings.Join(includeCols, ", "))
	}

	if filter := strings.TrimSpace(idx.Filter); filter != "" {
		expr, err := r.sqlServerExpression(filter)
		if err != nil {
			return "", fmt.Errorf("mapping filter for index %s: %w", idx.Name, err)
		}
		expr = rewriteSQLServerBitComparisons(expr, t.Columns)
		b.WriteString(" WHERE ")
		b.WriteString(expr)
	}

	return b.String(), nil
}

func (r deterministicDDL) createForeignKey(t *driver.Table, fk *driver.ForeignKey, targetSchema string) (string, error) {
	tableName := sanitizePGTableName(t.Name)
	fkName := sanitizePGIdentifier(fk.Name)
	if len(fk.Columns) == 0 {
		return "", fmt.Errorf("foreign key %s has no columns", fk.Name)
	}
	if len(fk.Columns) != len(fk.RefColumns) {
		return "", fmt.Errorf("foreign key %s has %d columns but %d referenced columns", fk.Name, len(fk.Columns), len(fk.RefColumns))
	}

	cols := make([]string, len(fk.Columns))
	for i, c := range fk.Columns {
		cols[i] = r.dialect.QuoteIdentifier(sanitizePGIdentifier(c))
	}
	refCols := make([]string, len(fk.RefColumns))
	for i, c := range fk.RefColumns {
		refCols[i] = r.dialect.QuoteIdentifier(sanitizePGIdentifier(c))
	}

	refTable := sanitizePGTableName(fk.RefTable)
	var b strings.Builder
	fmt.Fprintf(&b, "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
		r.dialect.QualifyTable(targetSchema, tableName),
		r.dialect.QuoteIdentifier(fkName),
		strings.Join(cols, ", "),
		r.dialect.QualifyTable(targetSchema, refTable),
		strings.Join(refCols, ", "))

	if rule := pgReferentialAction(fk.OnDelete); rule != "" {
		b.WriteString(" ON DELETE ")
		b.WriteString(rule)
	}
	if rule := pgReferentialAction(fk.OnUpdate); rule != "" {
		b.WriteString(" ON UPDATE ")
		b.WriteString(rule)
	}
	return b.String(), nil
}

func (r deterministicDDL) createCheckConstraint(t *driver.Table, chk *driver.CheckConstraint, targetSchema string) (string, error) {
	tableName := sanitizePGTableName(t.Name)
	checkName := sanitizePGIdentifier(chk.Name)
	def := strings.TrimSpace(chk.Definition)
	if def == "" {
		return "", fmt.Errorf("check constraint %s has no definition", chk.Name)
	}

	expr, err := r.sqlServerExpression(def)
	if err != nil {
		return "", fmt.Errorf("mapping check constraint %s: %w", chk.Name, err)
	}
	expr = rewriteSQLServerBitComparisons(expr, t.Columns)

	return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK %s",
		r.dialect.QualifyTable(targetSchema, tableName),
		r.dialect.QuoteIdentifier(checkName),
		expr), nil
}

func (r deterministicDDL) columnType(col driver.Column) (string, error) {
	dt := strings.ToLower(strings.TrimSpace(col.DataType))
	var typ string
	switch dt {
	case "int", "integer", "int4", "serial":
		if col.IsUnsigned {
			typ = "bigint"
			break
		}
		typ = "integer"
	case "bigint", "int8", "bigserial":
		if col.IsUnsigned && !col.IsIdentity {
			typ = "numeric(20,0)"
			break
		}
		typ = "bigint"
	case "smallint", "int2", "smallserial":
		if col.IsUnsigned {
			typ = "integer"
			break
		}
		typ = "smallint"
	case "tinyint":
		typ = "smallint"
	case "bit", "bool", "boolean":
		typ = "boolean"
	case "varchar", "nvarchar", "character varying":
		if col.MaxLength <= 0 || col.MaxLength == -1 {
			typ = "text"
		} else {
			typ = fmt.Sprintf("character varying(%d)", col.MaxLength)
		}
	case "char", "nchar", "character", "bpchar":
		if col.MaxLength <= 0 || col.MaxLength == -1 {
			typ = "text"
		} else {
			typ = fmt.Sprintf("character(%d)", col.MaxLength)
		}
	case "text", "ntext", "tinytext", "mediumtext", "longtext":
		typ = "text"
	case "datetime", "datetime2", "smalldatetime", "timestamp":
		typ = "timestamp without time zone"
	case "rowversion":
		// SQL Server's rowversion (reported by old snapshots as "timestamp")
		// is an opaque 8-byte binary counter.
		typ = "bytea"
	case "datetimeoffset", "timestamptz", "timestamp with time zone":
		typ = "timestamp with time zone"
	case "date":
		typ = "date"
	case "time":
		typ = "time"
	case "decimal", "numeric", "number":
		if col.Precision > 0 {
			typ = fmt.Sprintf("numeric(%d,%d)", col.Precision, col.Scale)
		} else {
			typ = "numeric"
		}
	case "money":
		typ = "numeric(19,4)"
	case "smallmoney":
		typ = "numeric(10,4)"
	case "float", "double", "double precision":
		typ = "double precision"
	case "real":
		typ = "real"
	case "uniqueidentifier", "uuid":
		typ = "uuid"
	case "varbinary", "binary", "image", "bytea":
		typ = "bytea"
	case "xml":
		typ = "xml"
	case "json", "jsonb":
		typ = "jsonb"
	case "enum", "set":
		typ = "text"
	case "_text", "text[]":
		typ = "text[]"
	case "_varchar", "varchar[]", "_bpchar", "bpchar[]":
		typ = "text[]"
	case "_int2", "int2[]", "_int4", "int4[]", "_int8", "int8[]":
		typ = "integer[]"
	case "_uuid", "uuid[]":
		typ = "uuid[]"
	default:
		switch r.unknownTypePolicy {
		case "warn":
			logging.Warn("deterministic PostgreSQL type mapper: unsupported source type %q for column %s; using text because unknown_type_policy=warn", col.DataType, col.Name)
			return "text", nil
		case "text_fallback":
			return "text", nil
		}
		return "", fmt.Errorf("unsupported source type %q", col.DataType)
	}

	if col.IsIdentity {
		switch typ {
		case "smallint", "integer", "bigint":
			typ += " GENERATED BY DEFAULT AS IDENTITY"
		default:
			return "", fmt.Errorf("identity is only supported for integer-compatible types, got %s", typ)
		}
	}
	return typ, nil
}

func (r deterministicDDL) defaultExpression(col driver.Column) (string, error) {
	raw := strings.TrimSpace(col.DefaultExpression)
	if raw == "" {
		return "", nil
	}
	expr := unwrapDefaultParens(raw)
	lower := strings.ToLower(expr)

	switch lower {
	case "getdate()", "current_timestamp":
		return "CURRENT_TIMESTAMP", nil
	case "getutcdate()":
		return "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')", nil
	case "sysdatetime()":
		return "CURRENT_TIMESTAMP", nil
	case "sysdatetimeoffset()":
		return "CURRENT_TIMESTAMP", nil
	case "sysutcdatetime()":
		if strings.EqualFold(strings.TrimSpace(col.DataType), "datetimeoffset") {
			return "CURRENT_TIMESTAMP", nil
		}
		return "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')", nil
	case "newid()", "uuid()":
		return "gen_random_uuid()", nil
	}
	if strings.HasPrefix(lower, "current_timestamp(") || strings.HasPrefix(lower, "now(") {
		return "CURRENT_TIMESTAMP", nil
	}

	if strings.EqualFold(col.DataType, "bit") {
		switch expr {
		case "0":
			return "false", nil
		case "1":
			return "true", nil
		}
	}
	if isTextualSourceType(col.DataType) && isBareSQLWord(expr) {
		return "'" + strings.ReplaceAll(expr, "'", "''") + "'", nil
	}

	return r.sqlServerExpression(expr)
}

func (r deterministicDDL) sqlServerExpression(expr string) (string, error) {
	out := strings.TrimSpace(expr)
	out = replaceBracketIdentifiers(out, func(name string) string {
		return r.dialect.QuoteIdentifier(sanitizePGIdentifier(name))
	})
	out = replaceBacktickIdentifiers(out, func(name string) string {
		return r.dialect.QuoteIdentifier(sanitizePGIdentifier(name))
	})
	out = stripSQLServerUnicodeStringPrefixes(out)
	out = stripMySQLCharsetStringPrefixes(out)
	out = rewriteMySQLConcat(out)
	out = rewriteMySQLRegexpLike(out)
	out = rewriteSQLServerLikePatterns(out)
	out = strings.ReplaceAll(out, "GETDATE()", "CURRENT_TIMESTAMP")
	out = strings.ReplaceAll(out, "getdate()", "CURRENT_TIMESTAMP")
	out = strings.ReplaceAll(out, "GETUTCDATE()", "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')")
	out = strings.ReplaceAll(out, "getutcdate()", "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')")
	out = strings.ReplaceAll(out, "SYSDATETIME()", "CURRENT_TIMESTAMP")
	out = strings.ReplaceAll(out, "sysdatetime()", "CURRENT_TIMESTAMP")
	out = strings.ReplaceAll(out, "SYSDATETIMEOFFSET()", "CURRENT_TIMESTAMP")
	out = strings.ReplaceAll(out, "sysdatetimeoffset()", "CURRENT_TIMESTAMP")
	out = strings.ReplaceAll(out, "SYSUTCDATETIME()", "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')")
	out = strings.ReplaceAll(out, "sysutcdatetime()", "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')")
	out = strings.ReplaceAll(out, "NEWID()", "gen_random_uuid()")
	out = strings.ReplaceAll(out, "newid()", "gen_random_uuid()")
	out = strings.ReplaceAll(out, "ISNULL(", "COALESCE(")
	out = strings.ReplaceAll(out, "isnull(", "COALESCE(")
	if err := rejectUnsupportedSQLServerExpression(out); err != nil {
		return "", err
	}
	return out, nil
}

var expressionFunctionRE = regexp.MustCompile(`(?i)\b([a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)*)\s*\(`)

func rejectUnsupportedSQLServerExpression(expr string) error {
	scan := stripSingleQuotedStrings(expr)
	for _, match := range expressionFunctionRE.FindAllStringSubmatch(scan, -1) {
		if len(match) != 2 {
			continue
		}
		name := strings.ToLower(match[1])
		switch name {
		case "and", "or", "case", "when", "then", "else", "end", "coalesce", "gen_random_uuid", "nullif", "lower", "upper", "in", "not", "exists", "any":
			continue
		default:
			return fmt.Errorf("unsupported SQL expression function %q in %q", match[1], expr)
		}
	}
	return nil
}

func stripSingleQuotedStrings(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inSingleQuote := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' {
			b.WriteByte(' ')
			if inSingleQuote && i+1 < len(s) && s[i+1] == '\'' {
				i++
				b.WriteByte(' ')
				continue
			}
			inSingleQuote = !inSingleQuote
			continue
		}
		if inSingleQuote {
			b.WriteByte(' ')
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func pgReferentialAction(rule string) string {
	switch strings.ToUpper(strings.TrimSpace(rule)) {
	case "", "NO ACTION":
		return ""
	case "CASCADE", "SET NULL", "SET DEFAULT", "RESTRICT":
		return strings.ToUpper(strings.TrimSpace(rule))
	default:
		return ""
	}
}

func unwrapDefaultParens(s string) string {
	s = strings.TrimSpace(s)
	for {
		if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
			return s
		}
		inner := strings.TrimSpace(s[1 : len(s)-1])
		if !balancedParens(inner) {
			return s
		}
		s = inner
	}
}

func balancedParens(s string) bool {
	depth := 0
	inSingleQuote := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' {
			if inSingleQuote && i+1 < len(s) && s[i+1] == '\'' {
				i++
				continue
			}
			inSingleQuote = !inSingleQuote
			continue
		}
		if inSingleQuote {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return false
			}
		}
	}
	return depth == 0 && !inSingleQuote
}

func replaceBracketIdentifiers(s string, quote func(string) string) string {
	var b strings.Builder
	b.Grow(len(s))
	inSingleQuote := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' {
			b.WriteByte(ch)
			if inSingleQuote && i+1 < len(s) && s[i+1] == '\'' {
				i++
				b.WriteByte(s[i])
				continue
			}
			inSingleQuote = !inSingleQuote
			continue
		}
		if !inSingleQuote && ch == '[' && startsBracketIdentifier(s, i) {
			if end := strings.IndexByte(s[i+1:], ']'); end >= 0 {
				name := s[i+1 : i+1+end]
				if name != "" {
					b.WriteString(quote(name))
					i += end + 1
					continue
				}
			}
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func startsBracketIdentifier(s string, idx int) bool {
	if idx == 0 {
		return true
	}
	prev := s[idx-1]
	return !((prev >= 'a' && prev <= 'z') || (prev >= 'A' && prev <= 'Z') || (prev >= '0' && prev <= '9') || prev == '_')
}

func replaceBacktickIdentifiers(s string, quote func(string) string) string {
	var b strings.Builder
	b.Grow(len(s))
	inSingleQuote := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' {
			b.WriteByte(ch)
			if inSingleQuote && i+1 < len(s) && s[i+1] == '\'' {
				i++
				b.WriteByte(s[i])
				continue
			}
			inSingleQuote = !inSingleQuote
			continue
		}
		if !inSingleQuote && ch == '`' {
			if end := strings.IndexByte(s[i+1:], '`'); end >= 0 {
				name := strings.ReplaceAll(s[i+1:i+1+end], "``", "`")
				b.WriteString(quote(name))
				i += end + 1
				continue
			}
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func stripSQLServerUnicodeStringPrefixes(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inSingleQuote := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if !inSingleQuote && (ch == 'N' || ch == 'n') && i+1 < len(s) && s[i+1] == '\'' && startsSQLStringPrefix(s, i) {
			b.WriteByte('\'')
			i++
			inSingleQuote = true
			continue
		}
		if ch == '\'' {
			b.WriteByte(ch)
			if inSingleQuote && i+1 < len(s) && s[i+1] == '\'' {
				i++
				b.WriteByte(s[i])
				continue
			}
			inSingleQuote = !inSingleQuote
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

var (
	mysqlEscapedCharsetStringRE = regexp.MustCompile(`(?i)_[a-z0-9]+\\'([^\\]*)\\'`)
	mysqlCharsetStringRE        = regexp.MustCompile(`(?i)_[a-z0-9]+'([^']*)'`)
)

func stripMySQLCharsetStringPrefixes(s string) string {
	out := mysqlEscapedCharsetStringRE.ReplaceAllString(s, `'$1'`)
	out = mysqlCharsetStringRE.ReplaceAllString(out, `'$1'`)
	return out
}

func startsSQLStringPrefix(s string, idx int) bool {
	if idx == 0 {
		return true
	}
	prev := s[idx-1]
	return !((prev >= 'a' && prev <= 'z') ||
		(prev >= 'A' && prev <= 'Z') ||
		(prev >= '0' && prev <= '9') ||
		prev == '_' ||
		prev == '"')
}

func rewriteMySQLConcat(expr string) string {
	trimmed := strings.TrimSpace(expr)
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "concat(") || !strings.HasSuffix(trimmed, ")") {
		return expr
	}
	inner := strings.TrimSpace(trimmed[len("concat(") : len(trimmed)-1])
	if !balancedParens(inner) {
		return expr
	}
	parts := splitTopLevelSQL(inner, ',')
	if len(parts) < 2 {
		return expr
	}
	return strings.Join(parts, " || ")
}

func rewriteMySQLRegexpLike(expr string) string {
	re := regexp.MustCompile(`(?i)regexp_like\s*\(\s*([^,]+?)\s*,\s*('(?:''|[^'])*')\s*\)`)
	return re.ReplaceAllString(expr, "($1 ~ $2)")
}

func splitTopLevelSQL(expr string, sep rune) []string {
	var parts []string
	var b strings.Builder
	depth := 0
	inSingleQuote := false
	inDoubleQuote := false
	for i, r := range expr {
		if r == '\'' && !inDoubleQuote {
			inSingleQuote = !inSingleQuote
			b.WriteRune(r)
			continue
		}
		if r == '"' && !inSingleQuote {
			inDoubleQuote = !inDoubleQuote
			b.WriteRune(r)
			continue
		}
		if !inSingleQuote && !inDoubleQuote {
			switch r {
			case '(':
				depth++
			case ')':
				if depth > 0 {
					depth--
				}
			default:
				if r == sep && depth == 0 {
					parts = append(parts, strings.TrimSpace(b.String()))
					b.Reset()
					continue
				}
			}
		}
		b.WriteRune(r)
		if r == '\'' && inSingleQuote && i+1 < len(expr) && expr[i+1] == '\'' {
			continue
		}
	}
	parts = append(parts, strings.TrimSpace(b.String()))
	return parts
}

func isTextualPGType(colType string) bool {
	colType = strings.ToLower(strings.TrimSpace(colType))
	return strings.HasPrefix(colType, "character varying") ||
		strings.HasPrefix(colType, "character(") ||
		colType == "text"
}

func isTextualSourceType(dataType string) bool {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "varchar", "nvarchar", "character varying", "char", "nchar", "character", "bpchar", "text", "ntext", "enum", "set":
		return true
	default:
		return false
	}
}

func isBareSQLWord(expr string) bool {
	if expr == "" {
		return false
	}
	for i, r := range expr {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' {
			continue
		}
		if i > 0 && r >= '0' && r <= '9' {
			continue
		}
		return false
	}
	// SQL keywords that must stay unquoted: DEFAULT (NULL) means SQL NULL,
	// not the string 'NULL'.
	switch strings.ToLower(expr) {
	case "null", "true", "false", "current_timestamp":
		return false
	default:
		return true
	}
}

func rewriteSQLServerStringConcat(expr string) string {
	var b strings.Builder
	b.Grow(len(expr))
	inSingleQuote := false
	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if ch == '\'' {
			b.WriteByte(ch)
			if inSingleQuote && i+1 < len(expr) && expr[i+1] == '\'' {
				i++
				b.WriteByte(expr[i])
				continue
			}
			inSingleQuote = !inSingleQuote
			continue
		}
		if !inSingleQuote && ch == '+' {
			b.WriteString(" || ")
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func rewriteSQLServerBooleanResultLiterals(expr string) string {
	resultLiteralRE := regexp.MustCompile(`(?i)\b(THEN|ELSE)\s+\(?([01])\)?`)
	return resultLiteralRE.ReplaceAllStringFunc(expr, func(match string) string {
		parts := resultLiteralRE.FindStringSubmatch(match)
		if len(parts) != 3 {
			return match
		}
		lit := "false"
		if parts[2] == "1" {
			lit = "true"
		}
		return parts[1] + " " + lit
	})
}

func rewriteSQLServerBitComparisons(expr string, cols []driver.Column) string {
	out := expr
	for _, col := range cols {
		if !strings.EqualFold(strings.TrimSpace(col.DataType), "bit") {
			continue
		}
		quoted := `"` + sanitizePGIdentifier(col.Name) + `"`
		ident := regexp.QuoteMeta(quoted)
		leftRE := regexp.MustCompile(`(?i)(` + ident + `)\s*(=|<>|!=)\s*(?:\(([01])\)|([01])\b)`)
		out = leftRE.ReplaceAllStringFunc(out, func(match string) string {
			parts := leftRE.FindStringSubmatch(match)
			if len(parts) != 5 {
				return match
			}
			return parts[1] + " " + parts[2] + " " + bitLiteral(firstNonEmpty(parts[3], parts[4]))
		})
		rightRE := regexp.MustCompile(`(?i)(?:\(([01])\)|([01])\b)\s*(=|<>|!=)\s*(` + ident + `)`)
		out = rightRE.ReplaceAllStringFunc(out, func(match string) string {
			parts := rightRE.FindStringSubmatch(match)
			if len(parts) != 5 {
				return match
			}
			return bitLiteral(firstNonEmpty(parts[1], parts[2])) + " " + parts[3] + " " + parts[4]
		})
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func bitLiteral(v string) string {
	if v == "1" {
		return "true"
	}
	return "false"
}

var sqlServerLikeLiteralRE = regexp.MustCompile(`(?i)(("[^"]+"|[a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)?)\s+)(NOT\s+)?LIKE\s+('(?:''|[^'])*')`)

func rewriteSQLServerLikePatterns(s string) string {
	return sqlServerLikeLiteralRE.ReplaceAllStringFunc(s, func(match string) string {
		parts := sqlServerLikeLiteralRE.FindStringSubmatch(match)
		if len(parts) != 5 {
			return match
		}
		pattern := unquoteSQLString(parts[4])
		if !strings.Contains(pattern, "[") || !strings.Contains(pattern, "]") {
			return match
		}
		re, ok := sqlServerLikePatternToRegex(pattern)
		if !ok {
			return match
		}
		op := "~"
		if strings.TrimSpace(parts[3]) != "" {
			op = "!~"
		}
		return strings.TrimSpace(parts[1]) + " " + op + " " + quoteSQLString(re)
	})
}

func sqlServerLikePatternToRegex(pattern string) (string, bool) {
	var b strings.Builder
	b.Grow(len(pattern) + 2)
	b.WriteByte('^')
	usedBracketClass := false
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		switch ch {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		case '[':
			end := strings.IndexByte(pattern[i+1:], ']')
			if end < 0 {
				b.WriteString(`\[`)
				continue
			}
			class := pattern[i+1 : i+1+end]
			if class == "" {
				b.WriteString(`\[\]`)
			} else {
				usedBracketClass = true
				if class[0] == '^' {
					class = "^" + regexp.QuoteMeta(class[1:])
				} else {
					class = regexp.QuoteMeta(class)
				}
				b.WriteByte('[')
				b.WriteString(class)
				b.WriteByte(']')
			}
			i += end + 1
		default:
			b.WriteString(regexp.QuoteMeta(string(ch)))
		}
	}
	b.WriteByte('$')
	return b.String(), usedBracketClass
}

func unquoteSQLString(lit string) string {
	lit = strings.TrimSpace(lit)
	if len(lit) >= 2 && lit[0] == '\'' && lit[len(lit)-1] == '\'' {
		lit = lit[1 : len(lit)-1]
	}
	return strings.ReplaceAll(lit, "''", "'")
}

func quoteSQLString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
