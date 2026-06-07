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
		def, colType, err := r.columnDefinition(col)
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

func (r deterministicDDL) columnDefinition(col driver.Column) (string, string, error) {
	colName := sanitizePGIdentifier(col.Name)
	colType, err := r.columnType(col)
	if err != nil {
		return "", "", err
	}
	if col.IsComputed {
		return "", "", fmt.Errorf("computed column %s is not yet supported by deterministic PostgreSQL DDL", col.Name)
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

	return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK %s",
		r.dialect.QualifyTable(targetSchema, tableName),
		r.dialect.QuoteIdentifier(checkName),
		expr), nil
}

func (r deterministicDDL) columnType(col driver.Column) (string, error) {
	dt := strings.ToLower(strings.TrimSpace(col.DataType))
	var typ string
	switch dt {
	case "int", "integer":
		typ = "integer"
	case "bigint":
		typ = "bigint"
	case "smallint":
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
	case "char", "nchar", "character":
		if col.MaxLength <= 0 || col.MaxLength == -1 {
			typ = "text"
		} else {
			typ = fmt.Sprintf("character(%d)", col.MaxLength)
		}
	case "text", "ntext":
		typ = "text"
	case "datetime", "datetime2", "smalldatetime", "timestamp":
		typ = "timestamp without time zone"
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
	case "newid()":
		return "gen_random_uuid()", nil
	}

	if strings.EqualFold(col.DataType, "bit") {
		switch expr {
		case "0":
			return "false", nil
		case "1":
			return "true", nil
		}
	}

	return r.sqlServerExpression(expr)
}

func (r deterministicDDL) sqlServerExpression(expr string) (string, error) {
	out := strings.TrimSpace(expr)
	out = replaceBracketIdentifiers(out, func(name string) string {
		return r.dialect.QuoteIdentifier(sanitizePGIdentifier(name))
	})
	out = strings.ReplaceAll(out, "GETDATE()", "CURRENT_TIMESTAMP")
	out = strings.ReplaceAll(out, "getdate()", "CURRENT_TIMESTAMP")
	out = strings.ReplaceAll(out, "GETUTCDATE()", "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')")
	out = strings.ReplaceAll(out, "getutcdate()", "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')")
	out = strings.ReplaceAll(out, "NEWID()", "gen_random_uuid()")
	out = strings.ReplaceAll(out, "newid()", "gen_random_uuid()")
	out = strings.ReplaceAll(out, "ISNULL(", "COALESCE(")
	out = strings.ReplaceAll(out, "isnull(", "COALESCE(")
	out = strings.ReplaceAll(out, "N'", "'")
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
		case "coalesce", "gen_random_uuid", "nullif", "lower", "upper", "in", "not", "exists":
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

var bracketIdentifierRE = regexp.MustCompile(`\[([^\]]+)\]`)

func replaceBracketIdentifiers(s string, quote func(string) string) string {
	return bracketIdentifierRE.ReplaceAllStringFunc(s, func(match string) string {
		parts := bracketIdentifierRE.FindStringSubmatch(match)
		if len(parts) != 2 {
			return match
		}
		return quote(parts[1])
	})
}
