// Package ddl renders deterministic schema DDL from SMT's schema metadata.
package ddl

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"smt/internal/canonical"
	"smt/internal/driver"
	pgddl "smt/internal/driver/postgres"
)

// RendererVersion identifies the deterministic renderer's output contract.
// Bump it whenever a change can alter the rendered DDL for the same input
// (a new type mapping, default rewrite, identifier rule, etc.) so persisted
// run manifests distinguish artifacts produced by different renderer logic.
// It is the single "renderer + type-mapper version" token #64 fingerprints
// on — type mapping and DDL rendering are one deterministic unit here.
const RendererVersion = "1"

type Renderer struct {
	target            string
	schema            string
	unknownTypePolicy string
	source            string // canonical source dialect; empty = unknown (cross-dialect mappings only)
}

func NewRenderer(target, schema, unknownTypePolicy string) (Renderer, error) {
	target = canonicalTarget(target)
	if target == "" {
		target = "postgres"
	}
	switch target {
	case "postgres", "mssql", "mysql":
	default:
		return Renderer{}, fmt.Errorf("unsupported deterministic DDL target %q", target)
	}
	if unknownTypePolicy == "" {
		unknownTypePolicy = "fail"
	}
	return Renderer{target: target, schema: schema, unknownTypePolicy: unknownTypePolicy}, nil
}

func (r Renderer) Target() string { return r.target }

// WithSource returns a copy of the renderer that knows the source dialect.
// Same-dialect runs use it to pass types through verbatim where the generic
// cross-dialect mapping would lose semantics (e.g. MySQL TIMESTAMP's UTC
// normalization, tinyint(1)'s boolean convention). An unrecognized or empty
// source leaves cross-dialect behavior unchanged.
func (r Renderer) WithSource(source string) Renderer {
	r.source = canonicalTarget(source)
	return r
}

func (r Renderer) CreateSchemaDDL() (string, error) {
	schema := strings.TrimSpace(r.schema)
	if schema == "" {
		return "", nil
	}
	switch r.target {
	case "postgres":
		return fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", r.quote(schema)), nil
	case "mssql":
		escapedName := strings.ReplaceAll(schema, "'", "''")
		escapedDDL := strings.ReplaceAll(fmt.Sprintf("CREATE SCHEMA %s", r.quote(schema)), "'", "''")
		return fmt.Sprintf("IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'%s') EXEC(N'%s')", escapedName, escapedDDL), nil
	case "mysql":
		return fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", r.quote(schema)), nil
	default:
		return "", fmt.Errorf("unsupported deterministic DDL target %q", r.target)
	}
}

func (r Renderer) CreateTableDDL(t *driver.Table) (string, map[string]string, error) {
	if r.target == "postgres" {
		return pgddl.RenderCreateTableDDLWithPolicy(t, r.schema, false, r.unknownTypePolicy)
	}

	tableName := r.normalize(t.Name)
	columnTypes := make(map[string]string, len(t.Columns))
	lines := make([]string, 0, len(t.Columns)+1)
	for _, col := range t.Columns {
		def, typ, err := r.ColumnDefinition(col, t.Columns)
		if err != nil {
			return "", nil, fmt.Errorf("mapping column %s.%s: %w", t.Name, col.Name, err)
		}
		columnTypes[col.Name] = typ
		columnTypes[r.normalize(col.Name)] = typ
		lines = append(lines, "    "+def)
	}

	if len(t.PrimaryKey) > 0 {
		cols := make([]string, len(t.PrimaryKey))
		for i, c := range t.PrimaryKey {
			cols[i] = r.quote(r.normalize(c))
		}
		lines = append(lines, fmt.Sprintf("    CONSTRAINT %s PRIMARY KEY (%s)",
			r.quote(r.normalize("pk_"+t.Name)), strings.Join(cols, ", ")))
	}

	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s (\n", r.qualify(tableName))
	b.WriteString(strings.Join(lines, ",\n"))
	b.WriteString("\n)")
	if r.target == "mysql" {
		b.WriteString(" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	}
	return b.String(), columnTypes, nil
}

func (r Renderer) ColumnDefinition(col driver.Column, tableColumns ...[]driver.Column) (string, string, error) {
	if r.target == "postgres" {
		def, err := pgddl.RenderColumnDefinitionWithContextAndPolicy(col, firstColumns(tableColumns), r.unknownTypePolicy)
		if err != nil {
			return "", "", err
		}
		typ, err := pgddl.RenderColumnTypeWithPolicy(col, r.unknownTypePolicy)
		return def, typ, err
	}

	colName := r.normalize(col.Name)
	colType, err := r.ColumnType(col)
	if err != nil {
		return "", "", err
	}

	if col.IsComputed {
		expr, err := r.Expression(col.ComputedExpression, firstColumns(tableColumns))
		if err != nil {
			return "", "", fmt.Errorf("mapping computed column %s: %w", col.Name, err)
		}
		if strings.TrimSpace(expr) == "" {
			return "", "", fmt.Errorf("computed column %s has no expression", col.Name)
		}
		switch r.target {
		case "mssql":
			suffix := ""
			if col.ComputedPersisted {
				suffix = " PERSISTED"
			}
			return fmt.Sprintf("%s AS (%s)%s", r.quote(colName), expr, suffix), colType, nil
		case "mysql":
			storage := "VIRTUAL"
			if col.ComputedPersisted {
				storage = "STORED"
			}
			return fmt.Sprintf("%s %s GENERATED ALWAYS AS (%s) %s", r.quote(colName), colType, expr, storage), colType, nil
		}
	}

	var b strings.Builder
	b.WriteString(r.quote(colName))
	b.WriteString(" ")
	b.WriteString(colType)
	if col.IsIdentity {
		switch r.target {
		case "mssql":
			b.WriteString(" IDENTITY(1,1)")
		case "mysql":
			b.WriteString(" AUTO_INCREMENT")
		}
	}
	if !col.IsNullable {
		b.WriteString(" NOT NULL")
	}
	if strings.TrimSpace(col.DefaultExpression) != "" && !col.IsIdentity {
		def, err := r.ColumnDefault(col)
		if err != nil {
			return "", "", err
		}
		if def != "" {
			b.WriteString(" DEFAULT ")
			b.WriteString(def)
		}
	}
	if r.target == "mysql" && strings.TrimSpace(col.OnUpdateExpression) != "" && !col.IsIdentity {
		updateCol := col
		updateCol.DefaultExpression = col.OnUpdateExpression
		def, err := r.ColumnDefault(updateCol)
		if err != nil {
			return "", "", err
		}
		if def != "" {
			b.WriteString(" ON UPDATE ")
			b.WriteString(def)
		}
	}
	return b.String(), colType, nil
}

func (r Renderer) ColumnType(col driver.Column) (string, error) {
	if r.target == "postgres" {
		return pgddl.RenderColumnTypeWithPolicy(col, r.unknownTypePolicy)
	}

	dt := normalizeTypeName(col.DataType)
	switch r.target {
	case "mssql":
		return r.mssqlColumnType(col, dt)
	case "mysql":
		return r.mysqlColumnType(col, dt)
	default:
		return "", fmt.Errorf("unsupported deterministic DDL target %q", r.target)
	}
}

func (r Renderer) ColumnDefault(col driver.Column) (string, error) {
	if r.target == "postgres" {
		return pgddl.RenderColumnDefaultDDLWithPolicy(col, r.unknownTypePolicy)
	}
	expr := unwrapDefaultParens(col.DefaultExpression)
	if expr == "" {
		return "", nil
	}
	expr = stripPostgresCasts(expr)
	if isBooleanColumn(col) {
		switch strings.ToLower(expr) {
		case "true":
			return boolLiteral(r.target, true), nil
		case "false":
			return boolLiteral(r.target, false), nil
		case "1":
			return boolLiteral(r.target, true), nil
		case "0":
			return boolLiteral(r.target, false), nil
		}
	}
	if isTextualSourceType(col.DataType) && isBareSQLWord(expr) {
		lit := "'" + escapeSQLString(expr) + "'"
		if r.target == "mysql" {
			lit = r.mysqlDefaultForm(lit, col)
		}
		return lit, nil
	}
	lower := strings.ToLower(expr)
	switch r.target {
	case "mssql":
		// MSSQL-native functions pass through unchanged; foreign now-style
		// functions translate to the equivalent with the same local-vs-UTC
		// class (now()/current_timestamp are local time, never UTC).
		if strings.HasPrefix(lower, "current_timestamp(") || strings.HasPrefix(lower, "now(") {
			return "SYSDATETIME()", nil
		}
		if strings.HasPrefix(lower, "utc_timestamp") {
			return "SYSUTCDATETIME()", nil
		}
		switch lower {
		case "current_timestamp", "now()":
			return "SYSDATETIME()", nil
		case "getdate()":
			return "GETDATE()", nil
		case "getutcdate()":
			return "GETUTCDATE()", nil
		case "sysdatetime()":
			return "SYSDATETIME()", nil
		case "sysutcdatetime()":
			return "SYSUTCDATETIME()", nil
		case "sysdatetimeoffset()":
			return "SYSDATETIMEOFFSET()", nil
		case "gen_random_uuid()", "uuid_generate_v4()", "uuid()", "newid()":
			return "NEWID()", nil
		}
	case "mysql":
		if strings.HasPrefix(lower, "current_timestamp(") || strings.HasPrefix(lower, "now(") {
			return mysqlNowDefault(col), nil
		}
		if isArraySourceType(col.DataType) {
			switch lower {
			case "'{}'", "{}":
				return "(JSON_ARRAY())", nil
			}
		}
		if isJSONSourceType(col.DataType) {
			switch lower {
			case "'{}'", "{}":
				return "(JSON_OBJECT())", nil
			case "'[]'", "[]":
				return "(JSON_ARRAY())", nil
			}
		}
		switch lower {
		case "current_timestamp", "now()", "getdate()", "getutcdate()", "sysdatetime()", "sysutcdatetime()", "sysdatetimeoffset()":
			return mysqlNowDefault(col), nil
		case "gen_random_uuid()", "uuid_generate_v4()", "newid()", "uuid()":
			return "(UUID())", nil
		}
	}
	out, err := r.Expression(expr, nil)
	if err != nil {
		return "", err
	}
	if r.target == "mysql" {
		out = r.mysqlDefaultForm(out, col)
	}
	return out, nil
}

var (
	plainNumberLiteralRE   = regexp.MustCompile(`^-?[0-9]+(?:\.[0-9]+)?$`)
	bareCurrentTimestampRE = regexp.MustCompile(`^CURRENT_TIMESTAMP(?:\([0-9]*\))?$`)
)

// mysqlDefaultForm wraps a rendered default in parentheses where MySQL 8
// requires the expression-default form: any non-literal expression, and every
// default on a BLOB/TEXT/JSON column (which accept only expression defaults).
func (r Renderer) mysqlDefaultForm(def string, col driver.Column) string {
	d := strings.TrimSpace(def)
	if d == "" || strings.HasPrefix(d, "(") {
		return d
	}
	upper := strings.ToUpper(d)
	// NULL and bare CURRENT_TIMESTAMP[(n)] are valid unparenthesized (and ON
	// UPDATE accepts only the bare form); larger expressions that merely start
	// with CURRENT_TIMESTAMP still need wrapping.
	if upper == "NULL" || bareCurrentTimestampRE.MatchString(upper) {
		return d
	}
	if isPlainLiteral(d) {
		if r.mysqlRequiresExpressionDefault(col) {
			return "(" + d + ")"
		}
		return d
	}
	return "(" + d + ")"
}

func isPlainLiteral(d string) bool {
	if len(d) >= 2 && d[0] == '\'' && d[len(d)-1] == '\'' {
		_, ok := unquoteSQLString(d)
		return ok
	}
	return plainNumberLiteralRE.MatchString(d)
}

func (r Renderer) mysqlRequiresExpressionDefault(col driver.Column) bool {
	typ, err := r.ColumnType(col)
	if err != nil {
		return false
	}
	base := strings.ToUpper(normalizeTypeName(typ))
	return base == "JSON" || base == "GEOMETRY" ||
		strings.HasSuffix(base, "TEXT") || strings.HasSuffix(base, "BLOB")
}

func (r Renderer) CreateIndexDDL(t *driver.Table, idx *driver.Index) (string, error) {
	if r.target == "postgres" {
		return pgddl.RenderCreateIndexDDL(t, idx, r.schema)
	}
	if len(idx.Columns) == 0 {
		return "", fmt.Errorf("index %s has no columns", idx.Name)
	}
	cols := make([]string, len(idx.Columns))
	for i, c := range idx.Columns {
		cols[i] = r.quote(r.normalize(c))
	}
	var b strings.Builder
	b.WriteString("CREATE ")
	if idx.IsUnique {
		b.WriteString("UNIQUE ")
	}
	fmt.Fprintf(&b, "INDEX %s ON %s (%s)", r.quote(r.normalize(idx.Name)), r.qualify(r.normalize(t.Name)), strings.Join(cols, ", "))
	if len(idx.IncludeCols) > 0 && r.target == "mssql" {
		includeCols := make([]string, len(idx.IncludeCols))
		for i, c := range idx.IncludeCols {
			includeCols[i] = r.quote(r.normalize(c))
		}
		fmt.Fprintf(&b, " INCLUDE (%s)", strings.Join(includeCols, ", "))
	}
	if filter := strings.TrimSpace(idx.Filter); filter != "" {
		expr, err := r.Expression(filter, t.Columns)
		if err != nil {
			return "", fmt.Errorf("mapping filter for index %s: %w", idx.Name, err)
		}
		b.WriteString(" WHERE ")
		b.WriteString(expr)
	}
	return b.String(), nil
}

func (r Renderer) CreateForeignKeyDDL(t *driver.Table, fk *driver.ForeignKey) (string, error) {
	if r.target == "postgres" {
		return pgddl.RenderCreateForeignKeyDDL(t, fk, r.schema)
	}
	if len(fk.Columns) == 0 {
		return "", fmt.Errorf("foreign key %s has no columns", fk.Name)
	}
	if len(fk.Columns) != len(fk.RefColumns) {
		return "", fmt.Errorf("foreign key %s has %d columns but %d referenced columns", fk.Name, len(fk.Columns), len(fk.RefColumns))
	}
	cols := make([]string, len(fk.Columns))
	for i, c := range fk.Columns {
		cols[i] = r.quote(r.normalize(c))
	}
	refCols := make([]string, len(fk.RefColumns))
	for i, c := range fk.RefColumns {
		refCols[i] = r.quote(r.normalize(c))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
		r.qualify(r.normalize(t.Name)),
		r.quote(r.normalize(fk.Name)),
		strings.Join(cols, ", "),
		r.qualify(r.normalize(fk.RefTable)),
		strings.Join(refCols, ", "))
	if action := referentialAction(fk.OnDelete); action != "" {
		b.WriteString(" ON DELETE ")
		b.WriteString(action)
	}
	if action := referentialAction(fk.OnUpdate); action != "" {
		b.WriteString(" ON UPDATE ")
		b.WriteString(action)
	}
	return b.String(), nil
}

func (r Renderer) CreateCheckConstraintDDL(t *driver.Table, chk *driver.CheckConstraint) (string, error) {
	if r.target == "postgres" {
		return pgddl.RenderCreateCheckConstraintDDL(t, chk, r.schema)
	}
	def := strings.TrimSpace(chk.Definition)
	if def == "" {
		return "", fmt.Errorf("check constraint %s has no definition", chk.Name)
	}
	expr, err := r.Expression(def, t.Columns)
	if err != nil {
		return "", fmt.Errorf("mapping check constraint %s: %w", chk.Name, err)
	}
	return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)",
		r.qualify(r.normalize(t.Name)),
		r.quote(r.normalize(chk.Name)),
		stripOuterCheckParens(expr)), nil
}

func (r Renderer) DropIndexDDL(tableName, indexName string) string {
	switch r.target {
	case "postgres":
		return fmt.Sprintf("DROP INDEX %s", r.qualify(r.normalize(indexName)))
	case "mssql":
		return fmt.Sprintf("DROP INDEX %s ON %s", r.quote(r.normalize(indexName)), r.qualify(r.normalize(tableName)))
	case "mysql":
		return fmt.Sprintf("DROP INDEX %s ON %s", r.quote(r.normalize(indexName)), r.qualify(r.normalize(tableName)))
	default:
		return ""
	}
}

func (r Renderer) DropForeignKeyDDL(tableName, fkName string) string {
	switch r.target {
	case "mysql":
		return fmt.Sprintf("ALTER TABLE %s DROP FOREIGN KEY %s", r.qualify(r.normalize(tableName)), r.quote(r.normalize(fkName)))
	default:
		return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", r.qualify(r.normalize(tableName)), r.quote(r.normalize(fkName)))
	}
}

func (r Renderer) DropCheckDDL(tableName, chkName string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", r.qualify(r.normalize(tableName)), r.quote(r.normalize(chkName)))
}

func (r Renderer) DropTableDDL(tableName string) string {
	// IF EXISTS is supported by all three targets (MSSQL since 2016, and SMT
	// requires compatibility level 140+).
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", r.qualify(r.normalize(tableName)))
}

func (r Renderer) AddColumnDDL(tableName string, col driver.Column, tableColumns []driver.Column) (string, error) {
	def, _, err := r.ColumnDefinition(col, tableColumns)
	if err != nil {
		return "", err
	}
	clause := "ADD COLUMN"
	if r.target == "mssql" {
		clause = "ADD"
	}
	return fmt.Sprintf("ALTER TABLE %s %s %s", r.qualify(r.normalize(tableName)), clause, def), nil
}

func (r Renderer) DropColumnDDL(tableName, colName string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", r.qualify(r.normalize(tableName)), r.quote(r.normalize(colName)))
}

func (r Renderer) AlterColumnTypeDDL(tableName string, col driver.Column) (string, error) {
	typ, err := r.ColumnType(col)
	if err != nil {
		return "", err
	}
	switch r.target {
	case "postgres":
		return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", r.qualify(r.normalize(tableName)), r.quote(r.normalize(col.Name)), typ), nil
	case "mssql":
		nullability := "NULL"
		if !col.IsNullable {
			nullability = "NOT NULL"
		}
		return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s %s %s", r.qualify(r.normalize(tableName)), r.quote(r.normalize(col.Name)), typ, nullability), nil
	case "mysql":
		def, _, err := r.ColumnDefinition(col)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s", r.qualify(r.normalize(tableName)), def), nil
	default:
		return "", fmt.Errorf("unsupported deterministic DDL target %q", r.target)
	}
}

func (r Renderer) AlterColumnNullabilityDDL(tableName string, col driver.Column) (string, error) {
	switch r.target {
	case "postgres":
		action := "DROP NOT NULL"
		if !col.IsNullable {
			action = "SET NOT NULL"
		}
		return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s %s", r.qualify(r.normalize(tableName)), r.quote(r.normalize(col.Name)), action), nil
	case "mssql", "mysql":
		return r.AlterColumnTypeDDL(tableName, col)
	default:
		return "", fmt.Errorf("unsupported deterministic DDL target %q", r.target)
	}
}

func (r Renderer) DropColumnDefaultDDL(tableName, colName string) string {
	switch r.target {
	case "postgres":
		return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT", r.qualify(r.normalize(tableName)), r.quote(r.normalize(colName)))
	case "mysql":
		return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT", r.qualify(r.normalize(tableName)), r.quote(r.normalize(colName)))
	case "mssql":
		schemaPredicate := "s.name = SCHEMA_NAME()"
		if strings.TrimSpace(r.schema) != "" {
			schemaPredicate = fmt.Sprintf("s.name = N'%s'", escapeSQLString(r.schema))
		}
		return fmt.Sprintf(
			"DECLARE @constraintName sysname; SELECT @constraintName = dc.name FROM sys.default_constraints dc JOIN sys.columns c ON c.default_object_id = dc.object_id JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE %s AND t.name = N'%s' AND c.name = N'%s'; IF @constraintName IS NOT NULL EXEC(N'ALTER TABLE %s DROP CONSTRAINT ' + QUOTENAME(@constraintName))",
			schemaPredicate,
			escapeSQLString(r.normalize(tableName)),
			escapeSQLString(r.normalize(colName)),
			escapeSQLString(r.qualify(r.normalize(tableName))),
		)
	default:
		return ""
	}
}

func (r Renderer) SetColumnDefaultDDL(tableName string, col driver.Column) (string, error) {
	def, err := r.ColumnDefault(col)
	if err != nil {
		return "", err
	}
	switch r.target {
	case "postgres":
		return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s", r.qualify(r.normalize(tableName)), r.quote(r.normalize(col.Name)), def), nil
	case "mysql":
		return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s", r.qualify(r.normalize(tableName)), r.quote(r.normalize(col.Name)), def), nil
	case "mssql":
		name := r.quote(r.normalize("df_" + tableName + "_" + col.Name))
		return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s DEFAULT %s FOR %s", r.qualify(r.normalize(tableName)), name, def, r.quote(r.normalize(col.Name))), nil
	default:
		return "", fmt.Errorf("unsupported deterministic DDL target %q", r.target)
	}
}

func (r Renderer) mssqlColumnType(col driver.Column, dt string) (string, error) {
	return r.canonicalColumnType(col, dt, "mssql")
}

func (r Renderer) mysqlColumnType(col driver.Column, dt string) (string, error) {
	return r.canonicalColumnType(col, dt, "mysql")
}

// canonicalColumnType maps a source type to the target dialect's DDL type
// through the canonical type IR (#62): the source column is normalized to a
// CanonicalType for the source dialect, then rendered for the target dialect.
// Non-portable (Raw) types fall through to the unknown-type policy.
func (r Renderer) canonicalColumnType(col driver.Column, dt, target string) (string, error) {
	ct := canonical.ToCanonical(dt, metaOf(col), r.source)
	typ, err := canonical.FromCanonical(ct, target, canonical.RenderOpts{IsIdentity: col.IsIdentity})
	if errors.Is(err, canonical.ErrUnknownType) {
		return r.unknownType(dt)
	}
	return typ, err
}

// metaOf extracts the type-shaping metadata canonical.ToCanonical needs.
func metaOf(col driver.Column) canonical.TypeMeta {
	return canonical.TypeMeta{
		MaxLength:         col.MaxLength,
		Precision:         col.Precision,
		Scale:             col.Scale,
		DatetimePrecision: col.DatetimePrecision,
		IsUnsigned:        col.IsUnsigned,
		DisplayWidth:      col.DisplayWidth,
		EnumValues:        col.EnumValues,
	}
}

func (r Renderer) Expression(expr string, tableColumns []driver.Column) (string, error) {
	out := strings.TrimSpace(expr)
	out = unwrapDefaultParens(out)
	out = strings.TrimPrefix(strings.TrimSpace(out), "CHECK ")
	out = replaceBracketIdentifiers(out, func(name string) string { return r.quote(r.normalize(name)) })
	out = replaceBacktickIdentifiers(out, func(name string) string { return r.quote(r.normalize(name)) })
	out = stripSQLServerUnicodeStringPrefixes(out)
	out = stripMySQLCharsetStringPrefixes(out)
	out = stripPostgresCasts(out)
	out = rewritePostgresAnyArray(out)
	out = rewriteFunctionNames(out, r.target)
	out = rewritePostgresStringConcat(out, r.target)
	out = rewriteBooleanLiterals(out, r.target, tableColumns, r.quote)
	var err error
	out, err = rewriteMySQLRegexpLike(out, r.target)
	if err != nil {
		return "", err
	}
	out, err = rewriteRegexOperators(out, r.target)
	if err != nil {
		return "", err
	}
	if r.target == "mysql" {
		out = rewriteSQLServerStringConcatToConcat(out)
	}
	out = trimExtraTrailingCloseParens(out)
	return out, nil
}

func (r Renderer) unknownType(dt string) (string, error) {
	switch r.unknownTypePolicy {
	case "warn", "text_fallback":
		switch r.target {
		case "mssql":
			return "NVARCHAR(MAX)", nil
		case "mysql":
			return "TEXT", nil
		default:
			return "text", nil
		}
	default:
		return "", fmt.Errorf("unsupported source type %q", dt)
	}
}

func (r Renderer) normalize(name string) string {
	return driver.NormalizeIdentifier(r.target, name)
}

func (r Renderer) quote(name string) string {
	switch r.target {
	case "postgres":
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	case "mssql":
		return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
	case "mysql":
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	default:
		return name
	}
}

func (r Renderer) qualify(table string) string {
	if r.schema == "" {
		return r.quote(table)
	}
	return r.quote(r.schema) + "." + r.quote(table)
}

func firstColumns(columns [][]driver.Column) []driver.Column {
	if len(columns) == 0 {
		return nil
	}
	return columns[0]
}

func canonicalTarget(target string) string {
	switch strings.ToLower(strings.TrimSpace(target)) {
	case "postgres", "postgresql", "pg":
		return "postgres"
	case "mssql", "sqlserver", "sql-server", "sql_server":
		return "mssql"
	case "mysql", "mariadb", "maria":
		return "mysql"
	default:
		// Defer to the driver registry so any alias a driver registers
		// (including future engines) resolves without editing this list.
		return strings.ToLower(driver.Canonicalize(strings.TrimSpace(target)))
	}
}

func normalizeTypeName(dt string) string {
	dt = strings.ToLower(strings.TrimSpace(dt))
	if idx := strings.Index(dt, "("); idx >= 0 {
		return strings.TrimSpace(dt[:idx])
	}
	return dt
}

// mysqlNowDefault renders the CURRENT_TIMESTAMP default with the same fsp the
// column type will carry — MySQL rejects a default whose fsp differs from the
// column's.
func mysqlNowDefault(col driver.Column) string {
	p := 6
	if col.DatetimePrecision != nil && *col.DatetimePrecision >= 0 {
		p = *col.DatetimePrecision
		if p > 6 {
			p = 6
		}
	}
	if p == 0 {
		return "CURRENT_TIMESTAMP"
	}
	return fmt.Sprintf("CURRENT_TIMESTAMP(%d)", p)
}

func isBooleanColumn(col driver.Column) bool {
	switch normalizeTypeName(col.DataType) {
	case "bit", "bool", "boolean", "tinyint(1)":
		return true
	default:
		return false
	}
}

func boolLiteral(target string, value bool) string {
	switch canonicalTarget(target) {
	case "postgres":
		if value {
			return "true"
		}
		return "false"
	default:
		if value {
			return "1"
		}
		return "0"
	}
}

func referentialAction(rule string) string {
	switch strings.ToUpper(strings.TrimSpace(rule)) {
	case "", "NO ACTION":
		return ""
	case "CASCADE", "SET NULL", "SET DEFAULT", "RESTRICT":
		return strings.ToUpper(strings.TrimSpace(rule))
	default:
		return ""
	}
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func isTextualSourceType(dt string) bool {
	switch normalizeTypeName(dt) {
	case "varchar", "nvarchar", "char", "nchar", "text", "ntext", "tinytext", "mediumtext", "longtext", "character", "character varying", "bpchar", "enum", "set":
		return true
	default:
		return false
	}
}

func isJSONSourceType(dt string) bool {
	switch normalizeTypeName(dt) {
	case "json", "jsonb":
		return true
	default:
		return false
	}
}

func isArraySourceType(dt string) bool {
	switch normalizeTypeName(dt) {
	case "array", "_text", "text[]", "_varchar", "varchar[]", "_bpchar", "bpchar[]", "_int2", "int2[]", "_int4", "int4[]", "_int8", "int8[]", "_uuid", "uuid[]":
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
		if r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (i > 0 && r >= '0' && r <= '9') {
			continue
		}
		return false
	}
	lower := strings.ToLower(expr)
	switch lower {
	case "null", "true", "false", "current_timestamp":
		return false
	default:
		return true
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

var (
	postgresCastTypePattern = `(?:character varying|timestamp without time zone|timestamp with time zone|double precision|[a-z_][a-z0-9_]*)(?:\[\])?`
	postgresStringCastRE    = regexp.MustCompile(`(?i)('(?:''|[^'])*')::` + postgresCastTypePattern)
	postgresNumberCastRE    = regexp.MustCompile(`(?i)(\([-+]?[0-9]+(?:\.[0-9]+)?\))::` + postgresCastTypePattern)
	postgresNullCastRE      = regexp.MustCompile(`(?i)\bNULL::` + postgresCastTypePattern)
	postgresParenCastRE     = regexp.MustCompile(`(?i)(\([a-z_][a-z0-9_]*\))::` + postgresCastTypePattern)
)

func stripPostgresCasts(s string) string {
	for {
		next := postgresStringCastRE.ReplaceAllString(s, "$1")
		next = postgresNumberCastRE.ReplaceAllString(next, "$1")
		next = postgresNullCastRE.ReplaceAllString(next, "NULL")
		next = postgresParenCastRE.ReplaceAllString(next, "$1")
		if next == s {
			return s
		}
		s = next
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

func trimExtraTrailingCloseParens(s string) string {
	for parenBalance(s) < 0 {
		trimmed := strings.TrimRight(s, " \t\n\r")
		if !strings.HasSuffix(trimmed, ")") {
			return s
		}
		s = trimmed[:len(trimmed)-1] + s[len(trimmed):]
	}
	return s
}

func parenBalance(s string) int {
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
		}
	}
	return depth
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
		if !inSingleQuote && ch == '[' {
			if !startsBracketIdentifier(s, i) {
				b.WriteByte(ch)
				continue
			}
			if end := strings.IndexByte(s[i+1:], ']'); end >= 0 {
				name := s[i+1 : i+1+end]
				b.WriteString(quote(name))
				i += end + 1
				continue
			}
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func startsBracketIdentifier(s string, idx int) bool {
	if idx <= 0 {
		return true
	}
	prev := s[idx-1]
	if (prev >= 'a' && prev <= 'z') ||
		(prev >= 'A' && prev <= 'Z') ||
		(prev >= '0' && prev <= '9') ||
		prev == '_' {
		return false
	}
	return true
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
	return !((prev >= 'a' && prev <= 'z') || (prev >= 'A' && prev <= 'Z') || (prev >= '0' && prev <= '9') || prev == '_' || prev == '"')
}

func rewriteFunctionNames(expr, target string) string {
	replacements := map[string]string{}
	switch canonicalTarget(target) {
	case "mssql":
		// CURRENT_TIMESTAMP is valid T-SQL (local time) and stays as-is;
		// rewriting it to SYSUTCDATETIME() would silently change local-time
		// semantics to UTC.
		replacements = map[string]string{
			"NOW()":              "SYSDATETIME()",
			"gen_random_uuid()":  "NEWID()",
			"uuid_generate_v4()": "NEWID()",
			"UUID()":             "NEWID()",
		}
	case "mysql":
		replacements = map[string]string{
			"GETDATE()":           "CURRENT_TIMESTAMP",
			"GETUTCDATE()":        "CURRENT_TIMESTAMP",
			"SYSDATETIME()":       "CURRENT_TIMESTAMP",
			"SYSUTCDATETIME()":    "CURRENT_TIMESTAMP",
			"SYSDATETIMEOFFSET()": "CURRENT_TIMESTAMP",
			"NEWID()":             "UUID()",
			"gen_random_uuid()":   "UUID()",
			"uuid_generate_v4()":  "UUID()",
		}
	}
	out := expr
	for from, to := range replacements {
		out = replaceCaseInsensitive(out, from, to)
	}
	return out
}

var ciPatternCache sync.Map // pattern string -> *regexp.Regexp

func replaceCaseInsensitive(s, old, new string) string {
	return cachedRegexp(`(?i)\b`+regexp.QuoteMeta(old)).ReplaceAllString(s, new)
}

func rewriteBooleanLiterals(expr, target string, columns []driver.Column, quote func(string) string) string {
	out := expr
	for _, col := range columns {
		if !isBooleanColumn(col) {
			continue
		}
		normalized := driver.NormalizeIdentifier(canonicalTarget(target), col.Name)
		// Cheap pre-check: the patterns below can only match if the column
		// name appears in the expression at all.
		lowerOut := strings.ToLower(out)
		if !strings.Contains(lowerOut, strings.ToLower(col.Name)) && !strings.Contains(lowerOut, strings.ToLower(normalized)) {
			continue
		}
		identifiers := []struct {
			pattern     string
			replacement string
		}{
			{`\b` + regexp.QuoteMeta(col.Name) + `\b`, col.Name},
			{`\b` + regexp.QuoteMeta(normalized) + `\b`, normalized},
			{regexp.QuoteMeta(quote(normalized)), quote(normalized)},
		}
		// Compiled per use, not cached: these patterns derive from schema
		// column names, so caching them would grow without bound across
		// schemas in long-lived processes. The pre-check above keeps this
		// path rare.
		for _, ident := range identifiers {
			out = regexp.MustCompile(`(?i)`+ident.pattern+`\s*=\s*\(?1\)?`).ReplaceAllString(out, ident.replacement+"="+boolLiteral(target, true))
			out = regexp.MustCompile(`(?i)`+ident.pattern+`\s*=\s*\(?0\)?`).ReplaceAllString(out, ident.replacement+"="+boolLiteral(target, false))
		}
	}
	return out
}

// cachedRegexp caches compiled patterns. Only call it with patterns drawn
// from a bounded set (e.g. the static function-name rewrites) — schema-derived
// patterns would grow the cache without bound.
func cachedRegexp(pattern string) *regexp.Regexp {
	cached, ok := ciPatternCache.Load(pattern)
	if !ok {
		cached, _ = ciPatternCache.LoadOrStore(pattern, regexp.MustCompile(pattern))
	}
	return cached.(*regexp.Regexp)
}

var pgRegexOperatorRE = regexp.MustCompile(`(\(?\s*(?:[A-Za-z_][A-Za-z0-9_]*|"[^"]+"|\[[^\]]+\]|` + "`[^`]+`" + `)\s*\)?)\s*~\s*('[^']*(?:''[^']*)*')`)

func rewriteRegexOperators(expr, target string) (string, error) {
	target = canonicalTarget(target)
	if target != "mysql" && target != "mssql" {
		return expr, nil
	}
	if !strings.Contains(expr, "~") {
		return expr, nil
	}
	re := pgRegexOperatorRE
	var rewriteErr error
	out := re.ReplaceAllStringFunc(expr, func(match string) string {
		if rewriteErr != nil {
			return match
		}
		parts := re.FindStringSubmatch(match)
		if len(parts) != 3 {
			return match
		}
		operand := strings.Trim(parts[1], "() \t\n\r")
		pattern := parts[2]
		switch target {
		case "mysql":
			return "(" + operand + " REGEXP " + pattern + ")"
		case "mssql":
			if likePattern, ok := regexLiteralToSQLServerLike(pattern); ok {
				return "(" + operand + " LIKE '" + escapeSQLString(likePattern) + "')"
			}
			if isEmailRegexLiteral(pattern) {
				return "(" + operand + " LIKE '%_@_%._%' AND " + operand + " NOT LIKE '% %')"
			}
			rewriteErr = fmt.Errorf("unsupported PostgreSQL regex pattern %s for SQL Server target", pattern)
			return match
		default:
			return match
		}
	})
	if rewriteErr != nil {
		return "", rewriteErr
	}
	return out, nil
}

var postgresAnyArrayRE = regexp.MustCompile(`(?is)\(?\s*(\(?\s*(?:[a-z_][a-z0-9_]*|"[^"]+"|\[[^\]]+\]|` + "`[^`]+`" + `)\s*\)?)\s*=\s*ANY\s*\(\s*\(?\s*ARRAY\[(.*?)\]\s*\)?(?:::` + postgresCastTypePattern + `)?\s*\)`)

func rewritePostgresAnyArray(expr string) string {
	return postgresAnyArrayRE.ReplaceAllStringFunc(expr, func(match string) string {
		parts := postgresAnyArrayRE.FindStringSubmatch(match)
		if len(parts) != 3 {
			return match
		}
		column := strings.TrimSpace(parts[1])
		column = strings.Trim(column, "() \t\n\r")
		values := strings.TrimSpace(parts[2])
		if column == "" || values == "" {
			return match
		}
		return column + " IN (" + values + ")"
	})
}

func rewritePostgresStringConcat(expr, target string) string {
	target = canonicalTarget(target)
	if target != "mssql" && target != "mysql" {
		return expr
	}
	if !strings.Contains(expr, "||") {
		return expr
	}
	parts := collectTopLevelOperatorParts(expr, "||")
	if len(parts) < 2 {
		return expr
	}
	switch target {
	case "mssql":
		return strings.Join(parts, " + ")
	case "mysql":
		return "CONCAT(" + strings.Join(parts, ", ") + ")"
	default:
		return expr
	}
}

func collectTopLevelOperatorParts(expr, op string) []string {
	expr = strings.TrimSpace(unwrapDefaultParens(expr))
	var parts []string
	start := 0
	depth := 0
	inSingleQuote := false
	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if ch == '\'' {
			if inSingleQuote && i+1 < len(expr) && expr[i+1] == '\'' {
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
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && strings.HasPrefix(expr[i:], op) {
				left := strings.TrimSpace(expr[start:i])
				if left != "" {
					parts = append(parts, collectTopLevelOperatorParts(left, op)...)
				}
				i += len(op) - 1
				start = i + 1
			}
		}
	}
	tail := strings.TrimSpace(expr[start:])
	if tail != "" {
		if strings.Contains(tail, op) {
			nested := collectTopLevelOperatorParts(tail, op)
			if len(nested) > 1 {
				parts = append(parts, nested...)
			} else {
				parts = append(parts, tail)
			}
		} else {
			parts = append(parts, tail)
		}
	}
	return parts
}

var mysqlRegexpLikeRE = regexp.MustCompile(`(?i)regexp_like\s*\(\s*([^,]+?)\s*,\s*('(?:''|[^'])*')\s*\)`)

func rewriteMySQLRegexpLike(expr, target string) (string, error) {
	target = canonicalTarget(target)
	var rewriteErr error
	out := mysqlRegexpLikeRE.ReplaceAllStringFunc(expr, func(match string) string {
		if rewriteErr != nil {
			return match
		}
		parts := mysqlRegexpLikeRE.FindStringSubmatch(match)
		if len(parts) != 3 {
			return match
		}
		switch target {
		case "mysql":
			return "(" + strings.TrimSpace(parts[1]) + " REGEXP " + parts[2] + ")"
		case "mssql":
			likePattern, ok := regexLiteralToSQLServerLike(parts[2])
			if !ok {
				rewriteErr = fmt.Errorf("unsupported MySQL REGEXP_LIKE pattern %s for SQL Server target", parts[2])
				return match
			}
			return "(" + strings.TrimSpace(parts[1]) + " LIKE '" + escapeSQLString(likePattern) + "')"
		default:
			return match
		}
	})
	if rewriteErr != nil {
		return "", rewriteErr
	}
	return out, nil
}

func regexLiteralToSQLServerLike(quoted string) (string, bool) {
	pattern, ok := unquoteSQLString(quoted)
	if !ok || !strings.HasPrefix(pattern, "^") || !strings.HasSuffix(pattern, "$") {
		return "", false
	}
	pattern = strings.TrimSuffix(strings.TrimPrefix(pattern, "^"), "$")
	var b strings.Builder
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		if ch == '[' {
			end := strings.IndexByte(pattern[i+1:], ']')
			if end < 0 {
				return "", false
			}
			token := pattern[i : i+end+2]
			i += end + 1
			repeat := 1
			if i+1 < len(pattern) && pattern[i+1] == '{' {
				close := strings.IndexByte(pattern[i+2:], '}')
				if close < 0 {
					return "", false
				}
				n := 0
				for _, r := range pattern[i+2 : i+2+close] {
					if r < '0' || r > '9' {
						return "", false
					}
					n = n*10 + int(r-'0')
				}
				if n <= 0 {
					return "", false
				}
				repeat = n
				i += close + 2
			}
			for j := 0; j < repeat; j++ {
				b.WriteString(token)
			}
			continue
		}
		if strings.ContainsRune(`\.^$*+?()|{}`, rune(ch)) {
			return "", false
		}
		if ch == '%' || ch == '_' {
			return "", false
		}
		b.WriteByte(ch)
	}
	return b.String(), true
}

func isEmailRegexLiteral(quoted string) bool {
	pattern, ok := unquoteSQLString(quoted)
	if !ok {
		return false
	}
	return pattern == `^[^@\s]+@[^@\s]+\.[^@\s]+$`
}

func unquoteSQLString(quoted string) (string, bool) {
	if len(quoted) < 2 || quoted[0] != '\'' || quoted[len(quoted)-1] != '\'' {
		return "", false
	}
	body := quoted[1 : len(quoted)-1]
	var b strings.Builder
	for i := 0; i < len(body); i++ {
		if body[i] == '\'' {
			if i+1 < len(body) && body[i+1] == '\'' {
				b.WriteByte('\'')
				i++
				continue
			}
			return "", false
		}
		b.WriteByte(body[i])
	}
	return b.String(), true
}

func rewriteSQLServerStringConcatToConcat(expr string) string {
	// Handles the CRM-style "a + ' ' + b" computed expression without trying
	// to become a SQL parser. Only fires when a top-level operand is a string
	// literal — numeric/date arithmetic ("subtotal + tax",
	// "CURRENT_TIMESTAMP + INTERVAL '1' DAY") must stay arithmetic.
	if !strings.Contains(expr, "+") || strings.ContainsAny(expr, "*/") {
		return expr
	}
	parts := splitTopLevel(expr, '+')
	if len(parts) < 2 {
		return expr
	}
	hasStringOperand := false
	for _, p := range parts {
		if strings.HasPrefix(strings.TrimSpace(p), "'") {
			hasStringOperand = true
			break
		}
	}
	if !hasStringOperand {
		return expr
	}
	return "CONCAT(" + strings.Join(parts, ", ") + ")"
}

func splitTopLevel(expr string, sep rune) []string {
	var parts []string
	var b strings.Builder
	depth := 0
	inSingleQuote := false
	for _, r := range expr {
		if r == '\'' {
			inSingleQuote = !inSingleQuote
			b.WriteRune(r)
			continue
		}
		if !inSingleQuote {
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
	}
	parts = append(parts, strings.TrimSpace(b.String()))
	return parts
}

func stripOuterCheckParens(expr string) string {
	expr = strings.TrimSpace(expr)
	if strings.HasPrefix(strings.ToUpper(expr), "CHECK") {
		// Only strip the keyword, not identifiers like "checked_in": the next
		// character must end the word.
		rest := expr[5:]
		if rest == "" || rest[0] == '(' || rest[0] == ' ' || rest[0] == '\t' || rest[0] == '\n' || rest[0] == '\r' {
			expr = strings.TrimSpace(rest)
		}
	}
	return unwrapDefaultParens(expr)
}
