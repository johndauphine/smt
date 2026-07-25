// Package schema exposes SMT's deterministic schema and DDL renderer as a
// library API. It deliberately accepts only public value types, while its
// built-in dialects adapt those values to SMT's existing renderer.
package schema

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/johndauphine/smt/internal/ddl"
	"github.com/johndauphine/smt/internal/driver"
	"github.com/johndauphine/smt/schema/canonical"
)

// UnknownTypePolicy controls what built-in dialects do with a source type
// that has no portable canonical mapping.
type UnknownTypePolicy string

const (
	// UnknownTypeFail rejects a column whose source type cannot be rendered.
	UnknownTypeFail UnknownTypePolicy = "fail"
	// UnknownTypeWarn renders a conservative text fallback and reports a warning.
	UnknownTypeWarn UnknownTypePolicy = "warn"
	// UnknownTypeTextFallback renders a conservative text fallback and reports a warning.
	UnknownTypeTextFallback UnknownTypePolicy = "text_fallback"
)

// Capabilities documents which DDL features a dialect implementation accepts.
// Callers can inspect it before constructing schemas, and Renderer returns an
// UnsupportedFeatureError instead of silently omitting an unsupported fact.
type Capabilities struct {
	CreateSchema           bool
	CreateTable            bool
	CreateColumn           bool
	PrimaryKeys            bool
	IdentityColumns        bool
	Defaults               bool
	ComputedColumns        bool
	SecondaryIndexes       bool
	StandalonePrimaryKeys  bool
	NamedUniqueConstraints bool
	CheckConstraints       bool
	IndexExpressionKeys    bool
	IndexPrefixLengths     bool
	IndexIncludeColumns    bool
	FilteredIndexes        bool
}

// Request is the rendering context supplied to a Dialect implementation.
// TargetDialect is intentionally absent: the selected Dialect owns that
// decision. SourceDialect is optional; it preserves source-specific type
// semantics when it is known (for example MySQL TINYINT(1)).
type Request struct {
	Schema            string
	SourceDialect     string
	UnknownTypePolicy UnknownTypePolicy
}

// Options selects a target dialect and configures a Renderer.
type Options struct {
	TargetDialect     string
	Schema            string
	SourceDialect     string
	UnknownTypePolicy UnknownTypePolicy
}

// Result is a deterministic SQL fragment together with advisory mapping or
// target-semantics warnings. A non-empty SQL string never contains comments or
// hidden policy decisions; callers can decide how to surface Warnings.
type Result struct {
	SQL      string
	Warnings []Warning
}

// StatementKind identifies the create artifact emitted by a Statement. The
// public create plan contains schema and table statements only. Independent
// side objects are rendered through Renderer.CreateIndex,
// Renderer.CreatePrimaryKey, Renderer.CreateUniqueConstraint, and
// Renderer.CreateCheckConstraint so callers retain their own schedule.
type StatementKind string

const (
	// StatementCreateSchema creates the configured schema or database.
	StatementCreateSchema StatementKind = "create_schema"
	// StatementCreateTable creates one table, including its columns and
	// primary key when supplied.
	StatementCreateTable StatementKind = "create_table"
)

// Statement is one deterministic executable DDL artifact. SQL is exactly one
// statement; callers retain execution, retry, and scheduling policy.
//
// Warnings apply only to this artifact. They are advisory and never change
// SQL, which keeps plans deterministic and safe to persist or inspect before
// execution.
type Statement struct {
	Kind     StatementKind
	SQL      string
	Warnings []Warning
}

// Plan is an ordered, deterministic set of CREATE statements. It owns no
// connection or execution behavior, so applications such as DMT can discover
// objects, schedule execution, and execute the resulting SQL themselves.
type Plan struct {
	Statements []Statement
}

// IsEmpty reports whether the plan has no executable statements.
func (p Plan) IsEmpty() bool { return len(p.Statements) == 0 }

// Warning identifies a lossy type mapping or target-specific semantic caveat.
// Table and Column are empty when a warning applies to a broader artifact.
type Warning struct {
	Kind          string
	Reason        string
	SourceDialect string
	TargetDialect string
	Table         string
	Column        string
}

// Column is the public schema-column input accepted by the built-in renderers.
// DataType is the source dialect's catalog type name. The metadata fields are
// the small, stable subset required for SMT's canonical type mapping.
type Column struct {
	Name              string
	DataType          string
	MaxLength         int
	Precision         int
	Scale             int
	DatetimePrecision *int
	IsNullable        bool
	IsIdentity        bool
	IsUnsigned        bool
	DisplayWidth      int
	DefaultExpression string
	// HasDefault preserves a DEFAULT clause whose expression is intentionally
	// empty. It is otherwise inferred from DefaultExpression for compatibility.
	HasDefault         bool
	OnUpdateExpression string
	IsComputed         bool
	ComputedExpression string
	ComputedPersisted  bool
	EnumValues         []string
	SRID               int
	SpatialSubType     string
}

// TypeMeta returns the public canonical-mapper metadata represented by c.
// It is useful to callers that need to inspect or preflight the exact same
// source-type facts that CreateColumn and CreateTable consume.
func (c Column) TypeMeta() canonical.TypeMeta {
	return canonical.TypeMeta{
		MaxLength:         c.MaxLength,
		Precision:         c.Precision,
		Scale:             c.Scale,
		DatetimePrecision: c.DatetimePrecision,
		IsUnsigned:        c.IsUnsigned,
		DisplayWidth:      c.DisplayWidth,
		EnumValues:        append([]string(nil), c.EnumValues...),
		SRID:              c.SRID,
		SpatialSubType:    c.SpatialSubType,
	}
}

// Table is the practical create-table subset shared by all built-in dialects.
type Table struct {
	Name       string
	Columns    []Column
	PrimaryKey []string
}

// TableRef identifies a target table for a standalone side object. Columns are
// optional context for source-expression translation in filtered indexes and
// check constraints; they are not used to infer or emit table DDL.
//
// Schema qualification deliberately comes from Renderer Options, just as it
// does for CreateTable. This keeps all artifact names on one deterministic
// target-schema policy and mirrors SMT's existing DMT rendering behavior.
type TableRef struct {
	Name    string
	Columns []Column
}

// Index is a named secondary index. IsUnique selects a unique index, which is
// intentionally distinct from UniqueConstraint: an index is an independent
// physical artifact while a named UNIQUE constraint is part of the table's
// relational contract.
//
// Columns, ColumnExpressions, and ColumnPrefixLengths are positionally
// aligned. Their input order is preserved in the rendered SQL.
type Index struct {
	Name                string
	Columns             []string
	ColumnExpressions   []bool
	ColumnPrefixLengths []int
	IsUnique            bool
	IncludeColumns      []string
	Filter              string
}

// PrimaryKey is a standalone primary-key constraint. Name is optional: when
// omitted, CreatePrimaryKey deterministically uses "pk_" plus the
// target-normalized TableRef name, matching SMT's existing CREATE TABLE
// convention.
type PrimaryKey struct {
	Name    string
	Columns []string
}

// UniqueConstraint is a named UNIQUE constraint. It is deliberately separate
// from Index with IsUnique so callers choose the database object they intend.
type UniqueConstraint struct {
	Name    string
	Columns []string
}

// CheckConstraint is a named CHECK constraint. Expression is source-dialect
// SQL and is translated by SMT's existing deterministic expression renderer.
type CheckConstraint struct {
	Name       string
	Expression string
}

// Dialect lets applications register a custom public renderer without
// importing SMT internals. Built-in dialects use this same interface, so the
// registry does not special-case external implementations.
type Dialect interface {
	Name() string
	Aliases() []string
	Capabilities() Capabilities
	RenderSchema(Request) (Result, error)
	RenderTable(Request, Table) (Result, error)
	RenderColumn(Request, Column) (Result, error)
}

// SideObjectDialect is the optional extension implemented by a Dialect that
// renders standalone indexes and constraints. Keeping it separate preserves
// source compatibility for existing custom Dialect implementations that only
// render schemas, tables, and columns.
//
// A SideObjectDialect should set the matching Capabilities fields to true.
// Built-in dialects implement this interface.
type SideObjectDialect interface {
	Dialect
	RenderIndex(Request, TableRef, Index) (Result, error)
	RenderPrimaryKey(Request, TableRef, PrimaryKey) (Result, error)
	RenderUniqueConstraint(Request, TableRef, UniqueConstraint) (Result, error)
	RenderCheckConstraint(Request, TableRef, CheckConstraint) (Result, error)
}

var (
	// ErrUnknownDialect means a target dialect or alias is absent from a Registry.
	ErrUnknownDialect = errors.New("unknown DDL dialect")
	// ErrDialectRegistered means a name or alias has already been registered.
	ErrDialectRegistered = errors.New("DDL dialect name or alias already registered")
)

// UnsupportedFeatureError is returned when input asks a selected dialect for a
// capability it explicitly does not offer.
type UnsupportedFeatureError struct {
	Dialect string
	Feature string
}

func (e *UnsupportedFeatureError) Error() string {
	return fmt.Sprintf("DDL dialect %q does not support %s", e.Dialect, e.Feature)
}

// Registry selects a Dialect by case-insensitive name or alias. A fresh
// registry is initialized with PostgreSQL, SQL Server, MySQL, SQLite, and
// ClickHouse; callers can register additional Dialect implementations.
type Registry struct {
	mu       sync.RWMutex
	dialects map[string]Dialect
	names    map[string]Dialect
}

// NewRegistry returns an isolated registry populated with SMT's built-in
// dialects. Registries are isolated so library users do not share mutable
// global registration state.
func NewRegistry() *Registry {
	r := &Registry{
		dialects: make(map[string]Dialect),
		names:    make(map[string]Dialect),
	}
	for _, dialect := range builtinDialects() {
		if err := r.Register(dialect); err != nil {
			panic(fmt.Sprintf("register built-in DDL dialect %q: %v", dialect.Name(), err))
		}
	}
	return r
}

// Register adds a Dialect and all of its aliases. Names and aliases must be
// non-empty and unique within the registry.
func (r *Registry) Register(d Dialect) error {
	if d == nil {
		return fmt.Errorf("register DDL dialect: nil dialect")
	}
	name := normalizedDialectName(d.Name())
	if name == "" {
		return fmt.Errorf("register DDL dialect: empty name")
	}
	keys := make([]string, 0, len(d.Aliases())+1)
	keys = append(keys, name)
	seen := map[string]struct{}{name: {}}
	for _, alias := range d.Aliases() {
		alias = normalizedDialectName(alias)
		if alias == "" {
			return fmt.Errorf("register DDL dialect %q: empty alias", name)
		}
		if _, exists := seen[alias]; exists {
			return fmt.Errorf("%w: %q", ErrDialectRegistered, alias)
		}
		seen[alias] = struct{}{}
		keys = append(keys, alias)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, key := range keys {
		if _, exists := r.names[key]; exists {
			return fmt.Errorf("%w: %q", ErrDialectRegistered, key)
		}
	}
	r.dialects[name] = d
	for _, key := range keys {
		r.names[key] = d
	}
	return nil
}

// Resolve returns the selected Dialect for name or alias.
func (r *Registry) Resolve(name string) (Dialect, error) {
	key := normalizedDialectName(name)
	r.mu.RLock()
	d, ok := r.names[key]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w %q (available: %v)", ErrUnknownDialect, name, r.Dialects())
	}
	return d, nil
}

// Dialects returns the canonical built-in and custom dialect names in stable
// order. Aliases are intentionally omitted.
func (r *Registry) Dialects() []string {
	r.mu.RLock()
	names := make([]string, 0, len(r.dialects))
	for name := range r.dialects {
		names = append(names, name)
	}
	r.mu.RUnlock()
	sort.Strings(names)
	return names
}

// NewRenderer selects opts.TargetDialect from this registry.
func (r *Registry) NewRenderer(opts Options) (Renderer, error) {
	dialect, err := r.Resolve(opts.TargetDialect)
	if err != nil {
		return Renderer{}, err
	}
	policy, err := normalizePolicy(opts.UnknownTypePolicy)
	if err != nil {
		return Renderer{}, err
	}
	return Renderer{
		dialect: dialect,
		request: Request{
			Schema:            opts.Schema,
			SourceDialect:     opts.SourceDialect,
			UnknownTypePolicy: policy,
		},
	}, nil
}

// NewRenderer selects a built-in target dialect from a fresh default registry.
// Use Registry.NewRenderer when an application registers custom dialects.
func NewRenderer(opts Options) (Renderer, error) {
	return NewRegistry().NewRenderer(opts)
}

// Renderer exposes schema, table, and column rendering for one selected
// dialect. Its methods are safe to use concurrently after construction.
type Renderer struct {
	dialect Dialect
	request Request
}

// Dialect returns the selected dialect's canonical name.
func (r Renderer) Dialect() string { return r.dialect.Name() }

// Capabilities reports the selected dialect's explicit feature contract.
func (r Renderer) Capabilities() Capabilities { return r.dialect.Capabilities() }

// CreateSchema renders the schema/database selected in Options.Schema. An empty
// schema is a no-op so callers can use one configuration for qualified and
// unqualified table output.
func (r Renderer) CreateSchema() (Result, error) {
	if strings.TrimSpace(r.request.Schema) == "" {
		return Result{}, nil
	}
	if !r.Capabilities().CreateSchema {
		return Result{}, r.unsupported("schema creation")
	}
	return r.dialect.RenderSchema(r.request)
}

// CreateTable renders one CREATE TABLE statement.
func (r Renderer) CreateTable(table Table) (Result, error) {
	if err := r.validateTable(table); err != nil {
		return Result{}, err
	}
	return r.dialect.RenderTable(r.request, table)
}

// CreateColumn renders one complete column definition, suitable for a CREATE
// TABLE column list or a dialect-appropriate ALTER TABLE operation.
func (r Renderer) CreateColumn(column Column) (Result, error) {
	if !r.Capabilities().CreateColumn {
		return Result{}, r.unsupported("column creation")
	}
	if err := r.validateColumn(column); err != nil {
		return Result{}, err
	}
	return r.dialect.RenderColumn(r.request, column)
}

// CreateIndex renders a standalone secondary index. The table must already
// exist when the returned statement is executed. Index column order is
// preserved, and unsupported index features return UnsupportedFeatureError
// before SMT attempts to render SQL.
func (r Renderer) CreateIndex(table TableRef, index Index) (Result, error) {
	caps := r.Capabilities()
	if !caps.SecondaryIndexes {
		return Result{}, r.unsupported("secondary indexes")
	}
	if hasTrue(index.ColumnExpressions) && !caps.IndexExpressionKeys {
		return Result{}, r.unsupported("expression index key parts")
	}
	if hasPositive(index.ColumnPrefixLengths) && !caps.IndexPrefixLengths {
		return Result{}, r.unsupported("index column prefix lengths")
	}
	if len(index.IncludeColumns) > 0 && !caps.IndexIncludeColumns {
		return Result{}, r.unsupported("index include columns")
	}
	if strings.TrimSpace(index.Filter) != "" && !caps.FilteredIndexes {
		return Result{}, r.unsupported("filtered indexes")
	}
	if err := validateIndex(table, index); err != nil {
		return Result{}, err
	}
	dialect, err := r.sideObjectDialect("secondary indexes")
	if err != nil {
		return Result{}, err
	}
	return dialect.RenderIndex(r.request, table, index)
}

// CreatePrimaryKey renders a standalone primary-key constraint. Its optional
// name defaults deterministically to pk_<table>, matching CreateTable.
func (r Renderer) CreatePrimaryKey(table TableRef, primaryKey PrimaryKey) (Result, error) {
	if !r.Capabilities().StandalonePrimaryKeys {
		return Result{}, r.unsupported("standalone primary keys")
	}
	if err := validateKeyConstraint("primary key", table, primaryKey.Name, primaryKey.Columns, true); err != nil {
		return Result{}, err
	}
	if strings.TrimSpace(primaryKey.Name) == "" {
		primaryKey.Name = "pk_" + driver.NormalizeIdentifier(r.Dialect(), table.Name)
	}
	dialect, err := r.sideObjectDialect("standalone primary keys")
	if err != nil {
		return Result{}, err
	}
	return dialect.RenderPrimaryKey(r.request, table, primaryKey)
}

// CreateUniqueConstraint renders a standalone, named UNIQUE constraint. Use
// CreateIndex with Index.IsUnique for a unique index instead.
func (r Renderer) CreateUniqueConstraint(table TableRef, unique UniqueConstraint) (Result, error) {
	if !r.Capabilities().NamedUniqueConstraints {
		return Result{}, r.unsupported("named unique constraints")
	}
	if err := validateKeyConstraint("unique", table, unique.Name, unique.Columns, false); err != nil {
		return Result{}, err
	}
	dialect, err := r.sideObjectDialect("named unique constraints")
	if err != nil {
		return Result{}, err
	}
	return dialect.RenderUniqueConstraint(r.request, table, unique)
}

// CreateCheckConstraint renders a standalone, named CHECK constraint. The
// table's optional Columns context is used for deterministic expression
// translation, including boolean-convention rewrites.
func (r Renderer) CreateCheckConstraint(table TableRef, check CheckConstraint) (Result, error) {
	if !r.Capabilities().CheckConstraints {
		return Result{}, r.unsupported("check constraints")
	}
	if err := validateTableRef("check constraint", table); err != nil {
		return Result{}, err
	}
	if strings.TrimSpace(check.Name) == "" {
		return Result{}, fmt.Errorf("render check constraint on table %q: empty constraint name", table.Name)
	}
	if strings.TrimSpace(check.Expression) == "" {
		return Result{}, fmt.Errorf("render check constraint %q: empty expression", check.Name)
	}
	dialect, err := r.sideObjectDialect("check constraints")
	if err != nil {
		return Result{}, err
	}
	return dialect.RenderCheckConstraint(r.request, table, check)
}

// PlanCreate renders the supported create path in execution order: an
// optional schema/database statement followed by the supplied tables in input
// order. It deliberately does not infer dependency ordering or include
// indexes, foreign keys, checks, alterations, or drops; those remain the
// caller's scheduling responsibility and are outside this API's create scope.
func (r Renderer) PlanCreate(tables []Table) (Plan, error) {
	plan := Plan{Statements: make([]Statement, 0, len(tables)+1)}

	// SQLite has no independently creatable schema. DMT has historically
	// accepted a configured schema for SQLite and ignored it at create time, so
	// retain that execution-compatible behavior in the whole create plan while
	// CreateSchema continues to report named SQLite schema creation as
	// unsupported when called directly.
	if r.Dialect() != "sqlite" {
		schemaResult, err := r.CreateSchema()
		if err != nil {
			return Plan{}, err
		}
		if schemaResult.SQL != "" {
			plan.Statements = append(plan.Statements, Statement{
				Kind:     StatementCreateSchema,
				SQL:      schemaResult.SQL,
				Warnings: append([]Warning(nil), schemaResult.Warnings...),
			})
		}
	}

	for _, table := range tables {
		result, err := r.CreateTable(table)
		if err != nil {
			return Plan{}, err
		}
		plan.Statements = append(plan.Statements, Statement{
			Kind:     StatementCreateTable,
			SQL:      result.SQL,
			Warnings: append([]Warning(nil), result.Warnings...),
		})
	}
	return plan, nil
}

func (r Renderer) validateTable(table Table) error {
	if !r.Capabilities().CreateTable {
		return r.unsupported("table creation")
	}
	if strings.TrimSpace(table.Name) == "" {
		return fmt.Errorf("render table: empty table name")
	}
	if len(table.Columns) == 0 {
		return fmt.Errorf("render table %q: no columns", table.Name)
	}
	if len(table.PrimaryKey) > 0 && !r.Capabilities().PrimaryKeys {
		return r.unsupported("primary keys")
	}
	columnNames := make(map[string]struct{}, len(table.Columns))
	for _, column := range table.Columns {
		if err := r.validateColumn(column); err != nil {
			return fmt.Errorf("render table %q: %w", table.Name, err)
		}
		columnNames[column.Name] = struct{}{}
	}
	for _, name := range table.PrimaryKey {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("render table %q: primary key contains an empty column name", table.Name)
		}
		if _, exists := columnNames[name]; !exists {
			return fmt.Errorf("render table %q: primary key column %q does not exist", table.Name, name)
		}
	}
	return nil
}

func (r Renderer) validateColumn(column Column) error {
	if strings.TrimSpace(column.Name) == "" {
		return fmt.Errorf("render column: empty column name")
	}
	if strings.TrimSpace(column.DataType) == "" {
		return fmt.Errorf("render column %q: empty source data type", column.Name)
	}
	caps := r.Capabilities()
	if column.IsIdentity && !caps.IdentityColumns {
		return r.unsupported("identity columns")
	}
	if (column.HasDefault || strings.TrimSpace(column.DefaultExpression) != "" || strings.TrimSpace(column.OnUpdateExpression) != "") && !caps.Defaults {
		return r.unsupported("default expressions")
	}
	if column.IsComputed && !caps.ComputedColumns {
		return r.unsupported("computed columns")
	}
	return nil
}

func (r Renderer) unsupported(feature string) error {
	return &UnsupportedFeatureError{Dialect: r.Dialect(), Feature: feature}
}

func (r Renderer) sideObjectDialect(feature string) (SideObjectDialect, error) {
	dialect, ok := r.dialect.(SideObjectDialect)
	if !ok {
		return nil, r.unsupported(feature)
	}
	return dialect, nil
}

func validateTableRef(artifact string, table TableRef) error {
	if strings.TrimSpace(table.Name) == "" {
		return fmt.Errorf("render %s: empty table name", artifact)
	}
	return nil
}

func validateIndex(table TableRef, index Index) error {
	if err := validateTableRef("index", table); err != nil {
		return err
	}
	if strings.TrimSpace(index.Name) == "" {
		return fmt.Errorf("render index on table %q: empty index name", table.Name)
	}
	if len(index.Columns) == 0 {
		return fmt.Errorf("render index %q: no columns", index.Name)
	}
	if len(index.ColumnExpressions) > len(index.Columns) {
		return fmt.Errorf("render index %q: column expression flags exceed columns", index.Name)
	}
	if len(index.ColumnPrefixLengths) > len(index.Columns) {
		return fmt.Errorf("render index %q: column prefix lengths exceed columns", index.Name)
	}
	for i, column := range index.Columns {
		if strings.TrimSpace(column) == "" {
			return fmt.Errorf("render index %q: column %d is empty", index.Name, i)
		}
	}
	for i, length := range index.ColumnPrefixLengths {
		if length < 0 {
			return fmt.Errorf("render index %q: column prefix length %d is negative", index.Name, length)
		}
		if length > 0 && i < len(index.ColumnExpressions) && index.ColumnExpressions[i] {
			return fmt.Errorf("render index %q: expression column %d cannot have a prefix length", index.Name, i)
		}
	}
	for i, column := range index.IncludeColumns {
		if strings.TrimSpace(column) == "" {
			return fmt.Errorf("render index %q: included column %d is empty", index.Name, i)
		}
	}
	return nil
}

func validateKeyConstraint(kind string, table TableRef, name string, columns []string, nameOptional bool) error {
	if err := validateTableRef(kind+" constraint", table); err != nil {
		return err
	}
	if !nameOptional && strings.TrimSpace(name) == "" {
		return fmt.Errorf("render %s constraint on table %q: empty constraint name", kind, table.Name)
	}
	if len(columns) == 0 {
		return fmt.Errorf("render %s constraint %q: no columns", kind, name)
	}
	for i, column := range columns {
		if strings.TrimSpace(column) == "" {
			return fmt.Errorf("render %s constraint %q: column %d is empty", kind, name, i)
		}
	}
	return nil
}

func hasTrue(values []bool) bool {
	for _, value := range values {
		if value {
			return true
		}
	}
	return false
}

func hasPositive(values []int) bool {
	for _, value := range values {
		if value > 0 {
			return true
		}
	}
	return false
}

type builtinDialect struct {
	name         string
	aliases      []string
	capabilities Capabilities
}

func builtinDialects() []Dialect {
	full := Capabilities{
		CreateSchema: true, CreateTable: true, CreateColumn: true,
		PrimaryKeys: true, IdentityColumns: true, Defaults: true, ComputedColumns: true,
		SecondaryIndexes: true, StandalonePrimaryKeys: true, NamedUniqueConstraints: true, CheckConstraints: true,
		IndexExpressionKeys: true, IndexIncludeColumns: true, FilteredIndexes: true,
	}
	return []Dialect{
		builtinDialect{name: "postgres", aliases: []string{"postgresql", "pg"}, capabilities: full},
		builtinDialect{
			name: "mssql", aliases: []string{"sqlserver", "sql-server", "sql_server"},
			capabilities: Capabilities{
				CreateSchema: true, CreateTable: true, CreateColumn: true,
				PrimaryKeys: true, IdentityColumns: true, Defaults: true, ComputedColumns: true,
				SecondaryIndexes: true, StandalonePrimaryKeys: true, NamedUniqueConstraints: true, CheckConstraints: true,
				IndexIncludeColumns: true, FilteredIndexes: true,
			},
		},
		builtinDialect{
			name: "mysql", aliases: []string{"mariadb", "maria"},
			capabilities: Capabilities{
				CreateSchema: true, CreateTable: true, CreateColumn: true,
				PrimaryKeys: true, IdentityColumns: true, Defaults: true, ComputedColumns: true,
				SecondaryIndexes: true, StandalonePrimaryKeys: true, NamedUniqueConstraints: true, CheckConstraints: true,
				IndexExpressionKeys: true, IndexPrefixLengths: true,
			},
		},
		builtinDialect{
			name: "sqlite", aliases: []string{"sqlite3"},
			capabilities: Capabilities{
				CreateTable: true, CreateColumn: true, PrimaryKeys: true, IdentityColumns: true, Defaults: true,
				SecondaryIndexes: true, FilteredIndexes: true,
			},
		},
		builtinDialect{
			name: "clickhouse", aliases: []string{"click-house"},
			capabilities: Capabilities{CreateSchema: true, CreateTable: true, CreateColumn: true, PrimaryKeys: true},
		},
	}
}

func (d builtinDialect) Name() string               { return d.name }
func (d builtinDialect) Aliases() []string          { return append([]string(nil), d.aliases...) }
func (d builtinDialect) Capabilities() Capabilities { return d.capabilities }

func (d builtinDialect) RenderSchema(request Request) (Result, error) {
	renderer, err := d.renderer(request)
	if err != nil {
		return Result{}, err
	}
	sql, err := renderer.CreateSchemaDDL()
	return Result{SQL: sql}, err
}

func (d builtinDialect) RenderTable(request Request, table Table) (Result, error) {
	renderer, err := d.renderer(request)
	if err != nil {
		return Result{}, err
	}
	sql, _, err := renderer.CreateTableDDL(toDriverTable(table))
	if err != nil {
		return Result{}, d.publicRenderError(err)
	}
	result := Result{SQL: sql, Warnings: mappingWarnings(d.name, request, table.Name, table.Columns)}
	if d.name == "clickhouse" && len(table.PrimaryKey) > 0 {
		result.Warnings = append(result.Warnings, Warning{
			Kind:          "primary-key-not-unique",
			Reason:        "ClickHouse PRIMARY KEY is a sparse sorting index and does not enforce uniqueness",
			SourceDialect: sourceDialect(request.SourceDialect),
			TargetDialect: d.name,
			Table:         table.Name,
		})
	}
	if d.name == "sqlite" {
		for _, column := range sqliteUnrepresentableIdentities(table) {
			result.Warnings = append(result.Warnings, Warning{
				Kind:          "sqlite-identity-best-effort",
				Reason:        "SQLite AUTOINCREMENT requires an identity column to be the sole primary-key column; rendered without AUTOINCREMENT",
				SourceDialect: sourceDialect(request.SourceDialect),
				TargetDialect: d.name,
				Table:         table.Name,
				Column:        column.Name,
			})
		}
	}
	return result, nil
}

func (d builtinDialect) RenderColumn(request Request, column Column) (Result, error) {
	if d.name == "sqlite" && column.IsIdentity {
		return Result{}, &UnsupportedFeatureError{
			Dialect: d.name,
			Feature: "standalone identity column definitions; use CreateTable with a sole primary key",
		}
	}
	renderer, err := d.renderer(request)
	if err != nil {
		return Result{}, err
	}
	sql, _, err := renderer.ColumnDefinition(toDriverColumn(column))
	if err != nil {
		return Result{}, d.publicRenderError(err)
	}
	return Result{SQL: sql, Warnings: mappingWarnings(d.name, request, "", []Column{column})}, nil
}

func (d builtinDialect) RenderIndex(request Request, table TableRef, index Index) (Result, error) {
	renderer, err := d.renderer(request)
	if err != nil {
		return Result{}, err
	}
	sql, err := renderer.CreateIndexDDL(toDriverTableRef(table), toDriverIndex(index))
	if err != nil {
		return Result{}, d.publicRenderError(err)
	}
	return Result{SQL: sql}, nil
}

func (d builtinDialect) RenderPrimaryKey(request Request, table TableRef, primaryKey PrimaryKey) (Result, error) {
	renderer, err := d.renderer(request)
	if err != nil {
		return Result{}, err
	}
	sql, err := renderer.CreatePrimaryKeyDDL(toDriverTableRef(table), primaryKey.Name, primaryKey.Columns)
	if err != nil {
		return Result{}, d.publicRenderError(err)
	}
	return Result{SQL: sql}, nil
}

func (d builtinDialect) RenderUniqueConstraint(request Request, table TableRef, unique UniqueConstraint) (Result, error) {
	renderer, err := d.renderer(request)
	if err != nil {
		return Result{}, err
	}
	sql, err := renderer.CreateUniqueConstraintDDL(toDriverTableRef(table), unique.Name, unique.Columns)
	if err != nil {
		return Result{}, d.publicRenderError(err)
	}
	return Result{SQL: sql}, nil
}

func (d builtinDialect) RenderCheckConstraint(request Request, table TableRef, check CheckConstraint) (Result, error) {
	renderer, err := d.renderer(request)
	if err != nil {
		return Result{}, err
	}
	sql, err := renderer.CreateCheckConstraintDDL(toDriverTableRef(table), &driver.CheckConstraint{
		Name:       check.Name,
		Definition: check.Expression,
	})
	if err != nil {
		return Result{}, d.publicRenderError(err)
	}
	return Result{SQL: sql}, nil
}

func (d builtinDialect) publicRenderError(err error) error {
	var composite *ddl.ClickHouseNullableCompositeError
	if errors.As(err, &composite) {
		return &UnsupportedFeatureError{
			Dialect: d.name,
			Feature: fmt.Sprintf("nullable %s column %q", composite.Type, composite.Column),
		}
	}
	var key *ddl.ClickHouseNullableKeyError
	if errors.As(err, &key) {
		return &UnsupportedFeatureError{
			Dialect: d.name,
			Feature: fmt.Sprintf("primary-key/order-by reference to nullable column %q", key.Column),
		}
	}
	return err
}

func (d builtinDialect) renderer(request Request) (ddl.Renderer, error) {
	targetSchema := request.Schema
	if d.name == "sqlite" {
		// SQLite's configured schema is a DMT compatibility input, not a
		// qualifier: the target connection selects the database and CREATE
		// TABLE must remain unqualified.
		targetSchema = ""
	}
	renderer, err := ddl.NewRenderer(d.name, targetSchema, string(request.UnknownTypePolicy))
	if err != nil {
		return ddl.Renderer{}, err
	}
	return renderer.WithSource(request.SourceDialect), nil
}

func toDriverTable(table Table) *driver.Table {
	columns := toDriverColumns(table.Columns)
	return &driver.Table{
		Name:       table.Name,
		Columns:    columns,
		PrimaryKey: append([]string(nil), table.PrimaryKey...),
	}
}

func toDriverTableRef(table TableRef) *driver.Table {
	return &driver.Table{Name: table.Name, Columns: toDriverColumns(table.Columns)}
}

func toDriverColumns(columns []Column) []driver.Column {
	driverColumns := make([]driver.Column, len(columns))
	for i, column := range columns {
		driverColumns[i] = toDriverColumn(column)
	}
	return driverColumns
}

func toDriverIndex(index Index) *driver.Index {
	return &driver.Index{
		Name:                index.Name,
		Columns:             append([]string(nil), index.Columns...),
		ColumnExpressions:   append([]bool(nil), index.ColumnExpressions...),
		ColumnPrefixLengths: append([]int(nil), index.ColumnPrefixLengths...),
		IsUnique:            index.IsUnique,
		IncludeCols:         append([]string(nil), index.IncludeColumns...),
		Filter:              index.Filter,
	}
}

func toDriverColumn(column Column) driver.Column {
	return driver.Column{
		Name:               column.Name,
		DataType:           column.DataType,
		MaxLength:          column.MaxLength,
		Precision:          column.Precision,
		Scale:              column.Scale,
		DatetimePrecision:  column.DatetimePrecision,
		IsNullable:         column.IsNullable,
		IsIdentity:         column.IsIdentity,
		IsUnsigned:         column.IsUnsigned,
		DisplayWidth:       column.DisplayWidth,
		DefaultExpression:  column.DefaultExpression,
		HasDefault:         column.HasDefault,
		OnUpdateExpression: column.OnUpdateExpression,
		IsComputed:         column.IsComputed,
		ComputedExpression: column.ComputedExpression,
		ComputedPersisted:  column.ComputedPersisted,
		EnumValues:         append([]string(nil), column.EnumValues...),
		SRID:               column.SRID,
		SpatialSubType:     column.SpatialSubType,
	}
}

// sqliteUnrepresentableIdentities returns identity columns that cannot use
// SQLite's only identity form, INTEGER PRIMARY KEY AUTOINCREMENT. The core
// renderer deliberately retains DMT's best-effort behavior for those tables:
// it keeps the ordinary column and table-level primary key rather than
// emitting invalid SQLite SQL.
func sqliteUnrepresentableIdentities(table Table) []Column {
	if len(table.PrimaryKey) == 1 {
		for _, column := range table.Columns {
			if column.Name == table.PrimaryKey[0] && column.IsIdentity {
				return nil
			}
		}
	}
	var out []Column
	for _, column := range table.Columns {
		if column.IsIdentity {
			out = append(out, column)
		}
	}
	return out
}

func mappingWarnings(target string, request Request, table string, columns []Column) []Warning {
	var out []Warning
	for _, column := range columns {
		ct := canonical.ToCanonical(column.DataType, column.TypeMeta(), request.SourceDialect)
		_, warnings, err := canonical.FromCanonicalWithWarnings(ct, target, canonical.RenderOpts{IsIdentity: column.IsIdentity})
		for _, warning := range warnings {
			out = append(out, Warning{
				Kind:          warning.Kind,
				Reason:        warning.Reason,
				SourceDialect: sourceDialect(request.SourceDialect),
				TargetDialect: target,
				Table:         table,
				Column:        column.Name,
			})
		}
		if err != nil && request.UnknownTypePolicy != UnknownTypeFail {
			out = append(out, Warning{
				Kind:          "unknown-type-fallback",
				Reason:        fmt.Sprintf("source type %q used the %s fallback", column.DataType, request.UnknownTypePolicy),
				SourceDialect: sourceDialect(request.SourceDialect),
				TargetDialect: target,
				Table:         table,
				Column:        column.Name,
			})
		}
	}
	return out
}

func normalizePolicy(policy UnknownTypePolicy) (UnknownTypePolicy, error) {
	if policy == "" {
		return UnknownTypeFail, nil
	}
	switch policy {
	case UnknownTypeFail, UnknownTypeWarn, UnknownTypeTextFallback:
		return policy, nil
	default:
		return "", fmt.Errorf("unknown type policy %q; want fail, warn, or text_fallback", policy)
	}
}

func normalizedDialectName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func sourceDialect(dialect string) string {
	if normalized := normalizedDialectName(dialect); normalized != "" {
		return normalized
	}
	return "unknown"
}
