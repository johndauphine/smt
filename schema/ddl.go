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

	"smt/internal/ddl"
	"smt/internal/driver"
	"smt/schema/canonical"
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
	CreateSchema    bool
	CreateTable     bool
	CreateColumn    bool
	PrimaryKeys     bool
	IdentityColumns bool
	Defaults        bool
	ComputedColumns bool
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

// Table is the practical create-table subset shared by all built-in dialects.
// Secondary indexes and relationships remain SMT application-level planning
// concerns; callers can render their tables first and emit those objects in an
// explicit dependency order.
type Table struct {
	Name       string
	Columns    []Column
	PrimaryKey []string
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

type builtinDialect struct {
	name         string
	aliases      []string
	capabilities Capabilities
}

func builtinDialects() []Dialect {
	full := Capabilities{
		CreateSchema: true, CreateTable: true, CreateColumn: true,
		PrimaryKeys: true, IdentityColumns: true, Defaults: true, ComputedColumns: true,
	}
	return []Dialect{
		builtinDialect{name: "postgres", aliases: []string{"postgresql", "pg"}, capabilities: full},
		builtinDialect{name: "mssql", aliases: []string{"sqlserver", "sql-server", "sql_server"}, capabilities: full},
		builtinDialect{name: "mysql", aliases: []string{"mariadb", "maria"}, capabilities: full},
		builtinDialect{
			name: "sqlite", aliases: []string{"sqlite3"},
			capabilities: Capabilities{CreateTable: true, CreateColumn: true, PrimaryKeys: true, Defaults: true},
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
		return Result{}, err
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
	return result, nil
}

func (d builtinDialect) RenderColumn(request Request, column Column) (Result, error) {
	renderer, err := d.renderer(request)
	if err != nil {
		return Result{}, err
	}
	sql, _, err := renderer.ColumnDefinition(toDriverColumn(column))
	if err != nil {
		return Result{}, err
	}
	return Result{SQL: sql, Warnings: mappingWarnings(d.name, request, "", []Column{column})}, nil
}

func (d builtinDialect) renderer(request Request) (ddl.Renderer, error) {
	renderer, err := ddl.NewRenderer(d.name, request.Schema, string(request.UnknownTypePolicy))
	if err != nil {
		return ddl.Renderer{}, err
	}
	return renderer.WithSource(request.SourceDialect), nil
}

func toDriverTable(table Table) *driver.Table {
	columns := make([]driver.Column, len(table.Columns))
	for i, column := range table.Columns {
		columns[i] = toDriverColumn(column)
	}
	return &driver.Table{
		Name:       table.Name,
		Columns:    columns,
		PrimaryKey: append([]string(nil), table.PrimaryKey...),
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

func mappingWarnings(target string, request Request, table string, columns []Column) []Warning {
	var out []Warning
	for _, column := range columns {
		ct := canonical.ToCanonical(column.DataType, canonical.TypeMeta{
			MaxLength:         column.MaxLength,
			Precision:         column.Precision,
			Scale:             column.Scale,
			DatetimePrecision: column.DatetimePrecision,
			IsUnsigned:        column.IsUnsigned,
			DisplayWidth:      column.DisplayWidth,
			EnumValues:        column.EnumValues,
			SRID:              column.SRID,
			SpatialSubType:    column.SpatialSubType,
		}, request.SourceDialect)
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
