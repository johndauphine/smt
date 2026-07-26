package schema

import (
	"fmt"
	"strings"

	"github.com/johndauphine/smt/internal/driver"
)

// Batch is an ordered, executable schema-evolution artifact. Statements must
// be executed in order. When RequiresSingleConnection is true, every
// Statement and, on an error, every Cleanup statement must execute on the
// same physical connection. Cleanup restores session state when a preceding
// statement did not complete (for example MySQL FOREIGN_KEY_CHECKS).
//
// SMT does not execute SQL: the caller owns transactions, retries, and error
// policy. Batch makes the otherwise easy-to-miss connection-affinity contract
// explicit without exposing database-driver types.
type Batch struct {
	Statements               []Statement
	Cleanup                  []Statement
	RequiresSingleConnection bool
	// BestEffortStatementIndexes identifies zero-based entries in Statements
	// whose execution errors are advisory. An executor should record those
	// errors but continue the containing batch. Entries not listed are
	// required. The built-in renderers emit sorted, unique, in-range indexes.
	BestEffortStatementIndexes []int
}

// IsEmpty reports whether b contains no executable statements.
func (b Batch) IsEmpty() bool { return len(b.Statements) == 0 }

// IsBestEffort reports whether the Statement at index has advisory failure
// semantics. Out-of-range indexes are never best effort.
func (b Batch) IsBestEffort(index int) bool {
	for _, bestEffort := range b.BestEffortStatementIndexes {
		if bestEffort == index {
			return true
		}
	}
	return false
}

// EvolutionKind identifies an operation supplied to an EvolutionDialect.
// Applications normally use the corresponding Renderer method; this type is
// public so custom dialect adapters can implement the optional extension.
type EvolutionKind string

const (
	EvolutionDropSchema             EvolutionKind = "drop_schema"
	EvolutionDropTable              EvolutionKind = "drop_table"
	EvolutionDropIndex              EvolutionKind = "drop_index"
	EvolutionDropConstraint         EvolutionKind = "drop_constraint"
	EvolutionAddColumn              EvolutionKind = "add_column"
	EvolutionDropColumn             EvolutionKind = "drop_column"
	EvolutionAlterColumnType        EvolutionKind = "alter_column_type"
	EvolutionAlterColumnNullability EvolutionKind = "alter_column_nullability"
	EvolutionSetColumnDefault       EvolutionKind = "set_column_default"
	EvolutionDropColumnDefault      EvolutionKind = "drop_column_default"
	EvolutionTruncateTable          EvolutionKind = "truncate_table"
)

// EvolutionCapability identifies one public schema-evolution feature. The
// corresponding Renderer method returns UnsupportedFeatureError when the
// selected dialect does not advertise the feature.
type EvolutionCapability string

const (
	EvolutionCapabilityDropSchema             EvolutionCapability = "drop_schema"
	EvolutionCapabilityDropSchemaCascade      EvolutionCapability = "drop_schema_cascade"
	EvolutionCapabilityDropTable              EvolutionCapability = "drop_table"
	EvolutionCapabilityDropTableCascade       EvolutionCapability = "drop_table_cascade"
	EvolutionCapabilityDropIndex              EvolutionCapability = "drop_index"
	EvolutionCapabilityDropConstraint         EvolutionCapability = "drop_constraint"
	EvolutionCapabilityAddColumn              EvolutionCapability = "add_column"
	EvolutionCapabilityDropColumn             EvolutionCapability = "drop_column"
	EvolutionCapabilityAlterColumnType        EvolutionCapability = "alter_column_type"
	EvolutionCapabilityAlterColumnNullability EvolutionCapability = "alter_column_nullability"
	EvolutionCapabilitySetColumnDefault       EvolutionCapability = "set_column_default"
	EvolutionCapabilityDropColumnDefault      EvolutionCapability = "drop_column_default"
	EvolutionCapabilityTruncateTable          EvolutionCapability = "truncate_table"
	EvolutionCapabilityTruncateTableCascade   EvolutionCapability = "truncate_table_cascade"
)

// EvolutionCapabilities is the feature set advertised by an EvolutionDialect.
// It is a slice rather than another fixed-width struct so future capabilities
// can be added without changing existing composite-literal layouts.
type EvolutionCapabilities []EvolutionCapability

// Supports reports whether c advertises capability.
func (c EvolutionCapabilities) Supports(capability EvolutionCapability) bool {
	for _, available := range c {
		if available == capability {
			return true
		}
	}
	return false
}

// DropOptions selects explicit destructive-drop behavior. Cascade is never
// inferred: callers must request it and the target dialect must advertise the
// matching capability.
type DropOptions struct {
	Cascade bool
}

// TruncateOptions selects explicit truncate behavior. Cascade is supported
// only where the selected dialect advertises TruncateTableCascade.
type TruncateOptions struct {
	Cascade bool
}

// ConstraintKind selects the dialect-specific form used to drop a named
// table constraint. A primary-key name is retained for deterministic input
// validation even though MySQL's DROP PRIMARY KEY syntax does not use it.
type ConstraintKind string

const (
	ConstraintPrimaryKey ConstraintKind = "primary_key"
	ConstraintUnique     ConstraintKind = "unique"
	ConstraintForeignKey ConstraintKind = "foreign_key"
	ConstraintCheck      ConstraintKind = "check"
)

// ConstraintRef identifies a named relational constraint on a table.
type ConstraintRef struct {
	Name string
	Kind ConstraintKind
}

// Evolution is the typed public value received by a custom EvolutionDialect.
// Renderer performs operation-specific validation before invoking the
// dialect. Top-level fields not used by Kind are zero; Column contains the
// caller's input, including any optional fields. No SMT internal driver value
// appears in this contract.
type Evolution struct {
	Kind            EvolutionKind
	Table           TableRef
	Column          Column
	Name            string
	Constraint      ConstraintRef
	DropOptions     DropOptions
	TruncateOptions TruncateOptions
}

// EvolutionDialect is the optional public extension for schema mutation DDL.
// It remains separate from Dialect so existing custom create-only and
// side-object implementations remain source compatible.
type EvolutionDialect interface {
	Dialect
	EvolutionCapabilities() EvolutionCapabilities
	RenderEvolution(Request, Evolution) (Batch, error)
}

// EvolutionCapabilities reports the schema-evolution features supported by
// the selected dialect. The returned slice is a copy and may be safely
// modified by the caller. A dialect without EvolutionDialect support returns
// no evolution capabilities.
func (r Renderer) EvolutionCapabilities() EvolutionCapabilities {
	dialect, ok := r.dialect.(EvolutionDialect)
	if !ok {
		return nil
	}
	return append(EvolutionCapabilities(nil), dialect.EvolutionCapabilities()...)
}

// SupportsEvolution reports whether the selected dialect supports capability.
func (r Renderer) SupportsEvolution(capability EvolutionCapability) bool {
	return r.EvolutionCapabilities().Supports(capability)
}

const (
	// StatementSessionSetup changes connection-local state before an operation.
	StatementSessionSetup StatementKind = "session_setup"
	// StatementSessionCleanup restores connection-local state after an operation.
	StatementSessionCleanup StatementKind = "session_cleanup"
	// StatementBestEffortCleanup performs non-critical post-operation cleanup.
	StatementBestEffortCleanup      StatementKind = "best_effort_cleanup"
	StatementDropSchema             StatementKind = "drop_schema"
	StatementDropTable              StatementKind = "drop_table"
	StatementDropIndex              StatementKind = "drop_index"
	StatementDropConstraint         StatementKind = "drop_constraint"
	StatementAddColumn              StatementKind = "add_column"
	StatementDropColumn             StatementKind = "drop_column"
	StatementAlterColumnType        StatementKind = "alter_column_type"
	StatementAlterColumnNullability StatementKind = "alter_column_nullability"
	StatementSetColumnDefault       StatementKind = "set_column_default"
	StatementDropColumnDefault      StatementKind = "drop_column_default"
	StatementTruncateTable          StatementKind = "truncate_table"
)

// DropSchema renders an idempotent drop of Options.Schema. An empty schema is
// a no-op, matching CreateSchema.
func (r Renderer) DropSchema(options DropOptions) (Batch, error) {
	if strings.TrimSpace(r.request.Schema) == "" {
		return Batch{}, nil
	}
	if !r.SupportsEvolution(EvolutionCapabilityDropSchema) {
		return Batch{}, r.unsupported("schema drops")
	}
	if options.Cascade && !r.SupportsEvolution(EvolutionCapabilityDropSchemaCascade) {
		return Batch{}, r.unsupported("schema-drop CASCADE")
	}
	return r.renderEvolution(Evolution{Kind: EvolutionDropSchema, DropOptions: options})
}

// DropTable renders an idempotent table drop. Cascade is an explicit option
// because it may remove dependent objects.
func (r Renderer) DropTable(table TableRef, options DropOptions) (Batch, error) {
	if !r.SupportsEvolution(EvolutionCapabilityDropTable) {
		return Batch{}, r.unsupported("table drops")
	}
	if options.Cascade && !r.SupportsEvolution(EvolutionCapabilityDropTableCascade) {
		return Batch{}, r.unsupported("table-drop CASCADE")
	}
	if err := validateTableRef("table drop", table); err != nil {
		return Batch{}, err
	}
	return r.renderEvolution(Evolution{Kind: EvolutionDropTable, Table: table, DropOptions: options})
}

// DropIndex renders a named-index drop for a table.
func (r Renderer) DropIndex(table TableRef, name string) (Batch, error) {
	if !r.SupportsEvolution(EvolutionCapabilityDropIndex) {
		return Batch{}, r.unsupported("index drops")
	}
	if err := validateTableRef("index drop", table); err != nil {
		return Batch{}, err
	}
	if strings.TrimSpace(name) == "" {
		return Batch{}, fmt.Errorf("render index drop on table %q: empty index name", table.Name)
	}
	return r.renderEvolution(Evolution{Kind: EvolutionDropIndex, Table: table, Name: name})
}

// DropConstraint renders a named primary-key, unique, foreign-key, or check
// constraint drop. SQLite and ClickHouse report a typed unsupported error
// rather than silently proposing a table rebuild.
func (r Renderer) DropConstraint(table TableRef, constraint ConstraintRef) (Batch, error) {
	if !r.SupportsEvolution(EvolutionCapabilityDropConstraint) {
		return Batch{}, r.unsupported("constraint drops")
	}
	if err := validateTableRef("constraint drop", table); err != nil {
		return Batch{}, err
	}
	if strings.TrimSpace(constraint.Name) == "" {
		return Batch{}, fmt.Errorf("render constraint drop on table %q: empty constraint name", table.Name)
	}
	switch constraint.Kind {
	case ConstraintPrimaryKey, ConstraintUnique, ConstraintForeignKey, ConstraintCheck:
	default:
		return Batch{}, fmt.Errorf("render constraint drop %q: invalid constraint kind %q", constraint.Name, constraint.Kind)
	}
	return r.renderEvolution(Evolution{Kind: EvolutionDropConstraint, Table: table, Constraint: constraint})
}

// AddColumn renders a complete ALTER TABLE add-column operation.
func (r Renderer) AddColumn(table TableRef, column Column) (Batch, error) {
	if !r.SupportsEvolution(EvolutionCapabilityAddColumn) {
		return Batch{}, r.unsupported("adding columns")
	}
	if err := validateTableRef("add column", table); err != nil {
		return Batch{}, err
	}
	if err := r.validateColumn(column); err != nil {
		return Batch{}, err
	}
	return r.renderEvolution(Evolution{Kind: EvolutionAddColumn, Table: table, Column: column})
}

// DropColumn renders a data-destructive column drop.
func (r Renderer) DropColumn(table TableRef, name string) (Batch, error) {
	if !r.SupportsEvolution(EvolutionCapabilityDropColumn) {
		return Batch{}, r.unsupported("dropping columns")
	}
	if err := validateTableRef("column drop", table); err != nil {
		return Batch{}, err
	}
	if strings.TrimSpace(name) == "" {
		return Batch{}, fmt.Errorf("render column drop on table %q: empty column name", table.Name)
	}
	return r.renderEvolution(Evolution{Kind: EvolutionDropColumn, Table: table, Name: name})
}

// AlterColumnType renders an in-place type change when the dialect supports
// one. SQLite and ClickHouse report a typed rebuild-required capability error.
func (r Renderer) AlterColumnType(table TableRef, column Column) (Batch, error) {
	if !r.SupportsEvolution(EvolutionCapabilityAlterColumnType) {
		return Batch{}, r.unsupported("altering column types without rebuilding the table")
	}
	if err := validateTableRef("column type change", table); err != nil {
		return Batch{}, err
	}
	if err := r.validateColumn(column); err != nil {
		return Batch{}, err
	}
	return r.renderEvolution(Evolution{Kind: EvolutionAlterColumnType, Table: table, Column: column})
}

// AlterColumnNullability renders an in-place NULL/NOT NULL transition.
// Column.Name and Column.IsNullable are always used. PostgreSQL and custom
// dialects may omit Column.DataType; SQL Server and MySQL require it because
// their ALTER syntax restates the column type.
func (r Renderer) AlterColumnNullability(table TableRef, column Column) (Batch, error) {
	if !r.SupportsEvolution(EvolutionCapabilityAlterColumnNullability) {
		return Batch{}, r.unsupported("altering column nullability without rebuilding the table")
	}
	if err := validateTableRef("column nullability change", table); err != nil {
		return Batch{}, err
	}
	if err := r.validateNullabilityColumn(column); err != nil {
		return Batch{}, err
	}
	return r.renderEvolution(Evolution{Kind: EvolutionAlterColumnNullability, Table: table, Column: column})
}

// SetColumnDefault renders a deterministic default assignment. The column
// must explicitly carry a default, including HasDefault for an intentional
// empty-string default. Column.Name is always required; Column.DataType is
// optional because no builtin dialect restates the type in a SET DEFAULT
// statement. Custom dialects that need DataType should validate it inside
// RenderEvolution.
func (r Renderer) SetColumnDefault(table TableRef, column Column) (Batch, error) {
	if !r.SupportsEvolution(EvolutionCapabilitySetColumnDefault) {
		return Batch{}, r.unsupported("setting column defaults")
	}
	if err := validateTableRef("set column default", table); err != nil {
		return Batch{}, err
	}
	if err := validateSetDefaultColumn(column); err != nil {
		return Batch{}, err
	}
	if !column.HasDefault && strings.TrimSpace(column.DefaultExpression) == "" {
		return Batch{}, fmt.Errorf("render column default %q: no default expression", column.Name)
	}
	return r.renderEvolution(Evolution{Kind: EvolutionSetColumnDefault, Table: table, Column: column})
}

// DropColumnDefault renders a deterministic default removal.
func (r Renderer) DropColumnDefault(table TableRef, name string) (Batch, error) {
	if !r.SupportsEvolution(EvolutionCapabilityDropColumnDefault) {
		return Batch{}, r.unsupported("dropping column defaults")
	}
	if err := validateTableRef("drop column default", table); err != nil {
		return Batch{}, err
	}
	if strings.TrimSpace(name) == "" {
		return Batch{}, fmt.Errorf("render column default drop on table %q: empty column name", table.Name)
	}
	return r.renderEvolution(Evolution{Kind: EvolutionDropColumnDefault, Table: table, Name: name})
}

// TruncateTable renders a data-clearing operation. SQLite uses DELETE and a
// deterministic sqlite_sequence cleanup; MySQL additionally declares the
// required same-connection foreign-key-check sequence.
func (r Renderer) TruncateTable(table TableRef, options TruncateOptions) (Batch, error) {
	if !r.SupportsEvolution(EvolutionCapabilityTruncateTable) {
		return Batch{}, r.unsupported("truncating tables")
	}
	if options.Cascade && !r.SupportsEvolution(EvolutionCapabilityTruncateTableCascade) {
		return Batch{}, r.unsupported("truncate CASCADE")
	}
	if err := validateTableRef("truncate table", table); err != nil {
		return Batch{}, err
	}
	return r.renderEvolution(Evolution{Kind: EvolutionTruncateTable, Table: table, TruncateOptions: options})
}

func (r Renderer) validateNullabilityColumn(column Column) error {
	if strings.TrimSpace(column.Name) == "" {
		return fmt.Errorf("render column: empty column name")
	}
	if dialect, ok := r.dialect.(builtinDialect); ok &&
		(dialect.name == "mssql" || dialect.name == "mysql") &&
		strings.TrimSpace(column.DataType) == "" {
		return fmt.Errorf("render column %q: empty source data type", column.Name)
	}
	return nil
}

// validateSetDefaultColumn validates that column has the minimum fields required
// for SET DEFAULT operations. Only column.Name is required: no builtin dialect
// restates the column type in a SET DEFAULT statement. Custom dialects that
// need DataType should validate it inside RenderEvolution.
func validateSetDefaultColumn(column Column) error {
	if strings.TrimSpace(column.Name) == "" {
		return fmt.Errorf("render column: empty column name")
	}
	return nil
}

func (r Renderer) renderEvolution(operation Evolution) (Batch, error) {
	dialect, ok := r.dialect.(EvolutionDialect)
	if !ok {
		return Batch{}, r.unsupported(string(operation.Kind))
	}
	return dialect.RenderEvolution(r.request, operation)
}

func (d builtinDialect) RenderEvolution(request Request, operation Evolution) (Batch, error) {
	if feature := d.unsupportedEvolutionFeature(operation); feature != "" {
		return Batch{}, &UnsupportedFeatureError{Dialect: d.name, Feature: feature}
	}
	renderer, err := d.renderer(request)
	if err != nil {
		return Batch{}, err
	}
	batch := func(kind StatementKind, sql string) Batch {
		if strings.TrimSpace(sql) == "" {
			return Batch{}
		}
		return Batch{Statements: []Statement{{Kind: kind, SQL: sql}}}
	}
	sessionBatch := func(kind StatementKind, sql, setup, cleanup string) Batch {
		return Batch{
			Statements: []Statement{
				{Kind: StatementSessionSetup, SQL: setup},
				{Kind: kind, SQL: sql},
				{Kind: StatementSessionCleanup, SQL: cleanup},
			},
			Cleanup:                  []Statement{{Kind: StatementSessionCleanup, SQL: cleanup}},
			RequiresSingleConnection: true,
		}
	}

	switch operation.Kind {
	case EvolutionDropSchema:
		sql, err := renderer.DropSchemaDDL(operation.DropOptions.Cascade)
		if err != nil {
			return Batch{}, err
		}
		return batch(StatementDropSchema, sql), nil
	case EvolutionDropTable:
		sql, err := renderer.DropTableWithOptionsDDL(operation.Table.Name, operation.DropOptions.Cascade)
		if err != nil {
			return Batch{}, err
		}
		switch d.name {
		case "mysql":
			return sessionBatch(StatementDropTable, sql, "SET FOREIGN_KEY_CHECKS = 0", "SET FOREIGN_KEY_CHECKS = 1"), nil
		case "sqlite":
			return sessionBatch(StatementDropTable, sql, "PRAGMA foreign_keys = OFF", "PRAGMA foreign_keys = ON"), nil
		default:
			return batch(StatementDropTable, sql), nil
		}
	case EvolutionDropIndex:
		return batch(StatementDropIndex, renderer.DropIndexDDL(operation.Table.Name, operation.Name)), nil
	case EvolutionDropConstraint:
		sql, err := renderer.DropConstraintDDL(operation.Table.Name, operation.Constraint.Name, string(operation.Constraint.Kind))
		if err != nil {
			return Batch{}, err
		}
		return batch(StatementDropConstraint, sql), nil
	case EvolutionAddColumn:
		sql, err := renderer.AddColumnDDL(operation.Table.Name, toDriverColumn(operation.Column), toDriverColumns(operation.Table.Columns))
		if err != nil {
			return Batch{}, err
		}
		return Batch{Statements: []Statement{{
			Kind: StatementAddColumn, SQL: sql,
			Warnings: mappingWarnings(d.name, request, operation.Table.Name, []Column{operation.Column}),
		}}}, nil
	case EvolutionDropColumn:
		return batch(StatementDropColumn, renderer.DropColumnDDL(operation.Table.Name, operation.Name)), nil
	case EvolutionAlterColumnType:
		sql, err := renderer.AlterColumnTypeDDL(operation.Table.Name, toDriverColumn(operation.Column))
		if err != nil {
			return Batch{}, err
		}
		return Batch{Statements: []Statement{{
			Kind: StatementAlterColumnType, SQL: sql,
			Warnings: mappingWarnings(d.name, request, operation.Table.Name, []Column{operation.Column}),
		}}}, nil
	case EvolutionAlterColumnNullability:
		sql, err := renderer.AlterColumnNullabilityDDL(operation.Table.Name, toDriverColumn(operation.Column))
		if err != nil {
			return Batch{}, err
		}
		return batch(StatementAlterColumnNullability, sql), nil
	case EvolutionSetColumnDefault:
		sql, err := renderer.SetColumnDefaultDDL(operation.Table.Name, toDriverColumn(operation.Column))
		if err != nil {
			return Batch{}, err
		}
		if d.name == "mssql" {
			// SQL Server permits only one DEFAULT constraint per column. The
			// existing constraint is catalog-named, so drop it first on the
			// same connection before adding this renderer's deterministic name.
			return Batch{
				Statements: []Statement{
					{Kind: StatementDropColumnDefault, SQL: renderer.DropColumnDefaultDDL(operation.Table.Name, operation.Column.Name)},
					{Kind: StatementSetColumnDefault, SQL: sql},
				},
				RequiresSingleConnection: true,
			}, nil
		}
		return batch(StatementSetColumnDefault, sql), nil
	case EvolutionDropColumnDefault:
		return batch(StatementDropColumnDefault, renderer.DropColumnDefaultDDL(operation.Table.Name, operation.Name)), nil
	case EvolutionTruncateTable:
		sql, err := renderer.TruncateTableDDL(operation.Table.Name, operation.TruncateOptions.Cascade)
		if err != nil {
			return Batch{}, err
		}
		switch d.name {
		case "mysql":
			return sessionBatch(StatementTruncateTable, sql, "SET FOREIGN_KEY_CHECKS = 0", "SET FOREIGN_KEY_CHECKS = 1"), nil
		case "sqlite":
			cleanup := fmt.Sprintf("DELETE FROM sqlite_sequence WHERE name = '%s'", strings.ReplaceAll(driver.NormalizeIdentifier(d.name, operation.Table.Name), "'", "''"))
			return Batch{Statements: []Statement{
				{Kind: StatementTruncateTable, SQL: sql},
				{Kind: StatementBestEffortCleanup, SQL: cleanup},
			}, BestEffortStatementIndexes: []int{1}}, nil
		default:
			return batch(StatementTruncateTable, sql), nil
		}
	default:
		return Batch{}, fmt.Errorf("render evolution: unknown operation %q", operation.Kind)
	}
}

func (d builtinDialect) unsupportedEvolutionFeature(operation Evolution) string {
	caps := d.EvolutionCapabilities()
	switch operation.Kind {
	case EvolutionDropSchema:
		if !caps.Supports(EvolutionCapabilityDropSchema) {
			return "schema drops"
		}
		if operation.DropOptions.Cascade && !caps.Supports(EvolutionCapabilityDropSchemaCascade) {
			return "schema-drop CASCADE"
		}
	case EvolutionDropTable:
		if !caps.Supports(EvolutionCapabilityDropTable) {
			return "table drops"
		}
		if operation.DropOptions.Cascade && !caps.Supports(EvolutionCapabilityDropTableCascade) {
			return "table-drop CASCADE"
		}
	case EvolutionDropIndex:
		if !caps.Supports(EvolutionCapabilityDropIndex) {
			return "index drops"
		}
	case EvolutionDropConstraint:
		if !caps.Supports(EvolutionCapabilityDropConstraint) {
			return "constraint drops"
		}
	case EvolutionAddColumn:
		if !caps.Supports(EvolutionCapabilityAddColumn) {
			return "adding columns"
		}
	case EvolutionDropColumn:
		if !caps.Supports(EvolutionCapabilityDropColumn) {
			return "dropping columns"
		}
	case EvolutionAlterColumnType:
		if !caps.Supports(EvolutionCapabilityAlterColumnType) {
			return "altering column types without rebuilding the table"
		}
	case EvolutionAlterColumnNullability:
		if !caps.Supports(EvolutionCapabilityAlterColumnNullability) {
			return "altering column nullability without rebuilding the table"
		}
	case EvolutionSetColumnDefault:
		if !caps.Supports(EvolutionCapabilitySetColumnDefault) {
			return "setting column defaults"
		}
	case EvolutionDropColumnDefault:
		if !caps.Supports(EvolutionCapabilityDropColumnDefault) {
			return "dropping column defaults"
		}
	case EvolutionTruncateTable:
		if !caps.Supports(EvolutionCapabilityTruncateTable) {
			return "truncating tables"
		}
		if operation.TruncateOptions.Cascade && !caps.Supports(EvolutionCapabilityTruncateTableCascade) {
			return "truncate CASCADE"
		}
	}
	return ""
}
