package schema

import (
	"errors"
	"fmt"
	"strings"

	"github.com/johndauphine/smt/internal/driver"
	"github.com/johndauphine/smt/internal/fidelity"
)

// FidelityProfile selects an explicitly opt-in source-semantics policy.
type FidelityProfile string

const (
	// FidelityLegacy preserves the existing renderer's exact output and behavior.
	FidelityLegacy FidelityProfile = "legacy"
	// FidelityChecked enforces supported restrictions and reports remaining gaps.
	FidelityChecked FidelityProfile = "checked"
	// FidelityStrict additionally rejects any known unresolved finding.
	FidelityStrict FidelityProfile = "strict"
)

// FidelityOptions configures the new renderer without changing Options or any
// existing public value layout. An empty Profile means legacy. SourceDialect
// comes from Options; omitted source context is never inferred in opt-in mode.
type FidelityOptions struct {
	Profile       FidelityProfile
	SourceContext FidelitySourceContext
}

// FidelitySourceContext supplies companion metadata without changing Table.
// ReservedConstraintNames is keyed by the exact input table name. Supply all
// existing and planned source constraint/index names that share its namespace;
// these names reserve generated-check names and are never rendered as SQL.
// Missing names mean the caller declares no such reserved names. Collation and
// input conversion context remain unknown and are reported as findings.
type FidelitySourceContext struct {
	ReservedConstraintNames map[string][]string
	// UniqueIndexes provides source uniqueness metadata keyed by exact input
	// table name. Every entry must set IsUnique. It is assessed and reserves
	// names, but does not add index/constraint SQL to the create artifact.
	// Omission makes no claim about uniqueness outside the supplied metadata.
	UniqueIndexes map[string][]Index
}

// FidelityFinding describes a known unresolved source/target semantic gap.
// Codes are deterministic; findings do not contain executable SQL.
type FidelityFinding struct {
	Code          string
	Schema        string
	Table         string
	Column        string
	SourceDialect string
	TargetDialect string
	Reason        string
}

// FidelityResult keeps an existing single-statement artifact separate from
// structured fidelity findings. Errors always return an empty artifact.
type FidelityResult struct {
	Artifact Result
	Findings []FidelityFinding
}

// FidelityPlan wraps an ordered create plan; side objects remain caller-owned.
type FidelityPlan struct {
	Artifact Plan
	Findings []FidelityFinding
}

// FidelityBatch wraps an existing evolution batch for explicit legacy use.
// Checked and strict evolution currently return UnsupportedFeatureError.
type FidelityBatch struct {
	Artifact Batch
	Findings []FidelityFinding
}

// FidelityViolationError means strict rendering found unresolved semantics.
// It exposes only public values; no partial executable artifact is returned.
type FidelityViolationError struct {
	Findings []FidelityFinding
}

func (e *FidelityViolationError) Error() string {
	return fmt.Sprintf("strict fidelity rejected %d unresolved finding(s)", len(e.Findings))
}

// FidelityDialect is an optional extension for a custom dialect that implements
// fidelity-aware whole-table rendering. Existing Dialect implementations need
// no new methods. An implementation owns enforcement and deterministic findings.
type FidelityDialect interface {
	Dialect
	FidelityProfiles() []FidelityProfile
	RenderTableWithFidelity(Request, Table, FidelityOptions) (FidelityResult, error)
}

// FidelityRenderer provides an explicit fidelity surface. It deliberately does
// not embed Renderer: unsupported operations must not bypass fidelity checks.
type FidelityRenderer struct {
	renderer      Renderer
	profile       FidelityProfile
	sourceContext FidelitySourceContext
}

// NewFidelityRenderer creates an opt-in built-in renderer. NewRenderer remains
// unchanged. The first checked/strict implementation targets PostgreSQL.
func NewFidelityRenderer(opts Options, fidelityOpts FidelityOptions) (FidelityRenderer, error) {
	return NewRegistry().NewFidelityRenderer(opts, fidelityOpts)
}

// NewFidelityRenderer also supports explicitly opted-in custom dialects.
func (r *Registry) NewFidelityRenderer(opts Options, fidelityOpts FidelityOptions) (FidelityRenderer, error) {
	profile, err := fidelity.ParseProfile(string(fidelityOpts.Profile))
	if err != nil {
		return FidelityRenderer{}, err
	}
	base, err := r.NewRenderer(opts)
	if err != nil {
		return FidelityRenderer{}, err
	}
	result := FidelityRenderer{renderer: base, profile: FidelityProfile(profile), sourceContext: cloneFidelitySourceContext(fidelityOpts.SourceContext)}
	if profile == fidelity.Legacy {
		return result, nil
	}
	for table, indexes := range result.sourceContext.UniqueIndexes {
		for _, index := range indexes {
			if !index.IsUnique {
				return FidelityRenderer{}, fmt.Errorf("fidelity source unique index %q on %q must set IsUnique", index.Name, table)
			}
		}
	}
	if d, ok := base.dialect.(builtinDialect); ok {
		if d.name != "postgres" {
			return FidelityRenderer{}, base.unsupported("fidelity profile " + string(profile))
		}
		internalRenderer, err := d.renderer(base.request)
		if err != nil {
			return FidelityRenderer{}, err
		}
		// Reject unsupported known source pairs even for schema-only requests.
		// Missing source context is assessed when actual columns are supplied.
		if strings.TrimSpace(opts.SourceDialect) != "" {
			if _, err := internalRenderer.AssessTableFidelity(&driver.Table{}, profile); err != nil {
				return FidelityRenderer{}, result.publicError(err)
			}
		}
		return result, nil
	}
	if d, ok := base.dialect.(FidelityDialect); ok {
		for _, supported := range d.FidelityProfiles() {
			if supported == FidelityProfile(profile) {
				return result, nil
			}
		}
	}
	return FidelityRenderer{}, base.unsupported("fidelity profile " + string(profile))
}

// Dialect reports the target's canonical name.
func (r FidelityRenderer) Dialect() string { return r.renderer.Dialect() }

// Profile reports the normalized policy, including legacy for an empty option.
func (r FidelityRenderer) Profile() FidelityProfile { return r.profile }

// CreateSchema renders schema DDL, which has no column-level fidelity rules.
func (r FidelityRenderer) CreateSchema() (FidelityResult, error) {
	artifact, err := r.renderer.CreateSchema()
	if err != nil {
		return FidelityResult{}, err
	}
	return FidelityResult{Artifact: artifact}, nil
}

// CreateTable enforces supported restrictions inside one CREATE TABLE statement.
// Missing source context is a finding in checked mode and a strict failure.
func (r FidelityRenderer) CreateTable(table Table) (FidelityResult, error) {
	if r.profile == FidelityLegacy {
		artifact, err := r.renderer.CreateTable(table)
		if err != nil {
			return FidelityResult{}, err
		}
		return FidelityResult{Artifact: artifact}, nil
	}
	if err := r.renderer.validateTable(table); err != nil {
		return FidelityResult{}, err
	}
	if err := r.validateIdentifiers(table); err != nil {
		return FidelityResult{}, err
	}
	if d, ok := r.renderer.dialect.(builtinDialect); ok {
		renderer, err := d.renderer(r.renderer.request)
		if err != nil {
			return FidelityResult{}, err
		}
		driverTable := toDriverTable(table)
		for _, name := range r.sourceContext.ReservedConstraintNames[table.Name] {
			driverTable.CheckConstraints = append(driverTable.CheckConstraints, driver.CheckConstraint{Name: name})
		}
		for _, index := range r.sourceContext.UniqueIndexes[table.Name] {
			driverTable.Indexes = append(driverTable.Indexes, *toDriverIndex(index))
		}
		sql, assessment, err := renderer.CreateTableWithFidelity(driverTable, fidelity.Profile(r.profile))
		if err != nil {
			return FidelityResult{}, r.publicError(err)
		}
		return FidelityResult{
			Artifact: Result{SQL: sql, Warnings: mappingWarnings(d.name, r.renderer.request, table.Name, table.Columns)},
			Findings: publicFidelityFindings(assessment.Findings),
		}, nil
	}
	d := r.renderer.dialect.(FidelityDialect) // validated at construction
	result, err := d.RenderTableWithFidelity(r.renderer.request, table, FidelityOptions{Profile: r.profile, SourceContext: cloneFidelitySourceContext(r.sourceContext)})
	if err != nil {
		return FidelityResult{}, err
	}
	if r.profile == FidelityStrict && len(result.Findings) > 0 {
		return FidelityResult{}, &FidelityViolationError{Findings: append([]FidelityFinding(nil), result.Findings...)}
	}
	return result, nil
}

// PlanCreate preserves caller table order and returns no partial plan on error.
func (r FidelityRenderer) PlanCreate(tables []Table) (FidelityPlan, error) {
	if r.profile == FidelityLegacy {
		artifact, err := r.renderer.PlanCreate(tables)
		if err != nil {
			return FidelityPlan{}, err
		}
		return FidelityPlan{Artifact: artifact}, nil
	}
	result := FidelityPlan{Artifact: Plan{Statements: make([]Statement, 0, len(tables)+1)}}
	names := make(map[string]string, len(tables))
	for _, table := range tables {
		name := driver.NormalizeIdentifier(r.Dialect(), table.Name)
		if previous, exists := names[name]; exists {
			return FidelityPlan{}, fmt.Errorf("fidelity table identifiers %q and %q collide as %q", previous, table.Name, name)
		}
		names[name] = table.Name
	}
	schemaResult, err := r.CreateSchema()
	if err != nil {
		return FidelityPlan{}, err
	}
	if schemaResult.Artifact.SQL != "" {
		result.Artifact.Statements = append(result.Artifact.Statements, Statement{Kind: StatementCreateSchema, SQL: schemaResult.Artifact.SQL, Warnings: schemaResult.Artifact.Warnings})
	}
	for _, table := range tables {
		got, err := r.CreateTable(table)
		if err != nil {
			return FidelityPlan{}, err
		}
		result.Artifact.Statements = append(result.Artifact.Statements, Statement{Kind: StatementCreateTable, SQL: got.Artifact.SQL, Warnings: append([]Warning(nil), got.Artifact.Warnings...)})
		result.Findings = append(result.Findings, got.Findings...)
	}
	return result, nil
}

func (r FidelityRenderer) validateIdentifiers(table Table) error {
	names := make(map[string]string, len(table.Columns))
	for _, column := range table.Columns {
		name := driver.NormalizeIdentifier(r.Dialect(), column.Name)
		if previous, exists := names[name]; exists {
			return fmt.Errorf("fidelity column identifiers %q and %q in table %q collide as %q", previous, column.Name, table.Name, name)
		}
		names[name] = column.Name
	}
	return nil
}

func cloneFidelitySourceContext(context FidelitySourceContext) FidelitySourceContext {
	var result FidelitySourceContext
	if context.ReservedConstraintNames != nil {
		result.ReservedConstraintNames = make(map[string][]string, len(context.ReservedConstraintNames))
		for table, names := range context.ReservedConstraintNames {
			result.ReservedConstraintNames[table] = append([]string(nil), names...)
		}
	}
	if context.UniqueIndexes != nil {
		result.UniqueIndexes = make(map[string][]Index, len(context.UniqueIndexes))
		for table, indexes := range context.UniqueIndexes {
			copied := append([]Index(nil), indexes...)
			for i := range copied {
				copied[i].Columns = append([]string(nil), copied[i].Columns...)
				copied[i].ColumnExpressions = append([]bool(nil), copied[i].ColumnExpressions...)
				copied[i].ColumnPrefixLengths = append([]int(nil), copied[i].ColumnPrefixLengths...)
				copied[i].IncludeColumns = append([]string(nil), copied[i].IncludeColumns...)
			}
			result.UniqueIndexes[table] = copied
		}
	}
	return result
}

func publicFidelityFindings(findings []fidelity.Finding) []FidelityFinding {
	if findings == nil {
		return nil
	}
	result := make([]FidelityFinding, len(findings))
	for i, finding := range findings {
		result[i] = FidelityFinding{
			Code: finding.Code, Schema: finding.Object.Schema, Table: finding.Object.Table, Column: finding.Object.Column,
			SourceDialect: finding.SourceDialect, TargetDialect: finding.TargetDialect, Reason: finding.Reason,
		}
	}
	return result
}

func (r FidelityRenderer) publicError(err error) error {
	var violation *fidelity.ViolationError
	if errors.As(err, &violation) {
		return &FidelityViolationError{Findings: publicFidelityFindings(violation.Findings)}
	}
	var unsupported *fidelity.UnsupportedError
	if errors.As(err, &unsupported) {
		return r.renderer.unsupported("fidelity: " + unsupported.Reason)
	}
	if d, ok := r.renderer.dialect.(builtinDialect); ok {
		return d.publicRenderError(err)
	}
	return err
}

func (r FidelityRenderer) legacyResult(feature string, render func() (Result, error)) (FidelityResult, error) {
	if r.profile != FidelityLegacy {
		return FidelityResult{}, r.renderer.unsupported("fidelity-aware " + feature)
	}
	artifact, err := render()
	if err != nil {
		return FidelityResult{}, err
	}
	return FidelityResult{Artifact: artifact}, nil
}

func (r FidelityRenderer) legacyBatch(feature string, render func() (Batch, error)) (FidelityBatch, error) {
	if r.profile != FidelityLegacy {
		return FidelityBatch{}, r.renderer.unsupported("fidelity-aware " + feature)
	}
	artifact, err := render()
	if err != nil {
		return FidelityBatch{}, err
	}
	return FidelityBatch{Artifact: artifact}, nil
}

// CreateColumn refuses opt-in standalone rendering because it lacks table-level
// constraint ownership; use CreateTable for supported fidelity enforcement.
func (r FidelityRenderer) CreateColumn(column Column) (FidelityResult, error) {
	return r.legacyResult("column creation", func() (Result, error) { return r.renderer.CreateColumn(column) })
}

// CreateIndex currently supports legacy rendering only.
func (r FidelityRenderer) CreateIndex(table TableRef, index Index) (FidelityResult, error) {
	return r.legacyResult("index creation", func() (Result, error) { return r.renderer.CreateIndex(table, index) })
}

// CreatePrimaryKey currently supports legacy rendering only.
func (r FidelityRenderer) CreatePrimaryKey(table TableRef, key PrimaryKey) (FidelityResult, error) {
	return r.legacyResult("primary-key creation", func() (Result, error) { return r.renderer.CreatePrimaryKey(table, key) })
}

// CreateForeignKey currently supports legacy rendering only.
func (r FidelityRenderer) CreateForeignKey(table TableRef, key ForeignKey) (FidelityResult, error) {
	return r.legacyResult("foreign-key creation", func() (Result, error) { return r.renderer.CreateForeignKey(table, key) })
}

// CreateUniqueConstraint currently supports legacy rendering only.
func (r FidelityRenderer) CreateUniqueConstraint(table TableRef, unique UniqueConstraint) (FidelityResult, error) {
	return r.legacyResult("unique-constraint creation", func() (Result, error) { return r.renderer.CreateUniqueConstraint(table, unique) })
}

// CreateCheckConstraint currently supports legacy rendering only.
func (r FidelityRenderer) CreateCheckConstraint(table TableRef, check CheckConstraint) (FidelityResult, error) {
	return r.legacyResult("check-constraint creation", func() (Result, error) { return r.renderer.CreateCheckConstraint(table, check) })
}

// DropSchema currently supports legacy rendering only.
func (r FidelityRenderer) DropSchema(options DropOptions) (FidelityBatch, error) {
	return r.legacyBatch("schema drops", func() (Batch, error) { return r.renderer.DropSchema(options) })
}

// DropTable currently supports legacy rendering only.
func (r FidelityRenderer) DropTable(table TableRef, options DropOptions) (FidelityBatch, error) {
	return r.legacyBatch("table drops", func() (Batch, error) { return r.renderer.DropTable(table, options) })
}

// DropIndex currently supports legacy rendering only.
func (r FidelityRenderer) DropIndex(table TableRef, name string) (FidelityBatch, error) {
	return r.legacyBatch("index drops", func() (Batch, error) { return r.renderer.DropIndex(table, name) })
}

// DropConstraint currently supports legacy rendering only.
func (r FidelityRenderer) DropConstraint(table TableRef, constraint ConstraintRef) (FidelityBatch, error) {
	return r.legacyBatch("constraint drops", func() (Batch, error) { return r.renderer.DropConstraint(table, constraint) })
}

// AddColumn currently supports legacy rendering only.
func (r FidelityRenderer) AddColumn(table TableRef, column Column) (FidelityBatch, error) {
	return r.legacyBatch("adding columns", func() (Batch, error) { return r.renderer.AddColumn(table, column) })
}

// DropColumn currently supports legacy rendering only.
func (r FidelityRenderer) DropColumn(table TableRef, name string) (FidelityBatch, error) {
	return r.legacyBatch("dropping columns", func() (Batch, error) { return r.renderer.DropColumn(table, name) })
}

// AlterColumnType currently supports legacy rendering only.
func (r FidelityRenderer) AlterColumnType(table TableRef, column Column) (FidelityBatch, error) {
	return r.legacyBatch("altering column types", func() (Batch, error) { return r.renderer.AlterColumnType(table, column) })
}

// AlterColumnNullability currently supports legacy rendering only.
func (r FidelityRenderer) AlterColumnNullability(table TableRef, column Column) (FidelityBatch, error) {
	return r.legacyBatch("altering nullability", func() (Batch, error) { return r.renderer.AlterColumnNullability(table, column) })
}

// SetColumnDefault currently supports legacy rendering only.
func (r FidelityRenderer) SetColumnDefault(table TableRef, column Column) (FidelityBatch, error) {
	return r.legacyBatch("setting defaults", func() (Batch, error) { return r.renderer.SetColumnDefault(table, column) })
}

// DropColumnDefault currently supports legacy rendering only.
func (r FidelityRenderer) DropColumnDefault(table TableRef, name string) (FidelityBatch, error) {
	return r.legacyBatch("dropping defaults", func() (Batch, error) { return r.renderer.DropColumnDefault(table, name) })
}

// TruncateTable currently supports legacy rendering only.
func (r FidelityRenderer) TruncateTable(table TableRef, options TruncateOptions) (FidelityBatch, error) {
	return r.legacyBatch("truncation", func() (Batch, error) { return r.renderer.TruncateTable(table, options) })
}
