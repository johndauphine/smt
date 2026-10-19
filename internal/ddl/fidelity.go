package ddl

import (
	"strings"

	"github.com/johndauphine/smt/internal/ddl/postgres"
	"github.com/johndauphine/smt/internal/driver"
	"github.com/johndauphine/smt/internal/expr"
	"github.com/johndauphine/smt/internal/fidelity"
)

// AssessTableFidelity assesses the opt-in contract without changing legacy type
// mapping. A legacy request has no new findings or execution requirements.
func (r Renderer) AssessTableFidelity(table *driver.Table, profile fidelity.Profile) (fidelity.Assessment, error) {
	profile, err := fidelity.ParseProfile(string(profile))
	if err != nil {
		return fidelity.Assessment{}, err
	}
	if profile == fidelity.Legacy {
		return fidelity.Assessment{}, nil
	}
	a, err := fidelity.AssessTable(table, r.source, r.target)
	if err != nil {
		return a, err
	}
	return a, fidelity.Gate(profile, a)
}

// FidelityChecks returns the assessed restrictions with stable target names.
// All source constraint names must be present to reserve their namespace.
func (r Renderer) FidelityChecks(table *driver.Table, profile fidelity.Profile) ([]fidelity.Check, fidelity.Assessment, error) {
	a, err := r.AssessTableFidelity(table, profile)
	if err != nil {
		return nil, a, err
	}
	reserved := []string{driver.NormalizeIdentifier(r.target, "pk_"+driver.NormalizeIdentifier(r.target, table.Name))}
	for _, c := range table.CheckConstraints {
		reserved = append(reserved, c.Name)
	}
	for _, c := range table.ForeignKeys {
		reserved = append(reserved, c.Name)
	}
	for _, i := range table.Indexes {
		reserved = append(reserved, i.Name)
	}
	checks, err := fidelity.RenderChecks(a, reserved)
	return checks, a, err
}

// CreateTableWithFidelity retains the legacy SQL contract unless callers
// explicitly select a new profile. Errors never return executable SQL.
func (r Renderer) CreateTableWithFidelity(table *driver.Table, profile fidelity.Profile) (string, fidelity.Assessment, error) {
	profile, err := fidelity.ParseProfile(string(profile))
	if err != nil {
		return "", fidelity.Assessment{}, err
	}
	if profile == fidelity.Legacy {
		sql, _, err := r.CreateTableDDL(table)
		return sql, fidelity.Assessment{}, err
	}
	checks, assessment, err := r.FidelityChecks(table, profile)
	if err != nil {
		return "", assessment, err
	}
	renderTable, err := r.withBinaryLiteralDefaults(table)
	if err != nil {
		return "", assessment, err
	}
	sql, _, err := postgres.RenderCreateTableWithFidelityChecks(renderTable, r.schema, r.unknownTypePolicy, r.source, checks)
	if err != nil {
		return "", assessment, err
	}
	return sql, assessment, nil
}

func (r Renderer) withBinaryLiteralDefaults(table *driver.Table) (*driver.Table, error) {
	result := *table
	result.Columns = append([]driver.Column(nil), table.Columns...)
	if driver.Canonicalize(r.source) != "mssql" || r.target != "postgres" {
		return &result, nil
	}
	for i, column := range result.Columns {
		kind := strings.ToLower(strings.TrimSpace(column.DataType))
		if (kind != "binary" && kind != "varbinary") || !driver.ColumnHasDefault(column) {
			continue
		}
		literal, err := expr.ParseBinaryLiteral(column.DefaultExpression, r.source)
		if err != nil {
			return nil, &fidelity.UnsupportedError{SourceDialect: r.source, TargetDialect: r.target, Reason: "binary default for " + table.Name + "." + column.Name + " is not a supported byte literal or NULL"}
		}
		rendered, err := expr.RenderBinaryLiteral(literal, r.target)
		if err != nil {
			return nil, err
		}
		result.Columns[i].DefaultExpressionOverride = rendered
	}
	return &result, nil
}
