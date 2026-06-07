package schemadiff

import (
	"fmt"
	"strings"

	"smt/internal/driver"
	pgddl "smt/internal/driver/postgres"
)

// RenderDeterministic converts a structural diff into a local, deterministic
// DDL plan. PostgreSQL is the first supported target because that is SMT's
// primary target path and the first deterministic DDL renderer.
func RenderDeterministic(diff Diff, targetSchema, targetDialect string) (Plan, error) {
	return RenderDeterministicWithUnknownTypePolicy(diff, targetSchema, targetDialect, "")
}

func RenderDeterministicWithUnknownTypePolicy(diff Diff, targetSchema, targetDialect, unknownTypePolicy string) (Plan, error) {
	if diff.IsEmpty() {
		return Plan{}, nil
	}
	if !isPostgresDialect(targetDialect) {
		return Plan{}, fmt.Errorf("deterministic schema diff rendering currently supports postgres targets, got %q", targetDialect)
	}

	renderer := deterministicPostgresRenderer{schema: targetSchema, dialect: pgddl.Dialect{}, unknownTypePolicy: unknownTypePolicy}
	return renderer.render(diff)
}

type deterministicPostgresRenderer struct {
	schema            string
	dialect           pgddl.Dialect
	unknownTypePolicy string
}

func (r deterministicPostgresRenderer) render(diff Diff) (Plan, error) {
	var plan Plan

	for _, t := range diff.AddedTables {
		if err := r.renderAddedTableDefinition(&plan, t); err != nil {
			return Plan{}, err
		}
	}
	if err := r.renderAddedTableSideObjects(&plan, diff.AddedTables); err != nil {
		return Plan{}, err
	}

	for _, td := range diff.ChangedTables {
		if err := r.renderTableDiff(&plan, td); err != nil {
			return Plan{}, err
		}
	}

	for _, t := range diff.RemovedTables {
		plan.Statements = append(plan.Statements, Statement{
			Table:       t.Name,
			Description: fmt.Sprintf("drop table %s", t.Name),
			SQL:         fmt.Sprintf("DROP TABLE %s", r.qualify(t.Name)),
			Risk:        RiskDataLoss,
			RiskNotes:   "drops the table and its data",
		})
	}

	return plan, nil
}

func (r deterministicPostgresRenderer) renderAddedTableDefinition(plan *Plan, t driver.Table) error {
	ddl, _, err := pgddl.RenderCreateTableDDLWithPolicy(&t, r.schema, false, r.unknownTypePolicy)
	if err != nil {
		return err
	}
	plan.Statements = append(plan.Statements, Statement{
		Table:       t.Name,
		Description: fmt.Sprintf("create table %s", t.Name),
		SQL:         ddl,
		Risk:        RiskSafe,
	})
	return nil
}

func (r deterministicPostgresRenderer) renderAddedTableSideObjects(plan *Plan, tables []driver.Table) error {
	for _, t := range tables {
		for _, idx := range t.Indexes {
			if err := r.renderAddedIndex(plan, t.Name, idx); err != nil {
				return err
			}
		}
	}
	for _, t := range tables {
		for _, chk := range t.CheckConstraints {
			if err := r.renderAddedCheck(plan, t.Name, chk); err != nil {
				return err
			}
		}
	}
	for _, t := range tables {
		for _, fk := range t.ForeignKeys {
			if err := r.renderAddedForeignKey(plan, t.Name, fk); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r deterministicPostgresRenderer) renderTableDiff(plan *Plan, td TableDiff) error {
	tableName := td.Name

	for _, fk := range td.RemovedForeignKeys {
		plan.Statements = append(plan.Statements, Statement{
			Table:       tableName,
			Description: fmt.Sprintf("drop foreign key %s", fk.Name),
			SQL:         fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", r.qualify(tableName), r.quote(fk.Name)),
			Risk:        RiskSafe,
		})
	}
	for _, chk := range td.RemovedChecks {
		plan.Statements = append(plan.Statements, Statement{
			Table:       tableName,
			Description: fmt.Sprintf("drop check constraint %s", chk.Name),
			SQL:         fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", r.qualify(tableName), r.quote(chk.Name)),
			Risk:        RiskSafe,
		})
	}
	for _, idx := range td.RemovedIndexes {
		plan.Statements = append(plan.Statements, Statement{
			Table:       tableName,
			Description: fmt.Sprintf("drop index %s", idx.Name),
			SQL:         fmt.Sprintf("DROP INDEX %s.%s", r.dialect.QuoteIdentifier(r.schema), r.quote(idx.Name)),
			Risk:        RiskSafe,
		})
	}

	for _, c := range td.AddedColumns {
		def, err := pgddl.RenderColumnDefinitionWithPolicy(c, r.unknownTypePolicy)
		if err != nil {
			return err
		}
		risk := RiskSafe
		notes := ""
		if !c.IsNullable && strings.TrimSpace(c.DefaultExpression) == "" && !c.IsIdentity {
			risk = RiskBlocking
			notes = "adding a NOT NULL column without a default may fail or require a table rewrite on non-empty tables"
		}
		plan.Statements = append(plan.Statements, Statement{
			Table:       tableName,
			Description: fmt.Sprintf("add column %s", c.Name),
			SQL:         fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", r.qualify(tableName), def),
			Risk:        risk,
			RiskNotes:   notes,
		})
	}

	for _, cc := range td.ChangedColumns {
		if err := r.renderColumnChange(plan, tableName, cc); err != nil {
			return err
		}
	}

	for _, c := range td.RemovedColumns {
		plan.Statements = append(plan.Statements, Statement{
			Table:       tableName,
			Description: fmt.Sprintf("drop column %s", c.Name),
			SQL:         fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", r.qualify(tableName), r.quote(c.Name)),
			Risk:        RiskDataLoss,
			RiskNotes:   "drops column data",
		})
	}

	for _, idx := range td.AddedIndexes {
		if err := r.renderAddedIndex(plan, tableName, idx); err != nil {
			return err
		}
	}
	for _, fk := range td.AddedForeignKeys {
		if err := r.renderAddedForeignKey(plan, tableName, fk); err != nil {
			return err
		}
	}
	for _, chk := range td.AddedChecks {
		if err := r.renderAddedCheck(plan, tableName, chk); err != nil {
			return err
		}
	}

	return nil
}

func (r deterministicPostgresRenderer) renderColumnChange(plan *Plan, tableName string, cc ColumnChange) error {
	if cc.Old.IsComputed || cc.New.IsComputed {
		return fmt.Errorf("computed column %s changes are not supported by deterministic PostgreSQL sync", cc.Name)
	}

	oldType, err := pgddl.RenderColumnTypeWithPolicy(cc.Old, r.unknownTypePolicy)
	if err != nil {
		return err
	}
	newType, err := pgddl.RenderColumnTypeWithPolicy(cc.New, r.unknownTypePolicy)
	if err != nil {
		return err
	}

	oldDefault := strings.TrimSpace(cc.Old.DefaultExpression)
	newDefault := strings.TrimSpace(cc.New.DefaultExpression)
	typeChanged := oldType != newType
	defaultChanged := oldDefault != newDefault
	preDropDefault := typeChanged && oldDefault != ""

	if preDropDefault {
		plan.Statements = append(plan.Statements, Statement{
			Table:       tableName,
			Description: fmt.Sprintf("drop column %s default", cc.Name),
			SQL:         fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT", r.qualify(tableName), r.quote(cc.Name)),
			Risk:        RiskSafe,
		})
	}

	if typeChanged {
		plan.Statements = append(plan.Statements, Statement{
			Table:       tableName,
			Description: fmt.Sprintf("change column %s type", cc.Name),
			SQL:         fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", r.qualify(tableName), r.quote(cc.Name), newType),
			Risk:        RiskRebuildNeeded,
			RiskNotes:   "type changes may rewrite the table and can fail if existing values cannot be cast",
		})
	}
	if cc.Old.IsNullable != cc.New.IsNullable {
		action := "DROP NOT NULL"
		risk := RiskSafe
		notes := ""
		if !cc.New.IsNullable {
			action = "SET NOT NULL"
			risk = RiskBlocking
			notes = "setting NOT NULL validates existing rows"
		}
		plan.Statements = append(plan.Statements, Statement{
			Table:       tableName,
			Description: fmt.Sprintf("change column %s nullability", cc.Name),
			SQL:         fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s %s", r.qualify(tableName), r.quote(cc.Name), action),
			Risk:        risk,
			RiskNotes:   notes,
		})
	}

	if defaultChanged || preDropDefault {
		if newDefault == "" {
			if !preDropDefault {
				plan.Statements = append(plan.Statements, Statement{
					Table:       tableName,
					Description: fmt.Sprintf("drop column %s default", cc.Name),
					SQL:         fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT", r.qualify(tableName), r.quote(cc.Name)),
					Risk:        RiskSafe,
				})
			}
		} else {
			def, err := pgddl.RenderColumnDefaultDDLWithPolicy(cc.New, r.unknownTypePolicy)
			if err != nil {
				return err
			}
			plan.Statements = append(plan.Statements, Statement{
				Table:       tableName,
				Description: fmt.Sprintf("change column %s default", cc.Name),
				SQL:         fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s", r.qualify(tableName), r.quote(cc.Name), def),
				Risk:        RiskSafe,
			})
		}
	}
	return nil
}

func (r deterministicPostgresRenderer) renderAddedIndex(plan *Plan, tableName string, idx driver.Index) error {
	t := driver.Table{Name: tableName}
	ddl, err := pgddl.RenderCreateIndexDDL(&t, &idx, r.schema)
	if err != nil {
		return err
	}
	plan.Statements = append(plan.Statements, Statement{
		Table:       tableName,
		Description: fmt.Sprintf("create index %s", idx.Name),
		SQL:         ddl,
		Risk:        RiskBlocking,
		RiskNotes:   "index creation can lock or scan the table",
	})
	return nil
}

func (r deterministicPostgresRenderer) renderAddedForeignKey(plan *Plan, tableName string, fk driver.ForeignKey) error {
	t := driver.Table{Name: tableName}
	ddl, err := pgddl.RenderCreateForeignKeyDDL(&t, &fk, r.schema)
	if err != nil {
		return err
	}
	plan.Statements = append(plan.Statements, Statement{
		Table:       tableName,
		Description: fmt.Sprintf("create foreign key %s", fk.Name),
		SQL:         ddl,
		Risk:        RiskBlocking,
		RiskNotes:   "foreign key validation can scan existing rows",
	})
	return nil
}

func (r deterministicPostgresRenderer) renderAddedCheck(plan *Plan, tableName string, chk driver.CheckConstraint) error {
	t := driver.Table{Name: tableName}
	ddl, err := pgddl.RenderCreateCheckConstraintDDL(&t, &chk, r.schema)
	if err != nil {
		return err
	}
	plan.Statements = append(plan.Statements, Statement{
		Table:       tableName,
		Description: fmt.Sprintf("create check constraint %s", chk.Name),
		SQL:         ddl,
		Risk:        RiskBlocking,
		RiskNotes:   "check validation can scan existing rows",
	})
	return nil
}

func (r deterministicPostgresRenderer) qualify(table string) string {
	return r.dialect.QualifyTable(r.schema, driver.NormalizeIdentifier("postgres", table))
}

func (r deterministicPostgresRenderer) quote(name string) string {
	return r.dialect.QuoteIdentifier(driver.NormalizeIdentifier("postgres", name))
}

func isPostgresDialect(dialect string) bool {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "postgres", "postgresql", "pg":
		return true
	default:
		return false
	}
}
