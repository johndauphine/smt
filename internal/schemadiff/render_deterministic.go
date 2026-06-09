package schemadiff

import (
	"fmt"
	"strings"

	"smt/internal/ddl"
	"smt/internal/driver"
)

// RenderDeterministic converts a structural diff into a local, deterministic
// DDL plan for the target dialect.
func RenderDeterministic(diff Diff, targetSchema, targetDialect string) (Plan, error) {
	return RenderDeterministicWithUnknownTypePolicy(diff, targetSchema, targetDialect, "")
}

func RenderDeterministicWithUnknownTypePolicy(diff Diff, targetSchema, targetDialect, unknownTypePolicy string) (Plan, error) {
	if diff.IsEmpty() {
		return Plan{}, nil
	}

	renderer, err := newDeterministicRenderer(targetDialect, targetSchema, unknownTypePolicy)
	if err != nil {
		return Plan{}, err
	}
	return renderer.render(diff)
}

type deterministicRenderer struct {
	target   string
	renderer ddl.Renderer
}

func newDeterministicRenderer(targetDialect, targetSchema, unknownTypePolicy string) (deterministicRenderer, error) {
	r, err := ddl.NewRenderer(targetDialect, targetSchema, unknownTypePolicy)
	if err != nil {
		return deterministicRenderer{}, err
	}
	return deterministicRenderer{target: r.Target(), renderer: r}, nil
}

func (r deterministicRenderer) render(diff Diff) (Plan, error) {
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
			SQL:         r.renderer.DropTableDDL(t.Name),
			Risk:        RiskDataLoss,
			RiskNotes:   "drops the table and its data",
		})
	}

	return plan, nil
}

func (r deterministicRenderer) renderAddedTableDefinition(plan *Plan, t driver.Table) error {
	sql, _, err := r.renderer.CreateTableDDL(&t)
	if err != nil {
		return err
	}
	plan.Statements = append(plan.Statements, Statement{
		Table:       t.Name,
		Description: fmt.Sprintf("create table %s", t.Name),
		SQL:         sql,
		Risk:        RiskSafe,
	})
	return nil
}

func (r deterministicRenderer) renderAddedTableSideObjects(plan *Plan, tables []driver.Table) error {
	for _, t := range tables {
		for _, idx := range t.Indexes {
			if err := r.renderAddedIndex(plan, t, idx); err != nil {
				return err
			}
		}
	}
	for _, t := range tables {
		for _, chk := range t.CheckConstraints {
			if err := r.renderAddedCheck(plan, t, chk); err != nil {
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

func (r deterministicRenderer) renderTableDiff(plan *Plan, td TableDiff) error {
	tableName := td.Name

	for _, fk := range td.RemovedForeignKeys {
		plan.Statements = append(plan.Statements, Statement{
			Table:       tableName,
			Description: fmt.Sprintf("drop foreign key %s", fk.Name),
			SQL:         r.renderer.DropForeignKeyDDL(tableName, fk.Name),
			Risk:        RiskSafe,
		})
	}
	for _, chk := range td.RemovedChecks {
		plan.Statements = append(plan.Statements, Statement{
			Table:       tableName,
			Description: fmt.Sprintf("drop check constraint %s", chk.Name),
			SQL:         r.renderer.DropCheckDDL(tableName, chk.Name),
			Risk:        RiskSafe,
		})
	}
	for _, idx := range td.RemovedIndexes {
		plan.Statements = append(plan.Statements, Statement{
			Table:       tableName,
			Description: fmt.Sprintf("drop index %s", idx.Name),
			SQL:         r.renderer.DropIndexDDL(tableName, idx.Name),
			Risk:        RiskSafe,
		})
	}

	for _, c := range td.AddedColumns {
		sql, err := r.renderer.AddColumnDDL(tableName, c, td.Curr.Columns)
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
			SQL:         sql,
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
			SQL:         r.renderer.DropColumnDDL(tableName, c.Name),
			Risk:        RiskDataLoss,
			RiskNotes:   "drops column data",
		})
	}

	for _, idx := range td.AddedIndexes {
		if err := r.renderAddedIndex(plan, td.Curr, idx); err != nil {
			return err
		}
	}
	for _, fk := range td.AddedForeignKeys {
		if err := r.renderAddedForeignKey(plan, tableName, fk); err != nil {
			return err
		}
	}
	for _, chk := range td.AddedChecks {
		if err := r.renderAddedCheck(plan, td.Curr, chk); err != nil {
			return err
		}
	}

	return nil
}

func (r deterministicRenderer) renderColumnChange(plan *Plan, tableName string, cc ColumnChange) error {
	if cc.Old.IsComputed || cc.New.IsComputed {
		return fmt.Errorf("computed column %s changes are not supported by deterministic %s sync", cc.Name, r.target)
	}

	oldType, err := r.renderer.ColumnType(cc.Old)
	if err != nil {
		return err
	}
	newType, err := r.renderer.ColumnType(cc.New)
	if err != nil {
		return err
	}

	oldDefault := strings.TrimSpace(cc.Old.DefaultExpression)
	newDefault := strings.TrimSpace(cc.New.DefaultExpression)
	typeChanged := oldType != newType
	defaultChanged := oldDefault != newDefault
	preDropDefault := oldDefault != "" && (typeChanged || (r.target == "mssql" && defaultChanged))

	if preDropDefault {
		plan.Statements = append(plan.Statements, Statement{
			Table:       tableName,
			Description: fmt.Sprintf("drop column %s default", cc.Name),
			SQL:         r.renderer.DropColumnDefaultDDL(tableName, cc.Name),
			Risk:        RiskSafe,
		})
	}

	if typeChanged {
		sql, err := r.renderer.AlterColumnTypeDDL(tableName, cc.New)
		if err != nil {
			return err
		}
		plan.Statements = append(plan.Statements, Statement{
			Table:       tableName,
			Description: fmt.Sprintf("change column %s type", cc.Name),
			SQL:         sql,
			Risk:        RiskRebuildNeeded,
			RiskNotes:   "type changes may rewrite the table and can fail if existing values cannot be cast",
		})
	}
	if cc.Old.IsNullable != cc.New.IsNullable {
		risk := RiskSafe
		notes := ""
		if !cc.New.IsNullable {
			risk = RiskBlocking
			notes = "setting NOT NULL validates existing rows"
		}
		sql, err := r.renderer.AlterColumnNullabilityDDL(tableName, cc.New)
		if err != nil {
			return err
		}
		plan.Statements = append(plan.Statements, Statement{
			Table:       tableName,
			Description: fmt.Sprintf("change column %s nullability", cc.Name),
			SQL:         sql,
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
					SQL:         r.renderer.DropColumnDefaultDDL(tableName, cc.Name),
					Risk:        RiskSafe,
				})
			}
		} else {
			sql, err := r.renderer.SetColumnDefaultDDL(tableName, cc.New)
			if err != nil {
				return err
			}
			plan.Statements = append(plan.Statements, Statement{
				Table:       tableName,
				Description: fmt.Sprintf("change column %s default", cc.Name),
				SQL:         sql,
				Risk:        RiskSafe,
			})
		}
	}
	return nil
}

func (r deterministicRenderer) renderAddedIndex(plan *Plan, t driver.Table, idx driver.Index) error {
	tableName := t.Name
	sql, err := r.renderer.CreateIndexDDL(&t, &idx)
	if err != nil {
		return err
	}
	plan.Statements = append(plan.Statements, Statement{
		Table:       tableName,
		Description: fmt.Sprintf("create index %s", idx.Name),
		SQL:         sql,
		Risk:        RiskBlocking,
		RiskNotes:   "index creation can lock or scan the table",
	})
	return nil
}

func (r deterministicRenderer) renderAddedForeignKey(plan *Plan, tableName string, fk driver.ForeignKey) error {
	t := driver.Table{Name: tableName}
	sql, err := r.renderer.CreateForeignKeyDDL(&t, &fk)
	if err != nil {
		return err
	}
	plan.Statements = append(plan.Statements, Statement{
		Table:       tableName,
		Description: fmt.Sprintf("create foreign key %s", fk.Name),
		SQL:         sql,
		Risk:        RiskBlocking,
		RiskNotes:   "foreign key validation can scan existing rows",
	})
	return nil
}

func (r deterministicRenderer) renderAddedCheck(plan *Plan, t driver.Table, chk driver.CheckConstraint) error {
	tableName := t.Name
	sql, err := r.renderer.CreateCheckConstraintDDL(&t, &chk)
	if err != nil {
		return err
	}
	plan.Statements = append(plan.Statements, Statement{
		Table:       tableName,
		Description: fmt.Sprintf("create check constraint %s", chk.Name),
		SQL:         sql,
		Risk:        RiskBlocking,
		RiskNotes:   "check validation can scan existing rows",
	})
	return nil
}
