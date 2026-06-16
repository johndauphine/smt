package schemadiff

import (
	"strings"
	"testing"

	"smt/internal/driver"
)

func TestComputeLiveDiff_NoFalsePositiveCrossDialect(t *testing.T) {
	desired := []driver.Table{{Schema: "public", Name: "users", Columns: []driver.Column{
		{Name: "id", DataType: "int", IsNullable: false},
		{Name: "name", DataType: "varchar", MaxLength: 20, IsNullable: true},
	}}}
	existing := []driver.Table{{Schema: "public", Name: "users", Columns: []driver.Column{
		{Name: "id", DataType: "integer", IsNullable: false},
		{Name: "name", DataType: "character varying", MaxLength: 20, IsNullable: true},
	}}}

	d := ComputeLiveDiff(desired, existing, "mssql", "postgres", DefaultDriftOptions())
	if !d.IsEmpty() {
		t.Fatalf("equivalent live target should not produce a plan: %+v", d)
	}
}

func TestComputeLiveDiff_ClassifiesStructuredChanges(t *testing.T) {
	desired := []driver.Table{{Schema: "public", Name: "users", PrimaryKey: []string{"id"}, Columns: []driver.Column{
		{Name: "id", DataType: "int", IsNullable: false},
		{Name: "email", DataType: "varchar", MaxLength: 100, IsNullable: true},
		{Name: "age", DataType: "int", IsNullable: true},
	}}}
	existing := []driver.Table{{Schema: "public", Name: "users", Columns: []driver.Column{
		{Name: "id", DataType: "integer", IsNullable: false},
		{Name: "legacy", DataType: "text", IsNullable: true},
		{Name: "age", DataType: "text", IsNullable: true},
	}}}

	d := ComputeLiveDiff(desired, existing, "mssql", "postgres", DefaultDriftOptions())
	if len(d.ChangedTables) != 1 {
		t.Fatalf("expected one changed table, got %+v", d)
	}
	td := d.ChangedTables[0]
	if len(td.AddedColumns) != 1 || td.AddedColumns[0].Name != "email" {
		t.Fatalf("expected email added, got %+v", td.AddedColumns)
	}
	if len(td.RemovedColumns) != 1 || td.RemovedColumns[0].Name != "legacy" {
		t.Fatalf("expected legacy removed, got %+v", td.RemovedColumns)
	}
	if len(td.ChangedColumns) != 1 || td.ChangedColumns[0].Name != "age" ||
		!containsString(td.ChangedColumns[0].Criteria, "type") {
		t.Fatalf("expected age type change, got %+v", td.ChangedColumns)
	}
	if len(d.Unsupported) != 1 || !strings.Contains(d.Unsupported[0].Description, "primary key") {
		t.Fatalf("expected unsupported PK change, got %+v", d.Unsupported)
	}
}

func TestComputeLiveDiff_IdentityChangeUnsupported(t *testing.T) {
	desired := []driver.Table{{Schema: "public", Name: "users", Columns: []driver.Column{
		{Name: "id", DataType: "int", IsIdentity: true},
	}}}
	existing := []driver.Table{{Schema: "public", Name: "users", Columns: []driver.Column{
		{Name: "id", DataType: "integer", IsIdentity: false},
	}}}

	d := ComputeLiveDiff(desired, existing, "mssql", "postgres", DefaultDriftOptions())
	if len(d.Unsupported) != 1 || !strings.Contains(d.Unsupported[0].Reason, "identity") {
		t.Fatalf("identity change should be unsupported, got %+v", d.Unsupported)
	}
	if len(d.ChangedTables) != 0 {
		t.Fatalf("unsupported identity change should not emit partial column DDL: %+v", d.ChangedTables)
	}
}

func TestComputeLiveDiff_AddedTableHonorsUnmanagedKinds(t *testing.T) {
	desired := []driver.Table{{
		Schema:  "public",
		Name:    "orders",
		Columns: []driver.Column{{Name: "id", DataType: "int", IsNullable: false}},
		Indexes: []driver.Index{{
			Name:    "ix_orders_id",
			Columns: []string{"id"},
		}},
		ForeignKeys: []driver.ForeignKey{{
			Name:       "fk_orders_customer",
			Columns:    []string{"id"},
			RefTable:   "customers",
			RefColumns: []string{"id"},
		}},
		CheckConstraints: []driver.CheckConstraint{{
			Name:       "ck_orders_id",
			Definition: "id > 0",
		}},
	}}

	d := ComputeLiveDiff(desired, nil, "mssql", "postgres", DriftOptions{
		CompareIndexes:     false,
		CompareForeignKeys: false,
		CompareChecks:      false,
	})
	if len(d.AddedTables) != 1 {
		t.Fatalf("expected one added table, got %+v", d.AddedTables)
	}
	added := d.AddedTables[0]
	if len(added.Indexes) != 0 || len(added.ForeignKeys) != 0 || len(added.CheckConstraints) != 0 {
		t.Fatalf("unmanaged side objects should be stripped from added table diff: %+v", added)
	}

	plan, err := RenderDeterministicWithOptions(d, RenderOptions{
		TargetSchema:      "public",
		TargetDialect:     "postgres",
		SourceDialect:     "mssql",
		UnknownTypePolicy: "fail",
	})
	if err != nil {
		t.Fatalf("RenderDeterministicWithOptions: %v", err)
	}
	if len(plan.Statements) != 1 || plan.Statements[0].Description != "create table orders" {
		t.Fatalf("expected only create-table statement, got %+v", plan.Statements)
	}
}

func TestComputeLiveDiff_MetadataChangeUnsupported(t *testing.T) {
	desired := []driver.Table{{Schema: "app", Name: "events", Columns: []driver.Column{
		{Name: "updated_at", DataType: "timestamp", DefaultExpression: "CURRENT_TIMESTAMP", OnUpdateExpression: "CURRENT_TIMESTAMP"},
		{Name: "status", DataType: "enum", EnumValues: []string{"new", "done"}},
	}}}
	existing := []driver.Table{{Schema: "app", Name: "events", Columns: []driver.Column{
		{Name: "updated_at", DataType: "timestamp", DefaultExpression: "CURRENT_TIMESTAMP"},
		{Name: "status", DataType: "enum", EnumValues: []string{"new"}},
	}}}

	d := ComputeLiveDiff(desired, existing, "mysql", "mysql", DefaultDriftOptions())
	if len(d.Unsupported) != 2 {
		t.Fatalf("metadata changes should be unsupported, got %+v", d.Unsupported)
	}
	reasons := d.Unsupported[0].Reason + " " + d.Unsupported[1].Reason
	if !strings.Contains(reasons, "ON UPDATE") || !strings.Contains(reasons, "ENUM/SET") {
		t.Fatalf("unsupported metadata reasons missing detail: %+v", d.Unsupported)
	}
	if len(d.ChangedTables) != 0 {
		t.Fatalf("unsupported metadata changes should not emit partial column DDL: %+v", d.ChangedTables)
	}
}

func TestComputeLiveDiff_SameDialectCheckDefinitionUnsupported(t *testing.T) {
	desired := []driver.Table{{Schema: "public", Name: "accounts",
		Columns:          []driver.Column{{Name: "balance", DataType: "int"}},
		CheckConstraints: []driver.CheckConstraint{{Name: "ck_balance", Definition: "balance >= 0"}},
	}}
	existing := []driver.Table{{Schema: "public", Name: "accounts",
		Columns:          []driver.Column{{Name: "balance", DataType: "int"}},
		CheckConstraints: []driver.CheckConstraint{{Name: "ck_balance", Definition: "balance > 0"}},
	}}

	d := ComputeLiveDiff(desired, existing, "postgres", "postgres", DefaultDriftOptions())
	if len(d.Unsupported) != 1 || !strings.Contains(d.Unsupported[0].Reason, "check predicate") {
		t.Fatalf("same-dialect check definition drift should be unsupported, got %+v", d.Unsupported)
	}

	if d := ComputeLiveDiff(desired, existing, "mssql", "postgres", DefaultDriftOptions()); !d.IsEmpty() {
		t.Fatalf("cross-dialect same-count check predicates should remain count-based, got %+v", d)
	}
}

func TestRenderDeterministic_LiveColumnChangeUsesExistingDialect(t *testing.T) {
	d := Diff{ChangedTables: []TableDiff{{
		Name: "users",
		Curr: driver.Table{Name: "users", Columns: []driver.Column{{Name: "name", DataType: "varchar", MaxLength: 20}}},
		ChangedColumns: []ColumnChange{{
			Name:     "name",
			Old:      driver.Column{Name: "name", DataType: "character varying", MaxLength: 10},
			New:      driver.Column{Name: "name", DataType: "varchar", MaxLength: 20},
			Criteria: []string{"max_length"},
		}},
	}}}
	plan, err := RenderDeterministicWithOptions(d, RenderOptions{
		TargetSchema:      "public",
		TargetDialect:     "postgres",
		SourceDialect:     "mssql",
		ExistingDialect:   "postgres",
		UnknownTypePolicy: "fail",
	})
	if err != nil {
		t.Fatalf("RenderDeterministicWithOptions: %v", err)
	}
	if len(plan.Statements) != 1 {
		t.Fatalf("expected one type-change statement, got %+v", plan.Statements)
	}
	if !strings.Contains(plan.Statements[0].SQL, `character varying(20)`) {
		t.Fatalf("unexpected type-change SQL: %s", plan.Statements[0].SQL)
	}
}

func TestPlanSQLIncludesUnsupportedChanges(t *testing.T) {
	plan := Plan{Unsupported: []UnsupportedChange{{
		Table:       "users",
		Description: "change primary key",
		Reason:      "not supported",
	}}}
	sql := plan.SQL()
	if !strings.Contains(sql, "-- [unsupported] change primary key") ||
		!strings.Contains(sql, "-- reason: not supported") {
		t.Fatalf("unsupported change missing from plan SQL:\n%s", sql)
	}
	if plan.IsEmpty() {
		t.Fatal("plan with unsupported changes must not be empty")
	}
}

func containsString(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}
