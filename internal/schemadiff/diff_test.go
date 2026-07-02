package schemadiff

import (
	"strings"
	"testing"
	"time"

	"smt/internal/driver"
)

func col(name, dtype string, nullable bool) driver.Column {
	return driver.Column{Name: name, DataType: dtype, IsNullable: nullable}
}

func table(name string, cols ...driver.Column) driver.Table {
	return driver.Table{Schema: "dbo", Name: name, Columns: cols}
}

func TestCompute_NoChanges(t *testing.T) {
	prev := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false))}}
	curr := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false))}}

	d := Compute(prev, curr)
	if !d.IsEmpty() {
		t.Fatalf("expected empty diff, got %s", d.Summary())
	}
}

func TestCompute_AddedColumn(t *testing.T) {
	prev := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false))}}
	curr := Snapshot{Tables: []driver.Table{table("Users",
		col("Id", "int", false),
		col("Email", "varchar", true),
	)}}

	d := Compute(prev, curr)
	if len(d.ChangedTables) != 1 {
		t.Fatalf("expected 1 changed table, got %d", len(d.ChangedTables))
	}
	td := d.ChangedTables[0]
	if len(td.AddedColumns) != 1 || td.AddedColumns[0].Name != "Email" {
		t.Fatalf("expected Email added, got %+v", td.AddedColumns)
	}
	if len(td.RemovedColumns) != 0 || len(td.ChangedColumns) != 0 {
		t.Fatalf("expected only addition, got %+v", td)
	}
}

func TestCompute_RemovedColumn(t *testing.T) {
	prev := Snapshot{Tables: []driver.Table{table("Users",
		col("Id", "int", false),
		col("LegacyField", "varchar", true),
	)}}
	curr := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false))}}

	d := Compute(prev, curr)
	if len(d.ChangedTables) != 1 || len(d.ChangedTables[0].RemovedColumns) != 1 {
		t.Fatalf("expected one removed column, got %+v", d)
	}
}

func TestCompute_ChangedColumnType(t *testing.T) {
	prev := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false))}}
	curr := Snapshot{Tables: []driver.Table{table("Users", col("Id", "bigint", false))}}

	d := Compute(prev, curr)
	if len(d.ChangedTables) != 1 || len(d.ChangedTables[0].ChangedColumns) != 1 {
		t.Fatalf("expected one changed column, got %+v", d)
	}
	cc := d.ChangedTables[0].ChangedColumns[0]
	if cc.Old.DataType != "int" || cc.New.DataType != "bigint" {
		t.Fatalf("unexpected change: %+v", cc)
	}
}

func TestCompute_ChangedColumnDefault(t *testing.T) {
	oldCol := col("Enabled", "bit", false)
	oldCol.DefaultExpression = "((0))"
	newCol := oldCol
	newCol.DefaultExpression = "((1))"
	prev := Snapshot{Tables: []driver.Table{table("Users", oldCol)}}
	curr := Snapshot{Tables: []driver.Table{table("Users", newCol)}}

	d := Compute(prev, curr)
	if len(d.ChangedTables) != 1 {
		t.Fatalf("expected 1 changed table, got %+v", d)
	}
	changed := d.ChangedTables[0].ChangedColumns
	if len(changed) != 1 || changed[0].Name != "Enabled" {
		t.Fatalf("expected Enabled default change, got %+v", changed)
	}
}

func TestCompute_ChangedEnumValues(t *testing.T) {
	oldCol := driver.Column{Name: "CustomerType", DataType: "enum", EnumValues: []string{"individual", "company"}}
	newCol := driver.Column{Name: "CustomerType", DataType: "enum", EnumValues: []string{"individual", "company", "government"}}
	prev := Snapshot{Tables: []driver.Table{table("Customers", oldCol)}}
	curr := Snapshot{Tables: []driver.Table{table("Customers", newCol)}}

	d := Compute(prev, curr)
	if len(d.ChangedTables) != 1 {
		t.Fatalf("expected 1 changed table, got %+v", d)
	}
	changed := d.ChangedTables[0].ChangedColumns
	if len(changed) != 1 || changed[0].Name != "CustomerType" {
		t.Fatalf("expected CustomerType enum values change, got %+v", changed)
	}
}

func TestCompute_ChangedMySQLColumnFlags(t *testing.T) {
	oldCol := driver.Column{Name: "UpdatedAt", DataType: "datetime", DefaultExpression: "CURRENT_TIMESTAMP"}
	newCol := oldCol
	newCol.OnUpdateExpression = "CURRENT_TIMESTAMP"
	prev := Snapshot{Version: CurrentSnapshotVersion, Tables: []driver.Table{table("Events", oldCol)}}
	curr := Snapshot{Version: CurrentSnapshotVersion, Tables: []driver.Table{table("Events", newCol)}}

	d := Compute(prev, curr)
	if len(d.ChangedTables) != 1 || len(d.ChangedTables[0].ChangedColumns) != 1 {
		t.Fatalf("expected on-update change to be detected, got %+v", d)
	}

	oldID := driver.Column{Name: "ID", DataType: "int"}
	newID := oldID
	newID.IsUnsigned = true
	d = Compute(Snapshot{Version: CurrentSnapshotVersion, Tables: []driver.Table{table("Events", oldID)}}, Snapshot{Version: CurrentSnapshotVersion, Tables: []driver.Table{table("Events", newID)}})
	if len(d.ChangedTables) != 1 || len(d.ChangedTables[0].ChangedColumns) != 1 {
		t.Fatalf("expected unsigned change to be detected, got %+v", d)
	}
}

func TestCompute_AddedAndRemovedTables(t *testing.T) {
	prev := Snapshot{Tables: []driver.Table{table("Users"), table("Sessions")}}
	curr := Snapshot{Tables: []driver.Table{table("Users"), table("Audit")}}

	d := Compute(prev, curr)
	if len(d.AddedTables) != 1 || d.AddedTables[0].Name != "Audit" {
		t.Fatalf("expected Audit added, got %+v", d.AddedTables)
	}
	if len(d.RemovedTables) != 1 || d.RemovedTables[0].Name != "Sessions" {
		t.Fatalf("expected Sessions removed, got %+v", d.RemovedTables)
	}
}

func TestCompute_ColumnsEqualIgnoresOrdinalAndSamples(t *testing.T) {
	c1 := driver.Column{Name: "X", DataType: "int", OrdinalPos: 1, SampleValues: []string{"1"}}
	c2 := driver.Column{Name: "X", DataType: "int", OrdinalPos: 5, SampleValues: []string{"99"}}
	if !columnsEqual(c1, c2) {
		t.Fatalf("expected columns to be considered equal despite ordinal/sample differences")
	}
}

func TestRenderDeterministic_AddedColumnPostgres(t *testing.T) {
	prev := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false))}}
	curr := Snapshot{Tables: []driver.Table{table("Users",
		col("Id", "int", false),
		driver.Column{Name: "DisplayName", DataType: "nvarchar", MaxLength: 40, IsNullable: true},
	)}}
	d := Compute(prev, curr).Normalize(func(name string) string {
		return driver.NormalizeIdentifier("postgres", name)
	}).WithTargetSchema("public")

	plan, err := RenderDeterministic(d, "public", "postgres")
	if err != nil {
		t.Fatalf("RenderDeterministic: %v", err)
	}
	if len(plan.Statements) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(plan.Statements))
	}
	stmt := plan.Statements[0]
	if stmt.Risk != RiskSafe {
		t.Fatalf("risk = %s", stmt.Risk)
	}
	if !strings.Contains(stmt.SQL, `ALTER TABLE "public"."users" ADD COLUMN "displayname" character varying(40)`) {
		t.Fatalf("unexpected SQL: %s", stmt.SQL)
	}
}

func TestRenderDeterministic_SQLIncludesMappingWarnings(t *testing.T) {
	d := Diff{ChangedTables: []TableDiff{{
		Name: "users",
		Curr: driver.Table{Name: "users", Columns: []driver.Column{{Name: "account_id", DataType: "bigint", IsUnsigned: true}}},
		AddedColumns: []driver.Column{{
			Name:       "account_id",
			DataType:   "bigint",
			IsUnsigned: true,
			IsNullable: true,
		}},
	}}}
	plan, err := RenderDeterministicWithOptions(d, RenderOptions{
		TargetSchema:      "public",
		TargetDialect:     "postgres",
		SourceDialect:     "mysql",
		UnknownTypePolicy: "fail",
	})
	if err != nil {
		t.Fatalf("RenderDeterministicWithOptions: %v", err)
	}
	if len(plan.Statements) != 1 || len(plan.Statements[0].Warnings) == 0 {
		t.Fatalf("expected mapping warning on added column statement, got %+v", plan.Statements)
	}
	if sql := plan.SQL(); !strings.Contains(sql, "-- warning: column account_id") ||
		!strings.Contains(sql, "target has no unsigned 64-bit integer") {
		t.Fatalf("plan SQL did not include mapping warning:\n%s", sql)
	}
}

func TestRenderDeterministic_AddedTableIncludesSideObjects(t *testing.T) {
	prev := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false))}}
	audit := driver.Table{
		Schema:     "dbo",
		Name:       "AuditLog",
		PrimaryKey: []string{"Id"},
		Columns: []driver.Column{
			{Name: "Id", DataType: "int", IsNullable: false},
			{Name: "UserId", DataType: "int", IsNullable: false},
			{Name: "Action", DataType: "nvarchar", MaxLength: 20, IsNullable: false},
		},
		Indexes: []driver.Index{{
			Name:    "IX_AuditLog_UserId",
			Columns: []string{"UserId"},
		}},
		ForeignKeys: []driver.ForeignKey{{
			Name:       "FK_AuditLog_Users",
			Columns:    []string{"UserId"},
			RefTable:   "Users",
			RefColumns: []string{"Id"},
		}},
		CheckConstraints: []driver.CheckConstraint{{
			Name:       "CK_AuditLog_Action",
			Definition: "([Action] IN ('create','update'))",
		}},
	}
	curr := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false)), audit}}
	d := Compute(prev, curr).Normalize(func(name string) string {
		return driver.NormalizeIdentifier("postgres", name)
	}).WithTargetSchema("public")

	plan, err := RenderDeterministic(d, "public", "postgres")
	if err != nil {
		t.Fatalf("RenderDeterministic: %v", err)
	}
	if len(plan.Statements) != 4 {
		t.Fatalf("expected create table plus 3 side-object statements, got %d: %+v", len(plan.Statements), plan.Statements)
	}
	sql := plan.SQL()
	for _, want := range []string{
		`CREATE TABLE "public"."auditlog"`,
		`CREATE INDEX "ix_auditlog_userid" ON "public"."auditlog" ("userid")`,
		`ALTER TABLE "public"."auditlog" ADD CONSTRAINT "fk_auditlog_users" FOREIGN KEY ("userid") REFERENCES "public"."users" ("id")`,
		`ALTER TABLE "public"."auditlog" ADD CONSTRAINT "ck_auditlog_action" CHECK ("action" IN ('create', 'update'))`,
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected SQL to contain %q, got:\n%s", want, sql)
		}
	}
}

func TestRenderDeterministic_ChangedTableBitSideObjectsUseColumnContext(t *testing.T) {
	d := Diff{ChangedTables: []TableDiff{{
		Name: "Customers",
		Curr: driver.Table{
			Name: "Customers",
			Columns: []driver.Column{
				{Name: "Id", DataType: "int", IsNullable: false},
				{Name: "IsActive", DataType: "bit", IsNullable: false},
			},
		},
		AddedIndexes: []driver.Index{{
			Name:    "IX_Customers_Active",
			Columns: []string{"Id"},
			Filter:  "([IsActive]=(1))",
		}},
		AddedChecks: []driver.CheckConstraint{{
			Name:       "CK_Customers_Active",
			Definition: "([IsActive]=(1))",
		}},
	}}}.Normalize(func(name string) string {
		return driver.NormalizeIdentifier("postgres", name)
	}).WithTargetSchema("public")

	plan, err := RenderDeterministic(d, "public", "postgres")
	if err != nil {
		t.Fatalf("RenderDeterministic: %v", err)
	}
	sql := plan.SQL()
	for _, want := range []string{
		`CREATE INDEX "ix_customers_active" ON "public"."customers" ("id") WHERE ("isactive" = true)`,
		`ALTER TABLE "public"."customers" ADD CONSTRAINT "ck_customers_active" CHECK ("isactive" = true)`,
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected SQL to contain %q, got:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "true))") {
		t.Fatalf("bit comparison rewrite left an extra close paren:\n%s", sql)
	}
}

func TestRenderDeterministic_AddedComputedColumnUsesTableBitContext(t *testing.T) {
	d := Diff{ChangedTables: []TableDiff{{
		Name: "Customers",
		Curr: driver.Table{
			Name: "Customers",
			Columns: []driver.Column{
				{Name: "Id", DataType: "int", IsNullable: false},
				{Name: "IsActive", DataType: "bit", IsNullable: false},
				{
					Name:               "ActiveRank",
					DataType:           "int",
					IsComputed:         true,
					ComputedExpression: "(case when [IsActive]=(1) then (10) else (0) end)",
					ComputedPersisted:  true,
				},
			},
		},
		AddedColumns: []driver.Column{{
			Name:               "ActiveRank",
			DataType:           "int",
			IsComputed:         true,
			ComputedExpression: "(case when [IsActive]=(1) then (10) else (0) end)",
			ComputedPersisted:  true,
		}},
	}}}.Normalize(func(name string) string {
		return driver.NormalizeIdentifier("postgres", name)
	}).WithTargetSchema("public")

	plan, err := RenderDeterministic(d, "public", "postgres")
	if err != nil {
		t.Fatalf("RenderDeterministic: %v", err)
	}
	sql := plan.SQL()
	want := `ADD COLUMN "activerank" integer GENERATED ALWAYS AS ((case when "isactive" = true then (10) else (0) end)) STORED`
	if !strings.Contains(sql, want) {
		t.Fatalf("expected SQL to contain %q, got:\n%s", want, sql)
	}
	if strings.Contains(sql, `"isactive"=(1)`) || strings.Contains(sql, "true))") {
		t.Fatalf("added computed column bit context was not rendered cleanly:\n%s", sql)
	}
}

func TestRenderDeterministic_AddedTableForeignKeysAfterAllCreateTables(t *testing.T) {
	comments := driver.Table{
		Schema: "dbo",
		Name:   "Comments",
		Columns: []driver.Column{
			{Name: "Id", DataType: "int", IsNullable: false},
			{Name: "PostId", DataType: "int", IsNullable: false},
		},
		ForeignKeys: []driver.ForeignKey{{
			Name:       "FK_Comments_Posts",
			Columns:    []string{"PostId"},
			RefTable:   "Posts",
			RefColumns: []string{"Id"},
		}},
	}
	posts := driver.Table{
		Schema:  "dbo",
		Name:    "Posts",
		Columns: []driver.Column{{Name: "Id", DataType: "int", IsNullable: false}},
	}
	d := Compute(Snapshot{}, Snapshot{Tables: []driver.Table{comments, posts}}).Normalize(func(name string) string {
		return driver.NormalizeIdentifier("postgres", name)
	}).WithTargetSchema("public")

	plan, err := RenderDeterministic(d, "public", "postgres")
	if err != nil {
		t.Fatalf("RenderDeterministic: %v", err)
	}
	sql := plan.SQL()
	commentsPos := strings.Index(sql, `CREATE TABLE "public"."comments"`)
	postsPos := strings.Index(sql, `CREATE TABLE "public"."posts"`)
	fkPos := strings.Index(sql, `ALTER TABLE "public"."comments" ADD CONSTRAINT "fk_comments_posts"`)
	if commentsPos < 0 || postsPos < 0 || fkPos < 0 {
		t.Fatalf("expected comments/posts create and FK statements, got:\n%s", sql)
	}
	if fkPos < commentsPos || fkPos < postsPos {
		t.Fatalf("expected FK after both CREATE TABLE statements, got:\n%s", sql)
	}
}

func TestRenderDeterministic_ColumnTypeAndDefaultChangeDropsDefaultFirst(t *testing.T) {
	oldScore := driver.Column{Name: "Score", DataType: "varchar", MaxLength: 10, IsNullable: false, DefaultExpression: "('0')"}
	newScore := driver.Column{Name: "Score", DataType: "int", IsNullable: false, DefaultExpression: "((0))"}
	d := Compute(
		Snapshot{Tables: []driver.Table{table("Users", oldScore)}},
		Snapshot{Tables: []driver.Table{table("Users", newScore)}},
	).Normalize(func(name string) string {
		return driver.NormalizeIdentifier("postgres", name)
	}).WithTargetSchema("public")

	plan, err := RenderDeterministic(d, "public", "postgres")
	if err != nil {
		t.Fatalf("RenderDeterministic: %v", err)
	}
	sql := plan.SQL()
	dropPos := strings.Index(sql, `ALTER TABLE "public"."users" ALTER COLUMN "score" DROP DEFAULT`)
	typePos := strings.Index(sql, `ALTER TABLE "public"."users" ALTER COLUMN "score" TYPE integer`)
	setPos := strings.Index(sql, `ALTER TABLE "public"."users" ALTER COLUMN "score" SET DEFAULT 0`)
	if dropPos < 0 || typePos < 0 || setPos < 0 {
		t.Fatalf("expected drop default, type change, and set default statements, got:\n%s", sql)
	}
	if !(dropPos < typePos && typePos < setPos) {
		t.Fatalf("expected default drop before type change before set default, got:\n%s", sql)
	}
}

func TestRenderDeterministic_ComputedColumnChangeFails(t *testing.T) {
	oldTotal := driver.Column{
		Name:               "Total",
		DataType:           "int",
		IsNullable:         false,
		IsComputed:         true,
		ComputedExpression: "([Quantity]*[Price])",
	}
	newTotal := oldTotal
	newTotal.ComputedExpression = "([Quantity]*[Price]*(1-[Discount]))"
	d := Compute(
		Snapshot{Tables: []driver.Table{table("Orders", oldTotal)}},
		Snapshot{Tables: []driver.Table{table("Orders", newTotal)}},
	).Normalize(func(name string) string {
		return driver.NormalizeIdentifier("postgres", name)
	}).WithTargetSchema("public")

	_, err := RenderDeterministic(d, "public", "postgres")
	if err == nil {
		t.Fatal("expected computed column change to fail")
	}
	if !strings.Contains(err.Error(), "computed column") {
		t.Fatalf("expected computed column error, got %v", err)
	}
}

func TestRenderDeterministic_ColumnDefaultChanges(t *testing.T) {
	activeOld := driver.Column{Name: "IsActive", DataType: "bit", IsNullable: false, DefaultExpression: "((0))"}
	activeNew := activeOld
	activeNew.DefaultExpression = "((1))"
	statusOld := driver.Column{Name: "Status", DataType: "nvarchar", MaxLength: 20, IsNullable: true, DefaultExpression: "('old')"}
	statusNew := statusOld
	statusNew.DefaultExpression = ""
	prev := Snapshot{Tables: []driver.Table{table("Users", activeOld, statusOld)}}
	curr := Snapshot{Tables: []driver.Table{table("Users", activeNew, statusNew)}}
	d := Compute(prev, curr).Normalize(func(name string) string {
		return driver.NormalizeIdentifier("postgres", name)
	}).WithTargetSchema("public")

	plan, err := RenderDeterministic(d, "public", "postgres")
	if err != nil {
		t.Fatalf("RenderDeterministic: %v", err)
	}
	if len(plan.Statements) != 2 {
		t.Fatalf("expected 2 default statements, got %d: %+v", len(plan.Statements), plan.Statements)
	}
	sql := plan.SQL()
	if !strings.Contains(sql, `ALTER TABLE "public"."users" ALTER COLUMN "isactive" SET DEFAULT true`) {
		t.Fatalf("expected SET DEFAULT true, got:\n%s", sql)
	}
	if !strings.Contains(sql, `ALTER TABLE "public"."users" ALTER COLUMN "status" DROP DEFAULT`) {
		t.Fatalf("expected DROP DEFAULT, got:\n%s", sql)
	}
}

func TestRenderDeterministic_MSSQLDefaultOnlyChangeDropsOldDefaultFirst(t *testing.T) {
	activeOld := driver.Column{Name: "IsActive", DataType: "bit", IsNullable: false, DefaultExpression: "((0))"}
	activeNew := activeOld
	activeNew.DefaultExpression = "((1))"
	d := Compute(
		Snapshot{Tables: []driver.Table{table("Users", activeOld)}},
		Snapshot{Tables: []driver.Table{table("Users", activeNew)}},
	)

	plan, err := RenderDeterministic(d, "dbo", "mssql")
	if err != nil {
		t.Fatalf("RenderDeterministic: %v", err)
	}
	if len(plan.Statements) != 2 {
		t.Fatalf("expected 2 default statements, got %d: %+v", len(plan.Statements), plan.Statements)
	}
	sql := plan.SQL()
	dropPos := strings.Index(sql, `DROP CONSTRAINT`)
	setPos := strings.Index(sql, `ADD CONSTRAINT [df_Users_IsActive] DEFAULT 1 FOR [IsActive]`)
	if dropPos < 0 || setPos < 0 {
		t.Fatalf("expected drop old default before adding new default, got:\n%s", sql)
	}
	if dropPos > setPos {
		t.Fatalf("expected old default drop before new default, got:\n%s", sql)
	}
}

func TestRenderDeterministic_UnknownTypePolicyAppliesToSync(t *testing.T) {
	prev := Snapshot{}
	curr := Snapshot{Tables: []driver.Table{table("Events", driver.Column{Name: "Payload", DataType: "sql_variant", IsNullable: true})}}
	d := Compute(prev, curr).Normalize(func(name string) string {
		return driver.NormalizeIdentifier("postgres", name)
	}).WithTargetSchema("public")

	plan, err := RenderDeterministicWithUnknownTypePolicy(d, "public", "postgres", "text_fallback")
	if err != nil {
		t.Fatalf("RenderDeterministicWithUnknownTypePolicy: %v", err)
	}
	if !strings.Contains(plan.SQL(), `"payload" text`) {
		t.Fatalf("expected text fallback, got:\n%s", plan.SQL())
	}
}

func TestRenderDeterministic_RemovedColumnIsDataLoss(t *testing.T) {
	prev := Snapshot{Tables: []driver.Table{table("Users",
		col("Id", "int", false),
		col("Legacy", "int", true),
	)}}
	curr := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false))}}
	d := Compute(prev, curr).Normalize(func(name string) string {
		return driver.NormalizeIdentifier("postgres", name)
	}).WithTargetSchema("public")

	plan, err := RenderDeterministic(d, "public", "postgres")
	if err != nil {
		t.Fatalf("RenderDeterministic: %v", err)
	}
	if len(plan.Statements) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(plan.Statements))
	}
	if plan.Statements[0].Risk != RiskDataLoss {
		t.Fatalf("risk = %s", plan.Statements[0].Risk)
	}
	if !strings.Contains(plan.Statements[0].SQL, `ALTER TABLE "public"."users" DROP COLUMN "legacy"`) {
		t.Fatalf("unexpected SQL: %s", plan.Statements[0].SQL)
	}
}

func TestRenderDeterministic_AddedColumnMSSQL(t *testing.T) {
	prev := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false))}}
	curr := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false), col("Email", "varchar", true))}}
	d := Compute(prev, curr)

	plan, err := RenderDeterministic(d, "dbo", "mssql")
	if err != nil {
		t.Fatalf("RenderDeterministic: %v", err)
	}
	if len(plan.Statements) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(plan.Statements))
	}
	if !strings.Contains(plan.Statements[0].SQL, `ALTER TABLE [dbo].[Users] ADD [Email] VARCHAR(MAX)`) {
		t.Fatalf("unexpected SQL: %s", plan.Statements[0].SQL)
	}
}

func TestDiff_NormalizeRewritesIdentifiers(t *testing.T) {
	prev := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false))}}
	curr := Snapshot{Tables: []driver.Table{table("Users",
		col("Id", "int", false), col("Email", "varchar", true))}}
	d := Compute(prev, curr).Normalize(func(s string) string {
		return "lc_" + s
	})

	if len(d.ChangedTables) != 1 || d.ChangedTables[0].Name != "lc_Users" {
		t.Fatalf("expected normalized table name lc_Users, got %+v", d.ChangedTables)
	}
	added := d.ChangedTables[0].AddedColumns
	if len(added) != 1 || added[0].Name != "lc_Email" {
		t.Fatalf("expected normalized column name lc_Email, got %+v", added)
	}
}

// TestDiff_WithTargetSchemaRewritesTableSchema is a regression guard for
// issue #4. The structural diff carries source schema names in
// Table.Schema (populated by source introspection); renderers must not
// use those source qualifiers for target DDL. WithTargetSchema must
// rewrite Schema across added / removed / changed tables.
func TestDiff_WithTargetSchemaRewritesTableSchema(t *testing.T) {
	prev := Snapshot{Tables: []driver.Table{
		{Schema: "smt_src_test", Name: "kept", Columns: []driver.Column{col("Id", "int", false)}},
		{Schema: "smt_src_test", Name: "dropped"},
	}}
	curr := Snapshot{Tables: []driver.Table{
		{Schema: "smt_src_test", Name: "kept", Columns: []driver.Column{col("Id", "int", false), col("Email", "varchar", true)}},
		{Schema: "smt_src_test", Name: "added"},
	}}

	d := Compute(prev, curr).WithTargetSchema("dbo")

	check := func(label, got string) {
		t.Helper()
		if got != "dbo" {
			t.Errorf("%s schema: got %q, want %q", label, got, "dbo")
		}
	}
	if len(d.AddedTables) != 1 {
		t.Fatalf("expected 1 added table, got %+v", d.AddedTables)
	}
	check("AddedTables[0]", d.AddedTables[0].Schema)
	if len(d.RemovedTables) != 1 {
		t.Fatalf("expected 1 removed table, got %+v", d.RemovedTables)
	}
	check("RemovedTables[0]", d.RemovedTables[0].Schema)
	if len(d.ChangedTables) != 1 {
		t.Fatalf("expected 1 changed table, got %+v", d.ChangedTables)
	}
	check("ChangedTables[0]", d.ChangedTables[0].Schema)
	check("ChangedTables[0].Curr", d.ChangedTables[0].Curr.Schema)
}

// TestDiff_WithTargetSchemaRewritesForeignKeyRefSchema is the second
// regression guard for #4: same-schema FK additions must use the target
// schema, not the source schema, in REFERENCES clauses. Cover
// AddedTables.ForeignKeys, ChangedTables.Curr.ForeignKeys, and the
// AddedForeignKeys / RemovedForeignKeys slices.
func TestDiff_WithTargetSchemaRewritesForeignKeyRefSchema(t *testing.T) {
	srcFK := driver.ForeignKey{
		Name: "fk_child_parent", Columns: []string{"parent_id"},
		RefSchema: "smt_src_test", RefTable: "parent", RefColumns: []string{"id"},
	}
	addedFK := driver.ForeignKey{
		Name: "fk_new", Columns: []string{"other_id"},
		RefSchema: "smt_src_test", RefTable: "other", RefColumns: []string{"id"},
	}
	removedFK := driver.ForeignKey{
		Name: "fk_legacy", RefSchema: "smt_src_test", RefTable: "legacy",
	}

	prev := Snapshot{Tables: []driver.Table{
		{Schema: "smt_src_test", Name: "child", ForeignKeys: []driver.ForeignKey{srcFK}, Columns: []driver.Column{col("Id", "int", false)}},
	}}
	curr := Snapshot{Tables: []driver.Table{
		{Schema: "smt_src_test", Name: "child", ForeignKeys: []driver.ForeignKey{srcFK}, Columns: []driver.Column{col("Id", "int", false), col("New", "int", true)}},
		{Schema: "smt_src_test", Name: "fresh", ForeignKeys: []driver.ForeignKey{srcFK}},
	}}

	d := Compute(prev, curr)
	// Compute only fills in column-level deltas — added/removed FKs come
	// from the diff caller's caller. Inject them so we can verify the
	// retarget paths for AddedForeignKeys / RemovedForeignKeys too.
	d.ChangedTables[0].AddedForeignKeys = []driver.ForeignKey{addedFK}
	d.ChangedTables[0].RemovedForeignKeys = []driver.ForeignKey{removedFK}

	d = d.WithTargetSchema("dbo")

	check := func(label, got string) {
		t.Helper()
		if got != "dbo" {
			t.Errorf("%s ref_schema: got %q, want %q", label, got, "dbo")
		}
	}

	if len(d.AddedTables) != 1 || len(d.AddedTables[0].ForeignKeys) != 1 {
		t.Fatalf("expected 1 added table with 1 FK, got %+v", d.AddedTables)
	}
	check("AddedTables[0].ForeignKeys[0].RefSchema", d.AddedTables[0].ForeignKeys[0].RefSchema)

	if len(d.ChangedTables) != 1 {
		t.Fatalf("expected 1 changed table, got %+v", d.ChangedTables)
	}
	td := d.ChangedTables[0]
	if len(td.Curr.ForeignKeys) != 1 {
		t.Fatalf("expected Curr.ForeignKeys to be preserved, got %+v", td.Curr.ForeignKeys)
	}
	check("ChangedTables[0].Curr.ForeignKeys[0].RefSchema", td.Curr.ForeignKeys[0].RefSchema)

	if len(td.AddedForeignKeys) != 1 {
		t.Fatalf("expected 1 added FK, got %+v", td.AddedForeignKeys)
	}
	check("ChangedTables[0].AddedForeignKeys[0].RefSchema", td.AddedForeignKeys[0].RefSchema)

	if len(td.RemovedForeignKeys) != 1 {
		t.Fatalf("expected 1 removed FK, got %+v", td.RemovedForeignKeys)
	}
	check("ChangedTables[0].RemovedForeignKeys[0].RefSchema", td.RemovedForeignKeys[0].RefSchema)
}

func TestPlan_FilterByRisk(t *testing.T) {
	plan := Plan{Statements: []Statement{
		{SQL: "ALTER 1", Risk: RiskSafe},
		{SQL: "ALTER 2", Risk: RiskBlocking},
		{SQL: "ALTER 3", Risk: RiskRebuildNeeded},
		{SQL: "ALTER 4", Risk: RiskDataLoss},
	}}
	got := plan.FilterByRisk(RiskBlocking)
	if len(got.Statements) != 2 {
		t.Fatalf("expected 2 stmts at RiskBlocking limit, got %d", len(got.Statements))
	}
}

// silence unused import if Compute changes signature
var _ = time.Second
