package schemadiff

import (
	"context"
	"testing"
	"time"

	"smt/internal/driver"
)

// stubAsker is a deterministic AI replacement for tests. It returns
// whatever JSON is registered in Responses keyed by prompt prefix, or
// "" when nothing matches (which causes Render to return a parse error).
type stubAsker struct {
	Response  string
	GotPrompt string
}

func (s *stubAsker) Ask(_ context.Context, prompt string) (string, error) {
	s.GotPrompt = prompt
	return s.Response, nil
}

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

func TestRender_EmptyDiffSkipsAI(t *testing.T) {
	asker := &stubAsker{Response: "should not be called"}
	plan, err := Render(context.Background(), asker, Diff{}, "public", "postgres")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !plan.IsEmpty() {
		t.Fatalf("expected empty plan")
	}
	if asker.GotPrompt != "" {
		t.Fatalf("AI was called for empty diff")
	}
}

func TestRender_ParsesAIResponse(t *testing.T) {
	asker := &stubAsker{Response: `{"statements":[
		{"table":"Users","description":"add Email column","sql":"ALTER TABLE \"public\".\"Users\" ADD COLUMN \"Email\" varchar NULL","risk":"safe"}
	]}`}

	prev := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false))}}
	curr := Snapshot{Tables: []driver.Table{table("Users",
		col("Id", "int", false), col("Email", "varchar", true))}}
	d := Compute(prev, curr)

	plan, err := Render(context.Background(), asker, d, "public", "postgres")
	if err != nil {
		t.Fatalf("render failed: %v", err)
	}
	if len(plan.Statements) != 1 {
		t.Fatalf("expected 1 stmt, got %d", len(plan.Statements))
	}
	if plan.Statements[0].Risk != RiskSafe {
		t.Fatalf("expected RiskSafe, got %s", plan.Statements[0].Risk)
	}
}

func TestRender_StripsCodeFences(t *testing.T) {
	asker := &stubAsker{Response: "```json\n{\"statements\":[{\"sql\":\"SELECT 1\",\"risk\":\"safe\"}]}\n```"}

	prev := Snapshot{Tables: []driver.Table{table("X")}}
	curr := Snapshot{Tables: []driver.Table{table("X", col("Y", "int", true))}}
	d := Compute(prev, curr)

	plan, err := Render(context.Background(), asker, d, "public", "postgres")
	if err != nil {
		t.Fatalf("render failed: %v", err)
	}
	if len(plan.Statements) != 1 {
		t.Fatalf("expected 1 stmt, got %d", len(plan.Statements))
	}
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
