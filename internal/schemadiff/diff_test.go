package schemadiff

import (
	"context"
	"errors"
	"strings"
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

// failingAsker always returns the configured error. Used to simulate AI
// provider outages / timeouts.
type failingAsker struct{ err error }

func (f *failingAsker) Ask(_ context.Context, _ string) (string, error) {
	return "", f.err
}

func TestRender_NilAskerReturnsError(t *testing.T) {
	prev := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false))}}
	curr := Snapshot{Tables: []driver.Table{table("Users",
		col("Id", "int", false), col("Email", "varchar", true))}}
	d := Compute(prev, curr)

	_, err := Render(context.Background(), nil, d, "public", "postgres")
	if err == nil {
		t.Fatal("expected error when ai is nil")
	}
	if !strings.Contains(err.Error(), "AI provider") {
		t.Errorf("error should explain AI is required, got: %v", err)
	}
}

func TestRender_AIErrorPropagates(t *testing.T) {
	asker := &failingAsker{err: errors.New("anthropic 429: rate limit")}
	prev := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false))}}
	curr := Snapshot{Tables: []driver.Table{table("Users",
		col("Id", "int", false), col("Email", "varchar", true))}}
	d := Compute(prev, curr)

	_, err := Render(context.Background(), asker, d, "public", "postgres")
	if err == nil {
		t.Fatal("expected propagated AI error")
	}
	if !strings.Contains(err.Error(), "rate limit") {
		t.Errorf("underlying error should be wrapped, got: %v", err)
	}
}

func TestRender_NonJSONResponseSurfacesRawText(t *testing.T) {
	// Some local providers (Ollama with the wrong model) return prose
	// instead of JSON. The error must include the raw text so the operator
	// can see what the model said.
	asker := &stubAsker{Response: "I cannot help with that request."}
	prev := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false))}}
	curr := Snapshot{Tables: []driver.Table{table("Users",
		col("Id", "int", false), col("Email", "varchar", true))}}
	d := Compute(prev, curr)

	_, err := Render(context.Background(), asker, d, "public", "postgres")
	if err == nil {
		t.Fatal("expected parse error")
	}
	if !strings.Contains(err.Error(), "I cannot help") {
		t.Errorf("error should include raw response for diagnosis, got: %v", err)
	}
}

func TestRender_EmptyStatementsListReturnsEmptyPlan(t *testing.T) {
	asker := &stubAsker{Response: `{"statements":[]}`}
	prev := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false))}}
	curr := Snapshot{Tables: []driver.Table{table("Users",
		col("Id", "int", false), col("Email", "varchar", true))}}
	d := Compute(prev, curr)

	plan, err := Render(context.Background(), asker, d, "public", "postgres")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !plan.IsEmpty() {
		t.Errorf("expected empty plan from empty statements list, got %+v", plan)
	}
}

func TestRender_ResponseWithExtraTextStillParses(t *testing.T) {
	// Models sometimes prefix the JSON with "Here is the migration plan:".
	// The renderer must extract the JSON object regardless.
	asker := &stubAsker{Response: `Here is your migration plan:
{"statements":[{"sql":"ALTER TABLE foo","description":"d","risk":"safe"}]}
Hope this helps!`}

	prev := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false))}}
	curr := Snapshot{Tables: []driver.Table{table("Users",
		col("Id", "int", false), col("Email", "varchar", true))}}
	d := Compute(prev, curr)

	plan, err := Render(context.Background(), asker, d, "public", "postgres")
	if err != nil {
		t.Fatalf("expected JSON to parse despite surrounding prose, got %v", err)
	}
	if len(plan.Statements) != 1 {
		t.Errorf("expected 1 statement, got %d", len(plan.Statements))
	}
}

func TestRender_PromptIncludesTargetDialectAndSchema(t *testing.T) {
	asker := &stubAsker{Response: `{"statements":[]}`}
	prev := Snapshot{Tables: []driver.Table{table("Users", col("Id", "int", false))}}
	curr := Snapshot{Tables: []driver.Table{table("Users",
		col("Id", "int", false), col("Email", "varchar", true))}}
	d := Compute(prev, curr)

	if _, err := Render(context.Background(), asker, d, "my_schema", "mysql"); err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(asker.GotPrompt, "mysql") {
		t.Errorf("prompt should mention target dialect, got: %s", asker.GotPrompt)
	}
	if !strings.Contains(asker.GotPrompt, "my_schema") {
		t.Errorf("prompt should mention target schema, got: %s", asker.GotPrompt)
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
// Table.Schema (populated by source introspection); when the AI sees
// those values in the prompt JSON it emits ALTER TABLE qualified to the
// source schema, which fails on the target. WithTargetSchema must
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
