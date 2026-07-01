package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"smt/internal/checkpoint"
	"smt/internal/config"
	"smt/internal/driver"
	"smt/internal/schemadiff"
)

// stubExecutor records every SQL it's asked to run and optionally fails
// on a specific statement index (failAt). Used for applyPlan tests.
type stubExecutor struct {
	executed []string
	failAt   int // 1-based; 0 means never fail
	failErr  error
}

func (s *stubExecutor) ExecRaw(_ context.Context, query string, _ ...any) (int64, error) {
	s.executed = append(s.executed, query)
	if s.failAt > 0 && len(s.executed) == s.failAt {
		return 0, s.failErr
	}
	return 1, nil
}

func TestApplyPlan_HappyPath(t *testing.T) {
	exec := &stubExecutor{}
	plan := schemadiff.Plan{Statements: []schemadiff.Statement{
		{SQL: "ALTER TABLE x ADD COLUMN a int", Description: "add a", Risk: schemadiff.RiskSafe},
		{SQL: "ALTER TABLE x ADD COLUMN b int", Description: "add b", Risk: schemadiff.RiskSafe},
	}}

	if err := applyPlan(context.Background(), exec, plan); err != nil {
		t.Fatalf("expected no err, got %v", err)
	}
	if len(exec.executed) != 2 {
		t.Fatalf("expected 2 statements executed, got %d", len(exec.executed))
	}
}

func TestApplyPlan_StopsAtFirstFailure(t *testing.T) {
	exec := &stubExecutor{failAt: 2, failErr: errors.New("syntax error near token")}
	plan := schemadiff.Plan{Statements: []schemadiff.Statement{
		{SQL: "ALTER 1", Description: "stmt 1", Risk: schemadiff.RiskSafe},
		{SQL: "ALTER 2 BAD", Description: "stmt 2", Risk: schemadiff.RiskBlocking},
		{SQL: "ALTER 3", Description: "stmt 3", Risk: schemadiff.RiskSafe},
	}}

	err := applyPlan(context.Background(), exec, plan)
	if err == nil {
		t.Fatal("expected error from stmt 2 failure")
	}
	if !strings.Contains(err.Error(), "statement 2") {
		t.Errorf("error should identify statement 2, got: %v", err)
	}
	if !strings.Contains(err.Error(), "ALTER 2 BAD") {
		t.Errorf("error should include the failing SQL, got: %v", err)
	}
	if len(exec.executed) != 2 {
		t.Fatalf("expected to stop after 2 statements, executed %d", len(exec.executed))
	}
}

func TestApplyPlan_EmptyPlan(t *testing.T) {
	exec := &stubExecutor{}
	if err := applyPlan(context.Background(), exec, schemadiff.Plan{}); err != nil {
		t.Fatalf("empty plan should be a no-op, got %v", err)
	}
	if len(exec.executed) != 0 {
		t.Fatalf("empty plan executed %d statements", len(exec.executed))
	}
}

func TestFormatUnsupportedChangesIncludesManualActionContext(t *testing.T) {
	out := formatUnsupportedChanges([]schemadiff.UnsupportedChange{{
		Table:       "users",
		Description: "change primary key",
		Reason:      "primary-key changes are not supported by deterministic sync",
	}})
	for _, want := range []string{
		"Unsupported change(s) skipped: 1",
		"change primary key",
		"table users",
		"primary-key changes are not supported by deterministic sync",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("unsupported output missing %q:\n%s", want, out)
		}
	}
}

// stubLoader records which tables had each loader called, and can fail
// on a specific table+method combination.
type stubLoader struct {
	indexCalls   []string
	fkCalls      []string
	checkCalls   []string
	failOnTable  string
	failOnMethod string // "indexes" | "fks" | "checks"
	err          error
}

func (s *stubLoader) LoadIndexes(_ context.Context, t *driver.Table) error {
	s.indexCalls = append(s.indexCalls, t.Name)
	if s.failOnTable == t.Name && s.failOnMethod == "indexes" {
		return s.err
	}
	return nil
}

func (s *stubLoader) LoadForeignKeys(_ context.Context, t *driver.Table) error {
	s.fkCalls = append(s.fkCalls, t.Name)
	if s.failOnTable == t.Name && s.failOnMethod == "fks" {
		return s.err
	}
	return nil
}

func (s *stubLoader) LoadCheckConstraints(_ context.Context, t *driver.Table) error {
	s.checkCalls = append(s.checkCalls, t.Name)
	if s.failOnTable == t.Name && s.failOnMethod == "checks" {
		return s.err
	}
	return nil
}

func TestLoadAllConstraints_HappyPath(t *testing.T) {
	loader := &stubLoader{}
	tables := []driver.Table{{Name: "Users"}, {Name: "Posts"}}

	if err := loadAllConstraints(context.Background(), loader, tables); err != nil {
		t.Fatalf("expected no err, got %v", err)
	}

	for _, want := range []string{"Users", "Posts"} {
		if !contains(loader.indexCalls, want) {
			t.Errorf("LoadIndexes never called for %s", want)
		}
		if !contains(loader.fkCalls, want) {
			t.Errorf("LoadForeignKeys never called for %s", want)
		}
		if !contains(loader.checkCalls, want) {
			t.Errorf("LoadCheckConstraints never called for %s", want)
		}
	}
}

func TestLoadAllConstraints_StopsAtFirstFailure(t *testing.T) {
	loader := &stubLoader{
		failOnTable:  "Posts",
		failOnMethod: "fks",
		err:          errors.New("permission denied"),
	}
	tables := []driver.Table{{Name: "Users"}, {Name: "Posts"}, {Name: "Comments"}}

	err := loadAllConstraints(context.Background(), loader, tables)
	if err == nil {
		t.Fatal("expected FK loader failure to surface")
	}
	if !strings.Contains(err.Error(), "Posts") {
		t.Errorf("error should mention failing table Posts, got: %v", err)
	}
	if !strings.Contains(err.Error(), "FKs") {
		t.Errorf("error should mention which constraint type failed, got: %v", err)
	}
	// Comments should never have been touched.
	if contains(loader.indexCalls, "Comments") {
		t.Errorf("loader continued past failure to Comments")
	}
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func TestLoadPreviousSnapshot_NoSnapshotReturnsHelpfulError(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("setup state: %v", err)
	}
	defer state.Close()

	_, err = loadPreviousSnapshot(state, "mssql", "dbo")
	if err == nil {
		t.Fatal("expected error when no snapshot exists")
	}
	if !strings.Contains(err.Error(), "smt snapshot") {
		t.Errorf("error should tell user to run `smt snapshot`, got: %v", err)
	}
	if !strings.Contains(err.Error(), "mssql/dbo") {
		t.Errorf("error should identify which (sourceType, schema) had no snapshot, got: %v", err)
	}
}

func TestLoadPreviousSnapshot_RoundTrip(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("setup state: %v", err)
	}
	defer state.Close()

	want := schemadiff.Snapshot{
		Schema:     "dbo",
		SourceType: "mssql",
		CapturedAt: time.Now().UTC().Truncate(time.Microsecond),
		Tables:     []driver.Table{{Schema: "dbo", Name: "Users"}},
	}
	payload, _ := json.Marshal(want)
	if _, err := state.SaveSnapshot(want.SourceType, want.Schema, want.CapturedAt, payload); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := loadPreviousSnapshot(state, "mssql", "dbo")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.SourceType != want.SourceType || got.Schema != want.Schema {
		t.Errorf("snapshot round-trip mismatch: got %+v, want %+v", got, want)
	}
	if len(got.Tables) != 1 || got.Tables[0].Name != "Users" {
		t.Errorf("table list not preserved: got %+v", got.Tables)
	}
}

// snapshotSyncCfg builds the minimal config buildSnapshotSyncPlan needs:
// mssql source, postgres target, all object kinds managed (the loaded-config
// default), no scope filters unless set by the test.
func snapshotSyncCfg() *config.Config {
	cfg := &config.Config{}
	cfg.Source.Type = "mssql"
	cfg.Source.Schema = "dbo"
	cfg.Target.Type = "postgres"
	cfg.Target.Schema = "public"
	cfg.SchemaGeneration.UnknownTypePolicy = "fail"
	cfg.Migration.CreateIndexes = true
	cfg.Migration.CreateForeignKeys = true
	cfg.Migration.CreateCheckConstraints = true
	return cfg
}

func snapshotWith(tables ...driver.Table) schemadiff.Snapshot {
	return schemadiff.Snapshot{
		Version:    schemadiff.CurrentSnapshotVersion,
		Schema:     "dbo",
		SourceType: "mssql",
		CapturedAt: time.Now().UTC(),
		Tables:     tables,
	}
}

func TestBuildSnapshotSyncPlan_AddedColumn(t *testing.T) {
	prev := snapshotWith(driver.Table{
		Schema:  "dbo",
		Name:    "Users",
		Columns: []driver.Column{{Name: "ID", DataType: "int", IsNullable: false}},
	})
	curr := snapshotWith(driver.Table{
		Schema: "dbo",
		Name:   "Users",
		Columns: []driver.Column{
			{Name: "ID", DataType: "int", IsNullable: false},
			{Name: "Age", DataType: "int", IsNullable: true},
		},
	})

	diff, plan, err := buildSnapshotSyncPlan(prev, curr, snapshotSyncCfg())
	if err != nil {
		t.Fatalf("expected no err, got %v", err)
	}
	if diff.IsEmpty() {
		t.Fatal("expected a non-empty diff for an added column")
	}
	if len(plan.Statements) != 1 {
		t.Fatalf("expected 1 statement, got %d: %+v", len(plan.Statements), plan.Statements)
	}
	sql := plan.Statements[0].SQL
	// Identifiers must be normalized to the postgres convention and the
	// schema retargeted, so the ALTER hits the table that exists there.
	if !strings.Contains(sql, "users") || !strings.Contains(sql, "age") {
		t.Errorf("statement should reference normalized identifiers, got: %s", sql)
	}
	if !strings.Contains(sql, "public") {
		t.Errorf("statement should be qualified with the target schema, got: %s", sql)
	}
	if strings.Contains(sql, "dbo") {
		t.Errorf("statement must not reference the source schema, got: %s", sql)
	}
	if plan.Statements[0].Risk != schemadiff.RiskSafe {
		t.Errorf("adding a nullable column should be RiskSafe, got %s", plan.Statements[0].Risk)
	}
}

func TestBuildSnapshotSyncPlan_NoChanges(t *testing.T) {
	table := driver.Table{
		Schema:  "dbo",
		Name:    "Users",
		Columns: []driver.Column{{Name: "ID", DataType: "int", IsNullable: false}},
	}
	diff, plan, err := buildSnapshotSyncPlan(snapshotWith(table), snapshotWith(table), snapshotSyncCfg())
	if err != nil {
		t.Fatalf("expected no err, got %v", err)
	}
	if !diff.IsEmpty() {
		t.Errorf("identical snapshots should diff empty, got: %s", diff.Summary())
	}
	if !plan.IsEmpty() {
		t.Errorf("identical snapshots should render no statements, got %d", len(plan.Statements))
	}
}

func TestBuildSnapshotSyncPlan_ExcludedTableIgnored(t *testing.T) {
	prev := snapshotWith(driver.Table{
		Schema:  "dbo",
		Name:    "Audit",
		Columns: []driver.Column{{Name: "ID", DataType: "int"}},
	})
	curr := snapshotWith(driver.Table{
		Schema: "dbo",
		Name:   "Audit",
		Columns: []driver.Column{
			{Name: "ID", DataType: "int"},
			{Name: "Extra", DataType: "int", IsNullable: true},
		},
	})

	cfg := snapshotSyncCfg()
	cfg.Migration.ExcludeTables = []string{"Audit"}
	diff, plan, err := buildSnapshotSyncPlan(prev, curr, cfg)
	if err != nil {
		t.Fatalf("expected no err, got %v", err)
	}
	if !diff.IsEmpty() {
		t.Errorf("change in an excluded table should not appear in the diff, got: %s", diff.Summary())
	}
	if !plan.IsEmpty() {
		t.Errorf("excluded table produced statements: %+v", plan.Statements)
	}
}

// buildSnapshotSyncPlan must never mutate its input snapshots: the caller
// persists currSnap as the next baseline (--apply --save-snapshot), and the
// diff's tables share slice backing arrays with it. A regression here means
// the saved baseline carries target-normalized names and the next sync
// proposes drop+re-add of every column.
func TestBuildSnapshotSyncPlan_DoesNotMutateInputSnapshots(t *testing.T) {
	mkSnap := func() schemadiff.Snapshot {
		return snapshotWith(
			driver.Table{
				Schema:     "dbo",
				Name:       "Users",
				PrimaryKey: []string{"ID"},
				Columns:    []driver.Column{{Name: "ID", DataType: "int", IsNullable: false}},
				Indexes:    []driver.Index{{Name: "IX_Users_ID", Columns: []string{"ID"}}},
				ForeignKeys: []driver.ForeignKey{{
					Name: "FK_Users_Orgs", Columns: []string{"OrgID"},
					RefSchema: "dbo", RefTable: "Orgs", RefColumns: []string{"ID"},
				}},
			},
		)
	}
	prev := snapshotWith() // empty baseline: Users becomes an added table
	curr := mkSnap()
	want := mkSnap()

	if _, _, err := buildSnapshotSyncPlan(prev, curr, snapshotSyncCfg()); err != nil {
		t.Fatalf("buildSnapshotSyncPlan: %v", err)
	}

	// Compare Tables only: CapturedAt differs between the two constructions.
	got, wantJSON := mustJSON(t, curr.Tables), mustJSON(t, want.Tables)
	if got != wantJSON {
		t.Errorf("input snapshot mutated by plan build:\ngot:  %s\nwant: %s", got, wantJSON)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

// Unmanaged object kinds must not produce statements: create_indexes etc.
// gate snapshot-mode deltas exactly like the live-target mode.
func TestBuildSnapshotSyncPlan_UnmanagedKindsGated(t *testing.T) {
	base := driver.Table{
		Schema:  "dbo",
		Name:    "Users",
		Columns: []driver.Column{{Name: "ID", DataType: "int", IsNullable: false}},
	}
	withIndex := base
	withIndex.Indexes = []driver.Index{{Name: "IX_Users_ID", Columns: []string{"ID"}}}

	cfg := snapshotSyncCfg()
	cfg.Migration.CreateIndexes = false

	diff, plan, err := buildSnapshotSyncPlan(snapshotWith(base), snapshotWith(withIndex), cfg)
	if err != nil {
		t.Fatalf("buildSnapshotSyncPlan: %v", err)
	}
	if !diff.IsEmpty() || !plan.IsEmpty() {
		t.Errorf("index-only change with create_indexes=false should gate to empty; diff=%s statements=%d",
			diff.Summary(), len(plan.Statements))
	}
}

func TestGatePlanForApply_RefusesDataLossWithoutFlag(t *testing.T) {
	plan := schemadiff.Plan{Statements: []schemadiff.Statement{
		{SQL: "ALTER TABLE public.users ADD COLUMN age integer", Risk: schemadiff.RiskSafe},
		{SQL: "DROP TABLE public.audit", Risk: schemadiff.RiskDataLoss},
	}}

	if err := gatePlanForApply(plan, false); err == nil {
		t.Fatal("expected data-loss plan to be refused without --allow-data-loss")
	}
	if err := gatePlanForApply(plan, true); err != nil {
		t.Fatalf("expected data-loss plan to pass with --allow-data-loss, got %v", err)
	}
}

func TestGatePlanForApply_SafePlanPasses(t *testing.T) {
	plan := schemadiff.Plan{Statements: []schemadiff.Statement{
		{SQL: "ALTER TABLE public.users ADD COLUMN age integer", Risk: schemadiff.RiskSafe},
	}}
	if err := gatePlanForApply(plan, false); err != nil {
		t.Fatalf("safe plan should pass the gate, got %v", err)
	}
}

func TestLoadPreviousSnapshot_MalformedPayload(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("setup state: %v", err)
	}
	defer state.Close()

	// Save a payload that isn't valid JSON for a Snapshot.
	if _, err := state.SaveSnapshot("mssql", "dbo", time.Now(), []byte("{not json")); err != nil {
		t.Fatalf("save: %v", err)
	}

	_, err = loadPreviousSnapshot(state, "mssql", "dbo")
	if err == nil {
		t.Fatal("expected unmarshal error")
	}
	if !strings.Contains(err.Error(), "decoding stored snapshot") {
		t.Errorf("error should mention decoding failure, got: %v", err)
	}
}
