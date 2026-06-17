package checkpoint

import (
	"database/sql"
	"testing"
)

func countRows(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatalf("count query error: %v", err)
	}
	return count
}

// TestRunLifecycle covers the kept run surface: create, phase, complete, and
// the two read paths used by history.
func TestRunLifecycle(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	const runID = "run-1"
	if err := state.CreateRun(runID, RunKindApply, "dbo", "public", map[string]string{"k": "v"}, "prof", "/cfg.yaml"); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if err := state.UpdatePhase(runID, "finalizing"); err != nil {
		t.Fatalf("UpdatePhase: %v", err)
	}
	if err := state.CompleteRun(runID, "success", ""); err != nil {
		t.Fatalf("CompleteRun: %v", err)
	}

	r, err := state.GetRunByID(runID)
	if err != nil || r == nil {
		t.Fatalf("GetRunByID: %v %v", r, err)
	}
	if r.Status != "success" || r.ProfileName != "prof" {
		t.Errorf("unexpected run: status=%q profile=%q", r.Status, r.ProfileName)
	}
	// GetRunByID does not surface phase; confirm UpdatePhase persisted it.
	if got := countRows(t, state.db, `SELECT COUNT(*) FROM runs WHERE id=? AND phase='finalizing'`, runID); got != 1 {
		t.Errorf("phase not persisted as finalizing")
	}

	runs, err := state.GetAllRuns()
	if err != nil {
		t.Fatalf("GetAllRuns: %v", err)
	}
	if len(runs) != 1 || runs[0].ID != runID {
		t.Fatalf("GetAllRuns = %+v, want one run %q", runs, runID)
	}
}

// TestDeadTablesNotCreated asserts the DMT-era tables are gone from fresh DBs
// while the SMT-active tables remain (#158).
func TestDeadTablesNotCreated(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	exists := func(name string) bool {
		return countRows(t, state.db,
			`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name) > 0
	}

	for _, dead := range []string{"tasks", "task_outputs", "transfer_progress", "table_sync_timestamps"} {
		if exists(dead) {
			t.Errorf("dead table %q should not be created", dead)
		}
	}
	for _, live := range []string{"runs", "profiles", "schema_snapshots"} {
		if !exists(live) {
			t.Errorf("live table %q is missing", live)
		}
	}
}

// TestOpensLegacyDBWithDeadTables is the forward-compat guarantee (option a):
// an old state DB that still has the dead tables opens cleanly and keeps working.
func TestOpensLegacyDBWithDeadTables(t *testing.T) {
	dir := t.TempDir()

	// First open creates the v1 schema.
	s1, err := New(dir)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	// Simulate a pre-#158 database by recreating a dead table with a stray row.
	if _, err := s1.db.Exec(`CREATE TABLE IF NOT EXISTS table_sync_timestamps (
		source_schema TEXT, table_name TEXT, target_schema TEXT,
		last_sync_timestamp TEXT, updated_at TEXT,
		PRIMARY KEY (source_schema, table_name, target_schema))`); err != nil {
		t.Fatalf("seed dead table: %v", err)
	}
	if _, err := s1.db.Exec(`INSERT INTO table_sync_timestamps VALUES ('dbo','t','public','x','y')`); err != nil {
		t.Fatalf("seed dead row: %v", err)
	}
	s1.Close()

	// Re-open: migrate() must not choke on the orphan table, and runs still work.
	s2, err := New(dir)
	if err != nil {
		t.Fatalf("reopen with orphan dead table: %v", err)
	}
	defer s2.Close()
	if err := s2.CreateRun("r", RunKindApply, "dbo", "public", nil, "", ""); err != nil {
		t.Fatalf("CreateRun after reopen: %v", err)
	}
	if r, err := s2.GetRunByID("r"); err != nil || r == nil {
		t.Fatalf("GetRunByID after reopen: %v %v", r, err)
	}
}
