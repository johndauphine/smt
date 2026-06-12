package checkpoint

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCleanupOldRuns(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	oldSuccess := "old-success"
	oldFailed := "old-failed"
	recentSuccess := "recent-success"
	running := "running"

	for _, runID := range []string{oldSuccess, oldFailed, recentSuccess, running} {
		if err := state.CreateRun(runID, RunKindApply, "dbo", "public", map[string]string{"run": runID}, "", ""); err != nil {
			t.Fatalf("CreateRun(%s) error: %v", runID, err)
		}
	}

	if err := state.CompleteRun(oldSuccess, "success", ""); err != nil {
		t.Fatalf("CompleteRun(%s) error: %v", oldSuccess, err)
	}
	if err := state.CompleteRun(oldFailed, "failed", "boom"); err != nil {
		t.Fatalf("CompleteRun(%s) error: %v", oldFailed, err)
	}
	if err := state.CompleteRun(recentSuccess, "success", ""); err != nil {
		t.Fatalf("CompleteRun(%s) error: %v", recentSuccess, err)
	}

	oldTime := time.Now().AddDate(0, 0, -31).Format("2006-01-02 15:04:05")
	if _, err := state.db.Exec(`UPDATE runs SET completed_at = ? WHERE id IN (?, ?)`, oldTime, oldSuccess, oldFailed); err != nil {
		t.Fatalf("update old completed_at error: %v", err)
	}

	recentTime := time.Now().AddDate(0, 0, -1).Format("2006-01-02 15:04:05")
	if _, err := state.db.Exec(`UPDATE runs SET completed_at = ? WHERE id = ?`, recentTime, recentSuccess); err != nil {
		t.Fatalf("update recent completed_at error: %v", err)
	}

	taskIDs := make(map[string]int64)
	for _, runID := range []string{oldSuccess, oldFailed, recentSuccess, running} {
		taskID, err := state.CreateTask(runID, "transfer", "transfer:dbo.Table")
		if err != nil {
			t.Fatalf("CreateTask(%s) error: %v", runID, err)
		}
		taskIDs[runID] = taskID
		if err := state.SaveTransferProgress(taskID, "Table", nil, int64(1), 10, 100); err != nil {
			t.Fatalf("SaveTransferProgress(%s) error: %v", runID, err)
		}
		if _, err := state.db.Exec(`INSERT INTO task_outputs (task_id, key, value) VALUES (?, ?, ?)`, taskID, "k", "v"); err != nil {
			t.Fatalf("insert task_outputs(%s) error: %v", runID, err)
		}
	}

	deleted, err := state.CleanupOldRuns(30)
	if err != nil {
		t.Fatalf("CleanupOldRuns error: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted runs = %d, want 2", deleted)
	}

	if got := countRows(t, state.db, `SELECT COUNT(*) FROM runs`); got != 2 {
		t.Fatalf("runs remaining = %d, want 2", got)
	}
	if got := countRows(t, state.db, `SELECT COUNT(*) FROM runs WHERE id = ?`, running); got != 1 {
		t.Fatalf("running run missing after cleanup")
	}
	if got := countRows(t, state.db, `SELECT COUNT(*) FROM tasks`); got != 2 {
		t.Fatalf("tasks remaining = %d, want 2", got)
	}
	if got := countRows(t, state.db, `SELECT COUNT(*) FROM transfer_progress`); got != 2 {
		t.Fatalf("transfer_progress remaining = %d, want 2", got)
	}
	if got := countRows(t, state.db, `SELECT COUNT(*) FROM task_outputs`); got != 2 {
		t.Fatalf("task_outputs remaining = %d, want 2", got)
	}
}

func countRows(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatalf("count query error: %v", err)
	}
	return count
}

func TestSyncTimestamps(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	sourceSchema := "dbo"
	tableName := "Orders"
	targetSchema := "public"

	// Test: Get timestamp for table with no prior sync
	ts, err := state.GetLastSyncTimestamp(sourceSchema, tableName, targetSchema)
	if err != nil {
		t.Fatalf("GetLastSyncTimestamp() error: %v", err)
	}
	if ts != nil {
		t.Errorf("Expected nil timestamp for first sync, got %v", ts)
	}

	// Test: Update sync timestamp
	syncTime := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	if err := state.UpdateSyncTimestamp(sourceSchema, tableName, targetSchema, syncTime); err != nil {
		t.Fatalf("UpdateSyncTimestamp() error: %v", err)
	}

	// Test: Get updated timestamp
	ts, err = state.GetLastSyncTimestamp(sourceSchema, tableName, targetSchema)
	if err != nil {
		t.Fatalf("GetLastSyncTimestamp() error: %v", err)
	}
	if ts == nil {
		t.Fatal("Expected non-nil timestamp after update")
	}
	if !ts.Equal(syncTime) {
		t.Errorf("Timestamp mismatch: got %v, want %v", ts, syncTime)
	}

	// Test: Update with newer timestamp (upsert)
	newerTime := time.Date(2024, 6, 16, 12, 0, 0, 0, time.UTC)
	if err := state.UpdateSyncTimestamp(sourceSchema, tableName, targetSchema, newerTime); err != nil {
		t.Fatalf("UpdateSyncTimestamp() error: %v", err)
	}

	ts, err = state.GetLastSyncTimestamp(sourceSchema, tableName, targetSchema)
	if err != nil {
		t.Fatalf("GetLastSyncTimestamp() error: %v", err)
	}
	if !ts.Equal(newerTime) {
		t.Errorf("Timestamp not updated: got %v, want %v", ts, newerTime)
	}

	// Test: Different table should have no timestamp
	ts, err = state.GetLastSyncTimestamp(sourceSchema, "OtherTable", targetSchema)
	if err != nil {
		t.Fatalf("GetLastSyncTimestamp() error: %v", err)
	}
	if ts != nil {
		t.Errorf("Expected nil timestamp for different table, got %v", ts)
	}

	// Test: Same table, different target schema should have no timestamp
	ts, err = state.GetLastSyncTimestamp(sourceSchema, tableName, "other_schema")
	if err != nil {
		t.Fatalf("GetLastSyncTimestamp() error: %v", err)
	}
	if ts != nil {
		t.Errorf("Expected nil timestamp for different target schema, got %v", ts)
	}
}

func TestUpdateRunConfig(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	const runID = "run-update-config"
	original := map[string]any{"workers": 12, "chunk_size": 113510}
	if err := state.CreateRun(runID, RunKindApply, "dbo", "public", original, "", ""); err != nil {
		t.Fatalf("CreateRun error: %v", err)
	}

	updated := map[string]any{"workers": 6, "chunk_size": 100000}
	if err := state.UpdateRunConfig(runID, updated); err != nil {
		t.Fatalf("UpdateRunConfig error: %v", err)
	}

	run, err := state.GetRunByID(runID)
	if err != nil {
		t.Fatalf("GetRunByID error: %v", err)
	}
	if run == nil {
		t.Fatalf("run not found after UpdateRunConfig")
	}
	if !strings.Contains(run.Config, `"workers":6`) || !strings.Contains(run.Config, `"chunk_size":100000`) {
		t.Errorf("config not updated, got: %s", run.Config)
	}
	if strings.Contains(run.Config, `"workers":12`) {
		t.Errorf("config still contains pre-update workers=12: %s", run.Config)
	}

	// Unknown run id should return an error rather than silently succeeding.
	if err := state.UpdateRunConfig("does-not-exist", updated); err == nil {
		t.Errorf("expected error for unknown run id, got nil")
	}
}

func TestCountPartitionTasks(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	runID := "test-run"
	if err := state.CreateRun(runID, RunKindApply, "public", "public", nil, "", ""); err != nil {
		t.Fatalf("CreateRun() error: %v", err)
	}

	// Create partition tasks for a table
	for i := 1; i <= 6; i++ {
		key := fmt.Sprintf("transfer:public.votes:p%d", i)
		if _, err := state.CreateTask(runID, "transfer", key); err != nil {
			t.Fatalf("CreateTask(%s) error: %v", key, err)
		}
	}

	// Create a non-partition task for the same table (should not be counted)
	if _, err := state.CreateTask(runID, "transfer", "transfer:public.votes"); err != nil {
		t.Fatalf("CreateTask() error: %v", err)
	}

	// Create partition tasks for a different table
	for i := 1; i <= 3; i++ {
		key := fmt.Sprintf("transfer:public.posts:p%d", i)
		if _, err := state.CreateTask(runID, "transfer", key); err != nil {
			t.Fatalf("CreateTask(%s) error: %v", key, err)
		}
	}

	// Count votes partitions
	count, err := state.CountPartitionTasks(runID, "transfer:public.votes")
	if err != nil {
		t.Fatalf("CountPartitionTasks() error: %v", err)
	}
	if count != 6 {
		t.Errorf("expected 6 partition tasks for votes, got %d", count)
	}

	// Count posts partitions
	count, err = state.CountPartitionTasks(runID, "transfer:public.posts")
	if err != nil {
		t.Fatalf("CountPartitionTasks() error: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 partition tasks for posts, got %d", count)
	}

	// Table with underscores — underscore must not act as wildcard
	for i := 1; i <= 2; i++ {
		key := fmt.Sprintf("transfer:public.order_items:p%d", i)
		if _, err := state.CreateTask(runID, "transfer", key); err != nil {
			t.Fatalf("CreateTask(%s) error: %v", key, err)
		}
	}
	count, err = state.CountPartitionTasks(runID, "transfer:public.order_items")
	if err != nil {
		t.Fatalf("CountPartitionTasks() error: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 partition tasks for order_items, got %d", count)
	}

	// Non-existent table
	count, err = state.CountPartitionTasks(runID, "transfer:public.nonexistent")
	if err != nil {
		t.Fatalf("CountPartitionTasks() error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 partition tasks for nonexistent table, got %d", count)
	}
}
