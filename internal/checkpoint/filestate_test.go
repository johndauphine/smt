package checkpoint

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileState_CreateCompleteAndRead(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.yaml")

	fs, err := NewFileState(stateFile)
	if err != nil {
		t.Fatalf("NewFileState: %v", err)
	}

	if err := fs.CreateRun("test123", RunKindApply, "dbo", "public", map[string]string{"key": "value"}, "myprofile", "/cfg"); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		t.Fatal("state file not created")
	}
	if info, err := os.Stat(stateFile); err != nil {
		t.Fatalf("stat state file: %v", err)
	} else if got := info.Mode().Perm(); got != 0600 {
		t.Fatalf("state file mode = %o, want 0600", got)
	}

	r, err := fs.GetRunByID("test123")
	if err != nil || r == nil {
		t.Fatalf("GetRunByID: %v %v", r, err)
	}
	if r.Status != "running" || r.ProfileName != "myprofile" {
		t.Errorf("unexpected run: status=%q profile=%q", r.Status, r.ProfileName)
	}
	if r.StartedAt.Location() != time.UTC {
		t.Fatalf("StartedAt location = %v, want UTC", r.StartedAt.Location())
	}

	if err := fs.UpdatePhase("test123", "finalizing"); err != nil {
		t.Fatalf("UpdatePhase: %v", err)
	}
	if err := fs.CompleteRun("test123", "success", ""); err != nil {
		t.Fatalf("CompleteRun: %v", err)
	}

	runs, err := fs.GetAllRuns()
	if err != nil {
		t.Fatalf("GetAllRuns: %v", err)
	}
	if len(runs) != 1 || runs[0].Status != "success" || runs[0].Phase != "finalizing" {
		t.Fatalf("GetAllRuns = %+v, want one completed run", runs)
	}
}

func TestFileState_LoadExisting(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.yaml")

	// A pre-existing file, including a legacy `tables:` block that the v1 reader
	// ignores (forward-compat).
	existing := `run_id: existing123
started_at: 2025-12-20T10:00:00Z
status: running
phase: finalizing
source_schema: dbo
target_schema: public
profile_name: p
tables:
  transfer:dbo.Users:
    status: success
    task_id: 1001
`
	if err := os.WriteFile(stateFile, []byte(existing), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.Chmod(stateFile, 0644); err != nil {
		t.Fatalf("Chmod: %v", err)
	}

	fs, err := NewFileState(stateFile)
	if err != nil {
		t.Fatalf("NewFileState: %v", err)
	}

	r, err := fs.GetRunByID("existing123")
	if err != nil || r == nil {
		t.Fatalf("GetRunByID: %v %v", r, err)
	}
	if r.Status != "failed" || r.Phase != "finalizing" || r.ProfileName != "p" {
		t.Errorf("unexpected run: %+v", r)
	}
	if r.CompletedAt == nil {
		t.Fatal("CompletedAt = nil, want interrupted timestamp")
	}
	if info, err := os.Stat(stateFile); err != nil {
		t.Fatalf("stat state file: %v", err)
	} else if got := info.Mode().Perm(); got != 0600 {
		t.Fatalf("state file mode = %o, want 0600", got)
	}
}
