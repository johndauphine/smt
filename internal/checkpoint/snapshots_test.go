package checkpoint

import (
	"testing"
	"time"
)

func TestSnapshotsIsolatedBySourceIdentity(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer state.Close()

	base := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	stagingID := "host=db.example.com;port=3306;database=staging"
	prodID := "host=db.example.com;port=3306;database=prod"
	if _, err := state.SaveSnapshot("mysql", stagingID, "app", base, []byte("staging")); err != nil {
		t.Fatalf("save staging: %v", err)
	}
	if _, err := state.SaveSnapshot("mysql", prodID, "app", base.Add(time.Hour), []byte("prod")); err != nil {
		t.Fatalf("save prod: %v", err)
	}

	got, err := state.GetLatestSnapshot("mysql", stagingID, "app")
	if err != nil {
		t.Fatalf("GetLatestSnapshot staging: %v", err)
	}
	if got == nil || string(got.Payload) != "staging" {
		t.Fatalf("staging lookup got %#v, want staging payload", got)
	}

	got, err = state.GetLatestSnapshot("mysql", prodID, "app")
	if err != nil {
		t.Fatalf("GetLatestSnapshot prod: %v", err)
	}
	if got == nil || string(got.Payload) != "prod" {
		t.Fatalf("prod lookup got %#v, want prod payload", got)
	}
}

func TestGetLatestSnapshotFallsBackToLegacyEmptyIdentity(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer state.Close()

	if _, err := state.SaveSnapshot("mssql", "", "dbo", time.Now().UTC(), []byte("legacy")); err != nil {
		t.Fatalf("save legacy: %v", err)
	}
	got, err := state.GetLatestSnapshot("mssql", "host=server;port=1433;database=crm", "dbo")
	if err != nil {
		t.Fatalf("GetLatestSnapshot: %v", err)
	}
	if got == nil || string(got.Payload) != "legacy" {
		t.Fatalf("legacy fallback got %#v, want legacy payload", got)
	}
}

func TestListSnapshotsIncludesSourceIdentity(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer state.Close()

	sourceID := "host=db.example.com;port=5432;database=crm"
	if _, err := state.SaveSnapshot("postgres", sourceID, "public", time.Now().UTC(), []byte("payload")); err != nil {
		t.Fatalf("save: %v", err)
	}
	snaps, err := state.ListSnapshots(1)
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("len(snaps) = %d, want 1", len(snaps))
	}
	if snaps[0].SourceIdentity != sourceID {
		t.Fatalf("SourceIdentity = %q, want %q", snaps[0].SourceIdentity, sourceID)
	}
}
