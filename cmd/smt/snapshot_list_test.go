package main

import (
	"encoding/json"
	"testing"
	"time"

	"smt/internal/checkpoint"
	"smt/internal/driver"
	"smt/internal/schemadiff"
)

// saveSnap stores a schemadiff.Snapshot with tableCount empty tables.
func saveSnap(t *testing.T, state *checkpoint.State, src, schema string, capturedAt time.Time, tableCount int) {
	t.Helper()
	snap := schemadiff.Snapshot{
		Version:    schemadiff.CurrentSnapshotVersion,
		Schema:     schema,
		SourceType: src,
		CapturedAt: capturedAt,
		Tables:     make([]driver.Table, tableCount),
	}
	payload, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := state.SaveSnapshot(src, schema, capturedAt, payload); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
}

// TestSnapshotList covers the data + decode path behind `smt snapshot list`:
// newest-first ordering, the --limit cap, the default cap, and the payload
// table-count decode that the command renders.
func TestSnapshotList(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer state.Close()

	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	saveSnap(t, state, "mssql", "dbo", base, 3)
	saveSnap(t, state, "postgres", "public", base.Add(1*time.Hour), 14)
	saveSnap(t, state, "mysql", "app", base.Add(2*time.Hour), 7)

	// --limit caps the result and returns newest first.
	snaps, err := state.ListSnapshots(2)
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("limit=2 returned %d snapshots", len(snaps))
	}
	if snaps[0].SourceType != "mysql" || snaps[1].SourceType != "postgres" {
		t.Errorf("not newest-first: got %s, %s", snaps[0].SourceType, snaps[1].SourceType)
	}

	// The payload decodes to the table count the command prints.
	var snap schemadiff.Snapshot
	if err := json.Unmarshal(snaps[0].Payload, &snap); err != nil {
		t.Fatalf("payload decode: %v", err)
	}
	if len(snap.Tables) != 7 {
		t.Errorf("newest snapshot table count = %d, want 7", len(snap.Tables))
	}

	// limit <= 0 falls back to the default cap and returns everything stored.
	all, err := state.ListSnapshots(0)
	if err != nil {
		t.Fatalf("ListSnapshots(0): %v", err)
	}
	if len(all) != 3 {
		t.Errorf("default limit returned %d, want all 3", len(all))
	}
}
