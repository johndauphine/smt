package checkpoint

// Schema snapshots: serialized point-in-time copies of a source schema,
// stored so `smt sync` can compute a diff against the last known state.
//
// Storage is one row per snapshot, keyed by source_type+schema+captured_at.
// The payload is opaque JSON written by the schemadiff package — this
// package does not unmarshal it, just keeps it intact.

import (
	"database/sql"
	"fmt"
	"time"
)

// SchemaSnapshot is one row in the schema_snapshots table.
type SchemaSnapshot struct {
	ID             int64
	SourceType     string
	SourceIdentity string
	Schema         string
	CapturedAt     time.Time
	Payload        []byte
}

// ensureSnapshotsTable creates the schema_snapshots table if missing.
// Called from State.init via ensureExtraTables.
func (s *State) ensureSnapshotsTable() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_snapshots (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			source_type TEXT NOT NULL,
			source_identity TEXT NOT NULL DEFAULT '',
			source_schema TEXT NOT NULL,
			captured_at TEXT NOT NULL,
			payload BLOB NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_snapshots_lookup
			ON schema_snapshots(source_type, source_schema, captured_at DESC);
		CREATE INDEX IF NOT EXISTS idx_snapshots_lookup_identity
			ON schema_snapshots(source_type, source_schema, source_identity, captured_at DESC);
	`)
	if err != nil {
		return err
	}

	columns, err := s.tableColumns("schema_snapshots")
	if err != nil {
		return err
	}
	hasSourceIdentity := false
	for _, col := range columns {
		if col == "source_identity" {
			hasSourceIdentity = true
			break
		}
	}
	if !hasSourceIdentity {
		// Ignore a concurrent process winning the ALTER race (duplicate
		// column), matching ensureRunColumns' handling of the same pattern.
		_, err := s.db.Exec(`ALTER TABLE schema_snapshots ADD COLUMN source_identity TEXT NOT NULL DEFAULT ''`)
		if err := ignoreDuplicateColumn(err); err != nil {
			return err
		}
	}
	_, err = s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_snapshots_lookup_identity
		ON schema_snapshots(source_type, source_schema, source_identity, captured_at DESC)`)
	return err
}

// SaveSnapshot persists a serialized source schema snapshot. Returns the
// new row's id.
func (s *State) SaveSnapshot(sourceType, sourceIdentity, schema string, capturedAt time.Time, payload []byte) (int64, error) {
	res, err := s.db.Exec(
		`INSERT INTO schema_snapshots (source_type, source_identity, source_schema, captured_at, payload) VALUES (?, ?, ?, ?, ?)`,
		sourceType, sourceIdentity, schema, capturedAt.Format(time.RFC3339Nano), payload,
	)
	if err != nil {
		return 0, fmt.Errorf("saving schema snapshot: %w", err)
	}
	return res.LastInsertId()
}

// GetLatestSnapshot returns the most recent snapshot for the given
// source_type+source_identity+schema, or (nil, nil) if none exists. Rows
// created before source_identity existed have an empty identity and remain a
// fallback for forward-compatible loading.
func (s *State) GetLatestSnapshot(sourceType, sourceIdentity, schema string) (*SchemaSnapshot, error) {
	row := s.db.QueryRow(
		`SELECT id, source_type, source_identity, source_schema, captured_at, payload
		 FROM schema_snapshots
		 WHERE source_type = ? AND source_schema = ?
		   AND (source_identity = ? OR source_identity = '')
		 ORDER BY CASE WHEN source_identity = ? THEN 0 ELSE 1 END, captured_at DESC LIMIT 1`,
		sourceType, schema, sourceIdentity, sourceIdentity,
	)
	return scanSnapshot(row)
}

// ListSnapshots returns the most recent N snapshots (by captured_at desc)
// across all source_type+schema combinations. Used by the history UI.
func (s *State) ListSnapshots(limit int) ([]SchemaSnapshot, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.Query(
		`SELECT id, source_type, source_identity, source_schema, captured_at, payload
		 FROM schema_snapshots ORDER BY captured_at DESC LIMIT ?`, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SchemaSnapshot
	for rows.Next() {
		snap, err := scanSnapshot(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *snap)
	}
	return out, rows.Err()
}

// scanRow is the common interface of *sql.Row and *sql.Rows for Scan.
type scanRow interface {
	Scan(dest ...any) error
}

func scanSnapshot(r scanRow) (*SchemaSnapshot, error) {
	var snap SchemaSnapshot
	var capturedAt string
	if err := r.Scan(&snap.ID, &snap.SourceType, &snap.SourceIdentity, &snap.Schema, &capturedAt, &snap.Payload); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	t, err := time.Parse(time.RFC3339Nano, capturedAt)
	if err != nil {
		return nil, fmt.Errorf("parsing captured_at: %w", err)
	}
	snap.CapturedAt = t
	return &snap, nil
}
