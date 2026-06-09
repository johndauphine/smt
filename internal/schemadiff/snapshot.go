// Package schemadiff captures point-in-time source schemas as snapshots,
// computes the delta between two snapshots, and renders that delta as
// dialect-specific ALTER / CREATE / DROP statements for the target.
//
// SMT's `snapshot` command stores a Snapshot in the checkpoint database;
// `sync` loads the latest snapshot, compares it with the current source
// schema, asks the AI advisor whether the changes are safe, and either
// emits the resulting SQL for review (default) or executes it against
// the target with --apply.
package schemadiff

import (
	"time"

	"smt/internal/driver"
)

// CurrentSnapshotVersion is stamped into every new snapshot. Bump it whenever
// driver.Column (or anything else snapshot-persisted) gains a field that
// columnsEqual compares, and teach backfillPreVersionFields how to fill the
// gap for older snapshots.
//
// Version history:
//   - 0/1: original format (a snapshot without a version field decodes as 0)
//   - 2: Column gained IsUnsigned, EnumValues, OnUpdateExpression
const CurrentSnapshotVersion = 2

// Snapshot is a serializable point-in-time view of a source schema.
type Snapshot struct {
	Version    int            `json:"version,omitempty"`
	Schema     string         `json:"schema"`
	SourceType string         `json:"source_type"`
	CapturedAt time.Time      `json:"captured_at"`
	Tables     []driver.Table `json:"tables"`
}
