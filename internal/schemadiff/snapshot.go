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

// Snapshot is a serializable point-in-time view of a source schema.
type Snapshot struct {
	Schema     string         `json:"schema"`
	SourceType string         `json:"source_type"`
	CapturedAt time.Time      `json:"captured_at"`
	Tables     []driver.Table `json:"tables"`
}
