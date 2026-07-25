package driver

import (
	"context"
	"database/sql"

	"github.com/johndauphine/smt/internal/stats"
)

// Writer represents a database writer that executes DDL against a target
// database. (SMT is schema-only; the DMT bulk-write surface — WriteBatch,
// UpsertBatch, ResetSequence, row counts, SetTableLogged — was removed in
// #191. DDL creation goes through the orchestrator's plan executor: render
// once, ExecRaw per statement.)
type Writer interface {
	// Connection management
	Close()
	Ping(ctx context.Context) error
	DB() *sql.DB // Access to underlying database connection

	// DatabaseContext returns cached metadata about the target database
	// (version, charset, collation) for optional AI review context.
	DatabaseContext() *DatabaseContext

	// Schema operations. DDL creation goes through the orchestrator's plan
	// executor (render once, ExecRaw per statement); writers only expose the
	// catalog existence checks the executor gates on (#87).
	DropTable(ctx context.Context, schema, table string) error
	TruncateTable(ctx context.Context, schema, table string) error
	TableExists(ctx context.Context, schema, table string) (bool, error)

	// IndexExists reports whether an index with the given name exists on the
	// target table. Used by the plan executor to short-circuit re-runs
	// without executing DDL that would fail with "already exists".
	IndexExists(ctx context.Context, schema, table, indexName string) (bool, error)

	// ForeignKeyExists reports whether a foreign key with the given name
	// exists on the target table. Used by the plan executor for idempotent
	// re-runs.
	ForeignKeyExists(ctx context.Context, schema, table, fkName string) (bool, error)

	// CheckConstraintExists reports whether a CHECK constraint with the given
	// name exists on the target table. Used by the plan executor for
	// idempotent re-runs.
	CheckConstraintExists(ctx context.Context, schema, table, checkName string) (bool, error)

	// DDL introspection
	// GetTableDDL returns the CREATE TABLE DDL for an existing table.
	// Returns empty string if DDL cannot be retrieved (non-fatal).
	GetTableDDL(ctx context.Context, schema, table string) string

	// Raw SQL execution for cleanup and special operations
	// Returns the number of rows affected and any error.
	ExecRaw(ctx context.Context, query string, args ...any) (int64, error)

	// Raw SQL query for single row results (e.g., EXISTS checks)
	// dest should be a pointer to the value to scan into
	QueryRowRaw(ctx context.Context, query string, dest any, args ...any) error

	// Pool info
	MaxConns() int
	DBType() string
	PoolStats() stats.PoolStats
}
