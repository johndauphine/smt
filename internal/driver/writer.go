package driver

import (
	"context"
	"database/sql"

	"smt/internal/stats"
)

// Writer represents a database writer that can write data to target tables.
// This is the "Consumer" in the Reader -> Queue -> Writer pipeline.
type Writer interface {
	// Connection management
	Close()
	Ping(ctx context.Context) error
	DB() *sql.DB // Access to underlying database connection for tuning analysis

	// DatabaseContext returns cached metadata about the target database
	// (version, charset, collation) for optional AI review context.
	DatabaseContext() *DatabaseContext

	// Schema operations. DDL creation goes through the orchestrator's plan
	// executor (render once, ExecRaw per statement); writers only expose the
	// catalog existence checks the executor gates on (#87).
	DropTable(ctx context.Context, schema, table string) error
	TruncateTable(ctx context.Context, schema, table string) error
	TableExists(ctx context.Context, schema, table string) (bool, error)
	SetTableLogged(ctx context.Context, schema, table string) error

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

	// Data operations
	GetRowCount(ctx context.Context, schema, table string) (int64, error)      // Tries fast first, falls back to exact
	GetRowCountFast(ctx context.Context, schema, table string) (int64, error)  // Fast approximate count from system statistics
	GetRowCountExact(ctx context.Context, schema, table string) (int64, error) // Exact COUNT(*) - may be slow on large tables
	ResetSequence(ctx context.Context, schema string, t *Table) error

	// Bulk write - for drop_recreate mode
	WriteBatch(ctx context.Context, opts WriteBatchOptions) error

	// Upsert - for upsert mode with per-writer isolation
	UpsertBatch(ctx context.Context, opts UpsertBatchOptions) error

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

// WriteBatchOptions configures a bulk write operation.
type WriteBatchOptions struct {
	// Schema is the target schema.
	Schema string

	// Table is the target table name.
	Table string

	// Columns is the list of columns to write.
	Columns []string

	// Rows is the data to write.
	Rows [][]any

	// BatchSize overrides the writer's default batch size for this call.
	// If 0, the writer uses its configured default.
	BatchSize int

	// OrderColumns hints that rows arrive sorted by these columns (ascending).
	// Drivers that support ordered bulk inserts (e.g., MSSQL BCP ORDER hint)
	// can use this to skip sorting, improving insert performance.
	OrderColumns []string
}

// UpsertBatchOptions configures an upsert operation.
type UpsertBatchOptions struct {
	// Schema is the target schema.
	Schema string

	// Table is the target table name.
	Table string

	// Columns is the list of columns to upsert.
	Columns []string

	// ColumnTypes contains the data types for each column.
	ColumnTypes []string

	// ColumnSRIDs contains the SRID for spatial columns (0 for non-spatial).
	ColumnSRIDs []int

	// PKColumns is the list of primary key columns for conflict detection.
	PKColumns []string

	// Rows is the data to upsert.
	Rows [][]any

	// BatchSize overrides the writer's default batch size for this call.
	// If 0, the writer uses its configured default.
	BatchSize int

	// WriterID identifies this writer for per-writer staging table isolation.
	WriterID int

	// PartitionID identifies the partition being written (for cleanup).
	PartitionID *int
}
