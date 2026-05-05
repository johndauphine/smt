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

	// Schema operations
	CreateSchema(ctx context.Context, schema string) error
	CreateTable(ctx context.Context, t *Table, targetSchema string) error
	CreateTableWithOptions(ctx context.Context, t *Table, targetSchema string, opts TableOptions) error
	DropTable(ctx context.Context, schema, table string) error
	TruncateTable(ctx context.Context, schema, table string) error
	TableExists(ctx context.Context, schema, table string) (bool, error)
	SetTableLogged(ctx context.Context, schema, table string) error

	// Constraint operations
	CreatePrimaryKey(ctx context.Context, t *Table, targetSchema string) error
	CreateIndex(ctx context.Context, t *Table, idx *Index, targetSchema string) error
	CreateIndexWithOptions(ctx context.Context, t *Table, idx *Index, targetSchema string, opts FinalizeOptions) error
	CreateForeignKey(ctx context.Context, t *Table, fk *ForeignKey, targetSchema string) error
	CreateForeignKeyWithOptions(ctx context.Context, t *Table, fk *ForeignKey, targetSchema string, opts FinalizeOptions) error
	CreateCheckConstraint(ctx context.Context, t *Table, chk *CheckConstraint, targetSchema string) error
	CreateCheckConstraintWithOptions(ctx context.Context, t *Table, chk *CheckConstraint, targetSchema string, opts FinalizeOptions) error
	HasPrimaryKey(ctx context.Context, schema, table string) (bool, error)

	// IndexExists reports whether an index with the given name exists on the
	// target table. Used by CreateIndexWithOptions to short-circuit re-runs
	// without invoking the AI or executing DDL that would fail with "already
	// exists". Catalog query, no AI involved.
	IndexExists(ctx context.Context, schema, table, indexName string) (bool, error)

	// ForeignKeyExists reports whether a foreign key with the given name
	// exists on the target table. Used by CreateForeignKeyWithOptions for
	// idempotent re-runs.
	ForeignKeyExists(ctx context.Context, schema, table, fkName string) (bool, error)

	// CheckConstraintExists reports whether a CHECK constraint with the given
	// name exists on the target table. Used by CreateCheckConstraintWithOptions
	// for idempotent re-runs.
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

// TableOptions contains options for table creation.
type TableOptions struct {
	// Unlogged creates an unlogged table (PostgreSQL only, for performance).
	Unlogged bool

	// SourceContext contains metadata about the source database.
	// This is passed to AI type mapper for better DDL generation.
	SourceContext *DatabaseContext

	// MaxRetries caps the number of validate-and-retry attempts when the AI's
	// DDL is rejected by the database with a syntactically-suspect error
	// (parser error, missing type, etc. — see each driver's
	// isRetryableDDLError). On a retryable error the writer regenerates the
	// DDL with the failed attempt + database error fed back into the prompt,
	// then retries up to this many times before surfacing the final failure.
	// Zero means "no retries" (current behavior); set in the orchestrator
	// from migration.ai_max_retries (which defaults to 3 when configured).
	// Non-retryable errors (FK violations, permission errors, real schema
	// conflicts) bypass the loop and surface immediately. See #29.
	MaxRetries int

	// Note: Indexes and CHECK constraints are always created separately in Finalize,
	// not included in the initial CREATE TABLE DDL.
}

// FinalizeOptions configures CreateIndex / CreateForeignKey / CreateCheckConstraint
// retry behavior. Shared across the three finalize calls because today they only
// differ in the metadata they need (Index vs FK vs Check), not in their retry
// semantics. If a phase ever needs DDL-type-specific tunables (e.g. PG concurrent
// index build) this can be split into per-phase structs without changing the
// retry-loop helpers — the helpers only consume MaxRetries.
type FinalizeOptions struct {
	// MaxRetries caps validate-and-retry attempts when the AI's CREATE INDEX /
	// FOREIGN KEY / CHECK CONSTRAINT DDL is rejected by the database with a
	// retryable error (parser/binder class — see each driver's
	// isRetryableDDLError). On a retryable failure the writer regenerates the
	// DDL with the prior failed DDL + database error fed back into the prompt
	// (FinalizationDDLRequest.PreviousAttempt) and retries up to this many
	// times. Zero means no retries (current behavior); set in the orchestrator
	// from migration.ai_max_retries. Non-retryable errors (object already
	// exists, FK target missing, permissions) bypass the loop and surface
	// immediately. See #29 PR B; PR A is the table-creation equivalent.
	MaxRetries int
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
