package driver

import (
	"context"
	"database/sql"

	"smt/internal/stats"
)

// Reader represents a database reader that introspects source-table schema.
type Reader interface {
	// Connection management
	Close() error
	DB() *sql.DB

	// Schema operations
	ExtractSchema(ctx context.Context, schema string) ([]Table, error)
	LoadIndexes(ctx context.Context, t *Table) error
	LoadForeignKeys(ctx context.Context, t *Table) error
	LoadCheckConstraints(ctx context.Context, t *Table) error

	// Metadata
	GetRowCount(ctx context.Context, schema, table string) (int64, error)      // Tries fast first, falls back to exact
	GetRowCountFast(ctx context.Context, schema, table string) (int64, error)  // Fast approximate count from system statistics
	GetRowCountExact(ctx context.Context, schema, table string) (int64, error) // Exact COUNT(*) - may be slow on large tables

	// Pool info
	MaxConns() int
	DBType() string
	PoolStats() stats.PoolStats

	// DatabaseContext returns metadata about this source database for the AI
	// prompt (version, charset, collation, identifier case, varchar semantics,
	// etc.). The orchestrator passes the result to target.CreateTableWithOptions
	// via TableOptions.SourceContext so the AI sees a populated SOURCE DATABASE
	// block alongside the existing TARGET DATABASE block. Implementations should
	// cache after first call — orchestrator may invoke this for every CREATE
	// TABLE on a wide schema.
	DatabaseContext() *DatabaseContext
}
