package driver

import (
	"context"
	"database/sql"

	"github.com/johndauphine/smt/internal/stats"
)

// Reader represents a database reader that extracts schema metadata from a
// source database. (SMT is schema-only; the DMT row-streaming surface —
// ReadTable, pagination, sampling — was removed in #191.)
type Reader interface {
	// Connection management
	Close() error
	DB() *sql.DB

	// Schema operations
	ExtractSchema(ctx context.Context, schema string) ([]Table, error)
	LoadIndexes(ctx context.Context, t *Table) error
	LoadForeignKeys(ctx context.Context, t *Table) error
	LoadCheckConstraints(ctx context.Context, t *Table) error

	// Row count — ExtractSchema populates Table.RowCount from this.
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
