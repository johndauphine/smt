package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/johndauphine/smt/internal/dbconfig"
	"github.com/johndauphine/smt/internal/driver"
	"github.com/johndauphine/smt/internal/logging"
	"github.com/johndauphine/smt/internal/stats"
)

// sanitizePGIdentifier delegates to driver.NormalizeIdentifier so the
// `smt create` path and the `smt sync` path use the same naming rules.
// The shared implementation is in internal/driver/identifiers.go; this
// alias is kept so the rest of this file reads unchanged.
func sanitizePGIdentifier(ident string) string {
	return driver.NormalizeIdentifier("postgres", ident)
}

// sanitizePGTableName is an alias for sanitizePGIdentifier for table names.
func sanitizePGTableName(ident string) string {
	return sanitizePGIdentifier(ident)
}

// Writer implements driver.Writer for PostgreSQL.
type Writer struct {
	pool              *pgxpool.Pool
	config            *dbconfig.TargetConfig
	maxConns          int
	sourceType        string
	dialect           *Dialect
	unknownTypePolicy string
	dbContext         *driver.DatabaseContext // Cached database context for AI review
	cachedDB          *sql.DB                 // Cached database/sql wrapper for tuning analysis
}

// NewWriter creates a new PostgreSQL writer.
func NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (*Writer, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing dsn: %w", err)
	}

	poolCfg.MaxConns = int32(maxConns)
	poolCfg.MinConns = int32(maxConns / 4)

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, fmt.Errorf("creating pool: %w", err)
	}

	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	logging.Debug("Connected to PostgreSQL target: %s:%d/%s", cfg.Host, cfg.Port, cfg.Database)

	unknownTypePolicy := opts.UnknownTypePolicy
	if unknownTypePolicy == "" {
		unknownTypePolicy = "fail"
	}

	w := &Writer{
		pool:              pool,
		config:            cfg,
		maxConns:          maxConns,
		sourceType:        opts.SourceType,
		dialect:           dialect,
		unknownTypePolicy: unknownTypePolicy,
	}

	// Gather database context for AI
	w.dbContext = w.gatherDatabaseContext()

	return w, nil
}

// gatherDatabaseContext collects PostgreSQL database metadata for AI context.
// Thin wrapper that calls the package-level helper so the Reader and Writer
// can share the same query logic — see issue #13.
func (w *Writer) gatherDatabaseContext() *driver.DatabaseContext {
	return gatherDatabaseContext(context.Background(), w.pool, w.config.Database, w.config.Host)
}

// gatherDatabaseContext queries a live PostgreSQL connection for metadata
// the AI prompt's SOURCE DATABASE / TARGET DATABASE block consumes (version,
// encoding, collation, identifier case, varchar semantics, version-gated
// feature list). Used by both the Writer (for target context) and the Reader
// (for source context, plumbed through TableOptions.SourceContext via the
// orchestrator). Failures on individual queries are non-fatal — the function
// returns whatever it could collect.
func gatherDatabaseContext(ctx context.Context, pool *pgxpool.Pool, dbName, host string) *driver.DatabaseContext {
	dbCtx := &driver.DatabaseContext{
		DatabaseName:             dbName,
		ServerName:               host,
		IdentifierCase:           "lower",
		CaseSensitiveIdentifiers: true, // PostgreSQL preserves case in quotes
		CaseSensitiveData:        true, // Default is case-sensitive
		MaxIdentifierLength:      63,
		VarcharSemantics:         "char", // PostgreSQL VARCHAR is always characters
		BytesPerChar:             4,      // UTF-8 max
		MaxVarcharLength:         10485760,
	}

	// Query server version
	var version string
	if pool.QueryRow(ctx, "SELECT version()").Scan(&version) == nil {
		dbCtx.Version = version
		// Parse major version using regex to handle any version format
		// Matches patterns like "PostgreSQL 16.1", "PostgreSQL 17", etc.
		versionRegex := regexp.MustCompile(`PostgreSQL\s+(\d+)`)
		if matches := versionRegex.FindStringSubmatch(version); len(matches) > 1 {
			if majorVer, err := strconv.Atoi(matches[1]); err == nil {
				dbCtx.MajorVersion = majorVer
			}
		}
	}

	// Query encoding
	var encoding string
	if pool.QueryRow(ctx, "SHOW server_encoding").Scan(&encoding) == nil {
		dbCtx.Charset = encoding
		dbCtx.Encoding = encoding
		if encoding == "UTF8" {
			dbCtx.BytesPerChar = 4
		} else if encoding == "LATIN1" || encoding == "SQL_ASCII" {
			dbCtx.BytesPerChar = 1
		}
	}

	// Query collation
	var collation sql.NullString
	if pool.QueryRow(ctx, `
		SELECT datcollate FROM pg_database WHERE datname = current_database()
	`).Scan(&collation) == nil && collation.Valid {
		dbCtx.Collation = collation.String
	}

	// Query LC_CTYPE for character classification
	var lcCtype sql.NullString
	if pool.QueryRow(ctx, `
		SELECT datctype FROM pg_database WHERE datname = current_database()
	`).Scan(&lcCtype) == nil && lcCtype.Valid {
		if dbCtx.Notes != "" {
			dbCtx.Notes += "; "
		}
		dbCtx.Notes += "LC_CTYPE=" + lcCtype.String
	}

	// Standard PostgreSQL features
	dbCtx.Features = []string{"TEXT", "JSON", "JSONB", "ARRAY", "HSTORE", "UUID", "BYTEA", "NUMERIC"}

	// Version-specific features
	if dbCtx.MajorVersion >= 14 {
		dbCtx.Features = append(dbCtx.Features, "MULTIRANGE")
	}
	if dbCtx.MajorVersion >= 15 {
		dbCtx.Features = append(dbCtx.Features, "JSON_TABLE")
	}

	logging.Debug("PostgreSQL context: encoding=%s, collation=%s, version=%d",
		dbCtx.Encoding, dbCtx.Collation, dbCtx.MajorVersion)

	return dbCtx
}

// Close closes all connections.
// Reset() is called first to immediately close idle connections and mark acquired
// connections for destruction. This prevents Close() from blocking indefinitely
// when a connection is held by a stalled operation (e.g. a COPY waiting for data).
func (w *Writer) Close() {
	if w.cachedDB != nil {
		w.cachedDB.Close()
	}
	w.pool.Reset()
	w.pool.Close()
}

// Ping tests the connection.
func (w *Writer) Ping(ctx context.Context) error {
	return w.pool.Ping(ctx)
}

// DB returns a database/sql connection for tuning analysis.
// The connection is cached and reused across calls to avoid resource leaks.
func (w *Writer) DB() *sql.DB {
	if w.cachedDB == nil {
		// Create stdlib connector from pool config (only once)
		w.cachedDB = stdlib.OpenDBFromPool(w.pool)
	}
	return w.cachedDB
}

// MaxConns returns the configured maximum connections.
func (w *Writer) MaxConns() int {
	return w.maxConns
}

// DBType returns the database type.
func (w *Writer) DBType() string {
	return "postgres"
}

// DatabaseContext returns the cached target database metadata gathered at
// connect time, for optional AI review context.
func (w *Writer) DatabaseContext() *driver.DatabaseContext {
	return w.dbContext
}

// PoolStats returns connection pool statistics.
func (w *Writer) PoolStats() stats.PoolStats {
	poolStats := w.pool.Stat()
	return stats.PoolStats{
		DBType:      "postgres",
		MaxConns:    int(poolStats.MaxConns()),
		ActiveConns: int(poolStats.AcquiredConns()),
		IdleConns:   int(poolStats.IdleConns()),
		WaitCount:   poolStats.EmptyAcquireCount(),
		WaitTimeMs:  0,
	}
}

// DropTable drops a table.
func (w *Writer) DropTable(ctx context.Context, schema, table string) error {
	sanitizedTable := sanitizePGTableName(table)
	_, err := w.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", w.dialect.QualifyTable(schema, sanitizedTable)))
	return err
}

// TruncateTable truncates a table.
func (w *Writer) TruncateTable(ctx context.Context, schema, table string) error {
	sanitizedTable := sanitizePGTableName(table)
	_, err := w.pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", w.dialect.QualifyTable(schema, sanitizedTable)))
	return err
}

// TableExists checks if a table exists.
func (w *Writer) TableExists(ctx context.Context, schema, table string) (bool, error) {
	sanitizedTable := sanitizePGTableName(table)
	var exists bool
	err := w.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = $1 AND table_name = $2
		)
	`, schema, sanitizedTable).Scan(&exists)
	return exists, err
}

// GetTableDDL retrieves the CREATE TABLE DDL for an existing table.
// This provides context to AI for generating indexes, FKs, etc.
func (w *Writer) GetTableDDL(ctx context.Context, schema, table string) string {
	// Use pg_get_tabledef extension if available, otherwise build from catalog
	var ddl string

	// First try the extension (if installed)
	err := w.pool.QueryRow(ctx,
		`SELECT pg_get_tabledef($1, $2)`,
		schema, table,
	).Scan(&ddl)
	if err == nil && ddl != "" {
		return ddl
	}

	// Fallback: build DDL from information_schema
	rows, err := w.pool.Query(ctx, `
		SELECT
			column_name,
			data_type,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			is_nullable,
			column_default
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`, schema, table)
	if err != nil {
		logging.Debug("Could not get table DDL for %s.%s: %v", schema, table, err)
		return ""
	}
	defer rows.Close()

	var sb strings.Builder
	// Use dialect's QuoteIdentifier for proper escaping
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s.%s (\n",
		w.dialect.QuoteIdentifier(schema),
		w.dialect.QuoteIdentifier(table)))

	first := true
	for rows.Next() {
		var colName, dataType, isNullable string
		var charMaxLen, numPrecision, numScale sql.NullInt64
		var colDefault sql.NullString

		if err := rows.Scan(&colName, &dataType, &charMaxLen, &numPrecision, &numScale, &isNullable, &colDefault); err != nil {
			logging.Debug("Failed to scan column for %s.%s: %v", schema, table, err)
			continue
		}

		if !first {
			sb.WriteString(",\n")
		}
		first = false

		sb.WriteString(fmt.Sprintf("    %s ", w.dialect.QuoteIdentifier(colName)))

		// Build type with precision
		typeStr := dataType
		if charMaxLen.Valid && charMaxLen.Int64 > 0 {
			typeStr = fmt.Sprintf("%s(%d)", dataType, charMaxLen.Int64)
		} else if numPrecision.Valid && numPrecision.Int64 > 0 {
			if numScale.Valid && numScale.Int64 > 0 {
				typeStr = fmt.Sprintf("%s(%d,%d)", dataType, numPrecision.Int64, numScale.Int64)
			} else {
				typeStr = fmt.Sprintf("%s(%d)", dataType, numPrecision.Int64)
			}
		}
		sb.WriteString(typeStr)

		if isNullable == "NO" {
			sb.WriteString(" NOT NULL")
		}
		if colDefault.Valid && colDefault.String != "" {
			sb.WriteString(fmt.Sprintf(" DEFAULT %s", colDefault.String))
		}
	}

	// Check if any columns were found
	if first {
		logging.Debug("No columns found for table %s.%s", schema, table)
		return ""
	}

	sb.WriteString("\n);")
	return sb.String()
}

// IndexExists reports whether an index with the given name exists in the
// schema. Postgres index names are unique per schema, so the table argument
// is unused but kept to match the Writer interface across drivers.
func (w *Writer) IndexExists(ctx context.Context, schema, table, indexName string) (bool, error) {
	var exists bool
	err := w.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_indexes
			WHERE schemaname = $1 AND indexname = $2
		)
	`, schema, sanitizePGIdentifier(indexName)).Scan(&exists)
	return exists, err
}

// ForeignKeyExists reports whether an FK constraint with the given name
// exists on the given table.
func (w *Writer) ForeignKeyExists(ctx context.Context, schema, table, fkName string) (bool, error) {
	var exists bool
	err := w.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.table_constraints
			WHERE table_schema = $1 AND table_name = $2 AND constraint_name = $3
			  AND constraint_type = 'FOREIGN KEY'
		)
	`, schema, sanitizePGTableName(table), sanitizePGIdentifier(fkName)).Scan(&exists)
	return exists, err
}

// CheckConstraintExists reports whether a CHECK constraint with the given
// name exists on the given table.
func (w *Writer) CheckConstraintExists(ctx context.Context, schema, table, checkName string) (bool, error) {
	var exists bool
	err := w.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.table_constraints
			WHERE table_schema = $1 AND table_name = $2 AND constraint_name = $3
			  AND constraint_type = 'CHECK'
		)
	`, schema, sanitizePGTableName(table), sanitizePGIdentifier(checkName)).Scan(&exists)
	return exists, err
}

// ExecRaw executes a raw SQL query and returns the number of rows affected.
// The query should use $1, $2, etc. for parameter placeholders.
func (w *Writer) ExecRaw(ctx context.Context, query string, args ...any) (int64, error) {
	result, err := w.pool.Exec(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

// QueryRowRaw executes a raw SQL query that returns a single row.
// The query should use $1, $2, etc. for parameter placeholders.
func (w *Writer) QueryRowRaw(ctx context.Context, query string, dest any, args ...any) error {
	return w.pool.QueryRow(ctx, query, args...).Scan(dest)
}
