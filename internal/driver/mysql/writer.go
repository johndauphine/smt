package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"smt/internal/dbconfig"
	"smt/internal/driver"
	"smt/internal/logging"
	"smt/internal/stats"
)

// Writer implements driver.Writer for MySQL/MariaDB.
type Writer struct {
	db                *sql.DB
	config            *dbconfig.TargetConfig
	maxConns          int
	defaultBatchSize  int
	sourceType        string
	dialect           *Dialect
	unknownTypePolicy string
	dbContext         *driver.DatabaseContext // Cached database context for AI review
	isMariaDB         bool
}

// NewWriter creates a new MySQL/MariaDB writer.
func NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (*Writer, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)
	db.SetConnMaxLifetime(30 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	// Detect MySQL vs MariaDB
	var version string
	if err := db.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		db.Close()
		return nil, fmt.Errorf("querying version: %w", err)
	}
	isMariaDB := strings.Contains(strings.ToLower(version), "mariadb")

	dbType := "MySQL"
	if isMariaDB {
		dbType = "MariaDB"
	}
	logging.Debug("Connected to %s target: %s:%d/%s (%s)", dbType, cfg.Host, cfg.Port, cfg.Database, version)

	unknownTypePolicy := opts.UnknownTypePolicy
	if unknownTypePolicy == "" {
		unknownTypePolicy = "fail"
	}

	w := &Writer{
		db:                db,
		config:            cfg,
		maxConns:          maxConns,
		defaultBatchSize:  opts.BatchSize,
		sourceType:        opts.SourceType,
		dialect:           dialect,
		unknownTypePolicy: unknownTypePolicy,
		isMariaDB:         isMariaDB,
	}

	// Gather database context for AI
	w.dbContext = w.gatherDatabaseContext(version)

	return w, nil
}

// gatherDatabaseContext collects MySQL/MariaDB database metadata for AI context.
// Thin wrapper that calls the package-level helper so the Reader and Writer
// can share the same query logic — see issue #13.
func (w *Writer) gatherDatabaseContext(version string) *driver.DatabaseContext {
	return gatherDatabaseContext(w.db, w.config.Database, w.config.Host, version, w.isMariaDB)
}

// gatherDatabaseContext queries a live MySQL/MariaDB connection for metadata
// the AI prompt's SOURCE DATABASE / TARGET DATABASE block consumes (charset,
// collation, identifier case, storage engine, version-gated feature list).
// Used by both the Writer (target context) and the Reader (source context,
// plumbed through TableOptions.SourceContext via the orchestrator). Failures
// on individual queries are non-fatal — the function returns whatever it
// could collect.
func gatherDatabaseContext(db *sql.DB, dbName, host, version string, isMariaDB bool) *driver.DatabaseContext {
	dbCtx := &driver.DatabaseContext{
		Version:                  version,
		DatabaseName:             dbName,
		ServerName:               host,
		IdentifierCase:           "preserve",
		CaseSensitiveIdentifiers: false, // Depends on OS/config
		MaxIdentifierLength:      64,
		VarcharSemantics:         "char", // utf8mb4 VARCHAR is characters
		BytesPerChar:             4,      // utf8mb4 max
	}

	// Parse version for major version number using regex
	// Matches patterns like "8.0.32", "5.7.44", "10.11.6-MariaDB", etc.
	versionRegex := regexp.MustCompile(`^(\d+)\.`)
	if matches := versionRegex.FindStringSubmatch(version); len(matches) > 1 {
		if majorVer, err := strconv.Atoi(matches[1]); err == nil {
			dbCtx.MajorVersion = majorVer
		}
	}

	if isMariaDB {
		dbCtx.StorageEngine = "MariaDB"
	}

	// Log warning if version couldn't be parsed
	if dbCtx.MajorVersion == 0 {
		logging.Warn("Could not parse MySQL/MariaDB version from '%s', version-specific features may not be detected", version)
	}

	// Query character set and collation
	var charsetVar, collationVar string
	if db.QueryRow("SELECT @@character_set_database, @@collation_database").Scan(&charsetVar, &collationVar) == nil {
		dbCtx.Charset = charsetVar
		dbCtx.Collation = collationVar

		// Determine bytes per char based on charset
		switch {
		case strings.HasPrefix(charsetVar, "utf8mb4"):
			dbCtx.BytesPerChar = 4
			dbCtx.Encoding = "UTF-8"
		case strings.HasPrefix(charsetVar, "utf8"):
			dbCtx.BytesPerChar = 3
			dbCtx.Encoding = "UTF-8 (3-byte)"
		case charsetVar == "latin1":
			dbCtx.BytesPerChar = 1
			dbCtx.Encoding = "Latin1"
		default:
			dbCtx.BytesPerChar = 1
			dbCtx.Encoding = charsetVar
		}

		// Parse collation for case sensitivity
		upperCollation := strings.ToUpper(collationVar)
		if strings.Contains(upperCollation, "_CS") || strings.Contains(upperCollation, "_BIN") {
			dbCtx.CaseSensitiveData = true
		} else if strings.Contains(upperCollation, "_CI") {
			dbCtx.CaseSensitiveData = false
		}
	}

	// Query lower_case_table_names for identifier case sensitivity
	// Use -1 as sentinel to distinguish "not queried" from actual value of 0
	lowerCaseTableNames := -1
	if db.QueryRow("SELECT @@lower_case_table_names").Scan(&lowerCaseTableNames) == nil {
		switch lowerCaseTableNames {
		case 0:
			dbCtx.CaseSensitiveIdentifiers = true
			dbCtx.IdentifierCase = "preserve"
		case 1:
			dbCtx.CaseSensitiveIdentifiers = false
			dbCtx.IdentifierCase = "lower"
		case 2:
			dbCtx.CaseSensitiveIdentifiers = false
			dbCtx.IdentifierCase = "preserve"
		}
	}

	// Query default storage engine
	var engine string
	if db.QueryRow("SELECT @@default_storage_engine").Scan(&engine) == nil {
		dbCtx.StorageEngine = engine
	}

	// Max varchar length depends on charset
	// utf8mb4: 16383 chars (65535 bytes / 4)
	// utf8: 21844 chars (65535 bytes / 3)
	// latin1: 65535 chars
	if dbCtx.BytesPerChar > 0 {
		dbCtx.MaxVarcharLength = 65535 / dbCtx.BytesPerChar
	} else {
		// Fallback to safe default if charset detection failed
		dbCtx.MaxVarcharLength = 16383 // Assume utf8mb4 (most restrictive)
	}

	// Standard MySQL features
	dbCtx.Features = []string{"JSON", "SPATIAL", "FULLTEXT"}
	if isMariaDB {
		dbCtx.Features = append(dbCtx.Features, "SEQUENCES", "SYSTEM_VERSIONING")
	}
	if dbCtx.MajorVersion >= 8 || (isMariaDB && dbCtx.MajorVersion >= 10) {
		dbCtx.Features = append(dbCtx.Features, "CTE", "WINDOW_FUNCTIONS")
	}

	// Log with appropriate handling of sentinel value
	if lowerCaseTableNames >= 0 {
		logging.Debug("MySQL context: charset=%s, collation=%s, storage_engine=%s, lower_case=%d",
			dbCtx.Charset, dbCtx.Collation, dbCtx.StorageEngine, lowerCaseTableNames)
	} else {
		logging.Debug("MySQL context: charset=%s, collation=%s, storage_engine=%s, lower_case=unknown",
			dbCtx.Charset, dbCtx.Collation, dbCtx.StorageEngine)
	}

	return dbCtx
}

// Close closes all connections.
func (w *Writer) Close() {
	w.db.Close()
}

// Ping tests the connection.
func (w *Writer) Ping(ctx context.Context) error {
	return w.db.PingContext(ctx)
}

// DB returns the underlying database connection for tuning analysis.
func (w *Writer) DB() *sql.DB {
	return w.db
}

// MaxConns returns the configured maximum connections.
func (w *Writer) MaxConns() int {
	return w.maxConns
}

// DBType returns the database type.
func (w *Writer) DBType() string {
	return "mysql"
}

// DatabaseContext returns the cached target database metadata gathered at
// connect time, for optional AI review context.
func (w *Writer) DatabaseContext() *driver.DatabaseContext {
	return w.dbContext
}

// PoolStats returns connection pool statistics.
func (w *Writer) PoolStats() stats.PoolStats {
	dbStats := w.db.Stats()
	return stats.PoolStats{
		DBType:      "mysql",
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}

// DropTable drops a table, disabling foreign-key checks around the drop.
func (w *Writer) DropTable(ctx context.Context, schema, table string) error {
	qualifiedTable := w.dialect.QualifyTable(schema, table)
	_, err := w.db.ExecContext(ctx, fmt.Sprintf(
		"SET FOREIGN_KEY_CHECKS = 0; DROP TABLE IF EXISTS %s; SET FOREIGN_KEY_CHECKS = 1;",
		qualifiedTable))
	return err
}

// TruncateTable truncates a table, disabling foreign key checks to allow
// truncating tables that are referenced by other tables.
func (w *Writer) TruncateTable(ctx context.Context, schema, table string) error {
	qualifiedTable := w.dialect.QualifyTable(schema, table)
	_, err := w.db.ExecContext(ctx, fmt.Sprintf(
		"SET FOREIGN_KEY_CHECKS = 0; TRUNCATE TABLE %s; SET FOREIGN_KEY_CHECKS = 1;",
		qualifiedTable))
	return err
}

// TableExists checks if a table exists.
func (w *Writer) TableExists(ctx context.Context, schema, table string) (bool, error) {
	dbName := schema
	if dbName == "" {
		dbName = w.config.Database
	}

	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT 1 FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`, dbName, table).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// IndexExists reports whether an index with the given name exists on the
// given table.
func (w *Writer) IndexExists(ctx context.Context, schema, table, indexName string) (bool, error) {
	dbName := schema
	if dbName == "" {
		dbName = w.config.Database
	}

	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT 1 FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ?
		LIMIT 1
	`, dbName, table, indexName).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// ForeignKeyExists reports whether an FK constraint with the given name
// exists on the given table.
func (w *Writer) ForeignKeyExists(ctx context.Context, schema, table, fkName string) (bool, error) {
	dbName := schema
	if dbName == "" {
		dbName = w.config.Database
	}

	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT 1 FROM information_schema.TABLE_CONSTRAINTS
		WHERE CONSTRAINT_TYPE = 'FOREIGN KEY'
		AND TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = ?
	`, dbName, table, fkName).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// CheckConstraintExists reports whether a CHECK constraint with the given
// name exists on the given table.
//
// Uses information_schema.TABLE_CONSTRAINTS (available on every MySQL
// version) rather than information_schema.CHECK_CONSTRAINTS (8.0.16+ only).
// Older MySQL targets that lack the CHECK_CONSTRAINTS view would error out
// of the existence check and block the create path entirely, even though
// CHECK semantics aren't supported there at all (the AI/exec path further
// down is already broken in the same way per CLAUDE.md). On pre-8.0.16
// MySQL, TABLE_CONSTRAINTS simply returns no rows for CHECK — falling
// through to the existing AI/exec path, same as before this PR.
func (w *Writer) CheckConstraintExists(ctx context.Context, schema, table, checkName string) (bool, error) {
	dbName := schema
	if dbName == "" {
		dbName = w.config.Database
	}

	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT 1 FROM information_schema.TABLE_CONSTRAINTS
		WHERE CONSTRAINT_TYPE = 'CHECK'
		AND TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = ?
	`, dbName, table, checkName).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// GetTableDDL retrieves the CREATE TABLE DDL for an existing table.
// Returns empty string if DDL cannot be retrieved.
func (w *Writer) GetTableDDL(ctx context.Context, schema, table string) string {
	dbName := schema
	if dbName == "" {
		dbName = w.config.Database
	}

	// Use dialect's QualifyTable for proper identifier escaping (prevents SQL injection)
	qualifiedTable := w.dialect.QualifyTable(dbName, table)
	var tableName, createStmt string
	err := w.db.QueryRowContext(ctx, "SHOW CREATE TABLE "+qualifiedTable).Scan(&tableName, &createStmt)
	if err != nil {
		logging.Debug("Could not get table DDL for %s.%s: %v", dbName, table, err)
		return ""
	}
	return createStmt
}

// ExecRaw executes a raw SQL query and returns the number of rows affected.
func (w *Writer) ExecRaw(ctx context.Context, query string, args ...any) (int64, error) {
	result, err := w.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// QueryRowRaw executes a raw SQL query that returns a single row.
func (w *Writer) QueryRowRaw(ctx context.Context, query string, dest any, args ...any) error {
	return w.db.QueryRowContext(ctx, query, args...).Scan(dest)
}
