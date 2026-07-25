package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/johndauphine/smt/internal/dbconfig"
	"github.com/johndauphine/smt/internal/driver"
	"github.com/johndauphine/smt/internal/logging"
	"github.com/johndauphine/smt/internal/stats"
)

// Writer implements driver.Writer for SQL Server.
type Writer struct {
	db                *sql.DB
	config            *dbconfig.TargetConfig
	maxConns          int
	defaultBatchSize  int
	compatLevel       int
	sourceType        string
	dialect           *Dialect
	unknownTypePolicy string
	dbContext         *driver.DatabaseContext // Cached database context for AI review
}

// NewWriter creates a new SQL Server writer.
func NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (*Writer, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	// Query database compatibility level
	var compatLevel int
	err = db.QueryRow(`
		SELECT compatibility_level
		FROM sys.databases
		WHERE name = DB_NAME()
	`).Scan(&compatLevel)
	if err != nil {
		compatLevel = 0
	}

	logging.Debug("Connected to MSSQL target: %s:%d/%s", cfg.Host, cfg.Port, cfg.Database)

	unknownTypePolicy := opts.UnknownTypePolicy
	if unknownTypePolicy == "" {
		unknownTypePolicy = "fail"
	}

	w := &Writer{
		db:                db,
		config:            cfg,
		maxConns:          maxConns,
		defaultBatchSize:  opts.BatchSize,
		compatLevel:       compatLevel,
		sourceType:        opts.SourceType,
		dialect:           dialect,
		unknownTypePolicy: unknownTypePolicy,
	}

	// Gather database context for AI
	w.dbContext = w.gatherDatabaseContext()

	return w, nil
}

// gatherDatabaseContext collects SQL Server database metadata for AI context.
// Thin wrapper that calls the package-level helper so the Reader and Writer
// can share the same query logic — see issue #13.
func (w *Writer) gatherDatabaseContext() *driver.DatabaseContext {
	return gatherDatabaseContext(w.db, w.config.Database, w.config.Host, w.compatLevel)
}

// gatherDatabaseContext queries a live SQL Server connection for metadata the
// AI prompt's SOURCE DATABASE / TARGET DATABASE block consumes (version,
// collation, code page, charset, compat-level-gated feature list). Used by
// both the Writer (target context) and the Reader (source context, plumbed
// through TableOptions.SourceContext via the orchestrator). Failures on
// individual queries are non-fatal — the function returns whatever it could
// collect.
func gatherDatabaseContext(db *sql.DB, dbName, host string, compatLevel int) *driver.DatabaseContext {
	ctx := &driver.DatabaseContext{
		DatabaseName:             dbName,
		ServerName:               host,
		IdentifierCase:           "insensitive",
		CaseSensitiveIdentifiers: false,
		MaxIdentifierLength:      128,
		VarcharSemantics:         "byte", // VARCHAR = bytes, NVARCHAR = chars
		BytesPerChar:             2,      // NVARCHAR uses 2 bytes per char
		MaxNVarcharLength:        4000,   // NVARCHAR(n) max is 4000 chars; beyond that use NVARCHAR(MAX)
	}

	// Query server version
	var version string
	if db.QueryRow("SELECT @@VERSION").Scan(&version) == nil {
		ctx.Version = version
		// Parse major version using regex
		// @@VERSION returns something like "Microsoft SQL Server 2022 (RTM) - 16.0.1000.6"
		// Try to match the product year first (2016, 2017, 2019, 2022, etc.)
		yearRegex := regexp.MustCompile(`SQL Server (\d{4})`)
		if matches := yearRegex.FindStringSubmatch(version); len(matches) > 1 {
			if year, err := strconv.Atoi(matches[1]); err == nil {
				// Map year to major version number
				switch {
				case year >= 2022:
					ctx.MajorVersion = 16
				case year >= 2019:
					ctx.MajorVersion = 15
				case year >= 2017:
					ctx.MajorVersion = 14
				case year >= 2016:
					ctx.MajorVersion = 13
				case year >= 2014:
					ctx.MajorVersion = 12
				default:
					ctx.MajorVersion = 11
				}
			}
		}
		// Fallback: try to parse version number directly (e.g., "16.0.1000.6")
		if ctx.MajorVersion == 0 {
			verNumRegex := regexp.MustCompile(`- (\d+)\.`)
			if matches := verNumRegex.FindStringSubmatch(version); len(matches) > 1 {
				if majorVer, err := strconv.Atoi(matches[1]); err == nil {
					ctx.MajorVersion = majorVer
				}
			}
		}
		if ctx.MajorVersion == 0 {
			logging.Warn("Could not parse SQL Server version from '%s', version-specific features may not be detected", version)
		}
	}

	// Query database collation
	var collation sql.NullString
	if db.QueryRow("SELECT DATABASEPROPERTYEX(DB_NAME(), 'Collation')").Scan(&collation) == nil && collation.Valid {
		ctx.Collation = collation.String
		// Parse collation for case sensitivity
		upperCollation := strings.ToUpper(collation.String)
		if strings.Contains(upperCollation, "_CS_") {
			ctx.CaseSensitiveData = true
		} else if strings.Contains(upperCollation, "_CI_") {
			ctx.CaseSensitiveData = false
		}
		// Parse for accent sensitivity
		if strings.Contains(upperCollation, "_AS") {
			ctx.Notes = "Accent-sensitive collation"
		}
	}

	// Query code page from collation
	var codePage sql.NullInt64
	if db.QueryRow(`
		SELECT COLLATIONPROPERTY(DATABASEPROPERTYEX(DB_NAME(), 'Collation'), 'CodePage')
	`).Scan(&codePage) == nil && codePage.Valid {
		ctx.CodePage = int(codePage.Int64)
		switch ctx.CodePage {
		case 65001:
			ctx.Encoding = "UTF-8"
		case 1252:
			ctx.Encoding = "Latin1 (Windows-1252)"
		case 1200:
			ctx.Encoding = "UTF-16LE"
		default:
			ctx.Encoding = fmt.Sprintf("CP%d", ctx.CodePage)
		}
	}

	// Set charset based on typical SQL Server setup
	ctx.Charset = "SQL_Latin1_General_CP1"
	if ctx.CodePage == 65001 {
		ctx.Charset = "UTF-8"
	}
	ctx.NationalCharset = "UTF-16"

	// Max varchar lengths
	ctx.MaxVarcharLength = 8000 // VARCHAR max, NVARCHAR max is 4000 chars

	// Features based on compatibility level
	ctx.Features = []string{"NVARCHAR", "VARCHAR_MAX", "DATETIME2", "JSON"}
	if compatLevel >= 130 { // SQL Server 2016+
		ctx.Features = append(ctx.Features, "JSON_FUNCTIONS", "TEMPORAL_TABLES")
	}
	if compatLevel >= 150 { // SQL Server 2019+
		ctx.Features = append(ctx.Features, "UTF8_SUPPORT")
	}

	logging.Debug("MSSQL context: collation=%s, code_page=%d, compat_level=%d",
		ctx.Collation, ctx.CodePage, compatLevel)

	return ctx
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
	return "mssql"
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
		DBType:      "mssql",
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}

// DropTable drops a table.
func (w *Writer) DropTable(ctx context.Context, schema, table string) error {
	_, err := w.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", w.dialect.QualifyTable(schema, table)))
	return err
}

// TruncateTable truncates a table.
func (w *Writer) TruncateTable(ctx context.Context, schema, table string) error {
	_, err := w.db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", w.dialect.QualifyTable(schema, table)))
	return err
}

// TableExists checks if a table exists.
func (w *Writer) TableExists(ctx context.Context, schema, table string) (bool, error) {
	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT 1 FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
	`, sql.Named("schema", schema), sql.Named("table", table)).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// IndexExists reports whether an index with the given name exists on the
// given table. SQL Server scopes index names per table, so both schema
// and table are required to disambiguate.
func (w *Writer) IndexExists(ctx context.Context, schema, table, indexName string) (bool, error) {
	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT CASE WHEN EXISTS (
			SELECT 1 FROM sys.indexes i
			JOIN sys.tables t ON i.object_id = t.object_id
			JOIN sys.schemas s ON t.schema_id = s.schema_id
			WHERE s.name = @schema AND t.name = @table AND i.name = @name
		) THEN 1 ELSE 0 END
	`, sql.Named("schema", schema), sql.Named("table", table), sql.Named("name", indexName)).Scan(&exists)
	return exists == 1, err
}

// ForeignKeyExists reports whether an FK constraint with the given name
// exists on the given table.
func (w *Writer) ForeignKeyExists(ctx context.Context, schema, table, fkName string) (bool, error) {
	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT CASE WHEN EXISTS (
			SELECT 1 FROM sys.foreign_keys fk
			JOIN sys.tables t ON fk.parent_object_id = t.object_id
			JOIN sys.schemas s ON t.schema_id = s.schema_id
			WHERE s.name = @schema AND t.name = @table AND fk.name = @name
		) THEN 1 ELSE 0 END
	`, sql.Named("schema", schema), sql.Named("table", table), sql.Named("name", fkName)).Scan(&exists)
	return exists == 1, err
}

// CheckConstraintExists reports whether a CHECK constraint with the given
// name exists on the given table.
func (w *Writer) CheckConstraintExists(ctx context.Context, schema, table, checkName string) (bool, error) {
	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT CASE WHEN EXISTS (
			SELECT 1 FROM sys.check_constraints cc
			JOIN sys.tables t ON cc.parent_object_id = t.object_id
			JOIN sys.schemas s ON t.schema_id = s.schema_id
			WHERE s.name = @schema AND t.name = @table AND cc.name = @name
		) THEN 1 ELSE 0 END
	`, sql.Named("schema", schema), sql.Named("table", table), sql.Named("name", checkName)).Scan(&exists)
	return exists == 1, err
}

// GetTableDDL retrieves the CREATE TABLE DDL for an existing table.
// Returns empty string if DDL cannot be retrieved.
func (w *Writer) GetTableDDL(ctx context.Context, schema, table string) string {
	// Build DDL from information_schema
	rows, err := w.db.QueryContext(ctx, `
		SELECT
			COLUMN_NAME,
			DATA_TYPE,
			CHARACTER_MAXIMUM_LENGTH,
			NUMERIC_PRECISION,
			NUMERIC_SCALE,
			IS_NULLABLE,
			COLUMN_DEFAULT
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
		ORDER BY ORDINAL_POSITION
	`, sql.Named("schema", schema), sql.Named("table", table))
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
			if charMaxLen.Int64 == -1 {
				typeStr = fmt.Sprintf("%s(MAX)", dataType)
			} else {
				typeStr = fmt.Sprintf("%s(%d)", dataType, charMaxLen.Int64)
			}
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

type spatialColumn struct {
	Name     string
	TypeName string
	SRID     int
}

// ExecRaw executes a raw SQL query and returns the number of rows affected.
// The query should use sql.Named parameters for SQL Server.
func (w *Writer) ExecRaw(ctx context.Context, query string, args ...any) (int64, error) {
	result, err := w.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// QueryRowRaw executes a raw SQL query that returns a single row.
// The query should use sql.Named parameters for SQL Server.
func (w *Writer) QueryRowRaw(ctx context.Context, query string, dest any, args ...any) error {
	return w.db.QueryRowContext(ctx, query, args...).Scan(dest)
}
