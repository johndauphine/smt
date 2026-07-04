package mssql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"smt/internal/dbconfig"
	"smt/internal/driver"
	"smt/internal/logging"
	"smt/internal/stats"
)

// Writer implements driver.Writer for SQL Server.
type Writer struct {
	db                 *sql.DB
	config             *dbconfig.TargetConfig
	maxConns           int
	compatLevel        int
	sourceType         string
	dialect            *Dialect
	typeMapper         driver.TypeMapper
	tableMapper        driver.TableTypeMapper       // Table-level DDL generation
	finalizationMapper driver.FinalizationDDLMapper // AI-driven finalization DDL
	dbContext          *driver.DatabaseContext      // Cached database context for AI

	// Optional override mappers used for the AI self-check pass when
	// migration.ai_verifier_model is set. Nil falls back to tableMapper /
	// finalizationMapper. Use tableVerifier()/finalizationVerifier() at the
	// callsites. See driver.WriterOptions.VerifierTypeMapper.
	verifierTableMapper        driver.TableTypeMapper
	verifierFinalizationMapper driver.FinalizationDDLMapper
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

	// Validate type mapper is provided
	if opts.TypeMapper == nil {
		db.Close()
		return nil, fmt.Errorf("TypeMapper is required")
	}

	// Require TableTypeMapper for table-level AI DDL generation
	tableMapper, ok := opts.TypeMapper.(driver.TableTypeMapper)
	if !ok {
		db.Close()
		return nil, fmt.Errorf("TypeMapper must implement TableTypeMapper interface for table-level DDL generation")
	}

	// Log AI mapper initialization
	if aiMapper, ok := opts.TypeMapper.(*driver.AITypeMapper); ok {
		logging.Debug("AI Table-Level Type Mapping enabled (provider: %s, model: %s)",
			aiMapper.ProviderName(), aiMapper.Model())
		if aiMapper.CacheSize() > 0 {
			logging.Debug("Loaded %d cached AI type mappings", aiMapper.CacheSize())
		}
	}

	// Check if type mapper also implements finalization DDL mapper
	finalizationMapper, _ := opts.TypeMapper.(driver.FinalizationDDLMapper)

	verifierTableMapper, verifierFinalizationMapper := driver.ResolveVerifierMappers(opts)
	if verifierTableMapper != nil {
		// INFO not Debug — see postgres writer for rationale.
		if aiMapper, ok := opts.VerifierTypeMapper.(*driver.AITypeMapper); ok {
			logging.Info("AI Verifier provider enabled (provider: %s, model: %s)",
				aiMapper.ProviderName(), aiMapper.Model())
		}
	}

	w := &Writer{
		db:                         db,
		config:                     cfg,
		maxConns:                   maxConns,
		compatLevel:                compatLevel,
		sourceType:                 opts.SourceType,
		dialect:                    dialect,
		typeMapper:                 opts.TypeMapper,
		tableMapper:                tableMapper,
		finalizationMapper:         finalizationMapper,
		verifierTableMapper:        verifierTableMapper,
		verifierFinalizationMapper: verifierFinalizationMapper,
	}

	// Gather database context for AI
	w.dbContext = w.gatherDatabaseContext()

	return w, nil
}

// tableVerifier returns the mapper to use for VerifyTableDDL: the configured
// cross-model verifier if WriterOptions.VerifierTypeMapper was non-nil, else
// the generator mapper. Same-mapper fallback preserves Phase 1 behavior.
func (w *Writer) tableVerifier() driver.TableTypeMapper {
	if w.verifierTableMapper != nil {
		return w.verifierTableMapper
	}
	return w.tableMapper
}

// finalizationVerifier mirrors tableVerifier for finalization DDL.
func (w *Writer) finalizationVerifier() driver.FinalizationDDLMapper {
	if w.verifierFinalizationMapper != nil {
		return w.verifierFinalizationMapper
	}
	return w.finalizationMapper
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

// CreateSchema creates the target schema if it doesn't exist.
func (w *Writer) CreateSchema(ctx context.Context, schema string) error {
	var exists int
	err := w.db.QueryRowContext(ctx,
		"SELECT 1 FROM sys.schemas WHERE name = @schema",
		sql.Named("schema", schema)).Scan(&exists)
	if err == sql.ErrNoRows {
		_, err = w.db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA %s", w.dialect.QuoteIdentifier(schema)))
		return err
	}
	return err
}

// CreateTable creates a table from source metadata.
func (w *Writer) CreateTable(ctx context.Context, t *driver.Table, targetSchema string) error {
	return w.CreateTableWithOptions(ctx, t, targetSchema, driver.TableOptions{})
}

// CreateTableWithOptions creates a table with options using AI-generated DDL.
// On retryable errors, regenerates with the prior failed DDL + error fed back
// into the prompt up to opts.MaxRetries times. See #29 / postgres equivalent.
func (w *Writer) CreateTableWithOptions(ctx context.Context, t *driver.Table, targetSchema string, opts driver.TableOptions) error {
	// Skip if the target table already exists. Idempotent re-runs land here
	// instead of failing on "There is already an object named ...". See
	// postgres equivalent.
	if exists, err := w.TableExists(ctx, targetSchema, t.Name); err != nil {
		return fmt.Errorf("checking table existence for %s: %w", t.FullName(), err)
	} else if exists {
		logging.Info("  ✓ table %s already exists, skipping", t.FullName())
		return nil
	}

	req := driver.TableDDLRequest{
		SourceDBType:  w.sourceType,
		TargetDBType:  "mssql",
		SourceTable:   t,
		TargetSchema:  targetSchema,
		SourceContext: opts.SourceContext,
		TargetContext: w.dbContext,
	}

	// Defensive clamp — see retryFinalize. Negative MaxRetries would skip
	// the loop and surface a wrapped-nil error. (Copilot review on PR #31.)
	if opts.MaxRetries < 0 {
		opts.MaxRetries = 0
	}
	var (
		lastDDL          string
		lastErr          error
		lastFromVerifier bool
	)
	for attempt := 0; attempt <= opts.MaxRetries; attempt++ {
		if attempt > 0 {
			req.PreviousAttempt = &driver.TableDDLAttempt{
				DDL:          lastDDL,
				Error:        lastErr.Error(),
				FromVerifier: lastFromVerifier,
			}
			logging.Info("retry attempt %d/%d for table %s after retryable DDL error: %v",
				attempt, opts.MaxRetries, t.FullName(), lastErr)
		}

		resp, err := w.tableMapper.GenerateTableDDL(ctx, req)
		if err != nil {
			if errors.Is(err, driver.ErrNotRetryable) {
				logging.Info("table %s: AI classified DB error as non-retryable (%v); surfacing original error", t.FullName(), err)
				return fmt.Errorf("creating table %s: %w\nDDL: %s", t.FullName(), lastErr, lastDDL)
			}
			return fmt.Errorf("AI DDL generation failed for table %s: %w", t.FullName(), err)
		}
		ddl := resp.CreateTableDDL
		logging.Debug("AI generated DDL for %s (attempt %d):\n%s", t.FullName(), attempt+1, ddl)
		for colName, colType := range resp.ColumnTypes {
			logging.Debug("  Column %s -> %s", colName, colType)
		}

		// AI self-check — see postgres equivalent for design.
		if opts.AIVerify && !resp.FromCache {
			vReq := driver.VerifyTableDDLRequest{
				SourceDBType: req.SourceDBType, TargetDBType: req.TargetDBType,
				SourceTable: req.SourceTable, TargetSchema: req.TargetSchema,
				SourceContext: req.SourceContext, TargetContext: req.TargetContext,
				ProposedDDL: ddl,
			}
			verdict, vErr := w.tableVerifier().VerifyTableDDL(ctx, vReq)
			if vErr != nil {
				return fmt.Errorf("AI verify failed for table %s: %w", t.FullName(), vErr)
			}
			if !verdict.OK {
				logging.Warn("verify flagged %d issue(s) on table %s, retrying:\n  %s",
					len(verdict.Issues), t.FullName(), strings.Join(verdict.Issues, "\n  "))
				lastDDL = ddl
				lastErr = fmt.Errorf("%s", strings.Join(verdict.Issues, "\n"))
				lastFromVerifier = true
				continue
			}
			logging.Debug("verify OK: table %s", t.FullName())
		}

		if _, err = w.db.ExecContext(ctx, ddl); err == nil {
			// Cache validated DDL after exec confirms it works (#32).
			w.tableMapper.CacheTableDDL(req, ddl)
			if attempt > 0 {
				logging.Info("table %s succeeded on retry attempt %d/%d", t.FullName(), attempt, opts.MaxRetries)
			}
			return nil
		}
		// No post-exec already-exists catch on the CREATE TABLE path —
		// see postgres equivalent for rationale.

		// Short-circuit on cancellation — see postgres equivalent for rationale.
		if driver.IsCanceled(ctx, err) {
			return fmt.Errorf("creating table %s: %w", t.FullName(), err)
		}

		lastDDL = ddl
		lastErr = err
		lastFromVerifier = false
		// No classifier — let the next iteration ask the AI.
	}
	return fmt.Errorf("creating table %s: %w\nDDL: %s", t.FullName(), lastErr, lastDDL)
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

// SetTableLogged is a no-op for SQL Server.
func (w *Writer) SetTableLogged(ctx context.Context, schema, table string) error {
	return nil
}

// CreatePrimaryKey is a no-op because PK is created with the table.
func (w *Writer) CreatePrimaryKey(ctx context.Context, t *driver.Table, targetSchema string) error {
	return nil
}

// HasPrimaryKey checks if a table has a primary key constraint.
func (w *Writer) HasPrimaryKey(ctx context.Context, schema, table string) (bool, error) {
	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT CASE WHEN EXISTS (
			SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
			WHERE CONSTRAINT_TYPE = 'PRIMARY KEY'
			AND TABLE_SCHEMA = @schema
			AND TABLE_NAME = @table
		) THEN 1 ELSE 0 END
	`, sql.Named("schema", schema), sql.Named("table", table)).Scan(&exists)
	return exists == 1, err
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

// GetRowCount returns the row count for a table.
// It first tries a fast statistics-based count, then falls back to COUNT(*) if needed.
func (w *Writer) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	// Try fast stats-based count first
	count, err := w.GetRowCountFast(ctx, schema, table)
	if err == nil && count > 0 {
		return count, nil
	}

	// Fall back to COUNT(*)
	err = w.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", w.dialect.QualifyTable(schema, table))).Scan(&count)
	return count, err
}

// GetRowCountFast returns an approximate row count using system statistics.
// This is much faster than COUNT(*) for large tables.
func (w *Writer) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	query := `
		SELECT SUM(p.rows)
		FROM sys.partitions p
		JOIN sys.tables t ON p.object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @schema AND t.name = @table AND p.index_id IN (0, 1)
	`
	err := w.db.QueryRowContext(ctx, query,
		sql.Named("schema", schema),
		sql.Named("table", table)).Scan(&count)
	return count, err
}

// GetRowCountExact returns the exact row count using COUNT(*).
// This may be slow on large tables.
func (w *Writer) GetRowCountExact(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := w.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", w.dialect.QualifyTable(schema, table))).Scan(&count)
	return count, err
}

// CreateIndex creates an index on the target table using AI-generated DDL.
func (w *Writer) CreateIndex(ctx context.Context, t *driver.Table, idx *driver.Index, targetSchema string) error {
	return w.CreateIndexWithOptions(ctx, t, idx, targetSchema, driver.FinalizeOptions{})
}

// CreateIndexWithOptions creates an index using AI-generated DDL, retrying on
// retryable DDL errors per opts.MaxRetries. See retryFinalize and #29 PR B.
func (w *Writer) CreateIndexWithOptions(ctx context.Context, t *driver.Table, idx *driver.Index, targetSchema string, opts driver.FinalizeOptions) error {
	if w.finalizationMapper == nil {
		return fmt.Errorf("finalization mapper not available for index creation")
	}

	if exists, err := w.IndexExists(ctx, targetSchema, t.Name, idx.Name); err != nil {
		return fmt.Errorf("checking index existence for %s.%s: %w", t.Name, idx.Name, err)
	} else if exists {
		logging.Info("  ✓ index %s.%s already exists, skipping", t.Name, idx.Name)
		return nil
	}

	req := driver.FinalizationDDLRequest{
		Type:          driver.DDLTypeIndex,
		SourceDBType:  w.sourceType,
		TargetDBType:  "mssql",
		Table:         t,
		Index:         idx,
		TargetSchema:  targetSchema,
		TargetContext: w.dbContext,
	}
	return w.retryFinalize(ctx, req, opts, fmt.Sprintf("index %s.%s", t.Name, idx.Name))
}

// CreateForeignKey creates a foreign key constraint using AI-generated DDL.
func (w *Writer) CreateForeignKey(ctx context.Context, t *driver.Table, fk *driver.ForeignKey, targetSchema string) error {
	return w.CreateForeignKeyWithOptions(ctx, t, fk, targetSchema, driver.FinalizeOptions{})
}

// CreateForeignKeyWithOptions creates a foreign key using AI-generated DDL,
// retrying on retryable DDL errors per opts.MaxRetries. See #29 PR B.
func (w *Writer) CreateForeignKeyWithOptions(ctx context.Context, t *driver.Table, fk *driver.ForeignKey, targetSchema string, opts driver.FinalizeOptions) error {
	if w.finalizationMapper == nil {
		return fmt.Errorf("finalization mapper not available for foreign key creation")
	}

	if exists, err := w.ForeignKeyExists(ctx, targetSchema, t.Name, fk.Name); err != nil {
		return fmt.Errorf("checking FK existence for %s.%s: %w", t.Name, fk.Name, err)
	} else if exists {
		logging.Info("  ✓ FK %s.%s already exists, skipping", t.Name, fk.Name)
		return nil
	}

	// Override RefSchema with the target schema. The source FK metadata
	// carries the source's schema name (e.g. "public" from PG), and the
	// AI honors that field when emitting the REFERENCES clause —
	// producing FKs that reference a schema that does not exist on the
	// target. Same root cause as #4 / PR #5, applied to the create path.
	fkForTarget := *fk
	fkForTarget.RefSchema = targetSchema

	req := driver.FinalizationDDLRequest{
		Type:          driver.DDLTypeForeignKey,
		SourceDBType:  w.sourceType,
		TargetDBType:  "mssql",
		Table:         t,
		ForeignKey:    &fkForTarget,
		TargetSchema:  targetSchema,
		TargetContext: w.dbContext,
	}
	return w.retryFinalize(ctx, req, opts, fmt.Sprintf("FK %s.%s", t.Name, fk.Name))
}

// CreateCheckConstraint creates a check constraint using AI-generated DDL.
func (w *Writer) CreateCheckConstraint(ctx context.Context, t *driver.Table, chk *driver.CheckConstraint, targetSchema string) error {
	return w.CreateCheckConstraintWithOptions(ctx, t, chk, targetSchema, driver.FinalizeOptions{})
}

// CreateCheckConstraintWithOptions creates a CHECK constraint using AI-generated
// DDL, retrying on retryable DDL errors per opts.MaxRetries. See #29 PR B.
func (w *Writer) CreateCheckConstraintWithOptions(ctx context.Context, t *driver.Table, chk *driver.CheckConstraint, targetSchema string, opts driver.FinalizeOptions) error {
	if w.finalizationMapper == nil {
		return fmt.Errorf("finalization mapper not available for check constraint creation")
	}

	if exists, err := w.CheckConstraintExists(ctx, targetSchema, t.Name, chk.Name); err != nil {
		return fmt.Errorf("checking CHECK existence for %s.%s: %w", t.Name, chk.Name, err)
	} else if exists {
		logging.Info("  ✓ CHECK %s.%s already exists, skipping", t.Name, chk.Name)
		return nil
	}

	req := driver.FinalizationDDLRequest{
		Type:            driver.DDLTypeCheckConstraint,
		SourceDBType:    w.sourceType,
		TargetDBType:    "mssql",
		Table:           t,
		CheckConstraint: chk,
		TargetSchema:    targetSchema,
		TargetContext:   w.dbContext,
	}
	return w.retryFinalize(ctx, req, opts, fmt.Sprintf("CHECK %s.%s", t.Name, chk.Name))
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
