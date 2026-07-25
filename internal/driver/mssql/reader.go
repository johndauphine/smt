package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/smt/internal/dbconfig"
	"github.com/johndauphine/smt/internal/driver"
	"github.com/johndauphine/smt/internal/logging"
	"github.com/johndauphine/smt/internal/stats"
	"github.com/johndauphine/smt/internal/util"
	_ "github.com/microsoft/go-mssqldb"
)

// Reader implements driver.Reader for SQL Server.
type Reader struct {
	db          *sql.DB
	config      *dbconfig.SourceConfig
	maxConns    int
	dialect     *Dialect
	compatLevel int

	// dbContextOnce gates the (single) lookup of dbContext for the source side.
	dbContextOnce sync.Once
	dbContext     *driver.DatabaseContext
}

// DatabaseContext returns metadata about this source database for the AI prompt
// (version, collation, code page, charset, compat-level-gated feature list).
// Cached after first call.
func (r *Reader) DatabaseContext() *driver.DatabaseContext {
	r.dbContextOnce.Do(func() {
		r.dbContext = gatherDatabaseContext(r.db, r.config.Database, r.config.Host, r.compatLevel)
	})
	return r.dbContext
}

// NewReader creates a new SQL Server reader.
func NewReader(cfg *dbconfig.SourceConfig, maxConns int) (*Reader, error) {
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

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	// Check database compatibility level - require 140+ for STRING_AGG support
	compatLevel, err := getCompatibilityLevel(db)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("checking database compatibility level: %w", err)
	}
	if compatLevel < 140 {
		db.Close()
		return nil, fmt.Errorf("database compatibility level 140+ required (found %d). Run: ALTER DATABASE [%s] SET COMPATIBILITY_LEVEL = 160", compatLevel, cfg.Database)
	}

	logging.Debug("Connected to MSSQL source: %s:%d/%s (compat level %d)", cfg.Host, cfg.Port, cfg.Database, compatLevel)

	return &Reader{
		db:          db,
		config:      cfg,
		maxConns:    maxConns,
		dialect:     dialect,
		compatLevel: compatLevel,
	}, nil
}

// Close closes all connections.
func (r *Reader) Close() error {
	return r.db.Close()
}

// DB returns the underlying sql.DB for compatibility.
func (r *Reader) DB() *sql.DB {
	return r.db
}

// MaxConns returns the configured maximum connections.
func (r *Reader) MaxConns() int {
	return r.maxConns
}

// DBType returns the database type.
func (r *Reader) DBType() string {
	return "mssql"
}

// PoolStats returns connection pool statistics.
func (r *Reader) PoolStats() stats.PoolStats {
	dbStats := r.db.Stats()
	return stats.PoolStats{
		DBType:      "mssql",
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}

// ExtractSchema extracts table metadata from the source database.
func (r *Reader) ExtractSchema(ctx context.Context, schema string) ([]driver.Table, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			t.TABLE_SCHEMA,
			t.TABLE_NAME
		FROM INFORMATION_SCHEMA.TABLES t
		WHERE t.TABLE_TYPE = 'BASE TABLE'
		  AND t.TABLE_SCHEMA = @schema
		ORDER BY t.TABLE_NAME
	`, sql.Named("schema", schema))
	if err != nil {
		return nil, fmt.Errorf("querying tables: %w", err)
	}
	defer rows.Close()

	var tables []driver.Table
	for rows.Next() {
		var t driver.Table
		if err := rows.Scan(&t.Schema, &t.Name); err != nil {
			return nil, fmt.Errorf("scanning table: %w", err)
		}

		// Get columns
		if err := r.loadColumns(ctx, &t); err != nil {
			return nil, fmt.Errorf("loading columns for %s: %w", t.FullName(), err)
		}

		// Get primary key
		if err := r.loadPrimaryKey(ctx, &t); err != nil {
			return nil, fmt.Errorf("loading PK for %s: %w", t.FullName(), err)
		}

		// Populate PKColumns with full column metadata
		t.PopulatePKColumns()

		// Get row count
		if err := r.loadRowCount(ctx, &t); err != nil {
			return nil, fmt.Errorf("loading row count for %s: %w", t.FullName(), err)
		}

		// Compute Go heap cost per row from column metadata (static baseline)
		t.EstimatedRowSize = t.GoHeapBytesPerRow()

		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating tables: %w", err)
	}

	// Override with actual avg row sizes from database statistics when available.
	r.applyActualRowSizes(ctx, schema, tables)

	return tables, nil
}

// applyActualRowSizes queries sys.dm_db_partition_stats for actual average row
// sizes and overrides the static estimate when the DB reports a larger value.
func (r *Reader) applyActualRowSizes(ctx context.Context, schema string, tables []driver.Table) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			t.name AS table_name,
			CASE WHEN SUM(p.rows) > 0
				THEN SUM(a.total_pages) * 8 * 1024 / SUM(p.rows)
				ELSE 0
			END AS avg_row_size
		FROM sys.tables t
		INNER JOIN sys.indexes i ON t.object_id = i.object_id
		INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
		INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
		INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @schema AND i.index_id <= 1
		GROUP BY t.name
	`, sql.Named("schema", schema))
	if err != nil {
		logging.Debug("Failed to query actual row sizes: %v", err)
		return
	}
	defer rows.Close()

	sizeMap := make(map[string]int64)
	for rows.Next() {
		var name string
		var avgSize int64
		if err := rows.Scan(&name, &avgSize); err != nil {
			continue
		}
		if avgSize > 0 {
			sizeMap[name] = avgSize
		}
	}

	for i := range tables {
		if dbSize, ok := sizeMap[tables[i].Name]; ok && dbSize > tables[i].EstimatedRowSize {
			logging.Debug("Table %s: using DB avg row size %d bytes (static estimate was %d)",
				tables[i].Name, dbSize, tables[i].EstimatedRowSize)
			tables[i].EstimatedRowSize = dbSize
		}
	}
}

func (r *Reader) loadColumns(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			COLUMN_NAME,
			DATA_TYPE,
			ISNULL(CHARACTER_MAXIMUM_LENGTH, 0),
			ISNULL(NUMERIC_PRECISION, 0),
			ISNULL(NUMERIC_SCALE, 0),
			ISNULL(DATETIME_PRECISION, -1),
			CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END,
			COLUMNPROPERTY(OBJECT_ID(QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME)), COLUMN_NAME, 'IsIdentity'),
			ORDINAL_POSITION,
			ISNULL(COLUMN_DEFAULT, '')
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
		ORDER BY ORDINAL_POSITION
	`, sql.Named("schema", t.Schema), sql.Named("table", t.Name))
	if err != nil {
		return fmt.Errorf("querying columns: %w", err)
	}
	defer rows.Close()

	t.Columns = nil
	for rows.Next() {
		var col driver.Column
		var isNullable, dtPrecision int
		var isIdentity sql.NullInt64
		if err := rows.Scan(&col.Name, &col.DataType, &col.MaxLength, &col.Precision, &col.Scale, &dtPrecision, &isNullable, &isIdentity, &col.OrdinalPos, &col.DefaultExpression); err != nil {
			return fmt.Errorf("scanning column: %w", err)
		}
		if dtPrecision >= 0 {
			p := dtPrecision
			col.DatetimePrecision = &p
		}
		col.IsNullable = isNullable == 1
		col.IsIdentity = isIdentity.Valid && isIdentity.Int64 == 1
		if strings.EqualFold(col.DataType, "timestamp") {
			// INFORMATION_SCHEMA reports rowversion under its deprecated
			// synonym "timestamp" with NULL CHARACTER_MAXIMUM_LENGTH; rename
			// so renderers don't conflate it with datetime types.
			col.DataType = "rowversion"
			col.MaxLength = 8
		}
		t.Columns = append(t.Columns, col)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating columns: %w", err)
	}

	if err := r.loadComputedColumns(ctx, t); err != nil {
		return fmt.Errorf("loading computed columns: %w", err)
	}

	return nil
}

// loadComputedColumns annotates t.Columns with computed-column metadata.
// MSSQL stores computed-column definitions in sys.computed_columns; INFORMATION_SCHEMA
// has no equivalent.
func (r *Reader) loadComputedColumns(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT cc.name, cc.definition, cc.is_persisted
		FROM sys.computed_columns cc
		JOIN sys.tables tb ON cc.object_id = tb.object_id
		JOIN sys.schemas s ON tb.schema_id = s.schema_id
		WHERE s.name = @schema AND tb.name = @table
	`, sql.Named("schema", t.Schema), sql.Named("table", t.Name))
	if err != nil {
		return fmt.Errorf("querying computed columns: %w", err)
	}
	defer rows.Close()

	computed := make(map[string]struct {
		def       string
		persisted bool
	})
	for rows.Next() {
		var name, def string
		var persisted bool
		if err := rows.Scan(&name, &def, &persisted); err != nil {
			return fmt.Errorf("scanning computed column: %w", err)
		}
		computed[name] = struct {
			def       string
			persisted bool
		}{def, persisted}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating computed columns: %w", err)
	}

	for i := range t.Columns {
		if c, ok := computed[t.Columns[i].Name]; ok {
			t.Columns[i].IsComputed = true
			t.Columns[i].ComputedExpression = c.def
			t.Columns[i].ComputedPersisted = c.persisted
			// Computed columns don't have meaningful DEFAULT clauses
			t.Columns[i].DefaultExpression = ""
		}
	}

	return nil
}

func (r *Reader) loadPrimaryKey(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT c.COLUMN_NAME
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
			ON c.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
			AND c.TABLE_SCHEMA = tc.TABLE_SCHEMA
			AND c.TABLE_NAME = tc.TABLE_NAME
		WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		  AND tc.TABLE_SCHEMA = @schema
		  AND tc.TABLE_NAME = @table
		ORDER BY c.ORDINAL_POSITION
	`, sql.Named("schema", t.Schema), sql.Named("table", t.Name))
	if err != nil {
		return fmt.Errorf("querying primary key: %w", err)
	}
	defer rows.Close()

	t.PrimaryKey = nil
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return fmt.Errorf("scanning PK column: %w", err)
		}
		t.PrimaryKey = append(t.PrimaryKey, colName)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating primary key: %w", err)
	}

	return nil
}

func (r *Reader) loadRowCount(ctx context.Context, t *driver.Table) error {
	query := `
		SELECT SUM(p.rows)
		FROM sys.partitions p
		JOIN sys.tables t ON p.object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @schema AND t.name = @table AND p.index_id IN (0, 1)
	`

	return r.db.QueryRowContext(ctx, query,
		sql.Named("schema", t.Schema),
		sql.Named("table", t.Name)).Scan(&t.RowCount)
}

// LoadIndexes loads all non-PK indexes for a table.
func (r *Reader) LoadIndexes(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			i.name AS index_name,
			i.is_unique,
			i.type_desc,
			ISNULL(key_cols.columns, '') AS columns,
			ISNULL(include_cols.columns, '') AS include_columns,
			ISNULL(i.filter_definition, '') AS filter_definition
		FROM sys.indexes i
		JOIN sys.tables tb ON i.object_id = tb.object_id
		JOIN sys.schemas s ON tb.schema_id = s.schema_id
		OUTER APPLY (
			SELECT STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
			FROM sys.index_columns ic
			JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
			WHERE ic.object_id = i.object_id
			  AND ic.index_id = i.index_id
			  AND ic.is_included_column = 0
		) key_cols
		OUTER APPLY (
			SELECT STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.index_column_id) AS columns
			FROM sys.index_columns ic
			JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
			WHERE ic.object_id = i.object_id
			  AND ic.index_id = i.index_id
			  AND ic.is_included_column = 1
		) include_cols
		WHERE s.name = @schema
		  AND tb.name = @table
		  AND i.is_primary_key = 0
		  AND i.type > 0
		ORDER BY i.name
	`, sql.Named("schema", t.Schema), sql.Named("table", t.Name))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var idx driver.Index
		var typeDesc, colsStr, includeStr string
		if err := rows.Scan(&idx.Name, &idx.IsUnique, &typeDesc, &colsStr, &includeStr, &idx.Filter); err != nil {
			return err
		}
		idx.IsClustered = typeDesc == "CLUSTERED"
		idx.Columns = util.SplitCSV(colsStr)
		if includeStr != "" {
			idx.IncludeCols = util.SplitCSV(includeStr)
		}
		t.Indexes = append(t.Indexes, idx)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating indexes: %w", err)
	}

	return nil
}

// fkColumnDelimiter is used to separate column names in STRING_AGG.
// Using CHAR(1) (SOH) as it cannot appear in valid SQL Server identifiers.
const fkColumnDelimiter = "\x01"

// LoadForeignKeys loads all foreign keys for a table.
func (r *Reader) LoadForeignKeys(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			fk.name AS fk_name,
			STRING_AGG(c.name, CHAR(1)) WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS columns,
			rs.name AS ref_schema,
			rt.name AS ref_table,
			STRING_AGG(rc.name, CHAR(1)) WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS ref_columns,
			CASE fk.delete_referential_action
				WHEN 0 THEN 'NO ACTION'
				WHEN 1 THEN 'CASCADE'
				WHEN 2 THEN 'SET NULL'
				WHEN 3 THEN 'SET DEFAULT'
			END AS delete_rule,
			CASE fk.update_referential_action
				WHEN 0 THEN 'NO ACTION'
				WHEN 1 THEN 'CASCADE'
				WHEN 2 THEN 'SET NULL'
				WHEN 3 THEN 'SET DEFAULT'
			END AS update_rule
		FROM sys.foreign_keys fk
		JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
		JOIN sys.tables pt ON fk.parent_object_id = pt.object_id
		JOIN sys.schemas ps ON pt.schema_id = ps.schema_id
		JOIN sys.columns c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
		JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
		JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
		JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
		WHERE ps.name = @schema AND pt.name = @table
		GROUP BY fk.name, rs.name, rt.name, fk.delete_referential_action, fk.update_referential_action
		ORDER BY fk.name
	`, sql.Named("schema", t.Schema), sql.Named("table", t.Name))
	if err != nil {
		return fmt.Errorf("querying foreign keys: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var fk driver.ForeignKey
		var columns, refColumns string
		if err := rows.Scan(&fk.Name, &columns, &fk.RefSchema, &fk.RefTable, &refColumns,
			&fk.OnDelete, &fk.OnUpdate); err != nil {
			return fmt.Errorf("scanning FK for %s.%s: %w", t.Schema, t.Name, err)
		}
		fk.Columns = strings.Split(columns, fkColumnDelimiter)
		fk.RefColumns = strings.Split(refColumns, fkColumnDelimiter)
		t.ForeignKeys = append(t.ForeignKeys, fk)
	}
	return rows.Err()
}

// LoadCheckConstraints loads all check constraints for a table.
func (r *Reader) LoadCheckConstraints(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			cc.name AS constraint_name,
			cc.definition
		FROM sys.check_constraints cc
		JOIN sys.tables t ON cc.parent_object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @schema AND t.name = @table
		ORDER BY cc.name
	`, sql.Named("schema", t.Schema), sql.Named("table", t.Name))
	if err != nil {
		return fmt.Errorf("querying check constraints: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var chk driver.CheckConstraint
		if err := rows.Scan(&chk.Name, &chk.Definition); err != nil {
			return fmt.Errorf("scanning check constraint for %s.%s: %w", t.Schema, t.Name, err)
		}
		t.CheckConstraints = append(t.CheckConstraints, chk)
	}
	return rows.Err()
}

// GetRowCount returns the row count for a table.
// It first tries a fast statistics-based count, then falls back to COUNT(*) if needed.
func (r *Reader) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	// Try fast stats-based count first
	count, err := r.GetRowCountFast(ctx, schema, table)
	if err == nil && count > 0 {
		return count, nil
	}

	// Fall back to COUNT(*)
	err = r.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", r.dialect.QualifyTable(schema, table))).Scan(&count)
	return count, err
}

// GetRowCountFast returns an approximate row count using system statistics.
// This is much faster than COUNT(*) for large tables.
func (r *Reader) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	query := `
		SELECT SUM(p.rows)
		FROM sys.partitions p
		JOIN sys.tables t ON p.object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @schema AND t.name = @table AND p.index_id IN (0, 1)
	`
	err := r.db.QueryRowContext(ctx, query,
		sql.Named("schema", schema),
		sql.Named("table", table)).Scan(&count)
	return count, err
}

// GetRowCountExact returns the exact row count using COUNT(*).
// This may be slow on large tables.
func (r *Reader) GetRowCountExact(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WITH (NOLOCK)", r.dialect.QualifyTable(schema, table))).Scan(&count)
	return count, err
}

// getCompatibilityLevel returns the database compatibility level.
func getCompatibilityLevel(db *sql.DB) (int, error) {
	var level int
	err := db.QueryRow("SELECT compatibility_level FROM sys.databases WHERE name = DB_NAME()").Scan(&level)
	if err != nil {
		return 0, fmt.Errorf("querying compatibility level: %w", err)
	}
	return level, nil
}
