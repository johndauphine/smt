package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
	"smt/internal/dbconfig"
	"smt/internal/driver"
	"smt/internal/logging"
	"smt/internal/stats"
)

// Reader implements driver.Reader for MySQL/MariaDB.
type Reader struct {
	db        *sql.DB
	config    *dbconfig.SourceConfig
	maxConns  int
	dialect   *Dialect
	version   string // raw VERSION() string from connect-time probe
	isMariaDB bool

	// dbContextOnce gates the (single) lookup of dbContext for the source side.
	dbContextOnce sync.Once
	dbContext     *driver.DatabaseContext
}

// DatabaseContext returns metadata about this source database for the AI prompt
// (charset, collation, identifier case, storage engine, version-gated feature
// list). Cached after first call.
func (r *Reader) DatabaseContext() *driver.DatabaseContext {
	r.dbContextOnce.Do(func() {
		r.dbContext = gatherDatabaseContext(r.db, r.config.Database, r.config.Host, r.version, r.isMariaDB)
	})
	return r.dbContext
}

// NewReader creates a new MySQL reader.
func NewReader(cfg *dbconfig.SourceConfig, maxConns int) (*Reader, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening connection: %w", err)
	}

	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)
	if db.Stats().MaxOpenConnections > 0 && maxConns/4 < 1 {
		db.SetMaxIdleConns(1)
	}
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	// Detect MySQL vs MariaDB
	var version string
	db.QueryRow("SELECT VERSION()").Scan(&version)
	isMariaDB := strings.Contains(strings.ToLower(version), "mariadb")
	dbType := "MySQL"
	if isMariaDB {
		dbType = "MariaDB"
	}

	logging.Debug("Connected to %s source: %s:%d/%s", dbType, cfg.Host, cfg.Port, cfg.Database)

	return &Reader{
		db:        db,
		config:    cfg,
		maxConns:  maxConns,
		dialect:   dialect,
		version:   version,
		isMariaDB: isMariaDB,
	}, nil
}

// Close closes all connections.
func (r *Reader) Close() error {
	return r.db.Close()
}

// DB returns the underlying sql.DB.
func (r *Reader) DB() *sql.DB {
	return r.db
}

// MaxConns returns the configured maximum connections.
func (r *Reader) MaxConns() int {
	return r.maxConns
}

// DBType returns the database type.
func (r *Reader) DBType() string {
	return "mysql"
}

// PoolStats returns connection pool statistics.
func (r *Reader) PoolStats() stats.PoolStats {
	dbStats := r.db.Stats()
	return stats.PoolStats{
		DBType:      "mysql",
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}

// ExtractSchema extracts table metadata from the database.
func (r *Reader) ExtractSchema(ctx context.Context, schema string) ([]driver.Table, error) {
	tables := []driver.Table{}

	// In MySQL, schema = database name
	dbName := schema
	if dbName == "" {
		dbName = r.config.Database
	}

	// Get tables
	rows, err := r.db.QueryContext(ctx, `
		SELECT TABLE_SCHEMA, TABLE_NAME
		FROM information_schema.TABLES
		WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = ?
		ORDER BY TABLE_NAME
	`, dbName)
	if err != nil {
		return nil, fmt.Errorf("querying tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var t driver.Table
		if err := rows.Scan(&t.Schema, &t.Name); err != nil {
			return nil, fmt.Errorf("scanning table: %w", err)
		}

		// Load columns
		if err := r.loadColumns(ctx, &t); err != nil {
			return nil, err
		}

		// Load primary key
		if err := r.loadPrimaryKey(ctx, &t); err != nil {
			return nil, err
		}

		// Populate PKColumns with full column metadata
		t.PopulatePKColumns()

		// Get row count
		count, err := r.GetRowCount(ctx, t.Schema, t.Name)
		if err != nil {
			logging.Warn("Failed to get row count for %s: %v", t.Name, err)
		}
		t.RowCount = count

		// Compute Go heap cost per row from column metadata (static baseline)
		t.EstimatedRowSize = t.GoHeapBytesPerRow()

		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Override with actual avg row sizes from database statistics when available.
	r.applyActualRowSizes(ctx, dbName, tables)

	return tables, nil
}

// applyActualRowSizes queries information_schema.TABLES for actual average row
// sizes and overrides the static estimate when the DB reports a larger value.
func (r *Reader) applyActualRowSizes(ctx context.Context, dbName string, tables []driver.Table) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT TABLE_NAME, IFNULL(AVG_ROW_LENGTH, 0)
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
	`, dbName)
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

// parseGeneratedColumnExtra inspects the value of information_schema.COLUMNS.EXTRA
// and reports whether the column is a true generated/computed column (and, if so,
// whether it is STORED).
//
// MySQL 8.0.13+ writes a few different markers to EXTRA:
//
//   - "VIRTUAL GENERATED"   — generated column, computed on read
//   - "STORED GENERATED"    — generated column, materialized on write
//   - "DEFAULT_GENERATED"   — *not* a generated column; just a marker that the
//     column has an expression default (e.g. "DEFAULT CURRENT_TIMESTAMP" or
//     any function default introduced in 8.0.13). Easy to misread because it
//     also contains the substring "GENERATED".
//
// A naïve substring check on "GENERATED" misclassifies the third case as a
// generated column and wipes its real default — see issue #18.
func parseGeneratedColumnExtra(extra string) (computed, persisted bool) {
	switch {
	case strings.Contains(extra, "STORED GENERATED"):
		return true, true
	case strings.Contains(extra, "VIRTUAL GENERATED"):
		return true, false
	}
	return false, false
}

func isUnsignedColumnType(columnType string) bool {
	fields := strings.Fields(strings.ToLower(columnType))
	for _, f := range fields {
		if f == "unsigned" {
			return true
		}
	}
	return false
}

// isTinyint1ColumnType reports whether COLUMN_TYPE declares the tinyint(1)
// boolean convention. Only width 1 matters: other display widths are
// deprecated cosmetics (MySQL 8.0.17+), but tinyint(1) is what connectors and
// ORMs treat as boolean, so a same-dialect migration must preserve it.
func isTinyint1ColumnType(columnType string) bool {
	first, _, _ := strings.Cut(strings.ToLower(strings.TrimSpace(columnType)), " ")
	return first == "tinyint(1)"
}

func parseOnUpdateExpression(extra string) string {
	lower := strings.ToLower(extra)
	idx := strings.Index(lower, "on update ")
	if idx < 0 {
		return ""
	}
	return strings.TrimSpace(extra[idx+len("on update "):])
}

func applyMySQLColumnDefault(c *driver.Column, defaultValue sql.NullString) {
	c.HasDefault = defaultValue.Valid
	if defaultValue.Valid {
		c.DefaultExpression = defaultValue.String
	} else {
		c.DefaultExpression = ""
	}
}

func parseEnumSetValues(columnType string) ([]string, error) {
	columnType = strings.TrimSpace(columnType)
	open := strings.IndexByte(columnType, '(')
	if open < 0 || !strings.HasSuffix(columnType, ")") {
		return nil, fmt.Errorf("invalid enum/set column type %q", columnType)
	}
	kind := strings.ToLower(strings.TrimSpace(columnType[:open]))
	if kind != "enum" && kind != "set" {
		return nil, fmt.Errorf("column type %q is not enum/set", columnType)
	}
	body := columnType[open+1 : len(columnType)-1]
	values := []string{}
	for i := 0; i < len(body); {
		for i < len(body) && (body[i] == ' ' || body[i] == '\t' || body[i] == '\n' || body[i] == '\r') {
			i++
		}
		if i >= len(body) {
			break
		}
		if body[i] != '\'' {
			return nil, fmt.Errorf("invalid enum/set literal list %q", columnType)
		}
		i++
		var b strings.Builder
		for i < len(body) {
			ch := body[i]
			switch ch {
			case '\\':
				if i+1 >= len(body) {
					return nil, fmt.Errorf("invalid enum/set escape in %q", columnType)
				}
				i++
				b.WriteByte(body[i])
			case '\'':
				if i+1 < len(body) && body[i+1] == '\'' {
					b.WriteByte('\'')
					i += 2
					continue
				}
				i++
				values = append(values, b.String())
				goto literalDone
			default:
				b.WriteByte(ch)
			}
			i++
		}
		return nil, fmt.Errorf("unterminated enum/set literal in %q", columnType)

	literalDone:
		for i < len(body) && (body[i] == ' ' || body[i] == '\t' || body[i] == '\n' || body[i] == '\r') {
			i++
		}
		if i < len(body) {
			if body[i] != ',' {
				return nil, fmt.Errorf("invalid enum/set separator in %q", columnType)
			}
			i++
		}
	}
	return values, nil
}

func (r *Reader) loadColumns(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			COLUMN_NAME,
			DATA_TYPE,
			COLUMN_TYPE,
			COALESCE(CHARACTER_MAXIMUM_LENGTH, 0),
			COALESCE(NUMERIC_PRECISION, 0),
			COALESCE(NUMERIC_SCALE, 0),
			COALESCE(DATETIME_PRECISION, -1),
			CASE WHEN IS_NULLABLE = 'YES' THEN true ELSE false END,
			CASE WHEN EXTRA LIKE '%auto_increment%' THEN true ELSE false END,
			ORDINAL_POSITION,
			COLUMN_DEFAULT,
			COALESCE(EXTRA, ''),
			COALESCE(GENERATION_EXPRESSION, '')
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("querying columns for %s: %w", t.Name, err)
	}
	defer rows.Close()

	for rows.Next() {
		var c driver.Column
		var columnType, extra, generationExpr string
		var defaultValue sql.NullString
		var dtPrecision int
		if err := rows.Scan(&c.Name, &c.DataType, &columnType, &c.MaxLength, &c.Precision, &c.Scale,
			&dtPrecision, &c.IsNullable, &c.IsIdentity, &c.OrdinalPos,
			&defaultValue, &extra, &generationExpr); err != nil {
			return fmt.Errorf("scanning column: %w", err)
		}
		applyMySQLColumnDefault(&c, defaultValue)
		if dtPrecision >= 0 {
			p := dtPrecision
			c.DatetimePrecision = &p
		}
		if strings.EqualFold(c.DataType, "enum") || strings.EqualFold(c.DataType, "set") {
			values, err := parseEnumSetValues(columnType)
			if err != nil {
				return fmt.Errorf("parsing enum/set values for %s.%s: %w", t.Name, c.Name, err)
			}
			c.EnumValues = values
		}
		c.IsUnsigned = isUnsignedColumnType(columnType)
		if strings.EqualFold(c.DataType, "tinyint") && isTinyint1ColumnType(columnType) {
			c.DisplayWidth = 1
		}
		c.OnUpdateExpression = parseOnUpdateExpression(extra)
		if computed, persisted := parseGeneratedColumnExtra(extra); computed {
			c.IsComputed = true
			c.ComputedExpression = generationExpr
			c.ComputedPersisted = persisted
			// Generated columns don't carry a regular DEFAULT clause; clear
			// any value information_schema reports here so the downstream
			// prompt doesn't double-emit.
			c.DefaultExpression = ""
			c.HasDefault = false
			c.OnUpdateExpression = ""
		}
		t.Columns = append(t.Columns, c)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return r.loadSpatialColumns(ctx, t)
}

func (r *Reader) loadSpatialColumns(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT COLUMN_NAME, COALESCE(SRS_ID, 0)
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		  AND DATA_TYPE IN ('geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection')
	`, t.Schema, t.Name)
	if err != nil {
		logging.Debug("skipping MySQL spatial SRID metadata for %s.%s: %v", t.Schema, t.Name, err)
		return nil
	}
	defer rows.Close()

	srids := make(map[string]int)
	for rows.Next() {
		var name string
		var srid int
		if err := rows.Scan(&name, &srid); err != nil {
			return fmt.Errorf("scanning spatial metadata: %w", err)
		}
		srids[strings.ToLower(name)] = srid
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating spatial metadata: %w", err)
	}
	for i := range t.Columns {
		if srid, ok := srids[strings.ToLower(t.Columns[i].Name)]; ok {
			t.Columns[i].SRID = srid
		}
	}
	return nil
}

func (r *Reader) loadPrimaryKey(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY ORDINAL_POSITION
	`, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("querying primary key for %s: %w", t.Name, err)
	}
	defer rows.Close()

	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return fmt.Errorf("scanning pk column: %w", err)
		}
		t.PrimaryKey = append(t.PrimaryKey, col)
	}
	return rows.Err()
}

type mysqlIndexPart struct {
	indexName  string
	isUnique   bool
	columnName sql.NullString
	expression sql.NullString
	subPart    sql.NullInt64
}

func mysqlIndexQuery(includeExpression bool) string {
	exprSelect := "NULL AS EXPRESSION"
	if includeExpression {
		exprSelect = "EXPRESSION"
	}
	return fmt.Sprintf(`
		SELECT
			INDEX_NAME,
			NOT NON_UNIQUE AS is_unique,
			COLUMN_NAME,
			%s,
			SUB_PART
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME != 'PRIMARY'
		ORDER BY INDEX_NAME, SEQ_IN_INDEX
	`, exprSelect)
}

func appendMySQLIndexPart(idx *driver.Index, part mysqlIndexPart) error {
	columnName := strings.TrimSpace(part.columnName.String)
	expression := strings.TrimSpace(part.expression.String)
	isExpression := !part.columnName.Valid || columnName == ""
	keyPart := columnName
	if isExpression {
		keyPart = expression
	}
	if keyPart == "" {
		return fmt.Errorf("index %s has an empty key part", part.indexName)
	}
	idx.Columns = append(idx.Columns, keyPart)
	idx.ColumnExpressions = append(idx.ColumnExpressions, isExpression)
	prefixLength := 0
	if part.subPart.Valid && part.subPart.Int64 > 0 {
		prefixLength = int(part.subPart.Int64)
	}
	idx.ColumnPrefixLengths = append(idx.ColumnPrefixLengths, prefixLength)
	return nil
}

func compactIndexKeyPartMetadata(idx *driver.Index) {
	hasExpression := false
	for _, isExpression := range idx.ColumnExpressions {
		if isExpression {
			hasExpression = true
			break
		}
	}
	if !hasExpression {
		idx.ColumnExpressions = nil
	}
	hasPrefix := false
	for _, prefixLength := range idx.ColumnPrefixLengths {
		if prefixLength > 0 {
			hasPrefix = true
			break
		}
	}
	if !hasPrefix {
		idx.ColumnPrefixLengths = nil
	}
}

// LoadIndexes loads index metadata for a table.
func (r *Reader) LoadIndexes(ctx context.Context, t *driver.Table) error {
	includeExpression := !r.isMariaDB
	rows, err := r.db.QueryContext(ctx, mysqlIndexQuery(includeExpression), t.Schema, t.Name)
	if err != nil && includeExpression && isUnknownExpressionColumnError(err) {
		rows, err = r.db.QueryContext(ctx, mysqlIndexQuery(false), t.Schema, t.Name)
	}
	if err != nil {
		return fmt.Errorf("querying indexes: %w", err)
	}
	defer rows.Close()

	indexByName := make(map[string]int)
	for rows.Next() {
		var part mysqlIndexPart
		if err := rows.Scan(&part.indexName, &part.isUnique, &part.columnName, &part.expression, &part.subPart); err != nil {
			return err
		}
		idxPos, ok := indexByName[part.indexName]
		if !ok {
			t.Indexes = append(t.Indexes, driver.Index{Name: part.indexName, IsUnique: part.isUnique})
			idxPos = len(t.Indexes) - 1
			indexByName[part.indexName] = idxPos
		}
		if err := appendMySQLIndexPart(&t.Indexes[idxPos], part); err != nil {
			return err
		}
	}
	for i := range t.Indexes {
		compactIndexKeyPartMetadata(&t.Indexes[i])
	}
	return rows.Err()
}

func isUnknownExpressionColumnError(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown column") && strings.Contains(msg, "expression")
}

// LoadForeignKeys loads foreign key metadata for a table.
func (r *Reader) LoadForeignKeys(ctx context.Context, t *driver.Table) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			kcu.CONSTRAINT_NAME,
			GROUP_CONCAT(kcu.COLUMN_NAME ORDER BY kcu.ORDINAL_POSITION) AS columns,
			kcu.REFERENCED_TABLE_SCHEMA,
			kcu.REFERENCED_TABLE_NAME,
			GROUP_CONCAT(kcu.REFERENCED_COLUMN_NAME ORDER BY kcu.ORDINAL_POSITION) AS ref_columns,
			rc.UPDATE_RULE,
			rc.DELETE_RULE
		FROM information_schema.KEY_COLUMN_USAGE kcu
		JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
			ON rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
			AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
		WHERE kcu.TABLE_SCHEMA = ? AND kcu.TABLE_NAME = ?
			AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
		GROUP BY kcu.CONSTRAINT_NAME, kcu.REFERENCED_TABLE_SCHEMA,
			kcu.REFERENCED_TABLE_NAME, rc.UPDATE_RULE, rc.DELETE_RULE
		ORDER BY kcu.CONSTRAINT_NAME
	`, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("querying foreign keys: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var fk driver.ForeignKey
		var columns, refColumns string
		if err := rows.Scan(&fk.Name, &columns, &fk.RefSchema, &fk.RefTable, &refColumns,
			&fk.OnUpdate, &fk.OnDelete); err != nil {
			return err
		}
		fk.Columns = strings.Split(columns, ",")
		fk.RefColumns = strings.Split(refColumns, ",")
		t.ForeignKeys = append(t.ForeignKeys, fk)
	}
	return rows.Err()
}

// LoadCheckConstraints loads check constraint metadata for a table.
func (r *Reader) LoadCheckConstraints(ctx context.Context, t *driver.Table) error {
	// MySQL 8.0.16+ and MariaDB 10.2.1+ support check constraints
	rows, err := r.db.QueryContext(ctx, mysqlCheckConstraintsQuery(r.isMariaDB), t.Schema, t.Name)
	if err != nil {
		// Check constraints not supported in older versions
		logging.Warn("Warning: loading check constraints for %s: %v", t.Name, err)
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var chk driver.CheckConstraint
		if err := rows.Scan(&chk.Name, &chk.Definition); err != nil {
			return err
		}
		t.CheckConstraints = append(t.CheckConstraints, chk)
	}
	return rows.Err()
}

func mysqlCheckConstraintsQuery(isMariaDB bool) string {
	tableJoin := ""
	if isMariaDB {
		tableJoin = "AND cc.TABLE_NAME = tc.TABLE_NAME"
	}
	return fmt.Sprintf(`
		SELECT
			cc.CONSTRAINT_NAME,
			cc.CHECK_CLAUSE
		FROM information_schema.TABLE_CONSTRAINTS tc
		JOIN information_schema.CHECK_CONSTRAINTS cc
		  ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
		 AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
		 %s
		WHERE tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'CHECK'
	`, tableJoin)
}

// GetRowCount returns the row count for a table.
func (r *Reader) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	count, err := r.GetRowCountFast(ctx, schema, table)
	if err == nil && count > 0 {
		return count, nil
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", r.dialect.QualifyTable(schema, table))
	err = r.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

// GetRowCountFast returns an approximate row count using system statistics.
func (r *Reader) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx,
		`SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
		schema, table).Scan(&count)
	return count, err
}

// GetRowCountExact returns the exact row count using COUNT(*).
func (r *Reader) GetRowCountExact(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", r.dialect.QualifyTable(schema, table))
	err := r.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}
