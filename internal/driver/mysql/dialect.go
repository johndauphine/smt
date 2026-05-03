package mysql

import (
	"fmt"
	"net/url"
	"strings"

	"smt/internal/driver"
)

// Dialect implements driver.Dialect for MySQL/MariaDB.
type Dialect struct{}

func (d *Dialect) DBType() string { return "mysql" }

func (d *Dialect) QuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func (d *Dialect) QualifyTable(schema, table string) string {
	// MySQL uses database.table, but schema is often empty (database is in DSN)
	if schema == "" {
		return d.QuoteIdentifier(table)
	}
	return d.QuoteIdentifier(schema) + "." + d.QuoteIdentifier(table)
}

func (d *Dialect) BuildDSN(host string, port int, database, user, password string, opts map[string]any) string {
	// MySQL DSN format: user:password@tcp(host:port)/database?params
	encodedUser := url.QueryEscape(user)
	encodedPassword := url.QueryEscape(password)

	params := url.Values{}
	params.Set("parseTime", "true")
	params.Set("multiStatements", "true")
	params.Set("interpolateParams", "true")

	// Handle SSL/TLS mode
	if sslMode, ok := opts["ssl_mode"].(string); ok && sslMode != "" {
		switch strings.ToLower(sslMode) {
		case "disable", "disabled", "false":
			params.Set("tls", "false")
		case "require", "required", "true":
			params.Set("tls", "true")
		case "verify-ca", "verify_ca":
			params.Set("tls", "skip-verify")
		case "verify-full", "verify_full", "verify-identity", "verify_identity":
			params.Set("tls", "true")
		default:
			params.Set("tls", "preferred")
		}
	} else {
		params.Set("tls", "preferred")
	}

	// Handle charset
	if charset, ok := opts["charset"].(string); ok && charset != "" {
		params.Set("charset", charset)
	} else {
		params.Set("charset", "utf8mb4")
	}

	// Handle collation
	if collation, ok := opts["collation"].(string); ok && collation != "" {
		params.Set("collation", collation)
	}

	// Handle timezone
	if loc, ok := opts["loc"].(string); ok && loc != "" {
		params.Set("loc", loc)
	} else {
		params.Set("loc", "UTC")
	}

	// Set read/write timeouts to prevent indefinite hangs on large batch inserts.
	// These are go-sql-driver/mysql DSN parameters that set deadlines on the
	// underlying TCP connection. 5 minutes is generous for bulk operations.
	if _, ok := opts["writeTimeout"]; !ok {
		params.Set("writeTimeout", "5m")
	}
	if _, ok := opts["readTimeout"]; !ok {
		params.Set("readTimeout", "5m")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		encodedUser, encodedPassword, host, port, database, params.Encode())

	return dsn
}

func (d *Dialect) ParameterPlaceholder(_ int) string {
	return "?"
}

func (d *Dialect) ColumnList(cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = d.QuoteIdentifier(c)
	}
	return strings.Join(quoted, ", ")
}

func (d *Dialect) TableHint(strictConsistency bool) string {
	if strictConsistency {
		return "" // No hint needed for consistent reads (use FOR SHARE if needed)
	}
	return "" // MySQL doesn't have NOLOCK equivalent; uses MVCC
}

func (d *Dialect) ColumnListForSelect(cols, colTypes []string, targetDBType string) string {
	quoted := make([]string, len(cols))
	isCrossEngine := targetDBType != "mysql"

	for i, c := range cols {
		colType := ""
		if i < len(colTypes) {
			colType = strings.ToLower(colTypes[i])
		}

		// Convert spatial types for cross-engine migrations
		if isCrossEngine && (colType == "geometry" || colType == "point" ||
			colType == "linestring" || colType == "polygon" ||
			colType == "multipoint" || colType == "multilinestring" ||
			colType == "multipolygon" || colType == "geometrycollection") {
			quoted[i] = fmt.Sprintf("ST_AsText(%s) AS %s", d.QuoteIdentifier(c), d.QuoteIdentifier(c))
			continue
		}
		quoted[i] = d.QuoteIdentifier(c)
	}
	return strings.Join(quoted, ", ")
}

func (d *Dialect) BuildKeysetQuery(cols, pkCol, schema, table, _ string, hasMaxPK bool, dateFilter *driver.DateFilter) string {
	dateClause := ""
	if dateFilter != nil {
		// Only include rows where the date column is >= the filter timestamp.
		// Rows with NULL dates are excluded (they haven't been modified).
		dateClause = fmt.Sprintf(" AND %s >= ?", d.QuoteIdentifier(dateFilter.Column))
	}

	qualifiedTable := d.QualifyTable(schema, table)
	qPK := d.QuoteIdentifier(pkCol)

	if hasMaxPK {
		return fmt.Sprintf(`
			SELECT %s FROM %s
			WHERE %s > ? AND %s <= ?%s
			ORDER BY %s
			LIMIT ?
		`, cols, qualifiedTable, qPK, qPK, dateClause, qPK)
	}
	return fmt.Sprintf(`
		SELECT %s FROM %s
		WHERE %s > ?%s
		ORDER BY %s
		LIMIT ?
	`, cols, qualifiedTable, qPK, dateClause, qPK)
}

func (d *Dialect) BuildKeysetArgs(lastPK, maxPK any, limit int, hasMaxPK bool, dateFilter *driver.DateFilter) []any {
	if hasMaxPK {
		if dateFilter != nil {
			return []any{lastPK, maxPK, dateFilter.Timestamp, limit}
		}
		return []any{lastPK, maxPK, limit}
	}
	if dateFilter != nil {
		return []any{lastPK, dateFilter.Timestamp, limit}
	}
	return []any{lastPK, limit}
}

func (d *Dialect) BuildRowNumberQuery(cols, orderBy, schema, table, _ string, dateFilter *driver.DateFilter) string {
	// MySQL 8.0+ supports window functions
	outerCols := extractColumnAliases(cols)
	qualifiedTable := d.QualifyTable(schema, table)

	// Build WHERE clause for date filter
	whereClause := ""
	if dateFilter != nil {
		// Only include rows where the date column is >= the filter timestamp.
		// Rows with NULL dates are excluded (they haven't been modified).
		whereClause = fmt.Sprintf(" WHERE %s >= ?", d.QuoteIdentifier(dateFilter.Column))
	}

	return fmt.Sprintf(`
		SELECT %s FROM (
			SELECT %s, ROW_NUMBER() OVER (ORDER BY %s) as __rn
			FROM %s%s
		) AS numbered
		WHERE __rn > ? AND __rn <= ?
		ORDER BY __rn
	`, outerCols, cols, orderBy, qualifiedTable, whereClause)
}

// extractColumnAliases extracts just the column aliases from a column expression list.
func extractColumnAliases(cols string) string {
	parts := strings.Split(cols, ",")
	aliases := make([]string, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		upperPart := strings.ToUpper(part)
		if idx := strings.LastIndex(upperPart, " AS "); idx != -1 {
			aliases[i] = strings.TrimSpace(part[idx+4:])
		} else {
			aliases[i] = part
		}
	}
	return strings.Join(aliases, ", ")
}

func (d *Dialect) BuildRowNumberArgs(rowNum int64, limit int, dateFilter *driver.DateFilter) []any {
	if dateFilter != nil {
		// Date filter parameter comes before the row number parameters
		return []any{dateFilter.Timestamp, rowNum, rowNum + int64(limit)}
	}
	return []any{rowNum, rowNum + int64(limit)}
}

func (d *Dialect) PartitionBoundariesQuery(pkCol, schema, table string, numPartitions int) string {
	qPK := d.QuoteIdentifier(pkCol)
	qualifiedTable := d.QualifyTable(schema, table)
	return fmt.Sprintf(`
		SELECT partition_id, MIN(%s), MAX(%s), COUNT(*) FROM (
			SELECT %s, NTILE(%d) OVER (ORDER BY %s) as partition_id FROM %s
		) AS numbered
		GROUP BY partition_id ORDER BY partition_id
	`, qPK, qPK, qPK, numPartitions, qPK, qualifiedTable)
}

func (d *Dialect) RowCountQuery(useStats bool) string {
	if useStats {
		// Use information_schema for fast approximate count
		return `SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`
	}
	return `SELECT COUNT(*) FROM %s`
}

func (d *Dialect) DateColumnQuery() string {
	return `SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`
}

func (d *Dialect) ValidDateTypes() map[string]bool {
	return map[string]bool{
		"datetime":  true,
		"timestamp": true,
		"date":      true,
	}
}

// AIPromptAugmentation returns MySQL-specific instructions for AI DDL
// generation. Mirrors the postgres and mssql dialect augmentations:
// auto-increment translation and Unicode-safe charset handling.
func (d *Dialect) AIPromptAugmentation() string {
	return `
CRITICAL MySQL AUTO_INCREMENT column rules:
- When the source column is auto-incrementing — PostgreSQL ` + "`GENERATED BY DEFAULT AS IDENTITY`" + `,
  ` + "`GENERATED ALWAYS AS IDENTITY`" + `, ` + "`SERIAL`" + ` family, or SQL Server ` + "`IDENTITY(seed,inc)`" + ` —
  translate it to MySQL ` + "`AUTO_INCREMENT`" + ` on the target column.
- Example: PostgreSQL ` + "`id integer GENERATED BY DEFAULT AS IDENTITY`" + ` -> MySQL ` + "`id INT NOT NULL AUTO_INCREMENT`" + `.
- Example: SQL Server ` + "`id INT IDENTITY(1,1)`" + ` -> MySQL ` + "`id INT NOT NULL AUTO_INCREMENT`" + `.
- The AUTO_INCREMENT column must be a key (typically the primary key). MySQL allows
  only one AUTO_INCREMENT column per table.
- NEVER drop the auto-increment behavior: emitting plain ` + "`INT NOT NULL`" + ` for a PK that
  was identity in the source breaks INSERT semantics on the target.

CRITICAL MySQL VARCHAR / charset rules:
- The N in MySQL ` + "`VARCHAR(N)`" + ` counts CHARACTERS when the column uses a multi-byte
  character set (utf8mb4 in particular). Translate PostgreSQL ` + "`varchar(N)`" + ` and SQL Server
  ` + "`NVARCHAR(N)`" + ` one-to-one: ` + "`varchar(40)`" + ` -> ` + "`VARCHAR(40)`" + `, ` + "`NVARCHAR(150)`" + ` -> ` + "`VARCHAR(150)`" + `.
- Do NOT scale the length. Both N values mean the same thing — max number of characters.
- Use ` + "`utf8mb4`" + ` charset and a ` + "`utf8mb4_*`" + ` collation for any column holding text from
  a Unicode-by-default source (PostgreSQL or SQL Server NVARCHAR).
- For unbounded source text (PostgreSQL ` + "`text`" + `, SQL Server ` + "`NVARCHAR(MAX)`" + `), pick the
  smallest MySQL TEXT type that comfortably fits the data. The TEXT-family limits
  are in BYTES, not characters, so with utf8mb4 (up to 4 bytes per character)
  effective character capacity is at worst 1/4 of the byte limit:
    ` + "`TEXT`" + `       — 65,535 bytes (~16k chars utf8mb4 worst case)
    ` + "`MEDIUMTEXT`" + ` — 16,777,215 bytes (~4M chars utf8mb4 worst case)
    ` + "`LONGTEXT`" + `   — 4,294,967,295 bytes (~1G chars utf8mb4 worst case)
  When in doubt prefer the larger type — silent truncation on insert is much
  worse than a few extra bytes of pointer overhead.

CRITICAL MySQL fractional-second precision rule:
- When mapping any source column with ` + "`scale > 0`" + ` (per the introspection metadata) to a
  MySQL ` + "`DATETIME(N)`" + ` / ` + "`TIMESTAMP(N)`" + ` / ` + "`TIME(N)`" + ` target — regardless of the
  source dialect's spelling (MSSQL ` + "`DATETIME2(N)`" + `, ` + "`DATETIMEOFFSET`" + `;
  PG ` + "`TIMESTAMP(N)`" + ` / ` + "`TIMESTAMPTZ(N)`" + `; MySQL ` + "`DATETIME(N)`" + `; etc.) — any
  ` + "`CURRENT_TIMESTAMP`" + ` / ` + "`NOW()`" + ` default in the target DDL MUST carry the same precision
  argument: ` + "`created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)`" + `.
- MySQL rejects mismatched precision with error 1067 "Invalid default value".
- This applies only to function defaults; literal-value defaults are unaffected.

CRITICAL MySQL expression-default parenthesization rule:
- MySQL 8.0.13+ allows arbitrary expressions as DEFAULT values, but the expression
  MUST be wrapped in parentheses. Bare function calls are a syntax error.
- Wrong: ` + "`employee_uuid CHAR(36) NOT NULL DEFAULT UUID()`" + `  (Error 1064)
- Right: ` + "`employee_uuid CHAR(36) NOT NULL DEFAULT (UUID())`" + `
- Wrong: ` + "`settings JSON NOT NULL DEFAULT JSON_OBJECT()`" + `  (Error 1064)
- Right: ` + "`settings JSON NOT NULL DEFAULT (JSON_OBJECT())`" + `
- Wrong: ` + "`settings JSON NOT NULL DEFAULT '{}'`" + `  (Error 1101 — JSON/TEXT/BLOB cols
   pre-8.0.13 forbid defaults entirely; 8.0.13+ requires the parenthesized
   expression form even for literal-looking JSON defaults)
- Right: ` + "`settings JSON NOT NULL DEFAULT (JSON_OBJECT())`" + `
- Right: ` + "`settings JSON NOT NULL DEFAULT (CAST('{}' AS JSON))`" + `
- The ` + "`CURRENT_TIMESTAMP`" + ` family is the one exception — it works without parens
  (` + "`DEFAULT CURRENT_TIMESTAMP`" + ` and ` + "`DEFAULT CURRENT_TIMESTAMP(6)`" + ` are both valid).
  Every other function default needs the parens.
`
}

// AIDropTablePromptAugmentation returns MySQL-specific instructions for DROP TABLE DDL.
func (d *Dialect) AIDropTablePromptAugmentation() string {
	return `
MySQL-specific requirements:
- MySQL does NOT support CASCADE for DROP TABLE
- You MUST disable foreign key checks before dropping: SET FOREIGN_KEY_CHECKS = 0;
- Then DROP TABLE IF EXISTS with fully qualified table name using backticks
- Then re-enable foreign key checks: SET FOREIGN_KEY_CHECKS = 1;
- Return all three statements as a single response, each ending with semicolon
`
}
