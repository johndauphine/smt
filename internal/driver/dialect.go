package driver

// Dialect abstracts database-specific SQL syntax differences.
// Each database driver provides its own Dialect implementation.
//
// SMT is schema-only; the DMT read-path surface (pagination/keyset/ROW_NUMBER
// query builders, table hints, column-list-for-select, row-count and
// date-column query helpers) was removed in #191. What remains is the
// identifier/DSN surface the deterministic renderer and connection code use.
type Dialect interface {
	// DBType returns the database type (e.g., "mssql", "postgres").
	DBType() string

	// QuoteIdentifier quotes an identifier (table, column name).
	// PostgreSQL: "identifier"
	// MSSQL: [identifier]
	// MySQL: `identifier`
	QuoteIdentifier(name string) string

	// QualifyTable returns a fully qualified table reference.
	// PostgreSQL: "schema"."table"
	// MSSQL: [schema].[table]
	QualifyTable(schema, table string) string

	// ParameterPlaceholder returns the parameter placeholder for the given index.
	// PostgreSQL: $1, $2, $3
	// MSSQL: @p1, @p2, @p3
	// MySQL: ?, ?, ?
	ParameterPlaceholder(index int) string

	// BuildDSN builds a connection string for this database.
	BuildDSN(host string, port int, database, user, password string, opts map[string]any) string
}
