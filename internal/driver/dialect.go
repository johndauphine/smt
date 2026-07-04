package driver

import (
	"strings"
)

// Dialect abstracts database-specific SQL syntax differences.
// Each database driver provides its own Dialect implementation.
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

	// RowCountQuery returns a query to get the row count.
	// If useStats is true, may use statistics tables for faster results.
	RowCountQuery(useStats bool) string

	// AIPromptAugmentation returns database-specific instructions to append to AI prompts.
	// This allows each driver to specify its own constraints for DDL generation.
	// Returns empty string if no augmentation is needed.
	AIPromptAugmentation() string

	// AIDropTablePromptAugmentation returns database-specific instructions for DROP TABLE DDL.
	// This allows each driver to specify how to handle foreign key constraints when dropping tables.
	// Returns empty string if no augmentation is needed.
	AIDropTablePromptAugmentation() string
}

// GetDialect returns the appropriate dialect for the given database type.
// This uses the driver registry to get the dialect, eliminating switch statements.
// Returns nil if no driver is registered for the given type.
func GetDialect(dbType string) Dialect {
	d, err := Get(strings.ToLower(dbType))
	if err != nil {
		return nil
	}
	return d.Dialect()
}
