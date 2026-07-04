package conformance

import (
	"reflect"
	"testing"

	"smt/internal/driver"
	"smt/internal/driver/postgres"
)

// TestPostgresConformance pins the PostgreSQL driver contract.
//
// DatabaseContext static metadata (IdentifierCase "lower", MaxIdentifierLength
// 63, VarcharSemantics "char") is NOT pinned here: it lives in the unexported
// postgres.gatherDatabaseContext, which is reachable only via a live pgxpool,
// and Reader.DatabaseContext() dereferences that live pool. No pure accessor
// exposes the static half, so it cannot be asserted without a database.
func TestPostgresConformance(t *testing.T) {
	RunDriverConformance(t, DriverCase{
		Name:    "postgres",
		Aliases: []string{"postgresql", "pg"},

		DriverType:  reflect.TypeOf((*postgres.Driver)(nil)),
		ReaderType:  reflect.TypeOf((*postgres.Reader)(nil)),
		WriterType:  reflect.TypeOf((*postgres.Writer)(nil)),
		DialectType: reflect.TypeOf((*postgres.Dialect)(nil)),

		// Double-quoted identifiers; embedded " is doubled.
		QuoteInput:       "Col",
		QuoteWant:        `"Col"`,
		QuoteEscapeInput: `a"b`,
		QuoteEscapeWant:  `"a""b"`,

		// schema.table, both sides always quoted (no schema-less special case:
		// an empty schema still emits a quoted-empty prefix).
		QualSchema:         "public",
		QualTable:          "users",
		QualWant:           `"public"."users"`,
		QualSchemalessWant: "\"\".\"users\"",

		PlaceholderIndex: 3,
		PlaceholderWant:  "$3",

		// PostgreSQL folds to lowercase, slugs non-alphanumeric to '_', and
		// prefixes a leading digit with col_.
		NormalizeInput: "1st Name",
		NormalizeWant:  "col_1st_name",

		WantDefaults: driver.DriverDefaults{
			Port:                  5432,
			Schema:                "public",
			SSLMode:               "require",
			WriteAheadWriters:     2,
			ScaleWritersWithCores: true,
		},

		DSNCases: []DSNCase{
			{
				Desc:     "nil opts -> sslmode=prefer, credentials url-escaped",
				Host:     "localhost",
				Port:     5432,
				Database: "my db",
				User:     "u@d",
				Password: "p:w/d",
				Opts:     nil,
				Want:     "postgres://u%40d:p%3Aw%2Fd@localhost:5432/my%20db?sslmode=prefer",
			},
			{
				Desc:     "explicit sslmode=disable honored",
				Host:     "h",
				Port:     5432,
				Database: "db",
				User:     "usr",
				Password: "pwd",
				Opts:     map[string]any{"sslmode": "disable"},
				Want:     "postgres://usr:pwd@h:5432/db?sslmode=disable",
			},
		},
	})
}
