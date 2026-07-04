package conformance

import (
	"reflect"
	"testing"

	"smt/internal/driver"
	"smt/internal/driver/mysql"
)

// TestMySQLConformance pins the MySQL/MariaDB driver contract.
//
// DatabaseContext static metadata is NOT pinned here for the same reason as the
// other engines: it lives in the unexported mysql.gatherDatabaseContext,
// reachable only through a live *sql.DB, with no pure accessor for the static
// half.
//
// Finding (existing behavior, not changed by this issue): BuildDSN does NOT
// url-escape the database name — it is interpolated raw into the DSN path (see
// the "my db" case below, which produces "/my db?"), whereas user/password are
// escaped. Postgres and MSSQL escape the database name. Pinned as-is; a fix, if
// wanted, belongs in its own issue.
func TestMySQLConformance(t *testing.T) {
	RunDriverConformance(t, DriverCase{
		Name:    "mysql",
		Aliases: []string{"mariadb", "maria"},

		DriverType:  reflect.TypeOf((*mysql.Driver)(nil)),
		ReaderType:  reflect.TypeOf((*mysql.Reader)(nil)),
		WriterType:  reflect.TypeOf((*mysql.Writer)(nil)),
		DialectType: reflect.TypeOf((*mysql.Dialect)(nil)),

		// Backtick-quoted identifiers; embedded backtick is doubled.
		QuoteInput:       "Col",
		QuoteWant:        "`Col`",
		QuoteEscapeInput: "a`b",
		QuoteEscapeWant:  "`a``b`",

		// MySQL is the one engine with a schema-less special case: an empty
		// schema qualifies to the bare quoted table (database lives in the DSN).
		QualSchema:         "mydb",
		QualTable:          "users",
		QualWant:           "`mydb`.`users`",
		QualSchemalessWant: "`users`",

		// MySQL uses positional '?' placeholders regardless of index.
		PlaceholderIndex: 3,
		PlaceholderWant:  "?",

		// MySQL preserves identifier case: NormalizeIdentifier is pass-through.
		NormalizeInput: "UserID",
		NormalizeWant:  "UserID",

		WantDefaults: driver.DriverDefaults{
			Port:                  3306,
			Schema:                "",
			SSLMode:               "preferred",
			WriteAheadWriters:     2,
			ScaleWritersWithCores: true,
		},

		DSNCases: []DSNCase{
			{
				Desc:     "nil opts -> utf8mb4/tls=preferred/UTC defaults; user+password escaped, database raw",
				Host:     "localhost",
				Port:     3306,
				Database: "my db",
				User:     "u@d",
				Password: "p:w/d",
				Opts:     nil,
				Want:     "u%40d:p%3Aw%2Fd@tcp(localhost:3306)/my db?charset=utf8mb4&interpolateParams=true&loc=UTC&multiStatements=true&parseTime=true&readTimeout=5m&tls=preferred&writeTimeout=5m",
			},
			{
				Desc:     "simple credentials",
				Host:     "h",
				Port:     3306,
				Database: "db",
				User:     "usr",
				Password: "pwd",
				Opts:     nil,
				Want:     "usr:pwd@tcp(h:3306)/db?charset=utf8mb4&interpolateParams=true&loc=UTC&multiStatements=true&parseTime=true&readTimeout=5m&tls=preferred&writeTimeout=5m",
			},
		},
	})
}
