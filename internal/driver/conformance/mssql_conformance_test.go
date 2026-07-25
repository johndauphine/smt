package conformance

import (
	"reflect"
	"testing"

	"github.com/johndauphine/smt/internal/driver"
	"github.com/johndauphine/smt/internal/driver/mssql"
)

// TestMSSQLConformance pins the SQL Server driver contract.
//
// DatabaseContext static metadata is NOT pinned here for the same reason as
// PostgreSQL: it lives in the unexported mssql.gatherDatabaseContext, reachable
// only through a live *sql.DB, with no pure accessor for the static half.
func TestMSSQLConformance(t *testing.T) {
	RunDriverConformance(t, DriverCase{
		Name:    "mssql",
		Aliases: []string{"sqlserver", "sql-server"},

		DriverType:  reflect.TypeOf((*mssql.Driver)(nil)),
		ReaderType:  reflect.TypeOf((*mssql.Reader)(nil)),
		WriterType:  reflect.TypeOf((*mssql.Writer)(nil)),
		DialectType: reflect.TypeOf((*mssql.Dialect)(nil)),

		// Bracket-quoted identifiers; embedded ] is doubled.
		QuoteInput:       "Col",
		QuoteWant:        "[Col]",
		QuoteEscapeInput: "a]b",
		QuoteEscapeWant:  "[a]]b]",

		// schema.table always bracketed; empty schema emits an empty [] prefix
		// (no schema-less special case).
		QualSchema:         "dbo",
		QualTable:          "users",
		QualWant:           "[dbo].[users]",
		QualSchemalessWant: "[].[users]",

		PlaceholderIndex: 3,
		PlaceholderWant:  "@p3",

		// SQL Server preserves identifier case: NormalizeIdentifier is
		// pass-through.
		NormalizeInput: "UserID",
		NormalizeWant:  "UserID",

		WantDefaults: driver.DriverDefaults{
			Port:                  1433,
			Schema:                "dbo",
			Encrypt:               true,
			PacketSize:            32767,
			WriteAheadWriters:     2,
			ScaleWritersWithCores: true,
		},

		DSNCases: []DSNCase{
			{
				Desc:     "nil opts -> no encrypt flag, connection/dial timeouts appended",
				Host:     "localhost",
				Port:     1433,
				Database: "my db",
				User:     "u@d",
				Password: "p:w/d",
				Opts:     nil,
				Want:     "sqlserver://u%40d:p%3Aw%2Fd@localhost:1433?database=my+db&connection+timeout=30&dial+timeout=15",
			},
			{
				Desc:     "encrypt=true honored",
				Host:     "h",
				Port:     1433,
				Database: "db",
				User:     "usr",
				Password: "pwd",
				Opts:     map[string]any{"encrypt": true},
				Want:     "sqlserver://usr:pwd@h:1433?database=db&encrypt=true&connection+timeout=30&dial+timeout=15",
			},
		},
	})
}
