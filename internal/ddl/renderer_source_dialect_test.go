package ddl

import (
	"testing"

	"smt/internal/driver"
)

// #101 — mysql→mysql round-trips must preserve TIMESTAMP (UTC-normalized,
// session-tz conversion) and the tinyint(1) boolean convention verbatim.
// Cross-dialect mappings stay unchanged: pg's naive "timestamp" still lands
// as DATETIME, and DisplayWidth without a mysql source is ignored.
func TestColumnType_MySQLSameDialectPassthrough(t *testing.T) {
	base, _ := NewRenderer("mysql", "crm", "fail")
	fromMySQL := base.WithSource("mysql")
	fromMaria := base.WithSource("mariadb")
	fromPG := base.WithSource("postgres")

	cases := []struct {
		name string
		r    Renderer
		col  driver.Column
		want string
	}{
		{"mysql ts fsp0", fromMySQL, driver.Column{DataType: "timestamp", DatetimePrecision: intp(0)}, "TIMESTAMP"},
		{"mysql ts fsp3", fromMySQL, driver.Column{DataType: "timestamp", DatetimePrecision: intp(3)}, "TIMESTAMP(3)"},
		{"mariadb alias ts", fromMaria, driver.Column{DataType: "timestamp", DatetimePrecision: intp(6)}, "TIMESTAMP(6)"},
		{"pg naive ts stays datetime", fromPG, driver.Column{DataType: "timestamp", DatetimePrecision: intp(3)}, "DATETIME(3)"},
		{"unknown source ts stays datetime", base, driver.Column{DataType: "timestamp", DatetimePrecision: intp(3)}, "DATETIME(3)"},
		{"mysql tinyint(1)", fromMySQL, driver.Column{DataType: "tinyint", DisplayWidth: 1}, "TINYINT(1)"},
		{"mysql tinyint(1) unsigned", fromMySQL, driver.Column{DataType: "tinyint", DisplayWidth: 1, IsUnsigned: true}, "TINYINT(1) UNSIGNED"},
		{"mysql tinyint plain", fromMySQL, driver.Column{DataType: "tinyint"}, "TINYINT"},
		{"unknown source ignores width", base, driver.Column{DataType: "tinyint", DisplayWidth: 1}, "TINYINT"},
	}
	for _, tc := range cases {
		got, err := tc.r.ColumnType(tc.col)
		if err != nil {
			t.Fatalf("%s: ColumnType(%+v): %v", tc.name, tc.col, err)
		}
		if got != tc.want {
			t.Errorf("%s: ColumnType = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// #101 — source awareness must not leak into non-mysql targets: a mysql
// TIMESTAMP still maps by datetime class on mssql, and bit/bool sources
// still map to TINYINT(1) on mysql regardless of source dialect.
func TestColumnType_SourceDialectScopedToMySQLTarget(t *testing.T) {
	mssql, _ := NewRenderer("mssql", "dbo", "fail")
	got, err := mssql.WithSource("mysql").ColumnType(driver.Column{DataType: "timestamp", DatetimePrecision: intp(3)})
	if err != nil {
		t.Fatalf("ColumnType: %v", err)
	}
	if got != "DATETIME2(3)" {
		t.Errorf("mssql target ColumnType = %q, want DATETIME2(3)", got)
	}

	mysql, _ := NewRenderer("mysql", "crm", "fail")
	got, err = mysql.WithSource("postgres").ColumnType(driver.Column{DataType: "boolean"})
	if err != nil {
		t.Fatalf("ColumnType: %v", err)
	}
	if got != "TINYINT(1)" {
		t.Errorf("pg boolean ColumnType = %q, want TINYINT(1)", got)
	}
}
