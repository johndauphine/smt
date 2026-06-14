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
		{"mysql tinyint(1) unsigned -> boolean drops sign", fromMySQL, driver.Column{DataType: "tinyint", DisplayWidth: 1, IsUnsigned: true}, "TINYINT(1)"},
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

// #108 — "text" is dialect-ambiguous on mysql targets: MySQL's own TEXT is
// the 64KiB tier and must round-trip verbatim, but pg text (1GB) and legacy
// mssql TEXT (2GB) are unbounded LOBs that silently lose capacity unless
// they land as LONGTEXT. ntext already mapped to LONGTEXT; tiers untouched.
func TestColumnType_TextToMySQLBySourceDialect(t *testing.T) {
	base, _ := NewRenderer("mysql", "crm", "fail")
	cases := []struct {
		name string
		r    Renderer
		dt   string
		want string
	}{
		{"mysql text keeps tier", base.WithSource("mysql"), "text", "TEXT"},
		{"mariadb text keeps tier", base.WithSource("mariadb"), "text", "TEXT"},
		{"pg text upgrades", base.WithSource("postgres"), "text", "LONGTEXT"},
		{"mssql legacy text upgrades", base.WithSource("mssql"), "text", "LONGTEXT"},
		{"unknown source upgrades", base, "text", "LONGTEXT"},
		{"ntext still longtext", base.WithSource("mssql"), "ntext", "LONGTEXT"},
		{"mysql mediumtext untouched", base.WithSource("mysql"), "mediumtext", "MEDIUMTEXT"},
	}
	for _, tc := range cases {
		got, err := tc.r.ColumnType(driver.Column{Name: "c", DataType: tc.dt})
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("%s: ColumnType(%s) = %q, want %q", tc.name, tc.dt, got, tc.want)
		}
	}
}

// #108 (binary/varchar capacity classes) — unbounded and oversized sources
// must land on a tier that holds their capacity; MySQL's own blob tiers
// round-trip verbatim instead of flattening to BLOB.
func TestColumnType_MySQLCapacityTiers(t *testing.T) {
	r, _ := NewRenderer("mysql", "crm", "fail")
	cases := []struct {
		col  driver.Column
		want string
	}{
		{driver.Column{DataType: "image", MaxLength: 2147483647}, "LONGBLOB"},
		{driver.Column{DataType: "blob"}, "BLOB"},
		{driver.Column{DataType: "tinyblob"}, "TINYBLOB"},
		{driver.Column{DataType: "mediumblob"}, "MEDIUMBLOB"},
		{driver.Column{DataType: "longblob"}, "LONGBLOB"},
		{driver.Column{DataType: "varchar", MaxLength: 5000000}, "LONGTEXT"},
		{driver.Column{DataType: "varchar"}, "LONGTEXT"},
	}
	for _, tc := range cases {
		got, err := r.ColumnType(tc.col)
		if err != nil {
			t.Fatalf("ColumnType(%s): %v", tc.col.DataType, err)
		}
		if got != tc.want {
			t.Errorf("ColumnType(%s, len=%d) = %q, want %q", tc.col.DataType, tc.col.MaxLength, got, tc.want)
		}
	}
}
