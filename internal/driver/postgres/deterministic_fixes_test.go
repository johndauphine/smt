package postgres

import (
	"strings"
	"testing"

	"smt/internal/driver"
)

// #79 — DEFAULT (NULL) on a textual column must stay SQL NULL, not become the
// string literal 'NULL'.
func TestDeterministicDefaultNullStaysKeyword(t *testing.T) {
	renderer := newDeterministicDDL()
	got, err := renderer.defaultExpression(driver.Column{
		Name:              "Notes",
		DataType:          "varchar",
		MaxLength:         50,
		DefaultExpression: "(NULL)",
	})
	if err != nil {
		t.Fatalf("defaultExpression: %v", err)
	}
	if got != "NULL" {
		t.Fatalf("DEFAULT (NULL) rendered as %q, want NULL", got)
	}
}

// #79 — bare textual defaults still get quoted.
func TestDeterministicDefaultBareWordStillQuoted(t *testing.T) {
	renderer := newDeterministicDDL()
	got, err := renderer.defaultExpression(driver.Column{
		Name:              "Status",
		DataType:          "varchar",
		MaxLength:         20,
		DefaultExpression: "(active)",
	})
	if err != nil {
		t.Fatalf("defaultExpression: %v", err)
	}
	if got != "'active'" {
		t.Fatalf("bare word default rendered as %q, want 'active'", got)
	}
}

func intp(v int) *int { return &v }

// #88 — fsp renders on pg targets when known; bare otherwise.
func TestDeterministicDatetimePrecision(t *testing.T) {
	renderer := newDeterministicDDL()
	cases := []struct {
		col  driver.Column
		want string
	}{
		{driver.Column{Name: "a", DataType: "datetime2", DatetimePrecision: intp(3)}, "timestamp(3) without time zone"},
		{driver.Column{Name: "b", DataType: "datetime2"}, "timestamp without time zone"},
		{driver.Column{Name: "c", DataType: "datetimeoffset", DatetimePrecision: intp(7)}, "timestamp(6) with time zone"},
		{driver.Column{Name: "d", DataType: "time", DatetimePrecision: intp(0)}, "time(0)"},
	}
	for _, tc := range cases {
		got, err := renderer.columnType(tc.col)
		if err != nil {
			t.Fatalf("columnType(%+v): %v", tc.col, err)
		}
		if got != tc.want {
			t.Errorf("columnType(%s fsp=%v) = %q, want %q", tc.col.DataType, tc.col.DatetimePrecision, got, tc.want)
		}
	}
}

// #46 — MySQL blob tiers must land as bytea instead of failing the
// unknown-type policy when the type_smoke fixture migrates mysql→pg.
func TestColumnType_MySQLBlobTiers(t *testing.T) {
	for _, dt := range []string{"blob", "tinyblob", "mediumblob", "longblob"} {
		typ, err := RenderColumnTypeWithPolicy(driver.Column{Name: "b", DataType: dt, MaxLength: 65535}, "fail")
		if err != nil {
			t.Fatalf("RenderColumnTypeWithPolicy(%s): %v", dt, err)
		}
		if typ != "bytea" {
			t.Errorf("RenderColumnTypeWithPolicy(%s) = %q, want bytea", dt, typ)
		}
	}
}

// #71 golden tests caught this: pg_get_expr returns bare expressions for
// single-node predicates (`is_active`), and synthetic sync diffs can carry
// unparenthesized definitions — CHECK requires the outer parens. Sources
// that already parenthesize must not get doubled.
func TestCreateCheckConstraint_EnsuresOuterParens(t *testing.T) {
	tbl := &driver.Table{Name: "users", Columns: []driver.Column{{Name: "is_active", DataType: "bool"}}}
	cases := []struct {
		def  string
		want string
	}{
		{"is_active", `CHECK ("is_active")`},
		{"amount > 0", `CHECK ("amount" > 0)`},
		{"(amount > 0)", `CHECK ("amount" > 0)`},
		{"(a > 0) AND (b > 0)", `CHECK (("a" > 0) AND ("b" > 0))`},
	}
	for _, tc := range cases {
		ddl, err := RenderCreateCheckConstraintDDL(tbl, &driver.CheckConstraint{Name: "ck_x", Definition: tc.def}, "public")
		if err != nil {
			t.Fatalf("render(%q): %v", tc.def, err)
		}
		if !strings.Contains(ddl, tc.want) {
			t.Errorf("render(%q) = %q, want substring %q", tc.def, ddl, tc.want)
		}
	}
}
