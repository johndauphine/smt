package postgres

import (
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
