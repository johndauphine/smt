package ddl

import (
	"testing"

	"smt/internal/driver"
)

func intp(v int) *int { return &v }

// #88 — fractional-seconds precision renders on every target when known and
// keeps the pre-#88 defaults when unknown.
func TestColumnType_DatetimePrecision(t *testing.T) {
	mssql, _ := NewRenderer("mssql", "dbo", "fail")
	mysql, _ := NewRenderer("mysql", "crm", "fail")
	cases := []struct {
		r    Renderer
		col  driver.Column
		want string
	}{
		{mssql, driver.Column{DataType: "datetime2", DatetimePrecision: intp(3)}, "DATETIME2(3)"},
		{mssql, driver.Column{DataType: "datetime2"}, "DATETIME2"},
		{mssql, driver.Column{DataType: "datetimeoffset", DatetimePrecision: intp(0)}, "DATETIMEOFFSET(0)"},
		{mssql, driver.Column{DataType: "time", DatetimePrecision: intp(3)}, "TIME(3)"},
		{mysql, driver.Column{DataType: "datetime", DatetimePrecision: intp(0)}, "DATETIME"},
		{mysql, driver.Column{DataType: "datetime", DatetimePrecision: intp(3)}, "DATETIME(3)"},
		{mysql, driver.Column{DataType: "datetime"}, "DATETIME(6)"},
		// mssql datetime2(7) clamps to MySQL's max of 6
		{mysql, driver.Column{DataType: "datetime2", DatetimePrecision: intp(7)}, "DATETIME(6)"},
		{mysql, driver.Column{DataType: "time", DatetimePrecision: intp(3)}, "TIME(3)"},
		{mysql, driver.Column{DataType: "time"}, "TIME"},
	}
	for _, tc := range cases {
		got, err := tc.r.ColumnType(tc.col)
		if err != nil {
			t.Fatalf("ColumnType(%+v): %v", tc.col, err)
		}
		if got != tc.want {
			t.Errorf("ColumnType(%s, %s fsp=%v) = %q, want %q", tc.r.Target(), tc.col.DataType, tc.col.DatetimePrecision, got, tc.want)
		}
	}
}

// #88 — MySQL rejects a CURRENT_TIMESTAMP default whose fsp differs from the
// column's, so the rendered default must track the column precision.
func TestColumnDefault_MySQLNowTracksColumnFsp(t *testing.T) {
	r, _ := NewRenderer("mysql", "crm", "fail")
	cases := []struct {
		col  driver.Column
		want string
	}{
		{driver.Column{DataType: "datetime", DatetimePrecision: intp(0), DefaultExpression: "CURRENT_TIMESTAMP"}, "CURRENT_TIMESTAMP"},
		{driver.Column{DataType: "datetime", DatetimePrecision: intp(3), DefaultExpression: "now()"}, "CURRENT_TIMESTAMP(3)"},
		{driver.Column{DataType: "datetime", DefaultExpression: "getdate()"}, "CURRENT_TIMESTAMP(6)"},
	}
	for _, tc := range cases {
		got, err := r.ColumnDefault(tc.col)
		if err != nil {
			t.Fatalf("ColumnDefault: %v", err)
		}
		if got != tc.want {
			t.Errorf("ColumnDefault(fsp=%v) = %q, want %q", tc.col.DatetimePrecision, got, tc.want)
		}
	}
}
