package ddl

import (
	"strings"
	"testing"

	"smt/internal/driver"
)

// #86 — the CHECK keyword must only be stripped at a word boundary, not off
// the front of identifiers like "checked_in".
func TestCheckConstraint_IdentifierStartingWithCheck(t *testing.T) {
	for _, target := range []string{"mssql", "mysql"} {
		r, err := NewRenderer(target, "crm", "fail")
		if err != nil {
			t.Fatalf("NewRenderer %s: %v", target, err)
		}
		table := &driver.Table{Name: "visits", Columns: []driver.Column{{Name: "checked_in", DataType: "int"}}}
		got, err := r.CreateCheckConstraintDDL(table, &driver.CheckConstraint{Name: "ck_visits", Definition: "(checked_in > 0)"})
		if err != nil {
			t.Fatalf("CreateCheckConstraintDDL %s: %v", target, err)
		}
		want := map[string]string{"mssql": "[checked_in] > 0", "mysql": "`checked_in` > 0"}[target]
		if !strings.Contains(got, want) {
			t.Fatalf("%s check DDL mangled the identifier (want %q): %q", target, want, got)
		}
	}
}

func TestStripOuterCheckParens(t *testing.T) {
	cases := map[string]string{
		"CHECK (x > 0)":    "x > 0",
		"CHECK(x > 0)":     "x > 0",
		"(checked_in > 0)": "checked_in > 0",
		"checked_in > 0":   "checked_in > 0",
		"(x > 0)":          "x > 0",
	}
	for in, want := range cases {
		if got := stripOuterCheckParens(in); got != want {
			t.Errorf("stripOuterCheckParens(%q) = %q, want %q", in, got, want)
		}
	}
}

// #85 — registry aliases accepted by config validation must construct a renderer.
func TestNewRenderer_RegistryAliases(t *testing.T) {
	for alias, want := range map[string]string{
		"sql-server": "mssql",
		"sql_server": "mssql",
		"sqlserver":  "mssql",
		"maria":      "mysql",
		"mariadb":    "mysql",
		"pg":         "postgres",
	} {
		r, err := NewRenderer(alias, "s", "fail")
		if err != nil {
			t.Fatalf("NewRenderer(%q): %v", alias, err)
		}
		if r.Target() != want {
			t.Errorf("NewRenderer(%q).Target() = %q, want %q", alias, r.Target(), want)
		}
	}
}

// #82 — MSSQL-native default functions pass through; foreign now-style
// functions keep their local-vs-UTC class.
func TestColumnDefault_MSSQLPreservesTimeClass(t *testing.T) {
	r, err := NewRenderer("mssql", "dbo", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	cases := map[string]string{
		"(getdate())":          "GETDATE()",
		"(getutcdate())":       "GETUTCDATE()",
		"(sysutcdatetime())":   "SYSUTCDATETIME()",
		"(sysdatetime())":      "SYSDATETIME()",
		"now()":                "SYSDATETIME()",
		"CURRENT_TIMESTAMP":    "SYSDATETIME()",
		"current_timestamp(6)": "SYSDATETIME()",
		"utc_timestamp()":      "SYSUTCDATETIME()",
		"(newid())":            "NEWID()",
	}
	for in, want := range cases {
		got, err := r.ColumnDefault(driver.Column{Name: "c", DataType: "datetime2", DefaultExpression: in})
		if err != nil {
			t.Fatalf("ColumnDefault(%q): %v", in, err)
		}
		if got != want {
			t.Errorf("ColumnDefault(%q) = %q, want %q", in, got, want)
		}
	}
}

// #82 — CURRENT_TIMESTAMP inside expressions is valid T-SQL and must not be
// rewritten to a UTC function.
func TestExpression_MSSQLKeepsCurrentTimestamp(t *testing.T) {
	r, err := NewRenderer("mssql", "dbo", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	got, err := r.Expression("([CreatedAt] <= CURRENT_TIMESTAMP)", nil)
	if err != nil {
		t.Fatalf("Expression: %v", err)
	}
	if strings.Contains(got, "SYSUTCDATETIME") {
		t.Fatalf("CURRENT_TIMESTAMP rewritten to UTC: %q", got)
	}
}

// #80 — MySQL requires parenthesized expression defaults, and BLOB/TEXT/JSON
// columns accept only the expression form.
func TestColumnDefault_MySQLExpressionForm(t *testing.T) {
	r, err := NewRenderer("mysql", "crm", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	cases := []struct {
		col  driver.Column
		want string
	}{
		{driver.Column{Name: "id", DataType: "binary", MaxLength: 16, DefaultExpression: "(uuid_to_bin(uuid()))"}, "(uuid_to_bin(uuid()))"},
		{driver.Column{Name: "doc", DataType: "json", DefaultExpression: `'{"a": 1}'`}, `('{"a": 1}')`},
		{driver.Column{Name: "doc", DataType: "json", DefaultExpression: "'{}'"}, "(JSON_OBJECT())"},
		{driver.Column{Name: "name", DataType: "varchar", MaxLength: 20, DefaultExpression: "'abc'"}, "'abc'"},
		{driver.Column{Name: "qty", DataType: "int", DefaultExpression: "0"}, "0"},
		{driver.Column{Name: "ts", DataType: "datetime", DefaultExpression: "CURRENT_TIMESTAMP"}, "CURRENT_TIMESTAMP(6)"},
		{driver.Column{Name: "note", DataType: "text", DefaultExpression: "'n/a'"}, "('n/a')"},
		// Expressions that merely start with CURRENT_TIMESTAMP still need parens.
		{driver.Column{Name: "due", DataType: "datetime", DefaultExpression: "(CURRENT_TIMESTAMP + INTERVAL '1' DAY)"}, "(CURRENT_TIMESTAMP + INTERVAL '1' DAY)"},
	}
	for _, tc := range cases {
		got, err := r.ColumnDefault(tc.col)
		if err != nil {
			t.Fatalf("ColumnDefault(%q): %v", tc.col.DefaultExpression, err)
		}
		if got != tc.want {
			t.Errorf("ColumnDefault(%s %q) = %q, want %q", tc.col.DataType, tc.col.DefaultExpression, got, tc.want)
		}
	}
}

func TestSetColumnDefaultDDL_MySQLFunctionDefaultUsesExpressionForm(t *testing.T) {
	r, err := NewRenderer("mysql", "crm", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	r = r.WithSource("postgres")

	got, err := r.SetColumnDefaultDDL("events", driver.Column{Name: "created_at", DataType: "datetime", DefaultExpression: "now()"})
	if err != nil {
		t.Fatalf("SetColumnDefaultDDL function: %v", err)
	}
	want := "ALTER TABLE `crm`.`events` ALTER COLUMN `created_at` SET DEFAULT (CURRENT_TIMESTAMP(6))"
	if got != want {
		t.Fatalf("SetColumnDefaultDDL function = %q, want %q", got, want)
	}

	got, err = r.SetColumnDefaultDDL("events", driver.Column{Name: "quantity", DataType: "int", DefaultExpression: "1"})
	if err != nil {
		t.Fatalf("SetColumnDefaultDDL literal: %v", err)
	}
	want = "ALTER TABLE `crm`.`events` ALTER COLUMN `quantity` SET DEFAULT 1"
	if got != want {
		t.Fatalf("SetColumnDefaultDDL literal = %q, want %q", got, want)
	}

	def, _, err := r.ColumnDefinition(driver.Column{Name: "created_at", DataType: "datetime", IsNullable: false, DefaultExpression: "now()"})
	if err != nil {
		t.Fatalf("ColumnDefinition: %v", err)
	}
	if !strings.Contains(def, "DEFAULT CURRENT_TIMESTAMP(6)") || strings.Contains(def, "DEFAULT (CURRENT_TIMESTAMP(6))") {
		t.Fatalf("column definition default form changed unexpectedly: %q", def)
	}
}

func TestColumnDefault_EmptyStringDefaultPresence(t *testing.T) {
	r, err := NewRenderer("mysql", "crm", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	cases := []struct {
		col  driver.Column
		want string
	}{
		{driver.Column{Name: "status", DataType: "varchar", MaxLength: 10, HasDefault: true}, "''"},
		{driver.Column{Name: "status", DataType: "varchar", MaxLength: 10, DefaultExpression: " ", HasDefault: true}, "' '"},
		{driver.Column{Name: "note", DataType: "text", HasDefault: true}, "('')"},
	}
	for _, tc := range cases {
		got, err := r.ColumnDefault(tc.col)
		if err != nil {
			t.Fatalf("ColumnDefault(%#v): %v", tc.col, err)
		}
		if got != tc.want {
			t.Fatalf("ColumnDefault(%#v) = %q, want %q", tc.col, got, tc.want)
		}
	}
}

func TestCreateIndexDDL_MySQLPrefixAndFunctionalParts(t *testing.T) {
	r, err := NewRenderer("mysql", "crm", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	got, err := r.CreateIndexDDL(&driver.Table{Name: "docs"}, &driver.Index{
		Name:                "idx_docs_body_name",
		Columns:             []string{"body", "lower(`name`)"},
		ColumnPrefixLengths: []int{100, 0},
		ColumnExpressions:   []bool{false, true},
	})
	if err != nil {
		t.Fatalf("CreateIndexDDL: %v", err)
	}
	want := "CREATE INDEX `idx_docs_body_name` ON `crm`.`docs` (`body`(100), (lower(`name`)))"
	if got != want {
		t.Fatalf("CreateIndexDDL = %q, want %q", got, want)
	}
}

func TestCreateIndexDDL_MySQLRejectsFilteredIndex(t *testing.T) {
	r, err := NewRenderer("mysql", "crm", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	_, err = r.CreateIndexDDL(&driver.Table{Name: "users"}, &driver.Index{
		Name:    "idx_users_active_email",
		Columns: []string{"email"},
		Filter:  "deleted_at IS NULL",
	})
	if err == nil {
		t.Fatal("CreateIndexDDL succeeded for filtered index on MySQL target")
	}
	if !strings.Contains(err.Error(), "filtered indexes are not supported on mysql target") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// #89 — lengths above the target's maximum degrade to the unbounded form
// instead of DDL the target rejects.
func TestColumnType_LengthClamping(t *testing.T) {
	mssql, err := NewRenderer("mssql", "dbo", "fail")
	if err != nil {
		t.Fatalf("NewRenderer mssql: %v", err)
	}
	mysql, err := NewRenderer("mysql", "crm", "fail")
	if err != nil {
		t.Fatalf("NewRenderer mysql: %v", err)
	}
	cases := []struct {
		r    Renderer
		col  driver.Column
		want string
	}{
		{mssql, driver.Column{DataType: "varchar", MaxLength: 10000}, "VARCHAR(MAX)"},
		{mssql, driver.Column{DataType: "varchar", MaxLength: 8000}, "VARCHAR(8000)"},
		{mssql, driver.Column{DataType: "nvarchar", MaxLength: 5000}, "NVARCHAR(MAX)"},
		{mssql, driver.Column{DataType: "char", MaxLength: 9000}, "VARCHAR(MAX)"},
		{mssql, driver.Column{DataType: "nchar", MaxLength: 5000}, "NVARCHAR(MAX)"},
		{mssql, driver.Column{DataType: "varbinary", MaxLength: 9000}, "VARBINARY(MAX)"},
		{mysql, driver.Column{DataType: "varchar", MaxLength: 20000}, "MEDIUMTEXT"},
		{mysql, driver.Column{DataType: "varchar", MaxLength: 16383}, "VARCHAR(16383)"},
		{mysql, driver.Column{DataType: "char", MaxLength: 300}, "VARCHAR(300)"},
		{mysql, driver.Column{DataType: "varbinary", MaxLength: 70000}, "MEDIUMBLOB"},
		// Unbounded binary sources (pg bytea = 0, mssql varbinary(max) = -1)
		// hold 1-2GB; only LONGBLOB keeps that capacity (#108).
		{mysql, driver.Column{DataType: "bytea", MaxLength: 0}, "LONGBLOB"},
		{mysql, driver.Column{DataType: "varbinary", MaxLength: -1}, "LONGBLOB"},
	}
	for _, tc := range cases {
		got, err := tc.r.ColumnType(tc.col)
		if err != nil {
			t.Fatalf("ColumnType(%s %d): %v", tc.col.DataType, tc.col.MaxLength, err)
		}
		if got != tc.want {
			t.Errorf("ColumnType(%s, %s(%d)) = %q, want %q", tc.r.Target(), tc.col.DataType, tc.col.MaxLength, got, tc.want)
		}
	}
}

// #83 — rowversion (normalized by the mssql reader) maps to the right type
// class on every target.
func TestColumnType_Rowversion(t *testing.T) {
	mssql, _ := NewRenderer("mssql", "dbo", "fail")
	mysql, _ := NewRenderer("mysql", "crm", "fail")
	col := driver.Column{Name: "Version", DataType: "rowversion", MaxLength: 8}
	if got, _ := mssql.ColumnType(col); got != "ROWVERSION" {
		t.Errorf("mssql rowversion = %q, want ROWVERSION", got)
	}
	if got, _ := mysql.ColumnType(col); got != "BINARY(8)" {
		t.Errorf("mysql rowversion = %q, want BINARY(8)", got)
	}
}

// Arithmetic '+' in CHECK constraints must not become CONCAT on MySQL targets.
func TestCheckConstraint_MySQLArithmeticPlusStaysArithmetic(t *testing.T) {
	r, err := NewRenderer("mysql", "crm", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	table := &driver.Table{Name: "orders", Columns: []driver.Column{
		{Name: "total", DataType: "decimal", Precision: 18, Scale: 4},
		{Name: "subtotal", DataType: "decimal", Precision: 18, Scale: 4},
		{Name: "tax", DataType: "decimal", Precision: 18, Scale: 4},
	}}
	got, err := r.CreateCheckConstraintDDL(table, &driver.CheckConstraint{Name: "ck_total", Definition: "(total >= subtotal + tax)"})
	if err != nil {
		t.Fatalf("CreateCheckConstraintDDL: %v", err)
	}
	if strings.Contains(got, "CONCAT") {
		t.Fatalf("arithmetic rewritten to CONCAT: %q", got)
	}
}

// String concatenation still becomes CONCAT on MySQL targets.
func TestComputedColumn_MySQLStringConcatStillRewritten(t *testing.T) {
	r, err := NewRenderer("mysql", "crm", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	got, err := r.Expression("[FirstName] + ' ' + [LastName]", nil)
	if err != nil {
		t.Fatalf("Expression: %v", err)
	}
	if !strings.Contains(got, "CONCAT(") {
		t.Fatalf("string concat not rewritten: %q", got)
	}
}

func TestComputedColumn_MySQLColumnOnlyStringConcatUsesConcat(t *testing.T) {
	r, err := NewRenderer("mysql", "crm", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	r = r.WithSource("mssql")
	tableColumns := []driver.Column{
		{Name: "first_name", DataType: "nvarchar", MaxLength: 50},
		{Name: "last_name", DataType: "nvarchar", MaxLength: 50},
	}

	def, _, err := r.ColumnDefinition(driver.Column{
		Name:               "combo",
		DataType:           "varchar",
		MaxLength:          100,
		IsNullable:         true,
		IsComputed:         true,
		ComputedExpression: "([first_name]+[last_name])",
		ComputedPersisted:  true,
	}, tableColumns)
	if err != nil {
		t.Fatalf("ColumnDefinition: %v", err)
	}
	if !strings.Contains(def, "CONCAT(`first_name`, `last_name`)") {
		t.Fatalf("string computed column did not render CONCAT:\n%s", def)
	}
	if strings.Contains(def, "`first_name` + `last_name`") {
		t.Fatalf("string computed column kept MySQL numeric addition:\n%s", def)
	}
}

func TestComputedColumn_MySQLNumericAdditionStaysArithmetic(t *testing.T) {
	r, err := NewRenderer("mysql", "crm", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	r = r.WithSource("mssql")
	tableColumns := []driver.Column{
		{Name: "subtotal", DataType: "decimal", Precision: 18, Scale: 2},
		{Name: "tax", DataType: "decimal", Precision: 18, Scale: 2},
	}

	def, _, err := r.ColumnDefinition(driver.Column{
		Name:               "total",
		DataType:           "decimal",
		Precision:          18,
		Scale:              2,
		IsNullable:         true,
		IsComputed:         true,
		ComputedExpression: "([subtotal]+[tax])",
		ComputedPersisted:  true,
	}, tableColumns)
	if err != nil {
		t.Fatalf("ColumnDefinition: %v", err)
	}
	if strings.Contains(def, "CONCAT") {
		t.Fatalf("numeric computed column was rewritten to CONCAT:\n%s", def)
	}
	if !strings.Contains(def, "`subtotal`+`tax`") && !strings.Contains(def, "`subtotal` + `tax`") {
		t.Fatalf("numeric computed column did not preserve arithmetic addition:\n%s", def)
	}
}

// #100 — pg's timestamptz udt_name is TZ-aware and must land as
// DATETIMEOFFSET on mssql targets, not naive DATETIME2.
func TestColumnType_TimestamptzKeepsTZClassOnMSSQL(t *testing.T) {
	r, _ := NewRenderer("mssql", "dbo", "fail")
	p := 6
	got, err := r.ColumnType(driver.Column{Name: "created_at", DataType: "timestamptz", DatetimePrecision: &p})
	if err != nil {
		t.Fatalf("ColumnType: %v", err)
	}
	if got != "DATETIMEOFFSET(6)" {
		t.Fatalf("timestamptz = %q, want DATETIMEOFFSET(6)", got)
	}
	spelled, err := r.ColumnType(driver.Column{Name: "created_at", DataType: "timestamp with time zone"})
	if err != nil {
		t.Fatalf("ColumnType: %v", err)
	}
	if spelled != "DATETIMEOFFSET" {
		t.Fatalf("timestamp with time zone = %q, want DATETIMEOFFSET", spelled)
	}
	naive, err := r.ColumnType(driver.Column{Name: "created_at", DataType: "timestamp without time zone"})
	if err != nil {
		t.Fatalf("ColumnType: %v", err)
	}
	if naive != "DATETIME2" {
		t.Fatalf("naive timestamp = %q, want DATETIME2", naive)
	}
}
