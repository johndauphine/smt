package mysql

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/johndauphine/smt/internal/driver"
)

// TestParseGeneratedColumnExtra is the regression test for issue #18.
// MySQL 8.0.13+ writes "DEFAULT_GENERATED" into information_schema.COLUMNS.EXTRA
// for any column with an expression default (e.g. "DEFAULT CURRENT_TIMESTAMP"),
// and that string contains the substring "GENERATED". The pre-fix code
// substring-matched "GENERATED" and misclassified every such column as a true
// generated/computed column — wiping its real default in the process and
// breaking every mysql-source migration on the first table with an audit
// timestamp column.
func TestParseGeneratedColumnExtra(t *testing.T) {
	tests := []struct {
		extra         string
		wantComputed  bool
		wantPersisted bool
	}{
		// Real generated columns
		{"VIRTUAL GENERATED", true, false},
		{"STORED GENERATED", true, true},

		// The bug case: expression default, NOT a generated column
		{"DEFAULT_GENERATED", false, false},

		// Expression default combined with auto-update (common on TIMESTAMP)
		{"DEFAULT_GENERATED on update CURRENT_TIMESTAMP", false, false},

		// Other EXTRA values that must not match
		{"", false, false},
		{"auto_increment", false, false},
		{"on update CURRENT_TIMESTAMP", false, false},
		{"INVISIBLE", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.extra, func(t *testing.T) {
			gotComputed, gotPersisted := parseGeneratedColumnExtra(tt.extra)
			if gotComputed != tt.wantComputed || gotPersisted != tt.wantPersisted {
				t.Errorf("parseGeneratedColumnExtra(%q) = (computed=%v, persisted=%v), want (%v, %v)",
					tt.extra, gotComputed, gotPersisted, tt.wantComputed, tt.wantPersisted)
			}
		})
	}
}

func TestParseEnumSetValues(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		want       []string
	}{
		{name: "enum", columnType: "enum('billing','shipping','physical','mailing')", want: []string{"billing", "shipping", "physical", "mailing"}},
		{name: "set", columnType: "set('vip','wholesale')", want: []string{"vip", "wholesale"}},
		{name: "escaped quote", columnType: "enum('owner''s','customer')", want: []string{"owner's", "customer"}},
		{name: "backslash escape", columnType: "enum('a\\'b','c')", want: []string{"a'b", "c"}},
		{name: "comma in value", columnType: "enum('a,b','c')", want: []string{"a,b", "c"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseEnumSetValues(tt.columnType)
			if err != nil {
				t.Fatalf("parseEnumSetValues: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d: %#v", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("value[%d] = %q, want %q; got %#v", i, got[i], tt.want[i], got)
				}
			}
		})
	}
}

func TestParseColumnTypeFlags(t *testing.T) {
	if !isUnsignedColumnType("bigint unsigned") {
		t.Fatal("expected bigint unsigned to be detected")
	}
	if isUnsignedColumnType("tinyint(1)") {
		t.Fatal("did not expect signed tinyint to be unsigned")
	}
}

func TestParseOnUpdateExpression(t *testing.T) {
	tests := []struct {
		extra string
		want  string
	}{
		{"DEFAULT_GENERATED on update CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"},
		{"on update CURRENT_TIMESTAMP(6)", "CURRENT_TIMESTAMP(6)"},
		{"DEFAULT_GENERATED", ""},
	}
	for _, tt := range tests {
		if got := parseOnUpdateExpression(tt.extra); got != tt.want {
			t.Fatalf("parseOnUpdateExpression(%q) = %q, want %q", tt.extra, got, tt.want)
		}
	}
}

func TestApplyMySQLColumnDefaultPreservesEmptyString(t *testing.T) {
	tests := []struct {
		name        string
		defaultVal  sql.NullString
		wantPresent bool
		wantExpr    string
	}{
		{name: "no default", defaultVal: sql.NullString{}, wantPresent: false, wantExpr: ""},
		{name: "empty string default", defaultVal: sql.NullString{String: "", Valid: true}, wantPresent: true, wantExpr: ""},
		{name: "whitespace string default", defaultVal: sql.NullString{String: " ", Valid: true}, wantPresent: true, wantExpr: " "},
		{name: "bare word default", defaultVal: sql.NullString{String: "active", Valid: true}, wantPresent: true, wantExpr: "active"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var col driver.Column
			applyMySQLColumnDefault(&col, tc.defaultVal)
			if col.HasDefault != tc.wantPresent {
				t.Fatalf("HasDefault = %v, want %v", col.HasDefault, tc.wantPresent)
			}
			if col.DefaultExpression != tc.wantExpr {
				t.Fatalf("DefaultExpression = %q, want %q", col.DefaultExpression, tc.wantExpr)
			}
		})
	}
}

func TestAppendMySQLIndexPartCapturesFunctionalAndPrefixParts(t *testing.T) {
	idx := driver.Index{Name: "idx_body_name"}
	if err := appendMySQLIndexPart(&idx, mysqlIndexPart{
		indexName:  "idx_body_name",
		columnName: sql.NullString{String: "body", Valid: true},
		subPart:    sql.NullInt64{Int64: 100, Valid: true},
	}); err != nil {
		t.Fatalf("append prefix part: %v", err)
	}
	if err := appendMySQLIndexPart(&idx, mysqlIndexPart{
		indexName:  "idx_body_name",
		expression: sql.NullString{String: "lower(`name`)", Valid: true},
	}); err != nil {
		t.Fatalf("append expression part: %v", err)
	}

	if got, want := strings.Join(idx.Columns, ","), "body,lower(`name`)"; got != want {
		t.Fatalf("Columns = %q, want %q", got, want)
	}
	if len(idx.ColumnPrefixLengths) != 2 || idx.ColumnPrefixLengths[0] != 100 || idx.ColumnPrefixLengths[1] != 0 {
		t.Fatalf("ColumnPrefixLengths = %#v, want [100 0]", idx.ColumnPrefixLengths)
	}
	if len(idx.ColumnExpressions) != 2 || idx.ColumnExpressions[0] || !idx.ColumnExpressions[1] {
		t.Fatalf("ColumnExpressions = %#v, want [false true]", idx.ColumnExpressions)
	}
}

func TestMySQLIndexQueryAvoidsGroupConcatAndSelectsKeyPartMetadata(t *testing.T) {
	q := mysqlIndexQuery(true)
	for _, want := range []string{"EXPRESSION", "SUB_PART", "ORDER BY INDEX_NAME, SEQ_IN_INDEX"} {
		if !strings.Contains(q, want) {
			t.Fatalf("mysqlIndexQuery missing %q:\n%s", want, q)
		}
	}
	if strings.Contains(q, "GROUP_CONCAT") {
		t.Fatalf("mysqlIndexQuery still uses GROUP_CONCAT:\n%s", q)
	}

	withoutExpression := mysqlIndexQuery(false)
	if !strings.Contains(withoutExpression, "NULL AS EXPRESSION") {
		t.Fatalf("fallback index query should avoid MySQL-only EXPRESSION column:\n%s", withoutExpression)
	}
}

func TestMySQLCheckConstraintQueryScopesMariaDBByTable(t *testing.T) {
	mysqlQuery := mysqlCheckConstraintsQuery(false)
	if strings.Contains(mysqlQuery, "cc.TABLE_NAME = tc.TABLE_NAME") {
		t.Fatalf("MySQL query should not reference CHECK_CONSTRAINTS.TABLE_NAME:\n%s", mysqlQuery)
	}

	mariaQuery := mysqlCheckConstraintsQuery(true)
	if !strings.Contains(mariaQuery, "cc.TABLE_NAME = tc.TABLE_NAME") {
		t.Fatalf("MariaDB query should join CHECK_CONSTRAINTS by table name:\n%s", mariaQuery)
	}
}

// #101 — COLUMN_TYPE "tinyint(1)" is the boolean convention and must survive
// a same-dialect round-trip; every other tinyint declaration is left alone.
func TestIsTinyint1ColumnType(t *testing.T) {
	tests := []struct {
		columnType string
		want       bool
	}{
		{"tinyint(1)", true},
		{"TINYINT(1)", true},
		{"tinyint(1) unsigned", true},
		{"tinyint(1) unsigned zerofill", true},
		{"tinyint", false},
		{"tinyint(4)", false},
		{"tinyint(3) unsigned", false},
		{"smallint(1)", false},
		{"", false},
	}
	for _, tc := range tests {
		if got := isTinyint1ColumnType(tc.columnType); got != tc.want {
			t.Errorf("isTinyint1ColumnType(%q) = %v, want %v", tc.columnType, got, tc.want)
		}
	}
}
