package mysql

import "testing"

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
