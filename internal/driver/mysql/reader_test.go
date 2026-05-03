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
