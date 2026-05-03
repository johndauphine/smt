package mysql

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// TestIsRetryableDDLError covers parser-class errors (RETRY) vs schema-state
// errors (DO NOT retry) for MySQL/MariaDB. See #29 for the full design and #25
// for the case (Error 1213 deadlock) that explicitly stays NON-retryable here
// because it's a transient lock conflict, not bad DDL.
func TestIsRetryableDDLError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		// Parser/binder — RETRY
		{"syntax 1064", &mysql.MySQLError{Number: 1064, Message: "You have an error in your SQL syntax"}, true},
		{"invalid default 1067", &mysql.MySQLError{Number: 1067, Message: "Invalid default value for 'created_at'"}, true},
		{"BLOB/TEXT/JSON default 1101", &mysql.MySQLError{Number: 1101, Message: "BLOB, TEXT, GEOMETRY or JSON column 'settings' can't have a default value"}, true},
		{"unknown column 1054", &mysql.MySQLError{Number: 1054}, true},
		{"FK target table missing 1146", &mysql.MySQLError{Number: 1146}, true},
		{"BLOB/TEXT in key without prefix 1170", &mysql.MySQLError{Number: 1170}, true},
		{"unsupported syntax 1235", &mysql.MySQLError{Number: 1235}, true},
		{"ENUM duplicate value 1291", &mysql.MySQLError{Number: 1291, Message: "Column 'address_type' has duplicated value '' in ENUM"}, true},
		{"invalid ON UPDATE 1294", &mysql.MySQLError{Number: 1294}, true},
		{"check disallowed function 3820", &mysql.MySQLError{Number: 3820}, true},

		// Schema-state — DO NOT retry
		{"table already exists 1050", &mysql.MySQLError{Number: 1050}, false},
		{"FK constraint fails 1452", &mysql.MySQLError{Number: 1452}, false},
		{"DEADLOCK 1213 (issue #25)", &mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"}, false},
		{"access denied 1045", &mysql.MySQLError{Number: 1045}, false},

		// String-fallback path
		{"text: Error 1064", errors.New("Error 1064 (42000): You have an error in your SQL syntax"), true},
		{"text: Invalid default value", errors.New("mysql: Invalid default value for some_col"), true},
		{"text: can't have a default value", errors.New("Column 'metadata' can't have a default value"), true},
		{"text: duplicated value", errors.New("Column 'x' has duplicated value '' in ENUM"), true},
		{"text: unrelated", errors.New("connection lost"), false},

		// Wrapping / nil
		{"wrapped retryable", fmt.Errorf("creating: %w", &mysql.MySQLError{Number: 1064}), true},
		{"wrapped non-retryable", fmt.Errorf("creating: %w", &mysql.MySQLError{Number: 1050}), false},
		{"wrapped deadlock (still NOT retried)", fmt.Errorf("creating: %w", &mysql.MySQLError{Number: 1213}), false},
		{"nil error", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRetryableDDLError(tt.err); got != tt.want {
				t.Errorf("isRetryableDDLError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
