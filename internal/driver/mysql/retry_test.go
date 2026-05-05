package mysql

import (
	"errors"
	"fmt"
	"testing"

	mysqldrv "github.com/go-sql-driver/mysql"
)

// TestIsAlreadyExists exercises the error-number-class detection that the
// create paths use to swallow re-run idempotency failures. See postgres
// equivalent.
func TestIsAlreadyExists(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"plain error", errors.New("boom"), false},
		{"1050 table exists", &mysqldrv.MySQLError{Number: 1050}, true},
		{"1061 duplicate key name", &mysqldrv.MySQLError{Number: 1061}, true},
		{"1826 duplicate FK constraint name", &mysqldrv.MySQLError{Number: 1826}, true},
		{"1068 multiple PK", &mysqldrv.MySQLError{Number: 1068}, true},
		{"1022 duplicate key", &mysqldrv.MySQLError{Number: 1022}, true},

		// Negatives — adjacent error classes that must NOT trigger.
		{"1452 FK violation", &mysqldrv.MySQLError{Number: 1452}, false},
		{"1146 table doesn't exist", &mysqldrv.MySQLError{Number: 1146}, false},
		{"1062 duplicate entry (data)", &mysqldrv.MySQLError{Number: 1062}, false},
		{"1142 access denied", &mysqldrv.MySQLError{Number: 1142}, false},

		{"wrapped mysql error", fmt.Errorf("exec failed: %w", &mysqldrv.MySQLError{Number: 1050}), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isAlreadyExists(tc.err); got != tc.want {
				t.Errorf("isAlreadyExists(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
