package mssql

import (
	"errors"
	"fmt"
	"testing"

	mssql "github.com/microsoft/go-mssqldb"
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
		{"2714 already an object", mssql.Error{Number: 2714, Message: "There is already an object named 'foo'"}, true},
		{"1779 PK already", mssql.Error{Number: 1779}, true},
		{"1781 column already constrained", mssql.Error{Number: 1781}, true},
		{"1913 index already", mssql.Error{Number: 1913}, true},
		{"2705 column names duplicate", mssql.Error{Number: 2705}, true},

		// Negatives — adjacent error classes that must NOT trigger.
		{"547 FK violation", mssql.Error{Number: 547}, false},
		{"208 invalid object name", mssql.Error{Number: 208}, false},
		{"229 permission denied", mssql.Error{Number: 229}, false},

		{"wrapped mssql error", fmt.Errorf("exec failed: %w", mssql.Error{Number: 2714}), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isAlreadyExists(tc.err); got != tc.want {
				t.Errorf("isAlreadyExists(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
