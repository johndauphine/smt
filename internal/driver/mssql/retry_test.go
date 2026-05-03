package mssql

import (
	"errors"
	"fmt"
	"testing"

	mssql "github.com/microsoft/go-mssqldb"
)

// TestIsRetryableDDLError covers parser/binder errors (RETRY) vs schema-state
// errors (DO NOT retry) for SQL Server. See #29 for the full design.
func TestIsRetryableDDLError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		// Parser/binder — RETRY
		{"syntax 102", mssql.Error{Number: 102, Message: "Incorrect syntax near 'AS'."}, true},
		{"syntax 156", mssql.Error{Number: 156, Message: "Incorrect syntax near the keyword 'PERSISTED'."}, true},
		{"syntax 170", mssql.Error{Number: 170, Message: "Incorrect syntax near ')'."}, true},
		{"unknown built-in 195", mssql.Error{Number: 195}, true},
		{"unknown type 2715", mssql.Error{Number: 2715, Message: "Cannot find data type nvarchar2."}, true},
		{"unknown type 2716", mssql.Error{Number: 2716}, true},
		{"persisted requires deterministic 4933", mssql.Error{Number: 4933}, true},
		{"computed col rejected 4936", mssql.Error{Number: 4936}, true},
		{"could not create constraint 1750", mssql.Error{Number: 1750}, true},
		{"set options for computed 1934", mssql.Error{Number: 1934}, true},

		// Schema-state — DO NOT retry
		{"object already exists 2714", mssql.Error{Number: 2714}, false},
		{"FK violation 547", mssql.Error{Number: 547}, false},
		{"cascade cycle 1785", mssql.Error{Number: 1785}, false},
		{"permission denied 229", mssql.Error{Number: 229}, false},

		// String-fallback path (when go-mssqldb wraps and loses .Number)
		{"text: incorrect syntax", errors.New("mssql: Incorrect syntax near 'PERSISTED'."), true},
		{"text: cannot find data type", errors.New("mssql: Cannot find data type uniqueidentifier."), true},
		{"text: cannot be persisted non-deterministic", errors.New("Computed column cannot be persisted because the column is non-deterministic."), true},
		{"text: not a recognized built-in", errors.New("'NEWGUID' is not a recognized built-in function name."), true},
		{"text: unrelated error", errors.New("connection reset"), false},

		// Wrapping / nil
		{"wrapped retryable", fmt.Errorf("creating table: %w", mssql.Error{Number: 102}), true},
		{"wrapped non-retryable", fmt.Errorf("creating table: %w", mssql.Error{Number: 2714}), false},
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
