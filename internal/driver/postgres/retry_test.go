package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestIsRetryableDDLError covers the parser/binder-class errors we want
// the retry loop to fire on, and the schema-state errors it must NOT fire on.
// See #29 for the full design and the empirical justification (variance
// experiment showed gpt-oss-20b produces correct DDL on retry ~1 in 3 attempts
// for cases where it's deterministically wrong on the first try).
func TestIsRetryableDDLError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		// Parser/binder errors — RETRY
		{"syntax error 42601", &pgconn.PgError{Code: "42601", Message: "syntax error at or near \"AS\""}, true},
		{"undefined type 42704", &pgconn.PgError{Code: "42704", Message: "type \"nvarchar\" does not exist"}, true},
		{"invalid object def 42P17", &pgconn.PgError{Code: "42P17", Message: "generation expression is not immutable"}, true},
		{"invalid table def 42P16", &pgconn.PgError{Code: "42P16", Message: "column \"x\" specified more than once"}, true},
		{"undefined table 42P01", &pgconn.PgError{Code: "42P01"}, true},
		{"feature not supported 0A000", &pgconn.PgError{Code: "0A000"}, true},

		// Schema-state errors — DO NOT retry
		{"table already exists 42P07", &pgconn.PgError{Code: "42P07", Message: "relation \"x\" already exists"}, false},
		{"duplicate object 42710", &pgconn.PgError{Code: "42710"}, false},
		{"FK violation 23503", &pgconn.PgError{Code: "23503"}, false},
		{"permission denied 42501", &pgconn.PgError{Code: "42501"}, false},
		{"connection failure 08006", &pgconn.PgError{Code: "08006"}, false},

		// Wrapping / nil
		{"wrapped retryable", fmt.Errorf("creating table: %w", &pgconn.PgError{Code: "42601"}), true},
		{"wrapped non-retryable", fmt.Errorf("creating table: %w", &pgconn.PgError{Code: "42P07"}), false},
		{"plain error with SQLSTATE 42601 in text", errors.New("ERROR: syntax error (SQLSTATE 42601)"), true},
		{"plain error without SQLSTATE marker", errors.New("some other failure mode"), false},
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
