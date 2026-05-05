package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestIsAlreadyExists exercises the SQLSTATE-class detection that the create
// paths use to swallow re-run idempotency failures (e.g. when the AI renames
// a constraint identifier in a way the normalizer doesn't predict, the
// pre-exec existence probe misses, and exec then fails with 42710). The
// matrix doubles as a guard against accidentally widening the allowlist.
func TestIsAlreadyExists(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"plain error", errors.New("boom"), false},
		{"42710 duplicate_object", &pgconn.PgError{Code: "42710", Message: "constraint already exists"}, true},
		{"42P07 duplicate_table", &pgconn.PgError{Code: "42P07", Message: "relation already exists"}, true},
		{"42P06 duplicate_schema", &pgconn.PgError{Code: "42P06", Message: "schema already exists"}, true},

		// Negatives — adjacent SQLSTATEs that must NOT trigger the swallow.
		// These are real errors users need to see (referential integrity,
		// permission, type/name resolution).
		{"23503 fk_violation", &pgconn.PgError{Code: "23503"}, false},
		{"42883 undefined_function", &pgconn.PgError{Code: "42883"}, false},
		{"42P01 undefined_table", &pgconn.PgError{Code: "42P01"}, false},
		{"28000 invalid_authorization", &pgconn.PgError{Code: "28000"}, false},

		{"wrapped pg error", fmt.Errorf("exec failed: %w", &pgconn.PgError{Code: "42710"}), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isAlreadyExists(tc.err); got != tc.want {
				t.Errorf("isAlreadyExists(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
