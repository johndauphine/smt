package postgres

import (
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// isRetryableDDLError reports whether a CREATE TABLE / index / FK / CHECK
// failure looks like the AI emitted bad DDL (parser rejected, type unknown,
// generation expression non-immutable, etc.) and is therefore worth feeding
// back to the AI for another attempt — versus a real schema-state error
// (table already exists, FK target missing, permission denied) where retry
// would just produce the same wrong outcome.
//
// PostgreSQL surfaces parser/planner errors via pgconn.PgError.Code (5-char
// SQLSTATE). We retry on:
//
//	42601 — syntax_error                            (malformed DDL)
//	42704 — undefined_object (used for type names)  (e.g. "type \"nvarchar\" does not exist")
//	42P17 — invalid_object_definition               (e.g. generation expression not immutable)
//	42P01 — undefined_table                         (rare in CREATE; appears when an FK references a table the AI assumed)
//	42P16 — invalid_table_definition                (e.g. duplicate columns, conflicting clauses)
//	42P11 — invalid_cursor_definition               (rare but DDL-shape)
//	0A000 — feature_not_supported                   (AI used a syntax PG version doesn't support)
//
// We do NOT retry on:
//
//	42P07 — duplicate_table        (object already exists; retry won't help)
//	42710 — duplicate_object       (constraint name conflict; retry won't help)
//	23xxx — integrity_constraint_violation (real data/state issue)
//	28xxx — invalid_authorization  (permission)
//	08xxx — connection failures    (handled by retryableHTTPDo elsewhere)
//
// If the error isn't a *pgconn.PgError (some other Go-level error), we don't
// retry — that's almost always a connection/transport issue that the
// transport-level retry layer should have already handled.
func isRetryableDDLError(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42601", // syntax_error
			"42704", // undefined_object (type names, sequences, etc.)
			"42P17", // invalid_object_definition (e.g. non-immutable generation expression)
			"42P01", // undefined_table (rare in CREATE; happens with mid-DDL identifier confusion)
			"42P16", // invalid_table_definition (duplicate column, conflicting modifiers)
			"42P11", // invalid_cursor_definition
			"0A000": // feature_not_supported
			return true
		}
		return false
	}
	// Fallback: some PG drivers / wrappers return plain errors with the SQLSTATE
	// embedded in the message. Be conservative — only match the syntax-error
	// substring that's reliably present in pgx's error formatting.
	msg := err.Error()
	return strings.Contains(msg, "SQLSTATE 42601") ||
		strings.Contains(msg, "SQLSTATE 42704") ||
		strings.Contains(msg, "SQLSTATE 42P17")
}
