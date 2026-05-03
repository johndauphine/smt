package mssql

import (
	"errors"
	"strings"

	mssql "github.com/microsoft/go-mssqldb"
)

// isRetryableDDLError reports whether a CREATE TABLE / index / FK / CHECK
// failure looks like the AI emitted bad DDL (parser rejected, wrong syntax
// for a computed column, unknown type) and is therefore worth feeding back
// to the AI for another attempt — versus a real schema-state error (object
// already exists, FK target missing, permission denied).
//
// SQL Server surfaces structured error info via mssql.Error.Number. Codes we
// retry on are the parser/binder/planner-rejection family:
//
//	102  — Incorrect syntax near ...                 (canonical parser error)
//	156  — Incorrect syntax near the keyword ...     (parser, keyword variant)
//	170  — Incorrect syntax near ...                 (parser, position variant)
//	195  — '...' is not a recognized built-in        (unknown function)
//	2715 — Column or parameter has invalid type      (e.g. "Cannot find data type")
//	2716 — Column has invalid type                   (related to 2715)
//	1023 — Invalid parameter ...                     (length/precision out of range)
//	1934 — Op failed because SET options ...         (SESSION SET issue, e.g. QUOTED_IDENTIFIER on computed)
//	4933 — PERSISTED requires deterministic expr     (computed column non-determinism, the gpt-oss case)
//	4934 — Cannot persist non-precise expression
//	4936 — Computed column is not allowed to ...     (general computed-column rejections)
//	4938 — Could not enable referenced computed col  (computed-column FK issue)
//	1750 — Could not create constraint or index      (vague but follows real DDL syntax issues)
//
// We do NOT retry on:
//
//	2714 — There is already an object named ...      (state, not syntax)
//	547  — INSERT/UPDATE/DELETE conflict with ref    (data integrity)
//	1785 — May cause cycles or multiple cascade paths (real schema design conflict)
//	229  — Permission denied
//	connection failures — handled at a different layer
//
// The string-fallback path is more permissive than ideal but go-mssqldb
// occasionally wraps the structured error in ways that lose .Number, so we
// also pattern-match on the text MSSQL puts at the front of these errors.
func isRetryableDDLError(err error) bool {
	if err == nil {
		return false
	}
	var mssqlErr mssql.Error
	if errors.As(err, &mssqlErr) {
		switch mssqlErr.Number {
		case 102, // syntax near
			156,  // syntax near keyword
			170,  // syntax near position
			195,  // unknown built-in
			1023, // invalid length/precision parameter
			1934, // SET options for computed columns / persisted indexes
			2715, // unknown type
			2716, // unknown type (column variant)
			4933, // PERSISTED requires deterministic
			4934, // cannot persist non-precise
			4936, // generic computed-column rejection
			4938, // computed col / FK interaction
			1750: // could not create constraint or index (follows on from DDL syntax)
			return true
		}
		return false
	}
	// String fallback — match the exact prefixes MSSQL uses for parser/binder
	// errors that come through wrapped (some go-mssqldb code paths drop the
	// structured Error type).
	msg := err.Error()
	switch {
	case strings.Contains(msg, "Incorrect syntax near"),
		strings.Contains(msg, "Cannot find data type"),
		strings.Contains(msg, "is not a recognized built-in"),
		strings.Contains(msg, "cannot be persisted because the column is non-deterministic"),
		strings.Contains(msg, "computed columns and/or filtered indexes"):
		return true
	}
	return false
}
