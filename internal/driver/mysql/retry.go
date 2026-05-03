package mysql

import (
	"errors"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// isRetryableDDLError reports whether a CREATE TABLE / index / FK / CHECK
// failure looks like the AI emitted bad DDL (parser rejected, default-clause
// requires parens, JSON column rejected default value) and is therefore
// worth feeding back to the AI for another attempt — versus a real schema-
// state error (table already exists, FK target missing, permission denied).
//
// MySQL surfaces structured error info via mysql.MySQLError.Number. Codes we
// retry on are the parser/optimizer/check-constraint-rejection family:
//
//	1064 — You have an error in your SQL syntax     (canonical parser error)
//	1067 — Invalid default value for ...            (e.g. DATETIME(6) DEFAULT CURRENT_TIMESTAMP missing precision)
//	1101 — BLOB/TEXT/JSON column can't have default (pre-8.0.13 form rejected; needs DEFAULT (expr) form)
//	1054 — Unknown column ...                       (DDL referenced a column it didn't declare)
//	1146 — Table ... doesn't exist                  (rare in CREATE; see PG note about FK-mid-table cases)
//	1170 — BLOB/TEXT used in key without prefix     (DDL chose wrong index strategy for TEXT columns)
//	1235 — This version of MySQL doesn't yet support ... (AI used a syntax not in this MySQL)
//	1291 — Column has duplicated value in ENUM      (ENUM/SET fabrication; addressed mostly by #26 reader fix)
//	1294 — Invalid ON UPDATE clause for ...         (DDL emitted ON UPDATE CURRENT_TIMESTAMP on wrong type)
//	3819 — CHECK constraint violated                (the AI's CHECK couldn't be validated against existing data — but pure-DDL, retry could rephrase)
//	3820 — Check constraint contains disallowed function (e.g. CHECK ... NOW())
//	3754 — Cannot use STORED column ...             (AI generated a STORED column with a forbidden expression)
//
// We do NOT retry on:
//
//	1050 — Table '...' already exists               (state, not syntax)
//	1452 — Cannot add or update child row: FK fails (data integrity, not DDL syntax)
//	1213 — Deadlock found ...                       (transient lock conflict — see #25)
//	1045 — Access denied for user ...               (permission)
//	connection failures — handled elsewhere
//
// String fallback for cases where the wrapper drops the structured type.
func isRetryableDDLError(err error) bool {
	if err == nil {
		return false
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1064, // syntax error
			1067, // invalid default value
			1101, // BLOB/TEXT/JSON cannot have default
			1054, // unknown column referenced in own DDL
			1146, // table doesn't exist (FK target naming)
			1170, // BLOB/TEXT in key without prefix
			1235, // MySQL version doesn't support this syntax
			1291, // ENUM duplicate value
			1294, // invalid ON UPDATE clause
			3754, // STORED column forbidden expression
			3819, // CHECK constraint violated
			3820: // CHECK contains disallowed function
			return true
		}
		return false
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "Error 1064"),
		strings.Contains(msg, "Error 1067"),
		strings.Contains(msg, "Error 1101"),
		strings.Contains(msg, "You have an error in your SQL syntax"),
		strings.Contains(msg, "Invalid default value"),
		strings.Contains(msg, "can't have a default value"),
		strings.Contains(msg, "duplicated value"):
		return true
	}
	return false
}
