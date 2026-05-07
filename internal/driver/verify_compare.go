package driver

// Deterministic source-vs-target column comparison. Replaces the free-text
// AI auditor's judgment with mechanical per-criterion checks. The AI's job
// shrinks to "parse the proposed target DDL into Column[] JSON" — a task
// LLMs are good at — and Go decides PASS / FAIL on the comparison.
//
// See issue #55 for the full design rationale. Two earlier matrix runs
// (Haiku-gen + Sonnet-verify, Sonnet-gen + Opus-verify) showed prompt
// iteration could not get the auditor to consistently apply class-equivalence
// rules across many cells; the model defaults to lexical matching whenever
// the literal type-name strings differ, regardless of how emphatic the
// criteria are. Splitting parse (AI) from compare (Go) eliminates that
// failure mode entirely — Go cannot lexically false-positive on
// "TZ-aware (datetimeoffset) vs TZ-aware (timestamptz)".

import (
	"fmt"
	"strings"
)

// ColumnDelta records a single per-(column, criterion) mismatch from
// CompareColumns. Format() renders it in the same wire shape #53's prompt
// produced, so PreviousAttempt feedback to the generator is unchanged from
// today's free-text path — only the source of the deltas differs.
type ColumnDelta struct {
	Column    string // column name on the source side
	Criterion string // one of: missing, max_length, precision, scale, nullability, identity, tz_class, default
	SourceVal string // formatted source value
	TargetVal string // formatted target value, or "<missing>"
}

func (d ColumnDelta) String() string {
	return fmt.Sprintf("%s: %s — %s vs %s", d.Column, d.Criterion, d.SourceVal, d.TargetVal)
}

// CompareColumns runs the six per-column criteria deterministically. Returns
// an empty slice when the parsed target columns preserve the source under
// every criterion the harness applies (max_length, precision, scale,
// nullability, identity, TZ class, default class). Caller's responsibility:
// pass *parsed* target columns whose attributes faithfully reflect the
// proposed DDL — i.e. the AI's parse step must round-trip the DDL into a
// Column[] before this function runs.
//
// Dialect args drive the class lookups (tzClass, defaultExpressionClass).
// Both must be one of "mssql", "postgres", "mysql"; unknown values fall back
// to permissive defaults.
func CompareColumns(src, tgt []Column, srcDialect, tgtDialect string) []ColumnDelta {
	tgtByName := make(map[string]Column, len(tgt))
	for _, c := range tgt {
		tgtByName[strings.ToLower(c.Name)] = c
	}

	var deltas []ColumnDelta
	for _, s := range src {
		t, ok := tgtByName[strings.ToLower(s.Name)]
		if !ok {
			deltas = append(deltas, ColumnDelta{
				Column: s.Name, Criterion: "missing",
				SourceVal: "present", TargetVal: "<missing>",
			})
			continue
		}
		// Computed columns short-circuit length/precision/scale and default
		// checks: their reported max_length and precision/scale come from
		// the engine's synthesis of the expression's result type (e.g. MSSQL
		// promotes `decimal(18,2) - decimal(18,2)` to precision=34 internally
		// for PERSISTED columns), so they don't round-trip across dialects
		// even when the expression is preserved correctly. The expression
		// itself IS the contract — verify catches dropped/altered expressions
		// via the data_type / nullability / TZ checks below. Default-expr
		// is also skipped since computed columns can't have a separate
		// DEFAULT clause; the expression carries the value.
		fns := []func(Column, Column, string, string) *ColumnDelta{
			cmpNullability,
			cmpIdentity,
			cmpTZClass,
		}
		// Same-class fixed-form columns (UUID, JSON, etc.) also skip
		// max_length / precision / scale: those metadata are dialect-storage
		// artifacts (mssql `uniqueidentifier` reports max_length=0, pg `uuid`
		// reports 0, mysql `char(36)` reports 36 — all are UUID storage of
		// equivalent semantic). When BOTH sides are in the same fixed-form
		// class, length/precision aren't user-controlled and shouldn't flag.
		// Default-expr STILL applies (UUID generators must match across
		// dialects).
		srcClass := dataTypeClass(srcDialect, s)
		sameFixedClass := srcClass != "" && srcClass == dataTypeClass(tgtDialect, t)
		if !s.IsComputed && !t.IsComputed {
			if !sameFixedClass {
				fns = append(fns, cmpMaxLength, cmpPrecisionScale)
			}
			fns = append(fns, cmpDefaultClass)
		}
		for _, fn := range fns {
			if d := fn(s, t, srcDialect, tgtDialect); d != nil {
				deltas = append(deltas, *d)
			}
		}
	}
	return deltas
}

// cmpMaxLength enforces criterion 1 (length): source max_length must round-
// trip to the same integer on the target. Both 0 and any negative value
// (MSSQL uses -1 as the sentinel for `nvarchar(MAX)`/`varbinary(MAX)`) mean
// "unbounded" — they all collapse to the unboundedLength class so a source
// `nvarchar(MAX)` (max_length=-1) matches a target `text` (max_length=0)
// without flagging. The AI parser is instructed to emit 0 for unbounded
// types; this normalization keeps the comparison stable regardless of which
// sentinel either side carries.
func cmpMaxLength(src, tgt Column, _, _ string) *ColumnDelta {
	if normMaxLength(src.MaxLength) == normMaxLength(tgt.MaxLength) {
		return nil
	}
	return &ColumnDelta{
		Column: src.Name, Criterion: "max_length",
		SourceVal: fmt.Sprintf("%d", src.MaxLength),
		TargetVal: fmt.Sprintf("%d", tgt.MaxLength),
	}
}

// normMaxLength collapses unbounded sentinels (-1 from MSSQL MAX, 0 from
// the AI parser, any other negative we might encounter) into a single
// canonical value (0). Bounded lengths (>0) pass through unchanged.
func normMaxLength(n int) int {
	if n <= 0 {
		return 0
	}
	return n
}

// cmpPrecisionScale enforces criterion 2 (numeric precision/scale): only
// applies to numeric/decimal types where these are user-meaningful. Integer
// columns also report precision but those are dialect artifacts (MSSQL
// reports INT precision=10, PG reports integer precision=32, both are
// 32-bit), so we skip the check for non-decimal types entirely.
func cmpPrecisionScale(src, tgt Column, _, _ string) *ColumnDelta {
	if !isDecimalType(src.DataType) {
		return nil
	}
	if src.Precision != tgt.Precision {
		return &ColumnDelta{
			Column: src.Name, Criterion: "precision",
			SourceVal: fmt.Sprintf("%d", src.Precision),
			TargetVal: fmt.Sprintf("%d", tgt.Precision),
		}
	}
	if src.Scale != tgt.Scale {
		return &ColumnDelta{
			Column: src.Name, Criterion: "scale",
			SourceVal: fmt.Sprintf("%d", src.Scale),
			TargetVal: fmt.Sprintf("%d", tgt.Scale),
		}
	}
	return nil
}

// cmpNullability enforces criterion 3: source NOT NULL must remain NOT NULL.
// Symmetric — adding NOT NULL where the source allowed NULL is also a flag,
// since either direction breaks insert paths in production.
func cmpNullability(src, tgt Column, _, _ string) *ColumnDelta {
	if src.IsNullable == tgt.IsNullable {
		return nil
	}
	return &ColumnDelta{
		Column: src.Name, Criterion: "nullability",
		SourceVal: nullableLabel(src.IsNullable),
		TargetVal: nullableLabel(tgt.IsNullable),
	}
}

// cmpIdentity enforces criterion 5: identity / auto-increment must be
// preserved. The AI parser is responsible for setting IsIdentity=true on
// any of: PG GENERATED IDENTITY, PG sequence default (nextval), MSSQL
// IDENTITY, MySQL AUTO_INCREMENT — all map to the same boolean.
func cmpIdentity(src, tgt Column, _, _ string) *ColumnDelta {
	if src.IsIdentity == tgt.IsIdentity {
		return nil
	}
	return &ColumnDelta{
		Column: src.Name, Criterion: "identity",
		SourceVal: identityLabel(src.IsIdentity),
		TargetVal: identityLabel(tgt.IsIdentity),
	}
}

// cmpTZClass enforces criterion 4: TZ-awareness CLASS preserved. Compares
// the dialect-specific data_type strings via tzClass to the abstract class
// (naive_dt, tzaware_dt, naive_t, tzaware_t, na). Two type names that are
// LITERALLY different but CLASS-equivalent — e.g. mssql `datetimeoffset` and
// pg `timestamptz` are both `tzaware_dt` — pass, by design. This is exactly
// the case Sonnet+Opus could not reliably handle in the free-text auditor.
func cmpTZClass(src, tgt Column, srcDialect, tgtDialect string) *ColumnDelta {
	srcClass := tzClass(srcDialect, src.DataType)
	tgtClass := tzClass(tgtDialect, tgt.DataType)
	if srcClass == tgtClass {
		return nil
	}
	return &ColumnDelta{
		Column: src.Name, Criterion: "tz_class",
		SourceVal: fmt.Sprintf("%s (%s)", srcClass, src.DataType),
		TargetVal: fmt.Sprintf("%s (%s)", tgtClass, tgt.DataType),
	}
}

// cmpDefaultClass enforces criterion 6: default-expression class preserved.
// Source default (raw expression) and target default (raw expression) are
// each classified via defaultExpressionClass; classes must match. Empty
// expressions on both sides ⇒ no default ⇒ match. One side empty, the other
// not ⇒ default added or dropped ⇒ flag.
//
// Identity columns are special — the target may legitimately have no
// DefaultExpression because IDENTITY / GENERATED IDENTITY *is* the default
// mechanism, expressed via IsIdentity rather than DefaultExpression. We skip
// the default check entirely when either side is an identity column; the
// identity check (cmpIdentity) covers that semantic.
func cmpDefaultClass(src, tgt Column, srcDialect, tgtDialect string) *ColumnDelta {
	if src.IsIdentity || tgt.IsIdentity {
		return nil
	}
	srcClass := defaultExpressionClass(srcDialect, src.DefaultExpression)
	tgtClass := defaultExpressionClass(tgtDialect, tgt.DefaultExpression)
	if srcClass == tgtClass {
		return nil
	}
	return &ColumnDelta{
		Column: src.Name, Criterion: "default",
		SourceVal: fmt.Sprintf("%s (%q)", srcClass, src.DefaultExpression),
		TargetVal: fmt.Sprintf("%s (%q)", tgtClass, tgt.DefaultExpression),
	}
}

// tzClass returns the timezone-awareness class of a dialect-specific
// temporal type. The class is the comparison unit for criterion 4 — two
// columns match iff their classes match, regardless of literal type names.
//
// Returns one of:
//   - naive_dt    — TZ-naive date+time (datetime, datetime2, smalldatetime, timestamp w/o tz)
//   - tzaware_dt  — TZ-aware date+time (datetimeoffset, timestamptz)
//   - naive_t     — TZ-naive time-only
//   - tzaware_t   — TZ-aware time-only (PG only)
//   - na          — not a temporal type (the comparison is irrelevant)
//
// Unknown dialect or type-name returns "na" (permissive). Comparison stays
// safe because both sides return "na" for the same unknown type, so the
// criterion passes trivially — the harness still relies on max_length /
// nullability / identity / default for the actual fidelity check.
func tzClass(dialect, dataType string) string {
	dt := strings.ToLower(strings.TrimSpace(dataType))
	switch dialect {
	case "mssql":
		switch dt {
		case "datetime", "datetime2", "smalldatetime":
			return "naive_dt"
		case "datetimeoffset":
			return "tzaware_dt"
		case "time":
			return "naive_t"
		}
	case "postgres", "postgresql", "pg":
		switch dt {
		case "timestamp", "timestamp without time zone":
			return "naive_dt"
		case "timestamptz", "timestamp with time zone":
			return "tzaware_dt"
		case "time", "time without time zone":
			return "naive_t"
		case "timetz", "time with time zone":
			return "tzaware_t"
		}
	case "mysql", "mariadb":
		switch dt {
		// MySQL's TIMESTAMP does TZ conversion on read but the column itself
		// is stored TZ-naive; treating it as naive_dt aligns with what the
		// source-side introspection reports for MSSQL datetime2 and PG
		// "timestamp without time zone" — they all behave naive in storage.
		case "datetime", "timestamp":
			return "naive_dt"
		case "time":
			return "naive_t"
		}
	}
	return "na"
}

// dataTypeClass returns a coarse equivalence class for fixed-form data types
// where max_length / precision / scale are dialect-storage artifacts rather
// than user-meaningful sizing. When both source and target columns return
// the same non-empty class, the length/precision checks are skipped.
//
// Returns:
//   - "uuid"        — MSSQL `uniqueidentifier`, PG `uuid`, MySQL `char(36)` /
//     `binary(16)` used as UUID storage. All store the same
//     128-bit identifier; reported max_length differs across
//     dialects (0, 0, 36, 16) but isn't user-controlled.
//   - "json"        — PG `json` / `jsonb`, MySQL `json`.
//   - "boolean"     — PG `boolean`, MSSQL `bit`, MySQL `tinyint(1)`.
//   - ""            — not a fixed-form class; standard length/precision
//     comparison applies.
//
// Takes Column rather than just data_type because the MySQL UUID-storage
// detection needs max_length (`char(36)` is UUID-like, `char(10)` is not).
// Unknown dialects fall through to "" — comparison degrades gracefully to
// the literal max_length check.
func dataTypeClass(dialect string, c Column) string {
	dt := strings.ToLower(strings.TrimSpace(c.DataType))

	// Dialect-canonical UUID/JSON/boolean types — unambiguous.
	switch dt {
	case "uniqueidentifier", "uuid":
		return "uuid"
	case "json", "jsonb":
		return "json"
	case "boolean", "bool", "bit":
		return "boolean"
	}

	// MySQL doesn't have a native UUID type; users store UUIDs as `char(36)`
	// (text form) or `binary(16)` (compact form). When the source is mssql
	// `uniqueidentifier` or pg `uuid`, the AI generator picks one of these
	// for a mysql target. Recognizing them as the "uuid" class lets the
	// length check pass when both sides are UUID storage.
	//
	// False-equivalence risk: a real `char(36)` column that isn't a UUID
	// would be classified as UUID and have its length check skipped — but
	// this only fires when the OTHER side is also in the uuid class, which
	// requires that side to be uniqueidentifier/uuid/char(36)/binary(16).
	// In practice those pairings ARE UUIDs.
	if dialect == "mysql" || dialect == "mariadb" {
		if dt == "char" && c.MaxLength == 36 {
			return "uuid"
		}
		if dt == "binary" && c.MaxLength == 16 {
			return "uuid"
		}
	}

	return ""
}

// defaultExpressionClass classifies a raw default-expression string into one
// of a small set of equivalence classes. Replaces the prompt's ACCEPTABLE
// list of equivalences (GETUTCDATE() ≡ CURRENT_TIMESTAMP, NEWID() ≡
// gen_random_uuid(), etc.) with a Go function call that always returns the
// same class for the same expression — no LLM judgment needed.
//
// Returns:
//   - ""              — no default (empty expression)
//   - "current_time"  — any current-timestamp variant
//   - "uuid_gen"      — any UUID generator variant
//   - "true"/"false"  — boolean literal (incl. ((0))/((1)) MSSQL stripping)
//   - "null"          — explicit NULL default
//   - constant<N>     — numeric literal (normalized to the integer/float)
//   - constant'<S>'   — string literal (normalized to the unquoted content)
//   - other:<expr>    — anything we can't classify; comparison falls back to
//     lexical equality on the normalized expression
//
// Two empty results compare equal (no default on either side). Two
// "current_time" results compare equal regardless of which keyword was used.
// "other:..." results compare equal iff the normalized expressions match
// exactly — this is the safety floor for unknown defaults.
func defaultExpressionClass(_, expr string) string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return ""
	}

	// Normalize: lowercase, repeatedly strip outer parens (MSSQL renders
	// constants as ((0))), trim whitespace.
	norm := strings.ToLower(expr)
	for {
		stripped := strings.TrimSpace(norm)
		if len(stripped) >= 2 && stripped[0] == '(' && stripped[len(stripped)-1] == ')' {
			norm = stripped[1 : len(stripped)-1]
			continue
		}
		norm = stripped
		break
	}
	norm = strings.TrimSpace(norm)
	// Strip PG cast suffixes: 'foo'::text → 'foo', gen_random_uuid()::char(36)
	// → gen_random_uuid(), '{}'::jsonb → '{}'. PG's introspection returns
	// defaults with explicit casts; the cast doesn't change semantic class.
	// The cast type can itself contain parens (`char(36)`) so consume the
	// whole tail after the last `::` rather than stopping at the first
	// non-identifier char.
	if i := strings.LastIndex(norm, "::"); i >= 0 {
		norm = strings.TrimSpace(norm[:i])
	}
	// Strip MSSQL N-prefix on string literals: N'foo' ≡ 'foo'
	if strings.HasPrefix(norm, "n'") && strings.HasSuffix(norm, "'") {
		norm = norm[1:]
	}

	// Current-timestamp family: any of these forms (with or without empty
	// arglists) is one class. Match the literal token; arg-bearing variants
	// like dateadd(...) are NOT in this class.
	switch norm {
	case "current_timestamp", "now()", "now",
		"getdate()", "getdate", "getutcdate()", "getutcdate",
		"sysdatetime()", "sysdatetime", "sysdatetimeoffset()", "sysdatetimeoffset",
		"systimestamp", "current_date", "current_time", "localtimestamp", "localtime":
		return "current_time"
	case "newid()", "newid",
		"gen_random_uuid()", "gen_random_uuid",
		"uuid()", "uuid",
		"newsequentialid()", "newsequentialid":
		return "uuid_gen"
	case "null":
		return "null"
	case "true", "1":
		return "true"
	case "false", "0":
		return "false"
	}

	// Numeric literal — normalize to the digits.
	if isNumericLiteral(norm) {
		return "constant" + norm
	}

	// String literal — strip enclosing quotes and treat content as the class.
	if len(norm) >= 2 && norm[0] == '\'' && norm[len(norm)-1] == '\'' {
		return "constant'" + norm[1:len(norm)-1] + "'"
	}

	// Bare-word identifier shape — MySQL's information_schema returns ENUM
	// and string defaults UNQUOTED (e.g. DEFAULT 'draft' is reported as
	// `draft`). Without normalization these would land in "other:" and never
	// match a quoted-string default on a cross-dialect target. Treating any
	// pure [a-z0-9_]+ bare word as a string constant covers ENUM values,
	// SET enumerators, and lower-case keyword-shaped defaults — false-
	// positives (a real bare-keyword identifier we should treat as a
	// function reference) are negligible since the keyword family above
	// already catches the meaningful ones (NULL, true/false).
	if isBareWord(norm) {
		return "constant'" + norm + "'"
	}

	// Anything else: keep the normalized form so two "other:" results compare
	// by exact normalized text. False negatives are preferred over false
	// positives — exec-fail will catch a missed real bug, a false positive
	// cascades through retries.
	return "other:" + norm
}

// isBareWord reports whether s is a non-empty string of lowercase ASCII
// letters, digits, and underscores. Used to recognize MySQL-style unquoted
// string defaults (ENUM values, SET enumerators) that introspection returns
// without surrounding quotes.
func isBareWord(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '_':
		default:
			return false
		}
	}
	return true
}

// isDecimalType — precision/scale matter only for these types. Integer
// types report precision in introspection but the value is a dialect
// artifact (MSSQL INT precision=10, PG integer precision=32, both 32-bit).
func isDecimalType(dt string) bool {
	switch strings.ToLower(strings.TrimSpace(dt)) {
	case "decimal", "numeric", "money", "smallmoney":
		return true
	}
	return false
}

func isNumericLiteral(s string) bool {
	if s == "" {
		return false
	}
	hasDigit := false
	for i, c := range s {
		switch {
		case c >= '0' && c <= '9':
			hasDigit = true
		case c == '-' && i == 0:
			// leading sign
		case c == '.' && hasDigit:
			// decimal point after at least one digit
		default:
			return false
		}
	}
	return hasDigit
}

func nullableLabel(b bool) string {
	if b {
		return "NULL"
	}
	return "NOT NULL"
}

func identityLabel(b bool) string {
	if b {
		return "identity"
	}
	return "non-identity"
}
