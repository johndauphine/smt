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
	"math/big"
	"regexp"
	"strings"

	"smt/internal/canonical"
	exprir "smt/internal/expr"
)

var renderedTypeArgsRE = regexp.MustCompile(`\([^)]*\)`)

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

// CompareColumns runs the per-column criteria deterministically. Returns
// an empty slice when the parsed target columns preserve the source under
// every criterion the harness applies (type, max_length, precision, scale,
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

	// Iteration is source-driven: for each source column we look up the
	// matching target. The inverse — target columns not present in the
	// source — is intentionally NOT flagged. SMT's contract is "the target
	// schema is derived from the source," so an extra target column would
	// only happen if the AI hallucinated, in which case the column likely
	// has wrong attributes that other criteria already catch (or the DDL
	// will fail to exec). Asymmetric on purpose; symmetric checking would
	// flag idempotent re-runs where the target already has columns from a
	// prior partial run.
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
		// Computed columns short-circuit engine-derived metadata checks:
		// reported max_length / precision / scale and nullability come from
		// each engine's synthesis of the expression's result type (e.g. MSSQL
		// promotes `decimal(18,2) - decimal(18,2)` to precision=34 internally
		// for PERSISTED columns, and can infer NOT NULL where MySQL reports
		// nullable). Those values don't round-trip across dialects even when
		// the computed column itself is preserved correctly. We still require
		// computed status, identity, TZ class, enum values, and canonical type
		// shape to match. Default-expr is skipped since computed columns can't
		// have a separate DEFAULT clause; the expression carries the value.
		fns := []func(Column, Column, string, string) *ColumnDelta{
			cmpComputed,
			cmpIdentity,
			cmpTZClass,
			cmpEnumValues,
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
			fns = append(fns, cmpNullability)
			if !sameFixedClass {
				fns = append(fns, cmpMaxLength, cmpPrecisionScale)
			}
			fns = append(fns, cmpDefaultClass)
		}
		var columnDeltas []ColumnDelta
		for _, fn := range fns {
			if d := fn(s, t, srcDialect, tgtDialect); d != nil {
				deltas = append(deltas, *d)
				columnDeltas = append(columnDeltas, *d)
			}
		}
		if !sameFixedClass && !hasTypeShapeDelta(columnDeltas) {
			if d := cmpCanonicalType(s, t, srcDialect, tgtDialect); d != nil {
				deltas = append(deltas, *d)
			}
		}
	}
	return deltas
}

func hasTypeShapeDelta(ds []ColumnDelta) bool {
	for _, d := range ds {
		switch d.Criterion {
		case "max_length", "precision", "scale", "tz_class":
			return true
		}
	}
	return false
}

func cmpCanonicalType(src, tgt Column, srcDialect, tgtDialect string) *ColumnDelta {
	expected, ok := renderedCanonicalType(src, srcDialect, tgtDialect)
	if !ok {
		return nil
	}
	actual, ok := renderedCanonicalType(tgt, tgtDialect, tgtDialect)
	if !ok {
		return nil
	}
	if (src.IsComputed || tgt.IsComputed) && normalizeRenderedTypeBase(expected) == normalizeRenderedTypeBase(actual) {
		return nil
	}
	if normalizeRenderedType(expected) == normalizeRenderedType(actual) {
		return nil
	}
	return &ColumnDelta{
		Column:    src.Name,
		Criterion: "type",
		SourceVal: expected,
		TargetVal: actual,
	}
}

func renderedCanonicalType(col Column, sourceDialect, targetDialect string) (string, bool) {
	ct := canonical.ToCanonical(col.DataType, MetaOf(col), sourceDialect)
	typ, err := canonical.FromCanonical(ct, targetDialect, canonical.RenderOpts{IsIdentity: col.IsIdentity})
	if err != nil {
		return "", false
	}
	return typ, true
}

func normalizeRenderedType(s string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(s))), " ")
}

func normalizeRenderedTypeBase(s string) string {
	return strings.Join(strings.Fields(renderedTypeArgsRE.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), "")), " ")
}

// cmpMaxLength enforces criterion 1 (length): source max_length must round-
// trip to the same integer on the target. Both 0 and any negative value
// (MSSQL uses -1 as the sentinel for `nvarchar(MAX)`/`varbinary(MAX)`) mean
// "unbounded" — they all collapse to the unboundedLength class so a source
// `nvarchar(MAX)` (max_length=-1) matches a target `text` (max_length=0)
// without flagging. The AI parser is instructed to emit 0 for unbounded
// types; this normalization keeps the comparison stable regardless of which
// sentinel either side carries.
func cmpMaxLength(src, tgt Column, srcDialect, tgtDialect string) *ColumnDelta {
	// PG has no sized binary type: every binary-family source necessarily
	// lands as unbounded bytea, so length is not user-controlled there and
	// must not flag (binary(16) → bytea is the only possible mapping).
	if isBinaryFamilyType(src.DataType) && strings.EqualFold(strings.TrimSpace(tgt.DataType), "bytea") {
		return nil
	}
	// A MySQL ENUM/SET maps to an unbounded target type (pg text) on a
	// NON-MySQL target; the enum's reported max_length is the longest member,
	// not a user bound, so length must not flag there. Gated on a non-MySQL
	// target so an ENUM→TEXT change on a MySQL target keeps its length signal
	// (the delta that reveals the enum constraint was lost).
	if isEnumSetType(src.DataType) && !isMySQLDialect(tgtDialect) &&
		lobDataTypes[strings.ToLower(strings.TrimSpace(tgt.DataType))] {
		return nil
	}
	// Same-family ENUM/SET → ENUM/SET: the reported max_length is the longest
	// member — a derived artifact, not a user bound — and it does NOT survive
	// the AI parse (which reports 0), so the old length "value-set proxy"
	// false-positived as e.g. 7 vs 0 (#170). Skip the length check; the member
	// list is now compared faithfully via the rendered ENUM(...)/SET(...) type
	// (cmpCanonicalType), since the parser supplies enum_values. That path
	// catches member add/remove/rename/reorder; it normalizes case and
	// whitespace, so a case-only label change (`ENUM('Draft')` vs `'draft'`)
	// does not flag — accepted deliberately: MySQL ENUM equality is
	// collation-dependent, and a case-sensitive AI-parsed comparison would
	// reintroduce the model-dependent false positives this change removes.
	if isEnumSetType(src.DataType) && isEnumSetType(tgt.DataType) {
		return nil
	}
	// MySQL's LOB tiers ARE user-meaningful capacity choices when both
	// sides speak them: LONGTEXT → TEXT silently rejects values above
	// 64KiB, so a same-family tier change must flag even though both types
	// are "unbounded LOBs" for cross-dialect purposes. Compared by name
	// rank rather than catalog length because the AI parser reports 0 for
	// the target's tier types.
	if sf, sr := mysqlLOBTier(srcDialect, src.DataType); sr > 0 {
		if tf, tr := mysqlLOBTier(tgtDialect, tgt.DataType); tr > 0 {
			if sf == tf && sr != tr {
				return &ColumnDelta{
					Column: src.Name, Criterion: "max_length",
					SourceVal: strings.ToLower(strings.TrimSpace(src.DataType)),
					TargetVal: strings.ToLower(strings.TrimSpace(tgt.DataType)),
				}
			}
			return nil
		}
	}
	if effectiveMaxLength(src) == effectiveMaxLength(tgt) {
		return nil
	}
	return &ColumnDelta{
		Column: src.Name, Criterion: "max_length",
		SourceVal: fmt.Sprintf("%d", src.MaxLength),
		TargetVal: fmt.Sprintf("%d", tgt.MaxLength),
	}
}

// mysqlLOBTier classifies MySQL's sized LOB families by capacity rank:
// tiny=1 (255B), base=2 (64KiB), medium=3 (16MiB), long=4 (4GiB), with the
// family ("text" or "blob") alongside. Returns rank 0 when the type isn't a
// MySQL-tiered LOB or the dialect doesn't encode tiers (mssql `text` is a
// 2GB LOB, not a 64KiB tier — only mysql/mariadb names carry tier meaning).
func mysqlLOBTier(dialect, dataType string) (family string, rank int) {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "mysql", "mariadb":
	default:
		return "", 0
	}
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "tinytext":
		return "text", 1
	case "text":
		return "text", 2
	case "mediumtext":
		return "text", 3
	case "longtext":
		return "text", 4
	case "tinyblob":
		return "blob", 1
	case "blob":
		return "blob", 2
	case "mediumblob":
		return "blob", 3
	case "longblob":
		return "blob", 4
	default:
		return "", 0
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

// lobDataTypes are unbounded LOB types whose reported max_length is a
// storage artifact rather than a user choice: mssql text/image report
// 2147483647 and ntext 1073741823, mysql text/blob tiers report their
// fixed capacity (65535, 16777215, ...), pg text/bytea report NULL → 0.
// None of those numbers round-trip across dialects even when the mapping
// is perfectly faithful, so they all collapse to the unbounded class.
var lobDataTypes = map[string]bool{
	"text": true, "ntext": true, "image": true,
	"tinytext": true, "mediumtext": true, "longtext": true,
	"blob": true, "tinyblob": true, "mediumblob": true, "longblob": true,
	"bytea": true, "xml": true,
}

// effectiveMaxLength is normMaxLength with LOB awareness: LOB types compare
// as unbounded regardless of the capacity number the catalog reports.
func effectiveMaxLength(c Column) int {
	if lobDataTypes[strings.ToLower(strings.TrimSpace(c.DataType))] {
		return 0
	}
	return normMaxLength(c.MaxLength)
}

// isMySQLDialect reports whether the dialect is MySQL/MariaDB.
func isMySQLDialect(dialect string) bool {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "mysql", "mariadb":
		return true
	default:
		return false
	}
}

// isEnumSetType reports whether the type is a MySQL ENUM or SET.
func isEnumSetType(dt string) bool {
	switch strings.ToLower(strings.TrimSpace(dt)) {
	case "enum", "set":
		return true
	default:
		return false
	}
}

// isBinaryFamilyType reports whether the type stores raw bytes (sized or
// unbounded). Used by the bytea exemption in cmpMaxLength.
func isBinaryFamilyType(dt string) bool {
	switch strings.ToLower(strings.TrimSpace(dt)) {
	case "binary", "varbinary", "image", "bytea", "rowversion",
		"blob", "tinyblob", "mediumblob", "longblob":
		return true
	default:
		return false
	}
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

// cmpComputed enforces that generated/computed columns remain computed. The
// expression text and storage class are not a v1 equivalence claim, but a
// computed column becoming a regular writable column is a real contract loss.
func cmpComputed(src, tgt Column, _, _ string) *ColumnDelta {
	if src.IsComputed == tgt.IsComputed {
		return nil
	}
	return &ColumnDelta{
		Column: src.Name, Criterion: "computed",
		SourceVal: fmt.Sprintf("%t", src.IsComputed),
		TargetVal: fmt.Sprintf("%t", tgt.IsComputed),
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
	// MySQL TIMESTAMP is the dialect's only time-zone-aware-capable column: it
	// stores UTC and converts on read, so it faithfully carries a TZ-aware
	// source (pg timestamptz, mssql datetimeoffset — rendered to TIMESTAMP by
	// the canonical mapper, #169) just as well as a naive one. tzClass reports
	// it as naive_dt to keep the MySQL-as-source mapping aligned, so reconcile
	// here: a MySQL TIMESTAMP on either side matches either date+time class.
	// MySQL DATETIME stays strictly naive — a TZ-aware source landing there is
	// the #169 fidelity loss and must still flag.
	if mysqlTimestampMatchesDT(srcDialect, src.DataType, tgtClass) ||
		mysqlTimestampMatchesDT(tgtDialect, tgt.DataType, srcClass) {
		return nil
	}
	return &ColumnDelta{
		Column: src.Name, Criterion: "tz_class",
		SourceVal: fmt.Sprintf("%s (%s)", srcClass, src.DataType),
		TargetVal: fmt.Sprintf("%s (%s)", tgtClass, tgt.DataType),
	}
}

// mysqlTimestampMatchesDT reports whether the column is a MySQL TIMESTAMP (the
// dialect's tz-aware-capable type) being compared against a date+time class it
// can faithfully store — naive_dt or tzaware_dt. Used to reconcile the #169
// mapping (TZ-aware source -> MySQL TIMESTAMP) against tzClass's deliberate
// naive_dt classification of MySQL TIMESTAMP.
func mysqlTimestampMatchesDT(dialect, dataType, otherClass string) bool {
	return isMySQLDialect(dialect) &&
		strings.EqualFold(strings.TrimSpace(dataType), "timestamp") &&
		(otherClass == "naive_dt" || otherClass == "tzaware_dt")
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
	srcClass := defaultExpressionClassForColumn(src.DefaultExpression, src, srcDialect)
	tgtClass := defaultExpressionClassForColumn(tgt.DefaultExpression, tgt, tgtDialect)
	if srcClass == tgtClass || structuredEmptyDefaultEquivalent(srcClass, tgtClass, src, tgt, srcDialect, tgtDialect) {
		return nil
	}
	return &ColumnDelta{
		Column: src.Name, Criterion: "default",
		SourceVal: fmt.Sprintf("%s (%q)", srcClass, src.DefaultExpression),
		TargetVal: fmt.Sprintf("%s (%q)", tgtClass, tgt.DefaultExpression),
	}
}

func defaultExpressionClassForColumn(expr string, col Column, dialect string) string {
	norm := normalizedDefaultLiteral(expr)
	if isDecimalType(col.DataType) {
		if n, ok := normalizedNumericLiteral(norm); ok {
			return "constant" + n
		}
	}
	if dataTypeClass(dialect, col) == "json" && isEmptyJSONObjectDefault(norm) {
		return "empty_json_object"
	}
	if isArrayType(col.DataType) && isEmptyArrayDefault(norm) {
		return "empty_array"
	}
	if dataTypeClass(dialect, col) == "json" && norm == "json_array()" {
		return "empty_array"
	}
	return defaultExpressionClass(expr)
}

func structuredEmptyDefaultEquivalent(srcClass, tgtClass string, src, tgt Column, srcDialect, tgtDialect string) bool {
	if !isMSSQLDialect(tgtDialect) || !isTextStorageType(tgt.DataType) {
		return false
	}
	switch srcClass {
	case "empty_json_object":
		return dataTypeClass(srcDialect, src) == "json" && tgtClass == "constant'{}'"
	case "empty_array":
		return isArrayType(src.DataType) && (tgtClass == "constant'{}'" || tgtClass == "constant'[]'")
	default:
		return false
	}
}

func isMSSQLDialect(dialect string) bool {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "mssql", "sqlserver", "sql_server":
		return true
	default:
		return false
	}
}

func isTextStorageType(dt string) bool {
	switch strings.ToLower(strings.TrimSpace(dt)) {
	case "char", "nchar", "varchar", "nvarchar", "text", "ntext", "xml":
		return true
	default:
		return false
	}
}

func normalizedDefaultLiteral(expr string) string {
	norm := strings.ToLower(strings.TrimSpace(expr))
	for {
		stripped := strings.TrimSpace(norm)
		if len(stripped) >= 2 && stripped[0] == '(' && stripped[len(stripped)-1] == ')' {
			norm = stripped[1 : len(stripped)-1]
			continue
		}
		norm = stripped
		break
	}
	if i := strings.LastIndex(norm, "::"); i >= 0 {
		norm = strings.TrimSpace(norm[:i])
	}
	for len(norm) >= 2 && norm[0] == '(' && norm[len(norm)-1] == ')' {
		norm = strings.TrimSpace(norm[1 : len(norm)-1])
	}
	if strings.HasPrefix(norm, "n'") && strings.HasSuffix(norm, "'") {
		norm = norm[1:]
	}
	return strings.TrimSpace(norm)
}

func normalizedNumericLiteral(norm string) (string, bool) {
	if !isNumericLiteral(norm) {
		return "", false
	}
	rat, ok := new(big.Rat).SetString(norm)
	if !ok {
		return "", false
	}
	if rat.IsInt() {
		return rat.Num().String(), true
	}
	return rat.FloatString(scaleForDecimalLiteral(norm)), true
}

func scaleForDecimalLiteral(s string) int {
	if i := strings.IndexByte(s, '.'); i >= 0 {
		scale := len(s) - i - 1
		for scale > 0 && s[i+scale] == '0' {
			scale--
		}
		return scale
	}
	return 0
}

func isEmptyJSONObjectDefault(norm string) bool {
	return norm == "'{}'" || norm == "json_object()"
}

func isEmptyArrayDefault(norm string) bool {
	return norm == "'{}'" || norm == "'[]'" || norm == "json_array()"
}

func isArrayType(dt string) bool {
	dt = strings.ToLower(strings.TrimSpace(dt))
	if strings.HasSuffix(dt, "[]") || strings.HasPrefix(dt, "_") {
		return true
	}
	return dt == "array"
}

// cmpEnumValues guards same-family ENUM/SET comparison. Length is no longer
// compared for enum/set (cmpMaxLength skips it — the longest-member length is
// a derived artifact the AI parse reports as 0), so the member list is the
// real signal, normally compared via the rendered ENUM(...) type. But that
// render FAILS when the parsed target omits enum_values, which would let a
// dropped/changed member set pass silently. A source enum/set always carries
// its members (from introspection), so a same-family target that parsed
// without them is an incomplete parse — flag it rather than skip.
func cmpEnumValues(src, tgt Column, _, _ string) *ColumnDelta {
	if !isEnumSetType(src.DataType) || !isEnumSetType(tgt.DataType) {
		return nil
	}
	if len(src.EnumValues) > 0 && len(tgt.EnumValues) == 0 {
		return &ColumnDelta{
			Column: src.Name, Criterion: "enum_values",
			SourceVal: fmt.Sprintf("%v", src.EnumValues),
			TargetVal: "<no members parsed>",
		}
	}
	return nil
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
//   - "current_dt"    — function returning current date+time (timestamp class).
//     Includes NOW() / CURRENT_TIMESTAMP / GETDATE() /
//     GETUTCDATE() / SYSDATETIME() / SYSDATETIMEOFFSET() /
//     SYSTIMESTAMP / LOCALTIMESTAMP. TZ-awareness of the
//     function itself is dialect-defined; the column's
//     TZ-awareness is checked separately via cmpTZClass
//     on data_type, so we don't subdivide here.
//   - "current_date"  — function returning today's date only (CURRENT_DATE).
//     MUST NOT be conflated with current_dt.
//   - "current_t"     — function returning current time-only (CURRENT_TIME,
//     LOCALTIME). MUST NOT be conflated with current_dt.
//   - "uuid_gen"      — any UUID generator variant
//   - "true"/"false"  — boolean literal (incl. ((0))/((1)) MSSQL stripping)
//   - "null"          — explicit NULL default
//   - constant<N>     — numeric literal (normalized to the integer/float)
//   - constant'<S>'   — string literal (normalized to the unquoted content)
//   - other:<expr>    — anything we can't classify; comparison falls back to
//     lexical equality on the normalized expression. CAVEAT: the bare-word
//     fallback below catches any pure [a-z0-9_]+ identifier as a string
//     constant — typos like "sysdatime" silently land in constant'sysdatime'
//     instead of "other:sysdatime", which can mask real misclassifications.
//     False-positive risk is low for real-world defaults but nonzero.
//
// Two empty results compare equal (no default on either side). Two same-
// class results compare equal regardless of dialect. Cross-class never
// matches (e.g. current_date ≠ current_dt — that would be a real fidelity
// loss). "other:..." results compare equal iff the normalized expressions
// match exactly — safety floor for unknown defaults.
// DefaultExpressionsEquivalent reports whether two default expressions resolve
// to the same semantic class — the deterministic equivalence check the drift /
// AI-review comparator uses. Since #175 it is structural: both sides parse
// into the expression IR (internal/expr) and compare with expr.Equal, which
// preserves the historical class semantics (GETUTCDATE() ≡ CURRENT_TIMESTAMP,
// NEWID() ≡ gen_random_uuid(), ((0)) ≡ false, CONVERT(date, GETDATE()) ≡
// CURRENT_DATE ≢ CURRENT_TIMESTAMP, casts/N-prefix/fsp-args stripped). A
// false result does not mean the translation is wrong, only that SMT cannot
// mechanically prove it equivalent (it can't equate, say,
// DATEADD(year,1,GETDATE()) with NOW() + INTERVAL '1 year').
func DefaultExpressionsEquivalent(source, target string) bool {
	return exprir.Equal(exprir.ParseDefault(source, ""), exprir.ParseDefault(target, ""))
}

// defaultExpressionClass labels a raw default with its equivalence class via
// the expression IR: "", current_dt, current_date, current_t, uuid_gen,
// null, true/false, constant<N>, constant'<s>', other:<normalized>. Kept as
// the base layer under defaultExpressionClassForColumn, which adds the
// column-type-aware classes (empty_json_object / empty_array / decimal
// normalization) on top.
func defaultExpressionClass(expr string) string {
	return exprir.ClassLabel(exprir.ParseDefault(expr, ""))
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
