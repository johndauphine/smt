package driver

import (
	"strings"
	"testing"
)

// TestTZClass pins the cross-dialect TZ-class lookup. Specifically protects
// the case the free-text auditor failed at in #55: mssql.datetimeoffset and
// pg.timestamptz must both resolve to "tzaware_dt" so they compare equal.
// If either returns a different class, criterion 4 starts emitting false
// positives like the Opus auditor did.
func TestTZClass(t *testing.T) {
	cases := []struct {
		dialect, dataType, want string
	}{
		// Cross-dialect class equivalence — the load-bearing case.
		{"mssql", "datetimeoffset", "tzaware_dt"},
		{"postgres", "timestamptz", "tzaware_dt"},
		{"postgres", "timestamp with time zone", "tzaware_dt"},

		// Naive-dt equivalence (mssql and pg have different literal names).
		{"mssql", "datetime2", "naive_dt"},
		{"mssql", "datetime", "naive_dt"},
		{"mssql", "smalldatetime", "naive_dt"},
		{"postgres", "timestamp", "naive_dt"},
		{"postgres", "timestamp without time zone", "naive_dt"},
		{"mysql", "datetime", "naive_dt"},
		{"mysql", "timestamp", "naive_dt"},

		// Time-only.
		{"mssql", "time", "naive_t"},
		{"postgres", "time", "naive_t"},
		{"postgres", "time without time zone", "naive_t"},
		{"postgres", "timetz", "tzaware_t"},
		{"postgres", "time with time zone", "tzaware_t"},

		// Non-temporal types — comparison is irrelevant; both sides should
		// return "na" so cmpTZClass passes trivially.
		{"mssql", "varchar", "na"},
		{"postgres", "integer", "na"},
		{"mysql", "json", "na"},

		// Case-insensitive on data_type; whitespace-tolerant.
		{"postgres", "TIMESTAMPTZ", "tzaware_dt"},
		{"mssql", "  datetime2  ", "naive_dt"},

		// Unknown dialect falls back to "na" — permissive default.
		{"oracle", "TIMESTAMP WITH LOCAL TIME ZONE", "na"},
	}
	for _, tc := range cases {
		got := tzClass(tc.dialect, tc.dataType)
		if got != tc.want {
			t.Errorf("tzClass(%q, %q) = %q, want %q", tc.dialect, tc.dataType, got, tc.want)
		}
	}
}

// TestDefaultExpressionClass pins the equivalence-class function that
// replaces the prompt's ACCEPTABLE list. Two source/target defaults that the
// old prompt called equivalent must produce identical class strings here, or
// the deterministic comparison would emit false positives that the
// AI-auditor era rules called PASS.
func TestDefaultExpressionClass(t *testing.T) {
	cases := []struct {
		name, expr, want string
	}{
		// Empty defaults — no default on either side ⇒ equal.
		{"empty", "", ""},
		{"whitespace only", "   ", ""},

		// current_dt — full date+time. Every "now"-style function returning
		// a timestamp value lands here regardless of TZ-awareness of the
		// function (the column's TZ is checked separately via cmpTZClass).
		{"GETUTCDATE", "GETUTCDATE()", "current_dt"},
		{"GETDATE", "GETDATE()", "current_dt"},
		{"SYSDATETIME", "SYSDATETIME()", "current_dt"},
		{"SYSDATETIMEOFFSET", "(SYSDATETIMEOFFSET())", "current_dt"},
		{"SYSTIMESTAMP", "SYSTIMESTAMP", "current_dt"},
		{"CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP", "current_dt"},
		{"LOCALTIMESTAMP", "LOCALTIMESTAMP", "current_dt"},
		{"now()", "now()", "current_dt"},
		{"NOW()", "NOW()", "current_dt"},

		// current_date — date-only. MUST be a separate class so a source
		// `DEFAULT CURRENT_DATE` doesn't silently match a target `DEFAULT
		// CURRENT_TIMESTAMP` (real fidelity loss the pre-#55-review
		// monolithic class hid). See the negative pins in
		// TestDefaultExpressionClass_DistinctNowFamilies for the proof.
		{"CURRENT_DATE", "CURRENT_DATE", "current_date"},
		{"current_date()", "current_date()", "current_date"},

		// current_t — time-only.
		{"CURRENT_TIME", "CURRENT_TIME", "current_t"},
		{"LOCALTIME", "LOCALTIME", "current_t"},
		{"LOCALTIME()", "LOCALTIME()", "current_t"},

		// uuid generator family.
		{"NEWID", "(NEWID())", "uuid_gen"},
		{"gen_random_uuid", "gen_random_uuid()", "uuid_gen"},
		{"UUID()", "UUID()", "uuid_gen"},
		{"NEWSEQUENTIALID", "newsequentialid()", "uuid_gen"},

		// Boolean / bit literal class — MSSQL ((0)) ≡ pg false, ((1)) ≡ pg true.
		{"MSSQL ((0))", "((0))", "false"},
		{"MSSQL ((1))", "((1))", "true"},
		{"PG false", "false", "false"},
		{"PG true", "true", "true"},
		{"bare 0", "0", "false"},
		{"bare 1", "1", "true"},

		// Numeric constants beyond 0/1 — pinned by exact value.
		{"constant 42", "42", "constant42"},
		{"constant -1", "-1", "constant-1"},
		{"constant 3.14", "3.14", "constant3.14"},

		// String literals — MSSQL N-prefix and outer parens stripped.
		{"MSSQL (('pending'))", "(('pending'))", "constant'pending'"},
		{"MSSQL N'foo'", "N'foo'", "constant'foo'"},
		{"PG 'pending'", "'pending'", "constant'pending'"},

		// NULL default.
		{"NULL", "NULL", "null"},
		{"null lowercase", "null", "null"},

		// PG cast-suffix stripping — `'foo'::text`, `'{}'::jsonb`,
		// `gen_random_uuid()::char(36)` all classify as if the cast weren't
		// there. PG's introspection emits explicit casts on most defaults;
		// the cast doesn't change semantic class. Without this rule, every
		// PG default is "other:..." and never matches a non-PG counterpart.
		{"PG text cast", "'pending'::text", "constant'pending'"},
		{"PG jsonb empty", "'{}'::jsonb", "constant'{}'"},
		{"PG uuid_gen with cast", "gen_random_uuid()::char(36)", "uuid_gen"},
		{"PG numeric cast", "0::integer", "false"},

		// MySQL bare-word defaults — information_schema returns ENUM and
		// string defaults UNQUOTED. The bare-word rule normalizes them to
		// the same class as a quoted-string default on the other side.
		{"MySQL bare ENUM value", "draft", "constant'draft'"},
		{"MySQL bare SET enumerator", "billing", "constant'billing'"},
		{"MySQL bare snake_case", "full_time", "constant'full_time'"},

		// Unknown expression falls into "other:" with normalized text. Two
		// matching unknowns are equal under this scheme — that's the safety
		// floor for defaults we haven't classified.
		{"unknown function", "FOO(bar, 1)", "other:foo(bar, 1)"},
		{"normalized parens", "((CONVERT(int, 42)))", "other:convert(int, 42)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := defaultExpressionClass(tc.expr)
			if got != tc.want {
				t.Errorf("defaultExpressionClass(%q) = %q, want %q",
					tc.expr, got, tc.want)
			}
		})
	}
}

// TestDefaultExpressionClass_DistinctNowFamilies pins the negative side of
// the now-family split: CURRENT_DATE, CURRENT_TIMESTAMP, and CURRENT_TIME
// MUST NOT classify equally. The pre-#55-review monolithic class lumped
// them all into "current_time", which would silently allow a source
// `DEFAULT CURRENT_DATE` to translate to a target `DEFAULT
// CURRENT_TIMESTAMP` — losing the date-only semantics. Each pair below
// asserts the classes diverge.
func TestDefaultExpressionClass_DistinctNowFamilies(t *testing.T) {
	pairs := []struct {
		a, b string
	}{
		{"CURRENT_DATE", "CURRENT_TIMESTAMP"},
		{"CURRENT_DATE", "GETDATE()"},
		{"CURRENT_DATE", "now()"},
		{"CURRENT_TIME", "CURRENT_TIMESTAMP"},
		{"CURRENT_TIME", "LOCALTIMESTAMP"},
		{"CURRENT_TIME", "CURRENT_DATE"},
	}
	for _, p := range pairs {
		t.Run(p.a+"_vs_"+p.b, func(t *testing.T) {
			ac := defaultExpressionClass(p.a)
			bc := defaultExpressionClass(p.b)
			if ac == bc {
				t.Errorf("expected distinct classes, both produced %q (a=%q, b=%q)", ac, p.a, p.b)
			}
		})
	}
}

// TestDefaultExpressionClass_CrossDialectEquivalence pins the pairs the old
// AI-prompt's ACCEPTABLE list called equivalent. Each pair MUST produce the
// same class string — that's the load-bearing property that makes the
// criterion 6 check pass on real cross-dialect translations.
func TestDefaultExpressionClass_CrossDialectEquivalence(t *testing.T) {
	pairs := []struct {
		name, src, tgt string
	}{
		{"GETUTCDATE ≡ CURRENT_TIMESTAMP", "(getutcdate())", "CURRENT_TIMESTAMP"},
		{"GETDATE ≡ NOW()", "(getdate())", "NOW()"},
		{"SYSDATETIMEOFFSET ≡ now()", "(sysdatetimeoffset())", "now()"},
		{"NEWID ≡ gen_random_uuid", "(newid())", "gen_random_uuid()"},
		{"NEWID ≡ UUID()", "(newid())", "UUID()"},
		{"((0)) ≡ false", "((0))", "false"},
		{"((1)) ≡ true", "((1))", "true"},
		{"(('pending')) ≡ 'pending'", "(('pending'))", "'pending'"},

		// MySQL bare-word ENUM default ≡ PG quoted string ≡ MSSQL N-quoted.
		{"mysql draft ≡ pg 'draft'", "draft", "'draft'"},
		{"mysql full_time ≡ pg 'full_time'::text", "full_time", "'full_time'::text"},
		{"mysql usd ≡ mssql N'USD'", "usd", "N'USD'"},

		// PG cast wrappings ≡ unwrapped equivalent on other dialects.
		{"pg '{}'::jsonb ≡ mssql '{}' ", "'{}'::jsonb", "'{}'"},
		{"pg gen_random_uuid()::char(36) ≡ mysql UUID()", "gen_random_uuid()::char(36)", "UUID()"},
	}
	for _, p := range pairs {
		t.Run(p.name, func(t *testing.T) {
			srcClass := defaultExpressionClass(p.src)
			tgtClass := defaultExpressionClass(p.tgt)
			if srcClass != tgtClass {
				t.Errorf("class mismatch: %s → %q vs %s → %q", p.src, srcClass, p.tgt, tgtClass)
			}
		})
	}
}

// TestCompareColumns_AllPass is the happy path — well-translated mssql→pg
// columns produce zero deltas. Specifically exercises the cases the
// AI-auditor era kept getting wrong: nvarchar→varchar, datetime2→TIMESTAMP,
// datetimeoffset→TIMESTAMPTZ, GETUTCDATE→CURRENT_TIMESTAMP.
func TestCompareColumns_AllPass(t *testing.T) {
	src := []Column{
		{Name: "id", DataType: "int", IsNullable: false, IsIdentity: true},
		{Name: "code", DataType: "nvarchar", MaxLength: 20, IsNullable: false},
		{Name: "amount", DataType: "decimal", Precision: 18, Scale: 4, IsNullable: false},
		{Name: "created_at", DataType: "datetime2", IsNullable: false, DefaultExpression: "(getutcdate())"},
		{Name: "sent_at", DataType: "datetimeoffset", IsNullable: true, DefaultExpression: "(sysdatetimeoffset())"},
		{Name: "is_active", DataType: "bit", IsNullable: false, DefaultExpression: "((1))"},
	}
	tgt := []Column{
		{Name: "id", DataType: "integer", IsNullable: false, IsIdentity: true},
		{Name: "code", DataType: "varchar", MaxLength: 20, IsNullable: false},
		{Name: "amount", DataType: "numeric", Precision: 18, Scale: 4, IsNullable: false},
		{Name: "created_at", DataType: "timestamp", IsNullable: false, DefaultExpression: "CURRENT_TIMESTAMP"},
		{Name: "sent_at", DataType: "timestamptz", IsNullable: true, DefaultExpression: "now()"},
		{Name: "is_active", DataType: "boolean", IsNullable: false, DefaultExpression: "true"},
	}
	deltas := CompareColumns(src, tgt, "mssql", "postgres")
	if len(deltas) != 0 {
		var msg strings.Builder
		for _, d := range deltas {
			msg.WriteString("\n  ")
			msg.WriteString(d.String())
		}
		t.Errorf("expected zero deltas, got %d:%s", len(deltas), msg.String())
	}
}

// TestCompareColumns_UnboundedLength pins the MSSQL-MAX sentinel
// equivalence. MSSQL's INFORMATION_SCHEMA reports max_length=-1 for
// `nvarchar(MAX)` / `varbinary(MAX)` / etc; PG and the AI parser report 0
// for unbounded text types. Both must compare equal — the matrix run that
// motivated this fix exhausted retries on `notes: max_length — -1 vs 0`
// false positives that the comparator should never have emitted.
func TestCompareColumns_UnboundedLength(t *testing.T) {
	cases := []struct {
		name string
		src  Column
		tgt  Column
	}{
		{"MSSQL -1 vs PG text 0", Column{Name: "notes", DataType: "nvarchar", MaxLength: -1}, Column{Name: "notes", DataType: "text", MaxLength: 0}},
		{"PG text 0 vs MSSQL -1", Column{Name: "notes", DataType: "text", MaxLength: 0}, Column{Name: "notes", DataType: "nvarchar", MaxLength: -1}},
		{"MSSQL -1 vs MSSQL -1", Column{Name: "notes", DataType: "nvarchar", MaxLength: -1}, Column{Name: "notes", DataType: "nvarchar", MaxLength: -1}},
		{"PG 0 vs PG 0", Column{Name: "notes", DataType: "text", MaxLength: 0}, Column{Name: "notes", DataType: "text", MaxLength: 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deltas := CompareColumns([]Column{tc.src}, []Column{tc.tgt}, "mssql", "postgres")
			if len(deltas) != 0 {
				t.Errorf("expected zero deltas for unbounded equivalence, got %v", deltas)
			}
		})
	}
}

// TestCompareColumns_BoundedVsUnbounded is the negative case of the
// unbounded-equivalence rule: a bounded source (max_length=200) translated
// to an unbounded target (max_length=0) IS a real fidelity loss and MUST
// flag. Otherwise we'd silently swallow nvarchar(200) → TEXT regressions
// (one of the matrix run's real Haiku-gen bugs).
func TestCompareColumns_BoundedVsUnbounded(t *testing.T) {
	src := []Column{{Name: "name", DataType: "nvarchar", MaxLength: 200}}
	tgt := []Column{{Name: "name", DataType: "text", MaxLength: 0}}
	deltas := CompareColumns(src, tgt, "mssql", "postgres")
	if len(deltas) != 1 {
		t.Fatalf("expected 1 delta for bounded→unbounded, got %d: %v", len(deltas), deltas)
	}
	if deltas[0].Criterion != "max_length" {
		t.Errorf("expected criterion=max_length, got %q", deltas[0].Criterion)
	}
}

// TestCompareColumns_HalvedVarchar pins the canonical "model halved a
// varchar" failure that PR #45 chased. Ensures the deterministic check
// catches it where the AI auditor used to false-negative on slow models.
func TestCompareColumns_HalvedVarchar(t *testing.T) {
	src := []Column{{Name: "code", DataType: "nvarchar", MaxLength: 20, IsNullable: false}}
	tgt := []Column{{Name: "code", DataType: "varchar", MaxLength: 10, IsNullable: false}}
	deltas := CompareColumns(src, tgt, "mssql", "postgres")
	if len(deltas) != 1 {
		t.Fatalf("expected 1 delta for halved varchar, got %d: %v", len(deltas), deltas)
	}
	if deltas[0].Criterion != "max_length" {
		t.Errorf("expected criterion=max_length, got %q", deltas[0].Criterion)
	}
	if !strings.Contains(deltas[0].String(), "20 vs 10") {
		t.Errorf("delta string missing 20 vs 10: %q", deltas[0].String())
	}
}

// TestCompareColumns_TZClassFlip is the regression guard for the failure
// mode that closed #53: mssql datetime2 (TZ-naive) silently mapped to PG
// timestamptz (TZ-aware) is a real fidelity loss. The AI auditor caught it
// SOMETIMES in the matrix runs; the deterministic check catches it always.
func TestCompareColumns_TZClassFlip(t *testing.T) {
	src := []Column{{Name: "created_at", DataType: "datetime2", IsNullable: false}}
	tgt := []Column{{Name: "created_at", DataType: "timestamptz", IsNullable: false}}
	deltas := CompareColumns(src, tgt, "mssql", "postgres")
	if len(deltas) != 1 {
		t.Fatalf("expected 1 delta for naive→aware flip, got %d: %v", len(deltas), deltas)
	}
	if deltas[0].Criterion != "tz_class" {
		t.Errorf("expected criterion=tz_class, got %q", deltas[0].Criterion)
	}
}

// TestCompareColumns_TZClassEquivalent is the false-positive guard:
// datetimeoffset → timestamptz should NOT flag, because both are tzaware_dt.
// This is the literal case Opus emitted under ISSUES in the matrix run that
// motivated #55. The deterministic check returns zero deltas where the AI
// auditor returned a false-positive ISSUES line.
func TestCompareColumns_TZClassEquivalent(t *testing.T) {
	src := []Column{{Name: "sent_at", DataType: "datetimeoffset", IsNullable: true}}
	tgt := []Column{{Name: "sent_at", DataType: "timestamptz", IsNullable: true}}
	deltas := CompareColumns(src, tgt, "mssql", "postgres")
	if len(deltas) != 0 {
		t.Errorf("expected zero deltas for class-equivalent TZ-aware types, got %d: %v", len(deltas), deltas)
	}
}

// TestCompareColumns_MissingColumn covers the case where the AI parser
// returns target Column[] that's missing a source column entirely (e.g. the
// generator dropped the column from the DDL). This must surface as a
// "missing" delta so the writer retries with corrective feedback.
func TestCompareColumns_MissingColumn(t *testing.T) {
	src := []Column{
		{Name: "id", DataType: "int"},
		{Name: "code", DataType: "varchar", MaxLength: 20},
	}
	tgt := []Column{{Name: "id", DataType: "integer"}}
	deltas := CompareColumns(src, tgt, "mssql", "postgres")
	if len(deltas) != 1 || deltas[0].Criterion != "missing" {
		t.Errorf("expected one missing-column delta, got %v", deltas)
	}
	if deltas[0].Column != "code" {
		t.Errorf("expected delta on column code, got %q", deltas[0].Column)
	}
}

// TestCompareColumns_UUIDClassEquivalence pins the cross-dialect UUID
// storage equivalence. The matrix run that motivated this fix exhausted
// retries on `external_id: max_length — 0 vs 36`: source is mssql
// `uniqueidentifier` (max_length=0 in introspection), target is mysql
// `char(36)` (max_length=36). Both are UUIDs; the reported lengths differ
// only because of dialect storage idioms. Same logic for pg `uuid` ↔ mysql
// `binary(16)`.
func TestCompareColumns_UUIDClassEquivalence(t *testing.T) {
	cases := []struct {
		name         string
		src          Column
		tgt          Column
		srcD, tgtD   string
		expectDeltas int
	}{
		{"mssql uniqueidentifier ↔ mysql char(36)",
			Column{Name: "id", DataType: "uniqueidentifier", MaxLength: 0},
			Column{Name: "id", DataType: "char", MaxLength: 36},
			"mssql", "mysql", 0},
		{"pg uuid ↔ mysql binary(16)",
			Column{Name: "id", DataType: "uuid", MaxLength: 0},
			Column{Name: "id", DataType: "binary", MaxLength: 16},
			"postgres", "mysql", 0},
		{"mssql uniqueidentifier ↔ pg uuid",
			Column{Name: "id", DataType: "uniqueidentifier", MaxLength: 0},
			Column{Name: "id", DataType: "uuid", MaxLength: 0},
			"mssql", "postgres", 0},
		{"mysql char(36) ↔ pg uuid (reverse direction)",
			Column{Name: "id", DataType: "char", MaxLength: 36},
			Column{Name: "id", DataType: "uuid", MaxLength: 0},
			"mysql", "postgres", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deltas := CompareColumns([]Column{tc.src}, []Column{tc.tgt}, tc.srcD, tc.tgtD)
			if len(deltas) != tc.expectDeltas {
				t.Errorf("got %d deltas, want %d: %v", len(deltas), tc.expectDeltas, deltas)
			}
		})
	}
}

// TestCompareColumns_NonUUIDChar_StillFlagsLength is the negative case for
// the UUID-class rule: a regular `char(10)` column that isn't UUID storage
// should still flag length mismatches. The rule only fires when both sides
// are in the uuid class.
func TestCompareColumns_NonUUIDChar_StillFlagsLength(t *testing.T) {
	src := []Column{{Name: "code", DataType: "char", MaxLength: 10}}
	tgt := []Column{{Name: "code", DataType: "char", MaxLength: 5}}
	deltas := CompareColumns(src, tgt, "mysql", "mysql")
	if len(deltas) != 1 || deltas[0].Criterion != "max_length" {
		t.Errorf("expected single max_length delta on non-UUID char, got %v", deltas)
	}
}

// TestCompareColumns_ComputedSkipsLengthPrecisionDefault pins the rule that
// computed columns aren't subjected to max_length / precision / scale /
// default checks. The matrix run that motivated this fix exhausted retries
// on `OrderItems.line_total: precision — 34 vs 0` — line_total is
// `AS (quantity * unit_price - discount) PERSISTED`, and MSSQL synthesizes
// precision=34 by promoting decimal(18,2) arithmetic. The target
// (also computed) reports a different synthesized precision. Real
// metadata mismatch, but the expression IS the contract — neither
// precision is user-specified, so neither one matters for fidelity.
func TestCompareColumns_ComputedSkipsLengthPrecisionDefault(t *testing.T) {
	src := []Column{{
		Name: "line_total", DataType: "decimal",
		IsComputed: true, ComputedExpression: "(quantity*unit_price-discount)", ComputedPersisted: true,
		Precision: 34, Scale: 2,
	}}
	tgt := []Column{{
		Name: "line_total", DataType: "numeric",
		IsComputed: true, ComputedExpression: "(quantity * unit_price - discount)", ComputedPersisted: true,
		// Target dialect synthesizes a different precision for the same
		// expression — that is the whole motivation for the skip rule.
		Precision: 0, Scale: 0,
	}}
	deltas := CompareColumns(src, tgt, "mssql", "postgres")
	if len(deltas) != 0 {
		t.Errorf("expected zero deltas for computed-column precision divergence, got %v", deltas)
	}
}

// TestCompareColumns_ComputedStillChecksNullability is the negative case of
// the computed-column skip: nullability still matters even on computed
// columns (MSSQL PERSISTED-implicit-NOT-NULL is real fidelity), so we don't
// silently swallow nullable-vs-NOT-NULL mismatches.
func TestCompareColumns_ComputedStillChecksNullability(t *testing.T) {
	src := []Column{{Name: "line_total", DataType: "decimal", IsComputed: true, IsNullable: false}}
	tgt := []Column{{Name: "line_total", DataType: "numeric", IsComputed: true, IsNullable: true}}
	deltas := CompareColumns(src, tgt, "mssql", "postgres")
	if len(deltas) != 1 || deltas[0].Criterion != "nullability" {
		t.Errorf("expected one nullability delta on computed column, got %v", deltas)
	}
}

// TestCompareColumns_IdentitySkipsDefault pins the "identity short-circuits
// the default check" rule. PG GENERATED IDENTITY is expressed via
// IsIdentity=true, not via a DefaultExpression — so an identity column on
// the target with empty DefaultExpression must NOT be flagged as
// "default dropped" relative to a source identity column with empty default.
func TestCompareColumns_IdentitySkipsDefault(t *testing.T) {
	src := []Column{{Name: "id", DataType: "int", IsIdentity: true, IsNullable: false}}
	tgt := []Column{{Name: "id", DataType: "integer", IsIdentity: true, IsNullable: false}}
	deltas := CompareColumns(src, tgt, "mssql", "postgres")
	if len(deltas) != 0 {
		t.Errorf("expected zero deltas for identity-to-identity match, got %v", deltas)
	}
}

// TestCompareColumns_CaseInsensitiveLookup pins the matching contract: PG
// folds names to lowercase, MSSQL preserves CamelCase. The match must work
// when the source has "Companies" and the target's parsed name is
// "companies" (lower-cased by PG).
func TestCompareColumns_CaseInsensitiveLookup(t *testing.T) {
	src := []Column{{Name: "FirstName", DataType: "nvarchar", MaxLength: 50}}
	tgt := []Column{{Name: "firstname", DataType: "varchar", MaxLength: 50}}
	deltas := CompareColumns(src, tgt, "mssql", "postgres")
	if len(deltas) != 0 {
		t.Errorf("expected zero deltas for case-equivalent column names, got %v", deltas)
	}
}

// TestCompareColumns_BinaryAndLOBLengths pins the #46 type_smoke cases:
// sized binary sources can only land as unbounded bytea on PG (no sized
// binary type exists), and LOB capacity numbers (mssql text=2147483647,
// mysql blob=65535, ...) are storage artifacts that must compare as
// unbounded — while sized-vs-sized binary mismatches still flag.
func TestCompareColumns_BinaryAndLOBLengths(t *testing.T) {
	pass := []struct {
		name             string
		src, tgt         Column
		srcDial, tgtDial string
	}{
		{"binary(16) → bytea", Column{Name: "b", DataType: "binary", MaxLength: 16}, Column{Name: "b", DataType: "bytea", MaxLength: 0}, "mssql", "postgres"},
		{"varbinary(50) → bytea", Column{Name: "b", DataType: "varbinary", MaxLength: 50}, Column{Name: "b", DataType: "bytea", MaxLength: 0}, "mssql", "postgres"},
		{"varbinary(MAX) → bytea", Column{Name: "b", DataType: "varbinary", MaxLength: -1}, Column{Name: "b", DataType: "bytea", MaxLength: 0}, "mssql", "postgres"},
		{"legacy text → text", Column{Name: "b", DataType: "text", MaxLength: 2147483647}, Column{Name: "b", DataType: "text", MaxLength: 0}, "mssql", "postgres"},
		{"legacy image → bytea", Column{Name: "b", DataType: "image", MaxLength: 2147483647}, Column{Name: "b", DataType: "bytea", MaxLength: 0}, "mssql", "postgres"},
		{"mysql blob → bytea", Column{Name: "b", DataType: "blob", MaxLength: 65535}, Column{Name: "b", DataType: "bytea", MaxLength: 0}, "mysql", "postgres"},
		{"mysql mediumtext → text", Column{Name: "b", DataType: "mediumtext", MaxLength: 16777215}, Column{Name: "b", DataType: "text", MaxLength: 0}, "mysql", "postgres"},
		{"mysql blob → mssql VARBINARY(MAX)", Column{Name: "b", DataType: "blob", MaxLength: 65535}, Column{Name: "b", DataType: "varbinary", MaxLength: -1}, "mysql", "mssql"},
	}
	for _, tc := range pass {
		t.Run(tc.name, func(t *testing.T) {
			deltas := CompareColumns([]Column{tc.src}, []Column{tc.tgt}, tc.srcDial, tc.tgtDial)
			if len(deltas) != 0 {
				t.Errorf("expected zero deltas, got %v", deltas)
			}
		})
	}

	// Negative: sized binary on a target that CAN size binary must still
	// round-trip exactly (the bytea exemption is pg-specific).
	src := []Column{{Name: "b", DataType: "varbinary", MaxLength: 50}}
	tgt := []Column{{Name: "b", DataType: "varbinary", MaxLength: 49}}
	deltas := CompareColumns(src, tgt, "mssql", "mysql")
	if len(deltas) != 1 || deltas[0].Criterion != "max_length" {
		t.Fatalf("expected one max_length delta for varbinary(50)→varbinary(49), got %v", deltas)
	}
}

// Codex review finding on the #46 LOB rules: MySQL tier names are real
// capacity choices when both sides speak them — LONGTEXT → TEXT rejects
// values above 64KiB and must flag — while cross-dialect mappings (pg text,
// mssql VARBINARY(MAX)) stay in the unbounded class. mssql's legacy `text`
// is a 2GB LOB, not a 64KiB tier, so it never participates.
func TestCompareColumns_MySQLLOBTiers(t *testing.T) {
	flag := []struct {
		name     string
		src, tgt Column
	}{
		{"longtext downgraded to text", Column{Name: "c", DataType: "longtext"}, Column{Name: "c", DataType: "text"}},
		{"mediumblob downgraded to blob", Column{Name: "c", DataType: "mediumblob"}, Column{Name: "c", DataType: "blob"}},
		{"text upgraded to longtext", Column{Name: "c", DataType: "text"}, Column{Name: "c", DataType: "longtext"}},
	}
	for _, tc := range flag {
		t.Run(tc.name, func(t *testing.T) {
			deltas := CompareColumns([]Column{tc.src}, []Column{tc.tgt}, "mysql", "mysql")
			if len(deltas) != 1 || deltas[0].Criterion != "max_length" {
				t.Fatalf("expected one max_length delta, got %v", deltas)
			}
		})
	}

	pass := []struct {
		name             string
		src, tgt         Column
		srcDial, tgtDial string
	}{
		{"same tier", Column{Name: "c", DataType: "mediumtext"}, Column{Name: "c", DataType: "mediumtext"}, "mysql", "mysql"},
		{"tier → pg text", Column{Name: "c", DataType: "longtext", MaxLength: 4294967295}, Column{Name: "c", DataType: "text"}, "mysql", "postgres"},
		{"tier → mssql MAX", Column{Name: "c", DataType: "longblob"}, Column{Name: "c", DataType: "varbinary", MaxLength: -1}, "mysql", "mssql"},
		{"mssql legacy text is not a tier", Column{Name: "c", DataType: "text", MaxLength: 2147483647}, Column{Name: "c", DataType: "longtext"}, "mssql", "mysql"},
	}
	for _, tc := range pass {
		t.Run(tc.name, func(t *testing.T) {
			deltas := CompareColumns([]Column{tc.src}, []Column{tc.tgt}, tc.srcDial, tc.tgtDial)
			if len(deltas) != 0 {
				t.Fatalf("expected zero deltas, got %v", deltas)
			}
		})
	}
}

// PG renders MSSQL GETUTCDATE()/SYSUTCDATETIME() as CURRENT_TIMESTAMP AT TIME
// ZONE 'UTC'. Both are current-datetime defaults; the comparator must treat
// them as equivalent so a freshly-created target doesn't report drift.
func TestDefaultExpressionClass_UTCNowEquivalence(t *testing.T) {
	utcForms := []string{
		"getutcdate()",
		"(getutcdate())",
		"CURRENT_TIMESTAMP AT TIME ZONE 'UTC'",
		"(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')",
		"(CURRENT_TIMESTAMP AT TIME ZONE 'UTC'::text)",
		"current_timestamp",
	}
	for _, f := range utcForms {
		if got := defaultExpressionClass(f); got != "current_dt" {
			t.Errorf("defaultExpressionClass(%q) = %q, want current_dt", f, got)
		}
	}
	// A source getutcdate() column vs a target rendered as the PG UTC form
	// must not flag a default delta.
	src := []Column{{Name: "at", DataType: "datetime2", DefaultExpression: "(getutcdate())"}}
	tgt := []Column{{Name: "at", DataType: "timestamp", DefaultExpression: "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC'::text)"}}
	for _, d := range CompareColumns(src, tgt, "mssql", "postgres") {
		if d.Criterion == "default" {
			t.Errorf("unexpected default delta for UTC-now round-trip: %s", d.String())
		}
	}
}
