package canonical_test

// Oracle/delta test: for a corpus of source columns, compare the canonical
// round-trip FromCanonical(ToCanonical(...), "postgres") against the CURRENT
// deterministic pg renderer. Matches confirm behavior preservation; the
// reported deltas are the deliberate clean-model changes (#62) for sign-off.

import (
	"strings"
	"testing"

	"smt/internal/canonical"
	"smt/internal/driver"
	pgddl "smt/internal/driver/postgres"
)

type tcase struct {
	src  string // source dialect
	typ  string // source data_type
	meta canonical.TypeMeta
	col  driver.Column // same facts, for the renderer oracle
}

func ip(v int) *int { return &v }

func corpus() []tcase {
	mk := func(src, typ string, m canonical.TypeMeta) tcase {
		return tcase{src: src, typ: typ, meta: m, col: driver.Column{
			Name: "c", DataType: typ, MaxLength: m.MaxLength, Precision: m.Precision,
			Scale: m.Scale, DatetimePrecision: m.DatetimePrecision, IsUnsigned: m.IsUnsigned,
			DisplayWidth: m.DisplayWidth, EnumValues: m.EnumValues,
		}}
	}
	var c []tcase
	add := func(t tcase) { c = append(c, t) }

	// integers / booleans
	add(mk("mssql", "bit", canonical.TypeMeta{}))
	add(mk("postgres", "boolean", canonical.TypeMeta{}))
	add(mk("mysql", "tinyint", canonical.TypeMeta{DisplayWidth: 1}))
	add(mk("mysql", "tinyint", canonical.TypeMeta{}))
	add(mk("mysql", "mediumint", canonical.TypeMeta{}))
	add(mk("mssql", "smallint", canonical.TypeMeta{}))
	add(mk("mysql", "smallint", canonical.TypeMeta{IsUnsigned: true}))
	add(mk("mssql", "int", canonical.TypeMeta{}))
	add(mk("mysql", "int", canonical.TypeMeta{IsUnsigned: true}))
	add(mk("mssql", "bigint", canonical.TypeMeta{}))
	add(mk("mysql", "bigint", canonical.TypeMeta{IsUnsigned: true}))
	// numeric
	add(mk("mssql", "decimal", canonical.TypeMeta{Precision: 18, Scale: 4}))
	add(mk("mssql", "numeric", canonical.TypeMeta{}))
	add(mk("mssql", "money", canonical.TypeMeta{}))
	add(mk("mssql", "smallmoney", canonical.TypeMeta{}))
	add(mk("mssql", "float", canonical.TypeMeta{}))
	add(mk("mssql", "real", canonical.TypeMeta{}))
	// character
	add(mk("mssql", "varchar", canonical.TypeMeta{MaxLength: 20}))
	add(mk("mssql", "nvarchar", canonical.TypeMeta{MaxLength: -1}))
	add(mk("mssql", "char", canonical.TypeMeta{MaxLength: 10}))
	add(mk("mssql", "text", canonical.TypeMeta{}))
	add(mk("mysql", "longtext", canonical.TypeMeta{}))
	add(mk("mysql", "mediumtext", canonical.TypeMeta{}))
	// binary
	add(mk("mssql", "varbinary", canonical.TypeMeta{MaxLength: 50}))
	add(mk("mssql", "image", canonical.TypeMeta{}))
	add(mk("postgres", "bytea", canonical.TypeMeta{}))
	add(mk("mssql", "rowversion", canonical.TypeMeta{}))
	// temporal
	add(mk("mssql", "datetime2", canonical.TypeMeta{DatetimePrecision: ip(3)}))
	add(mk("mssql", "datetime2", canonical.TypeMeta{DatetimePrecision: ip(7)})) // clamp to 6
	add(mk("postgres", "timestamp", canonical.TypeMeta{}))
	add(mk("mysql", "timestamp", canonical.TypeMeta{}))
	add(mk("mssql", "datetimeoffset", canonical.TypeMeta{}))
	add(mk("mssql", "date", canonical.TypeMeta{}))
	add(mk("mssql", "time", canonical.TypeMeta{DatetimePrecision: ip(3)}))
	// structured
	add(mk("mssql", "uniqueidentifier", canonical.TypeMeta{}))
	add(mk("mysql", "json", canonical.TypeMeta{}))
	add(mk("mysql", "enum", canonical.TypeMeta{MaxLength: 8, EnumValues: []string{"a", "b"}}))
	return c
}

// stripIdentity removes the GENERATED suffix the pg renderer appends so we
// compare TYPE to TYPE (FromCanonical returns the bare type).
func stripIdentity(s string) string {
	if i := strings.Index(s, " GENERATED "); i >= 0 {
		return s[:i]
	}
	return s
}

func TestOraclePG_DeltaReport(t *testing.T) {
	var deltas, errs int
	for _, tc := range corpus() {
		rendered, rerr := pgddl.RenderColumnTypeWithPolicy(tc.col, "fail")
		ct := canonical.ToCanonical(tc.typ, tc.meta, tc.src)
		canon, cerr := canonical.FromCanonical(ct, "postgres", canonical.RenderOpts{})

		switch {
		case rerr != nil && cerr != nil:
			// both reject — agreement
		case rerr != nil && cerr == nil:
			t.Logf("RENDERER-ERR  %-9s %-16s renderer=ERR(%v)  canonical=%q", tc.src, tc.typ, rerr, canon)
			errs++
		case rerr == nil && cerr != nil:
			t.Logf("CANONICAL-ERR %-9s %-16s renderer=%q  canonical=ERR(%v)", tc.src, tc.typ, stripIdentity(rendered), cerr)
			errs++
		default:
			want := stripIdentity(rendered)
			if want != canon {
				t.Logf("DELTA  %-9s %-16s  renderer=%-28q  canonical=%q", tc.src, tc.typ, want, canon)
				deltas++
			}
		}
	}
	t.Logf("=== *→pg: %d delta(s), %d error-disagreement(s) across %d cases ===", deltas, errs, len(corpus()))
}
