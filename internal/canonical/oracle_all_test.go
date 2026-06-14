package canonical_test

// Full *→{mssql,mysql} oracle/delta report against the live ddl.Renderer
// (mssql/mysql paths). Reuses corpus() from oracle_pg_test.go.

import (
	"testing"

	"smt/internal/canonical"
	"smt/internal/ddl"
)

func runOracle(t *testing.T, target string) {
	var deltas, errs int
	for _, tc := range corpus() {
		r, err := ddl.NewRenderer(target, "", "fail")
		if err != nil {
			t.Fatalf("NewRenderer(%s): %v", target, err)
		}
		r = r.WithSource(tc.src)
		rendered, rerr := r.ColumnType(tc.col)

		ct := canonical.ToCanonical(tc.typ, tc.meta, tc.src)
		canon, cerr := canonical.FromCanonical(ct, target, canonical.RenderOpts{})

		switch {
		case rerr != nil && cerr != nil:
		case rerr != nil && cerr == nil:
			t.Logf("RENDERER-ERR  %-9s %-16s renderer=ERR  canonical=%q", tc.src, tc.typ, canon)
			errs++
		case rerr == nil && cerr != nil:
			t.Logf("CANONICAL-ERR %-9s %-16s renderer=%q  canonical=ERR", tc.src, tc.typ, rendered)
			errs++
		default:
			if rendered != canon {
				t.Logf("DELTA  %-9s %-16s  renderer=%-22q  canonical=%q", tc.src, tc.typ, rendered, canon)
				deltas++
			}
		}
	}
	t.Logf("=== *→%s: %d delta(s), %d error-disagreement(s) across %d cases ===", target, deltas, errs, len(corpus()))
}

func TestOracleMSSQL_DeltaReport(t *testing.T) { runOracle(t, "mssql") }
func TestOracleMySQL_DeltaReport(t *testing.T) { runOracle(t, "mysql") }
