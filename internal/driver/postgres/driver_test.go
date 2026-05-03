package postgres

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"smt/internal/driver"
)

// TestLoadColumnsSQL_DetectsModernIdentity is a regression guard for the
// PG → MSSQL/MySQL identity-column miss. The previous query only flagged
// columns with `column_default LIKE 'nextval%'` (old SERIAL form), so
// modern PostgreSQL `GENERATED ... AS IDENTITY` columns came through with
// IsIdentity=false and the AI then generated plain `INT NOT NULL` on the
// target instead of `IDENTITY(1,1)` / `AUTO_INCREMENT`.
//
// reader.go now picks one of two SQL strings based on the server version
// (the modern PG 10+ query references is_identity, which doesn't exist
// on PG 9.x). This test reads reader.go's source and asserts BOTH
// branches are present so the modern query keeps the new is_identity
// check and the legacy query stays as a safety net.
func TestLoadColumnsSQL_DetectsModernIdentity(t *testing.T) {
	body := readReaderSource(t)
	for _, needle := range []string{
		"is_identity = 'YES'",            // modern (PG 10+) covers GENERATED ... AS IDENTITY
		"column_default LIKE 'nextval%'", // legacy (PG 9.x) covers SERIAL / BIGSERIAL
		"server_version_num",             // version detection that picks between the two
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("loadColumns code missing required marker %q", needle)
		}
	}
}

// TestLoadForeignKeysSQL_QueriesPgConstraint is a regression guard for issue
// #17. LoadForeignKeys was previously a `return nil` stub; every pg-as-source
// migration produced 0 FKs on the target. This test asserts the implementation
// uses the right pg_catalog primitives — accidental rewrites that drop any of
// these markers would silently regress every pg-source pair.
func TestLoadForeignKeysSQL_QueriesPgConstraint(t *testing.T) {
	body := readReaderSource(t)
	for _, needle := range []string{
		"pg_constraint",            // source of truth for FK metadata
		"c.contype = 'f'",          // filter to foreign keys only
		"confdeltype",              // ON DELETE action
		"confupdtype",              // ON UPDATE action
		"LATERAL unnest(c.conkey)", // composite-FK column ordering
		"c.confkey[u.attposition]", // referenced-column lookup by position
		"'CASCADE'",                // CASE-mapping to writer-friendly keyword
		"'SET NULL'",               // (proves all action codes are mapped)
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("LoadForeignKeys code missing required marker %q", needle)
		}
	}
}

// TestLoadCheckConstraintsSQL_UsesPgGetExpr is a regression guard for issue
// #17 (the CHECK half). The implementation must use pg_get_expr(conbin,...) to
// produce predicate-only text without the "CHECK ( ... )" wrapper, matching
// the format the mssql/mysql readers produce so the AI prompt stays
// dialect-agnostic. Accidental switches to `consrc` (deprecated since PG 12,
// removed in some builds) or to including the CHECK wrapper would break the
// downstream prompt shape.
func TestLoadCheckConstraintsSQL_UsesPgGetExpr(t *testing.T) {
	body := readReaderSource(t)
	for _, needle := range []string{
		"pg_constraint",        // source of truth for CHECK metadata
		"c.contype = 'c'",      // filter to check constraints only
		"pg_get_expr(c.conbin", // predicate-only text extraction
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("LoadCheckConstraints code missing required marker %q", needle)
		}
	}
}

// readReaderSource returns the contents of reader.go as a string. Used by the
// regression-guard tests that grep for SQL markers without needing a live DB.
func readReaderSource(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed; cannot locate reader.go")
	}
	src, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "reader.go"))
	if err != nil {
		t.Fatalf("read reader.go: %v", err)
	}
	return string(src)
}

func TestDriverRegistration(t *testing.T) {
	// The driver should be registered via init()
	d, err := driver.Get("postgres")
	if err != nil {
		t.Fatalf("Failed to get postgres driver: %v", err)
	}

	if d.Name() != "postgres" {
		t.Errorf("Expected driver name 'postgres', got %q", d.Name())
	}

	// Test aliases
	for _, alias := range []string{"postgresql", "pg"} {
		d, err := driver.Get(alias)
		if err != nil {
			t.Errorf("Failed to get driver by alias %q: %v", alias, err)
			continue
		}
		if d.Name() != "postgres" {
			t.Errorf("Expected driver name 'postgres' for alias %q, got %q", alias, d.Name())
		}
	}
}

func TestDialect(t *testing.T) {
	dialect := &Dialect{}

	tests := []struct {
		name     string
		method   func() string
		expected string
	}{
		{"DBType", dialect.DBType, "postgres"},
		{"QuoteIdentifier", func() string { return dialect.QuoteIdentifier("test") }, `"test"`},
		{"QualifyTable", func() string { return dialect.QualifyTable("public", "users") }, `"public"."users"`},
		{"ParameterPlaceholder", func() string { return dialect.ParameterPlaceholder(1) }, "$1"},
		{"TableHint", func() string { return dialect.TableHint(false) }, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.method()
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestAvailableDrivers(t *testing.T) {
	available := driver.Available()
	found := false
	for _, name := range available {
		if name == "postgres" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("PostgreSQL driver not in available list: %v", available)
	}
}

func TestEstimateRowBytes(t *testing.T) {
	tests := []struct {
		name string
		rows [][]any
		want int // minimum expected
	}{
		{"empty rows", nil, 64},
		{"narrow int rows", [][]any{{1, 2, 3}}, 64},       // 3*8=24, clamped to 64
		{"string rows", [][]any{{"hello world", 42}}, 64}, // 11+8=19, clamped to 64
		{"wide rows", [][]any{{string(make([]byte, 10000))}}, 10000},
		{"mixed", [][]any{
			{string(make([]byte, 500)), 1, true},
			{string(make([]byte, 300)), 2, false},
		}, 200}, // avg ~(516+316)/2 = 416
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := estimateRowBytes(tt.rows, 10)
			if got < tt.want {
				t.Errorf("estimateRowBytes() = %d, want >= %d", got, tt.want)
			}
		})
	}
}

func TestCopyBatchSize(t *testing.T) {
	targetBytes := fallbackCopyBytes // 3 MB

	// Narrow rows (~64 bytes): 3MB/64 = 49152, under maxCopyBatchRows (50000)
	narrow := make([][]any, 100000)
	for i := range narrow {
		narrow[i] = []any{i, i + 1}
	}
	got := copyBatchSize(narrow, targetBytes)
	if got < 40000 || got > 50000 {
		t.Errorf("narrow rows: copyBatchSize() = %d, want in [40000, 50000]", got)
	}

	// Wide rows (~10KB each): 3MB / ~10008 bytes ≈ 314
	wide := make([][]any, 1000)
	for i := range wide {
		wide[i] = []any{string(make([]byte, 10000)), i}
	}
	got = copyBatchSize(wide, targetBytes)
	if got < 200 || got > 400 {
		t.Errorf("wide rows: copyBatchSize() = %d, want in [200, 400]", got)
	}

	// Very wide rows (~100KB each): 3MB / 102400 = ~30, clamped to minCopyBatchRows (100)
	veryWide := make([][]any, 10)
	for i := range veryWide {
		veryWide[i] = []any{string(make([]byte, 100000)), string(make([]byte, 2400))}
	}
	got = copyBatchSize(veryWide, targetBytes)
	if got != minCopyBatchRows {
		t.Errorf("very wide rows: copyBatchSize() = %d, want %d", got, minCopyBatchRows)
	}
}

func TestAIPromptAugmentation(t *testing.T) {
	dialect := &Dialect{}
	aug := dialect.AIPromptAugmentation()

	// Verify the augmentation contains critical PostgreSQL identifier rules
	checks := []string{
		"CRITICAL PostgreSQL identifier rules",
		"MUST preserve the exact spelling and underscores from the source name",
		"ONLY allowed transformation is lowercasing letters",
		"Do NOT abbreviate, shorten, remove underscores, or change any non-letter characters",
		"user_id → user_id (NOT userid)",
		"LastEditorDisplayName → lasteditordisplayname",
		"created_at → created_at (NOT createdat)",
		"Do NOT use double-quotes around identifiers",
	}

	for _, check := range checks {
		if !strings.Contains(aug, check) {
			t.Errorf("AIPromptAugmentation should contain %q", check)
		}
	}
}

// TestReaderDatabaseContext_Populated is the regression guard for issue #13.
// The orchestrator passes Reader.DatabaseContext() into TableOptions.SourceContext;
// returning nil here would silently produce one-sided AI prompts (full TARGET
// block, empty SOURCE block) and fire the "No source context available" lie in
// MIGRATION RULES. This test asserts the symbol is present in reader.go so
// accidental refactors that drop it surface at test time, not in a debug-prompt
// dump weeks later.
func TestReaderDatabaseContext_Populated(t *testing.T) {
	body := readReaderSource(t)
	for _, needle := range []string{
		"func (r *Reader) DatabaseContext()",
		"dbContextOnce.Do",
		"gatherDatabaseContext(", // shared helper called by Reader and Writer
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("reader.go missing required marker %q", needle)
		}
	}
}

// TestRetrySuccessCachesPreUnloggedDDL is the regression guard for the
// cache-poisoning bug Copilot caught on PR #30: caching the post-Unlogged-
// rewrite form would mean a successful retry stores `CREATE UNLOGGED TABLE
// ...` in the AI cache. Because tableCacheKey doesn't include
// TableOptions.Unlogged, a future call with opts.Unlogged=false would
// hit that cache entry and unexpectedly create an UNLOGGED table.
//
// The fix is structural in the writer: hold the AI-returned DDL separately
// from the execution-time DDL, cache only the AI form, let opts.Unlogged
// drive the rewrite per-call. This test reads writer.go and asserts those
// markers are present so a future refactor that collapses the two
// variables back together fails the test instead of silently regressing.
func TestRetrySuccessCachesPreUnloggedDDL(t *testing.T) {
	src, err := os.ReadFile(filepath.Join(filepath.Dir(callerFile(t)), "writer.go"))
	if err != nil {
		t.Fatalf("read writer.go: %v", err)
	}
	body := string(src)
	for _, needle := range []string{
		"aiDDL := resp.CreateTableDDL", // canonical form held separately
		"execDDL := aiDDL",             // execution form starts as the canonical
		"CacheTableDDL(req, aiDDL)",    // cache the AI form, NOT the rewritten one
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("writer.go missing required marker %q — see PR #30 review", needle)
		}
	}
	// And the inverse: ensure the reverse-direction bug (caching execDDL)
	// is NOT lurking. If anyone reintroduces CacheTableDDL(req, execDDL) the
	// cache-poisoning bug is back.
	if strings.Contains(body, "CacheTableDDL(req, execDDL)") {
		t.Error("writer.go regresses cache-poisoning bug from PR #30: caches execDDL (Unlogged-rewritten) instead of aiDDL")
	}
}

// callerFile returns the absolute path of the file calling this helper.
// Used by readReader-style tests that need to locate sibling files via
// runtime.Caller without hardcoding the package path.
func callerFile(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(1)
	if !ok {
		t.Fatal("runtime.Caller(1) failed")
	}
	return thisFile
}
