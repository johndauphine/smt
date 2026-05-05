package driver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"smt/internal/secrets"
)

// finalizationDDLServer returns an httptest server that responds to every
// OpenAI-compatible chat completion request with the supplied DDL string,
// and atomically increments the supplied counter on each call.
func finalizationDDLServer(t *testing.T, ddl string, calls *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(openAIResponse{
			Choices: []struct {
				Message struct {
					Content          string `json:"content"`
					ReasoningContent string `json:"reasoning_content"`
				} `json:"message"`
				FinishReason string `json:"finish_reason"`
			}{
				{
					Message: struct {
						Content          string `json:"content"`
						ReasoningContent string `json:"reasoning_content"`
					}{Content: ddl},
					FinishReason: "stop",
				},
			},
		})
	}))
}

// TestFinalizationCacheKey_SensitivityAndStability is the structural
// counterpart of TestTableCacheKey_IncludesNewFields: the cache key must
// change whenever a field that affects DDL changes, and stay stable
// otherwise. Without this guard, a future code edit could silently make the
// cache return stale DDL after a meaningful metadata change (e.g. flipping
// IsUnique on an index, changing FK OnDelete, or rewriting a CHECK
// definition).
func TestFinalizationCacheKey_SensitivityAndStability(t *testing.T) {
	m := &AITypeMapper{}

	baseIdx := FinalizationDDLRequest{
		Type:         DDLTypeIndex,
		SourceDBType: "mssql", TargetDBType: "postgres",
		TargetSchema: "public",
		Table:        &Table{Name: "orders"},
		Index: &Index{
			Name:        "idx_customer",
			Columns:     []string{"customer_id"},
			IsUnique:    false,
			IncludeCols: []string{},
			Filter:      "",
		},
	}

	baseFK := FinalizationDDLRequest{
		Type:         DDLTypeForeignKey,
		SourceDBType: "mssql", TargetDBType: "postgres",
		TargetSchema: "public",
		Table:        &Table{Name: "orders"},
		ForeignKey: &ForeignKey{
			Name:       "fk_customer",
			Columns:    []string{"customer_id"},
			RefSchema:  "public",
			RefTable:   "customers",
			RefColumns: []string{"id"},
			OnDelete:   "NO ACTION",
			OnUpdate:   "NO ACTION",
		},
	}

	baseCheck := FinalizationDDLRequest{
		Type:         DDLTypeCheckConstraint,
		SourceDBType: "mssql", TargetDBType: "postgres",
		TargetSchema: "public",
		Table:        &Table{Name: "orders"},
		CheckConstraint: &CheckConstraint{
			Name:       "chk_total_positive",
			Definition: "total >= 0",
		},
	}

	t.Run("stable_under_repeat_call", func(t *testing.T) {
		// Same input twice → same key.
		for _, req := range []FinalizationDDLRequest{baseIdx, baseFK, baseCheck} {
			a := m.finalizationCacheKey(req)
			b := m.finalizationCacheKey(req)
			if a != b {
				t.Errorf("non-deterministic key for %s: %q vs %q", req.Type, a, b)
			}
			if a == "" {
				t.Errorf("empty key for valid %s req", req.Type)
			}
		}
	})

	t.Run("disambiguates_ddl_types", func(t *testing.T) {
		// Different DDL types must never collide even with shared metadata.
		seen := map[string]DDLType{}
		for _, req := range []FinalizationDDLRequest{baseIdx, baseFK, baseCheck} {
			k := m.finalizationCacheKey(req)
			if other, dup := seen[k]; dup {
				t.Errorf("%s and %s produced same key %q", req.Type, other, k)
			}
			seen[k] = req.Type
		}
	})

	idxCases := []struct {
		name string
		mut  func(r *FinalizationDDLRequest)
	}{
		{"unique_flipped", func(r *FinalizationDDLRequest) { r.Index.IsUnique = true }},
		{"columns_changed", func(r *FinalizationDDLRequest) { r.Index.Columns = []string{"order_id"} }},
		{"include_cols_added", func(r *FinalizationDDLRequest) { r.Index.IncludeCols = []string{"total"} }},
		{"filter_added", func(r *FinalizationDDLRequest) { r.Index.Filter = "active = 1" }},
		{"name_changed", func(r *FinalizationDDLRequest) { r.Index.Name = "idx_other" }},
		{"target_schema_changed", func(r *FinalizationDDLRequest) { r.TargetSchema = "audit" }},
		{"table_changed", func(r *FinalizationDDLRequest) { r.Table = &Table{Name: "shipments"} }},
		{"target_db_changed", func(r *FinalizationDDLRequest) { r.TargetDBType = "mysql" }},
		{"source_db_changed", func(r *FinalizationDDLRequest) { r.SourceDBType = "mysql" }},
	}
	for _, tc := range idxCases {
		t.Run("index_"+tc.name, func(t *testing.T) {
			req := baseIdx
			req.Index = &Index{
				Name: baseIdx.Index.Name, Columns: append([]string{}, baseIdx.Index.Columns...),
				IsUnique: baseIdx.Index.IsUnique, IncludeCols: append([]string{}, baseIdx.Index.IncludeCols...), Filter: baseIdx.Index.Filter,
			}
			tc.mut(&req)
			if a, b := m.finalizationCacheKey(baseIdx), m.finalizationCacheKey(req); a == b {
				t.Errorf("index key did not change after %s\nbase: %s\ngot:  %s", tc.name, a, b)
			}
		})
	}

	fkCases := []struct {
		name string
		mut  func(r *FinalizationDDLRequest)
	}{
		{"name_changed", func(r *FinalizationDDLRequest) { r.ForeignKey.Name = "fk_other" }},
		{"columns_changed", func(r *FinalizationDDLRequest) { r.ForeignKey.Columns = []string{"order_id"} }},
		{"ref_table_changed", func(r *FinalizationDDLRequest) { r.ForeignKey.RefTable = "users" }},
		{"ref_columns_changed", func(r *FinalizationDDLRequest) { r.ForeignKey.RefColumns = []string{"user_id"} }},
		{"ref_schema_changed", func(r *FinalizationDDLRequest) { r.ForeignKey.RefSchema = "audit" }},
		{"on_delete_changed", func(r *FinalizationDDLRequest) { r.ForeignKey.OnDelete = "CASCADE" }},
		{"on_update_changed", func(r *FinalizationDDLRequest) { r.ForeignKey.OnUpdate = "SET NULL" }},
	}
	for _, tc := range fkCases {
		t.Run("fk_"+tc.name, func(t *testing.T) {
			req := baseFK
			req.ForeignKey = &ForeignKey{
				Name: baseFK.ForeignKey.Name, Columns: append([]string{}, baseFK.ForeignKey.Columns...),
				RefSchema: baseFK.ForeignKey.RefSchema, RefTable: baseFK.ForeignKey.RefTable,
				RefColumns: append([]string{}, baseFK.ForeignKey.RefColumns...),
				OnDelete:   baseFK.ForeignKey.OnDelete, OnUpdate: baseFK.ForeignKey.OnUpdate,
			}
			tc.mut(&req)
			if a, b := m.finalizationCacheKey(baseFK), m.finalizationCacheKey(req); a == b {
				t.Errorf("fk key did not change after %s\nbase: %s\ngot:  %s", tc.name, a, b)
			}
		})
	}

	chkCases := []struct {
		name string
		mut  func(r *FinalizationDDLRequest)
	}{
		{"name_changed", func(r *FinalizationDDLRequest) { r.CheckConstraint.Name = "chk_other" }},
		{"definition_changed", func(r *FinalizationDDLRequest) { r.CheckConstraint.Definition = "total > 0" }},
	}
	for _, tc := range chkCases {
		t.Run("check_"+tc.name, func(t *testing.T) {
			req := baseCheck
			req.CheckConstraint = &CheckConstraint{
				Name: baseCheck.CheckConstraint.Name, Definition: baseCheck.CheckConstraint.Definition,
			}
			tc.mut(&req)
			if a, b := m.finalizationCacheKey(baseCheck), m.finalizationCacheKey(req); a == b {
				t.Errorf("check key did not change after %s\nbase: %s\ngot:  %s", tc.name, a, b)
			}
		})
	}

	t.Run("target_table_ddl_does_not_change_key", func(t *testing.T) {
		// TargetTableDDL is prompt context, not part of the constraint
		// fingerprint. If it changed the key, every FK/index/check would
		// re-invalidate any time an unrelated column on the parent table
		// changed — defeating the cache on incremental re-runs.
		req := baseFK
		req.TargetTableDDL = "CREATE TABLE orders (...);"
		baseKey := m.finalizationCacheKey(baseFK)
		ctxKey := m.finalizationCacheKey(req)
		if baseKey != ctxKey {
			t.Errorf("TargetTableDDL must not affect cache key:\n  base: %s\n  ctx:  %s", baseKey, ctxKey)
		}
	})

	t.Run("unknown_type_returns_empty", func(t *testing.T) {
		// Defensive: bogus DDLType returns empty key, which CacheFinalizationDDL
		// treats as a no-op (avoids polluting the cache file with garbage).
		req := FinalizationDDLRequest{Type: DDLType("nope"), Table: &Table{Name: "x"}}
		if k := m.finalizationCacheKey(req); k != "" {
			t.Errorf("expected empty key for unknown type, got %q", k)
		}
	})
}

// TestGenerateFinalizationDDL_DoesNotAutoCache mirrors the table-DDL #32
// regression test: GenerateFinalizationDDL must NOT write to the cache,
// because at that point the AI's output hasn't been validated against the
// target database. The writer's CacheFinalizationDDL is the only path that
// populates the cache, and it's only called after a successful exec — so a
// failed DDL can't poison the cache for subsequent runs.
func TestGenerateFinalizationDDL_DoesNotAutoCache(t *testing.T) {
	cases := []struct {
		name string
		ddl  string
		req  FinalizationDDLRequest
	}{
		{
			name: "index",
			ddl:  "CREATE INDEX idx_foo ON public.t (a);",
			req: FinalizationDDLRequest{
				Type:         DDLTypeIndex,
				SourceDBType: "mssql", TargetDBType: "postgres",
				TargetSchema: "public", Table: &Table{Name: "t"},
				Index: &Index{Name: "idx_foo", Columns: []string{"a"}},
			},
		},
		{
			name: "fk",
			ddl:  "ALTER TABLE public.t ADD CONSTRAINT fk_foo FOREIGN KEY (a) REFERENCES public.p (id);",
			req: FinalizationDDLRequest{
				Type:         DDLTypeForeignKey,
				SourceDBType: "mssql", TargetDBType: "postgres",
				TargetSchema: "public", Table: &Table{Name: "t"},
				ForeignKey: &ForeignKey{
					Name: "fk_foo", Columns: []string{"a"},
					RefSchema: "public", RefTable: "p", RefColumns: []string{"id"},
				},
			},
		},
		{
			name: "check",
			ddl:  "ALTER TABLE public.t ADD CONSTRAINT chk_foo CHECK (a >= 0);",
			req: FinalizationDDLRequest{
				Type:         DDLTypeCheckConstraint,
				SourceDBType: "mssql", TargetDBType: "postgres",
				TargetSchema: "public", Table: &Table{Name: "t"},
				CheckConstraint: &CheckConstraint{Name: "chk_foo", Definition: "a >= 0"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := finalizationDDLServer(t, tc.ddl, &calls)
			defer server.Close()

			mapper := testMapperWithTempCache(t, "lmstudio", &secrets.Provider{
				APIKey: "", Model: "test-model", BaseURL: server.URL,
			})
			mapper.client = server.Client()

			if _, err := mapper.GenerateFinalizationDDL(context.Background(), tc.req); err != nil {
				t.Fatalf("GenerateFinalizationDDL: %v", err)
			}

			// Cache must remain empty until CacheFinalizationDDL is called.
			cacheKey := mapper.finalizationCacheKey(tc.req)
			mapper.cacheMu.RLock()
			_, hit := mapper.cache.Get(cacheKey)
			mapper.cacheMu.RUnlock()
			if hit {
				t.Errorf("BUG: mapper auto-cached AI output before exec validation; cache must remain empty until CacheFinalizationDDL is called")
			}

			// Sanity: explicit CacheFinalizationDDL populates the cache.
			mapper.CacheFinalizationDDL(tc.req, tc.ddl)
			mapper.cacheMu.RLock()
			cached, _ := mapper.cache.Get(cacheKey)
			mapper.cacheMu.RUnlock()
			if cached != tc.ddl {
				t.Errorf("explicit CacheFinalizationDDL didn't populate cache:\n  got:  %q\n  want: %q", cached, tc.ddl)
			}
		})
	}
}

// TestGenerateFinalizationDDL_FailedFirstTryDoesNotPoison: two successive
// GenerateFinalizationDDL calls without an intervening CacheFinalizationDDL
// must each invoke the AI. Without this property, a first call that
// generated bad DDL would cache it and every subsequent run would fail in
// the same way without ever giving the AI another chance.
func TestGenerateFinalizationDDL_FailedFirstTryDoesNotPoison(t *testing.T) {
	var calls atomic.Int32
	server := finalizationDDLServer(t, "CREATE INDEX idx_foo ON public.t (a);", &calls)
	defer server.Close()

	mapper := testMapperWithTempCache(t, "lmstudio", &secrets.Provider{
		APIKey: "", Model: "test-model", BaseURL: server.URL,
	})
	mapper.client = server.Client()

	req := FinalizationDDLRequest{
		Type:         DDLTypeIndex,
		SourceDBType: "mssql", TargetDBType: "postgres",
		TargetSchema: "public", Table: &Table{Name: "t"},
		Index: &Index{Name: "idx_foo", Columns: []string{"a"}},
	}

	// Simulate writer calling GenerateFinalizationDDL but exec subsequently
	// failing — so CacheFinalizationDDL is never invoked.
	if _, err := mapper.GenerateFinalizationDDL(context.Background(), req); err != nil {
		t.Fatalf("first GenerateFinalizationDDL: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 AI call, got %d", got)
	}
	if _, err := mapper.GenerateFinalizationDDL(context.Background(), req); err != nil {
		t.Fatalf("second GenerateFinalizationDDL: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("BUG: second GenerateFinalizationDDL hit cache instead of re-calling AI; got %d total calls, want 2", got)
	}
}

// TestGenerateFinalizationDDL_RetrySkipsCache covers the retry path: a call
// with PreviousAttempt set must bypass any cached value. Otherwise the
// retry would return the stale (just-failed) DDL instead of giving the AI
// the corrective context.
func TestGenerateFinalizationDDL_RetrySkipsCache(t *testing.T) {
	var calls atomic.Int32
	server := finalizationDDLServer(t, "CREATE INDEX idx_foo ON public.t (a);", &calls)
	defer server.Close()

	mapper := testMapperWithTempCache(t, "lmstudio", &secrets.Provider{
		APIKey: "", Model: "test-model", BaseURL: server.URL,
	})
	mapper.client = server.Client()

	req := FinalizationDDLRequest{
		Type:         DDLTypeIndex,
		SourceDBType: "mssql", TargetDBType: "postgres",
		TargetSchema: "public", Table: &Table{Name: "t"},
		Index: &Index{Name: "idx_foo", Columns: []string{"a"}},
	}
	// Pre-populate the cache to verify retry bypasses it.
	mapper.CacheFinalizationDDL(req, "CACHED-BAD-DDL-MUST-NOT-RETURN")

	// First call: no PreviousAttempt → cache hit, no AI call.
	if _, err := mapper.GenerateFinalizationDDL(context.Background(), req); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if got := calls.Load(); got != 0 {
		t.Errorf("first-try call should have hit cache (no AI call), got %d AI calls", got)
	}

	// Retry call: PreviousAttempt set → must skip cache and call AI.
	retryReq := req
	retryReq.PreviousAttempt = &FinalizationDDLAttempt{DDL: "bad", Error: "syntax"}
	if _, err := mapper.GenerateFinalizationDDL(context.Background(), retryReq); err != nil {
		t.Fatalf("retry call: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("retry call should have invoked AI (cache skipped); got %d AI calls", got)
	}
}

// TestCacheFinalizationDDL exercises the writer's post-exec cache-write
// entry point. After CacheFinalizationDDL stores validated DDL, a
// subsequent first-try GenerateFinalizationDDL call hits the cache and
// makes zero AI calls. Replace-not-append behavior is also asserted: a
// second CacheFinalizationDDL with different DDL overwrites the prior
// value — without that, a successful retry couldn't fully clean up a
// previously-cached bad DDL.
func TestCacheFinalizationDDL(t *testing.T) {
	var calls atomic.Int32
	server := finalizationDDLServer(t, "AI-RESPONSE-MUST-NOT-BE-NEEDED", &calls)
	defer server.Close()

	mapper := testMapperWithTempCache(t, "lmstudio", &secrets.Provider{
		APIKey: "", Model: "test-model", BaseURL: server.URL,
	})
	mapper.client = server.Client()

	req := FinalizationDDLRequest{
		Type:         DDLTypeForeignKey,
		SourceDBType: "mssql", TargetDBType: "postgres",
		TargetSchema: "public", Table: &Table{Name: "orders"},
		ForeignKey: &ForeignKey{
			Name: "fk_customer", Columns: []string{"customer_id"},
			RefSchema: "public", RefTable: "customers", RefColumns: []string{"id"},
		},
	}

	const validatedDDL = "ALTER TABLE public.orders ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES public.customers (id);"
	mapper.CacheFinalizationDDL(req, validatedDDL)

	got, err := mapper.GenerateFinalizationDDL(context.Background(), req)
	if err != nil {
		t.Fatalf("GenerateFinalizationDDL after cache prime: %v", err)
	}
	if got != validatedDDL {
		t.Errorf("cache-hit path returned wrong DDL:\n  got:  %q\n  want: %q", got, validatedDDL)
	}
	if c := calls.Load(); c != 0 {
		t.Errorf("cache hit should not invoke AI; got %d AI calls", c)
	}

	// Replace-not-append.
	const replacementDDL = "ALTER TABLE public.orders ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES public.customers (id) ON DELETE CASCADE;"
	mapper.CacheFinalizationDDL(req, replacementDDL)
	got, err = mapper.GenerateFinalizationDDL(context.Background(), req)
	if err != nil {
		t.Fatalf("GenerateFinalizationDDL after replacement: %v", err)
	}
	if got != replacementDDL {
		t.Errorf("CacheFinalizationDDL did not replace prior value:\n  got:  %q\n  want: %q", got, replacementDDL)
	}
}

// TestGenerateDropTableDDL_DoesNotAutoCache mirrors #32 for DROP TABLE: the
// mapper must not write to its own cache. Pre-#32 the DROP path auto-cached
// AI output; this test guards against regressions to that behavior.
func TestGenerateDropTableDDL_DoesNotAutoCache(t *testing.T) {
	var calls atomic.Int32
	server := finalizationDDLServer(t, "DROP TABLE IF EXISTS public.foo CASCADE;", &calls)
	defer server.Close()

	mapper := testMapperWithTempCache(t, "lmstudio", &secrets.Provider{
		APIKey: "", Model: "test-model", BaseURL: server.URL,
	})
	mapper.client = server.Client()

	req := DropTableDDLRequest{
		TargetDBType: "postgres", TargetSchema: "public", TableName: "foo",
	}
	if _, err := mapper.GenerateDropTableDDL(context.Background(), req); err != nil {
		t.Fatalf("GenerateDropTableDDL: %v", err)
	}

	cacheKey := mapper.dropTableCacheKey(req)
	mapper.cacheMu.RLock()
	_, hit := mapper.cache.Get(cacheKey)
	mapper.cacheMu.RUnlock()
	if hit {
		t.Errorf("BUG: GenerateDropTableDDL auto-cached AI output; cache must remain empty until CacheDropTableDDL is called")
	}

	// Sanity: CacheDropTableDDL populates the cache, and a subsequent
	// Generate call hits it without calling AI.
	mapper.CacheDropTableDDL(req, "DROP TABLE IF EXISTS public.foo CASCADE;")
	calls.Store(0)
	if _, err := mapper.GenerateDropTableDDL(context.Background(), req); err != nil {
		t.Fatalf("second GenerateDropTableDDL: %v", err)
	}
	if c := calls.Load(); c != 0 {
		t.Errorf("post-CacheDropTableDDL Generate should hit cache, got %d AI calls", c)
	}
}

// TestGenerateDropTableDDL_FailedFirstTryDoesNotPoison: two Generate calls
// without intervening CacheDropTableDDL must each invoke the AI.
func TestGenerateDropTableDDL_FailedFirstTryDoesNotPoison(t *testing.T) {
	var calls atomic.Int32
	server := finalizationDDLServer(t, "DROP TABLE IF EXISTS public.foo CASCADE;", &calls)
	defer server.Close()

	mapper := testMapperWithTempCache(t, "lmstudio", &secrets.Provider{
		APIKey: "", Model: "test-model", BaseURL: server.URL,
	})
	mapper.client = server.Client()

	req := DropTableDDLRequest{TargetDBType: "postgres", TargetSchema: "public", TableName: "foo"}
	if _, err := mapper.GenerateDropTableDDL(context.Background(), req); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if _, err := mapper.GenerateDropTableDDL(context.Background(), req); err != nil {
		t.Fatalf("second call: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("BUG: second GenerateDropTableDDL hit cache; got %d AI calls, want 2", got)
	}
}

// TestGenerateDropTableDDL_RejectsNonIdempotent guards the validator: AI
// output without IF EXISTS must not be returned (and therefore must not be
// cached). All dialect prompts mandate IF EXISTS, but the cache replays the
// exact AI output — a model that ignores the prompt would otherwise produce
// a non-idempotent DDL that succeeds once and fails on every subsequent run.
func TestGenerateDropTableDDL_RejectsNonIdempotent(t *testing.T) {
	var calls atomic.Int32
	// AI returns a bare "DROP TABLE" with no IF EXISTS.
	server := finalizationDDLServer(t, "DROP TABLE public.foo;", &calls)
	defer server.Close()

	mapper := testMapperWithTempCache(t, "lmstudio", &secrets.Provider{
		APIKey: "", Model: "test-model", BaseURL: server.URL,
	})
	mapper.client = server.Client()

	req := DropTableDDLRequest{TargetDBType: "postgres", TargetSchema: "public", TableName: "foo"}
	_, err := mapper.GenerateDropTableDDL(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for non-idempotent DROP, got nil")
	}
	if !strings.Contains(err.Error(), "IF EXISTS") {
		t.Errorf("expected error to mention IF EXISTS, got: %v", err)
	}

	// And nothing should have been cached as a side effect.
	cacheKey := mapper.dropTableCacheKey(req)
	mapper.cacheMu.RLock()
	_, hit := mapper.cache.Get(cacheKey)
	mapper.cacheMu.RUnlock()
	if hit {
		t.Error("non-idempotent DROP must not populate cache")
	}
}
