package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"smt/internal/config"
	ddlpkg "smt/internal/ddl"
	"smt/internal/driver"
)

type fakeExpressionFixSuggester struct {
	fixes map[string]*driver.ExpressionFix
	calls []driver.FixRequest
	err   error
}

func (f *fakeExpressionFixSuggester) SuggestExpressionFix(_ context.Context, req driver.FixRequest) (*driver.ExpressionFix, error) {
	f.calls = append(f.calls, req)
	if f.err != nil {
		return nil, f.err
	}
	fix, ok := f.fixes[req.Kind+"\x00"+req.ColumnName]
	if !ok {
		return nil, fmt.Errorf("unexpected fix request for %s %s", req.Kind, req.ColumnName)
	}
	cp := *fix
	return &cp, nil
}

func mssqlPostgresCreateRenderer(t *testing.T) createDDLRenderer {
	t.Helper()
	r, err := ddlpkg.NewRenderer("postgres", "public", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	return createDDLRenderer{
		sourceType:   "mssql",
		targetType:   "postgres",
		targetSchema: "public",
		ddlRenderer:  r.WithSource("mssql"),
	}
}

func singleSplicedFixForTest(exprErr *driver.ExpressionRenderError, fix *driver.ExpressionFix, ddl string, classMatched bool) *splicedFix {
	return &splicedFix{
		expressions: []splicedExpressionFix{{
			exprErr:      exprErr,
			fix:          fix,
			classMatched: classMatched,
		}},
		ddl: ddl,
	}
}

// withDiagnosisHandler captures whether a diagnosis was emitted, so the
// opt-in/no-op guardrails can be asserted without a live AI provider.
func withDiagnosisHandler(t *testing.T) *bool {
	t.Helper()
	emitted := false
	driver.SetDiagnosisHandler(func(*driver.ErrorDiagnosis) { emitted = true })
	t.Cleanup(func() { driver.SetDiagnosisHandler(nil) })
	return &emitted
}

// When diagnose_failures is off, the hook must short-circuit before resolving a
// provider or emitting anything — failure diagnosis is strictly opt-in.
func TestDiagnoseSchemaFailure_DisabledIsNoOp(t *testing.T) {
	emitted := withDiagnosisHandler(t)
	o := &Orchestrator{config: &config.Config{}} // AIReview.DiagnoseFailures defaults false

	o.diagnoseSchemaFailure(context.Background(), "Employees", "dbo", "rendering CREATE TABLE DDL", errors.New("boom"))

	if *emitted {
		t.Error("diagnosis emitted while ai_review.diagnose_failures is disabled")
	}
	if o.diagnoser != nil {
		t.Error("provider resolved while diagnosis is disabled; should stay lazy/unresolved")
	}
}

// A nil cause is a no-op even when enabled (nothing to diagnose).
func TestDiagnoseSchemaFailure_NilCauseIsNoOp(t *testing.T) {
	emitted := withDiagnosisHandler(t)
	cfg := &config.Config{}
	cfg.AIReview.DiagnoseFailures = true
	o := &Orchestrator{config: cfg}

	o.diagnoseSchemaFailure(context.Background(), "Employees", "dbo", "rendering CREATE TABLE DDL", nil)

	if *emitted {
		t.Error("diagnosis emitted for a nil cause")
	}
}

// The suggestion file must be unmistakably labeled as AI-assisted/unverified,
// show exactly which expression(s) came from the AI, and contain SMT's
// deterministic DDL so a user can never confuse it with schema.sql.
func TestRenderSuggestionFile_LabeledAndProvenanced(t *testing.T) {
	out := renderSuggestionFile(
		"dbo.Subscriptions",
		singleSplicedFixForTest(
			&driver.ExpressionRenderError{Column: "ExpiresAt", Kind: "default", SourceExpr: "dateadd(year,1,getdate())"},
			&driver.ExpressionFix{Expression: "CURRENT_TIMESTAMP + INTERVAL '1 year'", Explanation: "interval arithmetic", Confidence: "high"},
			`CREATE TABLE "subscriptions" ("expiresat" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP + INTERVAL '1 year')`,
			false, // class not confirmed
		),
	)
	for _, want := range []string{
		"AI-ASSISTED", "review before applying", "did not and will not apply it",
		"dbo.Subscriptions", "Object 1: ExpiresAt (default)",
		"AI-translated 1: dateadd(year,1,getdate())  ->  CURRENT_TIMESTAMP + INTERVAL '1 year'",
		"Confidence 1: high", `CREATE TABLE "subscriptions"`,
		"[REVIEW]", // verification verdict for an unconfirmed class
	} {
		if !strings.Contains(out, want) {
			t.Errorf("suggestion file missing %q:\n%s", want, out)
		}
	}
}

// A class-confirmed translation stamps [OK] rather than [REVIEW].
func TestRenderSuggestionFile_ClassMatchedStampsOK(t *testing.T) {
	out := renderSuggestionFile("T",
		singleSplicedFixForTest(
			&driver.ExpressionRenderError{Column: "d", Kind: "default", SourceExpr: "(convert(date,getdate()))"},
			&driver.ExpressionFix{Expression: "CURRENT_DATE", Confidence: "high"},
			"CREATE TABLE t (d date)", true,
		))
	if !strings.Contains(out, "[OK]") || strings.Contains(out, "[REVIEW]") {
		t.Errorf("expected [OK] verdict, got:\n%s", out)
	}
}

func TestRenderSuggestionFile_ListsMultipleExpressions(t *testing.T) {
	out := renderSuggestionFile("dbo.SubscriptionWindows", &splicedFix{
		expressions: []splicedExpressionFix{
			{
				exprErr: &driver.ExpressionRenderError{Column: "StartsAt", Kind: "default", SourceExpr: "dateadd(day,7,getdate())"},
				fix:     &driver.ExpressionFix{Expression: "CURRENT_TIMESTAMP + INTERVAL '7 days'", Confidence: "high"},
			},
			{
				exprErr: &driver.ExpressionRenderError{Column: "EndsAt", Kind: "default", SourceExpr: "dateadd(year,1,getdate())"},
				fix:     &driver.ExpressionFix{Expression: "CURRENT_TIMESTAMP + INTERVAL '1 year'", Confidence: "medium"},
			},
		},
		ddl: `CREATE TABLE "subscriptionwindows" ("startsat" timestamp DEFAULT CURRENT_TIMESTAMP + INTERVAL '7 days', "endsat" timestamp DEFAULT CURRENT_TIMESTAMP + INTERVAL '1 year')`,
	})
	for _, want := range []string{
		"Object 1: StartsAt (default)",
		"Object 2: EndsAt (default)",
		"AI-translated 1: dateadd(day,7,getdate())  ->  CURRENT_TIMESTAMP + INTERVAL '7 days'",
		"AI-translated 2: dateadd(year,1,getdate())  ->  CURRENT_TIMESTAMP + INTERVAL '1 year'",
		"only the expression(s) above are AI-authored",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("suggestion file missing %q:\n%s", want, out)
		}
	}
}

// A multi-line AI/source string must not break out of the single-line "-- "
// banner comments — every line above the DDL body stays a comment.
func TestRenderSuggestionFile_SanitizesMultilineBannerFields(t *testing.T) {
	out := renderSuggestionFile(
		"T",
		singleSplicedFixForTest(
			&driver.ExpressionRenderError{Column: "c", Kind: "default", SourceExpr: "x\nDROP TABLE users; --"},
			&driver.ExpressionFix{Expression: "1\nDELETE FROM secrets;", Explanation: "ok\nrm -rf /", Confidence: "low"},
			"CREATE TABLE t (c int)",
			false,
		),
	)
	header := strings.SplitN(out, "\n\nCREATE TABLE", 2)[0]
	for _, line := range strings.Split(header, "\n") {
		if line != "" && !strings.HasPrefix(line, "--") {
			t.Errorf("non-comment line escaped into the banner: %q", line)
		}
	}
}

// suggest_fixes is opt-out: an omitted value follows diagnose_failures.
func TestAISuggestFixesEnabled_OptOutFollowsDiagnose(t *testing.T) {
	on, off := true, false
	cases := []struct {
		name      string
		diagnose  bool
		suggest   *bool
		wantEnabl bool
	}{
		{"omitted follows diagnose on", true, nil, true},
		{"omitted follows diagnose off", false, nil, false},
		{"explicit false overrides diagnose on", true, &off, false},
		{"explicit true overrides diagnose off", false, &on, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.AIReview.DiagnoseFailures = tc.diagnose
			cfg.AIReview.SuggestFixes = tc.suggest
			if got := aiSuggestFixesEnabled(cfg); got != tc.wantEnabl {
				t.Errorf("aiSuggestFixesEnabled = %v, want %v", got, tc.wantEnabl)
			}
		})
	}
}

// With both suggest_fixes and --apply-suggested off, the render failure is a
// no-op: no AI splice attempted, the table is not "applied", nothing written.
func TestHandleRenderFailure_DisabledIsNoOp(t *testing.T) {
	o := &Orchestrator{config: &config.Config{}} // SuggestFixes nil/false, ApplySuggested false
	ddl, applied := o.handleRenderFailure(context.Background(), "run1", "rendering CREATE TABLE DDL", createDDLRenderer{},
		&driver.Table{Name: "T", Schema: "dbo"}, errors.New("boom"))
	if applied || ddl != "" {
		t.Errorf("expected no fix applied, got applied=%v ddl=%q", applied, ddl)
	}
	if o.diagnoser != nil {
		t.Error("provider resolved while both features are disabled")
	}
}

// A failure that isn't a single splice-able expression must not be fixed (no
// whole-table rewrite) — even with --apply-suggested on it must abort.
func TestHandleRenderFailure_NonExpressionFailureNotApplied(t *testing.T) {
	o := &Orchestrator{config: &config.Config{}, opts: Options{ApplySuggested: true}}
	// A plain error (not *driver.ExpressionRenderError) can't be spliced.
	ddl, applied := o.handleRenderFailure(context.Background(), "run1", "rendering CREATE TABLE DDL", createDDLRenderer{},
		&driver.Table{Name: "T", Schema: "dbo"}, errors.New("unsupported source type \"geography\""))
	if applied || ddl != "" {
		t.Errorf("non-expression failure must not be applied, got applied=%v", applied)
	}
	if o.diagnoser != nil {
		t.Error("provider resolved for a non-expression failure; should abort")
	}
}

func TestHandleRenderFailure_AppliesMultipleDefaultFixesInOneTable(t *testing.T) {
	renderer := mssqlPostgresCreateRenderer(t)
	table := &driver.Table{
		Schema: "dbo",
		Name:   "SubscriptionWindows",
		Columns: []driver.Column{
			{Name: "Id", DataType: "bigint", IsIdentity: true, IsNullable: false},
			{Name: "StartsAt", DataType: "datetime2", DatetimePrecision: intPtr(3), IsNullable: false,
				DefaultExpression: "(dateadd(day,(7),getdate()))"},
			{Name: "EndsAt", DataType: "datetime2", DatetimePrecision: intPtr(3), IsNullable: false,
				DefaultExpression: "(dateadd(year,(1),getdate()))"},
		},
		PrimaryKey: []string{"Id"},
	}
	_, cause := renderer.renderTable(context.Background(), table)
	if cause == nil {
		t.Fatal("expected initial render to fail on unsupported DATEADD default")
	}

	suggester := &fakeExpressionFixSuggester{fixes: map[string]*driver.ExpressionFix{
		"default\x00StartsAt": {Expression: "CURRENT_TIMESTAMP + INTERVAL '7 days'", Confidence: "high"},
		"default\x00EndsAt":   {Expression: "CURRENT_TIMESTAMP + INTERVAL '1 year'", Confidence: "high"},
	}}
	cfg := &config.Config{}
	cfg.Source.Type = "mssql"
	cfg.Target.Type = "postgres"
	o := &Orchestrator{config: cfg, opts: Options{ApplySuggested: true}, fixSuggester: suggester}

	got, applied := o.handleRenderFailure(context.Background(), "run1", "rendering CREATE TABLE DDL", renderer, table, cause)
	if !applied {
		t.Fatalf("expected multi-default AI splice to apply; ddl=%q", got)
	}
	if strings.Count(got, "AI-ASSISTED FIX (--apply-suggested)") != 2 {
		t.Fatalf("expected two inline AI markers, got:\n%s", got)
	}
	for _, want := range []string{
		`"startsat" timestamp(3) without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP + INTERVAL '7 days'`,
		`"endsat" timestamp(3) without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP + INTERVAL '1 year'`,
		`-- AI-ASSISTED FIX (--apply-suggested): default StartsAt`,
		`-- AI-ASSISTED FIX (--apply-suggested): default EndsAt`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q:\n%s", want, got)
		}
	}
	if len(suggester.calls) != 2 {
		t.Fatalf("AI suggester calls = %d, want 2", len(suggester.calls))
	}
	if suggester.calls[0].ColumnName != "StartsAt" || suggester.calls[1].ColumnName != "EndsAt" {
		t.Fatalf("unexpected call order: %#v", suggester.calls)
	}
	if table.Columns[1].DefaultExpressionOverride != "" || table.Columns[2].DefaultExpressionOverride != "" {
		t.Fatal("handleRenderFailure mutated the original table instead of an in-memory copy")
	}
}

func TestHandleRenderFailure_WritesCombinedSuggestionForMultipleDefaults(t *testing.T) {
	renderer := mssqlPostgresCreateRenderer(t)
	table := &driver.Table{
		Schema: "dbo",
		Name:   "SubscriptionWindows",
		Columns: []driver.Column{
			{Name: "StartsAt", DataType: "datetime2", DatetimePrecision: intPtr(3), IsNullable: false,
				DefaultExpression: "(dateadd(day,(7),getdate()))"},
			{Name: "EndsAt", DataType: "datetime2", DatetimePrecision: intPtr(3), IsNullable: false,
				DefaultExpression: "(dateadd(year,(1),getdate()))"},
		},
	}
	_, cause := renderer.renderTable(context.Background(), table)
	if cause == nil {
		t.Fatal("expected initial render to fail on unsupported DATEADD default")
	}

	suggester := &fakeExpressionFixSuggester{fixes: map[string]*driver.ExpressionFix{
		"default\x00StartsAt": {Expression: "CURRENT_TIMESTAMP + INTERVAL '7 days'", Confidence: "high"},
		"default\x00EndsAt":   {Expression: "CURRENT_TIMESTAMP + INTERVAL '1 year'", Confidence: "medium"},
	}}
	cfg := &config.Config{}
	cfg.Source.Type = "mssql"
	cfg.Target.Type = "postgres"
	cfg.Migration.DataDir = t.TempDir()
	on := true
	cfg.AIReview.SuggestFixes = &on
	o := &Orchestrator{config: cfg, fixSuggester: suggester}

	got, applied := o.handleRenderFailure(context.Background(), "run1", "rendering CREATE TABLE DDL", renderer, table, cause)
	if applied || got != "" {
		t.Fatalf("advisory suggestion must not apply DDL, got applied=%v ddl=%q", applied, got)
	}
	content, err := os.ReadFile(filepath.Join(cfg.Migration.DataDir, "runs", "run1", "ddl", "schema.suggested.sql"))
	if err != nil {
		t.Fatalf("reading schema.suggested.sql: %v", err)
	}
	suggestion := string(content)
	for _, want := range []string{
		"Object 1: StartsAt (default)",
		"Object 2: EndsAt (default)",
		`"startsat" timestamp(3) without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP + INTERVAL '7 days'`,
		`"endsat" timestamp(3) without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP + INTERVAL '1 year'`,
	} {
		if !strings.Contains(suggestion, want) {
			t.Errorf("suggestion file missing %q:\n%s", want, suggestion)
		}
	}
}

func TestHandleRenderFailure_AbortsAtExpressionFixCap(t *testing.T) {
	renderer := mssqlPostgresCreateRenderer(t)
	table := &driver.Table{Schema: "dbo", Name: "TooManyDefaults"}
	table.Columns = append(table.Columns, driver.Column{Name: "Id", DataType: "int", IsNullable: false})

	fixes := map[string]*driver.ExpressionFix{}
	for i := 1; i <= maxAIExpressionFixesPerObject+1; i++ {
		name := fmt.Sprintf("D%d", i)
		table.Columns = append(table.Columns, driver.Column{
			Name:              name,
			DataType:          "datetime2",
			IsNullable:        false,
			DefaultExpression: fmt.Sprintf("(dateadd(day,(%d),getdate()))", i),
		})
		fixes["default\x00"+name] = &driver.ExpressionFix{
			Expression: fmt.Sprintf("CURRENT_TIMESTAMP + INTERVAL '%d days'", i),
			Confidence: "high",
		}
	}
	_, cause := renderer.renderTable(context.Background(), table)
	if cause == nil {
		t.Fatal("expected initial render to fail on unsupported DATEADD default")
	}

	suggester := &fakeExpressionFixSuggester{fixes: fixes}
	cfg := &config.Config{}
	cfg.Source.Type = "mssql"
	cfg.Target.Type = "postgres"
	o := &Orchestrator{config: cfg, opts: Options{ApplySuggested: true}, fixSuggester: suggester}

	got, applied := o.handleRenderFailure(context.Background(), "run1", "rendering CREATE TABLE DDL", renderer, table, cause)
	if applied || got != "" {
		t.Fatalf("expected safety cap abort, got applied=%v ddl=%q", applied, got)
	}
	if len(suggester.calls) != maxAIExpressionFixesPerObject {
		t.Fatalf("AI suggester calls = %d, want cap %d", len(suggester.calls), maxAIExpressionFixesPerObject)
	}
}

func TestHandleRenderFailure_AbortsOnRepeatedExpressionKey(t *testing.T) {
	renderer := mssqlPostgresCreateRenderer(t)
	table := &driver.Table{
		Schema: "dbo",
		Name:   "RepeatedDefaultKey",
		Columns: []driver.Column{
			{Name: "StartsAt", DataType: "datetime2", IsNullable: false,
				DefaultExpression: "(dateadd(day,(7),getdate()))"},
			{Name: "StartsAt", DataType: "datetime2", IsNullable: false,
				DefaultExpression: "(dateadd(year,(1),getdate()))"},
		},
	}
	_, cause := renderer.renderTable(context.Background(), table)
	if cause == nil {
		t.Fatal("expected initial render to fail on unsupported DATEADD default")
	}

	suggester := &fakeExpressionFixSuggester{fixes: map[string]*driver.ExpressionFix{
		"default\x00StartsAt": {Expression: "CURRENT_TIMESTAMP + INTERVAL '7 days'", Confidence: "high"},
	}}
	cfg := &config.Config{}
	cfg.Source.Type = "mssql"
	cfg.Target.Type = "postgres"
	o := &Orchestrator{config: cfg, opts: Options{ApplySuggested: true}, fixSuggester: suggester}

	got, applied := o.handleRenderFailure(context.Background(), "run1", "rendering CREATE TABLE DDL", renderer, table, cause)
	if applied || got != "" {
		t.Fatalf("expected repeated expression key abort, got applied=%v ddl=%q", applied, got)
	}
	if len(suggester.calls) != 1 {
		t.Fatalf("AI suggester calls = %d, want 1 before repeated-key abort", len(suggester.calls))
	}
}

// Enabled but no usable AI provider must degrade gracefully: no emit, no panic,
// and the original error path is unaffected (the caller still returns it).
func TestDiagnoseSchemaFailure_EnabledNoProviderDegradesGracefully(t *testing.T) {
	emitted := withDiagnosisHandler(t)
	cfg := &config.Config{}
	cfg.AIReview.DiagnoseFailures = true
	cfg.AIReview.Model = "this-provider-does-not-exist"
	o := &Orchestrator{config: cfg}

	// Must not panic; with no resolvable provider nothing is emitted.
	o.diagnoseSchemaFailure(context.Background(), "Employees", "dbo", "rendering CREATE TABLE DDL", errors.New("boom"))

	if *emitted {
		t.Error("diagnosis emitted despite no usable provider")
	}
}

func intPtr(v int) *int { return &v }
