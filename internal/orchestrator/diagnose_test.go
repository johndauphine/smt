package orchestrator

import (
	"context"
	"errors"
	"strings"
	"testing"

	"smt/internal/config"
	"smt/internal/driver"
)

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
// show exactly which one expression came from the AI, and contain SMT's
// deterministic DDL — so a user can never confuse it with schema.sql.
func TestRenderSuggestionFile_LabeledAndProvenanced(t *testing.T) {
	out := renderSuggestionFile(
		"dbo.Subscriptions",
		&driver.ExpressionRenderError{Column: "ExpiresAt", Kind: "default", SourceExpr: "dateadd(year,1,getdate())"},
		&driver.ExpressionFix{Expression: "CURRENT_TIMESTAMP + INTERVAL '1 year'", Explanation: "interval arithmetic", Confidence: "high"},
		`CREATE TABLE "subscriptions" ("expiresat" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP + INTERVAL '1 year')`,
		false, // class not confirmed
	)
	for _, want := range []string{
		"AI-ASSISTED", "review before applying", "did not and will not apply it",
		"dbo.Subscriptions", "ExpiresAt (default)",
		"dateadd(year,1,getdate())  ->  CURRENT_TIMESTAMP + INTERVAL '1 year'",
		"Confidence: high", `CREATE TABLE "subscriptions"`,
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
		&driver.ExpressionRenderError{Column: "d", Kind: "default", SourceExpr: "(convert(date,getdate()))"},
		&driver.ExpressionFix{Expression: "CURRENT_DATE", Confidence: "high"},
		"CREATE TABLE t (d date)", true)
	if !strings.Contains(out, "[OK]") || strings.Contains(out, "[REVIEW]") {
		t.Errorf("expected [OK] verdict, got:\n%s", out)
	}
}

// A multi-line AI/source string must not break out of the single-line "-- "
// banner comments — every line above the DDL body stays a comment.
func TestRenderSuggestionFile_SanitizesMultilineBannerFields(t *testing.T) {
	out := renderSuggestionFile(
		"T",
		&driver.ExpressionRenderError{Column: "c", Kind: "default", SourceExpr: "x\nDROP TABLE users; --"},
		&driver.ExpressionFix{Expression: "1\nDELETE FROM secrets;", Explanation: "ok\nrm -rf /", Confidence: "low"},
		"CREATE TABLE t (c int)",
		false,
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

// suggest_fixes off is a no-op: no provider resolved, nothing written.
func TestSuggestSchemaFix_DisabledIsNoOp(t *testing.T) {
	o := &Orchestrator{config: &config.Config{}} // SuggestFixes defaults false
	o.suggestSchemaFix(context.Background(), "run1", createDDLRenderer{},
		&driver.Table{Name: "T", Schema: "dbo"}, errors.New("boom"))
	if o.diagnoser != nil {
		t.Error("provider resolved while suggest_fixes is disabled")
	}
}

// A failure that isn't a single splice-able expression must not produce a
// suggestion (no whole-table rewrite) — even with suggest_fixes on.
func TestSuggestSchemaFix_NonExpressionFailureNoSuggestion(t *testing.T) {
	cfg := &config.Config{}
	on := true
	cfg.AIReview.SuggestFixes = &on
	o := &Orchestrator{config: cfg}
	// A plain error (not *driver.ExpressionRenderError) must short-circuit
	// before resolving a provider.
	o.suggestSchemaFix(context.Background(), "run1", createDDLRenderer{},
		&driver.Table{Name: "T", Schema: "dbo"}, errors.New("unsupported source type \"geography\""))
	if o.diagnoser != nil {
		t.Error("provider resolved for a non-expression failure; should be diagnosis-only")
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
