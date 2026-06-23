package orchestrator

// AI review contract tests (#68). The deterministic renderer owns executable
// DDL; the optional reviewer can only inspect it and return a verdict. These
// tests pin the orchestrator-level contract — verdict handling per mode,
// fail-closed on provider failure, no provider contact when disabled, and the
// structural guarantee that review cannot rewrite DDL. The AI parse layer
// (bad JSON / missing fields) is covered separately in driver/ai_verify_test.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"smt/internal/config"
	"smt/internal/driver"
)

// fakeReviewer implements both reviewer interfaces with scripted behavior and
// records whether it was contacted.
type fakeReviewer struct {
	verdict *driver.VerifyResult
	err     error
	calls   int
}

func (f *fakeReviewer) VerifyTableDDL(ctx context.Context, req driver.VerifyTableDDLRequest) (*driver.VerifyResult, error) {
	f.calls++
	return f.verdict, f.err
}

func (f *fakeReviewer) VerifyFinalizationDDL(ctx context.Context, req driver.VerifyFinalizationDDLRequest) (*driver.VerifyResult, error) {
	f.calls++
	return f.verdict, f.err
}

func reviewerFor(f *fakeReviewer, enabled bool, mode string) createDDLRenderer {
	return createDDLRenderer{
		sourceType:           "mssql",
		targetType:           "postgres",
		targetSchema:         "public",
		aiReviewEnabled:      enabled,
		aiReviewMode:         mode,
		reviewWarnings:       &reviewWarningRecorder{},
		tableVerifier:        f,
		finalizationVerifier: f,
	}
}

func sampleTable() *driver.Table {
	return &driver.Table{Schema: "dbo", Name: "Users", Columns: []driver.Column{{Name: "Id", DataType: "int"}}}
}

func TestReview_DisabledContactsNoProvider(t *testing.T) {
	f := &fakeReviewer{verdict: &driver.VerifyResult{OK: true}}
	r := reviewerFor(f, false, "fail")
	if err := r.reviewTable(context.Background(), sampleTable(), "CREATE TABLE ..."); err != nil {
		t.Fatalf("disabled review returned error: %v", err)
	}
	if f.calls != 0 {
		t.Errorf("disabled review contacted the provider %d time(s)", f.calls)
	}
}

func TestReview_VerdictHandlingPerMode(t *testing.T) {
	cases := []struct {
		name    string
		verdict *driver.VerifyResult
		mode    string
		wantErr bool
	}{
		{"pass blocks nothing", &driver.VerifyResult{OK: true}, "fail", false},
		{"nil verdict fails closed even in warn mode", nil, "warn", true},
		{"warn continues on issues", &driver.VerifyResult{OK: false, Issues: []string{"x"}}, "warn", false},
		{"fail blocks on issues", &driver.VerifyResult{OK: false, Issues: []string{"x"}}, "fail", true},
		{"warn continues even with issues + empty mode defaults to warn", &driver.VerifyResult{OK: false, Issues: []string{"x"}}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeReviewer{verdict: tc.verdict}
			r := reviewerFor(f, true, tc.mode)
			err := r.reviewTable(context.Background(), sampleTable(), "CREATE TABLE ...")
			if tc.wantErr != (err != nil) {
				t.Fatalf("reviewTable err=%v, wantErr=%v", err, tc.wantErr)
			}
			if f.calls != 1 {
				t.Errorf("expected exactly one provider call, got %d", f.calls)
			}
		})
	}
}

// Provider failure (timeout / unavailable) must fail closed — the review
// returns an error so apply is blocked — regardless of warn/fail mode.
func TestReview_ProviderFailureFailsClosed(t *testing.T) {
	for _, mode := range []string{"warn", "fail"} {
		t.Run(mode, func(t *testing.T) {
			f := &fakeReviewer{err: errors.New("provider timeout")}
			r := reviewerFor(f, true, mode)
			err := r.reviewTable(context.Background(), sampleTable(), "CREATE TABLE ...")
			if err == nil {
				t.Fatalf("provider failure in mode %q did not surface an error", mode)
			}
			if !strings.Contains(err.Error(), "provider timeout") {
				t.Errorf("error did not wrap the provider failure: %v", err)
			}
		})
	}
}

// Enabled review with no configured reviewer is a misconfiguration that must
// fail loudly rather than silently skip inspection.
func TestReview_EnabledWithoutReviewerErrors(t *testing.T) {
	r := createDDLRenderer{aiReviewEnabled: true, aiReviewMode: "warn"} // nil verifiers
	if err := r.reviewTable(context.Background(), sampleTable(), "ddl"); err == nil {
		t.Error("enabled review with nil table reviewer should error")
	}
	if err := r.reviewFinalization(context.Background(), driver.DDLTypeIndex, sampleTable(), &driver.Index{Name: "ix"}, nil, nil, "ddl"); err == nil {
		t.Error("enabled review with nil finalization reviewer should error")
	}
}

// The reviewer can only return a verdict (OK + issues); it has no channel to
// return modified DDL. This pins the "review cannot silently rewrite
// executable DDL" guarantee structurally — a warn verdict leaves the caller's
// DDL string untouched.
func TestReview_CannotRewriteDDL(t *testing.T) {
	const ddl = "CREATE TABLE original (...)"
	f := &fakeReviewer{verdict: &driver.VerifyResult{OK: false, Issues: []string{"prefers something else"}}}
	r := reviewerFor(f, true, "warn")
	// reviewTable returns only an error; the DDL it was given is the DDL the
	// caller keeps. A warn verdict returns nil, so the caller proceeds with
	// the unchanged string.
	if err := r.reviewTable(context.Background(), sampleTable(), ddl); err != nil {
		t.Fatalf("warn verdict should not block: %v", err)
	}
	// VerifyResult carries no DDL field — there is no API path for the
	// reviewer to substitute DDL. Asserted by construction here.
	var vr driver.VerifyResult = *f.verdict
	_ = vr.OK
	_ = vr.Issues
}

func TestReview_WarnModeRecordsWarningsForManifest(t *testing.T) {
	f := &fakeReviewer{verdict: &driver.VerifyResult{OK: false, Issues: []string{"missing index", "nullable changed"}}}
	r := reviewerFor(f, true, "warn")
	if err := r.reviewTable(context.Background(), sampleTable(), "CREATE TABLE ..."); err != nil {
		t.Fatalf("warn verdict should not block: %v", err)
	}
	warnings := r.reviewWarnings.Snapshot()
	if len(warnings) != 1 {
		t.Fatalf("recorded warnings = %d, want 1", len(warnings))
	}
	if warnings[0].Label != "table dbo.Users" {
		t.Fatalf("warning label = %q", warnings[0].Label)
	}
	if warnings[0].Method != reviewMethodDeterministicComparator {
		t.Fatalf("warning method = %q, want %q", warnings[0].Method, reviewMethodDeterministicComparator)
	}
	if got := strings.Join(warnings[0].Issues, "|"); got != "missing index|nullable changed" {
		t.Fatalf("warning issues = %q", got)
	}

	blob, err := json.Marshal(runManifest{AIReviewWarnings: warnings})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if !strings.Contains(string(blob), "ai_review_warnings") {
		t.Fatalf("manifest JSON did not include ai_review_warnings: %s", blob)
	}
}

func TestReview_FinalizationWarnModeRecordsFreeTextAuditor(t *testing.T) {
	f := &fakeReviewer{verdict: &driver.VerifyResult{OK: false, Issues: []string{"predicate may differ"}}}
	r := reviewerFor(f, true, "warn")
	err := r.reviewFinalization(
		context.Background(),
		driver.DDLTypeCheckConstraint,
		sampleTable(),
		nil,
		nil,
		&driver.CheckConstraint{Name: "ck_users_active"},
		"ALTER TABLE users ADD CONSTRAINT ck_users_active CHECK (active IN (0, 1))",
	)
	if err != nil {
		t.Fatalf("warn verdict should not block: %v", err)
	}
	warnings := r.reviewWarnings.Snapshot()
	if len(warnings) != 1 {
		t.Fatalf("recorded warnings = %d, want 1", len(warnings))
	}
	if warnings[0].Method != reviewMethodFreeTextAuditor {
		t.Fatalf("warning method = %q, want %q", warnings[0].Method, reviewMethodFreeTextAuditor)
	}
	if !strings.Contains(warnings[0].Label, string(driver.DDLTypeCheckConstraint)) {
		t.Fatalf("warning label = %q, want check-constraint label", warnings[0].Label)
	}
}

func TestReview_FailModeDoesNotRecordWarningsBeforeBlocking(t *testing.T) {
	f := &fakeReviewer{verdict: &driver.VerifyResult{OK: false, Issues: []string{"bad DDL"}}}
	r := reviewerFor(f, true, "fail")
	if err := r.reviewTable(context.Background(), sampleTable(), "CREATE TABLE ..."); err == nil {
		t.Fatal("fail verdict should block")
	}
	if got := r.reviewWarnings.Snapshot(); len(got) != 0 {
		t.Fatalf("fail-mode warnings recorded despite blocking: %#v", got)
	}
}

// handleReviewVerdict is the shared decision point; pin it directly too.
func TestHandleReviewVerdict(t *testing.T) {
	if err := handleReviewVerdict("fail", "t", &driver.VerifyResult{OK: true}); err != nil {
		t.Errorf("OK verdict must not error: %v", err)
	}
	if err := handleReviewVerdict("warn", "t", nil); err == nil {
		t.Error("nil verdict must fail closed (even in warn mode), not pass")
	}
	if err := handleReviewVerdict("warn", "t", &driver.VerifyResult{OK: false, Issues: []string{"a"}}); err != nil {
		t.Errorf("warn mode must not error on issues: %v", err)
	}
	if err := handleReviewVerdict("fail", "t", &driver.VerifyResult{OK: false, Issues: []string{"a", "b"}}); err == nil {
		t.Error("fail mode must error on issues")
	}
}

// aiReviewEnabled is the gate the "ai_review.enabled: false runs the full
// deterministic path with no provider" guarantee depends on. Omitted (nil)
// and explicit false both mean disabled; only explicit true enables.
func TestAIReviewEnabledGate(t *testing.T) {
	tru, fls := true, false
	cases := []struct {
		name string
		val  *bool
		want bool
	}{
		{"omitted", nil, false},
		{"explicit false", &fls, false},
		{"explicit true", &tru, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.AIReview.Enabled = tc.val
			if got := aiReviewEnabled(cfg); got != tc.want {
				t.Errorf("aiReviewEnabled = %v, want %v", got, tc.want)
			}
		})
	}
}
