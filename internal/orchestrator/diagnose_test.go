package orchestrator

import (
	"context"
	"errors"
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
