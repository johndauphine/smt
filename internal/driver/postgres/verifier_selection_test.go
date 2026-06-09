package postgres

import (
	"context"
	"testing"

	"smt/internal/driver"
)

// TestTableVerifier_FallsBackToDefault pins the default-reviewer behavior:
// with no verifierTableMapper set, the helper returns the configured fallback
// reviewer.
func TestTableVerifier_FallsBackToGenerator(t *testing.T) {
	gen := &fakeTableMapper{name: "gen"}
	w := &Writer{tableMapper: gen}

	got := w.tableVerifier()
	if got == nil {
		t.Fatal("tableVerifier returned nil")
	}
	if got.(*fakeTableMapper).name != "gen" {
		t.Errorf("expected fallback reviewer (gen), got %s", got.(*fakeTableMapper).name)
	}
}

// TestTableVerifier_PrefersVerifierWhenSet is the explicit-reviewer selector
// — when verifierTableMapper is non-nil it must be returned in preference to
// the fallback reviewer.
func TestTableVerifier_PrefersVerifierWhenSet(t *testing.T) {
	gen := &fakeTableMapper{name: "gen"}
	verifier := &fakeTableMapper{name: "verifier"}
	w := &Writer{tableMapper: gen, verifierTableMapper: verifier}

	got := w.tableVerifier()
	if got.(*fakeTableMapper).name != "verifier" {
		t.Errorf("expected verifier mapper, got %s", got.(*fakeTableMapper).name)
	}
}

// TestFinalizationVerifier_FallsBackToGenerator and ...PrefersVerifierWhenSet
// mirror the table-DDL tests for finalization (index/FK/CHECK) DDL.
func TestFinalizationVerifier_FallsBackToGenerator(t *testing.T) {
	gen := &fakeFinalizationMapper{name: "gen"}
	w := &Writer{finalizationMapper: gen}

	got := w.finalizationVerifier()
	if got == nil {
		t.Fatal("finalizationVerifier returned nil")
	}
	if got.(*fakeFinalizationMapper).name != "gen" {
		t.Errorf("expected fallback reviewer, got %s", got.(*fakeFinalizationMapper).name)
	}
}

func TestFinalizationVerifier_PrefersVerifierWhenSet(t *testing.T) {
	gen := &fakeFinalizationMapper{name: "gen"}
	verifier := &fakeFinalizationMapper{name: "verifier"}
	w := &Writer{finalizationMapper: gen, verifierFinalizationMapper: verifier}

	got := w.finalizationVerifier()
	if got.(*fakeFinalizationMapper).name != "verifier" {
		t.Errorf("expected verifier mapper, got %s", got.(*fakeFinalizationMapper).name)
	}
}

// fakeTableMapper / fakeFinalizationMapper are minimal reviewer implementers
// that record an identifying name. Verify methods panic because the selection
// helpers don't invoke them.
type fakeTableMapper struct{ name string }

func (f *fakeTableMapper) VerifyTableDDL(context.Context, driver.VerifyTableDDLRequest) (*driver.VerifyResult, error) {
	panic("unused")
}

type fakeFinalizationMapper struct{ name string }

func (f *fakeFinalizationMapper) VerifyFinalizationDDL(context.Context, driver.VerifyFinalizationDDLRequest) (*driver.VerifyResult, error) {
	panic("unused")
}
