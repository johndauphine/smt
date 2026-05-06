package postgres

import (
	"context"
	"testing"

	"smt/internal/driver"
)

// TestTableVerifier_FallsBackToGenerator pins Phase 1 behavior: with no
// verifierTableMapper set, the helper returns the generator mapper so verify
// runs against the same model that produced the DDL.
func TestTableVerifier_FallsBackToGenerator(t *testing.T) {
	gen := &fakeTableMapper{name: "gen"}
	w := &Writer{tableMapper: gen}

	got := w.tableVerifier()
	if got == nil {
		t.Fatal("tableVerifier returned nil")
	}
	if got.(*fakeTableMapper).name != "gen" {
		t.Errorf("expected generator mapper (gen), got %s", got.(*fakeTableMapper).name)
	}
}

// TestTableVerifier_PrefersVerifierWhenSet is the cross-model-verify selector
// — when verifierTableMapper is non-nil it must be returned in preference to
// the generator. This is the load-bearing assertion for #48: without it, the
// verify hook would still call the cheap local generator instead of the
// strong cloud auditor.
func TestTableVerifier_PrefersVerifierWhenSet(t *testing.T) {
	gen := &fakeTableMapper{name: "gen"}
	verifier := &fakeTableMapper{name: "verifier"}
	w := &Writer{tableMapper: gen, verifierTableMapper: verifier}

	got := w.tableVerifier()
	if got.(*fakeTableMapper).name != "verifier" {
		t.Errorf("expected verifier mapper, got %s — verify call would still hit generator", got.(*fakeTableMapper).name)
	}
}

// TestFinalizationVerifier_FallsBackToGenerator and ...PrefersVerifierWhenSet
// mirror the table-DDL tests for finalization (index/FK/CHECK) DDL — the
// same selection helper covers all three finalize phases since they share
// retryFinalize.
func TestFinalizationVerifier_FallsBackToGenerator(t *testing.T) {
	gen := &fakeFinalizationMapper{name: "gen"}
	w := &Writer{finalizationMapper: gen}

	got := w.finalizationVerifier()
	if got == nil {
		t.Fatal("finalizationVerifier returned nil")
	}
	if got.(*fakeFinalizationMapper).name != "gen" {
		t.Errorf("expected generator mapper, got %s", got.(*fakeFinalizationMapper).name)
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

// fakeTableMapper / fakeFinalizationMapper are minimal interface implementers
// that record an identifying name. Methods panic — they exist only to satisfy
// the type system; the selection helpers don't invoke them.
type fakeTableMapper struct{ name string }

func (f *fakeTableMapper) GenerateTableDDL(context.Context, driver.TableDDLRequest) (*driver.TableDDLResponse, error) {
	panic("unused")
}
func (f *fakeTableMapper) CanMap(string, string) bool                   { return true }
func (f *fakeTableMapper) CacheTableDDL(driver.TableDDLRequest, string) { panic("unused") }
func (f *fakeTableMapper) VerifyTableDDL(context.Context, driver.VerifyTableDDLRequest) (*driver.VerifyResult, error) {
	panic("unused")
}

type fakeFinalizationMapper struct{ name string }

func (f *fakeFinalizationMapper) GenerateFinalizationDDL(context.Context, driver.FinalizationDDLRequest) (*driver.FinalizationDDLResponse, error) {
	panic("unused")
}
func (f *fakeFinalizationMapper) CacheFinalizationDDL(driver.FinalizationDDLRequest, string) {
	panic("unused")
}
func (f *fakeFinalizationMapper) VerifyFinalizationDDL(context.Context, driver.VerifyFinalizationDDLRequest) (*driver.VerifyResult, error) {
	panic("unused")
}
