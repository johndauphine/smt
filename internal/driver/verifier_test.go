package driver

import (
	"context"
	"testing"
)

// TestResolveVerifierMappers_Nil pins the no-override path: with no verifier
// mapper configured, both return values are nil so writer callsites fall back
// to the generator mappers (preserving Phase 1 same-model verify behavior).
func TestResolveVerifierMappers_Nil(t *testing.T) {
	tm, fm := ResolveVerifierMappers(WriterOptions{})
	if tm != nil {
		t.Errorf("expected nil TableTypeMapper, got %T", tm)
	}
	if fm != nil {
		t.Errorf("expected nil FinalizationDDLMapper, got %T", fm)
	}
}

// TestResolveVerifierMappers_PopulatedFromAITypeMapper pins the
// cross-model-verify happy path: when WriterOptions.VerifierTypeMapper is an
// *AITypeMapper (which implements both TableTypeMapper and
// FinalizationDDLMapper), both fields come back non-nil and pointing at the
// same underlying mapper.
func TestResolveVerifierMappers_PopulatedFromAITypeMapper(t *testing.T) {
	verifier := &AITypeMapper{providerName: "anthropic", timeoutSeconds: 60}

	tm, fm := ResolveVerifierMappers(WriterOptions{VerifierTypeMapper: verifier})
	if tm == nil {
		t.Fatal("expected non-nil TableTypeMapper, got nil")
	}
	if fm == nil {
		t.Fatal("expected non-nil FinalizationDDLMapper, got nil")
	}
	// Identity check — both should be the same underlying mapper, not freshly
	// instantiated. Otherwise verifier-side cache and inflight-request state
	// would diverge between the table and finalization paths.
	if any(tm) != any(verifier) {
		t.Errorf("TableTypeMapper not the configured verifier (got %p, want %p)", tm, verifier)
	}
	if any(fm) != any(verifier) {
		t.Errorf("FinalizationDDLMapper not the configured verifier (got %p, want %p)", fm, verifier)
	}
}

// TestResolveVerifierMappers_PartialInterface asserts that a verifier which
// implements only TableTypeMapper (not FinalizationDDLMapper) leaves the
// finalization slot nil — callsites then fall back to the generator's
// finalization mapper for finalize-phase verify, which is the correct
// graceful-degradation behavior.
func TestResolveVerifierMappers_PartialInterface(t *testing.T) {
	// fakeTableOnly implements TableTypeMapper but NOT FinalizationDDLMapper.
	// Used to verify the type-assertion-without-panic contract: a partial
	// implementer leaves the unimplemented slot nil rather than blowing up.
	verifier := &fakeTableOnlyMapper{}
	tm, fm := ResolveVerifierMappers(WriterOptions{VerifierTypeMapper: verifier})
	if tm == nil {
		t.Error("expected non-nil TableTypeMapper from a TableTypeMapper-only verifier")
	}
	if fm != nil {
		t.Errorf("expected nil FinalizationDDLMapper, got %T", fm)
	}
}

// fakeTableOnlyMapper implements just enough of TypeMapper +
// TableTypeMapper to flow through ResolveVerifierMappers's type assertions.
// Methods panic — they're not exercised by the partial-interface test.
type fakeTableOnlyMapper struct{}

func (f *fakeTableOnlyMapper) MapType(TypeInfo) string                   { panic("unused") }
func (f *fakeTableOnlyMapper) MapTypeWithError(TypeInfo) (string, error) { panic("unused") }
func (f *fakeTableOnlyMapper) CanMap(string, string) bool                { return true }
func (f *fakeTableOnlyMapper) SupportedTargets() []string                { return []string{"*"} }
func (f *fakeTableOnlyMapper) GenerateTableDDL(context.Context, TableDDLRequest) (*TableDDLResponse, error) {
	panic("unused")
}
func (f *fakeTableOnlyMapper) CacheTableDDL(TableDDLRequest, string) { panic("unused") }
func (f *fakeTableOnlyMapper) VerifyTableDDL(context.Context, VerifyTableDDLRequest) (*VerifyResult, error) {
	panic("unused")
}
