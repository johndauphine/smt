package driver

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"smt/internal/secrets"
)

// TestResolveVerifierMappers_Nil pins the no-override path: with no verifier
// mapper configured, both return values are nil so writer callsites fall back
// to the default reviewer, when one is configured.
func TestResolveVerifierMappers_Nil(t *testing.T) {
	tm, fm := ResolveVerifierMappers(WriterOptions{})
	if tm != nil {
		t.Errorf("expected nil TableDDLReviewer, got %T", tm)
	}
	if fm != nil {
		t.Errorf("expected nil FinalizationDDLReviewer, got %T", fm)
	}
}

// TestResolveVerifierMappers_PopulatedFromAITypeMapper pins the
// cross-model-verify happy path: when WriterOptions.VerifierTypeMapper is an
// *AITypeMapper (which implements both reviewer interfaces), both fields
// come back non-nil and pointing at the
// same underlying mapper.
func TestResolveVerifierMappers_PopulatedFromAITypeMapper(t *testing.T) {
	verifier := &AITypeMapper{providerName: "anthropic", timeoutSeconds: 60}

	tm, fm := ResolveVerifierMappers(WriterOptions{VerifierTypeMapper: verifier})
	if tm == nil {
		t.Fatal("expected non-nil TableDDLReviewer, got nil")
	}
	if fm == nil {
		t.Fatal("expected non-nil FinalizationDDLReviewer, got nil")
	}
	// Identity check — both should be the same underlying mapper, not freshly
	// instantiated. Otherwise verifier-side cache and inflight-request state
	// would diverge between the table and finalization paths.
	if any(tm) != any(verifier) {
		t.Errorf("TableDDLReviewer not the configured verifier (got %p, want %p)", tm, verifier)
	}
	if any(fm) != any(verifier) {
		t.Errorf("FinalizationDDLReviewer not the configured verifier (got %p, want %p)", fm, verifier)
	}
}

// TestResolveVerifierMappers_PartialInterface asserts that a verifier which
// implements only TableDDLReviewer (not FinalizationDDLReviewer) leaves the
// finalization slot nil.
func TestResolveVerifierMappers_PartialInterface(t *testing.T) {
	// fakeTableOnly implements TableDDLReviewer but NOT FinalizationDDLReviewer.
	// Used to verify the type-assertion-without-panic contract: a partial
	// implementer leaves the unimplemented slot nil rather than blowing up.
	verifier := &fakeTableOnlyMapper{}
	tm, fm := ResolveVerifierMappers(WriterOptions{VerifierTypeMapper: verifier})
	if tm == nil {
		t.Error("expected non-nil TableDDLReviewer from a TableDDLReviewer-only verifier")
	}
	if fm != nil {
		t.Errorf("expected nil FinalizationDDLReviewer, got %T", fm)
	}
}

// fakeTableOnlyMapper implements just enough of TypeMapper + TableDDLReviewer
// to flow through ResolveVerifierMappers's type assertions.
// Methods panic — they're not exercised by the partial-interface test.
type fakeTableOnlyMapper struct{}

func (f *fakeTableOnlyMapper) MapType(TypeInfo) string                   { panic("unused") }
func (f *fakeTableOnlyMapper) MapTypeWithError(TypeInfo) (string, error) { panic("unused") }
func (f *fakeTableOnlyMapper) CanMap(string, string) bool                { return true }
func (f *fakeTableOnlyMapper) SupportedTargets() []string                { return []string{"*"} }
func (f *fakeTableOnlyMapper) VerifyTableDDL(context.Context, VerifyTableDDLRequest) (*VerifyResult, error) {
	panic("unused")
}

// writeTestSecrets drops a minimal secrets file at a temp path and points
// SMT_SECRETS_FILE at it for the duration of the calling test. The fixture
// has one cloud provider ("anthropic") and one local provider ("ollama") so
// both code paths in NewAITypeMapper are reachable.
func writeTestSecrets(t *testing.T) {
	t.Helper()
	tmp := t.TempDir()
	path := filepath.Join(tmp, "secrets.yaml")
	const content = `
ai:
  default_provider: anthropic
  providers:
    anthropic:
      api_key: "test-key"
      model: "claude-haiku-4-5-20251001"
    ollama:
      base_url: "http://localhost:11434"
      model: "llama3"
encryption:
  master_key: "test-master-key"
`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("write fixture secrets: %v", err)
	}
	t.Setenv(secrets.SecretsFileEnvVar, path)
	secrets.Reset()
	t.Cleanup(secrets.Reset)
}

// TestNewAITypeMapperByName_UnknownProviderFails pins the orchestrator-side
// validation contract: when migration.ai_verifier_model names a provider
// that's not in the secrets file, NewAITypeMapperByName must fail with an
// error mentioning the bad name. The orchestrator wraps the error and aborts
// startup before any DDL runs, which is the design intent of #48 (catch
// typos at startup, not mid-run).
func TestNewAITypeMapperByName_UnknownProviderFails(t *testing.T) {
	writeTestSecrets(t)

	_, err := NewAITypeMapperByName("not-a-real-provider")
	if err == nil {
		t.Fatal("expected error for unknown provider name, got nil")
	}
	if !strings.Contains(err.Error(), "not-a-real-provider") {
		t.Errorf("expected error to mention the bad name, got: %v", err)
	}
}

// TestNewAITypeMapperByName_KnownProviderSucceeds is the happy path — a
// provider that exists in the secrets file constructs successfully and the
// resulting mapper carries the right identity (providerName == YAML key).
func TestNewAITypeMapperByName_KnownProviderSucceeds(t *testing.T) {
	writeTestSecrets(t)

	mapper, err := NewAITypeMapperByName("anthropic")
	if err != nil {
		t.Fatalf("expected success for known provider, got error: %v", err)
	}
	if mapper.ProviderName() != "anthropic" {
		t.Errorf("ProviderName() = %q, want %q", mapper.ProviderName(), "anthropic")
	}
}
