package checkpoint

import "testing"

// #90 — run kind is persisted and surfaced so history can distinguish
// generate-only previews from runs that executed DDL.
func TestRunKindPersisted(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	if err := state.CreateRun("gen-run", RunKindGenerate, "dbo", "public", nil, "", ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if err := state.CreateRun("apply-run", RunKindApply, "dbo", "public", nil, "", ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if err := state.CreateRun("legacy-run", "", "dbo", "public", nil, "", ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	for id, want := range map[string]string{
		"gen-run":    RunKindGenerate,
		"apply-run":  RunKindApply,
		"legacy-run": RunKindApply,
	} {
		r, err := state.GetRunByID(id)
		if err != nil || r == nil {
			t.Fatalf("GetRunByID(%s): %v %v", id, r, err)
		}
		if r.Kind != want {
			t.Errorf("run %s kind = %q, want %q", id, r.Kind, want)
		}
	}

	runs, err := state.GetAllRuns()
	if err != nil {
		t.Fatalf("GetAllRuns: %v", err)
	}
	if len(runs) != 3 {
		t.Fatalf("GetAllRuns returned %d runs, want 3", len(runs))
	}
	for _, r := range runs {
		if r.Kind == "" {
			t.Errorf("run %s has empty kind", r.ID)
		}
	}
}
