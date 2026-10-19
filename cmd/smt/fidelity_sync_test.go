package main

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/smt/internal/config"
	"github.com/johndauphine/smt/internal/driver"
	"github.com/johndauphine/smt/internal/fidelity"
	"github.com/johndauphine/smt/internal/schemadiff"
)

func TestFidelitySnapshotSyncRefusesUnknownTargetStateEvenForNoChanges(t *testing.T) {
	snapshot := schemadiff.Snapshot{Tables: []driver.Table{{Name: "limits", Columns: []driver.Column{{Name: "byte", DataType: "tinyint"}}}}}
	for _, profile := range []string{"checked", "strict"} {
		cfg := &config.Config{}
		cfg.SchemaGeneration.FidelityProfile = profile
		diff, plan, err := buildSnapshotSyncPlan(snapshot, snapshot, cfg)
		var unsupported *schemadiff.FidelityEvolutionError
		if !errors.As(err, &unsupported) || !diff.IsEmpty() || !plan.IsEmpty() {
			t.Fatalf("%s snapshot no-op bypassed target-state assessment: %#v, %#v, %v", profile, diff, plan, err)
		}
	}
	cfg := &config.Config{}
	diff, plan, err := buildSnapshotSyncPlan(snapshot, snapshot, cfg)
	if err != nil || !diff.IsEmpty() || !plan.IsEmpty() {
		t.Fatalf("legacy snapshot no-op changed: %#v, %#v, %v", diff, plan, err)
	}
}

func TestFidelitySyncManifestAndLegacyFingerprint(t *testing.T) {
	validated := true
	snapshot := schemadiff.Snapshot{SourceType: "mssql", Schema: "dbo", Tables: []driver.Table{{Schema: "dbo", Name: "limits", Columns: []driver.Column{{Name: "byte", DataType: "tinyint"}, {Name: "bytes", DataType: "varbinary", MaxLength: 16}}}}}
	snapshot.Tables[0].CheckConstraints = []driver.CheckConstraint{{Name: "ck_user", Definition: "byte >= 0"}}
	withoutEvidence, err := syncSnapshotFingerprint(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Tables[0].CheckConstraints[0].Validated = &validated
	withEvidence, err := syncSnapshotFingerprint(snapshot)
	if err != nil || withoutEvidence != withEvidence || snapshot.Tables[0].CheckConstraints[0].Validated == nil {
		t.Fatalf("legacy validation evidence changed fingerprint or mutated input: %s, %s, %v", withoutEvidence, withEvidence, err)
	}
	cfg := &config.Config{}
	cfg.Source.Type, cfg.Target.Type = "mssql", "postgres"
	plan := schemadiff.Plan{Statements: []schemadiff.Statement{{SQL: "same SQL"}}}
	legacy, err := buildSyncRunManifest(plan, syncApplyOptions{cfg: cfg, currSnap: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(legacy)
	if strings.Contains(string(encoded), "fidelity_") || strings.Contains(string(encoded), "source_semantics_version") {
		t.Fatalf("legacy manifest gained opt-in fields: %s", encoded)
	}
	cfg.SchemaGeneration.FidelityProfile = "checked"
	checked, err := buildSyncRunManifest(plan, syncApplyOptions{cfg: cfg, currSnap: snapshot})
	if err != nil || checked.FidelityProfile != "checked" || checked.SourceSemanticsVersion != fidelity.SemanticsVersion || len(checked.FidelityRequirements) != 2 || len(checked.FidelityFindings) == 0 || checked.PlanFingerprint == legacy.PlanFingerprint || checked.SourceSnapshotFingerprint == legacy.SourceSnapshotFingerprint {
		t.Fatalf("opt-in manifest missing assessment or profile fingerprint: %#v, %v", checked, err)
	}
}

func TestFidelityFlagsOnSyncAndDrift(t *testing.T) {
	for _, command := range []struct {
		name string
	}{
		{name: "sync"}, {name: "drift"},
	} {
		var names [][]string
		if command.name == "sync" {
			for _, flag := range syncCommand().Flags {
				names = append(names, flag.Names())
			}
		} else {
			for _, flag := range driftCommand().Flags {
				names = append(names, flag.Names())
			}
		}
		found := false
		for _, name := range names {
			if reflect.DeepEqual(name, []string{"fidelity"}) {
				found = true
			}
		}
		if !found {
			t.Errorf("%s does not expose --fidelity", command.name)
		}
	}
}
