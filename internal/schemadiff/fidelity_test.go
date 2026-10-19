package schemadiff

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/smt/internal/ddl"
	"github.com/johndauphine/smt/internal/driver"
	"github.com/johndauphine/smt/internal/fidelity"
)

func fidelityTestTables(t *testing.T) ([]driver.Table, []driver.Table) {
	t.Helper()
	desired := []driver.Table{{Schema: "public", Name: "limits", Columns: []driver.Column{
		{Name: "byte", DataType: "tinyint", IsNullable: true},
		{Name: "bytes", DataType: "varbinary", MaxLength: 16, IsNullable: true},
	}}}
	existing := []driver.Table{deepCopyTable(desired[0])}
	existing[0].Columns[0].DataType = "smallint"
	existing[0].Columns[1].DataType = "bytea"
	existing[0].Columns[1].MaxLength = 0
	renderer, err := ddl.NewRenderer("postgres", "public", "fail")
	if err != nil {
		t.Fatal(err)
	}
	renderer = renderer.WithSource("mssql")
	checks, _, err := renderer.FidelityChecks(&desired[0], fidelity.Checked)
	if err != nil {
		t.Fatal(err)
	}
	for _, check := range checks {
		validated := true
		existing[0].CheckConstraints = append(existing[0].CheckConstraints, driver.CheckConstraint{Name: check.Name, Definition: "((" + check.Expression + "))", Validated: &validated})
	}
	return desired, existing
}

func TestFidelityLifecycleNoOpExactChecks(t *testing.T) {
	desired, existing := fidelityTestTables(t)
	originalDesired, originalExisting := deepCopyTable(desired[0]), deepCopyTable(existing[0])
	for _, opts := range []DriftOptions{DefaultDriftOptions(), {}} {
		drift, assessment, err := ComputeDriftWithFidelity(desired, existing, "mssql", "postgres", opts, fidelity.Checked)
		if err != nil || !drift.IsEmpty() || len(assessment.Requirements) != 2 {
			t.Fatalf("create/introspect drift: %#v, %#v, %v", drift, assessment, err)
		}
		for range 2 {
			diff, _, err := ComputeLiveDiffWithFidelity(desired, existing, "mssql", "postgres", opts, fidelity.Checked)
			if err != nil || !diff.IsEmpty() {
				t.Fatalf("repeated sync must be empty: %#v, %v", diff, err)
			}
		}
	}
	if !reflect.DeepEqual(desired[0], originalDesired) || !reflect.DeepEqual(existing[0], originalExisting) {
		t.Fatal("comparison mutated source or target metadata")
	}
}

func TestFidelityLifecycleStrictRejectsNullableUniqueEvenWhenSideObjectsUnmanaged(t *testing.T) {
	desired := []driver.Table{{Schema: "public", Name: "nullable_key", Columns: []driver.Column{{Name: "key", DataType: "int", IsNullable: true}}, Indexes: []driver.Index{{Name: "uq_key", Columns: []string{"key"}, IsUnique: true}}}}
	diff, assessment, err := ComputeLiveDiffWithFidelity(desired, nil, "mssql", "postgres", DriftOptions{}, fidelity.Checked)
	if err != nil || len(diff.AddedTables) != 1 || len(assessment.Findings) != 1 || assessment.Findings[0].Code != fidelity.NullableUniqueSemantics {
		t.Fatalf("checked table lost nullable unique finding: %#v, %#v, %v", diff, assessment, err)
	}
	strictDiff, _, err := ComputeLiveDiffWithFidelity(desired, nil, "mssql", "postgres", DriftOptions{}, fidelity.Strict)
	var violation *fidelity.ViolationError
	if !errors.As(err, &violation) || !strictDiff.IsEmpty() || violation.Findings[0].Code != fidelity.NullableUniqueSemantics {
		t.Fatalf("strict nullable unique escaped assessment: %#v, %v", strictDiff, err)
	}
	plan, err := RenderLiveDiffWithFidelity(diff, RenderOptions{SourceDialect: "mssql", TargetDialect: "postgres", TargetSchema: "public"}, DriftOptions{}, fidelity.Strict)
	if !errors.As(err, &violation) || !plan.IsEmpty() {
		t.Fatalf("strict renderer returned nullable unique partial plan: %#v, %v", plan, err)
	}
}

func TestFidelityLifecycleBinaryDefaultByteEquivalence(t *testing.T) {
	for _, tc := range []struct {
		source, target string
		match          bool
	}{
		{"((0xAB))", "decode('ab'::text, 'hex'::text)", true},
		{"0xA", "decode('0a'::text, 'hex'::text)", true},
		{"0x", "decode(''::text, 'hex'::text)", true},
		{"(NULL)", "NULL::bytea", true},
		{"0xAB", "decode('ac'::text, 'hex'::text)", false},
		{"0xAB", "custom.decode('ab'::text, 'hex'::text)", false},
		{"0xAB", "", false},
		{"0x", "NULL::bytea", false},
	} {
		desired, existing := fidelityTestTables(t)
		desired[0].Columns[1].DefaultExpression, desired[0].Columns[1].HasDefault = tc.source, true
		existing[0].Columns[1].DefaultExpression = tc.target
		existing[0].Columns[1].HasDefault = tc.target != ""
		drift, _, err := ComputeDriftWithFidelity(desired, existing, "mssql", "postgres", DefaultDriftOptions(), fidelity.Checked)
		if err != nil || drift.IsEmpty() != tc.match {
			t.Errorf("%s -> %s: drift %#v, %v", tc.source, tc.target, drift, err)
		}
		diff, _, err := ComputeLiveDiffWithFidelity(desired, existing, "mssql", "postgres", DefaultDriftOptions(), fidelity.Checked)
		if tc.match && (err != nil || !diff.IsEmpty()) {
			t.Errorf("equivalent default requires no sync: %#v, %v", diff, err)
		} else if !tc.match && err == nil {
			t.Errorf("different bytes silently considered equivalent: %#v", diff)
		}
		if desired[0].Columns[1].DefaultExpression != tc.source || existing[0].Columns[1].DefaultExpression != tc.target {
			t.Fatal("binary default comparison mutated source/target metadata")
		}
	}
}

func TestFidelityLifecycleRejectsMissingTamperedUnvalidatedChecks(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*driver.Table)
	}{
		{"missing", func(table *driver.Table) { table.CheckConstraints = table.CheckConstraints[1:] }},
		{"same name different bound", func(table *driver.Table) { table.CheckConstraints[0].Definition = `"byte" >= 0 AND "byte" <= 256` }},
		{"same name unsafe cast", func(table *driver.Table) {
			table.CheckConstraints[0].Definition = `"byte"::boolean >= 0 AND "byte" <= 255`
		}},
		{"not validated", func(table *driver.Table) { value := false; table.CheckConstraints[0].Validated = &value }},
		{"older snapshot unknown validation", func(table *driver.Table) { table.CheckConstraints[0].Validated = nil }},
		{"unrelated prefix", func(table *driver.Table) {
			table.CheckConstraints = append(table.CheckConstraints, driver.CheckConstraint{Name: "smt_fidelity_user_rule", Definition: `"byte" >= 8`})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			desired, existing := fidelityTestTables(t)
			tc.mutate(&existing[0])
			for _, opts := range []DriftOptions{DefaultDriftOptions(), {}} {
				drift, _, err := ComputeDriftWithFidelity(desired, existing, "mssql", "postgres", opts, fidelity.Checked)
				if err != nil || drift.IsEmpty() || len(drift.ChangedTables) == 0 || drift.ChangedTables[0].CheckDrift == "" {
					t.Fatalf("lost generated check mismatch: %#v, %v", drift, err)
				}
				diff, _, err := ComputeLiveDiffWithFidelity(desired, existing, "mssql", "postgres", opts, fidelity.Checked)
				var unsupported *FidelityEvolutionError
				if !errors.As(err, &unsupported) || !diff.IsEmpty() {
					t.Fatalf("unsafe plan produced: %#v, %v", diff, err)
				}
			}
		})
	}
}

func TestFidelityLifecycleDetectsBoundChangeAndRefusesExistingEvolution(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*driver.Table)
	}{
		{"tighten byte bound", func(table *driver.Table) { table.Columns[1].MaxLength = 8 }},
		{"widen byte bound", func(table *driver.Table) { table.Columns[1].MaxLength = 32 }},
		{"remove restriction same physical type", func(table *driver.Table) { table.Columns[0].DataType = "smallint" }},
		{"add column", func(table *driver.Table) {
			table.Columns = append(table.Columns, driver.Column{Name: "new_byte", DataType: "tinyint"})
		}},
		{"drop column", func(table *driver.Table) { table.Columns = table.Columns[1:] }},
		{"type change", func(table *driver.Table) { table.Columns[0].DataType = "bigint" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			desired, existing := fidelityTestTables(t)
			existing[0].RowCount = 100
			tc.mutate(&desired[0])
			for _, opts := range []DriftOptions{DefaultDriftOptions(), {}} {
				diff, _, err := ComputeLiveDiffWithFidelity(desired, existing, "mssql", "postgres", opts, fidelity.Checked)
				var unsupported *FidelityEvolutionError
				if !errors.As(err, &unsupported) || !diff.IsEmpty() {
					t.Fatalf("existing populated target change silently allowed: %#v, %v", diff, err)
				}
			}
		})
	}
}

func TestFidelityLifecycleNewTableUsesSharedRendererAndReservedNames(t *testing.T) {
	desired, _ := fidelityTestTables(t)
	renderer, _ := ddl.NewRenderer("postgres", "public", "fail")
	renderer = renderer.WithSource("mssql")
	checks, _, err := renderer.FidelityChecks(&desired[0], fidelity.Checked)
	if err != nil {
		t.Fatal(err)
	}
	// A real source constraint can collide with a generated candidate. Its
	// name must stay reserved even when source check emission is disabled.
	desired[0].CheckConstraints = []driver.CheckConstraint{{Name: checks[0].Name, Definition: "[byte] >= 0"}}
	checks, _, err = renderer.FidelityChecks(&desired[0], fidelity.Checked)
	if err != nil {
		t.Fatal(err)
	}
	for _, managed := range []DriftOptions{DefaultDriftOptions(), {}} {
		diff, _, err := ComputeLiveDiffWithFidelity(desired, nil, "mssql", "postgres", managed, fidelity.Checked)
		if err != nil || len(diff.AddedTables) != 1 {
			t.Fatalf("new-table diff: %#v, %v", diff, err)
		}
		plan, err := RenderLiveDiffWithFidelity(diff, RenderOptions{TargetDialect: "postgres", SourceDialect: "mssql", TargetSchema: "public"}, managed, fidelity.Checked)
		if err != nil || len(plan.Statements) == 0 {
			t.Fatalf("new-table plan: %#v, %v", plan, err)
		}
		want, _, err := renderer.CreateTableWithFidelity(&desired[0], fidelity.Checked)
		if err != nil || plan.Statements[0].SQL != want {
			t.Fatalf("sync/create SQL disagree: %#v, %v", plan, err)
		}
		for _, check := range checks {
			if strings.Count(plan.SQL(), check.Definition) != 1 {
				t.Fatalf("generated restriction duplicated or absent: %s", plan.SQL())
			}
		}
		if !managed.CompareChecks && len(plan.Statements) != 1 {
			t.Fatalf("disabled source checks emitted: %#v", plan)
		}
	}
}

func TestFidelityExpressionComparisonFailsClosed(t *testing.T) {
	expected := `"byte" >= 0 AND "byte" <= 255`
	for _, actual := range []string{`((byte >= (0)) AND (byte <= (255)))`, `("byte" >= (0)::integer AND "byte" <= 255::smallint)`} {
		if !equalFidelityExpression(expected, actual) {
			t.Errorf("catalog decoration rejected: %s", actual)
		}
	}
	for _, actual := range []string{
		`byte >= 0 AND byte <= 256`, `byte >= 0 OR byte <= 255`, `byte::boolean >= 0 AND byte <= 255`,
		`byte >= 0::boolean AND byte <= 255`, `byte >= 0 AND byte <= 255 OR true`,
		`byte >= 0 AND byte <= 255; DROP TABLE x`, `byte >= 0 AND byte <= 255 NOT VALID`,
		`"Byte" >= 0 AND "Byte" <= 255`, `(byte >= 0 AND byte <= 255`,
		`byte >= 0 AND byte <= 255 -- comment`,
	} {
		if equalFidelityExpression(expected, actual) {
			t.Errorf("unsafe predicate accepted: %s", actual)
		}
	}
	if !equalFidelityExpression(`octet_length("bytes") <= 16`, `((octet_length(bytes) <= (16)::integer))`) {
		t.Fatal("catalog byte predicate not recognized")
	}
	if equalFidelityExpression(`octet_length("bytes") <= 16`, `length(bytes) <= 16`) {
		t.Fatal("different function treated as same restriction")
	}
}

func TestFidelityLifecycleLegacyDelegatesAndUnsupportedNeverReturnsPartialSQL(t *testing.T) {
	desired, existing := fidelityTestTables(t)
	opts := DefaultDriftOptions()
	want := ComputeLiveDiff(desired, existing, "mssql", "postgres", opts)
	got, assessment, err := ComputeLiveDiffWithFidelity(desired, existing, "mssql", "postgres", opts, "")
	if err != nil || !reflect.DeepEqual(want, got) || len(assessment.Findings) > 0 || len(assessment.Requirements) > 0 {
		t.Fatalf("legacy live diff changed: %#v, %v", got, err)
	}
	wantDrift := ComputeDrift(desired, existing, "mssql", "postgres", opts)
	gotDrift, _, err := ComputeDriftWithFidelity(desired, existing, "mssql", "postgres", opts, fidelity.Legacy)
	if err != nil || !reflect.DeepEqual(wantDrift, gotDrift) {
		t.Fatalf("legacy drift changed: %#v, %v", gotDrift, err)
	}
	plan, err := RenderLiveDiffWithFidelity(Diff{AddedTables: desired, ChangedTables: []TableDiff{{Name: "populated"}}}, RenderOptions{TargetDialect: "postgres", SourceDialect: "mssql"}, opts, fidelity.Checked)
	if err == nil || len(plan.Statements) != 0 {
		t.Fatalf("unsafe partial plan returned: %#v, %v", plan, err)
	}
	_, _, err = ComputeDriftWithFidelity(nil, nil, "mysql", "postgres", opts, fidelity.Checked)
	var unsupported *fidelity.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("empty unsupported schema bypassed capability gate: %v", err)
	}
}
