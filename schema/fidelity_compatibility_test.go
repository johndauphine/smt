package schema_test

import (
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/johndauphine/smt/schema"
)

func TestFidelityLegacyPreservesAllPublicDialects(t *testing.T) {
	for _, target := range []string{"postgres", "mssql", "mysql", "sqlite", "clickhouse"} {
		for _, profile := range []schema.FidelityProfile{"", schema.FidelityLegacy} {
			t.Run(target+"/"+string(profile), func(t *testing.T) {
				opts := schema.Options{TargetDialect: target, Schema: "public", SourceDialect: "mssql"}
				legacy, err := schema.NewRenderer(opts)
				if err != nil {
					t.Fatal(err)
				}
				renderer, err := schema.NewFidelityRenderer(opts, schema.FidelityOptions{Profile: profile})
				if err != nil {
					t.Fatal(err)
				}
				if renderer.Profile() != schema.FidelityLegacy {
					t.Fatalf("profile = %q", renderer.Profile())
				}
				tables := []schema.Table{
					{Name: "Zulu", Columns: []schema.Column{{Name: "id", DataType: "tinyint"}, {Name: "payload", DataType: "varbinary", MaxLength: 8, IsNullable: true}}},
					{Name: "Alpha", Columns: []schema.Column{{Name: "id", DataType: "int"}}},
				}
				want, err := legacy.PlanCreate(tables)
				if err != nil {
					t.Fatal(err)
				}
				got, err := renderer.PlanCreate(tables)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got.Artifact, want) || len(got.Findings) != 0 {
					t.Fatalf("legacy plan changed:\n got %#v\nwant %#v", got, want)
				}
				wantColumn, err := legacy.CreateColumn(tables[0].Columns[0])
				if err != nil {
					t.Fatal(err)
				}
				gotColumn, err := renderer.CreateColumn(tables[0].Columns[0])
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(gotColumn.Artifact, wantColumn) || len(gotColumn.Findings) != 0 {
					t.Fatalf("legacy column changed: %#v", gotColumn)
				}
				ref := schema.TableRef{Name: "Zulu"}
				wantBatch, err := legacy.TruncateTable(ref, schema.TruncateOptions{})
				if err != nil {
					t.Fatal(err)
				}
				gotBatch, err := renderer.TruncateTable(ref, schema.TruncateOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(gotBatch.Artifact, wantBatch) || len(gotBatch.Findings) != 0 {
					t.Fatalf("legacy batch changed: %#v", gotBatch)
				}
			})
		}
	}
}

func TestFidelityCheckedEnforcesSupportedRestrictions(t *testing.T) {
	renderer, err := schema.NewFidelityRenderer(schema.Options{TargetDialect: "pg", Schema: "public", SourceDialect: "sqlserver"}, schema.FidelityOptions{Profile: schema.FidelityChecked})
	if err != nil {
		t.Fatal(err)
	}
	result, err := renderer.CreateTable(schema.Table{Name: "Limits", Columns: []schema.Column{
		{Name: "Small", DataType: "tinyint", IsNullable: true},
		{Name: "Bytes", DataType: "varbinary", MaxLength: 8, IsNullable: true},
		{Name: "Unbounded", DataType: "varbinary", MaxLength: -1, IsNullable: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{`"small" >= 0`, `"small" <= 255`, `octet_length("bytes") <= 8`} {
		if !strings.Contains(result.Artifact.SQL, fragment) {
			t.Fatalf("missing %s in %s", fragment, result.Artifact.SQL)
		}
	}
	if strings.Contains(result.Artifact.SQL, `octet_length("unbounded")`) {
		t.Fatal("MAX binary became bounded")
	}
	if strings.Count(result.Artifact.SQL, "CREATE TABLE") != 1 || strings.Contains(result.Artifact.SQL, "ALTER TABLE") {
		t.Fatalf("not one create artifact: %s", result.Artifact.SQL)
	}
	if len(result.Findings) != 2 {
		t.Fatalf("binary conversion findings = %#v", result.Findings)
	}
	for _, finding := range result.Findings {
		if finding.Code != "binary_input_conversion" || finding.SourceDialect != "mssql" || finding.TargetDialect != "postgres" || finding.Table != "Limits" {
			t.Fatalf("finding = %#v", finding)
		}
	}
}

func TestFidelityCheckedBinaryDefaultUsesTypedBytes(t *testing.T) {
	renderer, err := schema.NewFidelityRenderer(schema.Options{TargetDialect: "postgres", SourceDialect: "mssql"}, schema.FidelityOptions{Profile: schema.FidelityChecked})
	if err != nil {
		t.Fatal(err)
	}
	got, err := renderer.CreateTable(schema.Table{Name: "limits", Columns: []schema.Column{{Name: "payload", DataType: "varbinary", MaxLength: 50, DefaultExpression: "(0xAB)", IsNullable: true}}})
	if err != nil || !strings.Contains(got.Artifact.SQL, "DEFAULT decode('ab', 'hex')") {
		t.Fatalf("public binary default = %#v, %v", got, err)
	}
	got, err = renderer.CreateTable(schema.Table{Name: "limits", Columns: []schema.Column{{Name: "payload", DataType: "varbinary", MaxLength: 50, DefaultExpression: "CONVERT(varbinary(50), 'AB', 2)"}}})
	var unsupported *schema.UnsupportedFeatureError
	if !errors.As(err, &unsupported) || got.Artifact.SQL != "" {
		t.Fatalf("unsupported public binary default = %#v, %v", got, err)
	}
}

func TestFidelityMissingSourceAndStrictAtomicPlan(t *testing.T) {
	for _, profile := range []schema.FidelityProfile{schema.FidelityChecked, schema.FidelityStrict} {
		renderer, err := schema.NewFidelityRenderer(schema.Options{TargetDialect: "postgres", Schema: "public"}, schema.FidelityOptions{Profile: profile})
		if err != nil {
			t.Fatal(err)
		}
		got, err := renderer.PlanCreate([]schema.Table{{Name: "unknown", Columns: []schema.Column{{Name: "id", DataType: "int"}}}})
		if profile == schema.FidelityChecked {
			if err != nil || len(got.Findings) != 1 || got.Findings[0].Code != "source_context_missing" {
				t.Fatalf("checked missing source: %#v, %v", got, err)
			}
		} else {
			var violation *schema.FidelityViolationError
			if !errors.As(err, &violation) || len(violation.Findings) != 1 || len(got.Artifact.Statements) != 0 {
				t.Fatalf("strict returned partial plan or wrong error: %#v, %v", got, err)
			}
		}
	}
	renderer, err := schema.NewFidelityRenderer(schema.Options{TargetDialect: "postgres", SourceDialect: "mssql", Schema: "public"}, schema.FidelityOptions{Profile: schema.FidelityStrict})
	if err != nil {
		t.Fatal(err)
	}
	got, err := renderer.PlanCreate([]schema.Table{
		{Name: "valid", Columns: []schema.Column{{Name: "id", DataType: "tinyint"}}},
		{Name: "unresolved", Columns: []schema.Column{{Name: "stamp", DataType: "datetime"}}},
	})
	var violation *schema.FidelityViolationError
	if !errors.As(err, &violation) || len(got.Artifact.Statements) != 0 {
		t.Fatalf("strict partial plan: %#v, %v", got, err)
	}
	if len(violation.Findings) != 1 || violation.Findings[0].Code != "legacy_datetime_semantics" {
		t.Fatalf("strict findings: %#v", violation.Findings)
	}
}

func TestFidelityUnsupportedDialectsAndLifecycle(t *testing.T) {
	if _, err := schema.NewFidelityRenderer(schema.Options{TargetDialect: "postgres"}, schema.FidelityOptions{Profile: "typo"}); err == nil {
		t.Fatal("unknown profile accepted")
	}
	for _, target := range []string{"mssql", "mysql", "sqlite", "clickhouse"} {
		_, err := schema.NewFidelityRenderer(schema.Options{TargetDialect: target, SourceDialect: "mssql"}, schema.FidelityOptions{Profile: schema.FidelityChecked})
		var unsupported *schema.UnsupportedFeatureError
		if !errors.As(err, &unsupported) {
			t.Fatalf("%s error = %T %v", target, err, err)
		}
	}
	_, err := schema.NewFidelityRenderer(schema.Options{TargetDialect: "postgres", SourceDialect: "mysql"}, schema.FidelityOptions{Profile: schema.FidelityChecked})
	var unsupported *schema.UnsupportedFeatureError
	if !errors.As(err, &unsupported) {
		t.Fatalf("unsupported source accepted: %v", err)
	}
	renderer, err := schema.NewFidelityRenderer(schema.Options{TargetDialect: "postgres", SourceDialect: "mssql"}, schema.FidelityOptions{Profile: schema.FidelityChecked})
	if err != nil {
		t.Fatal(err)
	}
	column, err := renderer.CreateColumn(schema.Column{Name: "id", DataType: "tinyint"})
	if !errors.As(err, &unsupported) || column.Artifact.SQL != "" {
		t.Fatalf("standalone column bypassed fidelity: %#v, %v", column, err)
	}
	batch, err := renderer.AddColumn(schema.TableRef{Name: "table"}, schema.Column{Name: "id", DataType: "tinyint"})
	if !errors.As(err, &unsupported) || len(batch.Artifact.Statements) != 0 {
		t.Fatalf("evolution bypassed fidelity: %#v, %v", batch, err)
	}
}

func TestFidelityReservesCallerConstraintNamesWithoutMutation(t *testing.T) {
	opts := schema.Options{TargetDialect: "postgres", SourceDialect: "mssql"}
	table := schema.Table{Name: "Limits", Columns: []schema.Column{{Name: "id", DataType: "tinyint"}}}
	base, err := schema.NewFidelityRenderer(opts, schema.FidelityOptions{Profile: schema.FidelityChecked})
	if err != nil {
		t.Fatal(err)
	}
	plain, err := base.CreateTable(table)
	if err != nil {
		t.Fatal(err)
	}
	match := regexp.MustCompile(`CONSTRAINT "([^"]+)" CHECK`).FindStringSubmatch(plain.Artifact.SQL)
	if len(match) != 2 {
		t.Fatalf("missing generated check: %s", plain.Artifact.SQL)
	}
	reserved := map[string][]string{"Limits": {match[1]}}
	renderer, err := schema.NewFidelityRenderer(opts, schema.FidelityOptions{Profile: schema.FidelityChecked, SourceContext: schema.FidelitySourceContext{ReservedConstraintNames: reserved}})
	if err != nil {
		t.Fatal(err)
	}
	// The new renderer owns an immutable copy, including every map value slice.
	reserved["Limits"][0] = "mutated"
	delete(reserved, "Limits")
	got, err := renderer.CreateTable(table)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Artifact.SQL, `CONSTRAINT "`+match[1]+`_1" CHECK`) {
		t.Fatalf("reservation lost: %s", got.Artifact.SQL)
	}
	if strings.Contains(got.Artifact.SQL, "mutated") {
		t.Fatal("caller mutation leaked into SQL")
	}
	uniqueRenderer, err := schema.NewFidelityRenderer(opts, schema.FidelityOptions{Profile: schema.FidelityChecked, SourceContext: schema.FidelitySourceContext{
		UniqueIndexes: map[string][]schema.Index{"Limits": {{Name: match[1], Columns: []string{"id"}, IsUnique: true}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	got, err = uniqueRenderer.CreateTable(table)
	if err != nil || !strings.Contains(got.Artifact.SQL, `CONSTRAINT "`+match[1]+`_1" CHECK`) {
		t.Fatalf("unique-index namespace not reserved: %#v, %v", got, err)
	}
}

func TestFidelityRefusesNormalizedIdentifierCollisions(t *testing.T) {
	renderer, err := schema.NewFidelityRenderer(schema.Options{TargetDialect: "postgres", SourceDialect: "mssql"}, schema.FidelityOptions{Profile: schema.FidelityChecked})
	if err != nil {
		t.Fatal(err)
	}
	got, err := renderer.CreateTable(schema.Table{Name: "T", Columns: []schema.Column{{Name: "ID", DataType: "int"}, {Name: "id", DataType: "int"}}})
	if err == nil || got.Artifact.SQL != "" {
		t.Fatalf("colliding columns accepted: %#v, %v", got, err)
	}
	plan, err := renderer.PlanCreate([]schema.Table{
		{Name: "Limits", Columns: []schema.Column{{Name: "id", DataType: "int"}}},
		{Name: "limits", Columns: []schema.Column{{Name: "id", DataType: "int"}}},
	})
	if err == nil || len(plan.Artifact.Statements) != 0 {
		t.Fatalf("colliding tables accepted: %#v, %v", plan, err)
	}
}

func TestFidelityAssessesSuppliedUniqueIndexesWithoutRenderingThem(t *testing.T) {
	for _, nullable := range []bool{false, true} {
		for _, profile := range []schema.FidelityProfile{schema.FidelityChecked, schema.FidelityStrict} {
			t.Run(string(profile)+"/nullable="+map[bool]string{false: "false", true: "true"}[nullable], func(t *testing.T) {
				indexes := map[string][]schema.Index{"Limits": {{Name: "uq_source_id", Columns: []string{"id"}, IsUnique: true}}}
				renderer, err := schema.NewFidelityRenderer(schema.Options{TargetDialect: "postgres", SourceDialect: "mssql"}, schema.FidelityOptions{
					Profile: profile, SourceContext: schema.FidelitySourceContext{UniqueIndexes: indexes},
				})
				if err != nil {
					t.Fatal(err)
				}
				// Constructor copies both the map values and their nested key slices.
				indexes["Limits"][0].Columns[0] = "missing"
				delete(indexes, "Limits")
				got, err := renderer.CreateTable(schema.Table{Name: "Limits", Columns: []schema.Column{{Name: "id", DataType: "int", IsNullable: nullable}}})
				if nullable && profile == schema.FidelityStrict {
					var violation *schema.FidelityViolationError
					if !errors.As(err, &violation) || got.Artifact.SQL != "" || len(violation.Findings) != 1 || violation.Findings[0].Code != "nullable_unique_semantics" {
						t.Fatalf("strict nullable unique = %#v, %v", got, err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				wantFindings := 0
				if nullable {
					wantFindings = 1
				}
				if len(got.Findings) != wantFindings {
					t.Fatalf("unique findings = %#v", got.Findings)
				}
				if nullable && got.Findings[0].Code != "nullable_unique_semantics" {
					t.Fatalf("wrong unique finding: %#v", got.Findings)
				}
				if strings.Contains(got.Artifact.SQL, "uq_source_id") || strings.Contains(got.Artifact.SQL, "CREATE INDEX") || strings.Contains(got.Artifact.SQL, "UNIQUE") {
					t.Fatalf("source-only metadata emitted: %s", got.Artifact.SQL)
				}
			})
		}
	}
	_, err := schema.NewFidelityRenderer(schema.Options{TargetDialect: "postgres", SourceDialect: "mssql"}, schema.FidelityOptions{Profile: schema.FidelityChecked,
		SourceContext: schema.FidelitySourceContext{UniqueIndexes: map[string][]schema.Index{"T": {{Name: "ambiguous", Columns: []string{"id"}}}}},
	})
	if err == nil {
		t.Fatal("ambiguous nonunique source metadata accepted as a unique index")
	}
}

func TestFidelityCustomDialectOptInIsOptional(t *testing.T) {
	registry := schema.NewRegistry()
	if err := registry.Register(testDialect{}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.NewFidelityRenderer(schema.Options{TargetDialect: "test"}, schema.FidelityOptions{}); err != nil {
		t.Fatal(err)
	}
	_, err := registry.NewFidelityRenderer(schema.Options{TargetDialect: "test"}, schema.FidelityOptions{Profile: schema.FidelityChecked})
	var unsupported *schema.UnsupportedFeatureError
	if !errors.As(err, &unsupported) {
		t.Fatalf("old dialect opt-in = %T %v", err, err)
	}
	if err := registry.Register(fidelityTestDialect{}); err != nil {
		t.Fatal(err)
	}
	renderer, err := registry.NewFidelityRenderer(schema.Options{TargetDialect: "custom-fidelity"}, schema.FidelityOptions{Profile: schema.FidelityChecked})
	if err != nil {
		t.Fatal(err)
	}
	got, err := renderer.CreateTable(schema.Table{Name: "table", Columns: []schema.Column{{Name: "id", DataType: "int"}}})
	if err != nil || got.Artifact.SQL != "CUSTOM TABLE" || len(got.Findings) != 1 {
		t.Fatalf("custom result = %#v, %v", got, err)
	}
	renderer, err = registry.NewFidelityRenderer(schema.Options{TargetDialect: "custom-fidelity"}, schema.FidelityOptions{Profile: schema.FidelityStrict})
	if err != nil {
		t.Fatal(err)
	}
	got, err = renderer.CreateTable(schema.Table{Name: "table", Columns: []schema.Column{{Name: "id", DataType: "int"}}})
	var violation *schema.FidelityViolationError
	if !errors.As(err, &violation) || got.Artifact.SQL != "" {
		t.Fatalf("custom strict bypass = %#v, %v", got, err)
	}
	renderer, err = registry.NewFidelityRenderer(schema.Options{TargetDialect: "custom-fidelity", Schema: "public"}, schema.FidelityOptions{Profile: schema.FidelityChecked})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := renderer.PlanCreate([]schema.Table{
		{Name: "valid", Columns: []schema.Column{{Name: "id", DataType: "int"}}},
		{Name: "fail", Columns: []schema.Column{{Name: "id", DataType: "int"}}},
	})
	if err == nil || len(plan.Artifact.Statements) != 0 {
		t.Fatalf("custom error returned partial SQL: %#v, %v", plan, err)
	}
}

type fidelityTestDialect struct{ testDialect }

func (fidelityTestDialect) Name() string      { return "custom-fidelity" }
func (fidelityTestDialect) Aliases() []string { return nil }
func (fidelityTestDialect) FidelityProfiles() []schema.FidelityProfile {
	return []schema.FidelityProfile{schema.FidelityChecked, schema.FidelityStrict}
}
func (fidelityTestDialect) RenderTableWithFidelity(_ schema.Request, table schema.Table, _ schema.FidelityOptions) (schema.FidelityResult, error) {
	result := schema.FidelityResult{Artifact: schema.Result{SQL: "CUSTOM TABLE"}, Findings: []schema.FidelityFinding{{Code: "custom_unresolved"}}}
	if table.Name == "fail" {
		return result, errors.New("custom dialect failure with an accompanying artifact")
	}
	return result, nil
}
