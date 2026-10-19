package legacystructliterals

import (
	"testing"

	"github.com/johndauphine/smt/schema"
)

func TestLegacyCapabilitiesFieldOrder(t *testing.T) {
	if !LegacyCapabilities.CreateSchema || LegacyCapabilities.CreateTable ||
		!LegacyCapabilities.CreateColumn || LegacyCapabilities.PrimaryKeys ||
		!LegacyCapabilities.IdentityColumns || LegacyCapabilities.Defaults ||
		!LegacyCapabilities.ComputedColumns || LegacyCapabilities.SecondaryIndexes ||
		!LegacyCapabilities.StandalonePrimaryKeys || LegacyCapabilities.StandaloneForeignKeys ||
		!LegacyCapabilities.NamedUniqueConstraints || LegacyCapabilities.CheckConstraints ||
		!LegacyCapabilities.IndexExpressionKeys || LegacyCapabilities.IndexPrefixLengths ||
		!LegacyCapabilities.IndexIncludeColumns || LegacyCapabilities.FilteredIndexes {
		t.Fatalf("legacy Capabilities literal field order changed: %+v", LegacyCapabilities)
	}
}

func TestLegacyExternalDialectRunsWithoutExtensions(t *testing.T) {
	registry := schema.NewRegistry()
	if err := registry.Register(oldDialect{}); err != nil {
		t.Fatal(err)
	}
	renderer, err := registry.NewRenderer(schema.Options{TargetDialect: "external-legacy", Schema: "schema"})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := renderer.PlanCreate([]schema.Table{{Name: "table", Columns: []schema.Column{{Name: "id", DataType: "int"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Statements) != 2 || plan.Statements[0].SQL != "SCHEMA" || plan.Statements[1].SQL != "TABLE" {
		t.Fatalf("legacy external dialect plan = %#v", plan)
	}
}
