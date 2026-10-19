package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/smt/internal/config"
	"github.com/johndauphine/smt/internal/ddl"
	"github.com/johndauphine/smt/internal/driver"
	"github.com/johndauphine/smt/internal/fidelity"
	"github.com/johndauphine/smt/internal/schemadiff"
)

func TestFidelityEmptyScopeRejectsUnsupportedPair(t *testing.T) {
	for _, profile := range []string{"checked", "strict"} {
		cfg := &config.Config{}
		cfg.Source.Type = "postgres"
		cfg.Target.Type = "mysql"
		cfg.SchemaGeneration.FidelityProfile = profile
		cfg.SchemaGeneration.UnknownTypePolicy = "fail"
		o := &Orchestrator{config: cfg}
		_, err := o.newCreateDDLRenderer()
		var unsupported *fidelity.UnsupportedError
		if !errors.As(err, &unsupported) {
			t.Fatalf("%s empty scope: wanted unsupported pair before source/provider access, got %v", profile, err)
		}
	}
}

func TestFidelityCreateUsesReviewedPlan(t *testing.T) {
	r, err := ddl.NewRenderer("postgres", "public", "fail")
	if err != nil {
		t.Fatal(err)
	}
	r = r.WithSource("mssql")
	table := driver.Table{Name: "Items", Columns: []driver.Column{{Name: "Rank", DataType: "tinyint", IsNullable: true}}}
	legacy, _, err := r.CreateTableDDL(&table)
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range []fidelity.Profile{"", fidelity.Legacy, fidelity.Checked, fidelity.Strict} {
		t.Run(string(profile), func(t *testing.T) {
			reviewer := &fakeReviewer{verdict: &driver.VerifyResult{OK: true}}
			cr := createDDLRenderer{sourceType: "mssql", targetType: "postgres", ddlRenderer: r,
				fidelityProfile: profile, aiReviewEnabled: true, aiReviewMode: "fail", tableVerifier: reviewer}
			sql, err := cr.renderTable(context.Background(), &table)
			if err != nil {
				t.Fatal(err)
			}
			if profile == "" || profile == fidelity.Legacy {
				if sql != legacy {
					t.Fatal("legacy SQL changed")
				}
			} else if !strings.Contains(sql, `"rank" >= 0 AND "rank" <= 255`) {
				t.Fatalf("missing source range in reviewed SQL: %s", sql)
			}
		})
	}
}

func TestFidelityCreateDoesNotSkipExistingTables(t *testing.T) {
	w := &fakeWriter{tables: map[string]bool{"existing": true}}
	cfg := &config.Config{}
	cfg.SchemaGeneration.FidelityProfile = "checked"
	cfg.Target.Schema = "public"
	o := &Orchestrator{config: cfg, target: w, state: fakeState{}}
	p := schemadiff.Plan{Statements: []schemadiff.Statement{
		{Kind: schemadiff.StatementKindSchema, SQL: "CREATE SCHEMA IF NOT EXISTS public"},
		{Kind: schemadiff.StatementKindTable, Table: "fresh", Object: "fresh", SQL: "CREATE TABLE fresh(id int)"},
		{Kind: schemadiff.StatementKindTable, Table: "existing", Object: "existing", SQL: "CREATE TABLE existing(id int)"},
	}}
	err := o.executePlan(context.Background(), "fidelity-test", p)
	if err == nil || !strings.Contains(err.Error(), "existing table") {
		t.Fatalf("expected preflight refusal: %v", err)
	}
	if len(w.execed) != 0 {
		t.Fatalf("partially executed despite preflight: %v", w.execed)
	}
}

func TestLegacyManifestOmitsNewFidelityFields(t *testing.T) {
	b, err := json.Marshal(runManifest{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "fidelity_") {
		t.Fatalf("legacy manifest grew fidelity fields: %s", b)
	}
}

func TestFidelityFingerprintSeparatesPolicyAndPreservesCaller(t *testing.T) {
	valid := true
	original := driver.Table{Columns: []driver.Column{{SampleValues: []string{"sample"}}},
		CheckConstraints: []driver.CheckConstraint{{Name: "range_check", Validated: &valid}}}
	copy := original
	canonicalizeForFingerprint(&copy)
	if original.CheckConstraints[0].Validated == nil || len(original.Columns[0].SampleValues) != 1 {
		t.Fatal("fingerprinting mutated caller metadata")
	}
	if copy.CheckConstraints[0].Validated != nil || copy.Columns[0].SampleValues != nil {
		t.Fatal("transient metadata remains in fingerprint")
	}
	input := "identical source or plan"
	legacy := fingerprintBytes([]byte(input))
	checked := fidelityFingerprint(input, fidelity.Checked)
	strict := fidelityFingerprint(input, fidelity.Strict)
	if legacy == checked || checked == strict || legacy == strict {
		t.Fatal("fidelity policies share a cache key")
	}
}

type fidelityAppendReader struct {
	driver.Reader
	index driver.Index
	fk    driver.ForeignKey
	check driver.CheckConstraint
}

func (r fidelityAppendReader) LoadIndexes(_ context.Context, table *driver.Table) error {
	table.Indexes = append(table.Indexes, r.index)
	return nil
}
func (r fidelityAppendReader) LoadForeignKeys(_ context.Context, table *driver.Table) error {
	table.ForeignKeys = append(table.ForeignKeys, r.fk)
	return nil
}
func (r fidelityAppendReader) LoadCheckConstraints(_ context.Context, table *driver.Table) error {
	table.CheckConstraints = append(table.CheckConstraints, r.check)
	return nil
}

func TestFidelityReservedNamesDoNotDuplicateSideObjects(t *testing.T) {
	source := fidelityAppendReader{
		index: driver.Index{Name: "ix_value", Columns: []string{"Value"}},
		fk:    driver.ForeignKey{Name: "fk_value", Columns: []string{"Value"}, RefSchema: "dbo", RefTable: "Items", RefColumns: []string{"Value"}},
		check: driver.CheckConstraint{Name: "ck_value", Definition: "[Value] >= 0"},
	}
	table := driver.Table{Name: "Items", Schema: "dbo", Columns: []driver.Column{{Name: "Value", DataType: "tinyint"}},
		Indexes: []driver.Index{source.index}, ForeignKeys: []driver.ForeignKey{source.fk}, CheckConstraints: []driver.CheckConstraint{source.check}}
	cfg := &config.Config{}
	cfg.Migration.CreateIndexes = true
	cfg.Migration.CreateForeignKeys = true
	cfg.Migration.CreateCheckConstraints = true
	cfg.SchemaGeneration.FidelityProfile = "checked"
	r, err := ddl.NewRenderer("postgres", "public", "fail")
	if err != nil {
		t.Fatal(err)
	}
	cr := createDDLRenderer{sourceType: "mssql", targetType: "postgres", targetSchema: "public", ddlRenderer: r.WithSource("mssql"), fidelityProfile: fidelity.Checked}
	o := &Orchestrator{config: cfg, source: source, state: fakeState{}, tables: []driver.Table{table}}
	ctx := context.Background()
	for _, phase := range []func(context.Context, string, createDDLRenderer) ([]schemadiff.Statement, error){o.renderCreateIndexStatements, o.renderCreateForeignKeyStatements, o.renderCreateCheckStatements} {
		statements, err := phase(ctx, "test", cr)
		if err != nil {
			t.Fatal(err)
		}
		if len(statements) != 1 {
			t.Fatalf("preloaded metadata emitted %d side objects: %#v", len(statements), statements)
		}
	}
	snapshot, err := o.canonicalSourceSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot[0].Indexes) != 1 || len(snapshot[0].ForeignKeys) != 1 || len(snapshot[0].CheckConstraints) != 1 {
		t.Fatalf("duplicate source fingerprint metadata: %+v", snapshot[0])
	}
}
