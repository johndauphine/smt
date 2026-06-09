package orchestrator

import (
	"context"
	"testing"

	"smt/internal/checkpoint"
	"smt/internal/config"
	"smt/internal/driver"
	"smt/internal/schemadiff"
)

type fakeWriter struct {
	driver.Writer // embedded so unstubbed methods panic loudly if reached
	tables        map[string]bool
	indexes       map[string]bool
	execed        []string
}

func (f *fakeWriter) TableExists(_ context.Context, _, table string) (bool, error) {
	return f.tables[table], nil
}
func (f *fakeWriter) IndexExists(_ context.Context, _, _, name string) (bool, error) {
	return f.indexes[name], nil
}
func (f *fakeWriter) ForeignKeyExists(_ context.Context, _, _, _ string) (bool, error) {
	return false, nil
}
func (f *fakeWriter) CheckConstraintExists(_ context.Context, _, _, _ string) (bool, error) {
	return false, nil
}
func (f *fakeWriter) ExecRaw(_ context.Context, query string, _ ...any) (int64, error) {
	f.execed = append(f.execed, query)
	return 0, nil
}

type fakeState struct {
	checkpoint.StateBackend
}

func (fakeState) UpdatePhase(_, _ string) error { return nil }

// #87 — executePlan runs statements in plan order and skips objects that
// already exist on the target (idempotent re-runs).
func TestExecutePlanSkipsExistingObjects(t *testing.T) {
	w := &fakeWriter{
		tables:  map[string]bool{"existing": true},
		indexes: map[string]bool{"ix_old": true},
	}
	cfg := &config.Config{}
	cfg.Target.Schema = "public"
	o := &Orchestrator{config: cfg, target: w, state: fakeState{}}

	plan := schemadiff.Plan{Statements: []schemadiff.Statement{
		{Kind: schemadiff.StatementKindSchema, Object: "public", SQL: "CREATE SCHEMA IF NOT EXISTS public", Description: "create schema"},
		{Kind: schemadiff.StatementKindTable, Table: "existing", Object: "existing", SQL: "CREATE TABLE existing", Description: "create table existing"},
		{Kind: schemadiff.StatementKindTable, Table: "fresh", Object: "fresh", SQL: "CREATE TABLE fresh", Description: "create table fresh"},
		{Kind: schemadiff.StatementKindIndex, Table: "fresh", Object: "ix_old", SQL: "CREATE INDEX ix_old", Description: "create index ix_old"},
		{Kind: schemadiff.StatementKindIndex, Table: "fresh", Object: "ix_new", SQL: "CREATE INDEX ix_new", Description: "create index ix_new"},
	}}

	if err := o.executePlan(context.Background(), "run-1", plan); err != nil {
		t.Fatalf("executePlan: %v", err)
	}

	want := []string{"CREATE SCHEMA IF NOT EXISTS public", "CREATE TABLE fresh", "CREATE INDEX ix_new"}
	if len(w.execed) != len(want) {
		t.Fatalf("executed %v, want %v", w.execed, want)
	}
	for i := range want {
		if w.execed[i] != want[i] {
			t.Fatalf("executed %v, want %v", w.execed, want)
		}
	}
}
