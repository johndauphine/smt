package schemadiff

// Golden tests for deterministic sync plans (#71). One comprehensive diff —
// added table with side objects, changed table touching every delta kind,
// removed tables with an FK drop-order dependency — rendered for each target
// dialect and byte-compared against testdata/golden/sync_<target>.sql.
//
// Regenerate after an intentional renderer change:
//
//	UPDATE_GOLDEN=1 go test ./internal/schemadiff/ -run TestRenderDeterministic_Golden
//
// then review the .sql diff like any other code change.

import (
	"os"
	"path/filepath"
	"testing"

	"smt/internal/driver"
)

// goldenDiff builds a diff that exercises every statement family the sync
// renderer can emit. Identifiers are lowercase so the same diff is valid
// input for all three targets without a Normalize pass.
func goldenDiff() Diff {
	return Diff{
		AddedTables: []driver.Table{{
			Schema: "app",
			Name:   "audit_log",
			Columns: []driver.Column{
				{Name: "id", DataType: "bigint", IsIdentity: true, IsNullable: false, OrdinalPos: 1},
				{Name: "actor", DataType: "varchar", MaxLength: 100, IsNullable: false, OrdinalPos: 2},
				{Name: "action", DataType: "varchar", MaxLength: 30, IsNullable: false, DefaultExpression: "'created'", OrdinalPos: 3},
				{Name: "at", DataType: "datetime2", DatetimePrecision: intp(3), IsNullable: false, DefaultExpression: "getutcdate()", OrdinalPos: 4},
				{Name: "details", DataType: "nvarchar", MaxLength: -1, IsNullable: true, OrdinalPos: 5},
			},
			PrimaryKey: []string{"id"},
			Indexes: []driver.Index{
				{Name: "ix_audit_actor", Columns: []string{"actor"}},
				{Name: "uq_audit_actor_at", Columns: []string{"actor", "at"}, IsUnique: true},
			},
			ForeignKeys: []driver.ForeignKey{
				{Name: "fk_audit_actor", Columns: []string{"actor"}, RefSchema: "app", RefTable: "users", RefColumns: []string{"username"}, OnDelete: "CASCADE"},
			},
			CheckConstraints: []driver.CheckConstraint{
				{Name: "ck_audit_action", Definition: "action IN ('created','updated','deleted')"},
			},
		}},
		ChangedTables: []TableDiff{{
			Schema: "app",
			Name:   "users",
			Curr: driver.Table{
				Schema: "app",
				Name:   "users",
				Columns: []driver.Column{
					{Name: "id", DataType: "int", IsIdentity: true, IsNullable: false, OrdinalPos: 1},
					{Name: "username", DataType: "varchar", MaxLength: 80, IsNullable: false, OrdinalPos: 2},
					{Name: "active", DataType: "bit", IsNullable: false, DefaultExpression: "((1))", OrdinalPos: 3},
					{Name: "nickname", DataType: "varchar", MaxLength: 40, IsNullable: true, OrdinalPos: 4},
				},
				PrimaryKey: []string{"id"},
			},
			AddedColumns: []driver.Column{
				{Name: "nickname", DataType: "varchar", MaxLength: 40, IsNullable: true, OrdinalPos: 4},
			},
			RemovedColumns: []driver.Column{
				{Name: "legacy_code", DataType: "char", MaxLength: 4, IsNullable: true},
			},
			ChangedColumns: []ColumnChange{
				{
					Name: "username",
					Old:  driver.Column{Name: "username", DataType: "varchar", MaxLength: 40, IsNullable: false},
					New:  driver.Column{Name: "username", DataType: "varchar", MaxLength: 80, IsNullable: false},
				},
				{
					Name: "active",
					Old:  driver.Column{Name: "active", DataType: "bit", IsNullable: false, DefaultExpression: "((0))"},
					New:  driver.Column{Name: "active", DataType: "bit", IsNullable: false, DefaultExpression: "((1))"},
				},
			},
			AddedIndexes:       []driver.Index{{Name: "ix_users_nickname", Columns: []string{"nickname"}}},
			RemovedIndexes:     []driver.Index{{Name: "ix_users_legacy", Columns: []string{"legacy_code"}}},
			AddedForeignKeys:   []driver.ForeignKey{{Name: "fk_users_org", Columns: []string{"org_id"}, RefSchema: "app", RefTable: "orgs", RefColumns: []string{"id"}, OnDelete: "SET NULL"}},
			RemovedForeignKeys: []driver.ForeignKey{{Name: "fk_users_dept"}},
			AddedChecks:        []driver.CheckConstraint{{Name: "ck_users_username", Definition: "username <> ''"}},
			RemovedChecks:      []driver.CheckConstraint{{Name: "ck_users_legacy"}},
		}},
		RemovedTables: []driver.Table{
			// children-first drop ordering: line_items references orders.
			{
				Schema: "app",
				Name:   "orders",
				Columns: []driver.Column{
					{Name: "id", DataType: "int", IsIdentity: true, IsNullable: false},
				},
			},
			{
				Schema: "app",
				Name:   "line_items",
				Columns: []driver.Column{
					{Name: "id", DataType: "int", IsIdentity: true, IsNullable: false},
					{Name: "order_id", DataType: "int", IsNullable: false},
				},
				ForeignKeys: []driver.ForeignKey{
					{Name: "fk_li_order", Columns: []string{"order_id"}, RefSchema: "app", RefTable: "orders", RefColumns: []string{"id"}},
				},
			},
		},
	}
}

func TestRenderDeterministic_Golden(t *testing.T) {
	for _, target := range []string{"postgres", "mssql", "mysql"} {
		t.Run(target, func(t *testing.T) {
			plan, err := RenderDeterministicWithUnknownTypePolicy(goldenDiff(), "tgt", target, "fail")
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			got := plan.SQL()

			goldenPath := filepath.Join("testdata", "golden", "sync_"+target+".sql")
			if os.Getenv("UPDATE_GOLDEN") != "" {
				if err := os.MkdirAll(filepath.Dir(goldenPath), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(goldenPath, []byte(got), 0644); err != nil {
					t.Fatal(err)
				}
				t.Logf("updated %s", goldenPath)
				return
			}

			want, err := os.ReadFile(goldenPath)
			if err != nil {
				t.Fatalf("reading golden file (run with UPDATE_GOLDEN=1 to create): %v", err)
			}
			if got != string(want) {
				t.Errorf("plan SQL diverged from %s.\nRegenerate with UPDATE_GOLDEN=1 if the change is intentional.\n--- got ---\n%s", goldenPath, got)
			}
		})
	}
}

// TestRenderDeterministic_Stable pins the #71 acceptance criterion that
// dry-run output is identical across repeated runs. The renderer walks maps
// in places (drop ordering, dedup) — any missed sort surfaces here as a
// flaky diff long before an operator sees a churning migration.sql.
func TestRenderDeterministic_Stable(t *testing.T) {
	for _, target := range []string{"postgres", "mssql", "mysql"} {
		t.Run(target, func(t *testing.T) {
			first := ""
			for i := 0; i < 25; i++ {
				plan, err := RenderDeterministicWithUnknownTypePolicy(goldenDiff(), "tgt", target, "fail")
				if err != nil {
					t.Fatalf("render #%d: %v", i, err)
				}
				if i == 0 {
					first = plan.SQL()
					continue
				}
				if got := plan.SQL(); got != first {
					t.Fatalf("render #%d differs from #0:\n--- first ---\n%s\n--- got ---\n%s", i, first, got)
				}
			}
		})
	}
}

// TestRenderDeterministic_GoldenRiskGating locks the risk split of the
// golden plan: drops of tables and columns are data-loss and must be
// filtered out by the default sync --apply posture, everything else
// survives. Guards against a renderer change silently downgrading a
// destructive statement to safe.
func TestRenderDeterministic_GoldenRiskGating(t *testing.T) {
	plan, err := RenderDeterministicWithUnknownTypePolicy(goldenDiff(), "tgt", "postgres", "fail")
	if err != nil {
		t.Fatalf("render: %v", err)
	}

	var dataLoss []string
	for _, s := range plan.Statements {
		if s.Risk == RiskDataLoss {
			dataLoss = append(dataLoss, s.Description)
		}
	}
	// drop column legacy_code + drop table orders + drop table line_items
	if len(dataLoss) != 3 {
		t.Fatalf("expected 3 data-loss statements, got %d: %v", len(dataLoss), dataLoss)
	}

	kept := plan.FilterByRisk(RiskRebuildNeeded)
	if len(kept.Statements) != len(plan.Statements)-3 {
		t.Fatalf("FilterByRisk kept %d of %d statements, expected to drop exactly the 3 data-loss ones",
			len(kept.Statements), len(plan.Statements))
	}
	for _, s := range kept.Statements {
		if s.Risk == RiskDataLoss {
			t.Errorf("data-loss statement survived the filter: %s", s.Description)
		}
	}
}
