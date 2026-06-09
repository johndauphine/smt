package schemadiff

import (
	"strings"
	"testing"

	"smt/internal/driver"
)

// #78 — a pre-v2 snapshot (no IsUnsigned/EnumValues/OnUpdateExpression) must
// not diff those fields against a fresh extraction that has them.
func TestCompute_PreV2SnapshotDoesNotSpuriouslyDiff(t *testing.T) {
	prev := Snapshot{
		// Version 0: stored before the v2 fields existed.
		Tables: []driver.Table{{
			Name: "orders",
			Columns: []driver.Column{
				{Name: "id", DataType: "int"},
				{Name: "status", DataType: "enum"},
				{Name: "updated_at", DataType: "timestamp"},
			},
		}},
	}
	curr := Snapshot{
		Version: CurrentSnapshotVersion,
		Tables: []driver.Table{{
			Name: "orders",
			Columns: []driver.Column{
				{Name: "id", DataType: "int", IsUnsigned: true},
				{Name: "status", DataType: "enum", EnumValues: []string{"new", "shipped"}},
				{Name: "updated_at", DataType: "timestamp", OnUpdateExpression: "CURRENT_TIMESTAMP"},
			},
		}},
	}

	diff := Compute(prev, curr)
	if !diff.IsEmpty() {
		t.Fatalf("pre-v2 snapshot produced a spurious diff: %+v", diff.ChangedTables)
	}
}

// #78 — version-aware comparison still detects real changes between two
// current-version snapshots.
func TestCompute_CurrentVersionStillDetectsUnsignedChange(t *testing.T) {
	prev := Snapshot{
		Version: CurrentSnapshotVersion,
		Tables: []driver.Table{{
			Name:    "orders",
			Columns: []driver.Column{{Name: "id", DataType: "int"}},
		}},
	}
	curr := Snapshot{
		Version: CurrentSnapshotVersion,
		Tables: []driver.Table{{
			Name:    "orders",
			Columns: []driver.Column{{Name: "id", DataType: "int", IsUnsigned: true}},
		}},
	}

	diff := Compute(prev, curr)
	if len(diff.ChangedTables) != 1 || len(diff.ChangedTables[0].ChangedColumns) != 1 {
		t.Fatalf("unsigned change not detected: %+v", diff)
	}
}

// #78 — backfill must not mutate the caller's snapshot.
func TestBackfillDoesNotMutateInput(t *testing.T) {
	prev := Snapshot{
		Tables: []driver.Table{{
			Name:    "orders",
			Columns: []driver.Column{{Name: "id", DataType: "int"}},
		}},
	}
	curr := Snapshot{
		Version: CurrentSnapshotVersion,
		Tables: []driver.Table{{
			Name:    "orders",
			Columns: []driver.Column{{Name: "id", DataType: "int", IsUnsigned: true}},
		}},
	}
	_ = Compute(prev, curr)
	if prev.Tables[0].Columns[0].IsUnsigned {
		t.Fatal("Compute mutated the caller's snapshot")
	}
}

// #84 — removed tables drop children-first, with IF EXISTS.
func TestRenderDeterministic_RemovedTablesDropChildrenFirst(t *testing.T) {
	diff := Diff{
		RemovedTables: []driver.Table{
			{
				// Alphabetically first, but it's the parent — must drop last.
				Name:    "accounts",
				Columns: []driver.Column{{Name: "id", DataType: "int"}},
			},
			{
				Name:    "invoices",
				Columns: []driver.Column{{Name: "id", DataType: "int"}},
				ForeignKeys: []driver.ForeignKey{
					{Name: "fk_invoices_accounts", Columns: []string{"account_id"}, RefTable: "accounts", RefColumns: []string{"id"}},
				},
			},
		},
	}
	plan, err := RenderDeterministic(diff, "public", "postgres")
	if err != nil {
		t.Fatalf("RenderDeterministic: %v", err)
	}
	var drops []string
	for _, st := range plan.Statements {
		if strings.HasPrefix(st.SQL, "DROP TABLE") {
			if !strings.Contains(st.SQL, "IF EXISTS") {
				t.Errorf("drop without IF EXISTS: %q", st.SQL)
			}
			drops = append(drops, st.Table)
		}
	}
	if len(drops) != 2 || drops[0] != "invoices" || drops[1] != "accounts" {
		t.Fatalf("drop order = %v, want [invoices accounts]", drops)
	}
}

// #84 — an FK cycle among removed tables gets its FKs dropped first.
func TestRenderDeterministic_RemovedTableCycleDropsFKsFirst(t *testing.T) {
	diff := Diff{
		RemovedTables: []driver.Table{
			{
				Name:    "a",
				Columns: []driver.Column{{Name: "id", DataType: "int"}},
				ForeignKeys: []driver.ForeignKey{
					{Name: "fk_a_b", Columns: []string{"b_id"}, RefTable: "b", RefColumns: []string{"id"}},
				},
			},
			{
				Name:    "b",
				Columns: []driver.Column{{Name: "id", DataType: "int"}},
				ForeignKeys: []driver.ForeignKey{
					{Name: "fk_b_a", Columns: []string{"a_id"}, RefTable: "a", RefColumns: []string{"id"}},
				},
			},
		},
	}
	plan, err := RenderDeterministic(diff, "public", "postgres")
	if err != nil {
		t.Fatalf("RenderDeterministic: %v", err)
	}
	sawFKDrop := false
	for _, st := range plan.Statements {
		if strings.Contains(st.SQL, "DROP CONSTRAINT") {
			sawFKDrop = true
		}
		if strings.HasPrefix(st.SQL, "DROP TABLE") && !sawFKDrop {
			t.Fatalf("table dropped before cycle FKs: %v", plan.Statements)
		}
	}
	if !sawFKDrop {
		t.Fatal("no FK drop emitted for cycle")
	}
}

func TestOrderTablesForDrop_SelfReferenceIsNotACycle(t *testing.T) {
	tables := []driver.Table{{
		Name: "employees",
		ForeignKeys: []driver.ForeignKey{
			{Name: "fk_mgr", Columns: []string{"manager_id"}, RefTable: "employees", RefColumns: []string{"id"}},
		},
	}}
	ordered, cyclic := orderTablesForDrop(tables)
	if len(cyclic) != 0 || len(ordered) != 1 {
		t.Fatalf("self-reference treated as cycle: ordered=%v cyclic=%v", ordered, cyclic)
	}
}
