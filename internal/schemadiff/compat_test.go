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
	fkDrops := 0
	for _, st := range plan.Statements {
		if strings.Contains(st.SQL, "DROP CONSTRAINT") {
			fkDrops++
		}
		if strings.HasPrefix(st.SQL, "DROP TABLE") && fkDrops == 0 {
			t.Fatalf("cyclic table dropped before its FKs: %v", plan.Statements)
		}
	}
	if fkDrops != 2 {
		t.Fatalf("expected 2 intra-cycle FK drops, got %d", fkDrops)
	}
}

// #84 — only FKs pointing inside the cycle are dropped; an FK from a cyclic
// table to an already-dropped (acyclic) or surviving table is left alone.
func TestRenderDeterministic_CycleFKDropsAreScoped(t *testing.T) {
	diff := Diff{
		RemovedTables: []driver.Table{
			{
				Name:    "a",
				Columns: []driver.Column{{Name: "id", DataType: "int"}},
				ForeignKeys: []driver.ForeignKey{
					{Name: "fk_a_b", Columns: []string{"b_id"}, RefTable: "b", RefColumns: []string{"id"}},
					{Name: "fk_a_lookup", Columns: []string{"l_id"}, RefTable: "lookup", RefColumns: []string{"id"}},
				},
			},
			{
				Name:    "b",
				Columns: []driver.Column{{Name: "id", DataType: "int"}},
				ForeignKeys: []driver.ForeignKey{
					{Name: "fk_b_a", Columns: []string{"a_id"}, RefTable: "a", RefColumns: []string{"id"}},
				},
			},
			{
				// Acyclic: must drop before any FK-drop statements appear.
				Name:    "lookup",
				Columns: []driver.Column{{Name: "id", DataType: "int"}},
			},
		},
	}
	plan, err := RenderDeterministic(diff, "public", "postgres")
	if err != nil {
		t.Fatalf("RenderDeterministic: %v", err)
	}
	var tableOrder []string
	fkDrops := 0
	for _, st := range plan.Statements {
		if strings.Contains(st.SQL, "fk_a_lookup") {
			t.Fatalf("out-of-cycle FK dropped: %q", st.SQL)
		}
		if strings.Contains(st.SQL, "DROP CONSTRAINT") {
			fkDrops++
		}
		if strings.HasPrefix(st.SQL, "DROP TABLE") {
			tableOrder = append(tableOrder, st.Table)
		}
	}
	if fkDrops != 2 {
		t.Fatalf("expected 2 intra-cycle FK drops, got %d", fkDrops)
	}
	// lookup is referenced by cycle member a, so it can only drop after the
	// cycle is broken and a is gone.
	want := []string{"a", "b", "lookup"}
	if strings.Join(tableOrder, ",") != strings.Join(want, ",") {
		t.Fatalf("drop order = %v, want %v", tableOrder, want)
	}
}

func TestOrderTablesForDrop_SelfReferenceIsNotACycle(t *testing.T) {
	tables := []driver.Table{{
		Name: "employees",
		ForeignKeys: []driver.ForeignKey{
			{Name: "fk_mgr", Columns: []string{"manager_id"}, RefTable: "employees", RefColumns: []string{"id"}},
		},
	}}
	actions := orderTablesForDrop(tables)
	if len(actions) != 1 || actions[0].dropFK != nil {
		t.Fatalf("self-reference treated as cycle: %+v", actions)
	}
}

func intp(v int) *int { return &v }

// #88/#78 — pre-v3 snapshots lack DatetimePrecision; backfill prevents
// spurious diffs, while v3 snapshots still detect real fsp changes.
func TestCompute_DatetimePrecisionVersioning(t *testing.T) {
	oldSnap := Snapshot{
		Version: 2,
		Tables: []driver.Table{{
			Name:    "events",
			Columns: []driver.Column{{Name: "at", DataType: "datetime"}},
		}},
	}
	curr := Snapshot{
		Version: CurrentSnapshotVersion,
		Tables: []driver.Table{{
			Name:    "events",
			Columns: []driver.Column{{Name: "at", DataType: "datetime", DatetimePrecision: intp(3)}},
		}},
	}
	if d := Compute(oldSnap, curr); !d.IsEmpty() {
		t.Fatalf("v2 snapshot produced spurious fsp diff: %+v", d.ChangedTables)
	}

	prev := Snapshot{
		Version: CurrentSnapshotVersion,
		Tables: []driver.Table{{
			Name:    "events",
			Columns: []driver.Column{{Name: "at", DataType: "datetime", DatetimePrecision: intp(0)}},
		}},
	}
	if d := Compute(prev, curr); len(d.ChangedTables) != 1 {
		t.Fatalf("real fsp change not detected: %+v", d)
	}
}

// #101 — pre-v4 snapshots lack DisplayWidth; backfill prevents spurious
// diffs on tinyint(1) columns, while v4 snapshots still detect real changes.
func TestCompute_DisplayWidthVersioning(t *testing.T) {
	oldSnap := Snapshot{
		Version: 3,
		Tables: []driver.Table{{
			Name:    "flags",
			Columns: []driver.Column{{Name: "active", DataType: "tinyint"}},
		}},
	}
	curr := Snapshot{
		Version: CurrentSnapshotVersion,
		Tables: []driver.Table{{
			Name:    "flags",
			Columns: []driver.Column{{Name: "active", DataType: "tinyint", DisplayWidth: 1}},
		}},
	}
	if d := Compute(oldSnap, curr); !d.IsEmpty() {
		t.Fatalf("v3 snapshot produced spurious display-width diff: %+v", d.ChangedTables)
	}

	prev := Snapshot{
		Version: CurrentSnapshotVersion,
		Tables: []driver.Table{{
			Name:    "flags",
			Columns: []driver.Column{{Name: "active", DataType: "tinyint"}},
		}},
	}
	if d := Compute(prev, curr); len(d.ChangedTables) != 1 {
		t.Fatalf("real display-width change not detected: %+v", d)
	}
}
