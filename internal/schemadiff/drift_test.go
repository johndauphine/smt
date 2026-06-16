package schemadiff

import (
	"strings"
	"testing"

	"smt/internal/driver"
)

func dcol(name, dt string, opts ...func(*driver.Column)) driver.Column {
	c := driver.Column{Name: name, DataType: dt, IsNullable: true}
	for _, o := range opts {
		o(&c)
	}
	return c
}

func dtbl(name string, cols ...driver.Column) driver.Table {
	return driver.Table{Name: name, Columns: cols}
}

// Identical schemas (cross-dialect equivalent types) must report no drift:
// mssql varchar(20) ≡ pg character varying(20), datetime2 ≡ timestamp, etc.
func TestComputeDrift_NoFalsePositiveCrossDialect(t *testing.T) {
	desired := []driver.Table{
		dtbl("Users",
			dcol("Id", "int", func(c *driver.Column) { c.IsIdentity = true; c.IsNullable = false }),
			dcol("Name", "varchar", func(c *driver.Column) { c.MaxLength = 20; c.IsNullable = false }),
			dcol("CreatedAt", "datetime2"),
		),
	}
	existing := []driver.Table{
		dtbl("users",
			dcol("id", "integer", func(c *driver.Column) { c.IsIdentity = true; c.IsNullable = false }),
			dcol("name", "character varying", func(c *driver.Column) { c.MaxLength = 20; c.IsNullable = false }),
			dcol("createdat", "timestamp without time zone"),
		),
	}
	d := ComputeDrift(desired, existing, "mssql", "postgres", DefaultDriftOptions())
	if !d.IsEmpty() {
		t.Fatalf("expected no drift for equivalent schemas, got: missing=%v extra=%v changed=%+v",
			d.MissingTables, d.ExtraTables, d.ChangedTables)
	}
	if d.Summary() != "no drift: target matches the source-derived schema" {
		t.Errorf("unexpected summary: %q", d.Summary())
	}
}

func TestComputeDrift_MissingAndExtraTables(t *testing.T) {
	desired := []driver.Table{dtbl("Orders", dcol("Id", "int")), dtbl("Items", dcol("Id", "int"))}
	existing := []driver.Table{dtbl("orders", dcol("id", "integer")), dtbl("legacy", dcol("id", "integer"))}

	d := ComputeDrift(desired, existing, "mssql", "postgres", DefaultDriftOptions())
	if len(d.MissingTables) != 1 || d.MissingTables[0] != "items" {
		t.Errorf("missing tables = %v, want [items]", d.MissingTables)
	}
	if len(d.ExtraTables) != 1 || d.ExtraTables[0] != "legacy" {
		t.Errorf("extra tables = %v, want [legacy]", d.ExtraTables)
	}
	if !d.HasDestructiveDrift() {
		t.Error("an extra target table is destructive drift")
	}
}

func TestComputeDrift_ColumnLevel(t *testing.T) {
	desired := []driver.Table{dtbl("Users",
		dcol("Id", "int", func(c *driver.Column) { c.IsNullable = false }),
		dcol("Email", "varchar", func(c *driver.Column) { c.MaxLength = 100 }),
		dcol("Age", "int"),
	)}
	existing := []driver.Table{dtbl("users",
		dcol("id", "integer", func(c *driver.Column) { c.IsNullable = false }),
		// Email missing on target.
		dcol("age", "integer"),
		// Extra target column not in source.
		dcol("legacy_flag", "boolean"),
	)}

	d := ComputeDrift(desired, existing, "mssql", "postgres", DefaultDriftOptions())
	if len(d.ChangedTables) != 1 {
		t.Fatalf("expected 1 changed table, got %d: %+v", len(d.ChangedTables), d.ChangedTables)
	}
	td := d.ChangedTables[0]
	if len(td.MissingColumns) != 1 || td.MissingColumns[0] != "email" {
		t.Errorf("missing columns = %v, want [email]", td.MissingColumns)
	}
	if len(td.ExtraColumns) != 1 || td.ExtraColumns[0] != "legacy_flag" {
		t.Errorf("extra columns = %v, want [legacy_flag]", td.ExtraColumns)
	}
	if !d.HasDestructiveDrift() {
		t.Error("an extra target column is destructive drift")
	}
}

// A type/length/nullability change on a matched column surfaces as a
// ColumnDelta, not a missing/extra column.
func TestComputeDrift_ColumnMetadataDelta(t *testing.T) {
	desired := []driver.Table{dtbl("Users", dcol("Code", "varchar", func(c *driver.Column) { c.MaxLength = 20; c.IsNullable = false }))}
	existing := []driver.Table{dtbl("users", dcol("code", "character varying", func(c *driver.Column) { c.MaxLength = 10; c.IsNullable = false }))}

	d := ComputeDrift(desired, existing, "mssql", "postgres", DefaultDriftOptions())
	if len(d.ChangedTables) != 1 {
		t.Fatalf("expected 1 changed table, got %d", len(d.ChangedTables))
	}
	td := d.ChangedTables[0]
	if len(td.ColumnDeltas) == 0 {
		t.Fatal("expected a column delta for the length change")
	}
	joined := strings.Join(td.ColumnDeltas, " ")
	if !strings.Contains(joined, "max_length") {
		t.Errorf("column delta did not mention max_length: %v", td.ColumnDeltas)
	}
	if len(td.MissingColumns) != 0 || len(td.ExtraColumns) != 0 {
		t.Errorf("metadata change misclassified as missing/extra: %+v", td)
	}
	if d.HasDestructiveDrift() {
		t.Error("a length change is not destructive drift (no drops)")
	}
}

// Output ordering must be deterministic for stable reports / CI diffs.
func TestComputeDrift_StableOrdering(t *testing.T) {
	desired := []driver.Table{dtbl("Bravo", dcol("Id", "int")), dtbl("Alpha", dcol("Id", "int")), dtbl("Charlie", dcol("Id", "int"))}
	existing := []driver.Table{}
	first := ComputeDrift(desired, existing, "mssql", "postgres", DefaultDriftOptions()).MissingTables
	for i := 0; i < 10; i++ {
		got := ComputeDrift(desired, existing, "mssql", "postgres", DefaultDriftOptions()).MissingTables
		if strings.Join(got, ",") != strings.Join(first, ",") {
			t.Fatalf("unstable ordering: %v vs %v", got, first)
		}
	}
	if strings.Join(first, ",") != "alpha,bravo,charlie" {
		t.Errorf("missing tables not sorted: %v", first)
	}
}

// Index, FK, and CHECK drift on a table present on both sides, matched by
// column set / signature so renderer-normalized names don't cause false drift.
func TestComputeDrift_IndexFKCheck(t *testing.T) {
	desired := []driver.Table{{
		Name:    "Orders",
		Columns: []driver.Column{dcol("Id", "int"), dcol("CustomerId", "int")},
		Indexes: []driver.Index{
			{Name: "IX_Orders_Customer", Columns: []string{"CustomerId"}}, // present both sides
			{Name: "IX_Orders_Id", Columns: []string{"Id"}},               // missing on target
		},
		ForeignKeys: []driver.ForeignKey{
			{Name: "FK_Orders_Cust", Columns: []string{"CustomerId"}, RefTable: "Customers", RefColumns: []string{"Id"}},
		},
		CheckConstraints: []driver.CheckConstraint{{Name: "CK_a", Definition: "Id > 0"}, {Name: "CK_b", Definition: "CustomerId > 0"}},
	}}
	existing := []driver.Table{{
		Name:    "orders",
		Columns: []driver.Column{dcol("id", "integer"), dcol("customerid", "integer")},
		Indexes: []driver.Index{
			{Name: "ix_orders_customer", Columns: []string{"customerid"}},    // matches desired by column set
			{Name: "ix_orders_extra", Columns: []string{"id", "customerid"}}, // extra on target
		},
		// FK dropped on target.
		ForeignKeys: nil,
		// One CHECK dropped on target.
		CheckConstraints: []driver.CheckConstraint{{Name: "ck_a", Definition: "id > 0"}},
	}}

	d := ComputeDrift(desired, existing, "mssql", "postgres", DefaultDriftOptions())
	if len(d.ChangedTables) != 1 {
		t.Fatalf("expected 1 changed table, got %d: %+v", len(d.ChangedTables), d.ChangedTables)
	}
	td := d.ChangedTables[0]
	if len(td.MissingIndexes) != 1 || td.MissingIndexes[0] != "id" {
		t.Errorf("missing indexes = %v, want [id]", td.MissingIndexes)
	}
	if len(td.ExtraIndexes) != 1 || td.ExtraIndexes[0] != "id,customerid" {
		t.Errorf("extra indexes = %v, want [id,customerid]", td.ExtraIndexes)
	}
	if len(td.MissingForeignKeys) != 1 || td.MissingForeignKeys[0] != "customerid:id->self.customers|noaction|noaction" {
		t.Errorf("missing FKs = %v, want [customerid:id->customers|noaction|noaction]", td.MissingForeignKeys)
	}
	if td.CheckDrift == "" {
		t.Error("expected check drift (target dropped a CHECK)")
	}
	// Index/FK/check-only drops are not data-loss destructive (no row loss),
	// so HasDestructiveDrift stays false here (no extra columns/tables).
	if d.HasDestructiveDrift() {
		t.Error("index/FK/check drift should not be classified as destructive (no data loss)")
	}
}

// A matched column whose type changes family (int -> text) must drift even
// though CompareColumns sees no length/precision/nullability delta. Legitimate
// cross-family equivalences (bit -> boolean) must NOT false-flag.
func TestComputeDrift_TypeFamilyMismatch(t *testing.T) {
	desired := []driver.Table{dtbl("T",
		dcol("a", "int"),
		dcol("flag", "bit"),
		dcol("name", "varchar", func(c *driver.Column) { c.MaxLength = 20 }),
	)}
	existing := []driver.Table{dtbl("t",
		dcol("a", "text"),       // numeric -> string: drift
		dcol("flag", "boolean"), // bit -> boolean: equivalent, no drift
		dcol("name", "character varying", func(c *driver.Column) { c.MaxLength = 20 }), // equivalent
	)}
	d := ComputeDrift(desired, existing, "mssql", "postgres", DefaultDriftOptions())
	if len(d.ChangedTables) != 1 {
		t.Fatalf("expected 1 changed table, got %d: %+v", len(d.ChangedTables), d.ChangedTables)
	}
	deltas := strings.Join(d.ChangedTables[0].ColumnDeltas, "\n")
	if !strings.Contains(deltas, "a: type") {
		t.Errorf("missing int->text canonical type delta, got: %v", d.ChangedTables[0].ColumnDeltas)
	}
	if strings.Contains(deltas, "flag") {
		t.Errorf("bit->boolean false-flagged as type drift: %v", d.ChangedTables[0].ColumnDeltas)
	}
}

// Composite index column ORDER is significant; FK referential ACTIONS matter.
func TestComputeDrift_IndexOrderAndFKActions(t *testing.T) {
	desired := []driver.Table{{
		Name:    "T",
		Columns: []driver.Column{dcol("a", "int"), dcol("b", "int")},
		Indexes: []driver.Index{{Name: "ix", Columns: []string{"a", "b"}}},
		ForeignKeys: []driver.ForeignKey{
			{Name: "fk", Columns: []string{"a"}, RefTable: "P", RefColumns: []string{"id"}, OnDelete: "CASCADE"},
		},
	}}
	existing := []driver.Table{{
		Name:    "t",
		Columns: []driver.Column{dcol("a", "integer"), dcol("b", "integer")},
		Indexes: []driver.Index{{Name: "ix", Columns: []string{"b", "a"}}}, // reversed order
		ForeignKeys: []driver.ForeignKey{
			{Name: "fk", Columns: []string{"a"}, RefTable: "p", RefColumns: []string{"id"}, OnDelete: "NO ACTION"}, // action changed
		},
	}}
	d := ComputeDrift(desired, existing, "mssql", "postgres", DefaultDriftOptions())
	if len(d.ChangedTables) != 1 {
		t.Fatalf("expected 1 changed table, got %d", len(d.ChangedTables))
	}
	td := d.ChangedTables[0]
	if len(td.MissingIndexes) != 1 || len(td.ExtraIndexes) != 1 {
		t.Errorf("reversed-order index should drift: missing=%v extra=%v", td.MissingIndexes, td.ExtraIndexes)
	}
	if len(td.MissingForeignKeys) != 1 || len(td.ExtraForeignKeys) != 1 {
		t.Errorf("changed FK action should drift: missing=%v extra=%v", td.MissingForeignKeys, td.ExtraForeignKeys)
	}
}

// NormalizeIdentifiers folds all identifiers (including index/FK column lists
// and referenced tables) and must not mutate the input.
func TestNormalizeIdentifiers_FullAndPure(t *testing.T) {
	in := []driver.Table{{
		Name:       "Orders",
		Columns:    []driver.Column{{Name: "OrderID"}},
		PrimaryKey: []string{"OrderID"},
		Indexes:    []driver.Index{{Name: "IX", Columns: []string{"OrderID"}}},
		ForeignKeys: []driver.ForeignKey{
			{Name: "FK", Columns: []string{"OrderID"}, RefTable: "Customers", RefColumns: []string{"CustID"}},
		},
	}}
	out := NormalizeIdentifiers(in, func(s string) string { return strings.ToLower(s) })

	// Input untouched.
	if in[0].Name != "Orders" || in[0].Indexes[0].Columns[0] != "OrderID" || in[0].ForeignKeys[0].RefTable != "Customers" {
		t.Errorf("NormalizeIdentifiers mutated its input: %+v", in[0])
	}
	// Output fully normalized.
	o := out[0]
	if o.Name != "orders" || o.Columns[0].Name != "orderid" || o.Indexes[0].Columns[0] != "orderid" ||
		o.ForeignKeys[0].RefTable != "customers" || o.ForeignKeys[0].RefColumns[0] != "custid" {
		t.Errorf("NormalizeIdentifiers did not fold all identifiers: %+v", o)
	}
}

// A dropped or re-keyed primary key must drift — the index loaders exclude
// PK-backed indexes, so it's checked directly.
func TestComputeDrift_PrimaryKey(t *testing.T) {
	desired := []driver.Table{{
		Name:       "Users",
		Columns:    []driver.Column{dcol("Id", "int"), dcol("Email", "varchar")},
		PrimaryKey: []string{"Id"},
	}}
	// Target lost its PK.
	existing := []driver.Table{{
		Name:       "users",
		Columns:    []driver.Column{dcol("id", "integer"), dcol("email", "character varying")},
		PrimaryKey: nil,
	}}
	d := ComputeDrift(desired, existing, "mssql", "postgres", DefaultDriftOptions())
	if len(d.ChangedTables) != 1 || d.ChangedTables[0].PKDrift == "" {
		t.Fatalf("dropped PK should drift, got %+v", d.ChangedTables)
	}

	// Same PK (case-insensitive) → no drift.
	existing[0].PrimaryKey = []string{"id"}
	if d := ComputeDrift(desired, existing, "mssql", "postgres", DefaultDriftOptions()); !d.IsEmpty() {
		t.Errorf("equal PK should not drift, got %+v", d.ChangedTables)
	}
}

// DriftOptions gates unmanaged object kinds: with index/FK/check comparison
// off, differences in those kinds must not surface.
func TestComputeDrift_OptionsGateUnmanagedKinds(t *testing.T) {
	desired := []driver.Table{{
		Name:             "Orders",
		Columns:          []driver.Column{dcol("Id", "int")},
		Indexes:          []driver.Index{{Name: "ix", Columns: []string{"Id"}}},
		ForeignKeys:      []driver.ForeignKey{{Name: "fk", Columns: []string{"Id"}, RefTable: "P", RefColumns: []string{"id"}}},
		CheckConstraints: []driver.CheckConstraint{{Name: "ck", Definition: "Id > 0"}},
	}}
	existing := []driver.Table{{
		Name:    "orders",
		Columns: []driver.Column{dcol("id", "integer")},
		// No indexes/FKs/checks on the target.
	}}
	// All comparisons off → no drift despite the missing index/FK/check.
	off := DriftOptions{CompareIndexes: false, CompareForeignKeys: false, CompareChecks: false}
	if d := ComputeDrift(desired, existing, "mssql", "postgres", off); !d.IsEmpty() {
		t.Errorf("gated-off kinds should not drift, got %+v", d.ChangedTables)
	}
	// All on → drift across all three kinds.
	if d := ComputeDrift(desired, existing, "mssql", "postgres", DefaultDriftOptions()); d.IsEmpty() {
		t.Error("with comparison on, missing index/FK/check should drift")
	}
}

// Computed-column presence and storage class are cross-dialect structural
// facts drift must catch even when type/length/nullability are unchanged.
func TestComputeDrift_ComputedColumn(t *testing.T) {
	// Source column is generated; target made it a plain column.
	desired := []driver.Table{{Name: "T", Columns: []driver.Column{
		{Name: "total", DataType: "int", IsComputed: true, ComputedExpression: "a+b", ComputedPersisted: true},
	}}}
	existing := []driver.Table{{Name: "t", Columns: []driver.Column{
		{Name: "total", DataType: "integer", IsComputed: false},
	}}}
	d := ComputeDrift(desired, existing, "mssql", "postgres", DefaultDriftOptions())
	if len(d.ChangedTables) != 1 || len(d.ChangedTables[0].ColumnDeltas) == 0 {
		t.Fatalf("computed→plain should drift, got %+v", d.ChangedTables)
	}
	if !strings.Contains(strings.Join(d.ChangedTables[0].ColumnDeltas, " "), "computed") {
		t.Errorf("missing computed delta: %v", d.ChangedTables[0].ColumnDeltas)
	}

	// Both computed but storage class changed (persisted → virtual). Use an
	// MSSQL target, which can represent both — a PG target can't (always
	// STORED), so storage is not compared there (see the PG-virtual case below).
	desired[0].Columns[0].IsComputed, existing[0].Columns[0].IsComputed = true, true
	existing[0].Columns[0].ComputedPersisted = false
	d = ComputeDrift(desired, existing, "mssql", "mssql", DefaultDriftOptions())
	if len(d.ChangedTables) != 1 || !strings.Contains(strings.Join(d.ChangedTables[0].ColumnDeltas, " "), "storage") {
		t.Errorf("storage-class change should drift on an mssql target, got %+v", d.ChangedTables)
	}

	// Both computed, same storage → no drift.
	existing[0].Columns[0].ComputedPersisted = true
	if d := ComputeDrift(desired, existing, "mssql", "mssql", DefaultDriftOptions()); !d.IsEmpty() {
		t.Errorf("equivalent computed columns should not drift, got %+v", d.ChangedTables)
	}
}

// PostgreSQL generated columns are always STORED, so a source VIRTUAL computed
// column landing as STORED on a freshly created PG target is NOT drift — the
// storage class must not be compared for PG targets (#115 review).
func TestComputeDrift_PostgresVirtualComputedNoDrift(t *testing.T) {
	desired := []driver.Table{{Name: "T", Columns: []driver.Column{
		{Name: "margin", DataType: "decimal", IsComputed: true, ComputedExpression: "a-b", ComputedPersisted: false}, // VIRTUAL source
	}}}
	existing := []driver.Table{{Name: "t", Columns: []driver.Column{
		{Name: "margin", DataType: "numeric", IsComputed: true, ComputedExpression: "a-b", ComputedPersisted: true}, // PG forces STORED
	}}}
	if d := ComputeDrift(desired, existing, "mysql", "postgres", DefaultDriftOptions()); !d.IsEmpty() {
		t.Errorf("PG STORED of a VIRTUAL source must not drift, got %+v", d.ChangedTables)
	}
}

// A covering index that loses its INCLUDE columns, or an index that loses its
// filter, must drift even though the key columns match.
func TestComputeDrift_IndexIncludeAndFilter(t *testing.T) {
	desired := []driver.Table{{
		Name:    "T",
		Columns: []driver.Column{dcol("a", "int"), dcol("b", "int")},
		Indexes: []driver.Index{
			{Name: "cover", Columns: []string{"a"}, IncludeCols: []string{"b"}},
			{Name: "filt", Columns: []string{"a"}, Filter: "a > 0"},
		},
	}}
	existing := []driver.Table{{
		Name:    "t",
		Columns: []driver.Column{dcol("a", "integer"), dcol("b", "integer")},
		Indexes: []driver.Index{
			{Name: "cover", Columns: []string{"a"}}, // lost INCLUDE
			{Name: "filt", Columns: []string{"a"}},  // lost filter
		},
	}}
	d := ComputeDrift(desired, existing, "mssql", "postgres", DefaultDriftOptions())
	if len(d.ChangedTables) != 1 {
		t.Fatalf("expected drift, got %+v", d.ChangedTables)
	}
	// Both desired index variants are "missing" (their keyed forms aren't on
	// target) and both bare target indexes are "extra".
	td := d.ChangedTables[0]
	if len(td.MissingIndexes) != 2 || len(td.ExtraIndexes) != 2 {
		t.Errorf("INCLUDE/filter loss should drift: missing=%v extra=%v", td.MissingIndexes, td.ExtraIndexes)
	}
}

// FK referenced-schema comparison is schema-relative: a same-schema reference
// (RefSchema equal to the owning table's schema, or empty) reads as "self" and
// compares equal across dialects even when the literal schema names differ
// (source "dbo" vs target "public"); a genuine cross-schema reference that
// changes drifts.
func TestComputeDrift_FKRefSchemaRelative(t *testing.T) {
	// Same-schema FK on both sides but with different literal schema names —
	// must NOT drift (both are "self").
	desired := []driver.Table{{
		Name: "Orders", Schema: "dbo",
		Columns: []driver.Column{dcol("cust", "int")},
		ForeignKeys: []driver.ForeignKey{
			{Name: "fk", Columns: []string{"cust"}, RefSchema: "dbo", RefTable: "Customers", RefColumns: []string{"id"}},
		},
	}}
	existing := []driver.Table{{
		Name: "orders", Schema: "public",
		Columns: []driver.Column{dcol("cust", "integer")},
		ForeignKeys: []driver.ForeignKey{
			{Name: "fk", Columns: []string{"cust"}, RefSchema: "public", RefTable: "customers", RefColumns: []string{"id"}},
		},
	}}
	if d := ComputeDrift(desired, existing, "mssql", "postgres", DefaultDriftOptions()); !d.IsEmpty() {
		t.Fatalf("same-schema FK across dialects should not drift, got %+v", d.ChangedTables)
	}

	// Target FK drifted to reference a different (cross-)schema → must drift.
	existing[0].ForeignKeys[0].RefSchema = "legacy"
	d := ComputeDrift(desired, existing, "mssql", "postgres", DefaultDriftOptions())
	if len(d.ChangedTables) != 1 || len(d.ChangedTables[0].MissingForeignKeys) == 0 {
		t.Fatalf("cross-schema FK drift should be detected, got %+v", d.ChangedTables)
	}
}

// A target-only CHECK (target has MORE checks than the source) is drift too —
// it can reject rows the source allows.
func TestComputeDrift_TargetOnlyCheck(t *testing.T) {
	desired := []driver.Table{{Name: "T", Columns: []driver.Column{dcol("a", "int")}}}
	existing := []driver.Table{{
		Name:             "t",
		Columns:          []driver.Column{dcol("a", "integer")},
		CheckConstraints: []driver.CheckConstraint{{Name: "ck", Definition: "a > 0"}},
	}}
	d := ComputeDrift(desired, existing, "mssql", "postgres", DefaultDriftOptions())
	if len(d.ChangedTables) != 1 || d.ChangedTables[0].CheckDrift == "" {
		t.Fatalf("target-only CHECK should drift, got %+v", d.ChangedTables)
	}
}

// RetargetSchema collapses table + FK schema references to the target schema
// (mirroring create/sync) without mutating the input.
func TestRetargetSchema(t *testing.T) {
	in := []driver.Table{{
		Name: "Orders", Schema: "dbo",
		ForeignKeys: []driver.ForeignKey{{Name: "fk", RefSchema: "other", RefTable: "C"}},
	}}
	out := RetargetSchema(in, "public")
	if out[0].Schema != "public" || out[0].ForeignKeys[0].RefSchema != "public" {
		t.Errorf("RetargetSchema did not collapse to target schema: %+v", out[0])
	}
	if in[0].Schema != "dbo" || in[0].ForeignKeys[0].RefSchema != "other" {
		t.Errorf("RetargetSchema mutated its input: %+v", in[0])
	}
	// Empty target schema → pass-through (values unchanged).
	if got := RetargetSchema(in, ""); got[0].Schema != "dbo" {
		t.Errorf("empty target schema should pass through, got %q", got[0].Schema)
	}
}

// MySQL INT UNSIGNED → INT (same-dialect) must drift on the structured
// unsigned flag; cross-dialect (mysql→pg) must NOT, since unsigned is
// absorbed into a wider target type.
func TestComputeDrift_MySQLUnsigned(t *testing.T) {
	src := []driver.Table{{Name: "T", Columns: []driver.Column{{Name: "n", DataType: "int", IsUnsigned: true}}}}
	tgtSigned := []driver.Table{{Name: "t", Columns: []driver.Column{{Name: "n", DataType: "int", IsUnsigned: false}}}}

	d := ComputeDrift(src, tgtSigned, "mysql", "mysql", DefaultDriftOptions())
	if len(d.ChangedTables) != 1 || !strings.Contains(strings.Join(d.ChangedTables[0].ColumnDeltas, " "), "type") {
		t.Errorf("mysql int unsigned → int should drift, got %+v", d.ChangedTables)
	}
	// Cross-dialect: pg bigint target carries no unsigned flag — must not drift.
	tgtPG := []driver.Table{{Name: "t", Columns: []driver.Column{{Name: "n", DataType: "bigint", IsUnsigned: false}}}}
	if d := ComputeDrift(src, tgtPG, "mysql", "postgres", DefaultDriftOptions()); !d.IsEmpty() {
		t.Errorf("mysql int unsigned → pg bigint must not drift, got %+v", d.ChangedTables)
	}
}
