package schemadiff

// Compute compares two Snapshots and returns the per-table additions,
// removals, and changes. The diff is intentionally conservative: only
// columns/indexes/FKs/checks present-by-name are compared; renames look
// like drop+add, and the renderer leaves them as such (a rename detector
// would be a separate feature).

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"smt/internal/driver"
)

// Diff is the delta between a previous snapshot and the current source state.
type Diff struct {
	PrevCapturedAt time.Time `json:"prev_captured_at"`
	CurrCapturedAt time.Time `json:"curr_captured_at"`

	AddedTables   []driver.Table `json:"added_tables"`
	RemovedTables []driver.Table `json:"removed_tables"`
	ChangedTables []TableDiff    `json:"changed_tables"`
}

// TableDiff captures all per-table changes in one place.
type TableDiff struct {
	Schema string       `json:"schema"`
	Name   string       `json:"name"`
	Curr   driver.Table `json:"curr"`

	AddedColumns   []driver.Column `json:"added_columns"`
	RemovedColumns []driver.Column `json:"removed_columns"`
	ChangedColumns []ColumnChange  `json:"changed_columns"`

	AddedIndexes   []driver.Index `json:"added_indexes"`
	RemovedIndexes []driver.Index `json:"removed_indexes"`

	AddedForeignKeys   []driver.ForeignKey `json:"added_foreign_keys"`
	RemovedForeignKeys []driver.ForeignKey `json:"removed_foreign_keys"`

	AddedChecks   []driver.CheckConstraint `json:"added_checks"`
	RemovedChecks []driver.CheckConstraint `json:"removed_checks"`
}

// ColumnChange describes a column that exists in both snapshots but with
// different attributes (type, nullability, length, etc.).
type ColumnChange struct {
	Name string        `json:"name"`
	Old  driver.Column `json:"old"`
	New  driver.Column `json:"new"`
}

// IsEmpty returns true if the diff would produce no DDL.
func (d Diff) IsEmpty() bool {
	if len(d.AddedTables) > 0 || len(d.RemovedTables) > 0 {
		return false
	}
	for _, td := range d.ChangedTables {
		if !td.IsEmpty() {
			return false
		}
	}
	return true
}

// IsEmpty returns true if the per-table diff has no changes.
func (td TableDiff) IsEmpty() bool {
	return len(td.AddedColumns) == 0 &&
		len(td.RemovedColumns) == 0 &&
		len(td.ChangedColumns) == 0 &&
		len(td.AddedIndexes) == 0 &&
		len(td.RemovedIndexes) == 0 &&
		len(td.AddedForeignKeys) == 0 &&
		len(td.RemovedForeignKeys) == 0 &&
		len(td.AddedChecks) == 0 &&
		len(td.RemovedChecks) == 0
}

// Summary returns a short human-readable description of the diff.
func (d Diff) Summary() string {
	if d.IsEmpty() {
		return "no schema changes"
	}
	var b strings.Builder
	if len(d.AddedTables) > 0 {
		fmt.Fprintf(&b, "%d table(s) added, ", len(d.AddedTables))
	}
	if len(d.RemovedTables) > 0 {
		fmt.Fprintf(&b, "%d table(s) removed, ", len(d.RemovedTables))
	}
	if len(d.ChangedTables) > 0 {
		fmt.Fprintf(&b, "%d table(s) changed", len(d.ChangedTables))
	}
	return strings.TrimSuffix(strings.TrimSpace(b.String()), ",")
}

// Normalize rewrites every identifier in the diff (table names, column
// names, index names, FK names + ref columns, check names) through the
// supplied function. Callers use this to align source-original names
// (e.g. MSSQL "Posts") with the target's on-disk convention (e.g.
// PostgreSQL "posts") before handing the diff to the AI renderer, so
// the generated ALTERs hit tables that actually exist on the target.
//
// The transformation is purely structural; data types and other column
// attributes are left alone.
func (d Diff) Normalize(norm func(string) string) Diff {
	out := Diff{
		PrevCapturedAt: d.PrevCapturedAt,
		CurrCapturedAt: d.CurrCapturedAt,
	}
	for _, t := range d.AddedTables {
		out.AddedTables = append(out.AddedTables, normalizeTable(t, norm))
	}
	for _, t := range d.RemovedTables {
		out.RemovedTables = append(out.RemovedTables, normalizeTable(t, norm))
	}
	for _, td := range d.ChangedTables {
		out.ChangedTables = append(out.ChangedTables, normalizeTableDiff(td, norm))
	}
	return out
}

// WithTargetSchema rewrites every schema reference inside the diff to
// the supplied target schema name. The structural diff carries SOURCE
// schema names in two places that the AI renderer reads: Table.Schema
// (the table's own qualifier) and ForeignKey.RefSchema (the referenced
// table's qualifier in inline REFERENCES clauses). The AI honors both
// when emitting qualified DDL — leaving either one as the source value
// produces statements that fail on the target (e.g. MySQL source schema
// "smt_src_test" against MSSQL target schema "dbo": "Cannot find the
// object …", or "REFERENCES smt_src_test.parent" inside a target ADD FK).
//
// SMT migrates source.X to target.Y; we don't preserve cross-schema
// relationships across engines, so every schema reference in the diff
// resolves to the target schema after rewriting.
//
// Parallels Normalize: structural-only transformation, leaves all other
// fields alone. Call after Normalize and before Render.
func (d Diff) WithTargetSchema(targetSchema string) Diff {
	out := Diff{
		PrevCapturedAt: d.PrevCapturedAt,
		CurrCapturedAt: d.CurrCapturedAt,
	}
	for _, t := range d.AddedTables {
		retargetTable(&t, targetSchema)
		out.AddedTables = append(out.AddedTables, t)
	}
	for _, t := range d.RemovedTables {
		retargetTable(&t, targetSchema)
		out.RemovedTables = append(out.RemovedTables, t)
	}
	for _, td := range d.ChangedTables {
		td.Schema = targetSchema
		retargetTable(&td.Curr, targetSchema)
		retargetForeignKeys(td.AddedForeignKeys, targetSchema)
		retargetForeignKeys(td.RemovedForeignKeys, targetSchema)
		out.ChangedTables = append(out.ChangedTables, td)
	}
	return out
}

// retargetTable rewrites the table's own Schema and the RefSchema on
// every inline foreign key. Operates in place on the supplied pointer.
func retargetTable(t *driver.Table, targetSchema string) {
	t.Schema = targetSchema
	retargetForeignKeys(t.ForeignKeys, targetSchema)
}

// retargetForeignKeys rewrites RefSchema on every entry in the supplied
// slice. Slice elements are mutated directly (FKs are values, not
// pointers, so a `for _, fk := range` loop would not stick).
func retargetForeignKeys(fks []driver.ForeignKey, targetSchema string) {
	for i := range fks {
		fks[i].RefSchema = targetSchema
	}
}

func normalizeTable(t driver.Table, norm func(string) string) driver.Table {
	t.Name = norm(t.Name)
	for i := range t.Columns {
		t.Columns[i].Name = norm(t.Columns[i].Name)
	}
	for i := range t.PrimaryKey {
		t.PrimaryKey[i] = norm(t.PrimaryKey[i])
	}
	for i := range t.Indexes {
		t.Indexes[i].Name = norm(t.Indexes[i].Name)
		for j := range t.Indexes[i].Columns {
			t.Indexes[i].Columns[j] = norm(t.Indexes[i].Columns[j])
		}
		for j := range t.Indexes[i].IncludeCols {
			t.Indexes[i].IncludeCols[j] = norm(t.Indexes[i].IncludeCols[j])
		}
	}
	for i := range t.ForeignKeys {
		t.ForeignKeys[i].Name = norm(t.ForeignKeys[i].Name)
		t.ForeignKeys[i].RefTable = norm(t.ForeignKeys[i].RefTable)
		for j := range t.ForeignKeys[i].Columns {
			t.ForeignKeys[i].Columns[j] = norm(t.ForeignKeys[i].Columns[j])
		}
		for j := range t.ForeignKeys[i].RefColumns {
			t.ForeignKeys[i].RefColumns[j] = norm(t.ForeignKeys[i].RefColumns[j])
		}
	}
	for i := range t.CheckConstraints {
		t.CheckConstraints[i].Name = norm(t.CheckConstraints[i].Name)
	}
	return t
}

func normalizeTableDiff(td TableDiff, norm func(string) string) TableDiff {
	out := TableDiff{
		Schema: td.Schema,
		Name:   norm(td.Name),
		Curr:   normalizeTable(td.Curr, norm),
	}
	for _, c := range td.AddedColumns {
		c.Name = norm(c.Name)
		out.AddedColumns = append(out.AddedColumns, c)
	}
	for _, c := range td.RemovedColumns {
		c.Name = norm(c.Name)
		out.RemovedColumns = append(out.RemovedColumns, c)
	}
	for _, cc := range td.ChangedColumns {
		cc.Name = norm(cc.Name)
		cc.Old.Name = norm(cc.Old.Name)
		cc.New.Name = norm(cc.New.Name)
		out.ChangedColumns = append(out.ChangedColumns, cc)
	}
	for _, idx := range td.AddedIndexes {
		idx.Name = norm(idx.Name)
		for j := range idx.Columns {
			idx.Columns[j] = norm(idx.Columns[j])
		}
		out.AddedIndexes = append(out.AddedIndexes, idx)
	}
	for _, idx := range td.RemovedIndexes {
		idx.Name = norm(idx.Name)
		out.RemovedIndexes = append(out.RemovedIndexes, idx)
	}
	for _, fk := range td.AddedForeignKeys {
		fk.Name = norm(fk.Name)
		fk.RefTable = norm(fk.RefTable)
		for j := range fk.Columns {
			fk.Columns[j] = norm(fk.Columns[j])
		}
		for j := range fk.RefColumns {
			fk.RefColumns[j] = norm(fk.RefColumns[j])
		}
		out.AddedForeignKeys = append(out.AddedForeignKeys, fk)
	}
	for _, fk := range td.RemovedForeignKeys {
		fk.Name = norm(fk.Name)
		out.RemovedForeignKeys = append(out.RemovedForeignKeys, fk)
	}
	for _, c := range td.AddedChecks {
		c.Name = norm(c.Name)
		out.AddedChecks = append(out.AddedChecks, c)
	}
	for _, c := range td.RemovedChecks {
		c.Name = norm(c.Name)
		out.RemovedChecks = append(out.RemovedChecks, c)
	}
	return out
}

// Compute compares two snapshots and returns the delta. Tables, columns,
// indexes, FKs, and checks are matched by name; differences in attributes
// (column type/nullability, etc.) become ChangedColumns entries.
func Compute(prev, curr Snapshot) Diff {
	d := Diff{
		PrevCapturedAt: prev.CapturedAt,
		CurrCapturedAt: curr.CapturedAt,
	}

	prevByName := indexTablesByName(prev.Tables)
	currByName := indexTablesByName(curr.Tables)

	for _, name := range sortedKeys(currByName) {
		ct := currByName[name]
		pt, existed := prevByName[name]
		if !existed {
			d.AddedTables = append(d.AddedTables, ct)
			continue
		}
		if td := diffTable(pt, ct); !td.IsEmpty() {
			d.ChangedTables = append(d.ChangedTables, td)
		}
	}

	for _, name := range sortedKeys(prevByName) {
		if _, stillThere := currByName[name]; !stillThere {
			d.RemovedTables = append(d.RemovedTables, prevByName[name])
		}
	}

	return d
}

func indexTablesByName(tables []driver.Table) map[string]driver.Table {
	out := make(map[string]driver.Table, len(tables))
	for _, t := range tables {
		out[t.Name] = t
	}
	return out
}

func sortedKeys(m map[string]driver.Table) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func diffTable(prev, curr driver.Table) TableDiff {
	td := TableDiff{Schema: curr.Schema, Name: curr.Name, Curr: curr}

	td.AddedColumns, td.RemovedColumns, td.ChangedColumns = diffColumns(prev.Columns, curr.Columns)
	td.AddedIndexes, td.RemovedIndexes = diffIndexes(prev.Indexes, curr.Indexes)
	td.AddedForeignKeys, td.RemovedForeignKeys = diffForeignKeys(prev.ForeignKeys, curr.ForeignKeys)
	td.AddedChecks, td.RemovedChecks = diffChecks(prev.CheckConstraints, curr.CheckConstraints)

	return td
}

func diffColumns(prev, curr []driver.Column) (added, removed []driver.Column, changed []ColumnChange) {
	prevByName := make(map[string]driver.Column, len(prev))
	for _, c := range prev {
		prevByName[c.Name] = c
	}
	currByName := make(map[string]driver.Column, len(curr))
	for _, c := range curr {
		currByName[c.Name] = c
	}

	for _, c := range curr {
		old, existed := prevByName[c.Name]
		if !existed {
			added = append(added, c)
			continue
		}
		if !columnsEqual(old, c) {
			changed = append(changed, ColumnChange{Name: c.Name, Old: old, New: c})
		}
	}
	for _, c := range prev {
		if _, stillThere := currByName[c.Name]; !stillThere {
			removed = append(removed, c)
		}
	}
	return added, removed, changed
}

// columnsEqual compares two columns on the structural attributes that
// matter for DDL — sample values, ordinal position, and identity flag
// are intentionally excluded (sample values are not part of the schema,
// ordinal position changes when columns are added/removed, and identity
// is hard to alter and rarely changed in practice).
func columnsEqual(a, b driver.Column) bool {
	return a.DataType == b.DataType &&
		a.MaxLength == b.MaxLength &&
		a.Precision == b.Precision &&
		a.Scale == b.Scale &&
		a.IsNullable == b.IsNullable &&
		a.SRID == b.SRID
}

func diffIndexes(prev, curr []driver.Index) (added, removed []driver.Index) {
	prevByName := make(map[string]driver.Index, len(prev))
	for _, i := range prev {
		prevByName[i.Name] = i
	}
	currByName := make(map[string]driver.Index, len(curr))
	for _, i := range curr {
		currByName[i.Name] = i
	}
	for _, i := range curr {
		if _, existed := prevByName[i.Name]; !existed {
			added = append(added, i)
		}
	}
	for _, i := range prev {
		if _, stillThere := currByName[i.Name]; !stillThere {
			removed = append(removed, i)
		}
	}
	return added, removed
}

func diffForeignKeys(prev, curr []driver.ForeignKey) (added, removed []driver.ForeignKey) {
	prevByName := make(map[string]driver.ForeignKey, len(prev))
	for _, f := range prev {
		prevByName[f.Name] = f
	}
	currByName := make(map[string]driver.ForeignKey, len(curr))
	for _, f := range curr {
		currByName[f.Name] = f
	}
	for _, f := range curr {
		if _, existed := prevByName[f.Name]; !existed {
			added = append(added, f)
		}
	}
	for _, f := range prev {
		if _, stillThere := currByName[f.Name]; !stillThere {
			removed = append(removed, f)
		}
	}
	return added, removed
}

func diffChecks(prev, curr []driver.CheckConstraint) (added, removed []driver.CheckConstraint) {
	prevByName := make(map[string]driver.CheckConstraint, len(prev))
	for _, c := range prev {
		prevByName[c.Name] = c
	}
	currByName := make(map[string]driver.CheckConstraint, len(curr))
	for _, c := range curr {
		currByName[c.Name] = c
	}
	for _, c := range curr {
		if _, existed := prevByName[c.Name]; !existed {
			added = append(added, c)
		}
	}
	for _, c := range prev {
		if _, stillThere := currByName[c.Name]; !stillThere {
			removed = append(removed, c)
		}
	}
	return added, removed
}
