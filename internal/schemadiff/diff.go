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

	AddedTables   []driver.Table      `json:"added_tables"`
	RemovedTables []driver.Table      `json:"removed_tables"`
	ChangedTables []TableDiff         `json:"changed_tables"`
	Unsupported   []UnsupportedChange `json:"unsupported,omitempty"`
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
	Name     string        `json:"name"`
	Old      driver.Column `json:"old"`
	New      driver.Column `json:"new"`
	Criteria []string      `json:"criteria,omitempty"`
}

// IsEmpty returns true if the diff would produce no DDL.
func (d Diff) IsEmpty() bool {
	if len(d.AddedTables) > 0 || len(d.RemovedTables) > 0 {
		return false
	}
	if len(d.Unsupported) > 0 {
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
	if n := len(d.AddedTables); n > 0 {
		fmt.Fprintf(&b, "%d table(s) added, ", n)
	}
	if n := len(d.RemovedTables); n > 0 {
		fmt.Fprintf(&b, "%d table(s) removed, ", n)
	}
	if n := len(d.ChangedTables); n > 0 {
		fmt.Fprintf(&b, "%d table(s) changed, ", n)
	}
	if n := len(d.Unsupported); n > 0 {
		fmt.Fprintf(&b, "%d unsupported change(s), ", n)
	}
	return strings.TrimSuffix(strings.TrimSpace(b.String()), ",")
}

// Normalize rewrites every identifier in the diff (table names, column
// names, index names, FK names + ref columns, check names) through the
// supplied function. Callers use this to align source-original names
// (e.g. MSSQL "Posts") with the target's on-disk convention (e.g.
// PostgreSQL "posts") before handing the diff to the deterministic
// renderer, so the generated ALTERs hit tables that actually exist on
// the target.
//
// The transformation is purely structural; data types and other column
// attributes are left alone.
func (d Diff) Normalize(norm func(string) string) Diff {
	out := Diff{
		PrevCapturedAt: d.PrevCapturedAt,
		CurrCapturedAt: d.CurrCapturedAt,
		Unsupported:    append([]UnsupportedChange(nil), d.Unsupported...),
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
// schema names in two places used by renderers: Table.Schema (the table's
// own qualifier) and ForeignKey.RefSchema (the referenced table's
// qualifier in inline REFERENCES clauses). Leaving either one as the
// source value produces statements that fail on the target (e.g. MySQL
// source schema "smt_src_test" against MSSQL target schema "dbo":
// "Cannot find the object ...", or "REFERENCES smt_src_test.parent"
// inside a target ADD FK).
//
// SMT migrates source.X to target.Y; we don't preserve cross-schema
// relationships across engines, so every schema reference in the diff
// resolves to the target schema after rewriting.
//
// Parallels Normalize: structural-only transformation, leaves all other
// fields alone. Call after Normalize and before RenderDeterministic.
func (d Diff) WithTargetSchema(targetSchema string) Diff {
	out := Diff{
		PrevCapturedAt: d.PrevCapturedAt,
		CurrCapturedAt: d.CurrCapturedAt,
		Unsupported:    append([]UnsupportedChange(nil), d.Unsupported...),
	}
	for _, t := range d.AddedTables {
		out.AddedTables = append(out.AddedTables, retargetTable(t, targetSchema))
	}
	for _, t := range d.RemovedTables {
		out.RemovedTables = append(out.RemovedTables, retargetTable(t, targetSchema))
	}
	for _, td := range d.ChangedTables {
		td.Schema = targetSchema
		td.Curr = retargetTable(td.Curr, targetSchema)
		td.AddedForeignKeys = retargetForeignKeys(td.AddedForeignKeys, targetSchema)
		td.RemovedForeignKeys = retargetForeignKeys(td.RemovedForeignKeys, targetSchema)
		out.ChangedTables = append(out.ChangedTables, td)
	}
	return out
}

// FilterManagedKinds drops index / foreign-key / check deltas — and the
// corresponding side objects on added tables — for object kinds the
// migration does not manage, mirroring the create_* gating that `create` and
// the live-target sync apply. Returns copies; inputs are not mutated.
func (d Diff) FilterManagedKinds(indexes, fks, checks bool) Diff {
	if indexes && fks && checks {
		return d
	}
	out := Diff{
		PrevCapturedAt: d.PrevCapturedAt,
		CurrCapturedAt: d.CurrCapturedAt,
		Unsupported:    append([]UnsupportedChange(nil), d.Unsupported...),
	}
	strip := func(t driver.Table) driver.Table {
		if !indexes {
			t.Indexes = nil
		}
		if !fks {
			t.ForeignKeys = nil
		}
		if !checks {
			t.CheckConstraints = nil
		}
		return t
	}
	for _, t := range d.AddedTables {
		out.AddedTables = append(out.AddedTables, strip(t))
	}
	for _, t := range d.RemovedTables {
		out.RemovedTables = append(out.RemovedTables, strip(t))
	}
	for _, td := range d.ChangedTables {
		td.Curr = strip(td.Curr)
		if !indexes {
			td.AddedIndexes, td.RemovedIndexes = nil, nil
		}
		if !fks {
			td.AddedForeignKeys, td.RemovedForeignKeys = nil, nil
		}
		if !checks {
			td.AddedChecks, td.RemovedChecks = nil, nil
		}
		if !td.IsEmpty() {
			out.ChangedTables = append(out.ChangedTables, td)
		}
	}
	return out
}

// retargetTable returns a deep copy of the table with its own Schema and the
// RefSchema of every inline foreign key rewritten. A copy, not in-place: the
// table's slices share backing arrays with the snapshot the diff came from,
// which callers persist as the next baseline.
func retargetTable(t driver.Table, targetSchema string) driver.Table {
	t = deepCopyTable(t)
	t.Schema = targetSchema
	for i := range t.ForeignKeys {
		t.ForeignKeys[i].RefSchema = targetSchema
	}
	return t
}

// retargetForeignKeys returns a copy of the slice with RefSchema rewritten on
// every entry (copy for the same aliasing reason as retargetTable).
func retargetForeignKeys(fks []driver.ForeignKey, targetSchema string) []driver.ForeignKey {
	out := append([]driver.ForeignKey(nil), fks...)
	for i := range out {
		out[i].RefSchema = targetSchema
	}
	return out
}

func normalizeTable(t driver.Table, norm func(string) string) driver.Table {
	// The in-place rewrites below would otherwise reach through the slice
	// backing arrays into the caller's tables — Compute's diff shares them
	// with the snapshot it was built from, and callers persist that snapshot
	// as the next baseline (sync --save-snapshot).
	t = deepCopyTable(t)
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
		idx.Columns = append([]string(nil), idx.Columns...)
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
		fk.Columns = append([]string(nil), fk.Columns...)
		for j := range fk.Columns {
			fk.Columns[j] = norm(fk.Columns[j])
		}
		fk.RefColumns = append([]string(nil), fk.RefColumns...)
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
	prev = backfillPreVersionFields(prev, curr)

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

// backfillPreVersionFields copies snapshot-version-gated Column fields from
// the current extraction into an older snapshot before diffing. A snapshot
// written before a field existed stores its zero value, and comparing that
// against the freshly-extracted value would mark every such column as changed
// (#78) — e.g. every unsigned MySQL column after the v2 upgrade. Assuming the
// field is unchanged is the conservative read: a wrong assumption misses one
// real change (the next snapshot captures it) instead of emitting spurious
// rebuild ALTERs.
func backfillPreVersionFields(prev, curr Snapshot) Snapshot {
	if prev.Version >= CurrentSnapshotVersion {
		return prev
	}

	currTables := indexTablesByName(curr.Tables)
	tables := make([]driver.Table, len(prev.Tables))
	copy(tables, prev.Tables)
	for ti := range tables {
		ct, ok := currTables[tables[ti].Name]
		if !ok {
			continue
		}
		currCols := make(map[string]driver.Column, len(ct.Columns))
		for _, c := range ct.Columns {
			currCols[c.Name] = c
		}
		cols := make([]driver.Column, len(tables[ti].Columns))
		copy(cols, tables[ti].Columns)
		for ci := range cols {
			cc, ok := currCols[cols[ci].Name]
			if !ok {
				continue
			}
			if prev.Version < 2 {
				cols[ci].IsUnsigned = cc.IsUnsigned
				cols[ci].OnUpdateExpression = cc.OnUpdateExpression
				if len(cols[ci].EnumValues) == 0 {
					cols[ci].EnumValues = cc.EnumValues
				}
			}
			if prev.Version < 3 {
				cols[ci].DatetimePrecision = cc.DatetimePrecision
			}
			if prev.Version < 4 {
				cols[ci].DisplayWidth = cc.DisplayWidth
			}
		}
		tables[ti].Columns = cols
	}
	prev.Tables = tables
	return prev
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
// matter for DDL. Sample values, ordinal position, and identity flag are
// intentionally excluded (sample values are not part of the schema,
// ordinal position changes when columns are added/removed, and identity
// is hard to alter and rarely changed in practice).
func columnsEqual(a, b driver.Column) bool {
	return a.DataType == b.DataType &&
		a.MaxLength == b.MaxLength &&
		a.Precision == b.Precision &&
		a.Scale == b.Scale &&
		equalIntPtr(a.DatetimePrecision, b.DatetimePrecision) &&
		a.IsNullable == b.IsNullable &&
		a.SRID == b.SRID &&
		a.IsUnsigned == b.IsUnsigned &&
		a.DisplayWidth == b.DisplayWidth &&
		stringSlicesEqual(a.EnumValues, b.EnumValues) &&
		strings.TrimSpace(a.DefaultExpression) == strings.TrimSpace(b.DefaultExpression) &&
		strings.TrimSpace(a.OnUpdateExpression) == strings.TrimSpace(b.OnUpdateExpression) &&
		a.IsComputed == b.IsComputed &&
		strings.TrimSpace(a.ComputedExpression) == strings.TrimSpace(b.ComputedExpression) &&
		a.ComputedPersisted == b.ComputedPersisted
}

func equalIntPtr(a, b *int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
