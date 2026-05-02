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
