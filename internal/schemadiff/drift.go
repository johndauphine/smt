package schemadiff

// Target-aware drift detection (#69). The snapshot-based Compute() diffs the
// source against its own stored history; it cannot tell whether the target
// actually matches what the source implies. ComputeDrift closes that gap by
// comparing the DESIRED target schema (derived from the current source) against
// the EXISTING target schema (introspected live), classifying each difference
// as missing / extra / changed.
//
// Cross-dialect column comparison is the crux: a source `varchar(20)` and a
// target `character varying(20)` are equivalent even though their data_type
// strings differ, so column drift is judged by driver.CompareColumns (the same
// deterministic equivalence rules the optional AI reviewer uses), not by raw
// string equality.

import (
	"sort"
	"strconv"
	"strings"

	"smt/internal/driver"
)

// Drift is the classified difference between the desired and existing target
// schemas. It is read-only intelligence — callers decide what to do with it.
type Drift struct {
	// MissingTables are desired (in source) but absent on the target — they
	// would need creation. Stable, sorted by normalized name.
	MissingTables []string
	// ExtraTables exist on the target but are not in the source. Dropping one
	// is destructive, so they are reported, never acted on here.
	ExtraTables []string
	// ChangedTables exist on both sides but differ in columns.
	ChangedTables []TableDrift
}

// TableDrift captures per-table drift for a table present on both sides.
type TableDrift struct {
	Name string
	// MissingColumns are in the source but absent on the target.
	MissingColumns []string
	// ExtraColumns exist on the target but not in the source. Dropping one is
	// destructive.
	ExtraColumns []string
	// ColumnDeltas are columns present on both sides whose metadata differs
	// (type class, length, precision, nullability, identity, default class).
	// Each entry is a human-readable description from the deterministic
	// comparator.
	ColumnDeltas []string
	// MissingIndexes / ExtraIndexes are secondary-index column sets present on
	// only one side (matched by column set, not name, since the renderer
	// normalizes index names per dialect).
	MissingIndexes []string
	ExtraIndexes   []string
	// MissingForeignKeys / ExtraForeignKeys are FK signatures ("(cols)->ref")
	// present on only one side.
	MissingForeignKeys []string
	ExtraForeignKeys   []string
	// CheckDrift is non-empty when the target has fewer CHECK constraints than
	// the source (a likely dropped check). Predicate text is rewritten
	// cross-dialect, so checks are compared by count, not text.
	CheckDrift string
}

func (td TableDrift) hasChanges() bool {
	return len(td.MissingColumns) > 0 || len(td.ExtraColumns) > 0 || len(td.ColumnDeltas) > 0 ||
		len(td.MissingIndexes) > 0 || len(td.ExtraIndexes) > 0 ||
		len(td.MissingForeignKeys) > 0 || len(td.ExtraForeignKeys) > 0 || td.CheckDrift != ""
}

// IsEmpty reports whether the target matches the desired schema.
func (d Drift) IsEmpty() bool {
	return len(d.MissingTables) == 0 && len(d.ExtraTables) == 0 && len(d.ChangedTables) == 0
}

// Summary returns a one-line description of the drift.
func (d Drift) Summary() string {
	if d.IsEmpty() {
		return "no drift: target matches the source-derived schema"
	}
	parts := make([]string, 0, 3)
	if n := len(d.MissingTables); n > 0 {
		parts = append(parts, plural(n, "missing table", "missing tables"))
	}
	if n := len(d.ExtraTables); n > 0 {
		parts = append(parts, plural(n, "extra table", "extra tables"))
	}
	if n := len(d.ChangedTables); n > 0 {
		parts = append(parts, plural(n, "changed table", "changed tables"))
	}
	return strings.Join(parts, ", ")
}

// HasDestructiveDrift reports whether reconciling the target to the source
// would require dropping objects (extra tables or columns). Useful for a
// CI drift gate that wants to distinguish additive drift from destructive.
func (d Drift) HasDestructiveDrift() bool {
	if len(d.ExtraTables) > 0 {
		return true
	}
	for _, t := range d.ChangedTables {
		if len(t.ExtraColumns) > 0 {
			return true
		}
	}
	return false
}

// ComputeDrift compares the desired target schema (the current source tables,
// in source-dialect metadata) against the existing target schema (introspected
// from the live target, in target-dialect metadata). Tables and columns are
// matched case-insensitively by name; the caller is responsible for having
// normalized desired identifiers to the target's on-disk convention so the
// names line up (see Diff.Normalize / driver.NormalizeIdentifier).
//
// sourceDialect/targetDialect drive the cross-dialect column comparison.
func ComputeDrift(desired, existing []driver.Table, sourceDialect, targetDialect string) Drift {
	desiredByName := indexByLowerName(desired)
	existingByName := indexByLowerName(existing)

	var d Drift
	for _, name := range sortedLowerKeys(desiredByName) {
		want := desiredByName[name]
		have, ok := existingByName[name]
		if !ok {
			d.MissingTables = append(d.MissingTables, name)
			continue
		}
		if td, changed := tableDrift(want, have, sourceDialect, targetDialect); changed {
			d.ChangedTables = append(d.ChangedTables, td)
		}
	}
	for _, name := range sortedLowerKeys(existingByName) {
		if _, ok := desiredByName[name]; !ok {
			d.ExtraTables = append(d.ExtraTables, name)
		}
	}
	return d
}

func tableDrift(want, have driver.Table, sourceDialect, targetDialect string) (TableDrift, bool) {
	td := TableDrift{Name: strings.ToLower(have.Name)}

	wantCols := lowerColNames(want.Columns)

	// Extra columns (on target, not desired) — reported separately because
	// CompareColumns is intentionally source-driven and ignores them.
	for _, c := range have.Columns {
		if !wantCols[strings.ToLower(c.Name)] {
			td.ExtraColumns = append(td.ExtraColumns, strings.ToLower(c.Name))
		}
	}

	// Missing columns + per-column metadata drift via the deterministic
	// cross-dialect comparator. "missing" deltas become MissingColumns; the
	// rest become ColumnDeltas.
	for _, delta := range driver.CompareColumns(want.Columns, have.Columns, sourceDialect, targetDialect) {
		if delta.Criterion == "missing" {
			td.MissingColumns = append(td.MissingColumns, strings.ToLower(delta.Column))
			continue
		}
		td.ColumnDeltas = append(td.ColumnDeltas, delta.String())
	}

	// Secondary indexes, matched by column set (names are renderer-normalized
	// per dialect, so they don't compare across engines).
	td.MissingIndexes, td.ExtraIndexes = diffKeys(indexKeys(want.Indexes), indexKeys(have.Indexes))
	// Foreign keys, matched by local column set + referenced table.
	td.MissingForeignKeys, td.ExtraForeignKeys = diffKeys(fkKeys(want.ForeignKeys), fkKeys(have.ForeignKeys))
	// CHECK constraints: predicate text is rewritten cross-dialect, so a
	// faithful target may carry textually different predicates. Only flag a
	// likely drop — the target having fewer checks than the source.
	if len(have.CheckConstraints) < len(want.CheckConstraints) {
		td.CheckDrift = "source has " + strconv.Itoa(len(want.CheckConstraints)) +
			" check constraint(s), target has " + strconv.Itoa(len(have.CheckConstraints))
	}

	sort.Strings(td.MissingColumns)
	sort.Strings(td.ExtraColumns)
	sort.Strings(td.ColumnDeltas)

	return td, td.hasChanges()
}

// indexKeys returns the order-insensitive column-set key of each index.
func indexKeys(idxs []driver.Index) []string {
	out := make([]string, 0, len(idxs))
	for _, ix := range idxs {
		out = append(out, colSet(ix.Columns))
	}
	return out
}

// fkKeys returns a "(localcols)->reftable" signature for each foreign key.
func fkKeys(fks []driver.ForeignKey) []string {
	out := make([]string, 0, len(fks))
	for _, fk := range fks {
		out = append(out, colSet(fk.Columns)+"->"+strings.ToLower(fk.RefTable))
	}
	return out
}

// diffKeys returns the keys present only in want (missing on target) and only
// in have (extra on target), each sorted.
func diffKeys(want, have []string) (missing, extra []string) {
	haveSet := make(map[string]bool, len(have))
	for _, k := range have {
		haveSet[k] = true
	}
	wantSet := make(map[string]bool, len(want))
	for _, k := range want {
		wantSet[k] = true
	}
	for _, k := range want {
		if !haveSet[k] {
			missing = append(missing, k)
		}
	}
	for _, k := range have {
		if !wantSet[k] {
			extra = append(extra, k)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	return missing, extra
}

// colSet is an order-insensitive, case-insensitive key for a column list.
func colSet(cols []string) string {
	lowered := make([]string, len(cols))
	for i, c := range cols {
		lowered[i] = strings.ToLower(c)
	}
	sort.Strings(lowered)
	return strings.Join(lowered, ",")
}

func indexByLowerName(tables []driver.Table) map[string]driver.Table {
	out := make(map[string]driver.Table, len(tables))
	for _, t := range tables {
		out[strings.ToLower(t.Name)] = t
	}
	return out
}

func sortedLowerKeys(m map[string]driver.Table) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func lowerColNames(cols []driver.Column) map[string]bool {
	out := make(map[string]bool, len(cols))
	for _, c := range cols {
		out[strings.ToLower(c.Name)] = true
	}
	return out
}

func plural(n int, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return strconv.Itoa(n) + " " + many
}
