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
	// (canonical type, length, precision, nullability, identity, default class).
	// Each entry is a human-readable description from the deterministic
	// comparator.
	ColumnDeltas []string
	// MissingIndexes / ExtraIndexes are secondary indexes present on only one
	// side, keyed by ordered column list (+ a unique: prefix) rather than
	// name, since the renderer normalizes index names per dialect. Column
	// order is significant, so (a,b) and (b,a) are distinct.
	MissingIndexes []string
	ExtraIndexes   []string
	// MissingForeignKeys / ExtraForeignKeys are FKs present on only one side,
	// keyed by ordered local→referenced column pairs, referenced table, and
	// referential actions — so a changed ON DELETE/UPDATE or referenced
	// column drifts.
	MissingForeignKeys []string
	ExtraForeignKeys   []string
	// CheckDrift is non-empty when the target has fewer CHECK constraints than
	// the source (a likely dropped check). Predicate text is rewritten
	// cross-dialect, so checks are compared by count, not text.
	CheckDrift string
	// PKDrift is non-empty when the primary key column set differs (dropped,
	// added, or re-keyed). The index loaders exclude PK-backed indexes, so the
	// PK is compared here, not via MissingIndexes/ExtraIndexes.
	PKDrift string
}

func (td TableDrift) hasChanges() bool {
	return len(td.MissingColumns) > 0 || len(td.ExtraColumns) > 0 || len(td.ColumnDeltas) > 0 ||
		len(td.MissingIndexes) > 0 || len(td.ExtraIndexes) > 0 ||
		len(td.MissingForeignKeys) > 0 || len(td.ExtraForeignKeys) > 0 ||
		td.CheckDrift != "" || td.PKDrift != ""
}

// DriftOptions gates which object kinds ComputeDrift compares, so a config
// that intentionally leaves indexes / FKs / checks unmanaged
// (create_indexes/create_foreign_keys/create_check_constraints = false) does
// not see those reported as drift. All default to true via DefaultDriftOptions.
type DriftOptions struct {
	CompareIndexes     bool
	CompareForeignKeys bool
	CompareChecks      bool
}

// DefaultDriftOptions compares every dimension.
func DefaultDriftOptions() DriftOptions {
	return DriftOptions{CompareIndexes: true, CompareForeignKeys: true, CompareChecks: true}
}

// NormalizeIdentifiers returns a deep copy of tables with every identifier
// (table, column, PK, index columns, FK columns / referenced table /
// referenced columns, check names) folded through norm — so source-form
// identifiers line up with a target whose dialect rewrites names (e.g. PG
// lowercasing "Order ID" to "order_id"). Callers normalize the desired schema
// with the target's rule before ComputeDrift so constraint comparisons don't
// see false drift. The input is not mutated.
func NormalizeIdentifiers(tables []driver.Table, norm func(string) string) []driver.Table {
	out := make([]driver.Table, len(tables))
	for i := range tables {
		out[i] = normalizeTable(deepCopyTable(tables[i]), norm)
	}
	return out
}

// deepCopyTable copies the slices normalizeTable mutates so normalization
// can't reach back into the caller's tables.
func deepCopyTable(t driver.Table) driver.Table {
	t.Columns = append([]driver.Column(nil), t.Columns...)
	t.PrimaryKey = append([]string(nil), t.PrimaryKey...)
	t.Indexes = append([]driver.Index(nil), t.Indexes...)
	for i := range t.Indexes {
		t.Indexes[i].Columns = append([]string(nil), t.Indexes[i].Columns...)
		t.Indexes[i].IncludeCols = append([]string(nil), t.Indexes[i].IncludeCols...)
	}
	t.ForeignKeys = append([]driver.ForeignKey(nil), t.ForeignKeys...)
	for i := range t.ForeignKeys {
		t.ForeignKeys[i].Columns = append([]string(nil), t.ForeignKeys[i].Columns...)
		t.ForeignKeys[i].RefColumns = append([]string(nil), t.ForeignKeys[i].RefColumns...)
	}
	t.CheckConstraints = append([]driver.CheckConstraint(nil), t.CheckConstraints...)
	return t
}

// RetargetSchema returns a deep copy of tables with every schema reference —
// the table's own Schema and each foreign key's RefSchema — set to
// targetSchema. create/sync do this (Diff.WithTargetSchema) because SMT
// migrates everything into one target schema, collapsing even a source
// cross-schema FK to the target schema. Drift must apply the same collapse to
// the DESIRED side so its FK signatures match what was actually generated on
// the target; the existing (introspected) side is left untouched so a genuine
// cross-schema reference on the target still drifts. Input is not mutated;
// pass-through when targetSchema is empty.
func RetargetSchema(tables []driver.Table, targetSchema string) []driver.Table {
	if strings.TrimSpace(targetSchema) == "" {
		return tables
	}
	out := make([]driver.Table, len(tables))
	for i := range tables {
		t := deepCopyTable(tables[i])
		t.Schema = targetSchema
		for j := range t.ForeignKeys {
			t.ForeignKeys[j].RefSchema = targetSchema
		}
		out[i] = t
	}
	return out
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
// opts gates which object kinds are compared (see DriftOptions).
func ComputeDrift(desired, existing []driver.Table, sourceDialect, targetDialect string, opts DriftOptions) Drift {
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
		if td, changed := tableDrift(want, have, sourceDialect, targetDialect, opts); changed {
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

func tableDrift(want, have driver.Table, sourceDialect, targetDialect string, opts DriftOptions) (TableDrift, bool) {
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

	// CompareColumns handles canonical type equivalence plus length/precision/
	// nullability/identity/TZ/default. Drift adds only schema-object facts the
	// column comparator intentionally does not cover: target-only columns,
	// computed storage class, PKs, indexes, FKs, and CHECK counts.
	haveByName := make(map[string]driver.Column, len(have.Columns))
	for _, c := range have.Columns {
		haveByName[strings.ToLower(c.Name)] = c
	}
	for _, sc := range want.Columns {
		tc, ok := haveByName[strings.ToLower(sc.Name)]
		if !ok {
			continue
		}
		name := strings.ToLower(sc.Name)
		// Generated/computed status and storage class are cross-dialect
		// structural facts CompareColumns doesn't cover: a column that's
		// generated in the source must stay generated on the target, and a
		// STORED/PERSISTED column must not become VIRTUAL (or vice versa).
		// The expression TEXT itself differs cross-dialect (like CHECK
		// predicates), so only presence + storage class are compared here.
		if sc.IsComputed != tc.IsComputed {
			td.ColumnDeltas = append(td.ColumnDeltas,
				name+" computed: source="+boolStr(sc.IsComputed)+" target="+boolStr(tc.IsComputed))
		} else if sc.IsComputed && sc.ComputedPersisted != tc.ComputedPersisted && targetSupportsVirtualComputed(targetDialect) {
			// Storage class is only meaningful drift when the target can
			// actually represent both: PostgreSQL generated columns are always
			// STORED, so its reader reports every computed column persisted —
			// comparing a source VIRTUAL column against it would false-drift on
			// a faithfully created target. MySQL (VIRTUAL/STORED) and MSSQL
			// (PERSISTED or not) can represent both.
			td.ColumnDeltas = append(td.ColumnDeltas,
				name+" computed storage: source persisted="+boolStr(sc.ComputedPersisted)+" target persisted="+boolStr(tc.ComputedPersisted))
		}
	}

	// Primary key: the index loaders exclude PK-backed indexes, so a dropped
	// or re-keyed PK is invisible to the index comparison and must be checked
	// directly. Compared as a column set (order is not semantically part of
	// the key).
	if !sameColSetCI(want.PrimaryKey, have.PrimaryKey) {
		td.PKDrift = "source PK (" + joinLowerSorted(want.PrimaryKey) + ") vs target PK (" + joinLowerSorted(have.PrimaryKey) + ")"
	}

	if opts.CompareIndexes {
		td.MissingIndexes, td.ExtraIndexes = diffKeys(indexKeys(want.Indexes), indexKeys(have.Indexes))
	}
	if opts.CompareForeignKeys {
		td.MissingForeignKeys, td.ExtraForeignKeys = diffKeys(
			fkKeys(want.ForeignKeys, want.Schema), fkKeys(have.ForeignKeys, have.Schema))
	}
	// CHECK constraints: predicate text is rewritten cross-dialect, so a
	// faithful target may carry textually different predicates — only the
	// COUNT is compared. A mismatch in either direction is drift: fewer on the
	// target means a check was dropped; more means a target-only check that can
	// reject rows the source allows.
	if opts.CompareChecks && len(have.CheckConstraints) != len(want.CheckConstraints) {
		td.CheckDrift = "source has " + strconv.Itoa(len(want.CheckConstraints)) +
			" check constraint(s), target has " + strconv.Itoa(len(have.CheckConstraints))
	}

	sort.Strings(td.MissingColumns)
	sort.Strings(td.ExtraColumns)
	sort.Strings(td.ColumnDeltas)

	return td, td.hasChanges()
}

// sameColSetCI reports whether two column lists hold the same set of names,
// case-insensitively and order-independently.
func sameColSetCI(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	return joinLowerSorted(a) == joinLowerSorted(b)
}

func joinLowerSorted(in []string) string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = strings.ToLower(s)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// indexKeys returns an ordered column-list key for each index. Column ORDER
// is significant for an index (a, b) ≠ (b, a) for query planning, so the
// columns are NOT sorted — only lowercased. Uniqueness is folded in so a
// UNIQUE index does not match a non-unique one on the same columns.
func indexKeys(idxs []driver.Index) []string {
	out := make([]string, 0, len(idxs))
	for _, ix := range idxs {
		key := orderedCols(ix.Columns)
		// Covering-index INCLUDE columns are part of the index's identity:
		// dropping them changes what the index covers. Identifiers, so they
		// normalize cleanly across dialects.
		if len(ix.IncludeCols) > 0 {
			key += "/include:" + orderedCols(ix.IncludeCols)
		}
		// A filtered/partial index differs from an unfiltered one on the same
		// columns. The predicate text is cross-dialect-divergent (like CHECK
		// predicates), so fold in only whether a filter is present — a coarse
		// signal that still catches "filter added/removed" without
		// false-flagging an equivalent predicate rewritten per dialect.
		if strings.TrimSpace(ix.Filter) != "" {
			key += "/filtered"
		}
		if ix.IsUnique {
			key = "unique:" + key
		}
		out = append(out, key)
	}
	return out
}

// targetSupportsVirtualComputed reports whether the dialect can represent both
// a VIRTUAL (non-persisted) and a STORED/PERSISTED computed column. PostgreSQL
// only has STORED generated columns, so its storage class isn't a faithful
// round-trip of a VIRTUAL source and must not be compared as drift.
func targetSupportsVirtualComputed(dialect string) bool {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "mysql", "mariadb", "mssql", "sqlserver":
		return true
	default:
		return false
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// fkKeys returns a signature for each foreign key built from its ordered
// local→referenced column pairs, referenced table+schema, and referential
// actions — so a target FK that changes ON DELETE/UPDATE, points at different
// referenced columns, or references a different schema no longer matches.
//
// The referenced schema is encoded RELATIVE to the FK's owning table: a
// same-schema reference (empty RefSchema or equal to ownerSchema) becomes
// "self", so the common case compares equal across dialects without
// retargeting (source "dbo" and target "public" both read as "self"), while a
// genuine cross-schema reference keeps its literal schema and drifts if it
// changes.
func fkKeys(fks []driver.ForeignKey, ownerSchema string) []string {
	out := make([]string, 0, len(fks))
	for _, fk := range fks {
		pairs := make([]string, len(fk.Columns))
		for i, c := range fk.Columns {
			ref := ""
			if i < len(fk.RefColumns) {
				ref = strings.ToLower(fk.RefColumns[i])
			}
			pairs[i] = strings.ToLower(c) + ":" + ref
		}
		schema := "self"
		if s := strings.TrimSpace(fk.RefSchema); s != "" && !strings.EqualFold(s, ownerSchema) {
			schema = strings.ToLower(s)
		}
		out = append(out, strings.Join(pairs, ",")+"->"+schema+"."+strings.ToLower(fk.RefTable)+
			"|"+normAction(fk.OnDelete)+"|"+normAction(fk.OnUpdate))
	}
	return out
}

// orderedCols joins columns lowercased but order-preserved.
func orderedCols(cols []string) string {
	lowered := make([]string, len(cols))
	for i, c := range cols {
		lowered[i] = strings.ToLower(c)
	}
	return strings.Join(lowered, ",")
}

// normAction folds the no-op referential-action spellings to one token so a
// source NO ACTION matches a target reporting the default differently.
func normAction(a string) string {
	switch strings.ToUpper(strings.TrimSpace(a)) {
	case "", "NO ACTION", "RESTRICT":
		return "noaction"
	default:
		return strings.ToLower(strings.Join(strings.Fields(a), " "))
	}
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
