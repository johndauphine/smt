package orchestrator

// End-to-end acceptance test for the no-AI deterministic schema path (#65):
// migrate the StackOverflow2010 MSSQL schema to PostgreSQL with no AI provider
// configured, then introspect the target and verify tables, columns,
// nullability, identity, primary keys (including the PostLinks.Id PK that
// model-authored DDL used to drop), foreign keys, and indexes.
//
// Opt-in: set SMT_E2E_SO2010=1 with the live containers up. Skipped by
// default and under -short so the normal suite stays hermetic.
//
// Connection defaults match the project's docker fixtures; override via env:
//
//	SO2010_MSSQL_HOST/PORT/USER/PASS/DB   (default localhost:1433 sa StackOverflow2010)
//	SO2010_PG_HOST/PORT/USER/PASS/DB      (default localhost:5432 postgres postgres)
//	SO2010_PG_SCHEMA                       (default so2010_accept; dropped + recreated)
//	SO2010_REPORT_DIR                      durable dir for the JSON report (default: ephemeral temp)

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"smt/internal/config"
	"smt/internal/ddl"
	"smt/internal/driver"
	"smt/internal/pool"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(t *testing.T, key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("invalid int in %s=%q: %v", key, v, err)
	}
	return n
}

func TestSO2010_DeterministicMSSQLToPostgres(t *testing.T) {
	if os.Getenv("SMT_E2E_SO2010") == "" {
		t.Skip("set SMT_E2E_SO2010=1 with live mssql StackOverflow2010 + postgres to run")
	}
	if testing.Short() {
		t.Skip("skipping live e2e under -short")
	}
	// A stray secrets file on the host must not flip on AI review; point at a
	// path that does not exist so the deterministic path is the only path.
	t.Setenv("SMT_SECRETS_FILE", os.DevNull+".missing")

	srcDB := env("SO2010_MSSQL_DB", "StackOverflow2010")
	pgDB := env("SO2010_PG_DB", "postgres")
	pgSchema := env("SO2010_PG_SCHEMA", "so2010_accept")
	dataDir := t.TempDir()

	cfgYAML := fmt.Sprintf(`
source:
  type: mssql
  host: %s
  port: %d
  database: %s
  user: %s
  password: %s
  schema: dbo
  trust_server_cert: true
target:
  type: postgres
  host: %s
  port: %d
  database: %s
  user: %s
  password: %s
  schema: %s
  ssl_mode: disable
migration:
  create_indexes: true
  create_foreign_keys: true
  create_check_constraints: true
  data_dir: %s
ai_review:
  enabled: false
`,
		env("SO2010_MSSQL_HOST", "localhost"), envInt(t, "SO2010_MSSQL_PORT", 1433), srcDB,
		env("SO2010_MSSQL_USER", "sa"), env("SO2010_MSSQL_PASS", "TestPass2024"),
		env("SO2010_PG_HOST", "localhost"), envInt(t, "SO2010_PG_PORT", 5432), pgDB,
		env("SO2010_PG_USER", "postgres"), env("SO2010_PG_PASS", "TestPass2024"), pgSchema,
		dataDir)

	cfg, err := config.LoadBytes([]byte(cfgYAML))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	if cfg.AIReview.Enabled != nil && *cfg.AIReview.Enabled {
		t.Fatal("ai_review must be disabled for the no-AI acceptance test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Start from a clean target schema so the run exercises real creation,
	// not idempotent skips of a previous run.
	dropTargetSchema(t, ctx, cfg, pgSchema)

	orch, err := NewWithOptions(cfg, Options{StateFile: dataDir + "/state.yaml"})
	if err != nil {
		t.Fatalf("NewWithOptions: %v", err)
	}
	defer orch.Close()

	if err := orch.Run(ctx); err != nil {
		t.Fatalf("orchestrator Run (no-AI create+apply): %v", err)
	}

	// The run must have persisted a deterministic manifest (#64) recording
	// the renderer version + fingerprints that produced schema.sql.
	assertManifestWritten(t, dataDir)

	// Introspect both sides through the deterministic readers and compare.
	srcCfg := cfg.Source
	srcTables := extractAll(t, ctx, &srcCfg, "dbo")
	tgtTables := extractAll(t, ctx, pgSourceForTarget(cfg), pgSchema)

	report := verifySO2010(t, srcTables, tgtTables, pgSchema)

	// Save the verification report as a migration artifact (#65 requirement).
	// t.TempDir() is removed on cleanup, so honor SO2010_REPORT_DIR for a
	// durable copy callers (make test-so2010 / CI) can archive.
	blob, _ := json.MarshalIndent(report, "", "  ")
	reportDir := os.Getenv("SO2010_REPORT_DIR")
	if reportDir == "" {
		reportDir = dataDir // ephemeral fallback
	}
	reportPath := reportDir + "/so2010_verification.json"
	if err := os.WriteFile(reportPath, blob, 0600); err != nil {
		t.Fatalf("writing verification report: %v", err)
	}
	t.Logf("verification report written to %s", reportPath)
}

// verifyReport is the saved artifact: a per-table summary plus any failures.
type verifyReport struct {
	Schema       string         `json:"schema"`
	SourceTables int            `json:"source_tables"`
	TargetTables int            `json:"target_tables"`
	Tables       []tableReport  `json:"tables"`
	Failures     []string       `json:"failures"`
	PassedAt     string         `json:"passed_at,omitempty"`
	Counts       map[string]int `json:"counts"`
}

type tableReport struct {
	Name        string `json:"name"`
	Columns     int    `json:"columns"`
	PrimaryKey  string `json:"primary_key"`
	ForeignKeys int    `json:"foreign_keys"`
	Indexes     int    `json:"indexes"`
}

func verifySO2010(t *testing.T, src, tgt []driver.Table, schema string) verifyReport {
	t.Helper()
	rep := verifyReport{Schema: schema, SourceTables: len(src), TargetTables: len(tgt), Counts: map[string]int{}}

	tgtByName := map[string]driver.Table{}
	for _, tt := range tgt {
		tgtByName[strings.ToLower(tt.Name)] = tt
	}
	srcByName := map[string]bool{}
	for _, s := range src {
		srcByName[strings.ToLower(s.Name)] = true
	}
	// The schema was dropped before the run, so the target must contain
	// exactly the source tables — any extra was produced by the migration.
	for _, tt := range tgt {
		if !srcByName[strings.ToLower(tt.Name)] {
			rep.Failures = append(rep.Failures, fmt.Sprintf("unexpected extra table %s on target", tt.Name))
		}
	}

	var totalFK, totalPK int
	for _, s := range src {
		key := strings.ToLower(s.Name)
		tt, ok := tgtByName[key]
		if !ok {
			rep.Failures = append(rep.Failures, fmt.Sprintf("table %s missing on target", s.Name))
			continue
		}
		tr := tableReport{Name: key, Columns: len(tt.Columns), ForeignKeys: len(tt.ForeignKeys), Indexes: len(tt.Indexes)}

		if len(s.Columns) != len(tt.Columns) {
			rep.Failures = append(rep.Failures,
				fmt.Sprintf("%s column count: source=%d target=%d", s.Name, len(s.Columns), len(tt.Columns)))
		}
		// Per-column equivalence across all six criteria (max_length,
		// precision/scale, nullability, identity, TZ class, default class)
		// via the same deterministic comparator AI review uses.
		for _, d := range driver.CompareColumns(s.Columns, tt.Columns, "mssql", "postgres") {
			rep.Failures = append(rep.Failures, fmt.Sprintf("%s.%s", s.Name, d.String()))
		}

		// Primary key must round-trip (column set, order-insensitive).
		if len(tt.PrimaryKey) > 0 {
			tr.PrimaryKey = strings.Join(lowerSorted(tt.PrimaryKey), ",")
		}
		if !sameColsCI(s.PrimaryKey, tt.PrimaryKey) {
			rep.Failures = append(rep.Failures,
				fmt.Sprintf("%s primary key: source=%v target=%v", s.Name, s.PrimaryKey, tt.PrimaryKey))
		}

		// Every source secondary index must have a target index covering the
		// same column set. Compared by column set, not name, since the
		// renderer normalizes index names per dialect. LoadIndexes excludes
		// PK-backed indexes on both readers, so this is the secondary set.
		tgtIdx := map[string]bool{}
		for _, idx := range tt.Indexes {
			tgtIdx[colSetKey(idx.Columns)] = true
		}
		for _, idx := range s.Indexes {
			if !tgtIdx[colSetKey(idx.Columns)] {
				rep.Failures = append(rep.Failures,
					fmt.Sprintf("%s index on (%s) missing on target", s.Name, strings.Join(lowerSorted(idx.Columns), ",")))
			}
		}

		// Every source FK must have a target FK with the same local column
		// set and (normalized) referenced table.
		tgtFK := map[string]bool{}
		for _, fk := range tt.ForeignKeys {
			tgtFK[fkKey(fk)] = true
		}
		for _, fk := range s.ForeignKeys {
			if !tgtFK[fkKey(fk)] {
				rep.Failures = append(rep.Failures,
					fmt.Sprintf("%s FK on (%s)->%s missing on target", s.Name, strings.Join(lowerSorted(fk.Columns), ","), strings.ToLower(fk.RefTable)))
			}
		}

		// CHECK constraints must not be silently dropped. Predicate text is
		// rewritten cross-dialect (mssql `([x]>(0))` → pg `x > 0`), so exact
		// comparison would false-positive; assert the count instead, which
		// catches a renderer regression that omits CHECK statements wholesale.
		if len(tt.CheckConstraints) < len(s.CheckConstraints) {
			rep.Failures = append(rep.Failures,
				fmt.Sprintf("%s check constraints: source=%d target=%d", s.Name, len(s.CheckConstraints), len(tt.CheckConstraints)))
		}

		totalPK += len(tt.PrimaryKey)
		totalFK += len(tt.ForeignKeys)
		rep.Tables = append(rep.Tables, tr)
	}

	rep.Counts["source_pk_tables"] = countWithPK(src)
	rep.Counts["target_pk_columns"] = totalPK
	rep.Counts["target_fk_count"] = totalFK

	// The named regression: PostLinks.Id must be a primary key on the target.
	if pl, ok := tgtByName["postlinks"]; ok {
		if !containsCI(pl.PrimaryKey, "Id") {
			rep.Failures = append(rep.Failures, "PostLinks.Id is not a primary key on the target (the #65 regression)")
		}
	} else {
		rep.Failures = append(rep.Failures, "PostLinks table missing on target")
	}

	for _, f := range rep.Failures {
		t.Errorf("SO2010 verify: %s", f)
	}
	if len(rep.Failures) == 0 {
		rep.PassedAt = time.Now().UTC().Format(time.RFC3339)
	}
	return rep
}

// --- helpers -----------------------------------------------------------------

func extractAll(t *testing.T, ctx context.Context, sc *config.SourceConfig, schema string) []driver.Table {
	t.Helper()
	r, err := pool.NewSourcePool(sc, 4)
	if err != nil {
		t.Fatalf("opening reader for %s: %v", sc.Type, err)
	}
	defer r.Close()
	tables, err := r.ExtractSchema(ctx, schema)
	if err != nil {
		t.Fatalf("ExtractSchema(%s): %v", schema, err)
	}
	for i := range tables {
		if err := r.LoadIndexes(ctx, &tables[i]); err != nil {
			t.Fatalf("LoadIndexes(%s): %v", tables[i].Name, err)
		}
		if err := r.LoadForeignKeys(ctx, &tables[i]); err != nil {
			t.Fatalf("LoadForeignKeys(%s): %v", tables[i].Name, err)
		}
		if err := r.LoadCheckConstraints(ctx, &tables[i]); err != nil {
			t.Fatalf("LoadCheckConstraints(%s): %v", tables[i].Name, err)
		}
	}
	return tables
}

// pgSourceForTarget builds a SourceConfig that reads the migrated PG target
// so the same deterministic reader path verifies what apply produced.
func pgSourceForTarget(cfg *config.Config) *config.SourceConfig {
	return &config.SourceConfig{
		Type:     cfg.Target.Type,
		Host:     cfg.Target.Host,
		Port:     cfg.Target.Port,
		Database: cfg.Target.Database,
		User:     cfg.Target.User,
		Password: cfg.Target.Password,
		Schema:   cfg.Target.Schema,
		SSLMode:  cfg.Target.SSLMode,
	}
}

func dropTargetSchema(t *testing.T, ctx context.Context, cfg *config.Config, schema string) {
	t.Helper()
	w, err := pool.NewTargetPool(&cfg.Target, 2, cfg.Source.Type, cfg.SchemaGeneration.UnknownTypePolicy)
	if err != nil {
		t.Fatalf("opening target for cleanup: %v", err)
	}
	defer w.Close()
	db := w.DB()
	if db == nil {
		t.Fatal("target pool exposed no *sql.DB for cleanup")
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", schema)); err != nil {
		t.Fatalf("dropping target schema %s: %v", schema, err)
	}
}

func sameColsCI(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	return strings.Join(lowerSorted(a), ",") == strings.Join(lowerSorted(b), ",")
}

// colSetKey is an order-insensitive, case-insensitive key for a column set.
func colSetKey(cols []string) string {
	return strings.Join(lowerSorted(cols), ",")
}

// fkKey identifies a foreign key by its local→referenced column pairs,
// referenced table, and referential actions — dialect-independent, ignoring
// the (normalized) constraint name. Column/refcolumn pairing is positional,
// so pairs are formed first and then sorted as a set; this catches composite
// FKs that point at the wrong referenced columns or use different actions.
func fkKey(fk driver.ForeignKey) string {
	pairs := make([]string, 0, len(fk.Columns))
	for i, c := range fk.Columns {
		ref := ""
		if i < len(fk.RefColumns) {
			ref = strings.ToLower(fk.RefColumns[i])
		}
		pairs = append(pairs, strings.ToLower(c)+":"+ref)
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",") + "->" + strings.ToLower(fk.RefTable) +
		"|" + normAction(fk.OnDelete) + "|" + normAction(fk.OnUpdate)
}

// normAction folds the no-op referential-action spellings ("", "NO ACTION",
// "RESTRICT") to one token so a source NO ACTION matches a target that
// reports the default differently.
func normAction(a string) string {
	switch strings.ToUpper(strings.TrimSpace(a)) {
	case "", "NO ACTION", "RESTRICT":
		return "noaction"
	default:
		return strings.ToLower(strings.Join(strings.Fields(a), " "))
	}
}

func lowerSorted(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = strings.ToLower(s)
	}
	sort.Strings(out)
	return out
}

func containsCI(haystack []string, needle string) bool {
	for _, h := range haystack {
		if strings.EqualFold(h, needle) {
			return true
		}
	}
	return false
}

// assertManifestWritten finds the run manifest under dataDir and checks it
// records the current renderer version and a plan fingerprint (#64).
func assertManifestWritten(t *testing.T, dataDir string) {
	t.Helper()
	assertManifestWrittenWithAIReview(t, dataDir, false)
}

func assertManifestWrittenWithAIReview(t *testing.T, dataDir string, wantAIReview bool) {
	t.Helper()
	var found string
	_ = filepath.WalkDir(dataDir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && d.Name() == "manifest.json" {
			found = path
		}
		return nil
	})
	if found == "" {
		t.Fatal("no manifest.json artifact written by the run (#64)")
	}
	blob, err := os.ReadFile(found)
	if err != nil {
		t.Fatalf("reading manifest: %v", err)
	}
	var m runManifest
	if err := json.Unmarshal(blob, &m); err != nil {
		t.Fatalf("manifest is not valid JSON: %v", err)
	}
	if m.RendererVersion != ddl.RendererVersion {
		t.Errorf("manifest renderer_version=%q, want %q", m.RendererVersion, ddl.RendererVersion)
	}
	if !strings.HasPrefix(m.PlanFingerprint, "sha256:") || !strings.HasPrefix(m.SourceFingerprint, "sha256:") {
		t.Errorf("manifest fingerprints malformed: plan=%q source=%q", m.PlanFingerprint, m.SourceFingerprint)
	}
	if m.AIReviewEnabled != wantAIReview {
		t.Errorf("manifest ai_review_enabled=%v, want %v", m.AIReviewEnabled, wantAIReview)
	}
}

func countWithPK(tables []driver.Table) int {
	n := 0
	for _, t := range tables {
		if len(t.PrimaryKey) > 0 {
			n++
		}
	}
	return n
}

// Hermetic unit test for the FK identity key (no live DB). Pins that
// composite FKs are matched by paired local→ref columns and that
// no-op referential actions fold together while real ones don't.
func TestFKKey(t *testing.T) {
	base := driver.ForeignKey{
		Name: "fk1", Columns: []string{"a", "b"}, RefTable: "Parent",
		RefColumns: []string{"x", "y"}, OnDelete: "CASCADE", OnUpdate: "",
	}
	// Order-insensitive at the FK level, case-insensitive, name-independent.
	reordered := driver.ForeignKey{
		Name: "renamed", Columns: []string{"B", "A"}, RefTable: "parent",
		RefColumns: []string{"Y", "X"}, OnDelete: "cascade", OnUpdate: "NO ACTION",
	}
	if fkKey(base) != fkKey(reordered) {
		t.Errorf("equivalent FKs produced different keys:\n %s\n %s", fkKey(base), fkKey(reordered))
	}
	// Wrong referenced column must differ.
	wrongRef := base
	wrongRef.RefColumns = []string{"x", "z"}
	if fkKey(base) == fkKey(wrongRef) {
		t.Error("FK pointing at different referenced columns must not match")
	}
	// Different action must differ.
	wrongAction := base
	wrongAction.OnDelete = "SET NULL"
	if fkKey(base) == fkKey(wrongAction) {
		t.Error("FK with different ON DELETE must not match")
	}
}
