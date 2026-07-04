package main

// `smt snapshot` and `smt sync` — the schema-diff feature.
//
// snapshot: extract the current source schema and store it in the SMT state DB
//   as a source-schema baseline/history artifact.
//
// sync: extract the current source schema, diff it against a baseline, render
//   the structural diff as ALTER statements, and either write the SQL to a
//   file (default) or apply it against the target (--apply). The baseline is
//   selected with --against:
//     --against target   (default) introspect the live target schema and diff
//                        desired-vs-existing (needs a target connection).
//     --against snapshot diff against the latest stored source snapshot —
//                        "what changed in my source since the last baseline?"
//                        Fully offline for planning; a target connection is
//                        only opened for --apply.

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/urfave/cli/v2"

	"smt/internal/checkpoint"
	"smt/internal/config"
	"smt/internal/ddl"
	"smt/internal/driver"
	"smt/internal/exitcodes"
	"smt/internal/logging"
	"smt/internal/orchestrator"
	"smt/internal/pool"
	"smt/internal/schemadiff"
	"smt/internal/version"
)

func snapshotCommand() *cli.Command {
	return &cli.Command{
		Name:  "snapshot",
		Usage: "Capture the current source schema as a snapshot for future diffing",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "out", Aliases: []string{"o"}, Usage: "Also write the snapshot JSON to this file"},
		},
		Action: runSnapshot,
		Subcommands: []*cli.Command{
			{
				Name:    "list",
				Aliases: []string{"ls"},
				Usage:   "List stored source-schema snapshots (newest first)",
				Flags: []cli.Flag{
					&cli.IntFlag{Name: "limit", Aliases: []string{"n"}, Value: 50, Usage: "Maximum snapshots to show"},
				},
				Action: runSnapshotList,
			},
		},
	}
}

func runSnapshotList(c *cli.Context) error {
	if c.String("state-file") != "" {
		return fmt.Errorf("snapshot list requires the SQLite state backend; it is not available with --state-file")
	}
	cfg, _, _, err := loadConfig(c)
	if err != nil {
		return err
	}

	dataDir := cfg.Migration.DataDir
	if dataDir == "" {
		dataDir, err = config.DefaultDataDir()
		if err != nil {
			return err
		}
	}
	state, err := checkpoint.New(dataDir)
	if err != nil {
		return err
	}
	defer state.Close()

	snaps, err := state.ListSnapshots(c.Int("limit"))
	if err != nil {
		return err
	}
	if len(snaps) == 0 {
		fmt.Println("No snapshots found. Run `smt snapshot` to capture one.")
		return nil
	}

	fmt.Printf("%-5s  %-10s  %-20s  %-6s  %s\n", "ID", "SOURCE", "SCHEMA", "TABLES", "CAPTURED")
	for _, s := range snaps {
		tables := "?"
		var snap schemadiff.Snapshot
		if json.Unmarshal(s.Payload, &snap) == nil {
			tables = strconv.Itoa(len(snap.Tables))
		}
		fmt.Printf("%-5d  %-10s  %-20s  %-6s  %s\n",
			s.ID, s.SourceType, s.Schema, tables, s.CapturedAt.Format(time.RFC3339))
	}
	return nil
}

func runSnapshot(c *cli.Context) error {
	cfg, profileName, configPath, err := loadConfig(c)
	if err != nil {
		return err
	}

	orch, err := orchestrator.NewWithOptions(cfg, orchestrator.Options{
		StateFile:  c.String("state-file"),
		SourceOnly: true,
	})
	if err != nil {
		return err
	}
	defer orch.Close()
	orch.SetRunContext(profileName, configPath)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	logging.Info("extracting source schema for snapshot")
	tables, err := orch.Source().ExtractSchema(ctx, cfg.Source.Schema)
	if err != nil {
		return fmt.Errorf("extracting schema: %w", err)
	}
	if err := loadAllConstraints(ctx, orch.Source(), tables); err != nil {
		return err
	}

	snap := schemadiff.Snapshot{
		Version:    schemadiff.CurrentSnapshotVersion,
		Schema:     cfg.Source.Schema,
		SourceType: cfg.Source.Type,
		CapturedAt: time.Now().UTC(),
		Tables:     tables,
	}
	payload, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshaling snapshot: %w", err)
	}

	state, ok := orch.State().(*checkpoint.State)
	if !ok {
		return fmt.Errorf("snapshot storage requires the SQLite state backend")
	}
	id, err := state.SaveSnapshot(snap.SourceType, snapshotSourceIdentity(cfg.Source), snap.Schema, snap.CapturedAt, payload)
	if err != nil {
		return err
	}
	fmt.Printf("Snapshot saved (id=%d, %d tables, captured_at=%s)\n",
		id, len(tables), snap.CapturedAt.Format(time.RFC3339))

	if out := c.String("out"); out != "" {
		if err := os.WriteFile(out, payload, 0600); err != nil {
			return err
		}
		fmt.Printf("Snapshot also written to %s\n", out)
	}
	return nil
}

func syncCommand() *cli.Command {
	return &cli.Command{
		Name:  "sync",
		Usage: "Diff source schema against the live target (or the latest snapshot) and (optionally) apply ALTERs",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "against", Value: "target", Usage: "Baseline to diff against: 'target' (introspect the live target) or 'snapshot' (latest stored snapshot; offline planning)"},
			&cli.BoolFlag{Name: "apply", Usage: "Execute ALTERs against the target (default: emit SQL for review)"},
			&cli.StringFlag{Name: "out", Aliases: []string{"o"}, Value: "migration.sql", Usage: "Output file when not applying"},
			&cli.BoolFlag{Name: "allow-data-loss", Usage: "Permit data-loss-risk statements (column drops, table drops) when applying"},
			&cli.BoolFlag{Name: "save-snapshot", Usage: "After a successful sync, save the new schema as the next baseline snapshot"},
		},
		Action: runSync,
	}
}

func runSync(c *cli.Context) error {
	switch strings.ToLower(strings.TrimSpace(c.String("against"))) {
	case "", "target":
		return runSyncAgainstTarget(c)
	case "snapshot":
		return runSyncAgainstSnapshot(c)
	default:
		return fmt.Errorf("invalid --against value %q (expected 'target' or 'snapshot')", c.String("against"))
	}
}

const (
	syncModeTarget   = "target"
	syncModeSnapshot = "snapshot"

	syncStepPlan         = "plan"
	syncStepApply        = "apply"
	syncStepSaveSnapshot = "save_snapshot"
)

func syncPhase(mode, step string) string {
	return "sync_" + mode + "_" + step
}

func runSyncAgainstTarget(c *cli.Context) error {
	cfg, profileName, configPath, err := loadConfig(c)
	if err != nil {
		return err
	}

	orch, err := orchestrator.NewWithOptions(cfg, orchestrator.Options{
		StateFile: c.String("state-file"),
	})
	if err != nil {
		return err
	}
	defer orch.Close()
	orch.SetRunContext(profileName, configPath)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	var state *checkpoint.State
	if c.Bool("save-snapshot") {
		var ok bool
		state, ok = orch.State().(*checkpoint.State)
		if !ok {
			return fmt.Errorf("saving snapshots requires the SQLite state backend")
		}
	}

	sourceDialect := driver.Canonicalize(cfg.Source.Type)
	targetDialect := driver.Canonicalize(cfg.Target.Type)
	opts := schemadiff.DriftOptions{
		CompareIndexes:     cfg.Migration.CreateIndexes,
		CompareForeignKeys: cfg.Migration.CreateForeignKeys,
		CompareChecks:      cfg.Migration.CreateCheckConstraints,
	}

	logging.Info("extracting current source schema")
	currTables, err := orch.Source().ExtractSchema(ctx, cfg.Source.Schema)
	if err != nil {
		return fmt.Errorf("extracting current schema: %w", err)
	}
	if err := loadAllConstraints(ctx, orch.Source(), currTables); err != nil {
		return err
	}

	currSnap := schemadiff.Snapshot{
		Version:    schemadiff.CurrentSnapshotVersion,
		Schema:     cfg.Source.Schema,
		SourceType: cfg.Source.Type,
		CapturedAt: time.Now().UTC(),
		Tables:     currTables,
	}

	norm := func(name string) string { return driver.NormalizeIdentifier(targetDialect, name) }
	allSourceNorm := make(map[string]bool, len(currTables))
	for _, t := range currTables {
		allSourceNorm[strings.ToLower(norm(t.Name))] = true
	}
	desired := filterDesiredScope(currTables, cfg.Migration.IncludeTables, cfg.Migration.ExcludeTables)
	// Fail closed on identifier collisions before rendering any ALTER (#189):
	// on a PostgreSQL target two source names that fold to the same identifier
	// would drift the target silently.
	if err := driver.CheckIdentifierCollisions(targetDialect, desired); err != nil {
		return err
	}
	desired = schemadiff.NormalizeIdentifiers(desired, norm)
	desired = schemadiff.RetargetSchema(desired, cfg.Target.Schema)

	logging.Info("introspecting target schema (%s)", cfg.Target.Schema)
	targetReader, err := pool.NewSourcePool(targetAsSource(cfg), 4)
	if err != nil {
		return fmt.Errorf("opening target reader: %w", err)
	}
	defer targetReader.Close()
	existing, err := targetReader.ExtractSchema(ctx, cfg.Target.Schema)
	if err != nil {
		return fmt.Errorf("introspecting target: %w", err)
	}
	if len(cfg.Migration.IncludeTables) > 0 || len(cfg.Migration.ExcludeTables) > 0 {
		existing = filterToManagedSet(existing, desired, allSourceNorm)
	}
	if err := loadConstraintsGated(ctx, targetReader, existing, opts); err != nil {
		return err
	}

	diff := schemadiff.ComputeLiveDiff(desired, existing, sourceDialect, targetDialect, opts)
	if diff.IsEmpty() {
		fmt.Println("No schema drift: target matches the source-derived schema.")
		return nil
	}

	fmt.Printf("Diff: %s\n", diff.Summary())

	logging.Info("rendering diff deterministically as %s SQL...", cfg.Target.Type)
	plan, err := schemadiff.RenderDeterministicWithOptions(diff, schemadiff.RenderOptions{
		TargetSchema:      cfg.Target.Schema,
		TargetDialect:     targetDialect,
		SourceDialect:     sourceDialect,
		ExistingDialect:   targetDialect,
		UnknownTypePolicy: cfg.SchemaGeneration.UnknownTypePolicy,
	})
	if err != nil {
		return err
	}
	if plan.IsEmpty() {
		fmt.Println("Renderer returned no statements; nothing to apply.")
		return nil
	}
	return finishSyncPlan(c, ctx, orch, cfg, profileName, configPath, syncModeTarget, plan, state, currSnap, snapshotSourceIdentity(cfg.Source))
}

// runSyncAgainstSnapshot diffs the current source schema against the latest
// stored snapshot (offline — no target introspection) and renders the delta
// as deterministic target-dialect ALTERs. The target connection is opened
// only when --apply is set.
func runSyncAgainstSnapshot(c *cli.Context) error {
	if c.String("state-file") != "" {
		return fmt.Errorf("sync --against snapshot requires the SQLite state backend; it is not available with --state-file")
	}
	cfg, profileName, configPath, err := loadConfig(c)
	if err != nil {
		return err
	}

	apply := c.Bool("apply")
	orch, err := orchestrator.NewWithOptions(cfg, orchestrator.Options{
		StateFile:  c.String("state-file"),
		SourceOnly: !apply,
	})
	if err != nil {
		return err
	}
	defer orch.Close()
	orch.SetRunContext(profileName, configPath)

	state, ok := orch.State().(*checkpoint.State)
	if !ok {
		return fmt.Errorf("sync --against snapshot requires the SQLite state backend; it is not available with --state-file")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	sourceIdentity := snapshotSourceIdentity(cfg.Source)
	prevSnap, err := loadPreviousSnapshot(state, cfg.Source.Type, sourceIdentity, cfg.Source.Schema)
	if err != nil {
		return err
	}
	if apply {
		if err := refuseUnsafeSnapshotApplyRerun(state, prevSnap, cfg); err != nil {
			return err
		}
	}

	logging.Info("extracting current source schema")
	currTables, err := orch.Source().ExtractSchema(ctx, cfg.Source.Schema)
	if err != nil {
		return fmt.Errorf("extracting current schema: %w", err)
	}
	if err := loadAllConstraints(ctx, orch.Source(), currTables); err != nil {
		return err
	}
	currSnap := schemadiff.Snapshot{
		Version:    schemadiff.CurrentSnapshotVersion,
		Schema:     cfg.Source.Schema,
		SourceType: cfg.Source.Type,
		CapturedAt: time.Now().UTC(),
		Tables:     currTables,
	}

	diff, plan, err := buildSnapshotSyncPlan(prevSnap, currSnap, cfg)
	if err != nil {
		// Surface what changed even when rendering fails, so the operator
		// knows which delta the renderer could not express.
		if !diff.IsEmpty() {
			fmt.Printf("Diff since snapshot (%s): %s\n", prevSnap.CapturedAt.Format(time.RFC3339), diff.Summary())
		}
		return err
	}
	if diff.IsEmpty() {
		fmt.Printf("No schema changes since the last snapshot (captured %s).\n",
			prevSnap.CapturedAt.Format(time.RFC3339))
		return nil
	}
	fmt.Printf("Diff since snapshot (%s): %s\n", prevSnap.CapturedAt.Format(time.RFC3339), diff.Summary())
	if plan.IsEmpty() {
		fmt.Println("Renderer returned no statements; nothing to apply.")
		return nil
	}
	return finishSyncPlan(c, ctx, orch, cfg, profileName, configPath, syncModeSnapshot, plan, state, currSnap, sourceIdentity)
}

// buildSnapshotSyncPlan computes the offline snapshot-to-snapshot diff and
// renders it as a deterministic target-dialect ALTER plan. The migration
// include/exclude scope applies to both snapshots and unmanaged object kinds
// (create_indexes / create_foreign_keys / create_check_constraints) are
// dropped, matching the live-target mode's gating; the diff runs on
// source-side names, then identifiers and schema references are rewritten
// to the target convention before rendering (same order Normalize's and
// WithTargetSchema's contracts require). Pure — no database or state I/O,
// no mutation of either snapshot — which is what keeps snapshot-mode
// planning offline and the caller's snapshot safe to persist as the next
// baseline.
func buildSnapshotSyncPlan(prev, curr schemadiff.Snapshot, cfg *config.Config) (schemadiff.Diff, schemadiff.Plan, error) {
	include, exclude := cfg.Migration.IncludeTables, cfg.Migration.ExcludeTables
	prev.Tables = filterDesiredScope(prev.Tables, include, exclude)
	curr.Tables = filterDesiredScope(curr.Tables, include, exclude)

	diff := schemadiff.Compute(prev, curr).FilterManagedKinds(
		cfg.Migration.CreateIndexes,
		cfg.Migration.CreateForeignKeys,
		cfg.Migration.CreateCheckConstraints,
	)
	if diff.IsEmpty() {
		return diff, schemadiff.Plan{}, nil
	}

	sourceDialect := driver.Canonicalize(cfg.Source.Type)
	targetDialect := driver.Canonicalize(cfg.Target.Type)
	norm := func(name string) string { return driver.NormalizeIdentifier(targetDialect, name) }
	// Fail closed on identifier collisions before rendering any DDL (#189).
	if err := driver.CheckIdentifierCollisions(targetDialect, curr.Tables); err != nil {
		return diff, schemadiff.Plan{}, err
	}
	rendered := diff.Normalize(norm).WithTargetSchema(cfg.Target.Schema)

	plan, err := schemadiff.RenderDeterministicWithOptions(rendered, schemadiff.RenderOptions{
		TargetSchema:      cfg.Target.Schema,
		TargetDialect:     targetDialect,
		SourceDialect:     sourceDialect,
		UnknownTypePolicy: cfg.SchemaGeneration.UnknownTypePolicy,
	})
	if err != nil {
		return diff, schemadiff.Plan{}, err
	}
	return diff, plan, nil
}

// finishSyncPlan is the shared tail of both sync modes: write the plan to
// --out for review, or gate (unsupported changes, data-loss risk) and apply
// it against the target, optionally saving the new baseline snapshot.
func finishSyncPlan(c *cli.Context, ctx context.Context, orch *orchestrator.Orchestrator, cfg *config.Config, profileName, configPath, mode string, plan schemadiff.Plan, snapshotState *checkpoint.State, currSnap schemadiff.Snapshot, sourceIdentity string) error {
	printPlanSummary(plan)

	if !c.Bool("apply") {
		out := c.String("out")
		if err := os.WriteFile(out, []byte(plan.SQL()), 0600); err != nil {
			return err
		}
		fmt.Printf("%d statement(s) written to %s for review.\n", len(plan.Statements), out)
		fmt.Println("Run again with --apply to execute against the target.")
		return nil
	}

	return applySyncPlan(ctx, orch.Target(), plan, syncApplyOptions{
		mode:           mode,
		cfg:            cfg,
		profileName:    profileName,
		configPath:     configPath,
		state:          orch.State(),
		snapshotState:  snapshotState,
		currSnap:       currSnap,
		sourceIdentity: sourceIdentity,
		allowDataLoss:  c.Bool("allow-data-loss"),
		saveSnapshot:   c.Bool("save-snapshot"),
	})
}

type syncApplyOptions struct {
	mode           string
	cfg            *config.Config
	profileName    string
	configPath     string
	state          checkpoint.StateBackend
	snapshotState  *checkpoint.State
	currSnap       schemadiff.Snapshot
	sourceIdentity string
	allowDataLoss  bool
	saveSnapshot   bool
	runID          string
}

type syncRunConfig struct {
	Config any             `json:"config"`
	Sync   syncRunMetadata `json:"sync"`
}

type syncRunMetadata struct {
	Mode          string `json:"mode"`
	AllowDataLoss bool   `json:"allow_data_loss"`
	SaveSnapshot  bool   `json:"save_snapshot"`
}

type syncRunManifest struct {
	SMTVersion                string `json:"smt_version"`
	RendererVersion           string `json:"renderer_version"`
	SourceDialect             string `json:"source_dialect"`
	TargetDialect             string `json:"target_dialect"`
	TargetSchema              string `json:"target_schema"`
	UnknownTypePolicy         string `json:"unknown_type_policy"`
	SyncMode                  string `json:"sync_mode"`
	StatementCount            int    `json:"statement_count"`
	UnsupportedCount          int    `json:"unsupported_count"`
	SourceSnapshotFingerprint string `json:"source_snapshot_fingerprint"`
	PlanFingerprint           string `json:"plan_fingerprint"`
}

func applySyncPlan(ctx context.Context, tgt sqlExecutor, plan schemadiff.Plan, opts syncApplyOptions) error {
	if opts.state == nil {
		return fmt.Errorf("sync --apply requires a state backend")
	}
	if opts.cfg == nil {
		return fmt.Errorf("sync --apply requires configuration")
	}
	runID := opts.runID
	if runID == "" {
		runID = uuid.NewString()
	}
	runConfig := syncRunConfig{
		Config: opts.cfg.Sanitized(),
		Sync: syncRunMetadata{
			Mode:          opts.mode,
			AllowDataLoss: opts.allowDataLoss,
			SaveSnapshot:  opts.saveSnapshot,
		},
	}
	if err := opts.state.CreateRun(runID, checkpoint.RunKindApply, opts.cfg.Source.Schema, opts.cfg.Target.Schema, runConfig, opts.profileName, opts.configPath); err != nil {
		return fmt.Errorf("recording sync run start: %w", err)
	}

	fail := func(err error) error {
		_ = opts.state.CompleteRun(runID, "failed", err.Error())
		return err
	}

	if err := opts.state.UpdatePhase(runID, syncPhase(opts.mode, syncStepPlan)); err != nil {
		return fail(fmt.Errorf("recording sync plan phase: %w", err))
	}
	if err := writeSyncRunArtifacts(opts.cfg.Migration.DataDir, runID, plan, opts); err != nil {
		return fail(fmt.Errorf("writing sync run artifacts: %w", err))
	}

	if len(plan.Unsupported) > 0 {
		printUnsupportedChanges(plan.Unsupported)
		return fail(fmt.Errorf("refusing to apply plan with unsupported change(s)"))
	}

	if err := gatePlanForApply(plan, opts.allowDataLoss); err != nil {
		return fail(err)
	}

	if err := opts.state.UpdatePhase(runID, syncPhase(opts.mode, syncStepApply)); err != nil {
		return fail(fmt.Errorf("recording sync apply phase: %w", err))
	}
	if err := applyPlan(ctx, tgt, plan); err != nil {
		return fail(err)
	}
	fmt.Printf("Applied %d statement(s) successfully; skipped 0 unsupported change(s).\n", len(plan.Statements))

	if opts.saveSnapshot {
		if opts.snapshotState == nil {
			return fail(fmt.Errorf("saving snapshots requires the SQLite state backend"))
		}
		if err := opts.state.UpdatePhase(runID, syncPhase(opts.mode, syncStepSaveSnapshot)); err != nil {
			return fail(fmt.Errorf("recording sync snapshot phase: %w", err))
		}
		payload, err := json.Marshal(opts.currSnap)
		if err != nil {
			return fail(fmt.Errorf("marshaling baseline snapshot: %w", err))
		}
		id, err := opts.snapshotState.SaveSnapshot(opts.currSnap.SourceType, opts.sourceIdentity, opts.currSnap.Schema, opts.currSnap.CapturedAt, payload)
		if err != nil {
			return fail(fmt.Errorf("saving baseline snapshot: %w", err))
		}
		fmt.Printf("New baseline snapshot saved (id=%d).\n", id)
	}
	if err := opts.state.CompleteRun(runID, "success", ""); err != nil {
		return fmt.Errorf("recording sync run success: %w", err)
	}
	return nil
}

func writeSyncRunArtifacts(dataDir, runID string, plan schemadiff.Plan, opts syncApplyOptions) error {
	dir, err := syncDDLArtifactDir(dataDir, runID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "migration.sql"), []byte(plan.SQL()), 0600); err != nil {
		return err
	}
	manifest, err := buildSyncRunManifest(plan, opts)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "manifest.json"), append(data, '\n'), 0600)
}

func syncDDLArtifactDir(dataDir, runID string) (string, error) {
	if strings.TrimSpace(dataDir) == "" {
		var err error
		dataDir, err = config.DefaultDataDir()
		if err != nil {
			return "", err
		}
	}
	return filepath.Join(dataDir, "runs", runID, "ddl"), nil
}

func buildSyncRunManifest(plan schemadiff.Plan, opts syncApplyOptions) (syncRunManifest, error) {
	sourceFP, err := syncSnapshotFingerprint(opts.currSnap)
	if err != nil {
		return syncRunManifest{}, err
	}
	return syncRunManifest{
		SMTVersion:                version.Version,
		RendererVersion:           ddl.RendererVersion,
		SourceDialect:             driver.Canonicalize(opts.cfg.Source.Type),
		TargetDialect:             driver.Canonicalize(opts.cfg.Target.Type),
		TargetSchema:              opts.cfg.Target.Schema,
		UnknownTypePolicy:         opts.cfg.SchemaGeneration.UnknownTypePolicy,
		SyncMode:                  opts.mode,
		StatementCount:            len(plan.Statements),
		UnsupportedCount:          len(plan.Unsupported),
		SourceSnapshotFingerprint: sourceFP,
		PlanFingerprint:           syncFingerprintBytes([]byte(plan.SQL())),
	}, nil
}

func syncSnapshotFingerprint(snap schemadiff.Snapshot) (string, error) {
	payload := struct {
		Version    int            `json:"version,omitempty"`
		Schema     string         `json:"schema"`
		SourceType string         `json:"source_type"`
		Tables     []driver.Table `json:"tables"`
	}{
		Version:    snap.Version,
		Schema:     snap.Schema,
		SourceType: snap.SourceType,
		Tables:     syncFingerprintTables(snap.Tables),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return syncFingerprintBytes(data), nil
}

func syncFingerprintTables(tables []driver.Table) []driver.Table {
	out := make([]driver.Table, len(tables))
	for i := range tables {
		out[i] = tables[i]
		out[i].RowCount = 0
		out[i].EstimatedRowSize = 0
		out[i].Columns = append([]driver.Column(nil), tables[i].Columns...)
		for j := range out[i].Columns {
			out[i].Columns[j].SampleValues = nil
		}
	}
	return out
}

func syncFingerprintBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("sha256:%x", sum[:])
}

func refuseUnsafeSnapshotApplyRerun(state *checkpoint.State, prevSnap schemadiff.Snapshot, cfg *config.Config) error {
	runs, err := state.GetAllRuns()
	if err != nil {
		return err
	}
	for _, run := range runs {
		if run.SourceSchema != cfg.Source.Schema || run.TargetSchema != cfg.Target.Schema {
			continue
		}
		if !isPartialSnapshotApplyPhase(run.Phase) || run.Status != "failed" {
			continue
		}
		if run.CompletedAt != nil && prevSnap.CapturedAt.After(*run.CompletedAt) {
			return nil
		}
		return fmt.Errorf(
			"previous sync --against snapshot --apply run %s failed during %s; refusing to replay the stale snapshot plan. Inspect runs/%s/ddl/migration.sql, then recover with `smt sync --against target --apply` or capture a new baseline with `smt snapshot` after the target is correct",
			run.ID, run.Phase, run.ID,
		)
	}
	return nil
}

func isPartialSnapshotApplyPhase(phase string) bool {
	return phase == syncPhase(syncModeSnapshot, syncStepApply) ||
		phase == syncPhase(syncModeSnapshot, syncStepSaveSnapshot)
}

// gatePlanForApply refuses to apply a plan containing data-loss-risk
// statements unless the operator passed --allow-data-loss.
func gatePlanForApply(plan schemadiff.Plan, allowDataLoss bool) error {
	if allowDataLoss {
		return nil
	}
	filtered := plan.FilterByRisk(schemadiff.RiskRebuildNeeded)
	if len(filtered.Statements) < len(plan.Statements) {
		fmt.Printf("Refusing to apply %d data-loss-risk statement(s) without --allow-data-loss.\n",
			len(plan.Statements)-len(filtered.Statements))
		return fmt.Errorf("aborted")
	}
	return nil
}

// loadPreviousSnapshot returns the most recent stored snapshot for this
// (sourceType, sourceIdentity, schema). It is the baseline loader for `sync --against
// snapshot`; live target sync planning does not require a previous snapshot.
func loadPreviousSnapshot(state *checkpoint.State, sourceType, sourceIdentity, schema string) (schemadiff.Snapshot, error) {
	snapRow, err := state.GetLatestSnapshot(sourceType, sourceIdentity, schema)
	if err != nil {
		return schemadiff.Snapshot{}, err
	}
	if snapRow == nil {
		return schemadiff.Snapshot{}, fmt.Errorf("no snapshot found for %s/%s; run `smt snapshot` to capture one first", sourceType, schema)
	}
	var snap schemadiff.Snapshot
	if err := json.Unmarshal(snapRow.Payload, &snap); err != nil {
		return schemadiff.Snapshot{}, fmt.Errorf("decoding stored snapshot: %w", err)
	}
	return snap, nil
}

func snapshotSourceIdentity(src config.SourceConfig) string {
	host := strings.ToLower(strings.TrimSpace(src.Host))
	database := strings.TrimSpace(src.Database)
	return fmt.Sprintf("host=%s;port=%d;database=%s", host, src.Port, database)
}

// constraintLoader is the narrow subset of driver.Reader that
// loadAllConstraints uses. Declaring it as an interface lets tests pass
// a stub without standing up a full driver.
type constraintLoader interface {
	LoadIndexes(ctx context.Context, t *driver.Table) error
	LoadForeignKeys(ctx context.Context, t *driver.Table) error
	LoadCheckConstraints(ctx context.Context, t *driver.Table) error
}

// loadAllConstraints fills in the per-table indexes/FKs/checks. The
// driver's ExtractSchema returns just columns + PK; the constraint
// loaders are separate calls so the orchestrator can skip them when not
// needed. For snapshot/sync we always want the full picture.
func loadAllConstraints(ctx context.Context, src constraintLoader, tables []driver.Table) error {
	for i := range tables {
		t := &tables[i]
		if err := src.LoadIndexes(ctx, t); err != nil {
			return fmt.Errorf("loading indexes for %s: %w", t.Name, err)
		}
		if err := src.LoadForeignKeys(ctx, t); err != nil {
			return fmt.Errorf("loading FKs for %s: %w", t.Name, err)
		}
		if err := src.LoadCheckConstraints(ctx, t); err != nil {
			return fmt.Errorf("loading checks for %s: %w", t.Name, err)
		}
	}
	return nil
}

// sqlExecutor is the narrow subset of driver.Writer that applyPlan uses.
type sqlExecutor interface {
	ExecRaw(ctx context.Context, query string, args ...any) (int64, error)
}

// applyPlan executes each statement against the target writer in order.
// Stops at the first failure so the operator can investigate and re-run
// using the run artifact as the recovery record.
func applyPlan(ctx context.Context, tgt sqlExecutor, plan schemadiff.Plan) error {
	for i, s := range plan.Statements {
		logging.Info("[%d/%d] %s (risk=%s)", i+1, len(plan.Statements), s.Description, s.Risk)
		if _, err := tgt.ExecRaw(ctx, s.SQL); err != nil {
			return exitcodes.NewExitError(
				fmt.Errorf("statement %d (%s) failed: %w\nSQL: %s", i+1, s.Description, err, s.SQL),
				exitcodes.TransferError,
			)
		}
	}
	return nil
}

func printPlanSummary(plan schemadiff.Plan) {
	var safe, blocking, rebuild, destructive int
	for _, stmt := range plan.Statements {
		switch stmt.Risk {
		case schemadiff.RiskSafe:
			safe++
		case schemadiff.RiskBlocking:
			blocking++
		case schemadiff.RiskRebuildNeeded:
			rebuild++
		case schemadiff.RiskDataLoss:
			destructive++
		}
	}
	fmt.Printf("Plan: %d statement(s): %d safe, %d blocking, %d rebuild, %d destructive; %d unsupported change(s).\n",
		len(plan.Statements), safe, blocking, rebuild, destructive, len(plan.Unsupported))
}

func printUnsupportedChanges(changes []schemadiff.UnsupportedChange) {
	fmt.Print(formatUnsupportedChanges(changes))
}

func formatUnsupportedChanges(changes []schemadiff.UnsupportedChange) string {
	if len(changes) == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Unsupported change(s) skipped: %d\n", len(changes))
	for _, change := range changes {
		parts := []string{change.Description}
		if strings.TrimSpace(change.Table) != "" {
			parts = append(parts, "table "+change.Table)
		}
		if strings.TrimSpace(change.Reason) != "" {
			parts = append(parts, change.Reason)
		}
		fmt.Fprintf(&b, "  - %s\n", strings.Join(parts, " - "))
	}
	return b.String()
}
