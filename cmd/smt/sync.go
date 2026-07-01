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
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/urfave/cli/v2"

	"smt/internal/checkpoint"
	"smt/internal/config"
	"smt/internal/driver"
	"smt/internal/logging"
	"smt/internal/orchestrator"
	"smt/internal/pool"
	"smt/internal/schemadiff"
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
	id, err := state.SaveSnapshot(snap.SourceType, snap.Schema, snap.CapturedAt, payload)
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
	return finishSyncPlan(c, ctx, orch, plan, state, currSnap)
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

	prevSnap, err := loadPreviousSnapshot(state, cfg.Source.Type, cfg.Source.Schema)
	if err != nil {
		return err
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
	return finishSyncPlan(c, ctx, orch, plan, state, currSnap)
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
func finishSyncPlan(c *cli.Context, ctx context.Context, orch *orchestrator.Orchestrator, plan schemadiff.Plan, state *checkpoint.State, currSnap schemadiff.Snapshot) error {
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

	if len(plan.Unsupported) > 0 {
		printUnsupportedChanges(plan.Unsupported)
		return fmt.Errorf("refusing to apply plan with unsupported change(s)")
	}

	if err := gatePlanForApply(plan, c.Bool("allow-data-loss")); err != nil {
		return err
	}

	if err := applyPlan(ctx, orch.Target(), plan); err != nil {
		return err
	}
	fmt.Printf("Applied %d statement(s) successfully; skipped 0 unsupported change(s).\n", len(plan.Statements))

	if c.Bool("save-snapshot") {
		payload, _ := json.Marshal(currSnap)
		id, err := state.SaveSnapshot(currSnap.SourceType, currSnap.Schema, currSnap.CapturedAt, payload)
		if err != nil {
			return fmt.Errorf("saving baseline snapshot: %w", err)
		}
		fmt.Printf("New baseline snapshot saved (id=%d).\n", id)
	}
	return nil
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
// (sourceType, schema). It is the baseline loader for `sync --against
// snapshot`; live target sync planning does not require a previous snapshot.
func loadPreviousSnapshot(state *checkpoint.State, sourceType, schema string) (schemadiff.Snapshot, error) {
	snapRow, err := state.GetLatestSnapshot(sourceType, schema)
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
// (idempotent statements are the AI's responsibility, not ours).
func applyPlan(ctx context.Context, tgt sqlExecutor, plan schemadiff.Plan) error {
	for i, s := range plan.Statements {
		logging.Info("[%d/%d] %s (risk=%s)", i+1, len(plan.Statements), s.Description, s.Risk)
		if _, err := tgt.ExecRaw(ctx, s.SQL); err != nil {
			return fmt.Errorf("statement %d (%s) failed: %w\nSQL: %s", i+1, s.Description, err, s.SQL)
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
