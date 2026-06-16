package main

// `smt snapshot` and `smt sync` — the schema-diff feature.
//
// snapshot: extract the current source schema and store it in the SMT state DB
//   as a source-schema baseline/history artifact.
//
// sync: extract the current source schema, introspect the live target schema,
//   render the structural diff as ALTER statements, and either write the SQL
//   to a file (default) or apply it against the target (--apply).

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/urfave/cli/v2"

	"smt/internal/checkpoint"
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
	}
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
		Usage: "Diff source schema against the live target and (optionally) apply ALTERs",
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "apply", Usage: "Execute ALTERs against the target (default: emit SQL for review)"},
			&cli.StringFlag{Name: "out", Aliases: []string{"o"}, Value: "migration.sql", Usage: "Output file when not applying"},
			&cli.BoolFlag{Name: "allow-data-loss", Usage: "Permit data-loss-risk statements (column drops, table drops) when applying"},
			&cli.BoolFlag{Name: "save-snapshot", Usage: "After a successful sync, save the new schema as the next baseline snapshot"},
		},
		Action: runSync,
	}
}

func runSync(c *cli.Context) error {
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

	if !c.Bool("allow-data-loss") {
		filtered := plan.FilterByRisk(schemadiff.RiskRebuildNeeded)
		if len(filtered.Statements) < len(plan.Statements) {
			fmt.Printf("Refusing to apply %d data-loss-risk statement(s) without --allow-data-loss.\n",
				len(plan.Statements)-len(filtered.Statements))
			return fmt.Errorf("aborted")
		}
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

// loadPreviousSnapshot returns the most recent stored snapshot for this
// (sourceType, schema). Kept for snapshot-history callers and tests; live
// target sync planning does not require a previous snapshot.
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
	if len(changes) == 0 {
		return
	}
	fmt.Printf("Unsupported change(s) skipped: %d\n", len(changes))
	for _, change := range changes {
		parts := []string{change.Description}
		if strings.TrimSpace(change.Table) != "" {
			parts = append(parts, "table "+change.Table)
		}
		if strings.TrimSpace(change.Reason) != "" {
			parts = append(parts, change.Reason)
		}
		fmt.Printf("  - %s\n", strings.Join(parts, " - "))
	}
}
