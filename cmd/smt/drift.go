package main

// `smt drift` — read-only target drift detection (#69). It introspects the
// EXISTING target schema and compares it against the DESIRED schema derived
// from the current source, reporting tables/columns that are missing on the
// target, present only on the target (extra), or changed. Cross-dialect type
// equivalence is handled by the deterministic comparator, so an mssql
// varchar(20) does not "drift" against a pg character varying(20).
//
// Nothing is modified. Exit status: 0 = in sync, 3 = drift detected, non-zero
// (cli error) = connection/introspection failure. Useful as a CI gate.

import (
	"context"
	"fmt"
	"time"

	"github.com/urfave/cli/v2"

	"smt/internal/config"
	"smt/internal/driver"
	"smt/internal/logging"
	"smt/internal/orchestrator"
	"smt/internal/pool"
	"smt/internal/schemadiff"
)

func driftCommand() *cli.Command {
	return &cli.Command{
		Name:  "drift",
		Usage: "Report schema drift between the source-derived schema and the live target (read-only)",
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "fail-on-destructive-only", Usage: "Exit non-zero only when drift requires drops (extra tables/columns); additive drift exits 0"},
		},
		Action: runDrift,
	}
}

func runDrift(c *cli.Context) error {
	cfg, profileName, configPath, err := loadConfig(c)
	if err != nil {
		return err
	}

	orch, err := orchestrator.NewWithOptions(cfg, orchestrator.Options{StateFile: c.String("state-file")})
	if err != nil {
		return err
	}
	defer orch.Close()
	orch.SetRunContext(profileName, configPath)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Desired: the current source schema, with identifiers normalized to the
	// target's on-disk convention so names line up with the introspected
	// target (mssql "Posts" -> pg "posts").
	logging.Info("introspecting source schema (%s)", cfg.Source.Schema)
	desired, err := orch.Source().ExtractSchema(ctx, cfg.Source.Schema)
	if err != nil {
		return fmt.Errorf("introspecting source: %w", err)
	}
	if err := loadAllConstraints(ctx, orch.Source(), desired); err != nil {
		return err
	}
	normalizeTableNames(desired, cfg.Target.Type)

	// Existing: introspect the live target through a reader on the target
	// connection.
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
	if err := loadAllConstraints(ctx, targetReader, existing); err != nil {
		return err
	}

	drift := schemadiff.ComputeDrift(desired, existing, cfg.Source.Type, cfg.Target.Type)
	printDriftReport(drift)

	if drift.IsEmpty() {
		return nil
	}
	if c.Bool("fail-on-destructive-only") && !drift.HasDestructiveDrift() {
		return nil
	}
	// cli.Exit sets the process exit code without printing a Go error trace.
	return cli.Exit("", 3)
}

// targetAsSource adapts the target connection into a SourceConfig so the same
// deterministic reader path can introspect it.
func targetAsSource(cfg *config.Config) *config.SourceConfig {
	return &config.SourceConfig{
		Type:            cfg.Target.Type,
		Host:            cfg.Target.Host,
		Port:            cfg.Target.Port,
		Database:        cfg.Target.Database,
		User:            cfg.Target.User,
		Password:        cfg.Target.Password,
		Schema:          cfg.Target.Schema,
		SSLMode:         cfg.Target.SSLMode,
		TrustServerCert: cfg.Target.TrustServerCert,
		Encrypt:         cfg.Target.Encrypt,
	}
}

func normalizeTableNames(tables []driver.Table, targetType string) {
	for i := range tables {
		tables[i].Name = driver.NormalizeIdentifier(targetType, tables[i].Name)
		for j := range tables[i].Columns {
			tables[i].Columns[j].Name = driver.NormalizeIdentifier(targetType, tables[i].Columns[j].Name)
		}
	}
}

func printDriftReport(d schemadiff.Drift) {
	fmt.Printf("Drift: %s\n", d.Summary())
	if d.IsEmpty() {
		return
	}
	for _, t := range d.MissingTables {
		fmt.Printf("  [missing]   table %s — present in source, absent on target\n", t)
	}
	for _, t := range d.ExtraTables {
		fmt.Printf("  [extra]     table %s — on target, not in source (drop is destructive)\n", t)
	}
	for _, td := range d.ChangedTables {
		fmt.Printf("  [changed]   table %s\n", td.Name)
		for _, c := range td.MissingColumns {
			fmt.Printf("                + column %s missing on target\n", c)
		}
		for _, c := range td.ExtraColumns {
			fmt.Printf("                - column %s extra on target (drop is destructive)\n", c)
		}
		for _, delta := range td.ColumnDeltas {
			fmt.Printf("                ~ %s\n", delta)
		}
	}
}
