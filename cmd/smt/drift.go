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
	"path/filepath"
	"strings"
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

	// Canonicalize dialect aliases (sqlserver→mssql, pg→postgres, …) before
	// they drive identifier normalization and cross-dialect column comparison,
	// which both key off the canonical driver name.
	sourceDialect := driver.Canonicalize(cfg.Source.Type)
	targetDialect := driver.Canonicalize(cfg.Target.Type)

	// Honor the same scope `create`/`sync` use: only the create_* object kinds
	// that are managed participate in drift, so a config that intentionally
	// leaves indexes/FKs/checks unmanaged doesn't report them as drift.
	opts := schemadiff.DriftOptions{
		CompareIndexes:     cfg.Migration.CreateIndexes,
		CompareForeignKeys: cfg.Migration.CreateForeignKeys,
		CompareChecks:      cfg.Migration.CreateCheckConstraints,
	}

	// Desired: the current source schema, with identifiers normalized to the
	// target's on-disk convention so names line up with the introspected
	// target (mssql "Posts" -> pg "posts").
	logging.Info("introspecting source schema (%s)", cfg.Source.Schema)
	desired, err := orch.Source().ExtractSchema(ctx, cfg.Source.Schema)
	if err != nil {
		return fmt.Errorf("introspecting source: %w", err)
	}
	norm := func(name string) string { return driver.NormalizeIdentifier(targetDialect, name) }
	// Filter on SOURCE names with the source-cased patterns (globs intact).
	desired = filterTablesByScope(desired, cfg.Migration.IncludeTables, cfg.Migration.ExcludeTables, norm)
	if err := loadConstraintsGated(ctx, orch.Source(), desired, opts); err != nil {
		return err
	}
	// Fold every desired identifier — table, column, AND index/FK column lists
	// and referenced tables — to the target's on-disk convention so constraint
	// comparisons line up with the introspected (already-normalized) target.
	// FK referenced-schema comparison is schema-relative inside ComputeDrift,
	// so no schema retargeting is needed here.
	desired = schemadiff.NormalizeIdentifiers(desired, norm)

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
	// Scope the target the same way: only compare tables the source manages,
	// so unrelated objects living in the target schema aren't flagged extra.
	// The target names are already normalized, so the filter also tries the
	// normalized form of each literal pattern (see filterTablesByScope).
	existing = filterTablesByScope(existing, cfg.Migration.IncludeTables, cfg.Migration.ExcludeTables, norm)
	if err := loadConstraintsGated(ctx, targetReader, existing, opts); err != nil {
		return err
	}

	drift := schemadiff.ComputeDrift(desired, existing, sourceDialect, targetDialect, opts)
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
// filterTablesByScope applies the migration include/exclude rules the same way
// the orchestrator does (exclude wins; include, when set, is an allowlist).
// It matches a name against each pattern case-insensitively (with glob
// support), and additionally tries the target-normalized form of literal
// patterns via norm — so a literal pattern like "Order Items" still matches a
// target table the dialect slugged to "order_items". Globs are not normalized
// (normalization would mangle `*`/`?`), so they keep plain CI semantics.
func filterTablesByScope(tables []driver.Table, include, exclude []string, norm func(string) string) []driver.Table {
	if len(include) == 0 && len(exclude) == 0 {
		return tables
	}
	out := make([]driver.Table, 0, len(tables))
	for _, t := range tables {
		if matchesAnyScoped(t.Name, exclude, norm) {
			continue
		}
		if len(include) > 0 && !matchesAnyScoped(t.Name, include, norm) {
			continue
		}
		out = append(out, t)
	}
	return out
}

func matchesAnyScoped(name string, patterns []string, norm func(string) string) bool {
	lower := strings.ToLower(name)
	for _, p := range patterns {
		if ok, _ := filepath.Match(strings.ToLower(p), lower); ok {
			return true
		}
		// Literal (glob-free) patterns: also match the dialect-normalized
		// form, so a name that needed slugging on the target still matches.
		if norm != nil && !strings.ContainsAny(p, "*?[") && norm(p) == name {
			return true
		}
	}
	return false
}

// loadConstraintsGated loads only the constraint kinds that are managed
// (per the create_* flags carried in opts), so unmanaged object kinds are
// neither introspected nor compared.
func loadConstraintsGated(ctx context.Context, src constraintLoader, tables []driver.Table, opts schemadiff.DriftOptions) error {
	for i := range tables {
		t := &tables[i]
		if opts.CompareIndexes {
			if err := src.LoadIndexes(ctx, t); err != nil {
				return fmt.Errorf("loading indexes for %s: %w", t.Name, err)
			}
		}
		if opts.CompareForeignKeys {
			if err := src.LoadForeignKeys(ctx, t); err != nil {
				return fmt.Errorf("loading FKs for %s: %w", t.Name, err)
			}
		}
		if opts.CompareChecks {
			if err := src.LoadCheckConstraints(ctx, t); err != nil {
				return fmt.Errorf("loading checks for %s: %w", t.Name, err)
			}
		}
	}
	return nil
}

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
		if td.PKDrift != "" {
			fmt.Printf("                ~ primary key: %s\n", td.PKDrift)
		}
		for _, c := range td.MissingColumns {
			fmt.Printf("                + column %s missing on target\n", c)
		}
		for _, c := range td.ExtraColumns {
			fmt.Printf("                - column %s extra on target (drop is destructive)\n", c)
		}
		for _, delta := range td.ColumnDeltas {
			fmt.Printf("                ~ %s\n", delta)
		}
		for _, ix := range td.MissingIndexes {
			fmt.Printf("                + index on (%s) missing on target\n", ix)
		}
		for _, ix := range td.ExtraIndexes {
			fmt.Printf("                - index on (%s) extra on target\n", ix)
		}
		for _, fk := range td.MissingForeignKeys {
			fmt.Printf("                + foreign key %s missing on target\n", fk)
		}
		for _, fk := range td.ExtraForeignKeys {
			fmt.Printf("                - foreign key %s extra on target\n", fk)
		}
		if td.CheckDrift != "" {
			fmt.Printf("                ~ check constraints: %s\n", td.CheckDrift)
		}
	}
}
