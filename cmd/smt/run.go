package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/urfave/cli/v2"

	"smt/internal/orchestrator"
)

// createCommand defines `smt create`: extract the source schema and apply
// it to the target as CREATE TABLE / index / FK / check DDL. The work is
// driven by Orchestrator.Run which calls each phase in order.
func createCommand() *cli.Command {
	return &cli.Command{
		Name:  "create",
		Usage: "Extract source schema and create matching DDL on the target",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "source-schema", Usage: "Override source schema from config"},
			&cli.StringFlag{Name: "target-schema", Usage: "Override target schema from config"},
		},
		Action: runCreate,
	}
}

func runCreate(c *cli.Context) error {
	cfg, profileName, configPath, err := loadConfig(c)
	if err != nil {
		return err
	}
	if c.IsSet("source-schema") {
		cfg.Source.Schema = c.String("source-schema")
	}
	if c.IsSet("target-schema") {
		cfg.Target.Schema = c.String("target-schema")
	}

	orch, err := orchestrator.NewWithOptions(cfg, orchestrator.Options{
		StateFile: c.String("state-file"),
	})
	if err != nil {
		return err
	}
	defer orch.Close()
	orch.SetRunContext(profileName, configPath)

	ctx, cancel := withSignalCancel(context.Background(), c.Duration("shutdown-timeout"))
	defer cancel()

	return orch.Run(ctx)
}

// healthCheckCommand defines `smt health-check`: open both connections,
// ping each, and report. No DDL runs.
func healthCheckCommand() *cli.Command {
	return &cli.Command{
		Name:   "health-check",
		Usage:  "Test database connections",
		Action: runHealthCheck,
	}
}

func runHealthCheck(c *cli.Context) error {
	cfg, _, _, err := loadConfig(c)
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	r, err := orch.HealthCheck(ctx)
	if err != nil {
		return err
	}

	printHealth(r)
	if !r.Healthy {
		return fmt.Errorf("health check failed")
	}
	return nil
}

func printHealth(r *orchestrator.HealthCheckResult) {
	fmt.Printf("\nSource (%s): %s (%dms)\n", r.SourceDBType, statusWord(r.SourceConnected), r.SourceLatencyMs)
	if r.SourceError != "" {
		fmt.Printf("  Error: %s\n", r.SourceError)
	}
	if r.SourceConnected && r.SourceTableCount > 0 {
		fmt.Printf("  Tables: %d\n", r.SourceTableCount)
	}
	fmt.Printf("Target (%s): %s (%dms)\n", r.TargetDBType, statusWord(r.TargetConnected), r.TargetLatencyMs)
	if r.TargetError != "" {
		fmt.Printf("  Error: %s\n", r.TargetError)
	}
	fmt.Printf("\nOverall: %s\n", healthWord(r.Healthy))
}

func statusWord(ok bool) string {
	if ok {
		return "OK"
	}
	return "FAILED"
}

func healthWord(ok bool) string {
	if ok {
		return "HEALTHY"
	}
	return "UNHEALTHY"
}

// historyCommand defines `smt history`: list past runs, or detail one with --run.
func historyCommand() *cli.Command {
	return &cli.Command{
		Name:  "history",
		Usage: "List previous schema operations",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "run", Usage: "Show details for a specific run ID"},
		},
		Action: runHistory,
	}
}

func runHistory(c *cli.Context) error {
	cfg, _, _, err := loadConfig(c)
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

	if id := c.String("run"); id != "" {
		return orch.ShowRunDetails(id)
	}
	return orch.ShowHistory()
}

// syncCommand, validateCommand, snapshotCommand, and analyzeCommand are
// stubbed here; sync + validate + snapshot land in Phase 6 (schema diff)
// and analyze will return when the AI repurposing happens (also Phase 6).
func syncCommand() *cli.Command {
	return &cli.Command{
		Name:  "sync",
		Usage: "Diff source schema against last snapshot and apply ALTER statements",
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "apply", Usage: "Execute ALTERs against the target (default: emit SQL for review)"},
			&cli.StringFlag{Name: "out", Aliases: []string{"o"}, Value: "migration.sql", Usage: "Output file when not applying"},
		},
		Action: notImplemented("sync"),
	}
}

func validateCommand() *cli.Command {
	return &cli.Command{
		Name:   "validate",
		Usage:  "Compare source vs target schema and report drift",
		Action: notImplemented("validate"),
	}
}

func snapshotCommand() *cli.Command {
	return &cli.Command{
		Name:   "snapshot",
		Usage:  "Capture the current source schema for future diffing",
		Action: notImplemented("snapshot"),
	}
}

func analyzeCommand() *cli.Command {
	return &cli.Command{
		Name:   "analyze",
		Usage:  "Analyze source schema and suggest configuration",
		Action: notImplemented("analyze"),
	}
}

// withSignalCancel returns a derived context that is cancelled when the
// process receives SIGINT or SIGTERM. After timeout from cancellation,
// the process is force-exited.
func withSignalCancel(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-sigCh:
			fmt.Fprintln(os.Stderr, "\nReceived signal, shutting down...")
			cancel()
			time.AfterFunc(timeout, func() {
				fmt.Fprintln(os.Stderr, "Shutdown timeout reached; forcing exit.")
				os.Exit(int(syscall.SIGTERM))
			})
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
