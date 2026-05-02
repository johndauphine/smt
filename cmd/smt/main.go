// Command smt is the schema migration tool. It extracts schemas from a
// source database, generates matching DDL on a target database, and (in a
// later phase) applies ALTER statements derived from diffing the source
// schema against a stored snapshot.
//
// SMT is the schema-only counterpart to DMT (the data migration tool): it
// shares DMT's driver model, AI-assisted type mapping, and TUI scaffolding
// but does not move rows.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/urfave/cli/v2"

	"smt/internal/exitcodes"
	"smt/internal/logging"
	"smt/internal/version"
)

func main() {
	app := &cli.App{
		Name:     version.Name,
		Usage:    version.Description,
		Version:  version.Version,
		Flags:    globalFlags(),
		Before:   applyGlobalFlags,
		Action:   topLevelAction,
		Commands: commands(),
	}

	if err := app.Run(os.Args); err != nil {
		code := exitcodes.FromError(err)
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		fmt.Fprintf(os.Stderr, "Exit code %d (%s)\n", code, exitcodes.Description(code))
		os.Exit(code)
	}
}

func globalFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:    "config",
			Aliases: []string{"c"},
			Value:   "config.yaml",
			Usage:   "Path to configuration file",
		},
		&cli.StringFlag{
			Name:  "profile",
			Usage: "Profile name stored in SQLite (overrides --config)",
		},
		&cli.StringFlag{
			Name:  "state-file",
			Usage: "Use YAML state file instead of SQLite (for headless runs)",
		},
		&cli.StringFlag{
			Name:  "log-format",
			Value: "text",
			Usage: "Log format: text or json",
		},
		&cli.StringFlag{
			Name:  "verbosity",
			Value: "info",
			Usage: "Log verbosity level (debug, info, warn, error)",
		},
		&cli.DurationFlag{
			Name:  "shutdown-timeout",
			Value: 60 * time.Second,
			Usage: "Graceful shutdown timeout",
		},
	}
}

func applyGlobalFlags(c *cli.Context) error {
	level, err := logging.ParseLevel(c.String("verbosity"))
	if err != nil {
		return err
	}
	logging.SetLevel(level)
	if c.String("log-format") == "json" {
		logging.SetFormat("json")
	}
	return nil
}

func topLevelAction(c *cli.Context) error {
	if c.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "TUI not yet wired (added in phase 5). Run `smt --help` for available commands.")
		return nil
	}
	return cli.ShowAppHelp(c)
}

func commands() []*cli.Command {
	return []*cli.Command{
		createCommand(),
		syncCommand(),
		validateCommand(),
		snapshotCommand(),
		healthCheckCommand(),
		analyzeCommand(),
		initCommand(),
		initSecretsCommand(),
		profileCommand(),
		historyCommand(),
	}
}

func notImplemented(name string) cli.ActionFunc {
	return func(*cli.Context) error {
		return fmt.Errorf("%s: not yet implemented in this build", name)
	}
}
