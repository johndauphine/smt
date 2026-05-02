// Command smt is the schema migration tool. It extracts schemas from a source
// database, generates DDL on a target database, and applies incremental schema
// changes (ALTER TABLE, CREATE INDEX, etc.) detected by diffing against the
// last known source schema snapshot.
//
// SMT is the schema-only counterpart to DMT (the data migration tool); it
// shares DMT's driver model and AI-assisted type mapping but does not move
// rows.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/urfave/cli/v2"

	"smt/internal/exitcodes"
	"smt/internal/version"
)

func main() {
	app := &cli.App{
		Name:    version.Name,
		Usage:   version.Description,
		Version: version.Version,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Value:   "config.yaml",
				Usage:   "Path to configuration file",
			},
			&cli.StringFlag{
				Name:  "profile",
				Usage: "Profile name stored in SQLite",
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
		},
		Action: func(c *cli.Context) error {
			if c.NArg() == 0 {
				fmt.Fprintln(os.Stderr, "TUI not yet wired (added in phase 5). Run `smt --help` for available commands.")
				return nil
			}
			return cli.ShowAppHelp(c)
		},
		Commands: []*cli.Command{
			{
				Name:  "create",
				Usage: "Extract source schema and create matching DDL on the target",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "source-schema", Usage: "Source schema name (defaults from config)"},
					&cli.StringFlag{Name: "target-schema", Usage: "Target schema name (defaults from config)"},
					&cli.BoolFlag{Name: "dry-run", Usage: "Print DDL without executing"},
					&cli.StringFlag{Name: "out", Aliases: []string{"o"}, Usage: "Write DDL to file instead of executing"},
				},
				Action: notImplemented("create"),
			},
			{
				Name:  "sync",
				Usage: "Diff source schema against last snapshot and apply ALTER statements",
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "apply", Usage: "Execute ALTERs against the target (default: emit SQL for review)"},
					&cli.StringFlag{Name: "out", Aliases: []string{"o"}, Value: "migration.sql", Usage: "Output file for generated SQL when not applying"},
				},
				Action: notImplemented("sync"),
			},
			{
				Name:   "validate",
				Usage:  "Compare source vs target schema and report drift",
				Action: notImplemented("validate"),
			},
			{
				Name:   "snapshot",
				Usage:  "Capture the current source schema as a snapshot for future diffing",
				Action: notImplemented("snapshot"),
			},
			{
				Name:   "health-check",
				Usage:  "Test database connections",
				Action: notImplemented("health-check"),
			},
			{
				Name:   "analyze",
				Usage:  "Analyze source schema and suggest configuration",
				Action: notImplemented("analyze"),
			},
			{
				Name:  "init",
				Usage: "Create a new configuration file interactively",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "output", Aliases: []string{"o"}, Value: "config.yaml", Usage: "Output file path"},
					&cli.BoolFlag{Name: "force", Aliases: []string{"f"}, Usage: "Overwrite existing file"},
				},
				Action: notImplemented("init"),
			},
			{
				Name:  "init-secrets",
				Usage: "Create a secrets file for API keys and encryption",
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "force", Aliases: []string{"f"}, Usage: "Overwrite existing secrets file"},
				},
				Action: notImplemented("init-secrets"),
			},
			{
				Name:  "profile",
				Usage: "Manage encrypted profiles stored in SQLite",
				Subcommands: []*cli.Command{
					{Name: "save", Usage: "Save a profile from a config file", Action: notImplemented("profile save")},
					{Name: "list", Usage: "List saved profiles", Action: notImplemented("profile list")},
					{Name: "delete", Usage: "Delete a saved profile", Action: notImplemented("profile delete")},
					{Name: "export", Usage: "Export a profile to a config file", Action: notImplemented("profile export")},
				},
			},
			{
				Name:   "history",
				Usage:  "List previous schema operations",
				Action: notImplemented("history"),
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		code := exitcodes.FromError(err)
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		fmt.Fprintf(os.Stderr, "Exit code %d (%s)\n", code, exitcodes.Description(code))
		os.Exit(code)
	}
}

func notImplemented(name string) cli.ActionFunc {
	return func(*cli.Context) error {
		return fmt.Errorf("%s: not yet implemented in this build", name)
	}
}
