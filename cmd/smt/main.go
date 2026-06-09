// Command smt is the schema migration tool. It extracts schemas from a
// source database, generates matching target-dialect DDL, and optionally
// applies CREATE/ALTER statements against a configured target database.
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
	"smt/internal/tui"
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
		return tui.Start(cliFlagInfo())
	}
	_ = cli.ShowAppHelp(c)
	return exitcodes.NewExitError(fmt.Errorf("unknown command: %s", c.Args().First()), exitcodes.ConfigError)
}

// cliFlagInfo derives the flag grammar (which flags take values, which are
// global) from the real flag definitions, so the TUI's arg splitter cannot
// drift from the CLI (#92).
func cliFlagInfo() tui.CLIFlagInfo {
	info := tui.CLIFlagInfo{TakesValue: map[string]bool{}, Global: map[string]bool{}}
	collect := func(flags []cli.Flag, global bool) {
		for _, f := range flags {
			_, isBool := f.(*cli.BoolFlag)
			for _, name := range f.Names() {
				for _, prefix := range []string{"-", "--"} {
					key := prefix + name
					if !isBool {
						info.TakesValue[key] = true
					}
					if global {
						info.Global[key] = true
					}
				}
			}
		}
	}
	collect(globalFlags(), true)
	for _, cmd := range commands() {
		collect(cmd.Flags, false)
	}
	return info
}

func commands() []*cli.Command {
	return []*cli.Command{
		createCommand(),
		syncCommand(),
		snapshotCommand(),
		healthCheckCommand(),
		initSecretsCommand(),
		profileCommand(),
		historyCommand(),
	}
}
