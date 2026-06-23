package main

import (
	"errors"
	"flag"
	"path/filepath"
	"strings"
	"testing"

	"github.com/urfave/cli/v2"

	"smt/internal/exitcodes"
)

func TestTopLevelAction_UnknownCommandReturnsError(t *testing.T) {
	app := cli.NewApp()
	set := flag.NewFlagSet("test", flag.ContinueOnError)
	if err := set.Parse([]string{"validate"}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	ctx := cli.NewContext(app, set, nil)

	err := topLevelAction(ctx)
	if err == nil {
		t.Fatal("expected unknown command error")
	}
	if !strings.Contains(err.Error(), "unknown command: validate") {
		t.Fatalf("error = %q, want unknown command validate", err.Error())
	}
	var exitErr *exitcodes.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("error type = %T, want *exitcodes.ExitError", err)
	}
	if exitErr.Code != exitcodes.ConfigError {
		t.Fatalf("exit code = %d, want %d", exitErr.Code, exitcodes.ConfigError)
	}
}

func TestDocumentedCommandExitCodes(t *testing.T) {
	missingConfig := filepath.Join(t.TempDir(), "missing.yaml")
	cases := []struct {
		name string
		cmd  *cli.Command
		run  func(*cli.Context) error
		want int
	}{
		{"create missing config", createCommand(), runCreate, exitcodes.IOError},
		{"sync missing config", syncCommand(), runSync, exitcodes.IOError},
		{"health-check missing config", healthCheckCommand(), runHealthCheck, exitcodes.IOError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCommandContext(t, tc.cmd, "--config", missingConfig)
			err := tc.run(ctx)
			if err == nil {
				t.Fatal("command unexpectedly succeeded")
			}
			if got := exitCodeForError(err); got != tc.want {
				t.Fatalf("exitCodeForError(%v) = %d, want %d", err, got, tc.want)
			}
		})
	}

	t.Run("drift detected", func(t *testing.T) {
		err := cli.Exit("", 3)
		if got := exitCodeForError(err); got != 3 {
			t.Fatalf("drift exit code = %d, want 3", got)
		}
	})
}

func TestExperimentalFlagsMarkedInHelp(t *testing.T) {
	var found bool
	for _, fl := range createCommand().Flags {
		for _, name := range fl.Names() {
			if name == "apply-suggested" {
				found = true
				boolFlag, ok := fl.(*cli.BoolFlag)
				if !ok {
					t.Fatalf("--apply-suggested flag type = %T, want *cli.BoolFlag", fl)
				}
				if !strings.Contains(boolFlag.Usage, "[experimental]") {
					t.Fatalf("--apply-suggested help missing [experimental] marker: %s", boolFlag.Usage)
				}
			}
		}
	}
	if !found {
		t.Fatal("create --apply-suggested flag not found")
	}
}

func testCommandContext(t *testing.T, cmd *cli.Command, args ...string) *cli.Context {
	t.Helper()
	app := cli.NewApp()
	set := flag.NewFlagSet("test", flag.ContinueOnError)
	for _, fl := range append(globalFlags(), cmd.Flags...) {
		if err := fl.Apply(set); err != nil {
			t.Fatalf("apply flag %s: %v", fl.Names()[0], err)
		}
	}
	if err := set.Parse(args); err != nil {
		t.Fatalf("parse args: %v", err)
	}
	return cli.NewContext(app, set, nil)
}
