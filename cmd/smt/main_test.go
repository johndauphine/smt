package main

import (
	"errors"
	"flag"
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
