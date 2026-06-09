package tui

import (
	"strings"
	"testing"
)

func TestHandleCommand_SchemaOperationMarksRunningSynchronously(t *testing.T) {
	m := InitialModel()

	cmd := m.handleCommand("/sync --apply @config.yaml")
	if cmd == nil {
		t.Fatal("expected command")
	}
	if m.migrationStatus != "running" {
		t.Fatalf("migrationStatus = %q, want running", m.migrationStatus)
	}
	if m.mode != ModeMigration {
		t.Fatalf("mode = %v, want ModeMigration", m.mode)
	}
}

func TestHandleCommand_BlocksSecondSchemaOperationWhileStarting(t *testing.T) {
	m := InitialModel()

	_ = m.handleCommand("/sync @config.yaml")
	cmd := m.handleCommand("/snapshot @config.yaml")
	if cmd == nil {
		t.Fatal("expected blocking command")
	}
	msg := cmd()
	output, ok := msg.(OutputMsg)
	if !ok {
		t.Fatalf("message type = %T, want OutputMsg", msg)
	}
	if !strings.Contains(string(output), "already running") {
		t.Fatalf("output = %q, want already running message", output)
	}
}

func TestCreateCommandUsesCLIBackedPath(t *testing.T) {
	args := cliBackedCommandArgs("create", []string{"/create", "--apply", "@config.yaml", "--out", "schema.sql"})
	got := strings.Join(args, " ")
	want := "--config config.yaml create --apply --out schema.sql"
	if got != want {
		t.Fatalf("cliBackedCommandArgs() = %q, want %q", got, want)
	}
}

func TestCLIBackedCommandArgsAcceptsBareConfigPath(t *testing.T) {
	args := cliBackedCommandArgs("create", []string{"/create", "--apply", "crm.yaml", "--out", "schema.sql"})
	got := strings.Join(args, " ")
	want := "--config crm.yaml create --apply --out schema.sql"
	if got != want {
		t.Fatalf("cliBackedCommandArgs() = %q, want %q", got, want)
	}
}

func TestCLIBackedCommandArgsDoesNotTreatFlagValueAsConfig(t *testing.T) {
	args := cliBackedCommandArgs("create", []string{"/create", "--out", "schema.sql"})
	got := strings.Join(args, " ")
	want := "create --out schema.sql"
	if got != want {
		t.Fatalf("cliBackedCommandArgs() = %q, want %q", got, want)
	}
}

func TestCLIBackedCommandArgsAcceptsBareConfigAfterFlagValue(t *testing.T) {
	args := cliBackedCommandArgs("create", []string{"/create", "--out", "schema.sql", "crm.yaml"})
	got := strings.Join(args, " ")
	want := "--config crm.yaml create --out schema.sql"
	if got != want {
		t.Fatalf("cliBackedCommandArgs() = %q, want %q", got, want)
	}
}
