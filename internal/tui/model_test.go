package tui

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

// Esc during a running schema operation must cancel it (like Ctrl+C), not
// quit the process out from under the orchestrator. Regression for the
// DMT #558 pattern (Esc quit mid-run without cancel/cleanup).
func TestUpdate_EscCancelsRunningOperationInsteadOfQuitting(t *testing.T) {
	m := InitialModel()
	cancelled := false
	m.migrationStatus = "running"
	m.migrationCancel = func() { cancelled = true }

	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEsc})

	if !cancelled {
		t.Fatal("Esc during a running operation did not invoke migrationCancel")
	}
	if cmd != nil {
		t.Fatal("Esc during a running operation returned a command (expected nil, not tea.Quit)")
	}
}

// Esc while idle should still quit the TUI.
func TestUpdate_EscQuitsWhenIdle(t *testing.T) {
	m := InitialModel()

	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEsc})

	if cmd == nil {
		t.Fatal("Esc while idle should return a quit command")
	}
}

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

// #92 — value-taking global flags must not have their values swallowed as
// the config path, and globals must be placed before the command name.
func TestCLIBackedCommandArgsGlobalValueFlags(t *testing.T) {
	prev := cliFlags
	defer func() { cliFlags = prev }()
	cliFlags = CLIFlagInfo{
		TakesValue: map[string]bool{
			"--config": true, "-c": true, "--profile": true,
			"--state-file": true, "--out": true, "-o": true,
		},
		Global: map[string]bool{
			"--config": true, "-c": true, "--profile": true, "--state-file": true,
		},
	}

	args := cliBackedCommandArgs("create", []string{"/create", "--state-file", "/tmp/state.db", "crm.yaml"})
	want := "--config crm.yaml --state-file /tmp/state.db create"
	if got := strings.Join(args, " "); got != want {
		t.Fatalf("cliBackedCommandArgs() = %q, want %q", got, want)
	}

	args = cliBackedCommandArgs("create", []string{"/create", "--config", "crm.yaml", "--out", "schema.sql"})
	want = "--config crm.yaml create --out schema.sql"
	if got := strings.Join(args, " "); got != want {
		t.Fatalf("cliBackedCommandArgs() = %q, want %q", got, want)
	}

	args = cliBackedCommandArgs("create", []string{"/create", "--out=schema.sql", "crm.yaml"})
	want = "--config crm.yaml create --out=schema.sql"
	if got := strings.Join(args, " "); got != want {
		t.Fatalf("cliBackedCommandArgs() = %q, want %q", got, want)
	}
}
