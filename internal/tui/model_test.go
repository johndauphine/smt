package tui

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
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
