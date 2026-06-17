package tui

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"smt/internal/config"
)

// ready returns an initialized model with a sized viewport so appendOutput is
// exercised the way it is at runtime.
func ready() Model {
	m := InitialModel()
	res, _ := m.Update(tea.WindowSizeMsg{Width: 100, Height: 40})
	return res.(Model)
}

func TestWizard_StartEntersModeWizard(t *testing.T) {
	m := ready()
	m.handleCommand("/init @/tmp/does-not-matter.yaml")

	if m.mode != ModeWizard {
		t.Fatalf("mode = %v, want ModeWizard", m.mode)
	}
	if m.wiz == nil {
		t.Fatal("wiz state is nil after /init")
	}
	if got := m.wiz.steps[m.wiz.idx].Key; got != "source.type" {
		t.Fatalf("first step = %q, want source.type", got)
	}
	if !strings.Contains(m.content.String(), "Source database engine") {
		t.Errorf("first prompt not rendered:\n%s", m.content.String())
	}
}

func TestWizard_EscCancelsWithoutWriting(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "config.yaml")
	m := ready()
	m.handleCommand("/init @" + out)

	res, _ := m.updateWizard(tea.KeyMsg{Type: tea.KeyEsc})
	m = res.(Model)

	if m.mode != ModeNormal {
		t.Fatalf("mode = %v, want ModeNormal after Esc", m.mode)
	}
	if m.wiz != nil {
		t.Error("wiz state should be cleared after cancel")
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Error("cancelled wizard must not write a file")
	}
}

// drive runs the wizard to completion, supplying seed[key] for each step (or
// the step default when absent). It returns the finished model.
func drive(t *testing.T, m Model, seed map[string]string) Model {
	t.Helper()
	for i := 0; m.mode == ModeWizard; i++ {
		if i > len(m.wiz.steps)+8 {
			t.Fatalf("wizard did not terminate (stuck at %q)", m.wiz.steps[m.wiz.idx].Key)
		}
		val := seed[m.wiz.steps[m.wiz.idx].Key]
		res, _ := m.wizardSubmit(val)
		m = res.(Model)
	}
	return m
}

func TestWizard_FullFlowWritesValidConfig(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "config.yaml")

	m := ready()
	m.handleCommand("/init @" + out)
	m = drive(t, m, map[string]string{
		"source.database": "StackOverflow2010",
		"source.user":     "sa",
		"target.type":     "postgres",
	})

	if m.mode != ModeNormal {
		t.Fatalf("mode = %v, want ModeNormal after finish", m.mode)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("config not written: %v", err)
	}
	cfg, err := config.LoadBytes(data)
	if err != nil {
		t.Fatalf("written config does not load: %v\n%s", err, data)
	}
	if cfg.Source.Type != "mssql" || cfg.Target.Type != "postgres" {
		t.Errorf("got source=%q target=%q", cfg.Source.Type, cfg.Target.Type)
	}
	// 0600 enforced.
	info, _ := os.Stat(out)
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("file mode = %o, want 600", perm)
	}
}

func TestWizard_DoesNotOverwriteExisting(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(out, []byte("original: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	m := ready()
	m.handleCommand("/init @" + out)
	m = drive(t, m, map[string]string{
		"source.database": "DB", "source.user": "u", "target.type": "postgres",
	})

	data, _ := os.ReadFile(out)
	if string(data) != "original: true\n" {
		t.Errorf("existing file was overwritten:\n%s", data)
	}
}

// TestWizard_SecretFieldMasksInput: choosing a literal password switches the
// input to masked echo when the password step is rendered.
func TestWizard_SecretFieldMasksInput(t *testing.T) {
	out := filepath.Join(t.TempDir(), "config.yaml")
	m := ready()
	m.handleCommand("/init @" + out)

	masked := false
	for i := 0; m.mode == ModeWizard; i++ {
		if i > len(m.wiz.steps)+8 {
			t.Fatal("wizard did not terminate")
		}
		key := m.wiz.steps[m.wiz.idx].Key
		val := map[string]string{
			"source.database":      "DB",
			"source.user":          "u",
			"source.password_mode": "literal",
			"source.password":      "s3cret",
			"target.type":          "postgres",
		}[key]
		// Observe echo mode at the moment the password step is the active prompt.
		if key == "source.password" && m.textInput.EchoMode == textinput.EchoPassword {
			masked = true
		}
		res, _ := m.wizardSubmit(val)
		m = res.(Model)
	}
	if !masked {
		t.Error("literal password step did not mask input (EchoPassword expected)")
	}
}

// TestWizard_LiteralPasswordNeverInScrollback: a literal password must not land
// in m.content (which /logs exports) — neither echoed during entry nor shown in
// the completion preview.
func TestWizard_LiteralPasswordNeverInScrollback(t *testing.T) {
	const secret = "sup3rS3cretPW"
	out := filepath.Join(t.TempDir(), "config.yaml")
	m := ready()
	m.handleCommand("/init @" + out)
	m = drive(t, m, map[string]string{
		"source.database":      "DB",
		"source.user":          "u",
		"source.password_mode": "literal",
		"source.password":      secret,
		"target.type":          "postgres",
	})

	if strings.Contains(m.content.String(), secret) {
		t.Errorf("literal password leaked into scrollback:\n%s", m.content.String())
	}
	// It must still be in the written file (0600), just not on screen.
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), secret) {
		t.Errorf("password missing from written config:\n%s", data)
	}
}

// TestWizard_CancelAfterTypingClearsInput: pressing Esc with text in the buffer
// resets the input so a half-typed (masked) secret cannot reappear.
func TestWizard_CancelAfterTypingClearsInput(t *testing.T) {
	m := ready()
	m.handleCommand("/init @/tmp/never-written.yaml")
	m.textInput.SetValue("half-typed-secret")

	res, _ := m.updateWizard(tea.KeyMsg{Type: tea.KeyEsc})
	m = res.(Model)

	if m.textInput.Value() != "" {
		t.Errorf("input not cleared on cancel: %q", m.textInput.Value())
	}
	if m.textInput.EchoMode != textinput.EchoNormal {
		t.Error("echo mode not restored to normal on cancel")
	}
}
