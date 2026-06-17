package tui

// In-TUI config wizard. `/init` enters ModeWizard, which takes over key input
// and walks the shared wizard.Steps() one prompt at a time, then writes
// config.yaml. The prompts/defaults/validation come from internal/wizard, the
// same core the `smt init` CLI drives, so the two never diverge.

import (
	"fmt"
	"os"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"smt/internal/wizard"
)

// wizardState holds the in-progress wizard for ModeWizard.
type wizardState struct {
	answers *wizard.Answers
	steps   []wizard.Field
	idx     int // index of the current step in steps
	out     string
}

// startWizard initializes the wizard and renders the first prompt. It mutates
// the model into ModeWizard; subsequent keys are routed to updateWizard.
func (m *Model) startWizard(parts []string) tea.Cmd {
	out := "config.yaml"
	for i := 1; i < len(parts); i++ {
		arg := parts[i]
		switch {
		case strings.HasPrefix(arg, "@"):
			out = arg[1:]
		case arg == "--out" || arg == "-o":
			if i+1 < len(parts) {
				out = parts[i+1]
				i++
			}
		case !strings.HasPrefix(arg, "-"):
			out = arg
		}
	}

	m.suggestions = nil
	m.wiz = &wizardState{
		answers: wizard.NewAnswers(),
		steps:   wizard.Steps(),
		idx:     -1,
		out:     out,
	}
	m.mode = ModeWizard
	m.appendOutput(styleSystemOutput.Render(
		"Config wizard — Enter accepts the (default), Esc cancels.") + "\n")

	m.wizardAdvance()
	if m.wiz.idx >= len(m.wiz.steps) {
		m.exitWizard("Nothing to configure.")
		return nil
	}
	m.renderWizardPrompt()
	return nil
}

// updateWizard handles a key while in ModeWizard.
func (m Model) updateWizard(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC, tea.KeyEsc:
		m.exitWizard("Wizard cancelled. No file written.")
		return m, nil
	case tea.KeyEnter:
		val := m.textInput.Value()
		m.textInput.Reset()
		return m.wizardSubmit(val)
	default:
		var cmd tea.Cmd
		m.textInput, cmd = m.textInput.Update(msg)
		return m, cmd
	}
}

// wizardSubmit applies an answer to the current step, then advances or finishes.
func (m Model) wizardSubmit(val string) (tea.Model, tea.Cmd) {
	f := m.wiz.steps[m.wiz.idx]
	a := m.wiz.answers

	raw := val
	if strings.TrimSpace(raw) == "" {
		raw = f.DefaultValue(a)
	}

	secret := f.Secret != nil && f.Secret(a)
	echo := raw
	if secret {
		echo = "(hidden)"
	}
	m.appendOutput(styleUserInput.Render("> "+echo) + "\n")

	if err := f.Parse(raw, a); err != nil {
		m.appendOutput(styleError.Render("  ✖ "+err.Error()) + "\n")
		m.renderWizardPrompt() // re-prompt the same step
		return m, nil
	}

	m.wizardAdvance()
	if m.wiz.idx >= len(m.wiz.steps) {
		return m.wizardFinish()
	}
	m.renderWizardPrompt()
	return m, nil
}

// wizardAdvance moves idx to the next non-skipped step (or past the end).
func (m *Model) wizardAdvance() {
	m.wiz.idx++
	for m.wiz.idx < len(m.wiz.steps) && m.wiz.steps[m.wiz.idx].IsSkipped(m.wiz.answers) {
		m.wiz.idx++
	}
}

// renderWizardPrompt prints the current step's question and sets the input echo
// mode (masked for secret fields).
func (m *Model) renderWizardPrompt() {
	f := m.wiz.steps[m.wiz.idx]
	a := m.wiz.answers

	if f.Help != "" {
		m.appendOutput(styleSystemOutput.Render("  ("+f.Help+")") + "\n")
	}

	label := f.Prompt(a)
	if f.Options != nil {
		if opts := f.Options(a); len(opts) > 0 {
			label += " [" + strings.Join(opts, "/") + "]"
		}
	}
	secret := f.Secret != nil && f.Secret(a)
	if def := f.DefaultValue(a); def != "" && !secret {
		label += " (" + def + ")"
	}

	if secret {
		m.textInput.EchoMode = textinput.EchoPassword
	} else {
		m.textInput.EchoMode = textinput.EchoNormal
	}
	m.appendOutput(stylePrompt.Render("? ") + styleNormal.Render(label) + "\n")
}

// wizardFinish renders the YAML, writes it (unless the file exists), shows the
// result, and returns to ModeNormal.
func (m Model) wizardFinish() (tea.Model, tea.Cmd) {
	a := m.wiz.answers
	out := m.wiz.out

	data, err := wizard.RenderYAML(a)
	if err != nil {
		m.appendOutput(styleError.Render("  ✖ "+err.Error()) + "\n")
		m.exitWizard("")
		return m, nil
	}
	// A literal password is embedded in the YAML; never echo it into scrollback
	// (which /logs would also export). Show a note instead of the preview.
	secret := hasLiteralPassword(a)

	if _, statErr := os.Stat(out); statErr == nil {
		m.appendOutput(styleSystemOutput.Render(fmt.Sprintf(
			"  %s already exists — not overwriting. Use `smt init --force` to replace it.", out)) + "\n")
		m.exitWizard("")
		cmd := previewOrNote(&m, data, secret)
		return m, cmd
	}

	if err := os.WriteFile(out, data, 0o600); err != nil {
		m.appendOutput(styleError.Render("  ✖ writing "+out+": "+err.Error()) + "\n")
		m.exitWizard("")
		return m, nil
	}
	// WriteFile keeps an existing mode on overwrite; enforce 0600 since the
	// config may carry a literal password.
	_ = os.Chmod(out, 0o600)

	m.appendOutput(styleSuccess.Render("✔ Wrote "+out) + "\n")
	m.appendOutput(styleSystemOutput.Render(
		"  Next: /create writes DDL, /create --apply executes it. /profile save NAME stores it encrypted.") + "\n")
	m.exitWizard("")
	cmd := previewOrNote(&m, data, secret)
	return m, cmd
}

// previewOrNote returns a command that boxes the rendered config, unless it
// holds a literal password — in which case it appends a redaction note (now,
// before the caller's `return m`) and shows nothing.
func previewOrNote(m *Model, data []byte, secret bool) tea.Cmd {
	if secret {
		m.appendOutput(styleSystemOutput.Render(
			"  (config preview hidden — it contains a literal password)") + "\n")
		return nil
	}
	return func() tea.Msg { return BoxedOutputMsg(string(data)) }
}

// exitWizard returns to normal mode and clears wizard state. The input is reset
// before echo is restored so a half-typed masked password can never resurface
// as cleartext in the prompt (e.g. on Esc/Ctrl+C mid-field).
func (m *Model) exitWizard(note string) {
	m.mode = ModeNormal
	m.wiz = nil
	m.textInput.Reset()
	m.textInput.EchoMode = textinput.EchoNormal
	if note != "" {
		m.appendOutput(styleSystemOutput.Render("  "+note) + "\n")
	}
}

// hasLiteralPassword reports whether the rendered config embeds a literal
// password (vs. an ${env:}/${file:} reference). Such configs must not be echoed
// into scrollback or logs.
func hasLiteralPassword(a *wizard.Answers) bool {
	if a.Source.PwMode == wizard.PwLiteral {
		return true
	}
	return a.ConfigureTarget && a.Target.PwMode == wizard.PwLiteral
}
