package tui

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"gopkg.in/yaml.v3"
	"smt/internal/checkpoint"
	"smt/internal/config"
	"smt/internal/driver"
	"smt/internal/logging"
	"smt/internal/orchestrator"
	"smt/internal/version"
)

// maxContentLines limits output retained in memory to prevent unbounded growth
// during long migrations or verbose commands. 2000 lines provides sufficient
// scrollback for interactive use while keeping memory bounded.
const maxContentLines = 2000

// safeCmd wraps a tea.Cmd to recover from panics
func safeCmd(cmd tea.Cmd) tea.Cmd {
	if cmd == nil {
		return nil
	}
	return func() (msg tea.Msg) {
		defer func() {
			if r := recover(); r != nil {
				msg = OutputMsg(fmt.Sprintf("\n[ERROR] %v\n", r))
			}
		}()
		return cmd()
	}
}

// AppMode represents the current application mode
type AppMode int

const (
	ModeNormal AppMode = iota
	ModeMigration
)

// Model is the main TUI model - simplified single-viewport architecture
type Model struct {
	// Core components
	viewport  viewport.Model
	textInput textinput.Model
	ready     bool
	width     int
	height    int

	// Git integration
	gitInfo GitInfo
	cwd     string

	// Single content buffer with memory management
	content      *strings.Builder
	lineBuffer   string
	progressLine string

	// History & completion
	history       []string
	historyIdx    int
	suggestions   []string
	suggestionIdx int
	lastInput     string

	// Application mode
	mode AppMode

	// Single migration state (one at a time)
	migrationCancel context.CancelFunc
	migrationStatus string // "", "running", "completed", "failed", "cancelled"
}

type commandInfo struct {
	Name        string
	Description string
}

var availableCommands = []commandInfo{
	{"/create", "Build the target schema from the source"},
	{"/sync", "Diff source vs snapshot and emit/apply ALTERs"},
	{"/snapshot", "Capture current source schema for future diffing"},
	{"/config", "Show configuration details"},
	{"/history", "Show migration history"},
	{"/logs", "Save session logs to file"},
	{"/profile", "Manage encrypted profiles (save/list/delete/export)"},
	{"/verbosity", "Set log level (debug, info, warn, error)"},
	{"/about", "Show application information"},
	{"/help", "Show available commands"},
	{"/clear", "Clear screen"},
	{"/quit", "Exit application"},
}

// Message types

// TickMsg is used to update the UI periodically
type TickMsg time.Time

// OutputMsg is sent when new output is captured
type OutputMsg string

// BoxedOutputMsg is output that should be displayed in a bordered box
type BoxedOutputMsg string

// ProgressMsg updates the progress line
type ProgressMsg string

// MigrationDoneMsg signals migration completion
type MigrationDoneMsg struct {
	Status  string // "completed", "failed", "cancelled"
	Message string
}

// migrationStartedMsg carries the cancel function
type migrationStartedMsg struct {
	cancel context.CancelFunc
}

// Init initializes the model
func (m Model) Init() tea.Cmd {
	return tea.Batch(
		textinput.Blink,
		tickCmd(),
	)
}

func tickCmd() tea.Cmd {
	return tea.Tick(time.Second*5, func(t time.Time) tea.Msg {
		return TickMsg(t)
	})
}

// InitialModel returns the initial model state
func InitialModel() Model {
	ti := textinput.New()
	ti.Placeholder = "Type your message or @path/to/file"
	ti.Focus()
	ti.CharLimit = 156
	ti.Width = 20
	ti.Prompt = "❯ "
	ti.PromptStyle = stylePrompt

	cwd, _ := os.Getwd()

	return Model{
		textInput:  ti,
		gitInfo:    GetGitInfo(),
		cwd:        cwd,
		content:    &strings.Builder{},
		history:    []string{},
		historyIdx: -1,
		mode:       ModeNormal,
	}
}

// appendOutput adds text to the content buffer with memory management
func (m *Model) appendOutput(text string) {
	m.content.WriteString(text)

	// Trim to last N lines if exceeded
	content := m.content.String()
	lines := strings.Split(content, "\n")
	if len(lines) > maxContentLines {
		lines = lines[len(lines)-maxContentLines:]
		m.content.Reset()
		m.content.WriteString(strings.Join(lines, "\n"))
	}

	// Update viewport - only auto-scroll if at bottom
	wasAtBottom := m.viewport.AtBottom()
	m.viewport.SetContent(m.getDisplayContent())
	if wasAtBottom {
		m.viewport.GotoBottom()
	}
}

// getDisplayContent returns content with progress line appended if present
func (m *Model) getDisplayContent() string {
	content := m.content.String()
	if m.progressLine != "" {
		content += styleSystemOutput.Render("  "+m.progressLine) + "\n"
	}
	return content
}

// Update handles messages and updates the model
func (m Model) Update(msg tea.Msg) (model tea.Model, cmd tea.Cmd) {
	// Recover from panics in Update
	defer func() {
		if r := recover(); r != nil {
			m.appendOutput(fmt.Sprintf("\n[ERROR] %v\n", r))
			model = m
			cmd = nil
		}
	}()

	var (
		tiCmd tea.Cmd
		vpCmd tea.Cmd
	)

	switch msg := msg.(type) {
	case tea.KeyMsg:
		// Handle suggestion navigation if active
		if len(m.suggestions) > 0 {
			switch msg.Type {
			case tea.KeyUp:
				m.suggestionIdx--
				if m.suggestionIdx < 0 {
					m.suggestionIdx = len(m.suggestions) - 1
				}
				return m, nil
			case tea.KeyDown:
				m.suggestionIdx++
				if m.suggestionIdx >= len(m.suggestions) {
					m.suggestionIdx = 0
				}
				return m, nil
			case tea.KeyEnter, tea.KeyTab:
				if m.suggestionIdx >= 0 && m.suggestionIdx < len(m.suggestions) {
					selection := m.suggestions[m.suggestionIdx]
					completion := strings.Fields(selection)[0]

					input := m.textInput.Value()

					// File completion (@)
					if idx := strings.LastIndex(input, "@"); idx != -1 && (idx == 0 || input[idx-1] == ' ') {
						newValue := input[:idx+1] + completion
						if newValue == input && msg.Type == tea.KeyEnter {
							m.suggestions = nil
							break
						}
						m.textInput.SetValue(newValue)
						m.textInput.SetCursor(len(newValue))
					} else if strings.HasPrefix(input, "/") {
						// Command completion
						newValue := completion
						if newValue == input && msg.Type == tea.KeyEnter {
							m.suggestions = nil
							break
						}
						m.textInput.SetValue(newValue)
						m.textInput.SetCursor(len(newValue))
					}

					m.suggestions = nil
					m.suggestionIdx = 0
					return m, nil
				}
			case tea.KeyEsc:
				m.suggestions = nil
				return m, nil
			}
		}

		switch msg.Type {
		case tea.KeyCtrlC:
			// Cancel running migration
			if m.migrationStatus == "running" {
				if m.migrationCancel != nil {
					m.migrationCancel()
					m.appendOutput(styleSystemOutput.Render("Cancelling migration... please wait") + "\n")
				} else {
					m.appendOutput(styleSystemOutput.Render("Schema operation is starting; cancel will be available shortly.") + "\n")
				}
				return m, nil
			}
			// Quit if nothing running
			return m, tea.Quit

		case tea.KeyEsc:
			return m, tea.Quit

		case tea.KeyEnter:
			value := m.textInput.Value()
			if value != "" {
				m.appendOutput(styleUserInput.Render("> "+value) + "\n")
				m.textInput.Reset()
				m.history = append(m.history, value)
				m.historyIdx = len(m.history)
				return m, safeCmd(m.handleCommand(value))
			}

		case tea.KeyTab:
			if m.mode == ModeNormal {
				m.autocompleteCommand()
			}

		case tea.KeyPgUp:
			m.viewport.ScrollUp(m.viewport.Height / 2)
			return m, nil

		case tea.KeyPgDown:
			m.viewport.ScrollDown(m.viewport.Height / 2)
			return m, nil

		case tea.KeyHome:
			m.viewport.GotoTop()
			return m, nil

		case tea.KeyEnd:
			m.viewport.GotoBottom()
			return m, nil

		case tea.KeyUp:
			if m.textInput.Value() == "" && len(m.suggestions) == 0 {
				m.viewport.ScrollUp(1)
				return m, nil
			}
			if m.historyIdx > 0 {
				m.historyIdx--
				m.textInput.SetValue(m.history[m.historyIdx])
			}
			return m, nil

		case tea.KeyDown:
			if m.textInput.Value() == "" && len(m.suggestions) == 0 {
				m.viewport.ScrollDown(1)
				return m, nil
			}
			if m.historyIdx < len(m.history)-1 {
				m.historyIdx++
				m.textInput.SetValue(m.history[m.historyIdx])
			} else {
				m.historyIdx = len(m.history)
				m.textInput.Reset()
			}
			return m, nil
		}

	case tea.WindowSizeMsg:
		headerHeight := 0
		footerHeight := 7 // Input box (3) + Status bar (1) + Separator (1) + Suggestions (1) + Safety (1)
		verticalMarginHeight := headerHeight + footerHeight

		if !m.ready {
			m.viewport = viewport.New(msg.Width-2, msg.Height-verticalMarginHeight)
			m.viewport.YPosition = headerHeight
			m.content.WriteString(m.welcomeMessage())
			m.viewport.SetContent(m.content.String())
			m.ready = true
		} else {
			m.viewport.Width = msg.Width - 2
			m.viewport.Height = msg.Height - verticalMarginHeight
		}
		m.width = msg.Width
		m.height = msg.Height
		m.textInput.Width = msg.Width - 4

	case migrationStartedMsg:
		m.migrationCancel = msg.cancel
		m.migrationStatus = "running"
		m.mode = ModeMigration

	case MigrationDoneMsg:
		m.migrationStatus = msg.Status
		m.migrationCancel = nil
		m.progressLine = ""
		m.mode = ModeNormal

		prefix := styleSuccess.Render("✔ ")
		if msg.Status == "failed" || msg.Status == "cancelled" {
			prefix = styleError.Render("✖ ")
		}
		m.appendOutput(prefix + msg.Message + "\n")

	case BoxedOutputMsg:
		output := strings.TrimSpace(string(msg))
		if output == "" {
			break
		}

		boxWidth := m.viewport.Width - 4
		if boxWidth < 40 {
			boxWidth = 80
		}

		boxedOutput := styleOutputBox.Width(boxWidth).Render(output)
		m.appendOutput(boxedOutput + "\n")

	case OutputMsg:
		m.lineBuffer += string(msg)

		wrapWidth := m.viewport.Width - 4
		if wrapWidth < 20 {
			wrapWidth = 80
		}

		// Process complete lines
		for {
			newlineIdx := strings.Index(m.lineBuffer, "\n")
			if newlineIdx == -1 {
				break
			}

			m.progressLine = ""
			line := m.lineBuffer[:newlineIdx]
			m.lineBuffer = m.lineBuffer[newlineIdx+1:]

			// Handle carriage returns
			if lastCR := strings.LastIndex(line, "\r"); lastCR != -1 {
				line = line[lastCR+1:]
			}

			// Wrap and style
			wrappedLines := strings.Split(wrapLine(line, wrapWidth), "\n")
			for _, wrappedLine := range wrappedLines {
				lowerText := strings.ToLower(line)
				prefix := "  "

				isError := strings.Contains(lowerText, "error") ||
					(strings.Contains(lowerText, "fail") && !strings.Contains(lowerText, "0 failed"))

				if isError {
					wrappedLine = styleError.Render(wrappedLine)
					prefix = styleError.Render("✖ ")
				} else if strings.Contains(lowerText, "success") || strings.Contains(lowerText, "passed") || strings.Contains(lowerText, "complete") {
					wrappedLine = styleSuccess.Render(wrappedLine)
					prefix = styleSuccess.Render("✔ ")
				} else {
					wrappedLine = styleSystemOutput.Render(wrappedLine)
				}

				m.appendOutput(prefix + wrappedLine + "\n")
			}
		}

		// Handle progress bar updates (lines with \r but no \n)
		if strings.Contains(m.lineBuffer, "\r") {
			if lastCR := strings.LastIndex(m.lineBuffer, "\r"); lastCR != -1 {
				m.progressLine = strings.TrimSpace(m.lineBuffer[lastCR+1:])
				m.lineBuffer = m.lineBuffer[:lastCR+1]
			}
		}

		// Update viewport
		wasAtBottom := m.viewport.AtBottom()
		m.viewport.SetContent(m.getDisplayContent())
		if wasAtBottom {
			m.viewport.GotoBottom()
		}

	case TickMsg:
		m.gitInfo = GetGitInfo()
		return m, tickCmd()
	}

	m.textInput, tiCmd = m.textInput.Update(msg)

	// Handle auto-completion suggestions
	input := m.textInput.Value()
	if input != m.lastInput {
		m.lastInput = input
		m.suggestions = nil

		// File completion (@)
		if idx := strings.LastIndex(input, "@"); idx != -1 {
			if idx == 0 || input[idx-1] == ' ' {
				prefix := input[idx+1:]
				matches, err := filepath.Glob(prefix + "*")
				if err == nil {
					if len(matches) > 15 {
						matches = matches[:15]
					}
					m.suggestions = matches
					m.suggestionIdx = 0
				}
			}
		}

		// Command completion (/)
		if len(m.suggestions) == 0 && strings.HasPrefix(input, "/") {
			for _, cmd := range availableCommands {
				if strings.HasPrefix(cmd.Name, input) {
					m.suggestions = append(m.suggestions, fmt.Sprintf("%-10s %s", cmd.Name, cmd.Description))
				}
			}
			if len(m.suggestions) > 0 {
				m.suggestionIdx = 0
			}
		}
	}

	// Handle viewport updates (but not for arrow keys)
	handleViewport := true
	if key, ok := msg.(tea.KeyMsg); ok {
		if key.Type == tea.KeyUp || key.Type == tea.KeyDown {
			handleViewport = false
		}
	}

	if handleViewport {
		m.viewport, vpCmd = m.viewport.Update(msg)
	}

	return m, tea.Batch(tiCmd, vpCmd)
}

// autocompleteCommand attempts to complete the current input
func (m *Model) autocompleteCommand() {
	input := m.textInput.Value()

	// File completion
	if idx := strings.LastIndex(input, "@"); idx != -1 {
		prefix := input[idx+1:]
		matches, err := filepath.Glob(prefix + "*")
		if err == nil && len(matches) > 0 {
			completion := matches[0]
			newValue := input[:idx+1] + completion
			m.textInput.SetValue(newValue)
			m.textInput.SetCursor(len(newValue))
			m.suggestions = nil
			return
		}
	}

	commands := []string{"/create", "/sync", "/snapshot", "/config", "/history", "/logs", "/profile", "/verbosity", "/about", "/clear", "/quit", "/help"}

	for _, cmd := range commands {
		if strings.HasPrefix(cmd, input) {
			m.textInput.SetValue(cmd)
			m.textInput.SetCursor(len(cmd))
			return
		}
	}
}

// View renders the TUI
func (m Model) View() string {
	if !m.ready {
		return "\n  Initializing..."
	}

	// Suggestions popup
	suggestionsView := ""
	if len(m.suggestions) > 0 {
		var lines []string
		for i, s := range m.suggestions {
			style := lipgloss.NewStyle().Foreground(colorGray).PaddingLeft(2)
			if i == m.suggestionIdx {
				style = lipgloss.NewStyle().
					Foreground(colorWhite).
					Background(colorPurple).
					PaddingLeft(2).
					PaddingRight(2).
					Bold(true)
			}
			lines = append(lines, style.Render(s))
		}
		suggestionsView = strings.Join(lines, "\n") + "\n"
	}

	// Main viewport
	viewportView := styleViewport.Width(m.viewport.Width + 2).Render(m.viewport.View())

	// Progress line (if migration running)
	progressView := ""
	if m.progressLine != "" && m.mode == ModeMigration {
		progressView = styleSystemOutput.Render("  "+m.progressLine) + "\n"
	}

	return fmt.Sprintf("%s%s\n%s\n%s%s",
		viewportView,
		progressView,
		styleInputContainer.Width(m.width-2).Render(m.textInput.View()),
		suggestionsView,
		m.statusBarView(),
	)
}

func (m Model) statusBarView() string {
	w := lipgloss.Width

	dir := styleStatusDir.Render(m.cwd)
	branch := styleStatusBranch.Render(" " + m.gitInfo.Branch)

	// Mode indicator
	modeText := ""
	switch m.mode {
	case ModeMigration:
		modeText = styleStatusText.Render(" [migrating] ")
	}

	status := ""
	if m.gitInfo.Status == "Dirty" {
		status = styleStatusDirty.Render("Uncommitted Changes")
	} else {
		status = styleStatusClean.Render("All Changes Committed")
	}

	usedWidth := w(dir) + w(branch) + w(modeText) + w(status)
	if usedWidth > m.width {
		usedWidth = m.width
	}

	spacerWidth := m.width - usedWidth
	if spacerWidth < 0 {
		spacerWidth = 0
	}
	spacer := styleStatusBar.Width(spacerWidth).Render("")

	return lipgloss.JoinHorizontal(lipgloss.Top,
		dir,
		branch,
		modeText,
		spacer,
		status,
	)
}

func (m Model) welcomeMessage() string {
	logo := fmt.Sprintf(`
   ___  _ __ ___ | |_
  / __|| '_ ' _ \| __|
  \__ \| | | | | | |_
  |___/|_| |_| |_|\__|
  Schema Migration Tool %s
`, version.Version)

	welcome := styleTitle.Render(logo)

	body := `
 Welcome to smt. This tool extracts a source schema and
 creates the matching DDL on a target database.

 Type /help to see available commands.
`

	tips := lipgloss.NewStyle().Foreground(colorGray).Render(`
 Tip: /create runs the full schema build. /snapshot captures
      a source baseline, and /sync emits or applies ALTERs.
      Hold Shift to select text with mouse.`)

	return welcome + body + tips
}

func (m *Model) handleCommand(cmdStr string) tea.Cmd {
	parts := strings.Fields(cmdStr)
	if len(parts) == 0 {
		return nil
	}
	cmd := parts[0]

	// Handle shell commands (starting with !)
	if strings.HasPrefix(cmd, "!") {
		shellCmd := strings.TrimPrefix(cmdStr, "!")
		return m.runShellCmd(shellCmd)
	}

	switch cmd {
	case "/quit", "/exit":
		return tea.Quit

	case "/clear":
		m.content.Reset()
		m.content.WriteString(m.welcomeMessage())
		m.viewport.SetContent(m.content.String())
		return nil

	case "/help":
		help := `Available Commands:
  /create [config_file]   Build the target schema from the source
  /create --profile NAME  Build using a saved profile
  /snapshot [@config]     Capture current source schema for future diffing
  /sync [@config]         Diff source vs snapshot, emit/apply ALTERs
  /config [config_file]   Show configuration details
  /history                Show schema-run history
  /profile save NAME      Save an encrypted profile
  /profile list           List saved profiles
  /profile delete NAME    Delete a saved profile
  /profile export NAME    Export a profile to a config file
  /verbosity [LEVEL]      Set log level (debug, info, warn, error)
  /logs                   Save session logs to a file
  /clear                  Clear screen
  /quit                   Exit application
  !<command>              Run a shell command

Note: You can use @/path/to/file for config files.`
		return func() tea.Msg { return BoxedOutputMsg(help) }

	case "/logs":
		logFile := "session.log"
		err := os.WriteFile(logFile, []byte(m.content.String()), 0644)
		if err != nil {
			return func() tea.Msg { return OutputMsg(fmt.Sprintf("Error saving logs: %v\n", err)) }
		}
		return func() tea.Msg { return OutputMsg(fmt.Sprintf("Logs saved to %s\n", logFile)) }

	case "/verbosity":
		if len(parts) < 2 {
			// Show current level
			currentLevel := logging.GetLevel()
			return func() tea.Msg {
				return OutputMsg(fmt.Sprintf("Current log level: %s\nUsage: /verbosity <debug|info|warn|error>\n", currentLevel))
			}
		}
		levelStr := parts[1]
		level, err := logging.ParseLevel(levelStr)
		if err != nil {
			return func() tea.Msg {
				return OutputMsg(fmt.Sprintf("Invalid log level: %s\nValid levels: debug, info, warn, error\n", levelStr))
			}
		}
		logging.SetLevel(level)
		return func() tea.Msg {
			return OutputMsg(fmt.Sprintf("Log level set to: %s\n", levelStr))
		}

	case "/about":
		about := fmt.Sprintf(`smt v%s

%s

Features:
- Pluggable driver model (PostgreSQL, SQL Server, MySQL)
- AI-assisted review/type mapping (Anthropic / OpenAI / Google / Ollama / LM Studio)
- Encrypted profile storage
- Schema diff + ALTER generation

Built with Go and Bubble Tea.`, version.Version, version.Description)
		return func() tea.Msg { return BoxedOutputMsg(about) }

	case "/create":
		if m.migrationStatus == "running" {
			return func() tea.Msg {
				return OutputMsg("A schema build is already running. Wait for it to complete or press Ctrl+C to cancel.\n")
			}
		}
		configFile, profileName := parseConfigArgs(parts)
		m.markSchemaOperationStarting()
		return m.runMigrationCmd(configFile, profileName)

	case "/sync":
		if m.migrationStatus == "running" {
			return func() tea.Msg {
				return OutputMsg("A schema operation is already running. Wait for it to complete or press Ctrl+C to cancel.\n")
			}
		}
		m.markSchemaOperationStarting()
		return m.runSMTCommandCmd("sync", parts)

	case "/snapshot":
		if m.migrationStatus == "running" {
			return func() tea.Msg {
				return OutputMsg("A schema operation is already running. Wait for it to complete or press Ctrl+C to cancel.\n")
			}
		}
		m.markSchemaOperationStarting()
		return m.runSMTCommandCmd("snapshot", parts)

	case "/history":
		configFile, profileName, runID := parseHistoryArgs(parts)
		return m.runHistoryCmd(configFile, profileName, runID)

	case "/profile":
		return m.handleProfileCommand(parts)

	case "/config":
		configFile, profileName := parseConfigArgs(parts)
		return m.runConfigCmd(configFile, profileName)

	default:
		return func() tea.Msg { return OutputMsg("Unknown command: " + cmd + "\n") }
	}
}

// Migration commands

func (m *Model) markSchemaOperationStarting() {
	m.migrationStatus = "running"
	m.migrationCancel = nil
	m.progressLine = ""
	m.mode = ModeMigration
}

func (m Model) runMigrationCmd(configFile, profileName string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return MigrationDoneMsg{Status: "failed", Message: "Internal error: no program reference"}
		}

		label := configFile
		if profileName != "" {
			label = profileName
		}

		// Load config synchronously to catch errors before spawning goroutine
		cfg, err := loadConfigFromOrigin(configFile, profileName)
		if err != nil {
			return MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Error loading config: %v", err)}
		}

		orch, err := orchestrator.New(cfg)
		if err != nil {
			return MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Error initializing: %v", err)}
		}

		p.Send(OutputMsg(fmt.Sprintf("Starting migration with %s\n", label)))

		go func() {
			// Recover from panics and report as errors
			defer func() {
				if r := recover(); r != nil {
					p.Send(MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Panic: %v", r)})
				}
			}()
			defer orch.Close()

			if profileName != "" {
				orch.SetRunContext(profileName, "")
			} else {
				orch.SetRunContext("", configFile)
			}

			ctx, cancel := context.WithCancel(context.Background())
			p.Send(migrationStartedMsg{cancel: cancel})

			// Redirect output
			r, w, pipeErr := os.Pipe()
			if pipeErr != nil {
				p.Send(MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Error creating pipe: %v", pipeErr)})
				return
			}
			origStdout := os.Stdout
			origStderr := os.Stderr
			os.Stdout = w
			os.Stderr = w
			logging.SetOutput(w)

			done := make(chan struct{})
			go func() {
				defer close(done)
				buf := make([]byte, 1024)
				for {
					n, err := r.Read(buf)
					if n > 0 {
						p.Send(OutputMsg(string(buf[:n])))
					}
					if err != nil {
						break
					}
				}
			}()

			runErr := orch.Run(ctx)

			w.Close()
			os.Stdout = origStdout
			os.Stderr = origStderr
			logging.SetOutput(origStdout)
			<-done

			if runErr != nil {
				if ctx.Err() == context.Canceled {
					p.Send(MigrationDoneMsg{Status: "cancelled", Message: "Migration cancelled"})
				} else {
					p.Send(MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Migration failed: %v", runErr)})
				}
				return
			}
			p.Send(MigrationDoneMsg{Status: "completed", Message: "Migration completed successfully!"})
		}()

		return nil
	}
}

func (m Model) runConfigCmd(configFile, profileName string) tea.Cmd {
	return func() tea.Msg {
		origin := "config: " + configFile
		if profileName != "" {
			origin = "profile: " + profileName
		}

		cfg, err := loadConfigFromOrigin(configFile, profileName)
		if err != nil {
			return OutputMsg(fmt.Sprintf("Error loading %s: %v\n", origin, err))
		}

		return BoxedOutputMsg(cfg.DebugDump())
	}
}

func (m Model) runHistoryCmd(configFile, profileName, runID string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			cfg, err := loadConfigFromOrigin(configFile, profileName)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}
			orch, err := orchestrator.New(cfg)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error: %v\n", err)))
				return
			}
			defer orch.Close()

			var output string
			if runID != "" {
				output, err = CaptureToString(func() error { return orch.ShowRunDetails(runID) })
			} else {
				output, err = CaptureToString(orch.ShowHistory)
			}
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error showing history: %v\n", err)))
				return
			}
			p.Send(BoxedOutputMsg(output))
		}()

		return nil
	}
}

func (m Model) runSMTCommandCmd(commandName string, parts []string) tea.Cmd {
	args := cliBackedCommandArgs(commandName, parts)
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return MigrationDoneMsg{Status: "failed", Message: "Internal error: no program reference"}
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("Panic: %v", r)})
				}
			}()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			p.Send(migrationStartedMsg{cancel: cancel})
			p.Send(OutputMsg(fmt.Sprintf("Running smt %s\n", strings.Join(args, " "))))

			cmd := exec.CommandContext(ctx, os.Args[0], args...)
			output, err := cmd.CombinedOutput()
			text := string(output)
			if strings.TrimSpace(text) != "" {
				p.Send(BoxedOutputMsg(text))
			}

			switch {
			case ctx.Err() == context.Canceled:
				p.Send(MigrationDoneMsg{Status: "cancelled", Message: fmt.Sprintf("%s cancelled", commandName)})
			case err != nil:
				p.Send(MigrationDoneMsg{Status: "failed", Message: fmt.Sprintf("%s failed: %v", commandName, err)})
			default:
				p.Send(MigrationDoneMsg{Status: "completed", Message: fmt.Sprintf("%s completed successfully", commandName)})
			}
		}()

		return nil
	}
}

func (m Model) runShellCmd(shellCmd string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			cmd := exec.Command("sh", "-c", shellCmd)
			output, err := cmd.CombinedOutput()
			if err != nil {
				p.Send(BoxedOutputMsg(fmt.Sprintf("%s\nError: %v", string(output), err)))
				return
			}
			p.Send(BoxedOutputMsg(string(output)))
		}()

		return nil
	}
}

// Profile commands

func (m Model) handleProfileCommand(parts []string) tea.Cmd {
	if len(parts) < 2 {
		return func() tea.Msg { return OutputMsg("Usage: /profile save|list|delete|export\n") }
	}

	action := parts[1]
	switch action {
	case "list":
		return m.profileListCmd()
	case "save":
		name, configFile := parseProfileSaveArgs(parts)
		if name == "" {
			return func() tea.Msg { return OutputMsg("Usage: /profile save NAME [config_file]\n") }
		}
		return m.profileSaveCmd(name, configFile)
	case "delete":
		if len(parts) < 3 {
			return func() tea.Msg { return OutputMsg("Usage: /profile delete NAME\n") }
		}
		return m.profileDeleteCmd(parts[2])
	case "export":
		name, outFile := parseProfileExportArgs(parts)
		if name == "" {
			return func() tea.Msg { return OutputMsg("Usage: /profile export NAME [output_file]\n") }
		}
		return m.profileExportCmd(name, outFile)
	default:
		return func() tea.Msg { return OutputMsg("Unknown profile command: " + action + "\n") }
	}
}

func (m Model) profileSaveCmd(name, configFile string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			cfg, err := config.Load(configFile)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error loading config: %v\n", err)))
				return
			}
			if name == "" {
				if cfg.Profile.Name != "" {
					name = cfg.Profile.Name
				} else {
					base := filepath.Base(configFile)
					name = strings.TrimSuffix(base, filepath.Ext(base))
				}
			}
			payload, err := yaml.Marshal(cfg)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error encoding config: %v\n", err)))
				return
			}

			dataDir, err := config.DefaultDataDir()
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error resolving data dir: %v\n", err)))
				return
			}
			state, err := checkpoint.New(dataDir)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error opening profile store: %v\n", err)))
				return
			}
			defer state.Close()

			if err := state.SaveProfile(name, cfg.Profile.Description, payload); err != nil {
				if strings.Contains(err.Error(), "SMT_MASTER_KEY is not set") {
					p.Send(OutputMsg("Error saving profile: SMT_MASTER_KEY is not set. Start the TUI with the env var set.\n"))
					return
				}
				p.Send(OutputMsg(fmt.Sprintf("Error saving profile: %v\n", err)))
				return
			}
			p.Send(OutputMsg(fmt.Sprintf("Saved profile %q\n", name)))
		}()

		return nil
	}
}

func (m Model) profileListCmd() tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			dataDir, err := config.DefaultDataDir()
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error resolving data dir: %v\n", err)))
				return
			}
			state, err := checkpoint.New(dataDir)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error opening profile store: %v\n", err)))
				return
			}
			defer state.Close()

			profiles, err := state.ListProfiles()
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error listing profiles: %v\n", err)))
				return
			}
			if len(profiles) == 0 {
				p.Send(BoxedOutputMsg("No profiles found"))
				return
			}

			var b strings.Builder
			fmt.Fprintf(&b, "%-20s %-40s %-20s %-20s\n", "Name", "Description", "Created", "Updated")
			for _, prof := range profiles {
				desc := strings.ReplaceAll(strings.TrimSpace(prof.Description), "\n", " ")
				fmt.Fprintf(&b, "%-20s %-40s %-20s %-20s\n",
					prof.Name,
					desc,
					prof.CreatedAt.Format("2006-01-02 15:04:05"),
					prof.UpdatedAt.Format("2006-01-02 15:04:05"))
			}
			p.Send(BoxedOutputMsg(b.String()))
		}()

		return nil
	}
}

func (m Model) profileDeleteCmd(name string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			dataDir, err := config.DefaultDataDir()
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error resolving data dir: %v\n", err)))
				return
			}
			state, err := checkpoint.New(dataDir)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error opening profile store: %v\n", err)))
				return
			}
			defer state.Close()

			if err := state.DeleteProfile(name); err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error deleting profile: %v\n", err)))
				return
			}
			p.Send(OutputMsg(fmt.Sprintf("Deleted profile %q\n", name)))
		}()

		return nil
	}
}

func (m Model) profileExportCmd(name, outFile string) tea.Cmd {
	return func() tea.Msg {
		p := GetProgramRef()
		if p == nil {
			return OutputMsg("Internal error: no program reference\n")
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Send(OutputMsg(fmt.Sprintf("Panic: %v\n", r)))
				}
			}()

			dataDir, err := config.DefaultDataDir()
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error resolving data dir: %v\n", err)))
				return
			}
			state, err := checkpoint.New(dataDir)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error opening profile store: %v\n", err)))
				return
			}
			defer state.Close()

			blob, err := state.GetProfile(name)
			if err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error loading profile: %v\n", err)))
				return
			}
			if err := os.WriteFile(outFile, blob, 0600); err != nil {
				p.Send(OutputMsg(fmt.Sprintf("Error exporting profile: %v\n", err)))
				return
			}
			p.Send(OutputMsg(fmt.Sprintf("Exported profile %q to %s\n", name, outFile)))
		}()

		return nil
	}
}

// Helper functions

func cliBackedCommandArgs(commandName string, parts []string) []string {
	configFile := ""
	profileName := ""
	commandArgs := make([]string, 0, len(parts))

	for i := 1; i < len(parts); i++ {
		arg := parts[i]
		switch {
		case strings.HasPrefix(arg, "@"):
			configFile = arg[1:]
		case arg == "--profile" && i+1 < len(parts):
			profileName = parts[i+1]
			i++
		default:
			commandArgs = append(commandArgs, arg)
		}
	}

	args := make([]string, 0, 1+len(commandArgs)+4)
	if configFile != "" {
		args = append(args, "--config", configFile)
	}
	if profileName != "" {
		args = append(args, "--profile", profileName)
	}
	args = append(args, commandName)
	args = append(args, commandArgs...)
	return args
}

func parseConfigArgs(parts []string) (string, string) {
	configFile := "config.yaml"
	profileName := ""

	for i := 1; i < len(parts); i++ {
		arg := parts[i]
		if arg == "--profile" && i+1 < len(parts) {
			profileName = parts[i+1]
			i++
			continue
		}
		if strings.HasPrefix(arg, "@") {
			configFile = arg[1:]
		} else {
			configFile = arg
		}
	}

	return configFile, profileName
}

func parseHistoryArgs(parts []string) (string, string, string) {
	configFile := "config.yaml"
	profileName := ""
	runID := ""

	for i := 1; i < len(parts); i++ {
		arg := parts[i]
		switch arg {
		case "--run":
			if i+1 < len(parts) {
				runID = parts[i+1]
				i++
			}
		case "--profile":
			if i+1 < len(parts) {
				profileName = parts[i+1]
				i++
			}
		default:
			if strings.HasPrefix(arg, "@") {
				configFile = arg[1:]
			} else {
				configFile = arg
			}
		}
	}

	return configFile, profileName, runID
}

func parseProfileSaveArgs(parts []string) (string, string) {
	if len(parts) < 3 {
		return "", "config.yaml"
	}

	name := ""
	configFile := "config.yaml"

	if strings.HasPrefix(parts[2], "@") {
		configFile = parts[2][1:]
	} else {
		name = parts[2]
	}

	if len(parts) > 3 {
		if strings.HasPrefix(parts[3], "@") {
			configFile = parts[3][1:]
		} else {
			configFile = parts[3]
		}
	}

	return name, configFile
}

func parseProfileExportArgs(parts []string) (string, string) {
	if len(parts) < 3 {
		return "", "config.yaml"
	}
	name := parts[2]
	outFile := "config.yaml"
	if len(parts) > 3 {
		if strings.HasPrefix(parts[3], "@") {
			outFile = parts[3][1:]
		} else {
			outFile = parts[3]
		}
	}
	return name, outFile
}

func loadConfigFromOrigin(configFile, profileName string) (*config.Config, error) {
	if profileName != "" {
		return loadProfileConfig(profileName)
	}
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file not found: %s", configFile)
	}
	return config.Load(configFile)
}

func loadProfileConfig(name string) (*config.Config, error) {
	dataDir, err := config.DefaultDataDir()
	if err != nil {
		return nil, err
	}
	state, err := checkpoint.New(dataDir)
	if err != nil {
		return nil, err
	}
	defer state.Close()

	blob, err := state.GetProfile(name)
	if err != nil {
		return nil, err
	}
	return config.LoadBytes(blob)
}

// wrapLine wraps text to fit within width, preserving word boundaries where
// possible. Words longer than width are split at the boundary. Whitespace
// is preserved as separate tokens to maintain formatting.
func wrapLine(line string, width int) string {
	if width <= 0 || len(line) <= width {
		return line
	}

	var result strings.Builder
	currentLine := ""

	words := splitIntoWords(line)
	for _, word := range words {
		if len(currentLine)+len(word) > width {
			if currentLine != "" {
				result.WriteString(currentLine)
				result.WriteString("\n")
			}
			for len(word) > width {
				result.WriteString(word[:width])
				result.WriteString("\n")
				word = word[width:]
			}
			currentLine = word
		} else {
			currentLine += word
		}
	}

	if currentLine != "" {
		result.WriteString(currentLine)
	}

	return result.String()
}

func splitIntoWords(s string) []string {
	var words []string
	var current strings.Builder

	for _, r := range s {
		if unicode.IsSpace(r) {
			if current.Len() > 0 {
				words = append(words, current.String())
				current.Reset()
			}
			words = append(words, string(r))
		} else {
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		words = append(words, current.String())
	}

	return words
}

// Start launches the TUI program
func Start() error {
	logging.SetLevel(logging.LevelInfo)

	m := InitialModel()
	p := tea.NewProgram(m, tea.WithAltScreen(), tea.WithMouseCellMotion())

	SetProgramRef(p)

	// Register diagnosis handler to format as BoxedOutputMsg
	driver.SetDiagnosisHandler(func(diagnosis *driver.ErrorDiagnosis) {
		p.Send(BoxedOutputMsg(diagnosis.Format()))
	})
	defer driver.SetDiagnosisHandler(nil) // Cleanup on exit

	cleanup := CaptureOutput(p)
	defer cleanup()

	if _, err := p.Run(); err != nil {
		return err
	}
	return nil
}
