package tui

import (
	"os"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"smt/internal/logging"
)

// CaptureOutput pipes stdout and stderr to a channel that feeds the TUI
func CaptureOutput(p *tea.Program) func() {
	r, w, err := os.Pipe()
	if err != nil {
		return func() {}
	}

	origStdout := os.Stdout
	origStderr := os.Stderr

	os.Stdout = w
	os.Stderr = w

	// Redirect logging to the pipe and enable simple mode (no timestamps in TUI)
	logging.SetOutput(w)
	logging.SetSimpleMode(true)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
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

	return func() {
		w.Close()
		os.Stdout = origStdout
		os.Stderr = origStderr
		// Restore logging to stdout and disable simple mode
		logging.SetOutput(origStdout)
		logging.SetSimpleMode(false)
		// Wait a tiny bit to ensure last bytes are read
		time.Sleep(10 * time.Millisecond)
	}
}

// programRef holds a reference to the tea.Program for use by migration commands
var programRef *tea.Program

// SetProgramRef stores the program reference for migration commands
func SetProgramRef(p *tea.Program) {
	programRef = p
}

// GetProgramRef returns the stored program reference
func GetProgramRef() *tea.Program {
	return programRef
}
