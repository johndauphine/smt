package tui

import (
	"fmt"
	"io"
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

// CaptureToString captures stdout from a function and returns it as a string.
// Used for commands like /history that print to stdout.
func CaptureToString(fn func() error) (string, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return "", fmt.Errorf("creating pipe: %w", err)
	}

	origStdout := os.Stdout
	os.Stdout = w

	// Drain the pipe concurrently so fn's writes never block on a full pipe
	// buffer (~64KB on Linux). The old code ran fn to completion with nobody
	// reading, so any output larger than the buffer wedged fn forever (#187).
	type readResult struct {
		data []byte
		err  error
	}
	done := make(chan readResult, 1)
	go func() {
		data, readErr := io.ReadAll(r)
		done <- readResult{data: data, err: readErr}
	}()

	// Restore stdout and close the writer however fn returns — including a
	// panic — so the reader always sees EOF and the os.Stdout swap can't leak.
	fnErr := func() (err error) {
		defer func() {
			w.Close()
			os.Stdout = origStdout
		}()
		return fn()
	}()

	res := <-done
	r.Close()
	if res.err != nil && fnErr == nil {
		fnErr = fmt.Errorf("reading captured output: %w", res.err)
	}
	return string(res.data), fnErr
}
