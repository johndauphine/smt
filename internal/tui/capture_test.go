package tui

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// TestCaptureToString_LargeOutput guards #187: without a concurrent reader,
// output larger than the OS pipe buffer (~64KB) blocks fn's write forever.
// The capture must complete and return the full output.
func TestCaptureToString_LargeOutput(t *testing.T) {
	const lines = 20000 // ~ >64KB of output, well past the pipe buffer
	done := make(chan struct {
		out string
		err error
	}, 1)

	go func() {
		out, err := CaptureToString(func() error {
			for i := 0; i < lines; i++ {
				fmt.Printf("line %06d filler filler filler\n", i)
			}
			return nil
		})
		done <- struct {
			out string
			err error
		}{out, err}
	}()

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("CaptureToString returned error: %v", res.err)
		}
		if got := strings.Count(res.out, "\n"); got != lines {
			t.Fatalf("captured %d lines, want %d (output truncated or wedged)", got, lines)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CaptureToString deadlocked on large output (#187)")
	}
}

// TestCaptureToString_PropagatesError confirms fn's error still surfaces and
// stdout is restored.
func TestCaptureToString_PropagatesError(t *testing.T) {
	orig := os.Stdout
	sentinel := fmt.Errorf("boom")
	out, err := CaptureToString(func() error {
		fmt.Println("some output")
		return sentinel
	})
	if err != sentinel {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if !strings.Contains(out, "some output") {
		t.Fatalf("out = %q, want captured output", out)
	}
	if os.Stdout != orig {
		t.Fatal("os.Stdout was not restored")
	}
}
