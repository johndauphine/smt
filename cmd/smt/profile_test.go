package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWritePrivateFileTightensExistingMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "profile.yaml")
	if err := os.WriteFile(path, []byte("old"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatalf("Chmod: %v", err)
	}

	if err := writePrivateFile(path, []byte("new")); err != nil {
		t.Fatalf("writePrivateFile: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) != "new" {
		t.Fatalf("file content = %q, want new", data)
	}
	if info, err := os.Stat(path); err != nil {
		t.Fatalf("Stat: %v", err)
	} else if got := info.Mode().Perm(); got != 0600 {
		t.Fatalf("mode = %o, want 0600", got)
	}
}
