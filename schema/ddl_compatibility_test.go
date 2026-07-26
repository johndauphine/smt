package schema_test

import (
	"os/exec"
	"path/filepath"
	"testing"
)

// TestLegacyUnkeyedStructLiteralsCompile pins the public struct layouts that
// existed in v1.3.0. The fixture is an external package so it catches a
// source-incompatible field addition that keyed literals would miss.
func TestLegacyUnkeyedStructLiteralsCompile(t *testing.T) {
	repositoryRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("repository root: %v", err)
	}
	command := exec.Command("go", "test", "./schema/testdata/legacy_struct_literals")
	command.Dir = repositoryRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("legacy public struct-literal fixture failed: %v\n%s", err, output)
	}
}
