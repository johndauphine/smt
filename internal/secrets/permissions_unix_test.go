//go:build unix

package secrets

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadRejectsInsecureSecretsFilePermissions(t *testing.T) {
	tmpDir := t.TempDir()
	secretsFile := filepath.Join(tmpDir, "test-secrets.yaml")
	content := `
encryption:
  master_key: "test-master-key"
`
	if err := os.WriteFile(secretsFile, []byte(content), 0644); err != nil {
		t.Fatalf("write test secrets file: %v", err)
	}

	t.Setenv(SecretsFileEnvVar, secretsFile)
	Reset()
	t.Cleanup(Reset)

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want insecure permissions error")
	}
	if !strings.Contains(err.Error(), "insecure permissions") {
		t.Fatalf("Load() error = %v, want insecure permissions error", err)
	}
}
