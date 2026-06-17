package secrets

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"smt/internal/logging"
)

// TestMigrationDefaultsV1Shape pins the v1 migration_defaults contract: exactly
// the fields SMT consumes, and none of the removed DMT-era keys as struct fields.
func TestMigrationDefaultsV1Shape(t *testing.T) {
	want := []string{
		"create_check_constraints",
		"create_foreign_keys",
		"create_indexes",
		"data_dir",
		"max_source_connections",
		"max_target_connections",
	}
	typ := reflect.TypeOf(MigrationDefaults{})
	var got []string
	for i := 0; i < typ.NumField(); i++ {
		got = append(got, strings.Split(typ.Field(i).Tag.Get("yaml"), ",")[0])
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("migration_defaults shape drifted:\n got  %v\n want %v", got, want)
	}

	// No removed key may sneak back as a struct field.
	gotSet := map[string]bool{}
	for _, k := range got {
		gotSet[k] = true
	}
	for _, k := range legacyMigrationDefaultKeys {
		if gotSet[k] {
			t.Errorf("removed DMT-era key %q is still a MigrationDefaults field", k)
		}
	}
}

// TestLoadWarnsAndIgnoresLegacyKeys: a secrets file carrying removed keys loads
// successfully (supported fields applied), emitting a single warning that names
// the removed keys.
func TestLoadWarnsAndIgnoresLegacyKeys(t *testing.T) {
	tmp := t.TempDir()
	f := filepath.Join(tmp, "secrets.yaml")
	content := `
ai:
  default_provider: anthropic
  providers:
    anthropic:
      api_key: "k"
encryption:
  master_key: "mk"
migration_defaults:
  workers: 8
  max_memory_mb: 4096
  history_retention_days: 30
  ai_adjust: true
  max_source_connections: 5
  create_indexes: false
`
	if err := os.WriteFile(f, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(SecretsFileEnvVar, f)
	Reset()

	var buf bytes.Buffer
	logging.SetOutput(&buf)
	defer logging.SetOutput(nil)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load with legacy keys should not fail: %v", err)
	}

	// Supported fields still apply.
	if cfg.MigrationDefaults.MaxSourceConnections != 5 {
		t.Errorf("max_source_connections = %d, want 5", cfg.MigrationDefaults.MaxSourceConnections)
	}
	if cfg.MigrationDefaults.CreateIndexes == nil || *cfg.MigrationDefaults.CreateIndexes {
		t.Errorf("create_indexes = %v, want explicit false", cfg.MigrationDefaults.CreateIndexes)
	}

	out := buf.String()
	for _, k := range []string{"workers", "max_memory_mb", "history_retention_days", "ai_adjust"} {
		if !strings.Contains(out, k) {
			t.Errorf("warning should name removed key %q; got:\n%s", k, out)
		}
	}
	// Exactly one warning line about migration_defaults.
	if n := strings.Count(out, "removed migration_defaults keys"); n != 1 {
		t.Errorf("expected exactly 1 legacy-keys warning, got %d:\n%s", n, out)
	}
}

// TestNoLegacyKeysNoWarning: a clean v1 file produces no legacy warning.
func TestNoLegacyKeysNoWarning(t *testing.T) {
	tmp := t.TempDir()
	f := filepath.Join(tmp, "secrets.yaml")
	content := `
encryption:
  master_key: "mk"
migration_defaults:
  create_indexes: true
  max_source_connections: 4
`
	if err := os.WriteFile(f, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(SecretsFileEnvVar, f)
	Reset()

	var buf bytes.Buffer
	logging.SetOutput(&buf)
	defer logging.SetOutput(nil)

	if _, err := Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if strings.Contains(buf.String(), "removed migration_defaults keys") {
		t.Errorf("clean v1 file should not warn:\n%s", buf.String())
	}
}

// TestTemplateEmitsOnlyV1MigrationDefaults: the init-secrets template advertises
// supported keys and none of the removed ones.
func TestTemplateEmitsOnlyV1MigrationDefaults(t *testing.T) {
	tpl := GenerateTemplate()
	for _, k := range []string{"create_indexes", "create_foreign_keys", "create_check_constraints"} {
		if !strings.Contains(tpl, k) {
			t.Errorf("template missing supported key %q", k)
		}
	}
	for _, k := range legacyMigrationDefaultKeys {
		if strings.Contains(tpl, k+":") {
			t.Errorf("template still emits removed key %q", k)
		}
	}
}
