package main

import (
	"strings"
	"testing"

	"smt/internal/config"
	"smt/internal/driver"
)

func TestValidateCreateSupport(t *testing.T) {
	tests := []struct {
		name      string
		target    string
		mode      string
		wantError string
	}{
		{
			name:   "ddl only deterministic postgres",
			target: "postgres",
			mode:   driver.SchemaGenerationDeterministic,
		},
		{
			name:      "rejects unsupported target",
			target:    "oracle",
			wantError: "unsupported deterministic DDL target",
		},
		{
			name:   "ddl only deterministic mssql",
			target: "mssql",
			mode:   driver.SchemaGenerationDeterministic,
		},
		{
			name:   "apply deterministic mysql",
			target: "mysql",
			mode:   driver.SchemaGenerationDeterministic,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Target.Type = tt.target
			cfg.SchemaGeneration.Mode = tt.mode

			err := validateCreateSupport(cfg)
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("validateCreateSupport() error = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("validateCreateSupport() error = %v, want containing %q", err, tt.wantError)
			}
		})
	}
}

// #92 — the derived grammar must cover the real value-taking globals that
// the old hardcoded TUI list missed.
func TestCLIFlagInfoCoversGlobals(t *testing.T) {
	info := cliFlagInfo()
	for _, name := range []string{"--state-file", "--verbosity", "--log-format", "--shutdown-timeout", "--config", "--profile"} {
		if !info.TakesValue[name] {
			t.Errorf("flag %s not marked as value-taking", name)
		}
		if !info.Global[name] {
			t.Errorf("flag %s not marked global", name)
		}
	}
	for _, name := range []string{"--out", "--source-schema", "--target-schema"} {
		if !info.TakesValue[name] {
			t.Errorf("command flag %s not marked as value-taking", name)
		}
		if info.Global[name] {
			t.Errorf("command flag %s wrongly marked global", name)
		}
	}
	for _, name := range []string{"--apply", "--apply-suggested"} {
		if info.TakesValue[name] {
			t.Errorf("bool flag %s wrongly marked as value-taking", name)
		}
	}
}
