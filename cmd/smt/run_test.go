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
		apply     bool
		wantError string
	}{
		{
			name:   "ddl only deterministic postgres",
			target: "postgres",
			mode:   driver.SchemaGenerationDeterministic,
		},
		{
			name:      "rejects ai mode",
			target:    "postgres",
			mode:      "ai",
			wantError: "schema_generation.mode: ai is no longer supported",
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
			apply:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Target.Type = tt.target
			cfg.SchemaGeneration.Mode = tt.mode

			err := validateCreateSupport(cfg, tt.apply)
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
