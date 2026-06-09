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
			name:      "ddl only rejects ai mode",
			target:    "postgres",
			mode:      driver.SchemaGenerationAI,
			wantError: "without --apply writes deterministic DDL only",
		},
		{
			name:      "ddl only rejects non postgres target",
			target:    "mssql",
			mode:      driver.SchemaGenerationDeterministic,
			wantError: "postgres targets only",
		},
		{
			name:      "apply deterministic rejects non postgres target",
			target:    "mysql",
			mode:      driver.SchemaGenerationDeterministic,
			apply:     true,
			wantError: "with deterministic DDL currently supports postgres targets only",
		},
		{
			name:   "apply allows legacy ai non postgres target",
			target: "mysql",
			mode:   driver.SchemaGenerationAI,
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
