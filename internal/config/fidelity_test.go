package config

import (
	"strings"
	"testing"
)

func TestFidelityProfileOptInConfig(t *testing.T) {
	for _, profile := range []string{"legacy", "checked", "strict", "invalid"} {
		t.Run(profile, func(t *testing.T) {
			cfg, err := LoadBytes([]byte(minimalConfigYAML() + "\nschema_generation:\n  fidelity_profile: " + profile + "\n"))
			if profile == "invalid" {
				if err == nil || !strings.Contains(err.Error(), "fidelity_profile") {
					t.Fatalf("bad profile accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.SchemaGeneration.FidelityProfile != profile {
				t.Fatalf("profile not retained: %+v", cfg.SchemaGeneration)
			}
		})
	}
	cfg, err := LoadBytes([]byte(minimalConfigYAML()))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SchemaGeneration.FidelityProfile != "" {
		t.Fatal("default fidelity profile changed")
	}
}
