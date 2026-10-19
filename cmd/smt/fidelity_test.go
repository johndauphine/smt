package main

import (
	"flag"
	"testing"

	"github.com/johndauphine/smt/internal/config"
	"github.com/urfave/cli/v2"
)

func TestFidelityOverrideRequiresExplicitFlag(t *testing.T) {
	for _, value := range []string{"", "legacy", "checked", "strict", "invalid"} {
		t.Run(value, func(t *testing.T) {
			set := flag.NewFlagSet("fidelity", flag.ContinueOnError)
			set.String("fidelity", "", "")
			if value != "" {
				if err := set.Set("fidelity", value); err != nil {
					t.Fatal(err)
				}
			}
			cfg := &config.Config{}
			cfg.SchemaGeneration.FidelityProfile = "checked"
			err := applyFidelityOverride(cli.NewContext(nil, set, nil), cfg)
			if value == "invalid" {
				if err == nil {
					t.Fatal("invalid override accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := value
			if value == "" {
				want = "checked"
			}
			if cfg.SchemaGeneration.FidelityProfile != want {
				t.Fatalf("got %q want %q", cfg.SchemaGeneration.FidelityProfile, want)
			}
		})
	}
}
