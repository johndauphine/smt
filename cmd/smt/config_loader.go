package main

// Config loading helpers shared by the run/health/history command handlers.
// Either --profile (decrypted from SQLite) or --config (YAML on disk).

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	"smt/internal/checkpoint"
	"smt/internal/config"
)

// loadConfig returns the resolved Config along with the profile name and
// config path used to find it (for run-history attribution). One of the
// two will be empty depending on which source was used.
func loadConfig(c *cli.Context) (*config.Config, string, string, error) {
	if profileName := c.String("profile"); profileName != "" {
		cfg, err := loadProfile(profileName)
		return cfg, profileName, "", err
	}

	configPath, isSet := configPathOf(c)
	if _, err := os.Stat(configPath); os.IsNotExist(err) && !isSet {
		return nil, "", "", fmt.Errorf("configuration file not found: %s", configPath)
	}
	cfg, err := config.Load(configPath)
	return cfg, "", configPath, err
}

// configPathOf walks the cli.Context lineage so `smt -c X create` and
// `smt create -c X` both resolve to X.
func configPathOf(c *cli.Context) (path string, explicit bool) {
	for _, ctx := range c.Lineage() {
		if ctx != nil && ctx.IsSet("config") {
			return ctx.String("config"), true
		}
	}
	return "config.yaml", false
}

func loadProfile(name string) (*config.Config, error) {
	dataDir, err := config.DefaultDataDir()
	if err != nil {
		return nil, err
	}
	state, err := checkpoint.New(dataDir)
	if err != nil {
		return nil, err
	}
	defer state.Close()

	blob, err := state.GetProfile(name)
	if err != nil {
		return nil, err
	}
	return config.LoadBytes(blob)
}
