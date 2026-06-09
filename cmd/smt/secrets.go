package main

// `smt init-secrets` writes a template secrets file (~/.secrets/smt-config.yaml)
// holding the AI provider key, profile encryption master key, and Slack
// webhook URL.

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	"smt/internal/secrets"
)

func initSecretsCommand() *cli.Command {
	return &cli.Command{
		Name:   "init-secrets",
		Usage:  "Create a secrets file for AI keys and profile encryption",
		Action: runInitSecrets,
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "force", Aliases: []string{"f"}, Usage: "Overwrite existing secrets file"},
		},
	}
}

func runInitSecrets(c *cli.Context) error {
	dir, err := secrets.EnsureSecretsDir()
	if err != nil {
		return fmt.Errorf("creating secrets directory: %w", err)
	}
	path := secrets.GetSecretsPath()

	if !c.Bool("force") {
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("secrets file %s already exists (use --force to overwrite)", path)
		}
	}

	if err := os.WriteFile(path, []byte(secrets.GenerateTemplate()), 0600); err != nil {
		return fmt.Errorf("writing secrets file: %w", err)
	}

	fmt.Printf("Secrets file created: %s\n", path)
	fmt.Printf("Directory: %s (permissions: 0700)\n", dir)
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Println("  1. Edit the file to add your AI provider API key")
	fmt.Println("  2. Set encryption.master_key to a value generated with:")
	fmt.Println("     openssl rand -base64 32")
	fmt.Println()
	fmt.Println("Keep this file out of version control.")
	return nil
}
