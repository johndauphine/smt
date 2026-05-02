package main

// Encrypted profile management — wraps checkpoint.HistoryBackend so users
// can stash configs (with passwords) under a name in the SQLite state DB
// instead of having plaintext config.yaml files lying around.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"

	"smt/internal/checkpoint"
	"smt/internal/config"
)

func profileCommand() *cli.Command {
	return &cli.Command{
		Name:  "profile",
		Usage: "Manage encrypted profiles stored in SQLite",
		Subcommands: []*cli.Command{
			{Name: "save", Usage: "Save a profile from a config file", Action: profileSave, Flags: []cli.Flag{
				&cli.StringFlag{Name: "name", Aliases: []string{"n"}, Usage: "Profile name (defaults to filename)"},
			}},
			{Name: "list", Usage: "List saved profiles", Action: profileList},
			{Name: "delete", Usage: "Delete a saved profile", Action: profileDelete, Flags: []cli.Flag{
				&cli.StringFlag{Name: "name", Aliases: []string{"n"}, Required: true, Usage: "Profile name"},
			}},
			{Name: "export", Usage: "Export a profile to a config file", Action: profileExport, Flags: []cli.Flag{
				&cli.StringFlag{Name: "name", Aliases: []string{"n"}, Required: true, Usage: "Profile name"},
				&cli.StringFlag{Name: "out", Aliases: []string{"o"}, Value: "config.yaml", Usage: "Output path"},
			}},
		},
	}
}

func profileSave(c *cli.Context) error {
	configPath, _ := configPathOf(c)
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	name := c.String("name")
	if name == "" {
		if cfg.Profile.Name != "" {
			name = cfg.Profile.Name
		} else {
			base := filepath.Base(configPath)
			name = strings.TrimSuffix(base, filepath.Ext(base))
		}
	}
	payload, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}

	state, err := openProfileStore()
	if err != nil {
		return err
	}
	defer state.Close()

	if err := state.SaveProfile(name, cfg.Profile.Description, payload); err != nil {
		if strings.Contains(err.Error(), "SMT_MASTER_KEY is not set") {
			return fmt.Errorf("SMT_MASTER_KEY is not set; export it before saving profiles")
		}
		return err
	}
	fmt.Printf("Saved profile %q\n", name)
	return nil
}

func profileList(c *cli.Context) error {
	state, err := openProfileStore()
	if err != nil {
		return err
	}
	defer state.Close()

	profiles, err := state.ListProfiles()
	if err != nil {
		return err
	}
	if len(profiles) == 0 {
		fmt.Println("No profiles found")
		return nil
	}
	fmt.Printf("%-20s %-40s %-19s %-19s\n", "NAME", "DESCRIPTION", "CREATED", "UPDATED")
	for _, p := range profiles {
		desc := strings.ReplaceAll(strings.TrimSpace(p.Description), "\n", " ")
		fmt.Printf("%-20s %-40s %-19s %-19s\n",
			p.Name,
			desc,
			p.CreatedAt.Format("2006-01-02 15:04:05"),
			p.UpdatedAt.Format("2006-01-02 15:04:05"))
	}
	return nil
}

func profileDelete(c *cli.Context) error {
	state, err := openProfileStore()
	if err != nil {
		return err
	}
	defer state.Close()

	name := c.String("name")
	if err := state.DeleteProfile(name); err != nil {
		return err
	}
	fmt.Printf("Deleted profile %q\n", name)
	return nil
}

func profileExport(c *cli.Context) error {
	state, err := openProfileStore()
	if err != nil {
		return err
	}
	defer state.Close()

	name := c.String("name")
	out := c.String("out")
	blob, err := state.GetProfile(name)
	if err != nil {
		return err
	}
	if err := os.WriteFile(out, blob, 0600); err != nil {
		return err
	}
	fmt.Printf("Exported profile %q to %s\n", name, out)
	return nil
}

func openProfileStore() (checkpoint.HistoryBackend, error) {
	dataDir, err := config.DefaultDataDir()
	if err != nil {
		return nil, err
	}
	return checkpoint.New(dataDir)
}
