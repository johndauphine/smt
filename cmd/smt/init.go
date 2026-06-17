package main

// `smt init` is the config wizard: it builds a config.yaml interactively (or
// from flags for scripting/CI) by driving the shared wizard core, so prompts,
// defaults, and validation are defined once in internal/wizard.

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	xterm "github.com/charmbracelet/x/term"
	"github.com/urfave/cli/v2"

	"smt/internal/orchestrator"
	"smt/internal/wizard"
)

func initCommand() *cli.Command {
	flags := []cli.Flag{
		&cli.StringFlag{Name: "out", Aliases: []string{"o"}, Value: "config.yaml", Usage: "Output path"},
		&cli.BoolFlag{Name: "force", Aliases: []string{"f"}, Usage: "Overwrite an existing file"},
		&cli.BoolFlag{Name: "print", Usage: "Write the config to stdout instead of a file"},
		&cli.BoolFlag{Name: "non-interactive", Aliases: []string{"y"}, Usage: "Do not prompt; take values from flags and defaults"},
		&cli.BoolFlag{Name: "health-check", Usage: "Test the connection after writing (implied prompt when interactive)"},
		&cli.BoolFlag{Name: "save-profile", Usage: "Save the result as an encrypted profile (needs SMT_MASTER_KEY)"},
	}
	// One string flag per wizard field, named by its key, so a non-interactive
	// run can supply any answer. yes/no fields accept "yes"/"no".
	for _, f := range wizard.Steps() {
		flags = append(flags, &cli.StringFlag{Name: f.Key, Usage: fieldFlagUsage(f)})
	}
	return &cli.Command{
		Name:      "init",
		Usage:     "Create a config.yaml with a guided wizard",
		ArgsUsage: " ",
		Flags:     flags,
		Action:    runInit,
	}
}

func fieldFlagUsage(f wizard.Field) string {
	a := wizard.NewAnswers()
	u := f.Prompt(a)
	if f.Options != nil {
		if opts := f.Options(a); len(opts) > 0 {
			u += " [" + strings.Join(opts, "|") + "]"
		}
	}
	return u
}

func runInit(c *cli.Context) error {
	out := c.String("out")
	interactive := !c.Bool("non-interactive")

	if !c.Bool("print") && !c.Bool("force") {
		if _, err := os.Stat(out); err == nil {
			return fmt.Errorf("%s already exists (use --force to overwrite, or --print)", out)
		}
	}

	a := wizard.NewAnswers()
	var p *prompter
	if interactive {
		// Prompts go to stderr so stdout carries only the config (keeps
		// `smt init --print > config.yaml` clean).
		p = newPrompter(os.Stdin, os.Stderr)
		fmt.Fprintln(p.out, "smt init — let's build your config.yaml. Press Enter to accept the (default).")
	}

	var section string
	for _, f := range wizard.Steps() {
		if f.IsSkipped(a) {
			// A per-field flag for a disabled section would otherwise be dropped
			// silently, producing a config missing requested settings.
			if !interactive && c.IsSet(f.Key) {
				return fmt.Errorf("--%s was set but its section is off; enable it first with --%s yes",
					f.Key, gateFor(f.Key))
			}
			continue
		}
		if interactive {
			if s := sectionOf(f.Key); s != section {
				section = s
				fmt.Fprintf(p.out, "\n── %s ──\n", section)
			}
			if err := p.ask(f, a); err != nil {
				return err
			}
			continue
		}
		// Non-interactive: flag value if set, else default.
		raw := f.DefaultValue(a)
		if c.IsSet(f.Key) {
			raw = c.String(f.Key)
		}
		if err := f.Parse(raw, a); err != nil {
			return fmt.Errorf("--%s: %w", f.Key, err)
		}
	}

	data, err := wizard.RenderYAML(a)
	if err != nil {
		return err
	}

	if c.Bool("print") {
		_, err := os.Stdout.Write(data)
		return err
	}

	if err := os.WriteFile(out, data, 0o600); err != nil {
		return fmt.Errorf("writing %s: %w", out, err)
	}
	// WriteFile keeps an existing file's mode on overwrite, so a --force rewrite
	// of a 0644 file would stay world-readable. Enforce 0600 explicitly since the
	// config may carry a literal password.
	if err := os.Chmod(out, 0o600); err != nil {
		return fmt.Errorf("securing %s: %w", out, err)
	}
	fmt.Printf("\nWrote %s\n", out)

	// Optional post-steps. When requested via flag they are explicit
	// post-conditions, so their failures propagate; when offered via an
	// interactive prompt they are best-effort.
	if run, strict := postStep(c, p, "health-check", "Test the connection now?"); run {
		if err := initHealthCheck(c, a); err != nil {
			if strict {
				return err
			}
			fmt.Printf("Health check error: %v\n", err)
		}
	}
	if run, strict := postStep(c, p, "save-profile", "Save this as an encrypted profile?"); run {
		if err := initSaveProfile(a, data); err != nil {
			if strict {
				return err
			}
			fmt.Printf("Profile not saved: %v\n", err)
		}
	}
	return nil
}

// postStep decides whether an optional post-step runs and whether its failure
// is fatal. A flag makes it run and strict (explicit post-condition); an
// interactive yes makes it run best-effort. Non-interactive runs never prompt.
func postStep(c *cli.Context, p *prompter, flag, prompt string) (run, strict bool) {
	if c.Bool(flag) {
		return true, true
	}
	if p == nil {
		return false, false
	}
	return p.confirm(prompt, false), false
}

// gateFor returns the enabling flag for a gated per-field flag key.
func gateFor(key string) string {
	switch {
	case strings.HasPrefix(key, "target."):
		return "target.configure"
	case strings.HasPrefix(key, "ai_review."):
		return "ai_review"
	case strings.HasPrefix(key, "migration."):
		return "migration"
	case strings.HasPrefix(key, "slack."):
		return "slack"
	default:
		return key
	}
}

func initHealthCheck(c *cli.Context, a *wizard.Answers) error {
	cfg, err := wizard.Build(a)
	if err != nil {
		return err
	}
	orch, err := orchestrator.NewWithOptions(cfg, orchestrator.Options{
		StateFile:  c.String("state-file"),
		SourceOnly: !cfg.HasTargetConnection(),
	})
	if err != nil {
		return err
	}
	defer orch.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := orch.HealthCheck(ctx)
	if err != nil {
		return err
	}
	printHealth(r)
	if !r.Healthy {
		return fmt.Errorf("health check failed")
	}
	return nil
}

func initSaveProfile(a *wizard.Answers, data []byte) error {
	name := a.ProfileName
	if name == "" {
		name = a.Source.Database
	}
	if name == "" {
		return fmt.Errorf("no profile name (set profile.name)")
	}
	state, err := openProfileStore()
	if err != nil {
		return err
	}
	defer state.Close()
	if err := state.SaveProfile(name, a.ProfileDescription, data); err != nil {
		if strings.Contains(err.Error(), "SMT_MASTER_KEY is not set") {
			return fmt.Errorf("SMT_MASTER_KEY is not set; export it before saving profiles")
		}
		return err
	}
	fmt.Printf("Saved profile %q\n", name)
	return nil
}

// sectionOf maps a field key to a display section header.
func sectionOf(key string) string {
	switch {
	case strings.HasPrefix(key, "source."):
		return "Source database"
	case strings.HasPrefix(key, "target."):
		return "Target database"
	case strings.HasPrefix(key, "ai_review"):
		return "AI review (optional)"
	case strings.HasPrefix(key, "migration"):
		return "Migration overrides (optional)"
	case strings.HasPrefix(key, "slack"):
		return "Slack notifications (optional)"
	case strings.HasPrefix(key, "profile"):
		return "Profile (optional)"
	default:
		return "Schema generation"
	}
}

// prompter reads answers from a reader and writes prompts to a writer.
type prompter struct {
	in  *bufio.Reader
	out io.Writer
	fd  uintptr // stdin fd for TTY detection / no-echo reads
	tty bool
}

func newPrompter(stdin *os.File, out io.Writer) *prompter {
	return &prompter{
		in:  bufio.NewReader(stdin),
		out: out,
		fd:  stdin.Fd(),
		tty: xterm.IsTerminal(stdin.Fd()),
	}
}

// ask prompts for one field, re-prompting until Parse accepts the value.
func (p *prompter) ask(f wizard.Field, a *wizard.Answers) error {
	if f.Help != "" {
		fmt.Fprintf(p.out, "  (%s)\n", f.Help)
	}
	for {
		def := f.DefaultValue(a)
		secret := f.Secret != nil && f.Secret(a)
		label := f.Prompt(a)
		if f.Options != nil {
			if opts := f.Options(a); len(opts) > 0 {
				label += " [" + strings.Join(opts, "/") + "]"
			}
		}
		if def != "" && !secret {
			label += fmt.Sprintf(" (%s)", def)
		}
		fmt.Fprintf(p.out, "%s: ", label)

		raw, err := p.read(secret)
		if err != nil && raw == "" {
			return fmt.Errorf("input closed before %s was answered", f.Key)
		}
		raw = strings.TrimRight(raw, "\r\n")
		if strings.TrimSpace(raw) == "" {
			raw = def
		}
		if perr := f.Parse(raw, a); perr != nil {
			fmt.Fprintf(p.out, "  ! %v\n", perr)
			continue
		}
		return nil
	}
}

// read returns one line of input. Secret fields on a real terminal are read
// without echo; otherwise (piped/non-TTY) a normal buffered line is read.
func (p *prompter) read(secret bool) (string, error) {
	if secret && p.tty {
		b, err := xterm.ReadPassword(p.fd)
		fmt.Fprintln(p.out)
		return string(b), err
	}
	return p.in.ReadString('\n')
}

// confirm asks a yes/no question, returning def on blank input.
func (p *prompter) confirm(prompt string, def bool) bool {
	suffix := " [y/N]"
	if def {
		suffix = " [Y/n]"
	}
	fmt.Fprintf(p.out, "%s%s: ", prompt, suffix)
	raw, _ := p.in.ReadString('\n')
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return def
	case "y", "yes":
		return true
	default:
		return false
	}
}
