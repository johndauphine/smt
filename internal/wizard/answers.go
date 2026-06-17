// Package wizard holds the UI-agnostic core of SMT's config wizard: the
// collected Answers, per-engine defaults, deterministic YAML rendering, and a
// Build helper that validates the rendered config through the real loader.
//
// The CLI (`smt init`) and the TUI both drive this one package, so prompts,
// defaults, and validation are defined exactly once (see steps.go).
package wizard

import (
	"fmt"
	"strings"

	"smt/internal/config"
)

// Engines are the supported database engines, used for both source and target.
var Engines = []string{"mssql", "postgres", "mysql"}

// UnknownTypePolicies are the valid schema_generation.unknown_type_policy values.
var UnknownTypePolicies = []string{"fail", "warn", "text_fallback"}

// AIModes are the valid ai_review.mode values.
var AIModes = []string{"warn", "fail"}

// PasswordMode selects how a password is stored in the generated config.
type PasswordMode string

const (
	// PwEnv stores an ${env:VAR} reference (recommended — no secret in YAML).
	PwEnv PasswordMode = "env"
	// PwFile stores a ${file:/path} reference.
	PwFile PasswordMode = "file"
	// PwLiteral stores the password verbatim in the YAML.
	PwLiteral PasswordMode = "literal"
)

// PasswordModes lists the selectable password modes in display order.
var PasswordModes = []string{string(PwEnv), string(PwFile), string(PwLiteral)}

// Conn holds one side's connection answers (source or target).
type Conn struct {
	Type     string
	Host     string
	Port     int
	Database string
	User     string
	PwMode   PasswordMode
	PwValue  string // env: VAR name; file: path; literal: the password itself
	Schema   string
}

// passwordField renders the YAML value for the password line.
func (c Conn) passwordField() string {
	switch c.PwMode {
	case PwFile:
		if c.PwValue == "" {
			return ""
		}
		return "${file:" + c.PwValue + "}"
	case PwLiteral:
		return c.PwValue
	default: // PwEnv
		if c.PwValue == "" {
			return ""
		}
		return "${env:" + c.PwValue + "}"
	}
}

// Answers is the full set of collected wizard inputs.
type Answers struct {
	Source Conn

	// Target always carries Type + Schema. Connection fields are only collected
	// (and rendered) when ConfigureTarget is true.
	Target          Conn
	ConfigureTarget bool

	UnknownTypePolicy string

	// AI review (optional; block omitted entirely unless AIReview is true).
	AIReview   bool
	AIMode     string
	AIModel    string
	AIDiagnose bool
	AISuggest  bool

	// Migration overrides (optional; block omitted unless MigrationOverrides).
	MigrationOverrides bool
	IncludeTables      []string
	ExcludeTables      []string
	CreateIndexes      bool
	CreateForeignKeys  bool
	CreateChecks       bool

	// Slack (optional; block omitted unless Slack is true).
	Slack           bool
	SlackWebhookVar string
	SlackChannel    string
	SlackUsername   string

	// Profile metadata (optional; block omitted unless ProfileName is set).
	ProfileName        string
	ProfileDescription string
}

// NewAnswers returns Answers preloaded with the wizard's defaults so that a
// fully non-interactive run with no flags still produces a valid config.
func NewAnswers() *Answers {
	return &Answers{
		Source: Conn{Type: "mssql", Host: "localhost", PwMode: PwEnv},
		Target: Conn{Type: "postgres", PwMode: PwEnv},

		UnknownTypePolicy: "fail",

		AIMode:     "warn",
		AIDiagnose: false,
		AISuggest:  false,

		CreateIndexes:     true,
		CreateForeignKeys: true,
		CreateChecks:      true,

		SlackUsername: "smt",
	}
}

// DefaultPort returns the well-known port for an engine, or 0 if unknown.
func DefaultPort(engine string) int {
	switch engine {
	case "mssql":
		return 1433
	case "postgres":
		return 5432
	case "mysql":
		return 3306
	}
	return 0
}

// DefaultSchema returns the conventional schema name for an engine. MySQL has
// no separate schema namespace, so it defaults to the database name.
func DefaultSchema(engine, database string) string {
	switch engine {
	case "mssql":
		return "dbo"
	case "postgres":
		return "public"
	case "mysql":
		return database
	}
	return ""
}

// DefaultPasswordVar returns the conventional env var name for a side's
// password, e.g. "MSSQL_PASSWORD" for an mssql source.
func DefaultPasswordVar(engine string) string {
	e := strings.ToUpper(strings.TrimSpace(engine))
	if e == "" {
		e = "DB"
	}
	return e + "_PASSWORD"
}

// Build renders the answers to YAML and parses them back through the real
// config loader, returning the validated config (or a validation error). The
// rendered bytes and the returned config are guaranteed consistent because the
// config is produced from those exact bytes.
func Build(a *Answers) (*config.Config, error) {
	data, err := RenderYAML(a)
	if err != nil {
		return nil, err
	}
	return config.LoadBytes(data)
}

// RenderYAML produces a clean, commented config.yaml matching the shape of
// config.yaml.example. Optional blocks are emitted only when opted into.
func RenderYAML(a *Answers) ([]byte, error) {
	if err := Validate(a); err != nil {
		return nil, err
	}

	var b strings.Builder
	b.WriteString("# smt — Schema Migration Tool configuration\n")
	b.WriteString("# Generated by `smt init`. Edit freely; see config.yaml.example for all options.\n\n")

	// --- source ---
	b.WriteString("source:\n")
	fmt.Fprintf(&b, "  type: %s\n", a.Source.Type)
	writeConnBody(&b, a.Source)
	if a.Source.Schema != "" {
		fmt.Fprintf(&b, "  schema: %s\n", yamlScalar(a.Source.Schema))
	}
	b.WriteString("\n")

	// --- target ---
	b.WriteString("target:\n")
	fmt.Fprintf(&b, "  type: %s\n", a.Target.Type)
	fmt.Fprintf(&b, "  schema: %s\n", yamlScalar(a.Target.Schema))
	if a.ConfigureTarget {
		writeConnBody(&b, a.Target)
	} else {
		b.WriteString("  # Connection fields are only required for --apply or health-check.\n")
		b.WriteString("  # Re-run `smt init` or add host/port/database/user/password to enable them.\n")
	}
	b.WriteString("\n")

	// --- schema_generation ---
	b.WriteString("schema_generation:\n")
	b.WriteString("  mode: deterministic\n")
	fmt.Fprintf(&b, "  unknown_type_policy: %s\n", a.UnknownTypePolicy)
	b.WriteString("\n")

	// --- ai_review (optional) ---
	if a.AIReview {
		b.WriteString("ai_review:\n")
		b.WriteString("  enabled: true\n")
		fmt.Fprintf(&b, "  mode: %s\n", a.AIMode)
		if a.AIModel != "" {
			fmt.Fprintf(&b, "  model: %s\n", a.AIModel)
		}
		fmt.Fprintf(&b, "  diagnose_failures: %t\n", a.AIDiagnose)
		// suggest_fixes defaults to diagnose_failures in the loader (an opt-out).
		// Only emit it when it differs, so the documented default is preserved.
		if a.AISuggest != a.AIDiagnose {
			fmt.Fprintf(&b, "  suggest_fixes: %t\n", a.AISuggest)
		}
		b.WriteString("\n")
	}

	// --- migration overrides (optional) ---
	if a.MigrationOverrides {
		b.WriteString("migration:\n")
		writeStringList(&b, "include_tables", a.IncludeTables)
		writeStringList(&b, "exclude_tables", a.ExcludeTables)
		fmt.Fprintf(&b, "  create_indexes: %t\n", a.CreateIndexes)
		fmt.Fprintf(&b, "  create_foreign_keys: %t\n", a.CreateForeignKeys)
		fmt.Fprintf(&b, "  create_check_constraints: %t\n", a.CreateChecks)
		b.WriteString("\n")
	}

	// --- slack (optional) ---
	if a.Slack {
		b.WriteString("slack:\n")
		b.WriteString("  enabled: true\n")
		if a.SlackWebhookVar != "" {
			fmt.Fprintf(&b, "  webhook_url: ${env:%s}\n", a.SlackWebhookVar)
		}
		if a.SlackChannel != "" {
			fmt.Fprintf(&b, "  channel: %q\n", a.SlackChannel)
		}
		if a.SlackUsername != "" {
			fmt.Fprintf(&b, "  username: %s\n", yamlScalar(a.SlackUsername))
		}
		b.WriteString("\n")
	}

	// --- profile (optional) ---
	if a.ProfileName != "" {
		b.WriteString("profile:\n")
		fmt.Fprintf(&b, "  name: %s\n", yamlScalar(a.ProfileName))
		if a.ProfileDescription != "" {
			fmt.Fprintf(&b, "  description: %q\n", a.ProfileDescription)
		}
	}

	return []byte(b.String()), nil
}

// writeConnBody writes host/port/database/user/password lines. The schema line
// is written separately by the caller (source after the body, target before it)
// so the two sides can place it consistently without duplicating the key.
func writeConnBody(b *strings.Builder, c Conn) {
	fmt.Fprintf(b, "  host: %s\n", yamlScalar(c.Host))
	fmt.Fprintf(b, "  port: %d\n", c.Port)
	fmt.Fprintf(b, "  database: %s\n", yamlScalar(c.Database))
	fmt.Fprintf(b, "  user: %s\n", yamlScalar(c.User))
	// Literal passwords are always double-quoted: this preserves leading/trailing
	// whitespace and stops YAML from reinterpreting values like null, ~, true, or
	// bare numbers. ${env:}/${file:} references are safe as plain scalars.
	if c.PwMode == PwLiteral {
		fmt.Fprintf(b, "  password: %q\n", c.PwValue)
	} else {
		fmt.Fprintf(b, "  password: %s\n", c.passwordField())
	}
}

// yamlScalar renders a free-form, user-supplied string as a YAML scalar,
// double-quoting it when a plain scalar would be misparsed (special characters,
// surrounding whitespace, or a word YAML reads as a non-string like null/true).
// Clean values (hostnames, schema names) stay unquoted for readability.
func yamlScalar(s string) string {
	if needsYAMLQuote(s) {
		return fmt.Sprintf("%q", s)
	}
	return s
}

func needsYAMLQuote(s string) bool {
	if s == "" {
		return true
	}
	if strings.TrimSpace(s) != s {
		return true
	}
	if strings.ContainsAny(s, ":#{}[],&*?|<>=!%@`\"'\\\n\t") {
		return true
	}
	switch strings.ToLower(s) {
	case "null", "~", "true", "false", "yes", "no", "on", "off":
		return true
	}
	return false
}

func writeStringList(b *strings.Builder, key string, vals []string) {
	if len(vals) == 0 {
		fmt.Fprintf(b, "  %s: []\n", key)
		return
	}
	fmt.Fprintf(b, "  %s:\n", key)
	for _, v := range vals {
		fmt.Fprintf(b, "    - %q\n", v)
	}
}

// Validate checks the assembled answers for internal consistency. It is run by
// RenderYAML and is safe to call directly (flag-driven mode validates up front).
func Validate(a *Answers) error {
	if a == nil {
		return fmt.Errorf("no answers")
	}
	if err := validateEngine("source.type", a.Source.Type); err != nil {
		return err
	}
	if err := validateEngine("target.type", a.Target.Type); err != nil {
		return err
	}
	if a.Source.Database == "" {
		return fmt.Errorf("source.database is required")
	}
	if a.Source.User == "" {
		return fmt.Errorf("source.user is required")
	}
	if a.Source.Port <= 0 {
		return fmt.Errorf("source.port must be positive")
	}
	if a.ConfigureTarget {
		if a.Target.Database == "" {
			return fmt.Errorf("target.database is required when configuring the target connection")
		}
		if a.Target.User == "" {
			return fmt.Errorf("target.user is required when configuring the target connection")
		}
		if a.Target.Port <= 0 {
			return fmt.Errorf("target.port must be positive when configuring the target connection")
		}
	}
	if !contains(UnknownTypePolicies, a.UnknownTypePolicy) {
		return fmt.Errorf("schema_generation.unknown_type_policy %q invalid (want one of %s)",
			a.UnknownTypePolicy, strings.Join(UnknownTypePolicies, ", "))
	}
	if a.AIReview && !contains(AIModes, a.AIMode) {
		return fmt.Errorf("ai_review.mode %q invalid (want one of %s)",
			a.AIMode, strings.Join(AIModes, ", "))
	}
	return nil
}

func validateEngine(field, val string) error {
	if !contains(Engines, val) {
		return fmt.Errorf("%s %q invalid (want one of %s)", field, val, strings.Join(Engines, ", "))
	}
	return nil
}

func contains(set []string, v string) bool {
	for _, s := range set {
		if s == v {
			return true
		}
	}
	return false
}
