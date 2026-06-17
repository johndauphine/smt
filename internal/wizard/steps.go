package wizard

import (
	"fmt"
	"strconv"
	"strings"
)

// Field is one prompt in the wizard. Front-ends (CLI stdin, TUI) iterate
// Steps(), and for each non-skipped field obtain a raw string (from the user or
// a flag), then call Parse to validate-and-apply it into Answers. Defaults,
// option lists, and validation live here once.
type Field struct {
	// Key is a stable identifier, also used as the CLI flag name.
	Key string
	// Prompt returns the question text (may depend on prior answers).
	Prompt func(a *Answers) string
	// Help is an optional one-line hint shown under the prompt.
	Help string
	// Options returns a fixed choice list, or nil for free text.
	Options func(a *Answers) []string
	// Default returns the pre-filled value (may depend on prior answers).
	Default func(a *Answers) string
	// Secret reports whether the input should be masked / not echoed.
	Secret func(a *Answers) bool
	// Skip reports whether this field does not apply given prior answers.
	Skip func(a *Answers) bool
	// Parse validates raw and applies it to a. An error means re-prompt
	// (interactive) or fail (non-interactive).
	Parse func(raw string, a *Answers) error
}

// IsSkipped reports whether the field should be skipped for these answers.
func (f Field) IsSkipped(a *Answers) bool { return f.Skip != nil && f.Skip(a) }

// DefaultValue returns the field's default for these answers ("" if none).
func (f Field) DefaultValue(a *Answers) string {
	if f.Default == nil {
		return ""
	}
	return f.Default(a)
}

// Steps returns the ordered wizard fields. The slice is rebuilt per call so it
// is safe to mutate Answers between steps.
func Steps() []Field {
	return []Field{
		// ---- source ----
		choice("source.type", "Source database engine", func(a *Answers) []string { return Engines },
			func(a *Answers) string { return a.Source.Type },
			func(v string, a *Answers) { a.Source.Type = v }),
		text("source.host", "Source host", func(a *Answers) string { return orDefault(a.Source.Host, "localhost") },
			func(v string, a *Answers) { a.Source.Host = v }, false),
		port("source.port", "Source port", func(a *Answers) *Conn { return &a.Source }),
		required("source.database", "Source database name", nil,
			func(a *Answers) string { return a.Source.Database },
			func(v string, a *Answers) { a.Source.Database = v }),
		required("source.user", "Source user", nil,
			func(a *Answers) string { return a.Source.User },
			func(v string, a *Answers) { a.Source.User = v }),
		pwModeField("source.password_mode", "Source password storage", func(a *Answers) *Conn { return &a.Source }),
		pwValueField("source.password", func(a *Answers) *Conn { return &a.Source }, false),
		text("source.schema", "Source schema", func(a *Answers) string {
			return orDefault(a.Source.Schema, DefaultSchema(a.Source.Type, a.Source.Database))
		}, func(v string, a *Answers) { a.Source.Schema = v }, false),

		// ---- target ----
		choice("target.type", "Target database engine", func(a *Answers) []string { return Engines },
			func(a *Answers) string { return a.Target.Type },
			func(v string, a *Answers) { a.Target.Type = v }),
		text("target.schema", "Target schema", func(a *Answers) string {
			return orDefault(a.Target.Schema, DefaultSchema(a.Target.Type, a.Target.Database))
		}, func(v string, a *Answers) { a.Target.Schema = v }, false),
		yesNo("target.configure", "Configure the target connection now? (needed for --apply / health-check)",
			func(a *Answers) bool { return a.ConfigureTarget },
			func(v bool, a *Answers) { a.ConfigureTarget = v }),

		// target connection (only when configuring)
		skipUnlessTarget(text("target.host", "Target host",
			func(a *Answers) string { return orDefault(a.Target.Host, "localhost") },
			func(v string, a *Answers) { a.Target.Host = v }, false)),
		skipUnlessTarget(port("target.port", "Target port", func(a *Answers) *Conn { return &a.Target })),
		skipUnlessTarget(required("target.database", "Target database name", nil,
			func(a *Answers) string { return a.Target.Database },
			func(v string, a *Answers) { a.Target.Database = v })),
		skipUnlessTarget(required("target.user", "Target user", nil,
			func(a *Answers) string { return a.Target.User },
			func(v string, a *Answers) { a.Target.User = v })),
		skipUnlessTarget(pwModeField("target.password_mode", "Target password storage", func(a *Answers) *Conn { return &a.Target })),
		skipUnlessTarget(pwValueField("target.password", func(a *Answers) *Conn { return &a.Target }, false)),

		// ---- schema generation ----
		choice("unknown_type_policy", "Unknown source type policy",
			func(a *Answers) []string { return UnknownTypePolicies },
			func(a *Answers) string { return orDefault(a.UnknownTypePolicy, "fail") },
			func(v string, a *Answers) { a.UnknownTypePolicy = v }),

		// ---- ai review (optional) ----
		yesNo("ai_review", "Enable optional AI review of generated DDL?",
			func(a *Answers) bool { return a.AIReview },
			func(v bool, a *Answers) { a.AIReview = v }),
		skipUnlessAI(choice("ai_review.mode", "AI review mode",
			func(a *Answers) []string { return AIModes },
			func(a *Answers) string { return orDefault(a.AIMode, "warn") },
			func(v string, a *Answers) { a.AIMode = v })),
		skipUnlessAI(text("ai_review.model", "AI reviewer provider (secrets file entry; blank = default)",
			func(a *Answers) string { return a.AIModel },
			func(v string, a *Answers) { a.AIModel = v }, true)),
		skipUnlessAI(yesNo("ai_review.diagnose_failures", "Let AI advise on extract/render failures?",
			func(a *Answers) bool { return a.AIDiagnose },
			func(v bool, a *Answers) { a.AIDiagnose = v })),
		// suggest_fixes defaults to diagnose_failures (a loader opt-out), so the
		// prompt default tracks AIDiagnose rather than the zero value.
		skipUnlessAI(yesNo("ai_review.suggest_fixes", "Let AI suggest fixes (written to schema.suggested.sql; never applied)?",
			func(a *Answers) bool { return a.AISuggest || a.AIDiagnose },
			func(v bool, a *Answers) { a.AISuggest = v })),

		// ---- migration overrides (optional) ----
		yesNo("migration", "Set migration overrides (table filters / object toggles)?",
			func(a *Answers) bool { return a.MigrationOverrides },
			func(v bool, a *Answers) { a.MigrationOverrides = v }),
		skipUnlessMigration(list("migration.include_tables", "Only include these tables (comma-separated globs; blank = all)",
			func(a *Answers) []string { return a.IncludeTables },
			func(v []string, a *Answers) { a.IncludeTables = v })),
		skipUnlessMigration(list("migration.exclude_tables", "Exclude these tables (comma-separated globs)",
			func(a *Answers) []string { return a.ExcludeTables },
			func(v []string, a *Answers) { a.ExcludeTables = v })),
		skipUnlessMigration(yesNo("migration.create_indexes", "Create non-PK indexes?",
			func(a *Answers) bool { return a.CreateIndexes },
			func(v bool, a *Answers) { a.CreateIndexes = v })),
		skipUnlessMigration(yesNo("migration.create_foreign_keys", "Create foreign keys?",
			func(a *Answers) bool { return a.CreateForeignKeys },
			func(v bool, a *Answers) { a.CreateForeignKeys = v })),
		skipUnlessMigration(yesNo("migration.create_check_constraints", "Create CHECK constraints?",
			func(a *Answers) bool { return a.CreateChecks },
			func(v bool, a *Answers) { a.CreateChecks = v })),

		// ---- slack (optional) ----
		yesNo("slack", "Enable Slack notifications?",
			func(a *Answers) bool { return a.Slack },
			func(v bool, a *Answers) { a.Slack = v }),
		skipUnlessSlack(text("slack.webhook_var", "Env var holding the Slack webhook URL",
			func(a *Answers) string { return orDefault(a.SlackWebhookVar, "SLACK_WEBHOOK_URL") },
			func(v string, a *Answers) { a.SlackWebhookVar = v }, false)),
		skipUnlessSlack(text("slack.channel", "Slack channel",
			func(a *Answers) string { return a.SlackChannel },
			func(v string, a *Answers) { a.SlackChannel = v }, false)),
		skipUnlessSlack(text("slack.username", "Slack username",
			func(a *Answers) string { return orDefault(a.SlackUsername, "smt") },
			func(v string, a *Answers) { a.SlackUsername = v }, false)),

		// ---- profile (optional) ----
		text("profile.name", "Profile name (blank = no profile block)",
			func(a *Answers) string { return a.ProfileName },
			func(v string, a *Answers) { a.ProfileName = v }, false),
		text("profile.description", "Profile description",
			func(a *Answers) string { return a.ProfileDescription },
			func(v string, a *Answers) { a.ProfileDescription = v }, false),
	}
}

// --- field constructors -------------------------------------------------

func text(key, prompt string, def func(a *Answers) string, apply func(string, *Answers), secret bool) Field {
	return Field{
		Key:     key,
		Prompt:  func(*Answers) string { return prompt },
		Default: def,
		Secret:  func(*Answers) bool { return secret },
		Parse: func(raw string, a *Answers) error {
			apply(strings.TrimSpace(raw), a)
			return nil
		},
	}
}

func required(key, prompt string, _ func(a *Answers) []string, def func(a *Answers) string, apply func(string, *Answers)) Field {
	return Field{
		Key:     key,
		Prompt:  func(*Answers) string { return prompt },
		Default: def,
		Parse: func(raw string, a *Answers) error {
			v := strings.TrimSpace(raw)
			if v == "" {
				return fmt.Errorf("%s is required", key)
			}
			apply(v, a)
			return nil
		},
	}
}

func choice(key, prompt string, opts func(a *Answers) []string, def func(a *Answers) string, apply func(string, *Answers)) Field {
	return Field{
		Key:     key,
		Prompt:  func(*Answers) string { return prompt },
		Options: opts,
		Default: def,
		Parse: func(raw string, a *Answers) error {
			v := strings.TrimSpace(raw)
			if v == "" {
				v = def(a)
			}
			if !contains(opts(a), v) {
				return fmt.Errorf("%s %q invalid (want one of %s)", key, v, strings.Join(opts(a), ", "))
			}
			apply(v, a)
			return nil
		},
	}
}

func yesNo(key, prompt string, def func(a *Answers) bool, apply func(bool, *Answers)) Field {
	return Field{
		Key:     key,
		Prompt:  func(*Answers) string { return prompt },
		Options: func(*Answers) []string { return []string{"yes", "no"} },
		Default: func(a *Answers) string {
			if def(a) {
				return "yes"
			}
			return "no"
		},
		Parse: func(raw string, a *Answers) error {
			v := strings.ToLower(strings.TrimSpace(raw))
			switch v {
			case "", "y", "yes", "true":
				if v == "" {
					apply(def(a), a)
					return nil
				}
				apply(true, a)
			case "n", "no", "false":
				apply(false, a)
			default:
				return fmt.Errorf("%s: answer yes or no", key)
			}
			return nil
		},
	}
}

func list(key, prompt string, def func(a *Answers) []string, apply func([]string, *Answers)) Field {
	return Field{
		Key:    key,
		Prompt: func(*Answers) string { return prompt },
		Default: func(a *Answers) string {
			return strings.Join(def(a), ",")
		},
		Parse: func(raw string, a *Answers) error {
			apply(splitList(raw), a)
			return nil
		},
	}
}

func port(key, prompt string, side func(a *Answers) *Conn) Field {
	return Field{
		Key:    key,
		Prompt: func(*Answers) string { return prompt },
		Default: func(a *Answers) string {
			c := side(a)
			p := c.Port
			if p == 0 {
				p = DefaultPort(c.Type)
			}
			if p == 0 {
				return ""
			}
			return strconv.Itoa(p)
		},
		Parse: func(raw string, a *Answers) error {
			c := side(a)
			v := strings.TrimSpace(raw)
			if v == "" {
				c.Port = DefaultPort(c.Type)
				return nil
			}
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				return fmt.Errorf("%s: %q is not a valid port", key, raw)
			}
			c.Port = n
			return nil
		},
	}
}

func pwModeField(key, prompt string, side func(a *Answers) *Conn) Field {
	return Field{
		Key:     key,
		Prompt:  func(*Answers) string { return prompt },
		Options: func(*Answers) []string { return PasswordModes },
		Help:    "env = ${env:VAR} reference (recommended); file = ${file:/path}; literal = stored in YAML",
		Default: func(a *Answers) string {
			m := string(side(a).PwMode)
			return orDefault(m, string(PwEnv))
		},
		Parse: func(raw string, a *Answers) error {
			v := strings.ToLower(strings.TrimSpace(raw))
			if v == "" {
				v = string(PwEnv)
			}
			if !contains(PasswordModes, v) {
				return fmt.Errorf("%s %q invalid (want one of %s)", key, v, strings.Join(PasswordModes, ", "))
			}
			side(a).PwMode = PasswordMode(v)
			return nil
		},
	}
}

func pwValueField(key string, side func(a *Answers) *Conn, _ bool) Field {
	return Field{
		Key: key,
		Prompt: func(a *Answers) string {
			switch side(a).PwMode {
			case PwFile:
				return "Path to the password file"
			case PwLiteral:
				return "Password"
			default:
				return "Env var holding the password"
			}
		},
		Default: func(a *Answers) string {
			c := side(a)
			if c.PwMode == PwEnv {
				return orDefault(c.PwValue, DefaultPasswordVar(c.Type))
			}
			return c.PwValue
		},
		Secret: func(a *Answers) bool { return side(a).PwMode == PwLiteral },
		Parse: func(raw string, a *Answers) error {
			c := side(a)
			// A literal password is kept verbatim (whitespace is significant); env
			// var names and file paths are trimmed. The prompter already strips the
			// trailing newline.
			if c.PwMode == PwLiteral {
				c.PwValue = raw
				return nil
			}
			v := strings.TrimSpace(raw)
			if v == "" && c.PwMode == PwEnv {
				v = DefaultPasswordVar(c.Type)
			}
			c.PwValue = v
			return nil
		},
	}
}

// --- skip wrappers ------------------------------------------------------

func skipUnlessTarget(f Field) Field {
	return withSkip(f, func(a *Answers) bool { return !a.ConfigureTarget })
}
func skipUnlessAI(f Field) Field { return withSkip(f, func(a *Answers) bool { return !a.AIReview }) }
func skipUnlessMigration(f Field) Field {
	return withSkip(f, func(a *Answers) bool { return !a.MigrationOverrides })
}
func skipUnlessSlack(f Field) Field { return withSkip(f, func(a *Answers) bool { return !a.Slack }) }

func withSkip(f Field, skip func(a *Answers) bool) Field {
	f.Skip = skip
	return f
}

// --- helpers ------------------------------------------------------------

func orDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

func splitList(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
