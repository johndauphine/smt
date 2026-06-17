package wizard

import (
	"strings"
	"testing"

	"smt/internal/config"
)

// fullAnswers returns a maximal set of answers exercising every optional block.
func fullAnswers() *Answers {
	a := NewAnswers()
	a.Source = Conn{Type: "mssql", Host: "localhost", Port: 1433, Database: "StackOverflow2010", User: "sa", PwMode: PwEnv, PwValue: "MSSQL_PASSWORD", Schema: "dbo"}
	a.Target = Conn{Type: "postgres", Host: "localhost", Port: 5432, Database: "so", User: "postgres", PwMode: PwEnv, PwValue: "PG_PASSWORD", Schema: "public"}
	a.ConfigureTarget = true
	a.UnknownTypePolicy = "fail"
	a.AIReview = true
	a.AIMode = "warn"
	a.AIModel = "strong"
	a.AIDiagnose = true
	a.AISuggest = true
	a.MigrationOverrides = true
	a.ExcludeTables = []string{"__*", "temp_*"}
	a.CreateIndexes = true
	a.CreateForeignKeys = true
	a.CreateChecks = true
	a.Slack = true
	a.SlackWebhookVar = "SLACK_WEBHOOK_URL"
	a.SlackChannel = "#data"
	a.SlackUsername = "smt"
	a.ProfileName = "so2010"
	a.ProfileDescription = "StackOverflow 2010"
	return a
}

// minimalAnswers returns the smallest valid set: source + target descriptor only.
func minimalAnswers() *Answers {
	a := NewAnswers()
	a.Source = Conn{Type: "postgres", Host: "localhost", Port: 5432, Database: "app", User: "postgres", PwMode: PwEnv, PwValue: "PG_PASSWORD", Schema: "public"}
	a.Target = Conn{Type: "mysql", Schema: "app"}
	a.UnknownTypePolicy = "fail"
	return a
}

// TestBuildRoundTrips asserts wizard output parses cleanly through the real
// loader — the issue's "round-trips through config.Load without warnings" bar.
func TestBuildRoundTrips(t *testing.T) {
	for name, a := range map[string]*Answers{"full": fullAnswers(), "minimal": minimalAnswers()} {
		t.Run(name, func(t *testing.T) {
			cfg, err := Build(a)
			if err != nil {
				data, _ := RenderYAML(a)
				t.Fatalf("Build: %v\n--- rendered ---\n%s", err, data)
			}
			if cfg.Source.Type != a.Source.Type {
				t.Errorf("source type: got %q want %q", cfg.Source.Type, a.Source.Type)
			}
			if cfg.Target.Type != a.Target.Type {
				t.Errorf("target type: got %q want %q", cfg.Target.Type, a.Target.Type)
			}
		})
	}
}

// TestRenderMatchesLoadBytes proves the rendered file and the validated config
// are the same artifact (Build is derived from RenderYAML).
func TestRenderMatchesLoadBytes(t *testing.T) {
	a := fullAnswers()
	data, err := RenderYAML(a)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadBytes(data)
	if err != nil {
		t.Fatalf("LoadBytes on rendered yaml: %v\n%s", err, data)
	}
	if !cfg.HasTargetConnection() {
		t.Error("full answers should yield a usable target connection")
	}
}

func TestPasswordModes(t *testing.T) {
	cases := []struct {
		mode PasswordMode
		val  string
		want string
	}{
		{PwEnv, "MSSQL_PASSWORD", "${env:MSSQL_PASSWORD}"},
		{PwFile, "/run/secrets/db", "${file:/run/secrets/db}"},
		{PwLiteral, "hunter2", "hunter2"},
	}
	for _, c := range cases {
		got := Conn{PwMode: c.mode, PwValue: c.val}.passwordField()
		if got != c.want {
			t.Errorf("mode %s: got %q want %q", c.mode, got, c.want)
		}
	}
}

// TestOptionalBlocksOmitted asserts opt-out blocks never appear in output.
func TestOptionalBlocksOmitted(t *testing.T) {
	a := minimalAnswers()
	data, err := RenderYAML(a)
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)
	for _, block := range []string{"ai_review:", "migration:", "slack:", "profile:"} {
		if strings.Contains(s, block) {
			t.Errorf("minimal config should omit %q\n%s", block, s)
		}
	}
	// Target connection lines must be absent when not configured.
	if strings.Contains(s, "host: localhost\n  port: 3306") {
		t.Errorf("unconfigured target should not emit connection fields\n%s", s)
	}
}

// TestSuggestFixesOptOut: when suggest tracks diagnose, the key is omitted so
// the loader's opt-out default applies; when they differ, it is emitted.
func TestSuggestFixesOptOut(t *testing.T) {
	a := minimalAnswers()
	a.AIReview, a.AIMode, a.AIDiagnose, a.AISuggest = true, "warn", true, true
	data, err := RenderYAML(a)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "suggest_fixes:") {
		t.Errorf("suggest==diagnose should omit suggest_fixes\n%s", data)
	}
	a.AISuggest = false // diverge from diagnose
	data, _ = RenderYAML(a)
	if !strings.Contains(string(data), "suggest_fixes: false") {
		t.Errorf("suggest!=diagnose should emit suggest_fixes: false\n%s", data)
	}
}

// TestLiteralPasswordFidelity: whitespace and YAML-null-like literals survive
// rendering and the loader round-trip verbatim.
func TestLiteralPasswordFidelity(t *testing.T) {
	for _, pw := range []string{" s p a c e ", "null", "~", "true", "0755", "p@ss:#word"} {
		a := minimalAnswers()
		a.Source.PwMode = PwLiteral
		a.Source.PwValue = pw
		cfg, err := Build(a)
		if err != nil {
			data, _ := RenderYAML(a)
			t.Fatalf("Build with literal %q: %v\n%s", pw, err, data)
		}
		if cfg.Source.Password != pw {
			t.Errorf("literal password %q round-tripped as %q", pw, cfg.Source.Password)
		}
	}
}

// TestFreeFormFieldsRoundTrip: connection fields with YAML-special characters
// survive rendering + the loader verbatim.
func TestFreeFormFieldsRoundTrip(t *testing.T) {
	a := minimalAnswers()
	a.Source.Database = "sales:reporting" // colon
	a.Source.User = "domain\\svc#1"       // backslash + hash
	a.Source.Schema = "no"                // YAML bool-ish
	cfg, err := Build(a)
	if err != nil {
		data, _ := RenderYAML(a)
		t.Fatalf("Build: %v\n%s", err, data)
	}
	if cfg.Source.Database != "sales:reporting" {
		t.Errorf("database round-tripped as %q", cfg.Source.Database)
	}
	if cfg.Source.User != "domain\\svc#1" {
		t.Errorf("user round-tripped as %q", cfg.Source.User)
	}
	if cfg.Source.Schema != "no" {
		t.Errorf("schema round-tripped as %q", cfg.Source.Schema)
	}
}

func TestValidateCatchesBadEngine(t *testing.T) {
	a := minimalAnswers()
	a.Source.Type = "oracle"
	if err := Validate(a); err == nil {
		t.Fatal("expected error for unsupported source engine")
	}
}

func TestValidateRequiresSourceFields(t *testing.T) {
	a := minimalAnswers()
	a.Source.Database = ""
	if err := Validate(a); err == nil {
		t.Fatal("expected error for missing source.database")
	}
}

// TestStepsDriveAnswers simulates a non-interactive run: feed each step its
// default and confirm the assembled answers validate. This is the same path
// both front-ends use, so it guards the shared contract.
func TestStepsDriveAnswers(t *testing.T) {
	a := NewAnswers()
	// Seed the required free-text fields the defaults can't supply.
	seed := map[string]string{
		"source.database": "StackOverflow2010",
		"source.user":     "sa",
		"target.type":     "postgres",
	}
	for _, f := range Steps() {
		if f.IsSkipped(a) {
			continue
		}
		raw, ok := seed[f.Key]
		if !ok {
			raw = f.DefaultValue(a)
		}
		if err := f.Parse(raw, a); err != nil {
			t.Fatalf("step %s parse(%q): %v", f.Key, raw, err)
		}
	}
	if err := Validate(a); err != nil {
		t.Fatalf("assembled answers invalid: %v", err)
	}
	if _, err := Build(a); err != nil {
		t.Fatalf("Build after stepping: %v", err)
	}
}

func TestPortStepDefaultsByEngine(t *testing.T) {
	a := NewAnswers()
	a.Source.Type = "mysql"
	for _, f := range Steps() {
		if f.Key == "source.port" {
			if got := f.DefaultValue(a); got != "3306" {
				t.Fatalf("mysql source port default: got %q want 3306", got)
			}
			return
		}
	}
	t.Fatal("source.port step not found")
}
