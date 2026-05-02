package orchestrator

import (
	"testing"

	"smt/internal/config"
	"smt/internal/source"
)

func tbl(name string) source.Table { return source.Table{Name: name} }

func TestFilterTables_NoConfig(t *testing.T) {
	o := &Orchestrator{config: &config.Config{}}
	in := []source.Table{tbl("Users"), tbl("Posts"), tbl("__schema_versions")}
	out := o.filterTables(in)

	if len(out) != 3 {
		t.Fatalf("expected all 3 tables to pass through, got %d", len(out))
	}
}

func TestFilterTables_ExcludeOnly(t *testing.T) {
	cfg := &config.Config{}
	cfg.Migration.ExcludeTables = []string{"__*", "temp_*"}
	o := &Orchestrator{config: cfg}

	in := []source.Table{tbl("Users"), tbl("__migrations"), tbl("temp_staging"), tbl("Posts")}
	out := o.filterTables(in)

	if len(out) != 2 {
		t.Fatalf("expected 2 tables to remain, got %d", len(out))
	}
	for _, table := range out {
		if table.Name == "__migrations" || table.Name == "temp_staging" {
			t.Errorf("excluded table %s slipped through", table.Name)
		}
	}
}

func TestFilterTables_IncludeOnly(t *testing.T) {
	cfg := &config.Config{}
	cfg.Migration.IncludeTables = []string{"User*", "Post*"}
	o := &Orchestrator{config: cfg}

	in := []source.Table{tbl("Users"), tbl("UserSettings"), tbl("Posts"), tbl("Comments")}
	out := o.filterTables(in)

	if len(out) != 3 {
		t.Fatalf("expected 3 tables matched (Users, UserSettings, Posts), got %d", len(out))
	}
	for _, table := range out {
		if table.Name == "Comments" {
			t.Errorf("non-matching table Comments was kept")
		}
	}
}

func TestFilterTables_IncludeAndExclude(t *testing.T) {
	// Exclude wins over include — same precedence as the existing implementation.
	cfg := &config.Config{}
	cfg.Migration.IncludeTables = []string{"*"}
	cfg.Migration.ExcludeTables = []string{"__*"}
	o := &Orchestrator{config: cfg}

	in := []source.Table{tbl("Users"), tbl("__schema")}
	out := o.filterTables(in)

	if len(out) != 1 || out[0].Name != "Users" {
		t.Fatalf("expected only Users, got %+v", out)
	}
}

func TestFilterTables_GlobPatterns(t *testing.T) {
	cfg := &config.Config{}
	cfg.Migration.ExcludeTables = []string{"audit_?", "*_archive"}
	o := &Orchestrator{config: cfg}

	in := []source.Table{
		tbl("audit_1"),       // matches audit_?
		tbl("audit_log"),     // does NOT match audit_? (? is single char)
		tbl("posts"),         // kept
		tbl("posts_archive"), // matches *_archive
	}
	out := o.filterTables(in)

	want := map[string]bool{"audit_log": true, "posts": true}
	if len(out) != len(want) {
		t.Fatalf("expected 2 tables, got %d: %+v", len(out), out)
	}
	for _, table := range out {
		if !want[table.Name] {
			t.Errorf("unexpected table kept: %s", table.Name)
		}
	}
}

func TestMatchesAny_EmptyPatterns(t *testing.T) {
	if matchesAny("Users", nil) {
		t.Errorf("nil pattern list should match nothing")
	}
	if matchesAny("Users", []string{}) {
		t.Errorf("empty pattern list should match nothing")
	}
}

func TestMatchesAny_InvalidGlobIsTreatedAsNoMatch(t *testing.T) {
	// filepath.Match returns ErrBadPattern for malformed globs; we swallow
	// the error and treat it as a no-match so a typo in config doesn't
	// crash the run. Document that behavior here.
	if matchesAny("Users", []string{"[unclosed"}) {
		t.Errorf("malformed glob should not match")
	}
}
