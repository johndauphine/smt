package main

import (
	"testing"

	"smt/internal/config"
	"smt/internal/driver"
)

// targetAsSource must carry the connection details the target reader needs,
// including the MSSQL TLS knobs, so introspecting an MSSQL target works the
// same as a source.
func TestTargetAsSource_CopiesConnection(t *testing.T) {
	enc := true
	cfg := &config.Config{}
	cfg.Target.Type = "mssql"
	cfg.Target.Host = "tgt-host"
	cfg.Target.Port = 1433
	cfg.Target.Database = "TgtDB"
	cfg.Target.User = "sa"
	cfg.Target.Password = "pw"
	cfg.Target.Schema = "dbo"
	cfg.Target.TrustServerCert = true
	cfg.Target.Encrypt = &enc

	sc := targetAsSource(cfg)
	if sc.Type != "mssql" || sc.Host != "tgt-host" || sc.Port != 1433 || sc.Database != "TgtDB" ||
		sc.User != "sa" || sc.Password != "pw" || sc.Schema != "dbo" || !sc.TrustServerCert ||
		sc.Encrypt == nil || !*sc.Encrypt {
		t.Errorf("targetAsSource dropped a connection field: %+v", sc)
	}
}

// Dialect aliases must canonicalize before driving comparison/normalization,
// so sqlserver/pg configs don't produce false drift.
func TestDriftDialectCanonicalization(t *testing.T) {
	cases := map[string]string{
		"sqlserver":  "mssql",
		"pg":         "postgres",
		"postgresql": "postgres",
		"mariadb":    "mysql",
		"mssql":      "mssql",
	}
	for in, want := range cases {
		if got := driver.Canonicalize(in); got != want {
			t.Errorf("Canonicalize(%q) = %q, want %q", in, got, want)
		}
	}
}

// filterDesiredScope must match create/sync exactly: case-sensitive
// filepath.Match on the original source names (exclude wins; include is an
// allowlist).
func TestFilterDesiredScope_MatchesOrchestrator(t *testing.T) {
	tables := []driver.Table{{Name: "Orders"}, {Name: "AuditLog"}, {Name: "Items"}}

	// Exclude glob — case-sensitive, like the orchestrator.
	got := filterDesiredScope(tables, nil, []string{"Audit*"})
	if len(got) != 2 || got[0].Name != "Orders" || got[1].Name != "Items" {
		t.Errorf("exclude Audit* = %+v, want [Orders Items]", got)
	}
	// Case-sensitive: lowercase pattern must NOT match the capitalized name.
	got = filterDesiredScope(tables, nil, []string{"audit*"})
	if len(got) != 3 {
		t.Errorf("case-sensitive exclude should not match AuditLog: %+v", got)
	}
	// Include allowlist.
	got = filterDesiredScope(tables, []string{"Orders"}, nil)
	if len(got) != 1 || got[0].Name != "Orders" {
		t.Errorf("include [Orders] = %+v", got)
	}
	// No scope → unchanged.
	if got := filterDesiredScope(tables, nil, nil); len(got) != 3 {
		t.Errorf("no scope should pass through, got %+v", got)
	}
}

// filterToManagedSet scopes the target to managed tables by normalized-name
// membership (case-insensitive), so out-of-scope target tables aren't compared.
func TestFilterToManagedSet(t *testing.T) {
	desired := []driver.Table{{Name: "orders"}, {Name: "items"}} // normalized names
	existing := []driver.Table{{Name: "orders"}, {Name: "ORDERS_BAK"}, {Name: "items"}, {Name: "audit_log"}}
	got := filterToManagedSet(existing, desired)
	if len(got) != 2 {
		t.Fatalf("managed set should keep only orders+items, got %+v", got)
	}
	names := map[string]bool{}
	for _, t := range got {
		names[t.Name] = true
	}
	if !names["orders"] || !names["items"] {
		t.Errorf("managed set missing expected tables: %+v", got)
	}
}
