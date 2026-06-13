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

// filterToManagedSet keeps managed tables and genuine target-only extras,
// dropping target tables that correspond to an out-of-scope (e.g. excluded)
// source table.
func TestFilterToManagedSet(t *testing.T) {
	// Source has orders, items, auditlog; auditlog is excluded → not in desired.
	desired := []driver.Table{{Name: "orders"}, {Name: "items"}} // managed (normalized)
	allSource := map[string]bool{"orders": true, "items": true, "auditlog": true}
	existing := []driver.Table{
		{Name: "orders"},   // managed
		{Name: "items"},    // managed
		{Name: "auditlog"}, // excluded source table on target → drop
		{Name: "legacy"},   // target-only, not a source table → keep (extra)
	}
	got := filterToManagedSet(existing, desired, allSource)
	names := map[string]bool{}
	for _, tb := range got {
		names[tb.Name] = true
	}
	if names["auditlog"] {
		t.Error("excluded source table on target must be dropped from drift scope")
	}
	if !names["orders"] || !names["items"] {
		t.Error("managed tables must be kept")
	}
	if !names["legacy"] {
		t.Error("genuine target-only table must be kept for extra detection")
	}
}
