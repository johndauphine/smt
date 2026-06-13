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

// A literal include/exclude pattern that the target dialect slugs (spaces →
// underscores) must still match the normalized target table name.
func TestFilterTablesByScope_NormalizedLiteral(t *testing.T) {
	norm := func(s string) string { return driver.NormalizeIdentifier("postgres", s) }
	// Target table is already normalized to "order_items".
	tables := []driver.Table{{Name: "order_items"}, {Name: "orders"}}

	// Exclude the literal source name "Order Items" — must drop order_items.
	got := filterTablesByScope(tables, nil, []string{"Order Items"}, norm)
	if len(got) != 1 || got[0].Name != "orders" {
		t.Errorf("literal slug-needing exclude failed: %+v", got)
	}
	// Include the literal name — must keep only order_items.
	got = filterTablesByScope(tables, []string{"Order Items"}, nil, norm)
	if len(got) != 1 || got[0].Name != "order_items" {
		t.Errorf("literal slug-needing include failed: %+v", got)
	}
	// A glob still works via plain CI matching.
	got = filterTablesByScope(tables, []string{"order*"}, nil, norm)
	if len(got) != 2 {
		t.Errorf("glob include should match both: %+v", got)
	}
}
