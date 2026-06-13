package main

import (
	"testing"

	"smt/internal/config"
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
