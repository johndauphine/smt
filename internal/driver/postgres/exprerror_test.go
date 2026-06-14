package postgres

import (
	"errors"
	"strings"
	"testing"

	"smt/internal/driver"
)

// The deterministic renderer must surface an unsupported DEFAULT as a structured
// *driver.ExpressionRenderError (through all the error wrapping), so the AI
// fix-suggestion path can translate just that expression (#134).
func TestCreateTable_UnsupportedDefaultIsStructured(t *testing.T) {
	tbl := &driver.Table{
		Schema: "dbo", Name: "Subscriptions",
		Columns: []driver.Column{
			{Name: "ExpiresAt", DataType: "datetime2", IsNullable: false,
				DefaultExpression: "(dateadd(year,(1),getdate()))"},
		},
	}
	_, _, err := RenderCreateTableDDLWithSource(tbl, "public", false, "fail", "mssql")
	if err == nil {
		t.Fatal("expected render error for unsupported DATEADD default")
	}
	var ex *driver.ExpressionRenderError
	if !errors.As(err, &ex) {
		t.Fatalf("error is not *ExpressionRenderError: %v", err)
	}
	if ex.Kind != "default" || ex.Column != "ExpiresAt" {
		t.Errorf("unexpected structured error: kind=%q column=%q", ex.Kind, ex.Column)
	}
	if ex.SourceExpr != "(dateadd(year,(1),getdate()))" {
		t.Errorf("SourceExpr = %q, want the raw source default", ex.SourceExpr)
	}
}

// With the override set, the same column renders verbatim (the splice point).
func TestCreateTable_DefaultOverrideEmittedVerbatim(t *testing.T) {
	tbl := &driver.Table{
		Schema: "dbo", Name: "Subscriptions",
		Columns: []driver.Column{
			{Name: "ExpiresAt", DataType: "datetime2", IsNullable: false,
				DefaultExpression:         "(dateadd(year,(1),getdate()))",
				DefaultExpressionOverride: "CURRENT_TIMESTAMP + INTERVAL '1 year'"},
		},
	}
	ddl, _, err := RenderCreateTableDDLWithSource(tbl, "public", false, "fail", "mssql")
	if err != nil {
		t.Fatalf("render with override failed: %v", err)
	}
	if !strings.Contains(ddl, "DEFAULT CURRENT_TIMESTAMP + INTERVAL '1 year'") {
		t.Errorf("override not emitted verbatim:\n%s", ddl)
	}
}
