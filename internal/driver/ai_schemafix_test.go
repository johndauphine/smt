package driver

import (
	"strings"
	"testing"
)

func TestParseExpressionFix_Valid(t *testing.T) {
	fix, err := parseExpressionFix("```json\n{\"expression\":\"CURRENT_TIMESTAMP + INTERVAL '1 year'\",\"explanation\":\"x\",\"confidence\":\"HIGH\"}\n```")
	if err != nil {
		t.Fatalf("parseExpressionFix: %v", err)
	}
	if !strings.Contains(fix.Expression, "INTERVAL") {
		t.Fatalf("unexpected expression: %q", fix.Expression)
	}
	if fix.Confidence != "high" {
		t.Errorf("confidence not normalized: %q", fix.Confidence)
	}
}

func TestParseExpressionFix_MissingExpressionFails(t *testing.T) {
	if _, err := parseExpressionFix(`{"expression":"   ","confidence":"high"}`); err == nil {
		t.Error("expected error for empty expression")
	}
}

func TestParseExpressionFix_DefaultsConfidence(t *testing.T) {
	fix, err := parseExpressionFix(`{"expression":"CURRENT_DATE"}`)
	if err != nil {
		t.Fatalf("parseExpressionFix: %v", err)
	}
	if fix.Confidence != "medium" {
		t.Errorf("confidence default = %q, want medium", fix.Confidence)
	}
}

func TestBuildExpressionFixPrompt_ScopesToOneExpression(t *testing.T) {
	p := buildExpressionFixPrompt(FixRequest{
		Kind: "default", SourceExpr: "dateadd(year,1,getdate())",
		ColumnName: "ExpiresAt", ColumnType: "datetime2",
		SourceDialect: "mssql", TargetDialect: "postgres",
	})
	for _, want := range []string{
		"dateadd(year,1,getdate())", "ExpiresAt", "datetime2",
		"ONLY the replacement", "Do not include the DEFAULT",
	} {
		if !strings.Contains(p, want) {
			t.Errorf("prompt missing %q", want)
		}
	}
	// It must not invite a whole-table rewrite.
	if strings.Contains(strings.ToUpper(p), "CREATE TABLE") {
		t.Error("expression-fix prompt should not mention CREATE TABLE")
	}
}
