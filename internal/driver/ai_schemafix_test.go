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

func TestValidateTargetExpression(t *testing.T) {
	ok := []string{
		"CURRENT_DATE",
		"NOW() + INTERVAL '1 year'",
		"(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date",
		"gen_random_uuid()",
		"'a, b; c'", // commas/semicolons inside a string literal are fine
	}
	for _, e := range ok {
		if err := ValidateTargetExpression(e); err != nil {
			t.Errorf("ValidateTargetExpression(%q) = %v, want nil", e, err)
		}
	}
	bad := []string{
		"",
		"1), (\"hacked\" text DEFAULT (2", // injects a column
		"1; DROP TABLE users",             // statement separator
		"now() + interval '1 year",        // unterminated string
		"foo(()",                          // unbalanced parens
		"1, 2",                            // top-level comma
		"NOW() --",                        // line comment hides the separating comma
		"NOW() /* x",                      // block comment swallows the rest
		`'a\'`,                            // backslash escape can break out on some dialects
	}
	for _, e := range bad {
		if err := ValidateTargetExpression(e); err == nil {
			t.Errorf("ValidateTargetExpression(%q) = nil, want error", e)
		}
	}
}

func TestDefaultExpressionsEquivalent(t *testing.T) {
	// The #127 idiom translates into a known class -> confirmable.
	if !DefaultExpressionsEquivalent("(convert(date,getdate()))", "CURRENT_DATE") {
		t.Error("CONVERT(date,getdate()) should be class-equivalent to CURRENT_DATE")
	}
	// A novel translation the comparator can't equate -> review.
	if DefaultExpressionsEquivalent("(dateadd(year,(1),getdate()))", "NOW() + INTERVAL '1 year'") {
		t.Error("dateadd vs now()+interval should NOT be mechanically confirmed equivalent")
	}
}
