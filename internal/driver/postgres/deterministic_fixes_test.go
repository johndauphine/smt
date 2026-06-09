package postgres

import (
	"testing"

	"smt/internal/driver"
)

// #79 — DEFAULT (NULL) on a textual column must stay SQL NULL, not become the
// string literal 'NULL'.
func TestDeterministicDefaultNullStaysKeyword(t *testing.T) {
	renderer := newDeterministicDDL()
	got, err := renderer.defaultExpression(driver.Column{
		Name:              "Notes",
		DataType:          "varchar",
		MaxLength:         50,
		DefaultExpression: "(NULL)",
	})
	if err != nil {
		t.Fatalf("defaultExpression: %v", err)
	}
	if got != "NULL" {
		t.Fatalf("DEFAULT (NULL) rendered as %q, want NULL", got)
	}
}

// #79 — bare textual defaults still get quoted.
func TestDeterministicDefaultBareWordStillQuoted(t *testing.T) {
	renderer := newDeterministicDDL()
	got, err := renderer.defaultExpression(driver.Column{
		Name:              "Status",
		DataType:          "varchar",
		MaxLength:         20,
		DefaultExpression: "(active)",
	})
	if err != nil {
		t.Fatalf("defaultExpression: %v", err)
	}
	if got != "'active'" {
		t.Fatalf("bare word default rendered as %q, want 'active'", got)
	}
}
