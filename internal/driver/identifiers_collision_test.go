package driver

import (
	"strings"
	"testing"
)

// TestNormalizeIdentifier_AliasNotBypassed guards the #189 alias fix: pg /
// postgresql must fold exactly like the canonical postgres name, so an alias
// in config can't skip normalization.
func TestNormalizeIdentifier_AliasNotBypassed(t *testing.T) {
	const name = "Order Details"
	want := NormalizeIdentifier("postgres", name)
	if want != "order_details" {
		t.Fatalf("canonical postgres normalize = %q, want order_details", want)
	}
	for _, alias := range []string{"pg", "postgresql", "POSTGRES"} {
		if got := NormalizeIdentifier(alias, name); got != want {
			t.Errorf("NormalizeIdentifier(%q, %q) = %q, want %q", alias, name, got, want)
		}
	}
}

// TestNormalizePostgresIdentifier_Truncation guards the 63-byte limit (#189):
// over-long names are truncated to fit, and two names sharing a 63-byte prefix
// stay distinct via the hash suffix.
func TestNormalizePostgresIdentifier_Truncation(t *testing.T) {
	long := "a_very_long_" + strings.Repeat("x", 80) // > 63 bytes after folding
	got := NormalizeIdentifier("postgres", long)
	if len(got) > pgMaxIdentifierBytes {
		t.Fatalf("normalized len = %d, want <= %d", len(got), pgMaxIdentifierBytes)
	}

	// Two names identical for the first 70 chars but differing after byte 63
	// must not collapse to the same identifier.
	a := strings.Repeat("c", 70) + "_alpha"
	b := strings.Repeat("c", 70) + "_beta"
	na := NormalizeIdentifier("postgres", a)
	nb := NormalizeIdentifier("postgres", b)
	if na == nb {
		t.Fatalf("distinct long names both normalized to %q — hash suffix missing", na)
	}
	if len(na) > pgMaxIdentifierBytes || len(nb) > pgMaxIdentifierBytes {
		t.Fatalf("truncated names exceed limit: %d / %d", len(na), len(nb))
	}
	// Stable across calls.
	if NormalizeIdentifier("postgres", a) != na {
		t.Error("truncation is not deterministic")
	}
}

func TestCheckIdentifierCollisions(t *testing.T) {
	tbl := func(name string, cols ...string) Table {
		t := Table{Name: name}
		for _, c := range cols {
			t.Columns = append(t.Columns, Column{Name: c})
		}
		return t
	}

	t.Run("table collision on postgres errors", func(t *testing.T) {
		err := CheckIdentifierCollisions("postgres", []Table{
			tbl("Order Details"),
			tbl("Order-Details"),
		})
		if err == nil {
			t.Fatal("expected collision error")
		}
		if !strings.Contains(err.Error(), "Order Details") || !strings.Contains(err.Error(), "Order-Details") {
			t.Errorf("error should name both source identifiers: %v", err)
		}
	})

	t.Run("column collision within a table errors", func(t *testing.T) {
		err := CheckIdentifierCollisions("postgres", []Table{
			tbl("users", "User Name", "User-Name"),
		})
		if err == nil {
			t.Fatal("expected column collision error")
		}
		if !strings.Contains(err.Error(), "users") {
			t.Errorf("error should name the table: %v", err)
		}
	})

	t.Run("no collision passes", func(t *testing.T) {
		if err := CheckIdentifierCollisions("postgres", []Table{
			tbl("orders", "id", "status"),
			tbl("customers", "id", "name"),
		}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("alias also gated", func(t *testing.T) {
		if err := CheckIdentifierCollisions("pg", []Table{tbl("A B"), tbl("A-B")}); err == nil {
			t.Fatal("alias pg should be gated like postgres")
		}
	})

	t.Run("non-folding targets are a no-op", func(t *testing.T) {
		for _, dbType := range []string{"mssql", "mysql", "sqlserver"} {
			if err := CheckIdentifierCollisions(dbType, []Table{tbl("A B"), tbl("A-B")}); err != nil {
				t.Errorf("%s should not gate (case-preserving): %v", dbType, err)
			}
		}
	})
}
