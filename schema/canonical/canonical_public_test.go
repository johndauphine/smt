package canonical_test

import (
	"testing"

	"smt/schema/canonical"
)

func TestPublicPackageSupportsTypeMapping(t *testing.T) {
	ct := canonical.ToCanonical(
		"timestamp with time zone",
		canonical.TypeMeta{},
		"postgres",
	)
	got, err := canonical.FromCanonical(ct, "mysql", canonical.RenderOpts{})
	if err != nil {
		t.Fatalf("FromCanonical: %v", err)
	}
	if got != "TIMESTAMP(6)" {
		t.Fatalf("rendered type = %q, want TIMESTAMP(6)", got)
	}
}
