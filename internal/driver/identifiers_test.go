package driver

import "testing"

func TestNormalizeIdentifier_Postgres(t *testing.T) {
	cases := []struct {
		in, out string
	}{
		{"VoteTypes", "votetypes"},
		{"User-Id", "user_id"},
		{"Posts", "posts"},
		{"", "col_"},
		{"123abc", "col_123abc"},
		{"already_lower", "already_lower"},
	}
	for _, c := range cases {
		if got := NormalizeIdentifier("postgres", c.in); got != c.out {
			t.Errorf("postgres %q -> %q, want %q", c.in, got, c.out)
		}
	}
}

func TestNormalizeIdentifier_PassthroughDialects(t *testing.T) {
	for _, dbType := range []string{"mssql", "mysql", "unknown"} {
		for _, name := range []string{"Posts", "User-Id", "MixedCase"} {
			if got := NormalizeIdentifier(dbType, name); got != name {
				t.Errorf("%s should pass through %q, got %q", dbType, name, got)
			}
		}
	}
}
