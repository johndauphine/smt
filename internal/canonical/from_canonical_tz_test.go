package canonical_test

// #169: a TZ-aware timestamp source (pg timestamptz, mssql datetimeoffset) must
// stay time-zone-aware on every target. On MySQL that means TIMESTAMP (which
// stores UTC and converts on read), not naive DATETIME. These assertions pin
// the mapping and the cross-target consistency so the flatten-to-DATETIME
// regression can't return.

import (
	"strings"
	"testing"

	"smt/internal/canonical"
)

func TestFromCanonical_TZAwareTimestampMapping(t *testing.T) {
	tm := func(p int) canonical.TypeMeta { return canonical.TypeMeta{DatetimePrecision: ip(p)} }

	cases := []struct {
		name         string
		src, typ     string
		meta         canonical.TypeMeta
		target, want string
	}{
		// The fix: TZ-aware sources -> MySQL TIMESTAMP (not DATETIME).
		{"pg timestamptz -> mysql", "postgres", "timestamp with time zone", tm(6), "mysql", "TIMESTAMP(6)"},
		{"mssql datetimeoffset -> mysql", "mssql", "datetimeoffset", tm(2), "mysql", "TIMESTAMP(2)"},

		// Regression guards: these must NOT change.
		{"mysql native timestamp -> mysql stays TIMESTAMP", "mysql", "timestamp", tm(6), "mysql", "TIMESTAMP(6)"},
		{"pg naive timestamp -> mysql stays DATETIME", "postgres", "timestamp", tm(3), "mysql", "DATETIME(3)"},

		// Cross-target consistency: the same canonical tzaware_dt is TZ-aware everywhere.
		{"pg timestamptz -> mssql", "postgres", "timestamp with time zone", tm(6), "mssql", "DATETIMEOFFSET(6)"},
		{"pg timestamptz -> pg", "postgres", "timestamp with time zone", tm(6), "postgres", "timestamp(6) with time zone"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ct := canonical.ToCanonical(tc.typ, tc.meta, tc.src)
			got, err := canonical.FromCanonical(ct, tc.target, canonical.RenderOpts{})
			if err != nil {
				t.Fatalf("FromCanonical(%s -> %s): %v", tc.typ, tc.target, err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// The MySQL TIMESTAMP mapping preserves TZ-awareness but is range-limited
// (1970-2038), so the conversion carries an advisory warning rather than the
// old (now incorrect) "no equivalent time-zone-aware type" message.
func TestFromCanonical_TZAwareTimestampToMySQLWarns(t *testing.T) {
	ct := canonical.ToCanonical("timestamp with time zone", canonical.TypeMeta{DatetimePrecision: ip(6)}, "postgres")
	got, warns, err := canonical.FromCanonicalWithWarnings(ct, "mysql", canonical.RenderOpts{})
	if err != nil {
		t.Fatalf("FromCanonicalWithWarnings: %v", err)
	}
	if got != "TIMESTAMP(6)" {
		t.Fatalf("rendered type = %q, want TIMESTAMP(6)", got)
	}
	var found bool
	for _, w := range warns {
		if strings.Contains(w.Reason, "1970-2038") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a 1970-2038 range warning, got %+v", warns)
	}
}
