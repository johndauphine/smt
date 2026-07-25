// Package conformance pins each engine's driver contract declaratively.
//
// SMT's per-driver contract — registration + alias resolution, identifier
// quoting/escaping, table qualification, parameter placeholders, DSN
// construction, identifier normalization, Defaults, and the set of interfaces
// each concrete driver type implements — is otherwise pinned only indirectly
// (scattered per-package unit tests plus the live-DB CRM matrix). A refactor
// that silently changes quoting, alias resolution, or drops a capability on one
// engine can pass unit tests and only surface in a live matrix run.
//
// This package expresses those expectations as a declarative DriverCase, one
// per engine, run through the single RunDriverConformance helper that fails
// when declared != actual. Every assertion calls real SMT driver code; there is
// no live database, so these run under -short and the pre-commit hook.
//
// Scope note: this is the schema-only surface. DMT's version of this harness
// also pinned pagination SQL (keyset / ROW_NUMBER query builders); SMT removed
// the data-transfer surface (#191), so none of that exists here and none of it
// is pinned.
//
// Deliberately NOT pinned here, because it requires a live connection:
//   - Reader.DatabaseContext() — the static metadata (identifier case, max
//     identifier length, varchar semantics) is baked into each driver's
//     unexported gatherDatabaseContext, which queries a live pool/DB for
//     version/charset/collation. There is no pure accessor exposing just the
//     static half, and Reader.DatabaseContext() dereferences a live pool, so
//     it cannot be called without a database. See the per-engine comments.
package conformance

import (
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/smt/internal/driver"
)

// DriverCase describes one engine's driver-contract expectations. Every field
// is compared against the value produced by real driver code in
// RunDriverConformance.
type DriverCase struct {
	// Name is the canonical driver name (e.g. "postgres").
	Name string

	// Aliases are the exact alternate names Driver.Aliases() must return; each
	// must also resolve through the registry and Canonicalize back to Name.
	Aliases []string

	// Concrete types under test, as typed-nil pointers wrapped in reflect.Type
	// (e.g. reflect.TypeOf((*postgres.Driver)(nil))). These pin the capability
	// matrix: DriverType must implement driver.Driver, ReaderType
	// driver.Reader, WriterType driver.Writer, DialectType driver.Dialect. A
	// refactor that removes an interface method from any concrete type flips
	// the corresponding reflect.Implements to false and fails the test.
	DriverType  reflect.Type
	ReaderType  reflect.Type
	WriterType  reflect.Type
	DialectType reflect.Type

	// Identifier quoting.
	QuoteInput string // e.g. "Col"
	QuoteWant  string // e.g. `"Col"`

	// Quote escaping: an input containing the dialect's quote char, which must
	// be doubled/escaped in the output.
	QuoteEscapeInput string
	QuoteEscapeWant  string

	// Table qualification.
	QualSchema         string // schema for the qualified case
	QualTable          string // table name used for both cases
	QualWant           string // QualifyTable(QualSchema, QualTable)
	QualSchemalessWant string // QualifyTable("", QualTable)

	// Parameter placeholder.
	PlaceholderIndex int
	PlaceholderWant  string

	// Identifier normalization via driver.NormalizeIdentifier. Asserted against
	// the canonical Name AND against every alias (Canonicalize(alias) must feed
	// NormalizeIdentifier to the same result), pinning that aliases share the
	// engine's normalization behavior.
	NormalizeInput string
	NormalizeWant  string

	// Defaults expected from Driver.Defaults(), compared field-by-field.
	WantDefaults driver.DriverDefaults

	// DSNCases pin BuildDSN output for fixed inputs (credential escaping,
	// default port, ssl/encrypt defaults). Exact-string comparison.
	DSNCases []DSNCase
}

// DSNCase is one BuildDSN expectation.
type DSNCase struct {
	Desc     string
	Host     string
	Port     int
	Database string
	User     string
	Password string
	Opts     map[string]any
	Want     string
}

// RunDriverConformance runs the full declarative driver-contract check.
func RunDriverConformance(t *testing.T, tc DriverCase) {
	t.Helper()
	validateCase(t, tc)

	t.Run("registration and aliases", func(t *testing.T) {
		got, err := driver.Get(tc.Name)
		if err != nil {
			t.Fatalf("driver.Get(%q): %v", tc.Name, err)
		}
		if got.Name() != tc.Name {
			t.Fatalf("registered driver name = %q, want %q", got.Name(), tc.Name)
		}
		if c := driver.Canonicalize(strings.ToUpper(tc.Name)); c != tc.Name {
			t.Fatalf("Canonicalize(%q) = %q, want %q", strings.ToUpper(tc.Name), c, tc.Name)
		}
		if aliases := got.Aliases(); !sameStrings(aliases, tc.Aliases) {
			t.Fatalf("Driver.Aliases() = %v, want %v", aliases, tc.Aliases)
		}
		for _, alias := range tc.Aliases {
			ad, err := driver.Get(alias)
			if err != nil {
				t.Fatalf("driver.Get(alias %q): %v", alias, err)
			}
			if ad.Name() != tc.Name {
				t.Fatalf("alias %q resolved to %q, want %q", alias, ad.Name(), tc.Name)
			}
			if c := driver.Canonicalize(alias); c != tc.Name {
				t.Fatalf("Canonicalize(%q) = %q, want %q", alias, c, tc.Name)
			}
		}
	})

	t.Run("dialect identity and quoting", func(t *testing.T) {
		d := mustDialect(t, tc.Name)
		if d.DBType() != tc.Name {
			t.Fatalf("Dialect.DBType() = %q, want %q", d.DBType(), tc.Name)
		}
		if got := d.QuoteIdentifier(tc.QuoteInput); got != tc.QuoteWant {
			t.Fatalf("QuoteIdentifier(%q) = %q, want %q", tc.QuoteInput, got, tc.QuoteWant)
		}
		if got := d.QuoteIdentifier(tc.QuoteEscapeInput); got != tc.QuoteEscapeWant {
			t.Fatalf("QuoteIdentifier(%q) = %q, want %q (quote-char escaping)", tc.QuoteEscapeInput, got, tc.QuoteEscapeWant)
		}
	})

	t.Run("table qualification", func(t *testing.T) {
		d := mustDialect(t, tc.Name)
		if got := d.QualifyTable(tc.QualSchema, tc.QualTable); got != tc.QualWant {
			t.Fatalf("QualifyTable(%q, %q) = %q, want %q", tc.QualSchema, tc.QualTable, got, tc.QualWant)
		}
		if got := d.QualifyTable("", tc.QualTable); got != tc.QualSchemalessWant {
			t.Fatalf("QualifyTable(\"\", %q) = %q, want %q (schema-less)", tc.QualTable, got, tc.QualSchemalessWant)
		}
	})

	t.Run("parameter placeholder", func(t *testing.T) {
		d := mustDialect(t, tc.Name)
		if got := d.ParameterPlaceholder(tc.PlaceholderIndex); got != tc.PlaceholderWant {
			t.Fatalf("ParameterPlaceholder(%d) = %q, want %q", tc.PlaceholderIndex, got, tc.PlaceholderWant)
		}
	})

	t.Run("identifier normalization", func(t *testing.T) {
		if got := driver.NormalizeIdentifier(tc.Name, tc.NormalizeInput); got != tc.NormalizeWant {
			t.Fatalf("NormalizeIdentifier(%q, %q) = %q, want %q", tc.Name, tc.NormalizeInput, got, tc.NormalizeWant)
		}
		// Aliases must resolve (via Canonicalize) to the same normalization.
		for _, alias := range tc.Aliases {
			canon := driver.Canonicalize(alias)
			if got := driver.NormalizeIdentifier(canon, tc.NormalizeInput); got != tc.NormalizeWant {
				t.Fatalf("NormalizeIdentifier(Canonicalize(%q)=%q, %q) = %q, want %q",
					alias, canon, tc.NormalizeInput, got, tc.NormalizeWant)
			}
		}
	})

	t.Run("defaults", func(t *testing.T) {
		d, err := driver.Get(tc.Name)
		if err != nil {
			t.Fatalf("driver.Get(%q): %v", tc.Name, err)
		}
		if got := d.Defaults(); got != tc.WantDefaults {
			t.Fatalf("Defaults() = %+v, want %+v", got, tc.WantDefaults)
		}
	})

	t.Run("dsn construction", func(t *testing.T) {
		d := mustDialect(t, tc.Name)
		for _, dc := range tc.DSNCases {
			got := d.BuildDSN(dc.Host, dc.Port, dc.Database, dc.User, dc.Password, dc.Opts)
			if got != dc.Want {
				t.Fatalf("BuildDSN[%s] =\n  %q\nwant\n  %q", dc.Desc, got, dc.Want)
			}
		}
	})

	t.Run("capability matrix", func(t *testing.T) {
		assertImplements(t, tc.DriverType, reflect.TypeOf((*driver.Driver)(nil)).Elem(), "driver.Driver")
		assertImplements(t, tc.ReaderType, reflect.TypeOf((*driver.Reader)(nil)).Elem(), "driver.Reader")
		assertImplements(t, tc.WriterType, reflect.TypeOf((*driver.Writer)(nil)).Elem(), "driver.Writer")
		assertImplements(t, tc.DialectType, reflect.TypeOf((*driver.Dialect)(nil)).Elem(), "driver.Dialect")

		// The registered driver must hand back a dialect of the pinned concrete
		// type, tying the registry entry to the type asserted above.
		reg, err := driver.Get(tc.Name)
		if err != nil {
			t.Fatalf("driver.Get(%q): %v", tc.Name, err)
		}
		if got := reflect.TypeOf(reg.Dialect()); got != tc.DialectType {
			t.Fatalf("Driver.Dialect() concrete type = %v, want %v", got, tc.DialectType)
		}
	})
}

func mustDialect(t *testing.T, name string) driver.Dialect {
	t.Helper()
	d, err := driver.Get(name)
	if err != nil {
		t.Fatalf("driver.Get(%q): %v", name, err)
	}
	return d.Dialect()
}

func assertImplements(t *testing.T, concrete, iface reflect.Type, ifaceName string) {
	t.Helper()
	if concrete == nil {
		t.Fatalf("concrete type for %s is nil", ifaceName)
	}
	if !concrete.Implements(iface) {
		t.Fatalf("%v does not implement %s", concrete, ifaceName)
	}
}

func validateCase(t *testing.T, tc DriverCase) {
	t.Helper()
	if tc.Name == "" {
		t.Fatal("DriverCase.Name is required")
	}
	required := map[string]string{
		"QuoteInput":       tc.QuoteInput,
		"QuoteWant":        tc.QuoteWant,
		"QuoteEscapeInput": tc.QuoteEscapeInput,
		"QuoteEscapeWant":  tc.QuoteEscapeWant,
		"QualTable":        tc.QualTable,
		"QualWant":         tc.QualWant,
		"PlaceholderWant":  tc.PlaceholderWant,
		"NormalizeInput":   tc.NormalizeInput,
		"NormalizeWant":    tc.NormalizeWant,
	}
	for field, value := range required {
		if value == "" {
			t.Fatalf("DriverCase.%s is required", field)
		}
	}
	if tc.PlaceholderIndex <= 0 {
		t.Fatal("DriverCase.PlaceholderIndex must be greater than zero")
	}
	if len(tc.DSNCases) == 0 {
		t.Fatal("DriverCase.DSNCases is required")
	}
	if tc.DriverType == nil || tc.ReaderType == nil || tc.WriterType == nil || tc.DialectType == nil {
		t.Fatal("DriverCase capability types (Driver/Reader/Writer/Dialect) are required")
	}
}

func sameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
