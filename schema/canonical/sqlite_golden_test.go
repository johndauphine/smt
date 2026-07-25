package canonical

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestSQLiteToCanonical_Golden(t *testing.T) {
	intp := func(v int) *int { return &v }
	cases := []struct {
		name     string
		typeName string
		meta     TypeMeta
		want     CanonicalType
	}{
		{
			name: "integer affinity is 64 bit", typeName: "INTEGER",
			want: CanonicalType{Kind: BigInt},
		},
		{
			name: "int4 preserves four byte intent", typeName: "int4",
			want: CanonicalType{Kind: Integer},
		},
		{
			name: "boolean", typeName: "BOOLEAN",
			want: CanonicalType{Kind: Boolean},
		},
		{
			name: "fixed bit width", typeName: "BIT(8)",
			want: CanonicalType{Kind: BitString, Length: 8},
		},
		{
			name: "variable bit width", typeName: "BIT VARYING(12)",
			want: CanonicalType{Kind: VarBitString, Length: 12},
		},
		{
			name: "varchar inline length", typeName: "VARCHAR(255)",
			want: CanonicalType{Kind: Varchar, Length: 255},
		},
		{
			name: "metadata length wins", typeName: "varchar(255)",
			meta: TypeMeta{MaxLength: 128},
			want: CanonicalType{Kind: Varchar, Length: 128},
		},
		{
			name: "numeric inline precision and zero scale", typeName: "NUMERIC(12,0)",
			want: CanonicalType{Kind: Decimal, Precision: 12, Scale: 0},
		},
		{
			name: "text", typeName: "CLOB",
			want: CanonicalType{Kind: Text},
		},
		{
			name: "blob", typeName: "BLOB",
			want: CanonicalType{Kind: Blob},
		},
		{
			name: "datetime fractional seconds", typeName: "DATETIME(6)",
			want: CanonicalType{Kind: Timestamp, Fsp: intp(6)},
		},
		{
			name: "uuid convention", typeName: "UUID",
			want: CanonicalType{Kind: Uuid},
		},
		{
			name: "json convention", typeName: "JSON",
			want: CanonicalType{Kind: Json},
		},
		{
			name: "empty declaration is portable text", typeName: "",
			want: CanonicalType{Kind: Text},
		},
		{
			name: "custom blob affinity", typeName: "APP_BLOB",
			want: CanonicalType{Kind: Blob},
		},
		{
			name: "floating point affinity precedence", typeName: "FLOATING POINT",
			want: CanonicalType{Kind: BigInt},
		},
		{
			name: "unknown declaration stays raw", typeName: "GEOMETRY",
			want: CanonicalType{Kind: Raw, Raw: "geometry"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ToCanonical(tc.typeName, tc.meta, "sqlite3"); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("ToCanonical(%q, %#v, sqlite3) = %#v, want %#v", tc.typeName, tc.meta, got, tc.want)
			}
		})
	}
}

func TestFromCanonicalSQLite_Golden(t *testing.T) {
	intp := func(v int) *int { return &v }
	cases := []struct {
		name           string
		ct             CanonicalType
		want           string
		warningSubstrs []string
	}{
		{
			name: "boolean", ct: CanonicalType{Kind: Boolean},
			want: "BOOLEAN",
		},
		{
			name: "fixed bit string", ct: CanonicalType{Kind: BitString, Length: 8},
			want: "BIT(8)", warningSubstrs: []string{"no native bit-string"},
		},
		{
			name: "variable bit string", ct: CanonicalType{Kind: VarBitString, Length: 12},
			want: "BIT VARYING(12)", warningSubstrs: []string{"no native bit-string"},
		},
		{
			name: "narrow unsigned integer", ct: CanonicalType{Kind: TinyInt, Unsigned: true},
			want: "TINYINT", warningSubstrs: []string{"no unsigned integer", "does not enforce integer width"},
		},
		{
			name: "small integer width", ct: CanonicalType{Kind: SmallInt},
			want: "SMALLINT", warningSubstrs: []string{"does not enforce integer width"},
		},
		{
			name: "integer width", ct: CanonicalType{Kind: Integer},
			want: "INTEGER", warningSubstrs: []string{"does not enforce integer width"},
		},
		{
			name: "decimal precision", ct: CanonicalType{Kind: Decimal, Precision: 12, Scale: 2},
			want: "NUMERIC(12,2)", warningSubstrs: []string{"does not enforce declared precision"},
		},
		{
			name: "bounded varchar", ct: CanonicalType{Kind: Varchar, Length: 255},
			want: "VARCHAR(255)", warningSubstrs: []string{"does not enforce declared length"},
		},
		{
			name: "unbounded varchar", ct: CanonicalType{Kind: Varchar},
			want: "TEXT",
		},
		{
			name: "sized binary", ct: CanonicalType{Kind: VarBinary, Length: 16},
			want: "BLOB", warningSubstrs: []string{"does not enforce declared byte length"},
		},
		{
			name: "tz timestamp fractional seconds", ct: CanonicalType{Kind: Timestamp, WithTZ: true, Fsp: intp(6)},
			want: "DATETIME(6)", warningSubstrs: []string{"no time-zone-aware", "does not enforce fractional-seconds"},
		},
		{
			name: "uuid", ct: CanonicalType{Kind: Uuid},
			want: "TEXT", warningSubstrs: []string{"no native UUID"},
		},
		{
			name: "json", ct: CanonicalType{Kind: Json},
			want: "TEXT", warningSubstrs: []string{"stored as TEXT"},
		},
		{
			name: "enum", ct: CanonicalType{Kind: Enum, EnumValues: []string{"a", "b"}},
			want: "TEXT", warningSubstrs: []string{"no native enum/set"},
		},
		{
			name: "spatial", ct: CanonicalType{Kind: Spatial, SpatialType: "geometry", SRID: 4326},
			want: "BLOB", warningSubstrs: []string{"requires an extension"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, warnings, err := FromCanonicalWithWarnings(tc.ct, "sqlite", RenderOpts{})
			if err != nil {
				t.Fatalf("FromCanonicalWithWarnings: %v", err)
			}
			if got != tc.want {
				t.Fatalf("rendered type = %q, want %q", got, tc.want)
			}
			for _, want := range tc.warningSubstrs {
				if !hasSQLiteWarning(warnings, want) {
					t.Errorf("warnings = %+v, missing %q", warnings, want)
				}
			}
			if len(tc.warningSubstrs) == 0 && len(warnings) != 0 {
				t.Errorf("warnings = %+v, want none", warnings)
			}
		})
	}
}

func TestSQLiteRoundTripPreservesRowIDCapableBigInt(t *testing.T) {
	ct := ToCanonical("INTEGER", TypeMeta{}, "sqlite")
	if ct.Kind != BigInt {
		t.Fatalf("ToCanonical(INTEGER) kind = %v, want BigInt", ct.Kind)
	}

	got, err := FromCanonical(ct, "sqlite", RenderOpts{})
	if err != nil {
		t.Fatalf("FromCanonical: %v", err)
	}
	if got != "INTEGER" {
		t.Fatalf("INTEGER round-trip rendered type = %q, want INTEGER", got)
	}
}

func TestSQLiteIntegerRoundTripWarnsAndWidens(t *testing.T) {
	rendered, warnings, err := FromCanonicalWithWarnings(CanonicalType{Kind: Integer}, "sqlite", RenderOpts{})
	if err != nil {
		t.Fatalf("FromCanonicalWithWarnings: %v", err)
	}
	if rendered != "INTEGER" {
		t.Fatalf("Integer rendered type = %q, want INTEGER", rendered)
	}
	if !hasSQLiteWarning(warnings, "does not enforce integer width") {
		t.Fatalf("warnings = %+v, missing narrow-integer affinity warning", warnings)
	}

	got := ToCanonical(rendered, TypeMeta{}, "sqlite")
	if got.Kind != BigInt {
		t.Fatalf("ToCanonical(%q) kind = %v, want BigInt", rendered, got.Kind)
	}
}

func TestSQLiteRoundTripPreservesTemporalFsp(t *testing.T) {
	for _, tc := range []struct {
		name     string
		typeName string
		want     string
	}{
		{name: "datetime", typeName: "DATETIME(6)", want: "DATETIME(6)"},
		{name: "time", typeName: "TIME(6)", want: "TIME(6)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ct := ToCanonical(tc.typeName, TypeMeta{}, "sqlite")
			got, err := FromCanonical(ct, "sqlite", RenderOpts{})
			if err != nil {
				t.Fatalf("FromCanonical: %v", err)
			}
			if got != tc.want {
				t.Fatalf("%s round-trip rendered type = %q, want %q", tc.typeName, got, tc.want)
			}
		})
	}
}

func TestFromCanonicalSQLite_RawIsStillRejected(t *testing.T) {
	_, err := FromCanonical(CanonicalType{Kind: Raw, Raw: "GEOMETRY"}, "sqlite", RenderOpts{})
	if !errors.Is(err, ErrUnknownType) {
		t.Fatalf("FromCanonical raw error = %v, want ErrUnknownType", err)
	}
}

func TestFromCanonicalSQLite_AcceptsSQLite3Alias(t *testing.T) {
	got, err := FromCanonical(CanonicalType{Kind: Integer}, "sqlite3", RenderOpts{})
	if err != nil {
		t.Fatalf("FromCanonical: %v", err)
	}
	if got != "INTEGER" {
		t.Fatalf("rendered type = %q, want INTEGER", got)
	}
}

func hasSQLiteWarning(warnings []MappingWarning, want string) bool {
	for _, warning := range warnings {
		if warning.TargetDialect == "sqlite" && strings.Contains(warning.Reason, want) {
			return true
		}
	}
	return false
}
