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
			name: "narrow unsigned integer", ct: CanonicalType{Kind: TinyInt, Unsigned: true},
			want: "TINYINT", warningSubstrs: []string{"no unsigned integer", "does not enforce integer width"},
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
			want: "DATETIME", warningSubstrs: []string{"no time-zone-aware", "does not enforce fractional-seconds"},
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
