package canonical

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestClickHouseToCanonical_Golden(t *testing.T) {
	intp := func(v int) *int { return &v }
	cases := []struct {
		name     string
		typeName string
		want     CanonicalType
	}{
		{
			name: "nullable and low cardinality wrappers are stripped", typeName: "Nullable(LowCardinality(String))",
			want: CanonicalType{Kind: Text},
		},
		{
			name: "wrappers nest in either order", typeName: "LowCardinality(Nullable(FixedString(16)))",
			want: CanonicalType{Kind: Char, Length: 16},
		},
		{
			name: "signed integer widths", typeName: "Int8",
			want: CanonicalType{Kind: TinyInt},
		},
		{
			name: "unsigned integer widths", typeName: "UInt32",
			want: CanonicalType{Kind: Integer, Unsigned: true},
		},
		{
			name: "sixteen bit integer", typeName: "Int16",
			want: CanonicalType{Kind: SmallInt},
		},
		{
			name: "unsigned sixty four bit integer", typeName: "UInt64",
			want: CanonicalType{Kind: BigInt, Unsigned: true},
		},
		{
			name: "decimal precision and scale", typeName: "Decimal(30, 12)",
			want: CanonicalType{Kind: Decimal, Precision: 30, Scale: 12},
		},
		{
			name: "decimal alias", typeName: "Decimal256(20)",
			want: CanonicalType{Kind: Decimal, Precision: 76, Scale: 20},
		},
		{
			name: "datetime seconds", typeName: "DateTime",
			want: CanonicalType{Kind: Timestamp, Fsp: intp(0)},
		},
		{
			name: "datetime64 precision and zone", typeName: "DateTime64(9, 'UTC')",
			want: CanonicalType{Kind: Timestamp, Fsp: intp(9), WithTZ: true},
		},
		{
			name: "array recursively maps element", typeName: "Array(Nullable(UInt16))",
			want: CanonicalType{Kind: Array, Element: &CanonicalType{Kind: SmallInt, Unsigned: true}},
		},
		{
			name: "enum names are retained", typeName: "Enum8('a,b' = -1, 'it\\'s' = 2)",
			want: CanonicalType{Kind: Enum, EnumValues: []string{"a,b", "it's"}},
		},
		{
			name: "unrepresentable wide integer remains raw", typeName: "UInt128",
			want: CanonicalType{Kind: Raw, Raw: "UInt128"},
		},
		{
			name: "unsupported aggregate remains raw after wrapper stripping", typeName: "Nullable(AggregateFunction(sum, UInt64))",
			want: CanonicalType{Kind: Raw, Raw: "AggregateFunction(sum, UInt64)"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ToCanonical(tc.typeName, TypeMeta{}, "clickhouse"); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("ToCanonical(%q, clickhouse) = %#v, want %#v", tc.typeName, got, tc.want)
			}
		})
	}
}

func TestFromCanonicalClickHouse_Golden(t *testing.T) {
	intp := func(v int) *int { return &v }
	cases := []struct {
		name           string
		ct             CanonicalType
		want           string
		warningSubstrs []string
	}{
		{
			name: "integer widths and sign", ct: CanonicalType{Kind: TinyInt, Unsigned: true},
			want: "UInt8",
		},
		{
			name: "sixteen bit unsigned integer", ct: CanonicalType{Kind: SmallInt, Unsigned: true},
			want: "UInt16",
		},
		{
			name: "thirty two bit signed integer", ct: CanonicalType{Kind: Integer},
			want: "Int32",
		},
		{
			name: "sixty four bit unsigned integer", ct: CanonicalType{Kind: BigInt, Unsigned: true},
			want: "UInt64",
		},
		{
			name: "medium integer widens", ct: CanonicalType{Kind: MediumInt, Unsigned: true},
			want: "UInt32", warningSubstrs: []string{"no 24-bit integer"},
		},
		{
			name: "fixed string", ct: CanonicalType{Kind: Char, Length: 16},
			want: "FixedString(16)", warningSubstrs: []string{"byte-sized"},
		},
		{
			name: "bounded varchar becomes string", ct: CanonicalType{Kind: Varchar, Length: 255},
			want: "String", warningSubstrs: []string{"does not enforce"},
		},
		{
			name: "decimal precision and scale", ct: CanonicalType{Kind: Decimal, Precision: 30, Scale: 12},
			want: "Decimal(30, 12)",
		},
		{
			name: "decimal default is explicit", ct: CanonicalType{Kind: Decimal},
			want: "Decimal(38, 9)", warningSubstrs: []string{"no declared precision"},
		},
		{
			name: "date uses wider ClickHouse date", ct: CanonicalType{Kind: Date},
			want: "Date32", warningSubstrs: []string{"1900-2299"},
		},
		{
			name: "datetime seconds", ct: CanonicalType{Kind: Timestamp, Fsp: intp(0)},
			want: "DateTime",
		},
		{
			name: "datetime64 precision and zone", ct: CanonicalType{Kind: Timestamp, Fsp: intp(9), WithTZ: true},
			want: "DateTime64(9, 'UTC')", warningSubstrs: []string{"named-zone identity"},
		},
		{
			name: "unspecified timestamp precision has DMT compatible default", ct: CanonicalType{Kind: Timestamp},
			want: "DateTime64(3)", warningSubstrs: []string{"DateTime64(3) default"},
		},
		{
			name: "array maps supported elements", ct: CanonicalType{Kind: Array, Element: &CanonicalType{Kind: Integer, Unsigned: true}},
			want: "Array(UInt32)",
		},
		{
			name: "array without element is explicit fallback", ct: CanonicalType{Kind: Array},
			want: "Array(String)", warningSubstrs: []string{"element type is unavailable"},
		},
		{
			name: "enum preserves values with deterministic codes", ct: CanonicalType{Kind: Enum, EnumValues: []string{"active", "pending"}},
			want: "Enum8('active' = 1, 'pending' = 2)", warningSubstrs: []string{"assigned sequentially"},
		},
		{
			name: "enum without values degrades explicitly", ct: CanonicalType{Kind: Enum},
			want: "String", warningSubstrs: []string{"values are unavailable"},
		},
		{
			name: "json is version safe string", ct: CanonicalType{Kind: Json},
			want: "String", warningSubstrs: []string{"availability varies"},
		},
		{
			name: "time avoids experimental ClickHouse type", ct: CanonicalType{Kind: Time},
			want: "String", warningSubstrs: []string{"experimental"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, warnings, err := FromCanonicalWithWarnings(tc.ct, "clickhouse", RenderOpts{})
			if err != nil {
				t.Fatalf("FromCanonicalWithWarnings: %v", err)
			}
			if got != tc.want {
				t.Fatalf("rendered type = %q, want %q", got, tc.want)
			}
			for _, want := range tc.warningSubstrs {
				if !hasClickHouseWarning(warnings, want) {
					t.Errorf("warnings = %+v, missing %q", warnings, want)
				}
			}
			if len(tc.warningSubstrs) == 0 && len(warnings) != 0 {
				t.Errorf("warnings = %+v, want none", warnings)
			}
		})
	}
}

func TestFromCanonicalClickHouse_UnsupportedAndRawRemainExplicit(t *testing.T) {
	cases := []CanonicalType{
		{Kind: Raw, Raw: "AggregateFunction(sum, UInt64)"},
		{Kind: Decimal, Precision: 77, Scale: 2},
		{Kind: Array, Element: &CanonicalType{Kind: Raw, Raw: "Map(String, UInt64)"}},
	}
	for _, ct := range cases {
		_, err := FromCanonical(ct, "clickhouse", RenderOpts{})
		if !errors.Is(err, ErrUnknownType) {
			t.Errorf("FromCanonical(%#v, clickhouse) error = %v, want ErrUnknownType", ct, err)
		}
	}
}

func TestFromCanonicalClickHouse_AcceptsCaseAndPublicEntryPoint(t *testing.T) {
	ct := ToCanonical("LowCardinality(Nullable(DateTime64(6)))", TypeMeta{}, "CLICKHOUSE")
	got, err := FromCanonical(ct, "ClickHouse", RenderOpts{})
	if err != nil {
		t.Fatalf("FromCanonical: %v", err)
	}
	if got != "DateTime64(6)" {
		t.Fatalf("rendered type = %q, want DateTime64(6)", got)
	}
}

func hasClickHouseWarning(warnings []MappingWarning, want string) bool {
	for _, warning := range warnings {
		if warning.TargetDialect == "clickhouse" && strings.Contains(warning.Reason, want) {
			return true
		}
	}
	return false
}
