package canonical

import (
	"strings"
	"testing"
)

func TestToCanonical_Core(t *testing.T) {
	fsp6 := 6
	cases := []struct {
		name    string
		typ     string
		meta    TypeMeta
		dialect string
		want    CanonicalType
	}{
		{"mysql tinyint(1) is boolean", "tinyint", TypeMeta{DisplayWidth: 1}, "mysql", CanonicalType{Kind: Boolean}},
		{"plain tinyint is int", "tinyint", TypeMeta{}, "mysql", CanonicalType{Kind: TinyInt}},
		{"mssql bit is boolean", "bit", TypeMeta{}, "mssql", CanonicalType{Kind: Boolean}},
		{"int unsigned carries flag", "int", TypeMeta{IsUnsigned: true}, "mysql", CanonicalType{Kind: Integer, Unsigned: true}},
		{"mediumint is its own kind", "mediumint", TypeMeta{}, "mysql", CanonicalType{Kind: MediumInt}},
		{"varchar carries length", "varchar", TypeMeta{MaxLength: 20}, "mssql", CanonicalType{Kind: Varchar, Length: 20}},
		{"decimal carries p/s", "decimal", TypeMeta{Precision: 18, Scale: 4}, "postgres", CanonicalType{Kind: Decimal, Precision: 18, Scale: 4}},
		{"money is decimal(19,4)", "money", TypeMeta{}, "mssql", CanonicalType{Kind: Decimal, Precision: 19, Scale: 4}},
		{"mysql text is base tier", "text", TypeMeta{}, "mysql", CanonicalType{Kind: Text, Length: baseCap}},
		{"pg text is unbounded", "text", TypeMeta{}, "postgres", CanonicalType{Kind: Text}},
		{"mysql longtext tier", "longtext", TypeMeta{}, "mysql", CanonicalType{Kind: Text, Length: longCap}},
		{"mysql timestamp is UTC-normalized", "timestamp", TypeMeta{DatetimePrecision: &fsp6}, "mysql", CanonicalType{Kind: Timestamp, Fsp: &fsp6, UTCNormalized: true}},
		{"pg timestamp is naive", "timestamp", TypeMeta{}, "postgres", CanonicalType{Kind: Timestamp}},
		{"datetimeoffset is tz-aware", "datetimeoffset", TypeMeta{}, "mssql", CanonicalType{Kind: Timestamp, WithTZ: true}},
		{"uniqueidentifier is uuid", "uniqueidentifier", TypeMeta{}, "mssql", CanonicalType{Kind: Uuid}},
		{"enum carries values", "enum", TypeMeta{EnumValues: []string{"a", "b"}}, "mysql", CanonicalType{Kind: Enum, EnumValues: []string{"a", "b"}}},
		{"mssql geography is spatial", "geography", TypeMeta{SRID: 4326}, "mssql", CanonicalType{Kind: Spatial, SpatialType: "geography", SRID: 4326}},
		{"mysql point is spatial subtype", "point", TypeMeta{SRID: 3857}, "mysql", CanonicalType{Kind: Spatial, SpatialType: "geometry", SpatialSubType: "point", SRID: 3857}},
		{"postgis geometry carries subtype and srid", "geometry(Point,4326)", TypeMeta{}, "postgres", CanonicalType{Kind: Spatial, SpatialType: "geometry", SpatialSubType: "point", SRID: 4326}},
		{"postgis metadata supplies subtype", "geometry", TypeMeta{SpatialSubType: "Polygon", SRID: 3857}, "postgres", CanonicalType{Kind: Spatial, SpatialType: "geometry", SpatialSubType: "polygon", SRID: 3857}},
		{"postgres built-in point is not postgis", "point", TypeMeta{}, "postgres", CanonicalType{Kind: Raw, Raw: "point"}},
		{"unknown is raw", "hierarchyid", TypeMeta{}, "mssql", CanonicalType{Kind: Raw, Raw: "hierarchyid"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ToCanonical(tc.typ, tc.meta, tc.dialect)
			if got.Kind != tc.want.Kind || got.Length != tc.want.Length ||
				got.Precision != tc.want.Precision || got.Scale != tc.want.Scale ||
				got.WithTZ != tc.want.WithTZ || got.Unsigned != tc.want.Unsigned ||
				got.UTCNormalized != tc.want.UTCNormalized || got.Raw != tc.want.Raw ||
				got.SpatialType != tc.want.SpatialType || got.SpatialSubType != tc.want.SpatialSubType ||
				got.SRID != tc.want.SRID ||
				!eqFsp(got.Fsp, tc.want.Fsp) || !eqStrs(got.EnumValues, tc.want.EnumValues) {
				t.Errorf("ToCanonical(%q,%s) = %+v, want %+v", tc.typ, tc.dialect, got, tc.want)
			}
		})
	}
}

func TestFromCanonical_Spatial(t *testing.T) {
	ct := CanonicalType{Kind: Spatial, SpatialType: "geometry", SpatialSubType: "point", SRID: 4326}
	cases := []struct {
		target string
		want   string
	}{
		{"postgres", "geometry(Point,4326)"},
		{"mssql", "GEOMETRY"},
		{"mysql", "POINT SRID 4326"},
	}
	for _, tc := range cases {
		t.Run(tc.target, func(t *testing.T) {
			got, err := FromCanonical(ct, tc.target, RenderOpts{})
			if err != nil {
				t.Fatalf("FromCanonical: %v", err)
			}
			if got != tc.want {
				t.Errorf("FromCanonical spatial to %s = %q, want %q", tc.target, got, tc.want)
			}
		})
	}
}

func TestFromCanonicalWithWarnings_Lossy(t *testing.T) {
	fsp7 := 7
	cases := []struct {
		name       string
		ct         CanonicalType
		target     string
		wantType   string
		wantReason string
	}{
		{"unsigned bigint to pg decimal", CanonicalType{Kind: BigInt, Unsigned: true}, "postgres", "numeric(20,0)", "unsigned 64-bit"},
		{"unsigned integer widening", CanonicalType{Kind: Integer, Unsigned: true}, "postgres", "bigint", "unsigned integer flag"},
		{"mediumint widening", CanonicalType{Kind: MediumInt}, "mssql", "INT", "24-bit"},
		{"tinyint widening", CanonicalType{Kind: TinyInt}, "postgres", "smallint", "8-bit"},
		{"tz-aware timestamp to mysql", CanonicalType{Kind: Timestamp, WithTZ: true}, "mysql", "DATETIME(6)", "time-zone-aware"},
		{"mysql timestamp to pg", CanonicalType{Kind: Timestamp, UTCNormalized: true}, "postgres", "timestamp without time zone", "UTC-normalization"},
		{"fsp clamp", CanonicalType{Kind: Timestamp, Fsp: &fsp7}, "postgres", "timestamp(6) without time zone", "clamped"},
		{"mysql text tier to pg", CanonicalType{Kind: Text, Length: baseCap}, "postgres", "text", "LOB capacity tier"},
		{"postgis dependency", CanonicalType{Kind: Spatial, SpatialType: "geometry"}, "postgres", "geometry", "PostGIS"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, warnings, err := FromCanonicalWithWarnings(tc.ct, tc.target, RenderOpts{})
			if err != nil {
				t.Fatalf("FromCanonicalWithWarnings: %v", err)
			}
			if got != tc.wantType {
				t.Fatalf("rendered type = %q, want %q", got, tc.wantType)
			}
			if len(warnings) == 0 {
				t.Fatalf("expected warning, got none")
			}
			if warnings[0].Kind == "" || warnings[0].TargetDialect != tc.target {
				t.Fatalf("warning metadata = %#v", warnings[0])
			}
			if !strings.Contains(warnings[0].Reason, tc.wantReason) {
				t.Fatalf("warning reason = %q, want substring %q", warnings[0].Reason, tc.wantReason)
			}
		})
	}
}

func eqFsp(a, b *int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
func eqStrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
