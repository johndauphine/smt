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
		// pg/mysql varchar/char carry National (unicode intent). The MSSQL
		// renderer honors it as NVARCHAR/NCHAR up to 4000 chars and falls back
		// to length-preserving codepage VARCHAR/CHAR above that (#224), so
		// exact length is never sacrificed for unicode.
		{"postgres varchar is national", "varchar", TypeMeta{MaxLength: 20}, "postgres", CanonicalType{Kind: Varchar, Length: 20, National: true}},
		{"mysql char is national", "char", TypeMeta{MaxLength: 10}, "mysql", CanonicalType{Kind: Char, Length: 10, National: true}},
		{"mssql nvarchar is national", "nvarchar", TypeMeta{MaxLength: 20}, "mssql", CanonicalType{Kind: Varchar, Length: 20, National: true}},
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
				got.UTCNormalized != tc.want.UTCNormalized || got.National != tc.want.National || got.Raw != tc.want.Raw ||
				got.SpatialType != tc.want.SpatialType || got.SpatialSubType != tc.want.SpatialSubType ||
				got.SRID != tc.want.SRID ||
				!eqFsp(got.Fsp, tc.want.Fsp) || !eqStrs(got.EnumValues, tc.want.EnumValues) {
				t.Errorf("ToCanonical(%q,%s) = %+v, want %+v", tc.typ, tc.dialect, got, tc.want)
			}
		})
	}
}

func TestMinorFidelityMappings(t *testing.T) {
	cases := []struct {
		name   string
		source string
		typ    string
		meta   TypeMeta
		target string
		want   string
	}{
		{"mssql binary stays fixed", "mssql", "binary", TypeMeta{MaxLength: 16}, "mssql", "BINARY(16)"},
		{"mysql binary stays fixed", "mysql", "binary", TypeMeta{MaxLength: 16}, "mysql", "BINARY(16)"},
		{"mssql varbinary stays variable", "mssql", "varbinary", TypeMeta{MaxLength: 16}, "mssql", "VARBINARY(16)"},
		{"mysql varbinary stays variable", "mysql", "varbinary", TypeMeta{MaxLength: 16}, "mysql", "VARBINARY(16)"},
		{"postgres varchar to mssql is unicode under 4000", "postgres", "varchar", TypeMeta{MaxLength: 50}, "mssql", "NVARCHAR(50)"},
		{"mysql varchar to mssql is unicode under 4000", "mysql", "varchar", TypeMeta{MaxLength: 50}, "mssql", "NVARCHAR(50)"},
		{"postgres varchar to mssql over 4000 keeps length as codepage", "postgres", "varchar", TypeMeta{MaxLength: 8000}, "mssql", "VARCHAR(8000)"},
		{"postgres char to mssql over 4000 keeps length as codepage", "postgres", "char", TypeMeta{MaxLength: 5000}, "mssql", "CHAR(5000)"},
		{"mssql varchar stays codepage", "mssql", "varchar", TypeMeta{MaxLength: 50}, "mssql", "VARCHAR(50)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := FromCanonical(ToCanonical(tc.typ, tc.meta, tc.source), tc.target, RenderOpts{})
			if err != nil {
				t.Fatalf("FromCanonical: %v", err)
			}
			if got != tc.want {
				t.Fatalf("rendered type = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPostgresArrayRoundTripPreservesElementType(t *testing.T) {
	cases := []struct {
		name     string
		typ      string
		meta     TypeMeta
		wantElem CanonicalType
		wantPG   string
	}{
		{"bigint udt", "_int8", TypeMeta{}, CanonicalType{Kind: BigInt}, "bigint[]"},
		{"smallint udt", "_int2", TypeMeta{}, CanonicalType{Kind: SmallInt}, "smallint[]"},
		{"varchar udt length", "_varchar", TypeMeta{MaxLength: 20}, CanonicalType{Kind: Varchar, Length: 20}, "character varying(20)[]"},
		{"varchar spelled length", "varchar[]", TypeMeta{MaxLength: 20}, CanonicalType{Kind: Varchar, Length: 20}, "character varying(20)[]"},
		{"uuid udt", "_uuid", TypeMeta{}, CanonicalType{Kind: Uuid}, "uuid[]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ToCanonical(tc.typ, tc.meta, "postgres")
			if got.Kind != Array {
				t.Fatalf("ToCanonical(%q).Kind = %v, want Array", tc.typ, got.Kind)
			}
			if got.Element == nil {
				t.Fatalf("ToCanonical(%q).Element = nil", tc.typ)
			}
			if got.Element.Kind != tc.wantElem.Kind || got.Element.Length != tc.wantElem.Length {
				t.Fatalf("array element = %+v, want %+v", *got.Element, tc.wantElem)
			}
			rendered, err := FromCanonical(got, "postgres", RenderOpts{})
			if err != nil {
				t.Fatalf("FromCanonical: %v", err)
			}
			if rendered != tc.wantPG {
				t.Fatalf("rendered array = %q, want %q", rendered, tc.wantPG)
			}
		})
	}
}

func TestBitStringRoundTripPreservesWidth(t *testing.T) {
	cases := []struct {
		name       string
		typ        string
		meta       TypeMeta
		source     string
		wantKind   Kind
		wantLength int
		target     string
		wantType   string
	}{
		{"mssql bit is boolean", "bit", TypeMeta{}, "mssql", Boolean, 0, "postgres", "boolean"},
		{"mysql bit one is boolean", "bit", TypeMeta{Precision: 1}, "mysql", Boolean, 0, "mysql", "TINYINT(1)"},
		{"mysql bit eight stays bit string", "bit", TypeMeta{Precision: 8}, "mysql", BitString, 8, "mysql", "BIT(8)"},
		{"mysql bit width in type name", "bit(8)", TypeMeta{}, "mysql", BitString, 8, "mysql", "BIT(8)"},
		{"postgres bit three stays bit string", "bit", TypeMeta{MaxLength: 3}, "postgres", BitString, 3, "postgres", "bit(3)"},
		{"postgres varbit keeps bound", "varbit", TypeMeta{MaxLength: 12}, "postgres", VarBitString, 12, "postgres", "bit varying(12)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ct := ToCanonical(tc.typ, tc.meta, tc.source)
			if ct.Kind != tc.wantKind {
				t.Fatalf("kind = %v, want %v", ct.Kind, tc.wantKind)
			}
			if ct.Length != tc.wantLength {
				t.Fatalf("length = %d, want %d", ct.Length, tc.wantLength)
			}
			rendered, err := FromCanonical(ct, tc.target, RenderOpts{})
			if err != nil {
				t.Fatalf("FromCanonical: %v", err)
			}
			if rendered != tc.wantType {
				t.Fatalf("rendered type = %q, want %q", rendered, tc.wantType)
			}
		})
	}
}

func TestNumericFidelity(t *testing.T) {
	unsignedDecimal := ToCanonical("decimal", TypeMeta{Precision: 10, Scale: 2, IsUnsigned: true}, "mysql")
	if unsignedDecimal.Kind != Decimal || !unsignedDecimal.Unsigned {
		t.Fatalf("unsigned decimal canonical = %+v, want unsigned decimal", unsignedDecimal)
	}
	got, err := FromCanonical(unsignedDecimal, "mysql", RenderOpts{})
	if err != nil {
		t.Fatalf("FromCanonical unsigned decimal: %v", err)
	}
	if got != "DECIMAL(10,2) UNSIGNED" {
		t.Fatalf("unsigned decimal rendered as %q", got)
	}

	for _, tc := range []struct {
		name string
		ct   CanonicalType
		want string
	}{
		{"unsigned float", ToCanonical("float", TypeMeta{IsUnsigned: true}, "mysql"), "FLOAT UNSIGNED"},
		{"unsigned double", ToCanonical("double", TypeMeta{IsUnsigned: true}, "mysql"), "DOUBLE UNSIGNED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := FromCanonical(tc.ct, "mysql", RenderOpts{})
			if err != nil {
				t.Fatalf("FromCanonical: %v", err)
			}
			if got != tc.want {
				t.Fatalf("rendered type = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNumericTargetLimitsWarnAndClamp(t *testing.T) {
	cases := []struct {
		name       string
		ct         CanonicalType
		target     string
		wantType   string
		wantReason string
	}{
		{"bare numeric to mysql", CanonicalType{Kind: Decimal}, "mysql", "DECIMAL(65,30)", "unconstrained"},
		{"bare numeric to mssql", CanonicalType{Kind: Decimal}, "mssql", "DECIMAL(38,18)", "unconstrained"},
		{"over precision to mssql", CanonicalType{Kind: Decimal, Precision: 50, Scale: 10}, "mssql", "DECIMAL(38,10)", "precision 50"},
		{"over precision scale to mysql", CanonicalType{Kind: Decimal, Precision: 70, Scale: 40}, "mysql", "DECIMAL(65,30)", "precision 70"},
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
			if !strings.Contains(warnings[0].Reason, tc.wantReason) {
				t.Fatalf("warning reason = %q, want substring %q", warnings[0].Reason, tc.wantReason)
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
		{"tz-aware timestamp to mysql", CanonicalType{Kind: Timestamp, WithTZ: true}, "mysql", "TIMESTAMP(6)", "1970-2038"},
		{"tz-aware time to mssql", CanonicalType{Kind: Time, WithTZ: true}, "mssql", "TIME", "time-zone-aware"},
		{"mysql timestamp to pg", CanonicalType{Kind: Timestamp, UTCNormalized: true}, "postgres", "timestamp without time zone", "UTC-normalization"},
		{"fsp clamp", CanonicalType{Kind: Timestamp, Fsp: &fsp7}, "postgres", "timestamp(6) without time zone", "clamped"},
		{"mysql text tier to pg", CanonicalType{Kind: Text, Length: baseCap}, "postgres", "text", "LOB capacity tier"},
		{"array to mysql", CanonicalType{Kind: Array, Element: &CanonicalType{Kind: BigInt}}, "mysql", "JSON", "no native array"},
		{"array element warning", CanonicalType{Kind: Array, Element: &CanonicalType{Kind: TinyInt}}, "postgres", "smallint[]", "array element"},
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
