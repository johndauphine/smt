package canonical

import "testing"

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
		{"unknown is raw", "geography", TypeMeta{}, "mssql", CanonicalType{Kind: Raw, Raw: "geography"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ToCanonical(tc.typ, tc.meta, tc.dialect)
			if got.Kind != tc.want.Kind || got.Length != tc.want.Length ||
				got.Precision != tc.want.Precision || got.Scale != tc.want.Scale ||
				got.WithTZ != tc.want.WithTZ || got.Unsigned != tc.want.Unsigned ||
				got.UTCNormalized != tc.want.UTCNormalized || got.Raw != tc.want.Raw ||
				!eqFsp(got.Fsp, tc.want.Fsp) || !eqStrs(got.EnumValues, tc.want.EnumValues) {
				t.Errorf("ToCanonical(%q,%s) = %+v, want %+v", tc.typ, tc.dialect, got, tc.want)
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
