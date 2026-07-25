package canonical

import (
	"fmt"
	"reflect"
	"testing"
)

// The baseline is deliberately representative rather than exhaustive. It
// covers each currently supported source dialect and the metadata that changes
// canonical meaning: width, sign, length, precision/scale, timezone,
// MySQL LOB tiers, enum values, and unportable source types.
//
// Keep the count in sync when intentionally extending this contract. The
// separate count guard makes an accidental loss of a dialect/type pair clear
// in code review.
const sourceCanonicalGoldenBaselineSize = 25

func TestToCanonical_SourceGolden(t *testing.T) {
	intp := func(v int) *int { return &v }
	cases := []struct {
		name     string
		source   string
		typeName string
		meta     TypeMeta
		want     CanonicalType
	}{
		// SQL Server
		{
			name: "mssql bit", source: "mssql", typeName: "bit",
			want: CanonicalType{Kind: Boolean},
		},
		{
			name: "mssql tinyint", source: "mssql", typeName: "tinyint",
			want: CanonicalType{Kind: TinyInt},
		},
		{
			name: "mssql decimal precision and scale", source: "mssql", typeName: "decimal",
			meta: TypeMeta{Precision: 18, Scale: 4},
			want: CanonicalType{Kind: Decimal, Precision: 18, Scale: 4},
		},
		{
			name: "mssql nvarchar max", source: "mssql", typeName: "nvarchar",
			meta: TypeMeta{MaxLength: -1},
			want: CanonicalType{Kind: Varchar, Length: -1, National: true},
		},
		{
			name: "mssql varbinary max", source: "mssql", typeName: "varbinary",
			meta: TypeMeta{MaxLength: -1},
			want: CanonicalType{Kind: VarBinary, Length: -1},
		},
		{
			name: "mssql datetimeoffset", source: "mssql", typeName: "datetimeoffset",
			meta: TypeMeta{DatetimePrecision: intp(7)},
			want: CanonicalType{Kind: Timestamp, WithTZ: true, Fsp: intp(7)},
		},
		{
			name: "mssql uniqueidentifier", source: "mssql", typeName: "uniqueidentifier",
			want: CanonicalType{Kind: Uuid},
		},
		{
			name: "mssql vendor type remains raw", source: "mssql", typeName: "hierarchyid",
			want: CanonicalType{Kind: Raw, Raw: "hierarchyid"},
		},

		// PostgreSQL
		{
			name: "postgres bool", source: "postgres", typeName: "bool",
			want: CanonicalType{Kind: Boolean},
		},
		{
			name: "postgres int2", source: "postgres", typeName: "int2",
			want: CanonicalType{Kind: SmallInt},
		},
		{
			name: "postgres int4", source: "postgres", typeName: "int4",
			want: CanonicalType{Kind: Integer},
		},
		{
			name: "postgres int8", source: "postgres", typeName: "int8",
			want: CanonicalType{Kind: BigInt},
		},
		{
			name: "postgres numeric precision and scale", source: "postgres", typeName: "numeric",
			meta: TypeMeta{Precision: 18, Scale: 4},
			want: CanonicalType{Kind: Decimal, Precision: 18, Scale: 4},
		},
		{
			name: "postgres character varying length", source: "postgres", typeName: "character varying",
			meta: TypeMeta{MaxLength: 120},
			want: CanonicalType{Kind: Varchar, Length: 120, National: true},
		},
		{
			name: "postgres bytea", source: "postgres", typeName: "bytea",
			want: CanonicalType{Kind: Blob},
		},
		{
			name: "postgres timestamptz", source: "postgres", typeName: "timestamptz",
			meta: TypeMeta{DatetimePrecision: intp(6)},
			want: CanonicalType{Kind: Timestamp, WithTZ: true, Fsp: intp(6)},
		},
		{
			name: "postgres uuid array", source: "postgres", typeName: "_uuid",
			want: CanonicalType{Kind: Array, Element: &CanonicalType{Kind: Uuid}},
		},

		// MySQL / MariaDB
		{
			name: "mysql tinyint display width one", source: "mysql", typeName: "tinyint",
			meta: TypeMeta{DisplayWidth: 1},
			want: CanonicalType{Kind: Boolean},
		},
		{
			name: "mysql plain tinyint", source: "mysql", typeName: "tinyint",
			want: CanonicalType{Kind: TinyInt},
		},
		{
			name: "mysql mediumint unsigned", source: "mysql", typeName: "mediumint",
			meta: TypeMeta{IsUnsigned: true},
			want: CanonicalType{Kind: MediumInt, Unsigned: true},
		},
		{
			name: "mysql int unsigned", source: "mysql", typeName: "int",
			meta: TypeMeta{IsUnsigned: true},
			want: CanonicalType{Kind: Integer, Unsigned: true},
		},
		{
			name: "mysql bigint unsigned", source: "mysql", typeName: "bigint",
			meta: TypeMeta{IsUnsigned: true},
			want: CanonicalType{Kind: BigInt, Unsigned: true},
		},
		{
			name: "mysql enum values", source: "mysql", typeName: "enum",
			meta: TypeMeta{MaxLength: 8, EnumValues: []string{"active", "pending"}},
			want: CanonicalType{Kind: Enum, Length: 8, EnumValues: []string{"active", "pending"}},
		},
		{
			name: "mysql mediumtext tier", source: "mysql", typeName: "mediumtext",
			want: CanonicalType{Kind: Text, Length: mediumCap},
		},
		{
			name: "mysql timestamp is utc normalized", source: "mysql", typeName: "timestamp",
			meta: TypeMeta{DatetimePrecision: intp(6)},
			want: CanonicalType{Kind: Timestamp, Fsp: intp(6), UTCNormalized: true},
		},
	}

	if len(cases) != sourceCanonicalGoldenBaselineSize {
		t.Fatalf("golden baseline has %d cases, want %d", len(cases), sourceCanonicalGoldenBaselineSize)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ToCanonical(tc.typeName, tc.meta, tc.source); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("ToCanonical(%q, %#v, %q) = %#v, want %#v", tc.typeName, tc.meta, tc.source, got, tc.want)
			}
		})
	}
}

// ClickHouse remains intentionally unsupported. ToCanonical is permissive for
// shared type spellings and is not a supported-dialect registry; FromCanonical
// is the target capability boundary. Add a dedicated dialect mapper and change
// this expectation with its own golden suite when ClickHouse becomes supported.
func TestFromCanonical_UnsupportedClickHouseTarget(t *testing.T) {
	_, err := FromCanonical(CanonicalType{Kind: Integer}, "clickhouse", RenderOpts{})
	if err == nil {
		t.Fatal("FromCanonical unexpectedly supports ClickHouse")
	}
	want := fmt.Sprintf("FromCanonical: unsupported target dialect %q", "clickhouse")
	if err.Error() != want {
		t.Fatalf("FromCanonical error = %q, want %q", err, want)
	}
}
