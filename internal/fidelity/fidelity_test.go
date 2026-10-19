package fidelity

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/smt/internal/driver"
	"github.com/johndauphine/smt/schema/canonical"
)

func precision(n int) *int { return &n }

func assess(t *testing.T, column driver.Column) Assessment {
	t.Helper()
	a, err := AssessColumn(column, Object{Schema: "dbo", Table: "Types"}, "mssql", "postgres")
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func codes(a Assessment) []string {
	var result []string
	for _, finding := range a.Findings {
		result = append(result, finding.Code)
	}
	return result
}

func TestProfilesAndStrictGate(t *testing.T) {
	for input, want := range map[string]Profile{"": Legacy, "legacy": Legacy, " Checked ": Checked, "STRICT": Strict} {
		got, err := ParseProfile(input)
		if err != nil || got != want {
			t.Errorf("ParseProfile(%q) = %q, %v", input, got, err)
		}
	}
	if _, err := ParseProfile("equivalent"); err == nil {
		t.Fatal("unknown profile accepted")
	}
	a := assess(t, driver.Column{Name: "Clock", DataType: "time", DatetimePrecision: precision(6)})
	for _, profile := range []Profile{"", Legacy, Checked} {
		if err := Gate(profile, a); err != nil {
			t.Fatal(err)
		}
	}
	var violation *ViolationError
	if err := Gate(Strict, a); !errors.As(err, &violation) || len(violation.Findings) != 1 || violation.Findings[0].Code != TimeInputConversion {
		t.Fatalf("strict must reject known unresolved conversion: %#v, %v", violation, err)
	}
	if err := Gate(Strict, assess(t, driver.Column{Name: "Code", DataType: "tinyint"})); err != nil {
		t.Fatal(err)
	}
}

func TestTinyIntUsesSourceDialectWithoutChangingCanonicalMapping(t *testing.T) {
	column := driver.Column{Name: "Byte", DataType: "tinyint", IsNullable: true, DisplayWidth: 1}
	before := canonical.ToCanonical(column.DataType, driver.MetaOf(column), "mssql")
	a := assess(t, column)
	if len(a.Requirements) != 1 || len(a.Findings) != 0 {
		t.Fatalf("assessment: %#v", a)
	}
	r := a.Requirements[0]
	if r.Code != TinyIntRange || r.Min != 0 || r.Max != 255 || r.Object.Column != "Byte" {
		t.Fatalf("incorrect SQL Server unsigned restriction: %#v", r)
	}
	after := canonical.ToCanonical(column.DataType, driver.MetaOf(column), "mssql")
	if !reflect.DeepEqual(before, after) || before.Kind != canonical.TinyInt || before.Unsigned {
		t.Fatalf("legacy canonical result changed: %#v -> %#v", before, after)
	}
	for _, source := range []string{"mysql", "mariadb", "postgres"} {
		a, err := AssessColumn(column, Object{}, source, "postgres")
		var unsupported *UnsupportedError
		if !errors.As(err, &unsupported) || len(a.Requirements) != 0 {
			t.Errorf("unsupported source %q received SQL Server restriction: %#v, %v", source, a, err)
		}
	}
	if ct := canonical.ToCanonical("tinyint", driver.MetaOf(column), "mysql"); ct.Kind != canonical.Boolean {
		t.Fatalf("MySQL boolean convention changed: %#v", ct)
	}
	column.DisplayWidth = 0
	if ct := canonical.ToCanonical("tinyint", driver.MetaOf(column), "mysql"); ct.Kind != canonical.TinyInt || ct.Unsigned {
		t.Fatalf("MySQL signed tinyint changed: %#v", ct)
	}
}

func TestBinaryRequirementsNeverConfusePaddingWithLength(t *testing.T) {
	for _, length := range []int{1, 16, 8000} {
		a := assess(t, driver.Column{Name: "Bytes", DataType: "varbinary", MaxLength: length})
		if len(a.Requirements) != 1 || a.Requirements[0].Code != VarBinaryLength || a.Requirements[0].MaxBytes != length {
			t.Fatalf("length %d: %#v", length, a)
		}
		if !reflect.DeepEqual(codes(a), []string{BinaryInputConversion}) || Gate(Strict, a) == nil {
			t.Fatalf("conversion loss must remain visible after stored byte limit: %#v", a)
		}
	}
	for _, tc := range []struct {
		dataType string
		length   int
		want     []string
	}{
		{"varbinary", -1, []string{BinaryInputConversion}},
		{"varbinary", 0, []string{BinaryInputConversion, SourceMetadataMissing}},
		{"varbinary", 8001, []string{BinaryInputConversion, SourceMetadataMissing}},
		{"binary", 16, []string{FixedBinaryPadding}},
		{"image", 0, []string{UnassessedType}},
	} {
		a := assess(t, driver.Column{Name: "Bytes", DataType: tc.dataType, MaxLength: tc.length})
		if len(a.Requirements) != 0 || !reflect.DeepEqual(codes(a), tc.want) {
			t.Errorf("%s(%d): %#v", tc.dataType, tc.length, a)
		}
	}
}

func TestBinaryDefaultAssessmentNeverAssumesArbitraryConversion(t *testing.T) {
	for _, tc := range []struct {
		defaultExpr string
		unsupported bool
	}{
		{"(0xAB)", false}, {"((NULL))", false},
		{"CONVERT(varbinary(16), 'abc')", true}, {"newid()", true},
		{"0xA", false}, {"0x", false}, {"0xAG", true}, {"", true},
	} {
		a := assess(t, driver.Column{Name: "Bytes", DataType: "varbinary", MaxLength: 16, HasDefault: true, DefaultExpression: tc.defaultExpr})
		found := false
		for _, f := range a.Findings {
			if f.Code == BinaryDefaultSemantics {
				found = true
			}
		}
		if found != tc.unsupported {
			t.Errorf("default %q findings: %#v", tc.defaultExpr, a.Findings)
		}
	}
}

func TestKnownSemanticLossesAndMissingMetadata(t *testing.T) {
	for _, tc := range []struct {
		column driver.Column
		want   []string
	}{
		{driver.Column{DataType: "datetime2", DatetimePrecision: precision(7)}, []string{TemporalPrecision, TemporalRange}},
		{driver.Column{DataType: "datetime2", DatetimePrecision: precision(6)}, []string{TemporalRange}},
		{driver.Column{DataType: "datetime2"}, []string{SourceMetadataMissing, TemporalRange}},
		{driver.Column{DataType: "datetime2", DatetimePrecision: precision(9)}, []string{SourceMetadataMissing, TemporalRange}},
		{driver.Column{DataType: "datetimeoffset", DatetimePrecision: precision(7)}, []string{OriginalOffset, TemporalPrecision, TemporalRange}},
		{driver.Column{DataType: "datetimeoffset", DatetimePrecision: precision(0)}, []string{OriginalOffset, TemporalRange}},
		{driver.Column{DataType: "datetime"}, []string{LegacyDateTime}},
		{driver.Column{DataType: "smalldatetime"}, []string{LegacyDateTime}},
		{driver.Column{DataType: "nvarchar", MaxLength: 4}, []string{TextComparison, UTF16Length}},
		{driver.Column{DataType: "nchar", MaxLength: 4}, []string{TextComparison, UTF16Length}},
		{driver.Column{DataType: "nvarchar", MaxLength: -1}, []string{TextComparison}},
		{driver.Column{DataType: "varchar", MaxLength: 4}, []string{TextComparison}},
		{driver.Column{DataType: "ntext"}, []string{TextComparison}},
		{driver.Column{DataType: "time", DatetimePrecision: precision(6)}, []string{TimeInputConversion}},
		{driver.Column{DataType: "timestamp"}, []string{RowVersionSemantics}},
		{driver.Column{DataType: "rowversion"}, []string{RowVersionSemantics}},
		{driver.Column{DataType: "date"}, []string{TemporalRange}},
		{driver.Column{DataType: "int", IsIdentity: true}, []string{IdentitySemantics}},
		{driver.Column{DataType: "int", IsComputed: true}, []string{ComputedSemantics}},
		{driver.Column{DataType: "uniqueidentifier"}, []string{UnassessedType}},
		{driver.Column{DataType: "bit"}, []string{UnassessedType}},
		{driver.Column{DataType: "float"}, []string{UnassessedType}},
		{driver.Column{DataType: "xml"}, []string{UnassessedType}},
		{driver.Column{DataType: "custom_udt"}, []string{UnassessedType}},
	} {
		t.Run(tc.column.DataType, func(t *testing.T) {
			tc.column.Name = "Value"
			a := assess(t, tc.column)
			if !reflect.DeepEqual(codes(a), tc.want) || len(a.Requirements) != 0 {
				t.Fatalf("got %#v, want findings %v and no unsafe restriction", a, tc.want)
			}
			if len(tc.want) > 0 && Gate(Strict, a) == nil {
				t.Fatal("strict accepted unresolved feature")
			}
		})
	}
}

func TestMissingContextAndUnsupportedPairs(t *testing.T) {
	a, err := AssessColumn(driver.Column{Name: "Byte", DataType: "tinyint"}, Object{Table: "T"}, "", "pg")
	if err != nil || !reflect.DeepEqual(codes(a), []string{SourceContextMissing}) || len(a.Requirements) != 0 || Gate(Strict, a) == nil {
		t.Fatalf("missing source context: %#v, %v", a, err)
	}
	for _, target := range []string{"mysql", "mssql", "sqlite", "clickhouse", "custom"} {
		_, err := AssessTable(&driver.Table{}, "mssql", target)
		var unsupported *UnsupportedError
		if !errors.As(err, &unsupported) {
			t.Errorf("target %q did not fail explicitly: %v", target, err)
		}
	}
	if _, err := AssessTable(nil, "mssql", "postgres"); err == nil {
		t.Fatal("nil table accepted")
	}
	a, err = AssessTable(&driver.Table{Name: "Empty"}, "", "postgres")
	if err != nil || !reflect.DeepEqual(codes(a), []string{SourceContextMissing}) || Gate(Strict, a) == nil {
		t.Fatalf("empty table concealed missing source context: %#v, %v", a, err)
	}
}

func TestTableAssessmentHasStableOrderingAndOriginalContext(t *testing.T) {
	table := &driver.Table{Schema: "dbo", Name: "Types", Columns: []driver.Column{
		{Name: "Z", DataType: "tinyint"}, {Name: "A", DataType: "datetimeoffset", DatetimePrecision: precision(7)}, {Name: "B", DataType: "tinyint"},
	}}
	a, err := AssessTable(table, "SQL_SERVER", "PostgreSQL")
	if err != nil {
		t.Fatal(err)
	}
	if a.Requirements[0].Object.Column != "B" || a.Requirements[1].Object.Column != "Z" || a.Findings[0].Object != (Object{Schema: "dbo", Table: "Types", Column: "A"}) {
		t.Fatalf("unstable order/context: %#v", a)
	}
	table.Columns[0], table.Columns[2] = table.Columns[2], table.Columns[0]
	b, err := AssessTable(table, "mssql", "postgres")
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatalf("column discovery order changed findings: %#v, %v", b, err)
	}
}

func TestNullableUniqueIndexAssessmentAndStrictGate(t *testing.T) {
	for _, tc := range []struct {
		name  string
		index driver.Index
		want  []string
	}{
		{"nullable unique key", driver.Index{Name: "uq_nullable", Columns: []string{"Nullable"}, IsUnique: true}, []string{NullableUniqueSemantics}},
		{"composite nullable key", driver.Index{Name: "uq_composite", Columns: []string{"Required", "Nullable"}, IsUnique: true}, []string{NullableUniqueSemantics}},
		{"filtered nullable key", driver.Index{Name: "uq_filtered", Columns: []string{"Nullable"}, IsUnique: true, Filter: "[Nullable] IS NOT NULL"}, []string{NullableUniqueSemantics}},
		{"nonnullable unique key", driver.Index{Name: "uq_required", Columns: []string{"Required"}, IsUnique: true}, nil},
		{"nullable include only", driver.Index{Name: "uq_include", Columns: []string{"Required"}, IncludeCols: []string{"Nullable"}, IsUnique: true}, nil},
		{"ordinary nullable key", driver.Index{Name: "ix_nullable", Columns: []string{"Nullable"}}, nil},
		{"missing key metadata", driver.Index{Name: "uq_missing", Columns: []string{"Unknown"}, IsUnique: true}, []string{SourceMetadataMissing}},
		{"expression key metadata", driver.Index{Name: "uq_expression", Columns: []string{"Required"}, ColumnExpressions: []bool{true}, IsUnique: true}, []string{SourceMetadataMissing}},
		{"empty key metadata", driver.Index{Name: "uq_empty", IsUnique: true}, []string{SourceMetadataMissing}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := &driver.Table{Schema: "dbo", Name: "T", Columns: []driver.Column{{Name: "Nullable", DataType: "int", IsNullable: true}, {Name: "Required", DataType: "int"}}, Indexes: []driver.Index{tc.index}}
			a, err := AssessTable(table, "mssql", "postgres")
			if err != nil || !reflect.DeepEqual(codes(a), tc.want) {
				t.Fatalf("assessment %#v, %v, want %v", a, err, tc.want)
			}
			if err := Gate(Checked, a); err != nil {
				t.Fatal(err)
			}
			if (Gate(Strict, a) != nil) != (len(tc.want) > 0) {
				t.Fatalf("strict result disagrees with known/missing unique semantics: %#v", a)
			}
			for _, finding := range a.Findings {
				if finding.Object != (Object{Schema: "dbo", Table: "T"}) || !strings.Contains(finding.Reason, tc.index.Name) {
					t.Fatalf("table/index context missing: %#v", finding)
				}
			}
		})
	}
}

func TestTableFindingOrderingIndependentOfIndexCatalogOrder(t *testing.T) {
	table := &driver.Table{Name: "T", Columns: []driver.Column{{Name: "C", DataType: "int", IsNullable: true}}, Indexes: []driver.Index{
		{Name: "uq_z", Columns: []string{"C"}, IsUnique: true}, {Name: "uq_a", Columns: []string{"C"}, IsUnique: true},
	}}
	a, err := AssessTable(table, "mssql", "postgres")
	if err != nil {
		t.Fatal(err)
	}
	table.Indexes[0], table.Indexes[1] = table.Indexes[1], table.Indexes[0]
	b, err := AssessTable(table, "mssql", "postgres")
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatalf("index catalog ordering changed findings: %#v vs %#v, %v", a, b, err)
	}
}

func TestRenderChecksTypedQuotedAndNullable(t *testing.T) {
	for _, tc := range []struct {
		column driver.Column
		want   string
	}{
		{driver.Column{Name: `Odd"; DROP TABLE users; --`, DataType: "tinyint", IsNullable: true}, `"odd___drop_table_users____" >= 0 AND "odd___drop_table_users____" <= 255`},
		{driver.Column{Name: "Binary Data", DataType: "varbinary", MaxLength: 16, IsNullable: true}, `octet_length("binary_data") <= 16`},
	} {
		checks, err := RenderChecks(assess(t, tc.column), nil)
		if err != nil || len(checks) != 1 {
			t.Fatalf("checks %#v, %v", checks, err)
		}
		if checks[0].Expression != tc.want || strings.Contains(checks[0].Definition, "IS NOT NULL") || !strings.Contains(checks[0].Definition, " CHECK ("+tc.want+")") {
			t.Fatalf("unexpected or null-rejecting predicate: %#v", checks[0])
		}
	}
}

func TestGeneratedCheckNamesStableAcrossNormalizationBoundsAndCollisions(t *testing.T) {
	a := assess(t, driver.Column{Name: strings.Repeat("Bytes", 30), DataType: "varbinary", MaxLength: 16})
	first, err := RenderChecks(a, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(first[0].Name) > 63 {
		t.Fatalf("overlong target name %q", first[0].Name)
	}
	b := a
	b.Requirements = append([]Requirement(nil), a.Requirements...)
	b.Requirements[0].Object.Schema = "public"
	b.Requirements[0].Object.Table = driver.NormalizeIdentifier("postgres", b.Requirements[0].Object.Table)
	b.Requirements[0].Object.Column = driver.NormalizeIdentifier("postgres", b.Requirements[0].Object.Column)
	b.Requirements[0].MaxBytes = 32
	second, err := RenderChecks(b, nil)
	if err != nil || first[0].Name != second[0].Name || first[0].Expression == second[0].Expression {
		t.Fatalf("normalization/bound change renamed constraint: %#v, %v", second, err)
	}
	reserved := []string{first[0].Name, "Other", strings.ToUpper(first[0].Name)}
	third, err := RenderChecks(a, reserved)
	if err != nil || third[0].Name == first[0].Name || len(third[0].Name) > 63 {
		t.Fatalf("collision unresolved: %#v, %v", third, err)
	}
	reserved[0], reserved[1] = reserved[1], reserved[0]
	fourth, err := RenderChecks(a, reserved)
	if err != nil || !reflect.DeepEqual(third, fourth) {
		t.Fatalf("reserved name order changed SQL: %#v, %v", fourth, err)
	}
}

func TestRenderChecksRefusesUntrustedOrUnsupportedRequirements(t *testing.T) {
	valid := assess(t, driver.Column{Name: "Value", DataType: "tinyint"}).Requirements[0]
	for _, mutate := range []func(*Requirement){
		func(r *Requirement) { r.Code = `x); DROP TABLE users; --` },
		func(r *Requirement) { r.SourceDialect = "mysql" },
		func(r *Requirement) { r.TargetDialect = "sqlite" },
		func(r *Requirement) { r.Max = 256 },
		func(r *Requirement) { r.Min = -128 },
		func(r *Requirement) { r.Object.Column = "" },
		func(r *Requirement) { r.Object.Column = "bad\x00name" },
		func(r *Requirement) { r.Code, r.Max, r.MaxBytes = VarBinaryLength, 0, -1 },
		func(r *Requirement) { r.Code, r.Max, r.MaxBytes = FixedBinaryPadding, 0, 16 },
		func(r *Requirement) { r.Code = TimeInputConversion },
	} {
		r := valid
		mutate(&r)
		if checks, err := RenderChecks(Assessment{Requirements: []Requirement{valid, r}}, nil); err == nil || checks != nil {
			t.Errorf("unsafe partial rendering returned %#v, %v for %#v", checks, err, r)
		}
	}
	if checks, err := RenderChecks(Assessment{Requirements: []Requirement{valid, valid}}, nil); err == nil || checks != nil {
		t.Fatalf("duplicate requirement accepted: %#v, %v", checks, err)
	}
}
