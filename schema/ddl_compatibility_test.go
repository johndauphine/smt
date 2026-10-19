package schema_test

import (
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/smt/schema"
	"github.com/johndauphine/smt/schema/canonical"
)

// TestLegacyUnkeyedStructLiteralsCompile pins the public struct layouts that
// existed in v1.3.0. The fixture is an external package so it catches a
// source-incompatible field addition that keyed literals would miss.
func TestLegacyUnkeyedStructLiteralsCompile(t *testing.T) {
	repositoryRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("repository root: %v", err)
	}
	command := exec.Command("go", "test", "./schema/testdata/legacy_struct_literals")
	command.Dir = repositoryRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("legacy public struct-literal fixture failed: %v\n%s", err, output)
	}
}

// Positional literals catch additions and removals; field names also catch a
// reorder of adjacent fields of the same Go type.
func TestReleasedPublicValueLayouts(t *testing.T) {
	for _, tc := range []struct {
		value  any
		fields string
	}{
		{schema.Capabilities{}, "CreateSchema CreateTable CreateColumn PrimaryKeys IdentityColumns Defaults ComputedColumns SecondaryIndexes StandalonePrimaryKeys StandaloneForeignKeys NamedUniqueConstraints CheckConstraints IndexExpressionKeys IndexPrefixLengths IndexIncludeColumns FilteredIndexes"},
		{schema.Request{}, "Schema SourceDialect UnknownTypePolicy"},
		{schema.Options{}, "TargetDialect Schema SourceDialect UnknownTypePolicy"},
		{schema.Result{}, "SQL Warnings"},
		{schema.Statement{}, "Kind SQL Warnings"},
		{schema.Plan{}, "Statements"},
		{schema.Warning{}, "Kind Reason SourceDialect TargetDialect Table Column"},
		{schema.Column{}, "Name DataType MaxLength Precision Scale DatetimePrecision IsNullable IsIdentity IsUnsigned DisplayWidth DefaultExpression HasDefault OnUpdateExpression IsComputed ComputedExpression ComputedPersisted EnumValues SRID SpatialSubType"},
		{schema.Table{}, "Name Columns PrimaryKey"},
		{schema.TableRef{}, "Name Columns"},
		{schema.Index{}, "Name Columns ColumnExpressions ColumnPrefixLengths IsUnique IncludeColumns Filter"},
		{schema.PrimaryKey{}, "Name Columns"},
		{schema.ForeignKey{}, "Name Columns RefSchema RefTable RefColumns OnDelete OnUpdate"},
		{schema.UniqueConstraint{}, "Name Columns"},
		{schema.CheckConstraint{}, "Name Expression"},
		{schema.UnsupportedFeatureError{}, "Dialect Feature"},
		{schema.Batch{}, "Statements Cleanup RequiresSingleConnection BestEffortStatementIndexes"},
		{schema.DropOptions{}, "Cascade"},
		{schema.TruncateOptions{}, "Cascade"},
		{schema.ConstraintRef{}, "Name Kind"},
		{schema.Evolution{}, "Kind Table Column Name Constraint DropOptions TruncateOptions"},
		{canonical.CanonicalType{}, "Kind Length Precision Scale Fsp WithTZ UTCNormalized Unsigned National EnumValues Element SpatialType SpatialSubType SRID Raw"},
		{canonical.TypeMeta{}, "MaxLength Precision Scale DatetimePrecision IsUnsigned DisplayWidth EnumValues SRID SpatialSubType"},
		{canonical.RenderOpts{}, "IsIdentity"},
		{canonical.MappingWarning{}, "Kind TargetDialect Reason"},
	} {
		typ := reflect.TypeOf(tc.value)
		t.Run(typ.PkgPath()+"/"+typ.Name(), func(t *testing.T) {
			var names []string
			for i := 0; i < typ.NumField(); i++ {
				names = append(names, typ.Field(i).Name)
			}
			if got := strings.Join(names, " "); got != tc.fields {
				t.Fatalf("released layout changed:\n got %s\nwant %s", got, tc.fields)
			}
		})
	}
}
