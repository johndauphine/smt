package schema_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"smt/schema"
)

func TestPublicDDLRepresentativeGoldens(t *testing.T) {
	precision := 3
	cases := []struct {
		name         string
		opts         schema.Options
		table        schema.Table
		schemaGolden string
		tableGolden  string
		warningKinds []string
	}{
		{
			name: "postgres",
			opts: schema.Options{
				TargetDialect: "pg", Schema: "public", SourceDialect: "mysql",
			},
			table: schema.Table{
				Name: "Accounts",
				Columns: []schema.Column{
					{Name: "ID", DataType: "bigint", IsUnsigned: true, IsIdentity: true},
					{Name: "Name", DataType: "varchar", MaxLength: 80},
				},
				PrimaryKey: []string{"ID"},
			},
			schemaGolden: "postgres_schema.sql.golden",
			tableGolden:  "postgres_table.sql.golden",
		},
		{
			name: "sqlite",
			opts: schema.Options{
				TargetDialect: "sqlite3", SourceDialect: "postgres",
			},
			table: schema.Table{
				Name: "audit_log",
				Columns: []schema.Column{
					{Name: "id", DataType: "integer"},
					{Name: "payload", DataType: "json", IsNullable: true},
				},
				PrimaryKey: []string{"id"},
			},
			tableGolden:  "sqlite_table.sql.golden",
			warningKinds: []string{"json"},
		},
		{
			name: "clickhouse",
			opts: schema.Options{
				TargetDialect: "click-house", Schema: "analytics", SourceDialect: "postgres",
			},
			table: schema.Table{
				Name: "events",
				Columns: []schema.Column{
					{Name: "event_id", DataType: "bigint"},
					{Name: "attributes", DataType: "json", IsNullable: true},
					{Name: "occurred_at", DataType: "timestamp with time zone", DatetimePrecision: &precision, IsNullable: true},
				},
				PrimaryKey: []string{"event_id"},
			},
			schemaGolden: "clickhouse_schema.sql.golden",
			tableGolden:  "clickhouse_table.sql.golden",
			warningKinds: []string{"json", "timestamp", "primary-key-not-unique"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			renderer, err := schema.NewRenderer(tc.opts)
			if err != nil {
				t.Fatalf("NewRenderer: %v", err)
			}
			if tc.schemaGolden != "" {
				result, err := renderer.CreateSchema()
				if err != nil {
					t.Fatalf("CreateSchema: %v", err)
				}
				assertGolden(t, result.SQL, tc.schemaGolden)
			}
			result, err := renderer.CreateTable(tc.table)
			if err != nil {
				t.Fatalf("CreateTable: %v", err)
			}
			assertGolden(t, result.SQL, tc.tableGolden)
			for _, kind := range tc.warningKinds {
				if !hasWarningKind(result.Warnings, kind) {
					t.Fatalf("warnings = %#v, missing kind %q", result.Warnings, kind)
				}
			}
		})
	}
}

func TestPublicDDLColumnAndFallbackWarnings(t *testing.T) {
	renderer, err := schema.NewRenderer(schema.Options{
		TargetDialect:     "clickhouse",
		SourceDialect:     "postgres",
		UnknownTypePolicy: schema.UnknownTypeWarn,
	})
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}

	result, err := renderer.CreateColumn(schema.Column{
		Name: "opaque", DataType: "opaque_extension", IsNullable: true,
	})
	if err != nil {
		t.Fatalf("CreateColumn: %v", err)
	}
	if result.SQL != "`opaque` Nullable(String)" {
		t.Fatalf("column SQL = %q", result.SQL)
	}
	if !hasWarningKind(result.Warnings, "unknown-type-fallback") {
		t.Fatalf("warnings = %#v, missing unknown-type-fallback", result.Warnings)
	}
}

func TestPublicDDLPreservesEmptyDefault(t *testing.T) {
	renderer, err := schema.NewRenderer(schema.Options{TargetDialect: "postgres", SourceDialect: "postgres"})
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}

	result, err := renderer.CreateColumn(schema.Column{
		Name: "label", DataType: "varchar", MaxLength: 30, HasDefault: true,
	})
	if err != nil {
		t.Fatalf("CreateColumn: %v", err)
	}
	if result.SQL != `"label" character varying(30) NOT NULL DEFAULT ''` {
		t.Fatalf("column SQL = %q", result.SQL)
	}
}

func TestPublicDDLCapabilitiesAreEnforced(t *testing.T) {
	sqlite, err := schema.NewRenderer(schema.Options{TargetDialect: "sqlite", Schema: "named"})
	if err != nil {
		t.Fatalf("NewRenderer sqlite: %v", err)
	}
	if _, err := sqlite.CreateSchema(); err == nil {
		t.Fatal("CreateSchema succeeded for SQLite")
	} else {
		var unsupported *schema.UnsupportedFeatureError
		if !errors.As(err, &unsupported) || unsupported.Feature != "schema creation" {
			t.Fatalf("CreateSchema error = %v, want unsupported schema creation", err)
		}
	}

	clickhouse, err := schema.NewRenderer(schema.Options{TargetDialect: "clickhouse"})
	if err != nil {
		t.Fatalf("NewRenderer clickhouse: %v", err)
	}
	if clickhouse.Capabilities().Defaults {
		t.Fatal("ClickHouse defaults capability is unexpectedly true")
	}
	if _, err := clickhouse.CreateColumn(schema.Column{Name: "created", DataType: "timestamp", DefaultExpression: "now()"}); err == nil {
		t.Fatal("CreateColumn accepted a ClickHouse default")
	} else {
		var unsupported *schema.UnsupportedFeatureError
		if !errors.As(err, &unsupported) || unsupported.Feature != "default expressions" {
			t.Fatalf("CreateColumn error = %v, want unsupported default expressions", err)
		}
	}
}

func TestPublicDDLClickHouseNullableValidation(t *testing.T) {
	renderer, err := schema.NewRenderer(schema.Options{TargetDialect: "clickhouse", SourceDialect: "clickhouse"})
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}

	for _, tc := range []struct {
		name    string
		column  schema.Column
		feature string
	}{
		{name: "array", column: schema.Column{Name: "tags", DataType: "Array(String)", IsNullable: true}, feature: `nullable Array column "tags"`},
		{name: "map", column: schema.Column{Name: "labels", DataType: "Map(String, String)", IsNullable: true}, feature: `nullable Map column "labels"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := renderer.CreateColumn(tc.column)
			var unsupported *schema.UnsupportedFeatureError
			if !errors.As(err, &unsupported) {
				t.Fatalf("CreateColumn error = %v, want UnsupportedFeatureError", err)
			}
			if unsupported.Dialect != "clickhouse" || unsupported.Feature != tc.feature {
				t.Fatalf("unsupported error = %#v, want dialect clickhouse and feature %q", unsupported, tc.feature)
			}
		})
	}

	_, err = renderer.CreateTable(schema.Table{
		Name:       "events",
		Columns:    []schema.Column{{Name: "event_id", DataType: "Int64", IsNullable: true}},
		PrimaryKey: []string{"event_id"},
	})
	var nullableKey *schema.UnsupportedFeatureError
	if !errors.As(err, &nullableKey) {
		t.Fatalf("CreateTable error = %v, want UnsupportedFeatureError", err)
	}
	if nullableKey.Dialect != "clickhouse" || nullableKey.Feature != `primary-key/order-by reference to nullable column "event_id"` {
		t.Fatalf("nullable key error = %#v", nullableKey)
	}

	result, err := renderer.CreateColumn(schema.Column{Name: "event_id", DataType: "Int64", IsNullable: true})
	if err != nil {
		t.Fatalf("CreateColumn nullable primitive: %v", err)
	}
	if result.SQL != "`event_id` Nullable(Int64)" {
		t.Fatalf("nullable primitive SQL = %q, want `event_id` Nullable(Int64)", result.SQL)
	}
}

func TestRegistryRegistersAndSelectsCustomDialect(t *testing.T) {
	registry := schema.NewRegistry()
	if err := registry.Register(testDialect{}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	renderer, err := registry.NewRenderer(schema.Options{TargetDialect: "TEST"})
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	if renderer.Dialect() != "test" {
		t.Fatalf("Dialect() = %q, want test", renderer.Dialect())
	}
	result, err := renderer.CreateColumn(schema.Column{Name: "value", DataType: "ignored"})
	if err != nil {
		t.Fatalf("CreateColumn: %v", err)
	}
	if result.SQL != "TEST COLUMN value" {
		t.Fatalf("column SQL = %q", result.SQL)
	}
	if _, err := registry.Resolve("example-test"); err != nil {
		t.Fatalf("Resolve alias: %v", err)
	}
}

func TestRegistryRejectsDuplicateAliasesWithinDialect(t *testing.T) {
	tests := []struct {
		name    string
		dialect duplicateAliasesDialect
	}{
		{
			name:    "repeated alias",
			dialect: duplicateAliasesDialect{name: "duplicate-aliases", aliases: []string{"alias-one", "ALIAS-ONE"}},
		},
		{
			name:    "alias matches name",
			dialect: duplicateAliasesDialect{name: "same-name-alias", aliases: []string{"SAME-NAME-ALIAS"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := schema.NewRegistry().Register(tc.dialect)
			if !errors.Is(err, schema.ErrDialectRegistered) {
				t.Fatalf("Register error = %v, want ErrDialectRegistered", err)
			}
		})
	}
}

func TestPublicDDLRejectsInvalidPrimaryKeyColumns(t *testing.T) {
	renderer, err := schema.NewRenderer(schema.Options{TargetDialect: "postgres"})
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}

	tests := []struct {
		name       string
		primaryKey []string
		want       string
	}{
		{name: "empty", primaryKey: []string{""}, want: "primary key contains an empty column name"},
		{name: "missing", primaryKey: []string{"missing"}, want: `primary key column "missing" does not exist`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := renderer.CreateTable(schema.Table{
				Name:       "accounts",
				Columns:    []schema.Column{{Name: "id", DataType: "bigint"}},
				PrimaryKey: tc.primaryKey,
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CreateTable error = %v, want text %q", err, tc.want)
			}
		})
	}
}

type testDialect struct{}

func (testDialect) Name() string      { return "test" }
func (testDialect) Aliases() []string { return []string{"example-test"} }
func (testDialect) Capabilities() schema.Capabilities {
	return schema.Capabilities{CreateSchema: true, CreateTable: true, CreateColumn: true}
}
func (testDialect) RenderSchema(schema.Request) (schema.Result, error) {
	return schema.Result{SQL: "TEST SCHEMA"}, nil
}
func (testDialect) RenderTable(_ schema.Request, table schema.Table) (schema.Result, error) {
	return schema.Result{SQL: "TEST TABLE " + table.Name}, nil
}
func (testDialect) RenderColumn(_ schema.Request, column schema.Column) (schema.Result, error) {
	return schema.Result{SQL: "TEST COLUMN " + column.Name}, nil
}

type duplicateAliasesDialect struct {
	testDialect
	name    string
	aliases []string
}

func (d duplicateAliasesDialect) Name() string      { return d.name }
func (d duplicateAliasesDialect) Aliases() []string { return d.aliases }

func assertGolden(t *testing.T, got, name string) {
	t.Helper()
	wantBytes, err := os.ReadFile(filepath.Join("testdata", "ddl", name))
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	want := strings.TrimSpace(string(wantBytes))
	if got != want {
		t.Fatalf("DDL mismatch for %s:\n got: %s\nwant: %s", name, got, want)
	}
}

func hasWarningKind(warnings []schema.Warning, want string) bool {
	for _, warning := range warnings {
		if warning.Kind == want {
			return true
		}
	}
	return false
}
