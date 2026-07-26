package schema_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/smt/schema"
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

func TestPublicDDLCreatePlanGoldens(t *testing.T) {
	cases := []struct {
		name   string
		opts   schema.Options
		tables []schema.Table
		golden string
	}{
		{
			name: "postgres",
			opts: schema.Options{TargetDialect: "postgres", Schema: "public", SourceDialect: "postgres"},
			tables: []schema.Table{{
				Name: "Accounts",
				Columns: []schema.Column{
					{Name: "ID", DataType: "int4", IsIdentity: true},
					{Name: "Name", DataType: "varchar", MaxLength: 80},
				},
				PrimaryKey: []string{"ID"},
			}},
			golden: "postgres_create_plan.sql.golden",
		},
		{
			name: "mssql",
			opts: schema.Options{TargetDialect: "mssql", Schema: "dbo", SourceDialect: "mssql"},
			tables: []schema.Table{{
				Name: "Accounts",
				Columns: []schema.Column{
					{Name: "ID", DataType: "int", IsIdentity: true},
					{Name: "Name", DataType: "varchar", MaxLength: 80},
				},
				PrimaryKey: []string{"ID"},
			}},
			golden: "mssql_create_plan.sql.golden",
		},
		{
			name: "mysql",
			opts: schema.Options{TargetDialect: "mysql", Schema: "crm", SourceDialect: "mysql"},
			tables: []schema.Table{{
				Name: "Accounts",
				Columns: []schema.Column{
					{Name: "ID", DataType: "int", IsIdentity: true},
					{Name: "Name", DataType: "varchar", MaxLength: 80},
				},
				PrimaryKey: []string{"ID"},
			}},
			golden: "mysql_create_plan.sql.golden",
		},
		{
			name: "sqlite",
			opts: schema.Options{TargetDialect: "sqlite", SourceDialect: "postgres"},
			tables: []schema.Table{{
				Name: "accounts",
				Columns: []schema.Column{
					{Name: "id", DataType: "int4", IsIdentity: true},
					{Name: "name", DataType: "varchar", MaxLength: 80},
				},
				PrimaryKey: []string{"id"},
			}},
			golden: "sqlite_create_plan.sql.golden",
		},
		{
			name: "clickhouse",
			opts: schema.Options{TargetDialect: "clickhouse", Schema: "analytics", SourceDialect: "postgres"},
			tables: []schema.Table{{
				Name: "events",
				Columns: []schema.Column{
					{Name: "event_id", DataType: "bigint"},
					{Name: "payload", DataType: "json", IsNullable: true},
				},
				PrimaryKey: []string{"event_id"},
			}},
			golden: "clickhouse_create_plan.sql.golden",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			renderer, err := schema.NewRenderer(tc.opts)
			if err != nil {
				t.Fatalf("NewRenderer: %v", err)
			}
			plan, err := renderer.PlanCreate(tc.tables)
			if err != nil {
				t.Fatalf("PlanCreate: %v", err)
			}
			assertPlanGolden(t, plan, tc.golden)
		})
	}
}

func TestPublicDDLPostgresEmptySchemaUsesUnqualifiedTable(t *testing.T) {
	renderer, err := schema.NewRenderer(schema.Options{TargetDialect: "postgres", SourceDialect: "postgres"})
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	plan, err := renderer.PlanCreate([]schema.Table{{
		Name:       "Accounts",
		Columns:    []schema.Column{{Name: "ID", DataType: "int4"}},
		PrimaryKey: []string{"ID"},
	}})
	if err != nil {
		t.Fatalf("PlanCreate: %v", err)
	}
	if len(plan.Statements) != 1 || plan.Statements[0].Kind != schema.StatementCreateTable {
		t.Fatalf("plan = %#v, want one create-table statement", plan)
	}
	if strings.Contains(plan.Statements[0].SQL, `"".`) {
		t.Fatalf("empty PostgreSQL schema was qualified: %s", plan.Statements[0].SQL)
	}
	if !strings.HasPrefix(plan.Statements[0].SQL, "CREATE TABLE \"accounts\"") {
		t.Fatalf("unqualified PostgreSQL table = %s", plan.Statements[0].SQL)
	}
}

func TestPublicDDLSQLiteIdentityCompatibility(t *testing.T) {
	renderer, err := schema.NewRenderer(schema.Options{TargetDialect: "sqlite", Schema: "ignored_by_sqlite", SourceDialect: "postgres"})
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}

	result, err := renderer.CreateTable(schema.Table{
		Name: "users",
		Columns: []schema.Column{
			{Name: "id", DataType: "int4", IsIdentity: true},
			{Name: "email", DataType: "varchar", MaxLength: 255},
		},
		PrimaryKey: []string{"id"},
	})
	if err != nil {
		t.Fatalf("CreateTable single-column identity: %v", err)
	}
	if !strings.Contains(result.SQL, `"id" INTEGER PRIMARY KEY AUTOINCREMENT`) {
		t.Fatalf("SQLite identity DDL = %s", result.SQL)
	}
	if strings.Contains(result.SQL, `"ignored_by_sqlite".`) {
		t.Fatalf("SQLite table was qualified by its ignored configured schema: %s", result.SQL)
	}
	if strings.Contains(result.SQL, `CONSTRAINT "pk_users"`) {
		t.Fatalf("SQLite inline identity retained a duplicate primary key: %s", result.SQL)
	}
	plan, err := renderer.PlanCreate([]schema.Table{{
		Name:       "plan_users",
		Columns:    []schema.Column{{Name: "id", DataType: "int4", IsIdentity: true}},
		PrimaryKey: []string{"id"},
	}})
	if err != nil || len(plan.Statements) != 1 || strings.Contains(plan.Statements[0].SQL, `"ignored_by_sqlite".`) {
		t.Fatalf("PlanCreate SQLite schema compatibility = %#v, %v", plan, err)
	}

	result, err = renderer.CreateTable(schema.Table{
		Name: "audit",
		Columns: []schema.Column{
			{Name: "tenant", DataType: "int4"},
			{Name: "id", DataType: "int4", IsIdentity: true},
		},
		PrimaryKey: []string{"tenant", "id"},
	})
	if err != nil {
		t.Fatalf("CreateTable composite identity: %v", err)
	}
	if strings.Contains(result.SQL, "AUTOINCREMENT") {
		t.Fatalf("SQLite composite primary key used AUTOINCREMENT: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, `"id" INTEGER NOT NULL`) || !strings.Contains(result.SQL, `PRIMARY KEY ("tenant", "id")`) {
		t.Fatalf("SQLite composite identity lost table shape: %s", result.SQL)
	}
	if !hasWarningKind(result.Warnings, "sqlite-identity-best-effort") {
		t.Fatalf("SQLite composite identity warnings = %#v", result.Warnings)
	}

	_, err = renderer.CreateColumn(schema.Column{Name: "id", DataType: "int4", IsIdentity: true})
	var unsupported *schema.UnsupportedFeatureError
	if !errors.As(err, &unsupported) || !strings.Contains(unsupported.Feature, "standalone identity") {
		t.Fatalf("CreateColumn identity error = %v, want standalone-identity guidance", err)
	}
}

func TestPublicDDLCreationCapabilitiesAcrossDialects(t *testing.T) {
	cases := []struct {
		dialect  string
		schema   bool
		identity bool
		defaults bool
		computed bool
	}{
		{dialect: "postgres", schema: true, identity: true, defaults: true, computed: true},
		{dialect: "mssql", schema: true, identity: true, defaults: true, computed: true},
		{dialect: "mysql", schema: true, identity: true, defaults: true, computed: true},
		{dialect: "sqlite", schema: false, identity: true, defaults: true, computed: false},
		{dialect: "clickhouse", schema: true, identity: false, defaults: false, computed: false},
	}
	for _, tc := range cases {
		t.Run(tc.dialect, func(t *testing.T) {
			renderer, err := schema.NewRenderer(schema.Options{TargetDialect: tc.dialect})
			if err != nil {
				t.Fatalf("NewRenderer: %v", err)
			}
			got := renderer.Capabilities()
			if !got.CreateTable || !got.CreateColumn || !got.PrimaryKeys || got.CreateSchema != tc.schema ||
				got.IdentityColumns != tc.identity || got.Defaults != tc.defaults || got.ComputedColumns != tc.computed {
				t.Fatalf("Capabilities() = %#v", got)
			}
		})
	}
}

func TestPublicDDLCanonicalTypeMetaCompatibility(t *testing.T) {
	column := schema.Column{
		MaxLength: 80, Precision: 12, Scale: 4, IsUnsigned: true, DisplayWidth: 1,
		EnumValues: []string{"new", "done"}, SRID: 4326, SpatialSubType: "point",
	}
	meta := column.TypeMeta()
	if meta.MaxLength != 80 || meta.Precision != 12 || meta.Scale != 4 || !meta.IsUnsigned ||
		meta.DisplayWidth != 1 || meta.SRID != 4326 || meta.SpatialSubType != "point" {
		t.Fatalf("TypeMeta() = %#v", meta)
	}
	meta.EnumValues[0] = "mutated"
	if column.EnumValues[0] != "new" {
		t.Fatalf("TypeMeta() exposed Column.EnumValues backing storage")
	}
}

// TestDMTCreatePathCompatibility documents the full first-milestone handoff:
// discovery projects its data into schema.Table, DMT schedules and executes
// the returned statements, and no DMT-owned SQL construction is needed.
func TestDMTCreatePathCompatibility(t *testing.T) {
	renderer, err := schema.NewRenderer(schema.Options{
		TargetDialect: "postgres", Schema: "public", SourceDialect: "postgres",
	})
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	plan, err := renderer.PlanCreate([]schema.Table{
		{
			Name:       "Accounts",
			Columns:    []schema.Column{{Name: "ID", DataType: "int4", IsIdentity: true}},
			PrimaryKey: []string{"ID"},
		},
		{
			Name:       "Events",
			Columns:    []schema.Column{{Name: "EventID", DataType: "int8"}},
			PrimaryKey: []string{"EventID"},
		},
	})
	if err != nil {
		t.Fatalf("PlanCreate: %v", err)
	}

	var executed []string
	for _, statement := range plan.Statements {
		switch statement.Kind {
		case schema.StatementCreateSchema, schema.StatementCreateTable:
			executed = append(executed, statement.SQL) // DMT's executor receives SMT SQL verbatim.
		default:
			t.Fatalf("unexpected create-path statement kind %q", statement.Kind)
		}
	}
	if len(executed) != 3 || !strings.Contains(executed[1], `CREATE TABLE "public"."accounts"`) ||
		!strings.Contains(executed[2], `CREATE TABLE "public"."events"`) {
		t.Fatalf("DMT-compatible execution sequence = %#v", executed)
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

func TestPublicDDLCreateColumnGoldens(t *testing.T) {
	cases := []struct {
		name   string
		opts   schema.Options
		column schema.Column
		golden string
	}{
		{
			name: "postgres", opts: schema.Options{TargetDialect: "postgres", SourceDialect: "postgres"},
			column: schema.Column{Name: "name", DataType: "varchar", MaxLength: 80}, golden: "postgres_column.sql.golden",
		},
		{
			name: "mssql", opts: schema.Options{TargetDialect: "mssql", SourceDialect: "mssql"},
			column: schema.Column{Name: "name", DataType: "varchar", MaxLength: 80}, golden: "mssql_column.sql.golden",
		},
		{
			name: "mysql", opts: schema.Options{TargetDialect: "mysql", SourceDialect: "mysql"},
			column: schema.Column{Name: "name", DataType: "varchar", MaxLength: 80}, golden: "mysql_column.sql.golden",
		},
		{
			name: "sqlite", opts: schema.Options{TargetDialect: "sqlite", SourceDialect: "postgres"},
			column: schema.Column{Name: "name", DataType: "varchar", MaxLength: 80}, golden: "sqlite_column.sql.golden",
		},
		{
			name: "clickhouse", opts: schema.Options{TargetDialect: "clickhouse", SourceDialect: "postgres"},
			column: schema.Column{Name: "name", DataType: "varchar", MaxLength: 80}, golden: "clickhouse_column.sql.golden",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			renderer, err := schema.NewRenderer(tc.opts)
			if err != nil {
				t.Fatalf("NewRenderer: %v", err)
			}
			result, err := renderer.CreateColumn(tc.column)
			if err != nil {
				t.Fatalf("CreateColumn: %v", err)
			}
			assertGolden(t, result.SQL, tc.golden)
		})
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

// legacySideObjectDialect intentionally implements the pre-ForeignKey
// SideObjectDialect method set to pin source compatibility for custom users.
type legacySideObjectDialect struct{ testDialect }

func (legacySideObjectDialect) Name() string      { return "legacy-side-objects" }
func (legacySideObjectDialect) Aliases() []string { return nil }
func (legacySideObjectDialect) Capabilities() schema.Capabilities {
	return schema.Capabilities{
		CreateSchema: true, CreateTable: true, CreateColumn: true,
		SecondaryIndexes: true, StandalonePrimaryKeys: true,
		NamedUniqueConstraints: true, CheckConstraints: true,
	}
}
func (legacySideObjectDialect) RenderIndex(schema.Request, schema.TableRef, schema.Index) (schema.Result, error) {
	return schema.Result{SQL: "LEGACY INDEX"}, nil
}
func (legacySideObjectDialect) RenderPrimaryKey(schema.Request, schema.TableRef, schema.PrimaryKey) (schema.Result, error) {
	return schema.Result{SQL: "LEGACY PRIMARY KEY"}, nil
}
func (legacySideObjectDialect) RenderUniqueConstraint(schema.Request, schema.TableRef, schema.UniqueConstraint) (schema.Result, error) {
	return schema.Result{SQL: "LEGACY UNIQUE"}, nil
}
func (legacySideObjectDialect) RenderCheckConstraint(schema.Request, schema.TableRef, schema.CheckConstraint) (schema.Result, error) {
	return schema.Result{SQL: "LEGACY CHECK"}, nil
}

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

func assertPlanGolden(t *testing.T, plan schema.Plan, name string) {
	t.Helper()
	var b strings.Builder
	for i, statement := range plan.Statements {
		if i > 0 {
			b.WriteString("\n\n")
		}
		b.WriteString(string(statement.Kind))
		b.WriteString("\n")
		b.WriteString(statement.SQL)
	}
	assertGolden(t, b.String(), name)
}

func hasWarningKind(warnings []schema.Warning, want string) bool {
	for _, warning := range warnings {
		if warning.Kind == want {
			return true
		}
	}
	return false
}
