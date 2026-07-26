package schema_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/smt/internal/ddl"
	"github.com/johndauphine/smt/internal/driver"
	"github.com/johndauphine/smt/schema"
)

func TestPublicDDLEvolutionGoldens(t *testing.T) {
	cases := []struct {
		name   string
		opts   schema.Options
		column schema.Column
		alter  schema.Column
		golden string
	}{
		{
			name:   "postgres",
			opts:   schema.Options{TargetDialect: "postgres", Schema: "public", SourceDialect: "postgres"},
			column: schema.Column{Name: "subtotal", DataType: "int4", IsNullable: true, DefaultExpression: "0"},
			alter:  schema.Column{Name: "subtotal", DataType: "int8", IsNullable: true},
			golden: "postgres_evolution.sql.golden",
		},
		{
			name:   "mssql",
			opts:   schema.Options{TargetDialect: "mssql", Schema: "dbo", SourceDialect: "mssql"},
			column: schema.Column{Name: "subtotal", DataType: "int", IsNullable: true, DefaultExpression: "0"},
			alter:  schema.Column{Name: "subtotal", DataType: "bigint", IsNullable: true},
			golden: "mssql_evolution.sql.golden",
		},
		{
			name:   "mysql",
			opts:   schema.Options{TargetDialect: "mysql", Schema: "crm", SourceDialect: "mysql"},
			column: schema.Column{Name: "subtotal", DataType: "int", IsNullable: true, DefaultExpression: "0"},
			alter:  schema.Column{Name: "subtotal", DataType: "bigint", IsNullable: true},
			golden: "mysql_evolution.sql.golden",
		},
		{
			name:   "sqlite",
			opts:   schema.Options{TargetDialect: "sqlite", Schema: "ignored_by_sqlite", SourceDialect: "sqlite"},
			column: schema.Column{Name: "subtotal", DataType: "integer", IsNullable: true, DefaultExpression: "0"},
			alter:  schema.Column{Name: "subtotal", DataType: "integer", IsNullable: true},
			golden: "sqlite_evolution.sql.golden",
		},
		{
			name:   "clickhouse",
			opts:   schema.Options{TargetDialect: "clickhouse", Schema: "analytics", SourceDialect: "clickhouse"},
			column: schema.Column{Name: "subtotal", DataType: "Int64", IsNullable: true},
			alter:  schema.Column{Name: "subtotal", DataType: "Int64", IsNullable: true},
			golden: "clickhouse_evolution.sql.golden",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			renderer, err := schema.NewRenderer(tc.opts)
			if err != nil {
				t.Fatalf("NewRenderer: %v", err)
			}
			got := renderEvolutionArtifacts(t, renderer, tc.column, tc.alter)
			assertGolden(t, got, tc.golden)
		})
	}
}

func renderEvolutionArtifacts(t *testing.T, renderer schema.Renderer, column, alter schema.Column) string {
	t.Helper()
	table := schema.TableRef{Name: "Orders", Columns: []schema.Column{{Name: "id", DataType: column.DataType}, column}}
	caps := renderer.Capabilities()
	var artifacts []string
	appendBatch := func(name string, render func() (schema.Batch, error)) {
		t.Helper()
		batch, err := render()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		artifacts = append(artifacts, name+"\n"+formatBatch(batch))
	}

	if caps.DropSchemas {
		appendBatch("drop_schema", func() (schema.Batch, error) {
			return renderer.DropSchema(schema.DropOptions{Cascade: caps.DropSchemaCascade})
		})
	}
	appendBatch("drop_table", func() (schema.Batch, error) {
		return renderer.DropTable(table, schema.DropOptions{Cascade: caps.DropTableCascade})
	})
	if caps.DropIndexes {
		appendBatch("drop_index", func() (schema.Batch, error) {
			return renderer.DropIndex(table, "IX Orders Subtotal")
		})
	}
	if caps.DropConstraints {
		appendBatch("drop_foreign_key", func() (schema.Batch, error) {
			return renderer.DropConstraint(table, schema.ConstraintRef{Name: "FK Orders Account", Kind: schema.ConstraintForeignKey})
		})
	}
	appendBatch("add_column", func() (schema.Batch, error) { return renderer.AddColumn(table, column) })
	appendBatch("drop_column", func() (schema.Batch, error) { return renderer.DropColumn(table, column.Name) })
	if caps.AlterColumnTypes {
		appendBatch("alter_column_type", func() (schema.Batch, error) { return renderer.AlterColumnType(table, alter) })
	}
	if caps.AlterColumnNullability {
		appendBatch("alter_column_nullability", func() (schema.Batch, error) {
			return renderer.AlterColumnNullability(table, alter)
		})
	}
	if caps.SetColumnDefaults {
		appendBatch("set_column_default", func() (schema.Batch, error) { return renderer.SetColumnDefault(table, column) })
	}
	if caps.DropColumnDefaults {
		appendBatch("drop_column_default", func() (schema.Batch, error) {
			return renderer.DropColumnDefault(table, column.Name)
		})
	}
	appendBatch("truncate_table", func() (schema.Batch, error) {
		return renderer.TruncateTable(table, schema.TruncateOptions{Cascade: caps.TruncateTableCascade})
	})
	return strings.Join(artifacts, "\n\n")
}

func formatBatch(batch schema.Batch) string {
	var lines []string
	if batch.RequiresSingleConnection {
		lines = append(lines, "requires_single_connection")
	}
	for _, statement := range batch.Statements {
		if statement.BestEffort {
			lines = append(lines, "best_effort")
		}
		lines = append(lines, string(statement.Kind), statement.SQL)
	}
	for _, statement := range batch.Cleanup {
		lines = append(lines, "on_failure_"+string(statement.Kind), statement.SQL)
	}
	return strings.Join(lines, "\n")
}

func TestPublicDDLEvolutionCapabilitiesAndUnsupportedErrors(t *testing.T) {
	type expected struct {
		dropSchemas, dropSchemaCascade, dropTables, dropTableCascade, dropIndexes, dropConstraints                  bool
		addColumns, dropColumns, alterTypes, alterNullability, setDefaults, dropDefaults, truncate, truncateCascade bool
	}
	cases := []struct {
		dialect string
		want    expected
	}{
		{"postgres", expected{true, true, true, true, true, true, true, true, true, true, true, true, true, true}},
		{"mssql", expected{true, false, true, false, true, true, true, true, true, true, true, true, true, false}},
		{"mysql", expected{true, false, true, false, true, true, true, true, true, true, true, true, true, false}},
		{"sqlite", expected{false, false, true, false, true, false, true, true, false, false, false, false, true, false}},
		{"clickhouse", expected{true, false, true, false, false, false, true, true, false, false, false, false, true, false}},
	}
	for _, tc := range cases {
		t.Run(tc.dialect, func(t *testing.T) {
			renderer, err := schema.NewRenderer(schema.Options{TargetDialect: tc.dialect, Schema: "scope"})
			if err != nil {
				t.Fatalf("NewRenderer: %v", err)
			}
			got := renderer.Capabilities()
			if got.DropSchemas != tc.want.dropSchemas || got.DropSchemaCascade != tc.want.dropSchemaCascade ||
				got.DropTables != tc.want.dropTables || got.DropTableCascade != tc.want.dropTableCascade ||
				got.DropIndexes != tc.want.dropIndexes || got.DropConstraints != tc.want.dropConstraints ||
				got.AddColumns != tc.want.addColumns || got.DropColumns != tc.want.dropColumns ||
				got.AlterColumnTypes != tc.want.alterTypes || got.AlterColumnNullability != tc.want.alterNullability ||
				got.SetColumnDefaults != tc.want.setDefaults || got.DropColumnDefaults != tc.want.dropDefaults ||
				got.TruncateTables != tc.want.truncate || got.TruncateTableCascade != tc.want.truncateCascade {
				t.Fatalf("evolution capabilities = %+v, want %+v", got, tc.want)
			}
		})
	}

	table := schema.TableRef{Name: "events"}
	casesUnsupported := []struct {
		name string
		call func() error
	}{
		{
			name: "sqlite drop schema",
			call: func() error {
				r, _ := schema.NewRenderer(schema.Options{TargetDialect: "sqlite", Schema: "main"})
				_, err := r.DropSchema(schema.DropOptions{})
				return err
			},
		},
		{
			name: "sqlite constraint drop",
			call: func() error {
				r, _ := schema.NewRenderer(schema.Options{TargetDialect: "sqlite"})
				_, err := r.DropConstraint(table, schema.ConstraintRef{Name: "fk_events_parent", Kind: schema.ConstraintForeignKey})
				return err
			},
		},
		{
			name: "sqlite alter type",
			call: func() error {
				r, _ := schema.NewRenderer(schema.Options{TargetDialect: "sqlite"})
				_, err := r.AlterColumnType(table, schema.Column{Name: "id", DataType: "integer"})
				return err
			},
		},
		{
			name: "clickhouse index drop",
			call: func() error {
				r, _ := schema.NewRenderer(schema.Options{TargetDialect: "clickhouse"})
				_, err := r.DropIndex(table, "ix_events_id")
				return err
			},
		},
		{
			name: "mssql table cascade",
			call: func() error {
				r, _ := schema.NewRenderer(schema.Options{TargetDialect: "mssql"})
				_, err := r.DropTable(table, schema.DropOptions{Cascade: true})
				return err
			},
		},
	}
	for _, tc := range casesUnsupported {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			var unsupported *schema.UnsupportedFeatureError
			if !errors.As(err, &unsupported) {
				t.Fatalf("error = %v, want UnsupportedFeatureError", err)
			}
		})
	}
}

func TestPublicDDLEvolutionNullabilityUsesOperationSpecificValidation(t *testing.T) {
	table := schema.TableRef{Name: "accounts"}
	column := schema.Column{Name: "status", IsNullable: true}

	postgres, err := schema.NewRenderer(schema.Options{TargetDialect: "postgres", Schema: "public"})
	if err != nil {
		t.Fatalf("NewRenderer(postgres): %v", err)
	}
	batch, err := postgres.AlterColumnNullability(table, column)
	if err != nil {
		t.Fatalf("PostgreSQL nullability without DataType: %v", err)
	}
	if got, want := batch.Statements[0].SQL, `ALTER TABLE "public"."accounts" ALTER COLUMN "status" DROP NOT NULL`; got != want {
		t.Fatalf("PostgreSQL nullability SQL = %q, want %q", got, want)
	}

	registry := schema.NewRegistry()
	if err := registry.Register(nullableOnlyEvolutionDialect{}); err != nil {
		t.Fatalf("Register custom evolution dialect: %v", err)
	}
	custom, err := registry.NewRenderer(schema.Options{TargetDialect: "nullable-only"})
	if err != nil {
		t.Fatalf("NewRenderer(custom): %v", err)
	}
	if _, err := custom.AlterColumnNullability(table, column); err != nil {
		t.Fatalf("custom nullability without DataType: %v", err)
	}

	for _, dialect := range []string{"mssql", "mysql"} {
		renderer, err := schema.NewRenderer(schema.Options{TargetDialect: dialect})
		if err != nil {
			t.Fatalf("NewRenderer(%s): %v", dialect, err)
		}
		if _, err := renderer.AlterColumnNullability(table, column); err == nil ||
			!strings.Contains(err.Error(), "empty source data type") {
			t.Fatalf("%s nullability error = %v, want empty source data type", dialect, err)
		}
	}
}

func TestBuiltinEvolutionDialectReturnsTypedUnsupportedErrorsDirectly(t *testing.T) {
	dialect, err := schema.NewRegistry().Resolve("sqlite")
	if err != nil {
		t.Fatalf("Resolve(sqlite): %v", err)
	}
	evolution := dialect.(schema.EvolutionDialect)
	_, err = evolution.RenderEvolution(
		schema.Request{Schema: "named"},
		schema.Evolution{Kind: schema.EvolutionDropSchema},
	)
	var unsupported *schema.UnsupportedFeatureError
	if !errors.As(err, &unsupported) {
		t.Fatalf("RenderEvolution error = %v, want UnsupportedFeatureError", err)
	}
	if unsupported.Feature != "schema drops" {
		t.Fatalf("Unsupported feature = %q, want %q", unsupported.Feature, "schema drops")
	}
}

func TestPublicDDLEvolutionConstraintKindsAndCleanupContracts(t *testing.T) {
	mysql, err := schema.NewRenderer(schema.Options{TargetDialect: "mysql", Schema: "crm"})
	if err != nil {
		t.Fatalf("NewRenderer(mysql): %v", err)
	}
	table := schema.TableRef{Name: "orders"}
	for _, tc := range []struct {
		kind schema.ConstraintKind
		want string
	}{
		{schema.ConstraintPrimaryKey, "ALTER TABLE `crm`.`orders` DROP PRIMARY KEY"},
		{schema.ConstraintUnique, "ALTER TABLE `crm`.`orders` DROP INDEX `uq_orders_code`"},
		{schema.ConstraintForeignKey, "ALTER TABLE `crm`.`orders` DROP FOREIGN KEY `uq_orders_code`"},
		{schema.ConstraintCheck, "ALTER TABLE `crm`.`orders` DROP CHECK `uq_orders_code`"},
	} {
		batch, err := mysql.DropConstraint(table, schema.ConstraintRef{Name: "uq_orders_code", Kind: tc.kind})
		if err != nil {
			t.Fatalf("DropConstraint(%s): %v", tc.kind, err)
		}
		if got := batch.Statements[0].SQL; got != tc.want {
			t.Fatalf("DropConstraint(%s) = %q, want %q", tc.kind, got, tc.want)
		}
	}

	for _, tc := range []struct {
		dialect string
		setup   string
		cleanup string
	}{
		{"mysql", "SET FOREIGN_KEY_CHECKS = 0", "SET FOREIGN_KEY_CHECKS = 1"},
		{"sqlite", "PRAGMA foreign_keys = OFF", "PRAGMA foreign_keys = ON"},
	} {
		t.Run(tc.dialect+" drop batch", func(t *testing.T) {
			renderer, err := schema.NewRenderer(schema.Options{TargetDialect: tc.dialect})
			if err != nil {
				t.Fatalf("NewRenderer: %v", err)
			}
			batch, err := renderer.DropTable(schema.TableRef{Name: "events"}, schema.DropOptions{})
			if err != nil {
				t.Fatalf("DropTable: %v", err)
			}
			if !batch.RequiresSingleConnection || len(batch.Statements) != 3 || len(batch.Cleanup) != 1 {
				t.Fatalf("drop batch contract = %+v, want same-connection setup/DDL/cleanup plus failure cleanup", batch)
			}
			if batch.Statements[0].SQL != tc.setup || batch.Statements[2].SQL != tc.cleanup || batch.Cleanup[0].SQL != tc.cleanup {
				t.Fatalf("drop batch SQL = %+v, want setup=%q cleanup=%q", batch, tc.setup, tc.cleanup)
			}
		})
	}

	sqlite, err := schema.NewRenderer(schema.Options{TargetDialect: "sqlite"})
	if err != nil {
		t.Fatalf("NewRenderer(sqlite): %v", err)
	}
	batch, err := sqlite.TruncateTable(schema.TableRef{Name: "O'Reilly"}, schema.TruncateOptions{})
	if err != nil {
		t.Fatalf("TruncateTable(sqlite): %v", err)
	}
	if len(batch.Statements) != 2 || !batch.Statements[1].BestEffort || batch.Statements[1].Kind != schema.StatementBestEffortCleanup || batch.Statements[1].SQL != "DELETE FROM sqlite_sequence WHERE name = 'O''Reilly'" {
		t.Fatalf("sqlite truncate cleanup = %+v, want quoted best-effort sqlite_sequence cleanup", batch)
	}
}

func TestPublicDDLEvolutionPreservesConfiguredSchemaName(t *testing.T) {
	for _, tc := range []struct {
		dialect string
		want    string
	}{
		{"postgres", `DROP SCHEMA IF EXISTS "Tenant Space"`},
		{"mssql", `IF SCHEMA_ID(N'Tenant Space') IS NOT NULL EXEC(N'DROP SCHEMA [Tenant Space]')`},
		{"mysql", "DROP DATABASE IF EXISTS `Tenant Space`"},
		{"clickhouse", "DROP DATABASE IF EXISTS `Tenant Space`"},
	} {
		t.Run(tc.dialect, func(t *testing.T) {
			renderer, err := schema.NewRenderer(schema.Options{TargetDialect: tc.dialect, Schema: "Tenant Space"})
			if err != nil {
				t.Fatalf("NewRenderer: %v", err)
			}
			batch, err := renderer.DropSchema(schema.DropOptions{})
			if err != nil {
				t.Fatalf("DropSchema: %v", err)
			}
			if got := batch.Statements[0].SQL; got != tc.want {
				t.Fatalf("DropSchema = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPublicDDLEvolutionAdapterParity(t *testing.T) {
	cases := []struct {
		dialect, targetSchema, source, dataType string
		cascade                                 bool
	}{
		{"postgres", "public", "postgres", "int4", true},
		{"mssql", "dbo", "mssql", "int", false},
		{"mysql", "crm", "mysql", "int", false},
		{"sqlite", "ignored", "sqlite", "integer", false},
		{"clickhouse", "analytics", "clickhouse", "Int64", false},
	}
	for _, tc := range cases {
		t.Run(tc.dialect, func(t *testing.T) {
			public, err := schema.NewRenderer(schema.Options{TargetDialect: tc.dialect, Schema: tc.targetSchema, SourceDialect: tc.source})
			if err != nil {
				t.Fatalf("NewRenderer: %v", err)
			}
			internalSchema := tc.targetSchema
			if tc.dialect == "sqlite" {
				internalSchema = ""
			}
			internal, err := ddl.NewRenderer(tc.dialect, internalSchema, "fail")
			if err != nil {
				t.Fatalf("ddl.NewRenderer: %v", err)
			}
			internal = internal.WithSource(tc.source)
			table := schema.TableRef{Name: "Orders"}
			column := schema.Column{Name: "extra", DataType: tc.dataType, IsNullable: true}

			gotDrop, err := public.DropTable(table, schema.DropOptions{Cascade: tc.cascade})
			if err != nil {
				t.Fatalf("DropTable: %v", err)
			}
			wantDrop, err := internal.DropTableWithOptionsDDL(table.Name, tc.cascade)
			if err != nil {
				t.Fatalf("internal DropTable: %v", err)
			}
			if !batchContainsSQL(gotDrop, wantDrop) {
				t.Fatalf("drop core SQL = %+v, want %q", gotDrop.Statements, wantDrop)
			}

			gotAdd, err := public.AddColumn(table, column)
			if err != nil {
				t.Fatalf("AddColumn: %v", err)
			}
			wantAdd, err := internal.AddColumnDDL(table.Name, toInternalColumn(column), nil)
			if err != nil {
				t.Fatalf("internal AddColumn: %v", err)
			}
			if gotAdd.Statements[0].SQL != wantAdd {
				t.Fatalf("add SQL = %q, want %q", gotAdd.Statements[0].SQL, wantAdd)
			}

			caps := public.Capabilities()
			if caps.DropSchemas {
				gotSchema, err := public.DropSchema(schema.DropOptions{})
				if err != nil {
					t.Fatalf("DropSchema: %v", err)
				}
				wantSchema, err := internal.DropSchemaDDL(false)
				if err != nil {
					t.Fatalf("internal DropSchema: %v", err)
				}
				if gotSchema.Statements[0].SQL != wantSchema {
					t.Fatalf("drop schema SQL = %q, want %q", gotSchema.Statements[0].SQL, wantSchema)
				}
			}
			if caps.DropIndexes {
				gotIndex, err := public.DropIndex(table, "ix_orders_extra")
				if err != nil {
					t.Fatalf("DropIndex: %v", err)
				}
				if want := internal.DropIndexDDL(table.Name, "ix_orders_extra"); gotIndex.Statements[0].SQL != want {
					t.Fatalf("drop index SQL = %q, want %q", gotIndex.Statements[0].SQL, want)
				}
			}
			if caps.DropConstraints {
				gotConstraint, err := public.DropConstraint(table, schema.ConstraintRef{Name: "fk_orders_parent", Kind: schema.ConstraintForeignKey})
				if err != nil {
					t.Fatalf("DropConstraint: %v", err)
				}
				wantConstraint, err := internal.DropConstraintDDL(table.Name, "fk_orders_parent", string(schema.ConstraintForeignKey))
				if err != nil {
					t.Fatalf("internal DropConstraint: %v", err)
				}
				if gotConstraint.Statements[0].SQL != wantConstraint {
					t.Fatalf("drop constraint SQL = %q, want %q", gotConstraint.Statements[0].SQL, wantConstraint)
				}
			}
			if caps.DropColumns {
				gotColumn, err := public.DropColumn(table, column.Name)
				if err != nil {
					t.Fatalf("DropColumn: %v", err)
				}
				if want := internal.DropColumnDDL(table.Name, column.Name); gotColumn.Statements[0].SQL != want {
					t.Fatalf("drop column SQL = %q, want %q", gotColumn.Statements[0].SQL, want)
				}
			}
			if caps.AlterColumnTypes {
				gotType, err := public.AlterColumnType(table, column)
				if err != nil {
					t.Fatalf("AlterColumnType: %v", err)
				}
				wantType, err := internal.AlterColumnTypeDDL(table.Name, toInternalColumn(column))
				if err != nil {
					t.Fatalf("internal AlterColumnType: %v", err)
				}
				if gotType.Statements[0].SQL != wantType {
					t.Fatalf("alter type SQL = %q, want %q", gotType.Statements[0].SQL, wantType)
				}
			}
			if caps.AlterColumnNullability {
				gotNullability, err := public.AlterColumnNullability(table, column)
				if err != nil {
					t.Fatalf("AlterColumnNullability: %v", err)
				}
				wantNullability, err := internal.AlterColumnNullabilityDDL(table.Name, toInternalColumn(column))
				if err != nil {
					t.Fatalf("internal AlterColumnNullability: %v", err)
				}
				if gotNullability.Statements[0].SQL != wantNullability {
					t.Fatalf("alter nullability SQL = %q, want %q", gotNullability.Statements[0].SQL, wantNullability)
				}
			}
			if caps.SetColumnDefaults {
				defaultColumn := column
				defaultColumn.DefaultExpression = "0"
				gotDefault, err := public.SetColumnDefault(table, defaultColumn)
				if err != nil {
					t.Fatalf("SetColumnDefault: %v", err)
				}
				wantDefault, err := internal.SetColumnDefaultDDL(table.Name, toInternalColumn(defaultColumn))
				if err != nil {
					t.Fatalf("internal SetColumnDefault: %v", err)
				}
				if gotDefault.Statements[0].SQL != wantDefault {
					t.Fatalf("set default SQL = %q, want %q", gotDefault.Statements[0].SQL, wantDefault)
				}
			}
			if caps.DropColumnDefaults {
				gotDefault, err := public.DropColumnDefault(table, column.Name)
				if err != nil {
					t.Fatalf("DropColumnDefault: %v", err)
				}
				if want := internal.DropColumnDefaultDDL(table.Name, column.Name); gotDefault.Statements[0].SQL != want {
					t.Fatalf("drop default SQL = %q, want %q", gotDefault.Statements[0].SQL, want)
				}
			}

			gotTruncate, err := public.TruncateTable(table, schema.TruncateOptions{Cascade: tc.cascade})
			if err != nil {
				t.Fatalf("TruncateTable: %v", err)
			}
			wantTruncate, err := internal.TruncateTableDDL(table.Name, tc.cascade)
			if err != nil {
				t.Fatalf("internal TruncateTable: %v", err)
			}
			if !batchContainsSQL(gotTruncate, wantTruncate) {
				t.Fatalf("truncate core SQL = %+v, want %q", gotTruncate.Statements, wantTruncate)
			}
			if want := tc.dialect == "mysql" || tc.dialect == "sqlite"; gotDrop.RequiresSingleConnection != want {
				t.Fatalf("drop affinity = %t, want %t", gotDrop.RequiresSingleConnection, want)
			}
			if want := tc.dialect == "mysql"; gotTruncate.RequiresSingleConnection != want {
				t.Fatalf("truncate affinity = %t, want %t", gotTruncate.RequiresSingleConnection, want)
			}
		})
	}
}

type nullableOnlyEvolutionDialect struct{ testDialect }

func (nullableOnlyEvolutionDialect) Name() string      { return "nullable-only" }
func (nullableOnlyEvolutionDialect) Aliases() []string { return nil }
func (nullableOnlyEvolutionDialect) Capabilities() schema.Capabilities {
	return schema.Capabilities{AlterColumnNullability: true}
}
func (nullableOnlyEvolutionDialect) RenderEvolution(_ schema.Request, operation schema.Evolution) (schema.Batch, error) {
	if operation.Kind != schema.EvolutionAlterColumnNullability {
		return schema.Batch{}, errors.New("unexpected evolution kind")
	}
	if operation.Column.DataType != "" {
		return schema.Batch{}, errors.New("unexpected column data type")
	}
	return schema.Batch{Statements: []schema.Statement{{
		Kind: schema.StatementAlterColumnNullability,
		SQL:  "ALTER NULLABILITY",
	}}}, nil
}

func batchContainsSQL(batch schema.Batch, want string) bool {
	for _, statement := range batch.Statements {
		if statement.SQL == want {
			return true
		}
	}
	return false
}

// toInternalColumn keeps the parity assertion intentionally public-value
// driven while accepting the internal test helper's exact type.
func toInternalColumn(column schema.Column) driver.Column {
	return driver.Column{
		Name:               column.Name,
		DataType:           column.DataType,
		MaxLength:          column.MaxLength,
		Precision:          column.Precision,
		Scale:              column.Scale,
		DatetimePrecision:  column.DatetimePrecision,
		IsNullable:         column.IsNullable,
		IsIdentity:         column.IsIdentity,
		IsUnsigned:         column.IsUnsigned,
		DisplayWidth:       column.DisplayWidth,
		DefaultExpression:  column.DefaultExpression,
		HasDefault:         column.HasDefault,
		OnUpdateExpression: column.OnUpdateExpression,
		IsComputed:         column.IsComputed,
		ComputedExpression: column.ComputedExpression,
		ComputedPersisted:  column.ComputedPersisted,
		EnumValues:         append([]string(nil), column.EnumValues...),
		SRID:               column.SRID,
		SpatialSubType:     column.SpatialSubType,
	}
}
