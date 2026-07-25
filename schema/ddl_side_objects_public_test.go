package schema_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/smt/internal/ddl"
	"github.com/johndauphine/smt/internal/driver"
	"github.com/johndauphine/smt/schema"
)

func TestPublicDDLSideObjectGoldens(t *testing.T) {
	cases := []struct {
		name   string
		opts   schema.Options
		table  schema.TableRef
		index  schema.Index
		golden string
	}{
		{
			name: "postgres",
			opts: schema.Options{TargetDialect: "postgres", Schema: "public", SourceDialect: "postgres"},
			table: schema.TableRef{
				Name:    "Orders",
				Columns: []schema.Column{{Name: "ID", DataType: "int4"}, {Name: "Email", DataType: "varchar", MaxLength: 255}, {Name: "Total", DataType: "numeric", Precision: 12, Scale: 2}},
			},
			index:  schema.Index{Name: "IX_Orders_Email", Columns: []string{"Email"}, IsUnique: true, IncludeColumns: []string{"ID"}, Filter: "Email IS NOT NULL"},
			golden: "postgres_side_objects.sql.golden",
		},
		{
			name: "mssql",
			opts: schema.Options{TargetDialect: "mssql", Schema: "dbo", SourceDialect: "mssql"},
			table: schema.TableRef{
				Name:    "Orders",
				Columns: []schema.Column{{Name: "ID", DataType: "int"}, {Name: "Email", DataType: "varchar", MaxLength: 255}, {Name: "Total", DataType: "decimal", Precision: 12, Scale: 2}},
			},
			index:  schema.Index{Name: "IX_Orders_Email", Columns: []string{"Email"}, IsUnique: true, IncludeColumns: []string{"ID"}, Filter: "Email IS NOT NULL"},
			golden: "mssql_side_objects.sql.golden",
		},
		{
			name: "mysql",
			opts: schema.Options{TargetDialect: "mysql", Schema: "crm", SourceDialect: "mysql"},
			table: schema.TableRef{
				Name:    "Orders",
				Columns: []schema.Column{{Name: "ID", DataType: "int"}, {Name: "Email", DataType: "varchar", MaxLength: 255}, {Name: "Total", DataType: "decimal", Precision: 12, Scale: 2}},
			},
			index:  schema.Index{Name: "IX_Orders_Email", Columns: []string{"Email"}, IsUnique: true, ColumnPrefixLengths: []int{16}},
			golden: "mysql_side_objects.sql.golden",
		},
		{
			name: "sqlite",
			opts: schema.Options{TargetDialect: "sqlite", Schema: "ignored_by_sqlite", SourceDialect: "postgres"},
			table: schema.TableRef{
				Name:    "orders",
				Columns: []schema.Column{{Name: "id", DataType: "int4"}, {Name: "email", DataType: "varchar", MaxLength: 255}},
			},
			index:  schema.Index{Name: "ix_orders_email", Columns: []string{"email"}, IsUnique: true, Filter: "email IS NOT NULL"},
			golden: "sqlite_side_objects.sql.golden",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			renderer, err := schema.NewRenderer(tc.opts)
			if err != nil {
				t.Fatalf("NewRenderer: %v", err)
			}
			index, err := renderer.CreateIndex(tc.table, tc.index)
			if err != nil {
				t.Fatalf("CreateIndex: %v", err)
			}

			var statements []string
			statements = append(statements, "create_index\n"+index.SQL)
			if tc.name != "sqlite" {
				primaryKey, err := renderer.CreatePrimaryKey(tc.table, schema.PrimaryKey{Columns: []string{"ID"}})
				if err != nil {
					t.Fatalf("CreatePrimaryKey: %v", err)
				}
				unique, err := renderer.CreateUniqueConstraint(tc.table, schema.UniqueConstraint{Name: "UQ_Orders_Email", Columns: []string{"Email"}})
				if err != nil {
					t.Fatalf("CreateUniqueConstraint: %v", err)
				}
				check, err := renderer.CreateCheckConstraint(tc.table, schema.CheckConstraint{Name: "CK_Orders_Total", Expression: "Total >= 0"})
				if err != nil {
					t.Fatalf("CreateCheckConstraint: %v", err)
				}
				foreignKey, err := renderer.CreateForeignKey(tc.table, schema.ForeignKey{
					Name: "FK_Orders_Accounts", Columns: []string{"ID"}, RefSchema: "identity", RefTable: "Accounts", RefColumns: []string{"ID"},
					OnDelete: schema.ReferentialActionCascade, OnUpdate: schema.ReferentialActionSetNull,
				})
				if err != nil {
					t.Fatalf("CreateForeignKey: %v", err)
				}
				statements = append(statements,
					"create_primary_key\n"+primaryKey.SQL,
					"create_unique_constraint\n"+unique.SQL,
					"create_check_constraint\n"+check.SQL,
					"create_foreign_key\n"+foreignKey.SQL,
				)
			}
			assertGolden(t, strings.Join(statements, "\n\n"), tc.golden)
		})
	}
}

func TestPublicDDLClickHouseSideObjectErrorGolden(t *testing.T) {
	renderer, err := schema.NewRenderer(schema.Options{TargetDialect: "clickhouse", Schema: "analytics"})
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	table := schema.TableRef{Name: "events", Columns: []schema.Column{{Name: "id", DataType: "Int64"}}}
	cases := []struct {
		kind string
		call func() error
	}{
		{
			kind: "create_index",
			call: func() error {
				_, err := renderer.CreateIndex(table, schema.Index{Name: "ix_events_id", Columns: []string{"id"}})
				return err
			},
		},
		{
			kind: "create_primary_key",
			call: func() error {
				_, err := renderer.CreatePrimaryKey(table, schema.PrimaryKey{Columns: []string{"id"}})
				return err
			},
		},
		{
			kind: "create_unique_constraint",
			call: func() error {
				_, err := renderer.CreateUniqueConstraint(table, schema.UniqueConstraint{Name: "uq_events_id", Columns: []string{"id"}})
				return err
			},
		},
		{
			kind: "create_check_constraint",
			call: func() error {
				_, err := renderer.CreateCheckConstraint(table, schema.CheckConstraint{Name: "ck_events_id", Expression: "id > 0"})
				return err
			},
		},
		{
			kind: "create_foreign_key",
			call: func() error {
				_, err := renderer.CreateForeignKey(table, schema.ForeignKey{Name: "fk_events_id", Columns: []string{"id"}, RefTable: "accounts", RefColumns: []string{"id"}})
				return err
			},
		},
	}

	lines := make([]string, 0, len(cases))
	for _, tc := range cases {
		err := tc.call()
		var unsupported *schema.UnsupportedFeatureError
		if !errors.As(err, &unsupported) {
			t.Fatalf("%s error = %v, want UnsupportedFeatureError", tc.kind, err)
		}
		lines = append(lines, tc.kind+"\n"+unsupported.Error())
	}
	assertGolden(t, strings.Join(lines, "\n\n"), "clickhouse_side_objects.error.golden")
}

func TestPublicDDLSideObjectsPreserveOrderAndIdentifierConvention(t *testing.T) {
	renderer, err := schema.NewRenderer(schema.Options{TargetDialect: "postgres", Schema: "public"})
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	table := schema.TableRef{Name: "Order Lines"}

	index, err := renderer.CreateIndex(table, schema.Index{
		Name: "IX Order Lines", Columns: []string{"Tenant ID", "Order ID"},
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	const wantIndex = `CREATE INDEX "ix_order_lines" ON "public"."order_lines" ("tenant_id", "order_id")`
	if index.SQL != wantIndex {
		t.Fatalf("index SQL = %q, want %q", index.SQL, wantIndex)
	}

	primaryKey, err := renderer.CreatePrimaryKey(table, schema.PrimaryKey{Columns: []string{"Tenant ID", "Order ID"}})
	if err != nil {
		t.Fatalf("CreatePrimaryKey: %v", err)
	}
	const wantPrimaryKey = `ALTER TABLE "public"."order_lines" ADD CONSTRAINT "pk_order_lines" PRIMARY KEY ("tenant_id", "order_id")`
	if primaryKey.SQL != wantPrimaryKey {
		t.Fatalf("primary-key SQL = %q, want %q", primaryKey.SQL, wantPrimaryKey)
	}

	foreignKey, err := renderer.CreateForeignKey(table, schema.ForeignKey{
		Name: "FK Order Lines Orders", Columns: []string{"Tenant ID", "Order ID"}, RefSchema: "Catalog Data", RefTable: "Orders", RefColumns: []string{"Tenant ID", "ID"},
		OnDelete: schema.ReferentialActionNoAction, OnUpdate: schema.ReferentialActionCascade,
	})
	if err != nil {
		t.Fatalf("CreateForeignKey: %v", err)
	}
	const wantForeignKey = `ALTER TABLE "public"."order_lines" ADD CONSTRAINT "fk_order_lines_orders" FOREIGN KEY ("tenant_id", "order_id") REFERENCES "catalog_data"."orders" ("tenant_id", "id") ON DELETE NO ACTION ON UPDATE CASCADE`
	if foreignKey.SQL != wantForeignKey {
		t.Fatalf("foreign-key SQL = %q, want %q", foreignKey.SQL, wantForeignKey)
	}
}

func TestPublicDDLStandalonePrimaryKeyDefaultMatchesCreateTableNaming(t *testing.T) {
	renderer, err := schema.NewRenderer(schema.Options{TargetDialect: "postgres", Schema: "public"})
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}

	for _, tableName := range []string{"1Orders", strings.Repeat("order_", 12)} {
		t.Run(tableName, func(t *testing.T) {
			wantName := driver.NormalizeIdentifier("postgres", "pk_"+driver.NormalizeIdentifier("postgres", tableName))
			createTable, err := renderer.CreateTable(schema.Table{
				Name:       tableName,
				Columns:    []schema.Column{{Name: "ID", DataType: "int4"}},
				PrimaryKey: []string{"ID"},
			})
			if err != nil {
				t.Fatalf("CreateTable: %v", err)
			}
			primaryKey, err := renderer.CreatePrimaryKey(schema.TableRef{Name: tableName}, schema.PrimaryKey{Columns: []string{"ID"}})
			if err != nil {
				t.Fatalf("CreatePrimaryKey: %v", err)
			}
			wantConstraint := `CONSTRAINT "` + wantName + `" PRIMARY KEY`
			if !strings.Contains(createTable.SQL, wantConstraint) {
				t.Fatalf("CreateTable SQL = %q, want %q", createTable.SQL, wantConstraint)
			}
			if !strings.Contains(primaryKey.SQL, wantConstraint) {
				t.Fatalf("CreatePrimaryKey SQL = %q, want %q", primaryKey.SQL, wantConstraint)
			}
		})
	}
}

func TestPublicDDLRejectsExpressionIndexPrefixCombination(t *testing.T) {
	renderer, err := schema.NewRenderer(schema.Options{TargetDialect: "mysql", Schema: "app"})
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	_, err = renderer.CreateIndex(schema.TableRef{Name: "orders"}, schema.Index{
		Name:                "ix_orders_email_expression",
		Columns:             []string{"LOWER(email)"},
		ColumnExpressions:   []bool{true},
		ColumnPrefixLengths: []int{16},
	})
	if err == nil || !strings.Contains(err.Error(), "expression column 0 cannot have a prefix length") {
		t.Fatalf("CreateIndex error = %v, want expression-prefix validation error", err)
	}
}

func TestPublicDDLRejectsNegativeIndexPrefixLength(t *testing.T) {
	renderer, err := schema.NewRenderer(schema.Options{TargetDialect: "mysql", Schema: "app"})
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	_, err = renderer.CreateIndex(schema.TableRef{Name: "orders"}, schema.Index{
		Name:                "ix_orders_email",
		Columns:             []string{"email"},
		ColumnPrefixLengths: []int{-7},
	})
	if err == nil || !strings.Contains(err.Error(), "column prefix length -7 is negative") {
		t.Fatalf("CreateIndex error = %v, want negative prefix-length validation error", err)
	}
}

func TestPublicDDLFilteredIndexRejectsUnsupportedCrossDialectFunction(t *testing.T) {
	renderer, err := schema.NewRenderer(schema.Options{TargetDialect: "mssql", Schema: "dbo", SourceDialect: "postgres"})
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	_, err = renderer.CreateIndex(schema.TableRef{
		Name:    "orders",
		Columns: []schema.Column{{Name: "email", DataType: "varchar", MaxLength: 255}, {Name: "created_at", DataType: "timestamp"}},
	}, schema.Index{
		Name:    "ix_orders_active_email",
		Columns: []string{"email"},
		Filter:  "date_trunc('day', created_at) IS NOT NULL",
	})
	if err == nil || !strings.Contains(err.Error(), `unsupported SQL expression function "date_trunc"`) {
		t.Fatalf("CreateIndex error = %v, want cross-dialect function validation error", err)
	}
}

func TestPublicDDLFilteredIndexPreservesSameDialectFunction(t *testing.T) {
	renderer, err := schema.NewRenderer(schema.Options{TargetDialect: "postgres", Schema: "public", SourceDialect: "postgres"})
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	result, err := renderer.CreateIndex(schema.TableRef{
		Name:    "orders",
		Columns: []schema.Column{{Name: "email", DataType: "varchar", MaxLength: 255}, {Name: "created_at", DataType: "timestamp"}},
	}, schema.Index{
		Name:    "ix_orders_active_email",
		Columns: []string{"email"},
		Filter:  "date_trunc('day', created_at) IS NOT NULL",
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if !strings.Contains(result.SQL, "date_trunc('day', created_at)") {
		t.Fatalf("CreateIndex SQL = %q, want same-dialect date_trunc predicate", result.SQL)
	}
}

func TestPublicDDLSideObjectCapabilitiesAndUnsupportedErrors(t *testing.T) {
	cases := []struct {
		dialect                  string
		secondaryIndexes         bool
		standalonePrimaryKeys    bool
		standaloneForeignKeys    bool
		namedUniqueConstraints   bool
		checkConstraints         bool
		expressionKeys           bool
		prefixLengths            bool
		includeColumns           bool
		filteredIndexes          bool
		unsupportedSideArtifacts []string
	}{
		{
			dialect: "postgres", secondaryIndexes: true, standalonePrimaryKeys: true, standaloneForeignKeys: true, namedUniqueConstraints: true, checkConstraints: true,
			expressionKeys: true, includeColumns: true, filteredIndexes: true,
		},
		{
			dialect: "mssql", secondaryIndexes: true, standalonePrimaryKeys: true, standaloneForeignKeys: true, namedUniqueConstraints: true, checkConstraints: true,
			includeColumns: true, filteredIndexes: true,
		},
		{
			dialect: "mysql", secondaryIndexes: true, standalonePrimaryKeys: true, standaloneForeignKeys: true, namedUniqueConstraints: true, checkConstraints: true,
			expressionKeys: true, prefixLengths: true,
		},
		{
			dialect: "sqlite", secondaryIndexes: true,
			filteredIndexes:          true,
			unsupportedSideArtifacts: []string{"standalone primary keys", "standalone foreign keys", "named unique constraints", "check constraints"},
		},
		{
			dialect:                  "clickhouse",
			unsupportedSideArtifacts: []string{"secondary indexes", "standalone primary keys", "standalone foreign keys", "named unique constraints", "check constraints"},
		},
	}

	table := schema.TableRef{Name: "items", Columns: []schema.Column{{Name: "id", DataType: "int"}, {Name: "name", DataType: "varchar", MaxLength: 80}}}
	for _, tc := range cases {
		t.Run(tc.dialect, func(t *testing.T) {
			renderer, err := schema.NewRenderer(schema.Options{TargetDialect: tc.dialect})
			if err != nil {
				t.Fatalf("NewRenderer: %v", err)
			}
			got := renderer.Capabilities()
			if got.SecondaryIndexes != tc.secondaryIndexes || got.StandalonePrimaryKeys != tc.standalonePrimaryKeys || got.StandaloneForeignKeys != tc.standaloneForeignKeys ||
				got.NamedUniqueConstraints != tc.namedUniqueConstraints || got.CheckConstraints != tc.checkConstraints ||
				got.IndexExpressionKeys != tc.expressionKeys || got.IndexPrefixLengths != tc.prefixLengths ||
				got.IndexIncludeColumns != tc.includeColumns || got.FilteredIndexes != tc.filteredIndexes {
				t.Fatalf("Capabilities() = %#v", got)
			}

			for _, feature := range tc.unsupportedSideArtifacts {
				var err error
				switch feature {
				case "secondary indexes":
					_, err = renderer.CreateIndex(table, schema.Index{Name: "ix_items_name", Columns: []string{"name"}})
				case "standalone primary keys":
					_, err = renderer.CreatePrimaryKey(table, schema.PrimaryKey{Columns: []string{"id"}})
				case "standalone foreign keys":
					_, err = renderer.CreateForeignKey(table, schema.ForeignKey{Name: "fk_items_parent", Columns: []string{"id"}, RefTable: "parent", RefColumns: []string{"id"}})
				case "named unique constraints":
					_, err = renderer.CreateUniqueConstraint(table, schema.UniqueConstraint{Name: "uq_items_name", Columns: []string{"name"}})
				case "check constraints":
					_, err = renderer.CreateCheckConstraint(table, schema.CheckConstraint{Name: "ck_items_id", Expression: "id > 0"})
				}
				var unsupported *schema.UnsupportedFeatureError
				if !errors.As(err, &unsupported) || unsupported.Dialect != tc.dialect || unsupported.Feature != feature {
					t.Fatalf("%s error = %#v, want UnsupportedFeatureError for %q", feature, err, feature)
				}
			}
		})
	}
}

func TestPublicDDLSideObjectFeatureValidation(t *testing.T) {
	table := schema.TableRef{Name: "items", Columns: []schema.Column{{Name: "id", DataType: "int"}, {Name: "name", DataType: "varchar", MaxLength: 80}}}

	tests := []struct {
		name    string
		dialect string
		call    func(schema.Renderer) error
		feature string
	}{
		{
			name: "postgres prefix lengths", dialect: "postgres", feature: "index column prefix lengths",
			call: func(r schema.Renderer) error {
				_, err := r.CreateIndex(table, schema.Index{Name: "ix", Columns: []string{"name"}, ColumnPrefixLengths: []int{4}})
				return err
			},
		},
		{
			name: "mssql expression keys", dialect: "mssql", feature: "expression index key parts",
			call: func(r schema.Renderer) error {
				_, err := r.CreateIndex(table, schema.Index{Name: "ix", Columns: []string{"LOWER(name)"}, ColumnExpressions: []bool{true}})
				return err
			},
		},
		{
			name: "mysql include columns", dialect: "mysql", feature: "index include columns",
			call: func(r schema.Renderer) error {
				_, err := r.CreateIndex(table, schema.Index{Name: "ix", Columns: []string{"name"}, IncludeColumns: []string{"id"}})
				return err
			},
		},
		{
			name: "sqlite expression keys", dialect: "sqlite", feature: "expression index key parts",
			call: func(r schema.Renderer) error {
				_, err := r.CreateIndex(table, schema.Index{Name: "ix", Columns: []string{"LOWER(name)"}, ColumnExpressions: []bool{true}})
				return err
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			renderer, err := schema.NewRenderer(schema.Options{TargetDialect: tc.dialect})
			if err != nil {
				t.Fatalf("NewRenderer: %v", err)
			}
			err = tc.call(renderer)
			var unsupported *schema.UnsupportedFeatureError
			if !errors.As(err, &unsupported) || unsupported.Dialect != tc.dialect || unsupported.Feature != tc.feature {
				t.Fatalf("error = %#v, want UnsupportedFeatureError for %q", err, tc.feature)
			}
		})
	}

	postgres, err := schema.NewRenderer(schema.Options{TargetDialect: "postgres"})
	if err != nil {
		t.Fatalf("NewRenderer postgres: %v", err)
	}
	for _, tc := range []struct {
		name string
		call func() error
		want string
	}{
		{
			name: "index has too many expressions",
			call: func() error {
				_, err := postgres.CreateIndex(table, schema.Index{Name: "ix", Columns: []string{"name"}, ColumnExpressions: []bool{false, true}})
				return err
			},
			want: "column expression flags exceed columns",
		},
		{
			name: "unique name required",
			call: func() error {
				_, err := postgres.CreateUniqueConstraint(table, schema.UniqueConstraint{Columns: []string{"name"}})
				return err
			},
			want: "empty constraint name",
		},
		{
			name: "check expression required",
			call: func() error {
				_, err := postgres.CreateCheckConstraint(table, schema.CheckConstraint{Name: "ck_items_name"})
				return err
			},
			want: "empty expression",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want text %q", err, tc.want)
			}
		})
	}
}

func TestPublicDDLForeignKeyValidationAndActionCapabilities(t *testing.T) {
	table := schema.TableRef{Name: "items"}
	foreignKey := schema.ForeignKey{Name: "fk_items_parent", Columns: []string{"parent_id"}, RefTable: "parents", RefColumns: []string{"id"}}

	postgres, err := schema.NewRenderer(schema.Options{TargetDialect: "postgres"})
	if err != nil {
		t.Fatalf("NewRenderer postgres: %v", err)
	}
	foreignKey.OnDelete = schema.ReferentialAction("rename")
	if _, err := postgres.CreateForeignKey(table, foreignKey); err == nil || !strings.Contains(err.Error(), "invalid ON DELETE action") {
		t.Fatalf("CreateForeignKey invalid action error = %v", err)
	}
	foreignKey.OnDelete = ""
	foreignKey.RefColumns = nil
	if _, err := postgres.CreateForeignKey(table, foreignKey); err == nil || !strings.Contains(err.Error(), "1 columns but 0 referenced columns") {
		t.Fatalf("CreateForeignKey mismatched columns error = %v", err)
	}

	for _, tc := range []struct {
		name    string
		dialect string
		action  schema.ReferentialAction
		feature string
	}{
		{name: "mssql restrict", dialect: "mssql", action: schema.ReferentialActionRestrict, feature: "foreign-key RESTRICT actions"},
		{name: "mysql set default", dialect: "mysql", action: schema.ReferentialActionSetDefault, feature: "foreign-key SET DEFAULT actions"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			renderer, err := schema.NewRenderer(schema.Options{TargetDialect: tc.dialect})
			if err != nil {
				t.Fatalf("NewRenderer: %v", err)
			}
			fk := schema.ForeignKey{Name: "fk_items_parent", Columns: []string{"parent_id"}, RefTable: "parents", RefColumns: []string{"id"}, OnDelete: tc.action}
			_, err = renderer.CreateForeignKey(table, fk)
			var unsupported *schema.UnsupportedFeatureError
			if !errors.As(err, &unsupported) || unsupported.Dialect != tc.dialect || unsupported.Feature != tc.feature {
				t.Fatalf("CreateForeignKey error = %#v, want UnsupportedFeatureError for %q", err, tc.feature)
			}
		})
	}
}

func TestPublicDDLSideObjectAdapterParity(t *testing.T) {
	cases := []struct {
		name       string
		options    schema.Options
		coreSchema string
		table      schema.TableRef
		index      schema.Index
	}{
		{
			name: "postgres", options: schema.Options{TargetDialect: "postgres", Schema: "public", SourceDialect: "postgres"}, coreSchema: "public",
			table: schema.TableRef{Name: "Orders", Columns: []schema.Column{{Name: "ID", DataType: "int4"}, {Name: "Email", DataType: "varchar", MaxLength: 255}, {Name: "Total", DataType: "numeric", Precision: 12, Scale: 2}}},
			index: schema.Index{Name: "IX_Orders_Email", Columns: []string{"Email"}, IsUnique: true, IncludeColumns: []string{"ID"}, Filter: "Email IS NOT NULL"},
		},
		{
			name: "mssql", options: schema.Options{TargetDialect: "mssql", Schema: "dbo", SourceDialect: "mssql"}, coreSchema: "dbo",
			table: schema.TableRef{Name: "Orders", Columns: []schema.Column{{Name: "ID", DataType: "int"}, {Name: "Email", DataType: "varchar", MaxLength: 255}, {Name: "Total", DataType: "decimal", Precision: 12, Scale: 2}}},
			index: schema.Index{Name: "IX_Orders_Email", Columns: []string{"Email"}, IsUnique: true, IncludeColumns: []string{"ID"}, Filter: "Email IS NOT NULL"},
		},
		{
			name: "mysql", options: schema.Options{TargetDialect: "mysql", Schema: "crm", SourceDialect: "mysql"}, coreSchema: "crm",
			table: schema.TableRef{Name: "Orders", Columns: []schema.Column{{Name: "ID", DataType: "int"}, {Name: "Email", DataType: "varchar", MaxLength: 255}, {Name: "Total", DataType: "decimal", Precision: 12, Scale: 2}}},
			index: schema.Index{Name: "IX_Orders_Email", Columns: []string{"Email"}, IsUnique: true, ColumnPrefixLengths: []int{16}},
		},
		{
			name: "sqlite", options: schema.Options{TargetDialect: "sqlite", Schema: "ignored_by_sqlite", SourceDialect: "postgres"},
			table: schema.TableRef{Name: "orders", Columns: []schema.Column{{Name: "id", DataType: "int4"}, {Name: "email", DataType: "varchar", MaxLength: 255}}},
			index: schema.Index{Name: "ix_orders_email", Columns: []string{"email"}, IsUnique: true, Filter: "email IS NOT NULL"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			public, err := schema.NewRenderer(tc.options)
			if err != nil {
				t.Fatalf("NewRenderer: %v", err)
			}
			core, err := ddl.NewRenderer(tc.options.TargetDialect, tc.coreSchema, string(schema.UnknownTypeFail))
			if err != nil {
				t.Fatalf("ddl.NewRenderer: %v", err)
			}
			core = core.WithSource(tc.options.SourceDialect)
			coreTable := &driver.Table{Name: tc.table.Name, Columns: toDriverColumns(tc.table.Columns)}
			coreIndex := &driver.Index{
				Name: tc.index.Name, Columns: append([]string(nil), tc.index.Columns...),
				ColumnExpressions: append([]bool(nil), tc.index.ColumnExpressions...), ColumnPrefixLengths: append([]int(nil), tc.index.ColumnPrefixLengths...),
				IsUnique: tc.index.IsUnique, IncludeCols: append([]string(nil), tc.index.IncludeColumns...), Filter: tc.index.Filter,
			}

			got, err := public.CreateIndex(tc.table, tc.index)
			if err != nil {
				t.Fatalf("CreateIndex: %v", err)
			}
			want, err := core.CreateIndexDDL(coreTable, coreIndex)
			if err != nil {
				t.Fatalf("core CreateIndexDDL: %v", err)
			}
			if got.SQL != want {
				t.Fatalf("index adapter SQL = %q, want core SQL %q", got.SQL, want)
			}

			if tc.name == "sqlite" {
				return
			}
			gotPrimaryKey, err := public.CreatePrimaryKey(tc.table, schema.PrimaryKey{Columns: []string{"ID"}})
			if err != nil {
				t.Fatalf("CreatePrimaryKey: %v", err)
			}
			wantPrimaryKey, err := core.CreatePrimaryKeyDDL(coreTable, "pk_"+tc.table.Name, []string{"ID"})
			if err != nil {
				t.Fatalf("core CreatePrimaryKeyDDL: %v", err)
			}
			if gotPrimaryKey.SQL != wantPrimaryKey {
				t.Fatalf("primary-key adapter SQL = %q, want core SQL %q", gotPrimaryKey.SQL, wantPrimaryKey)
			}

			foreignKey := schema.ForeignKey{
				Name: "FK_Orders_Accounts", Columns: []string{"ID"}, RefSchema: "identity", RefTable: "Accounts", RefColumns: []string{"ID"},
				OnDelete: schema.ReferentialActionCascade, OnUpdate: schema.ReferentialActionSetNull,
			}
			gotForeignKey, err := public.CreateForeignKey(tc.table, foreignKey)
			if err != nil {
				t.Fatalf("CreateForeignKey: %v", err)
			}
			wantForeignKey, err := core.CreateForeignKeyDDL(coreTable, &driver.ForeignKey{
				Name: foreignKey.Name, Columns: append([]string(nil), foreignKey.Columns...), RefSchema: foreignKey.RefSchema, RefTable: foreignKey.RefTable,
				RefColumns: append([]string(nil), foreignKey.RefColumns...), OnDelete: string(foreignKey.OnDelete), OnUpdate: string(foreignKey.OnUpdate),
			})
			if err != nil {
				t.Fatalf("core CreateForeignKeyDDL: %v", err)
			}
			if gotForeignKey.SQL != wantForeignKey {
				t.Fatalf("foreign-key adapter SQL = %q, want core SQL %q", gotForeignKey.SQL, wantForeignKey)
			}

			gotUnique, err := public.CreateUniqueConstraint(tc.table, schema.UniqueConstraint{Name: "UQ_Orders_Email", Columns: []string{"Email"}})
			if err != nil {
				t.Fatalf("CreateUniqueConstraint: %v", err)
			}
			wantUnique, err := core.CreateUniqueConstraintDDL(coreTable, "UQ_Orders_Email", []string{"Email"})
			if err != nil {
				t.Fatalf("core CreateUniqueConstraintDDL: %v", err)
			}
			if gotUnique.SQL != wantUnique {
				t.Fatalf("unique-constraint adapter SQL = %q, want core SQL %q", gotUnique.SQL, wantUnique)
			}

			gotCheck, err := public.CreateCheckConstraint(tc.table, schema.CheckConstraint{Name: "CK_Orders_Total", Expression: "Total >= 0"})
			if err != nil {
				t.Fatalf("CreateCheckConstraint: %v", err)
			}
			wantCheck, err := core.CreateCheckConstraintDDL(coreTable, &driver.CheckConstraint{Name: "CK_Orders_Total", Definition: "Total >= 0"})
			if err != nil {
				t.Fatalf("core CreateCheckConstraintDDL: %v", err)
			}
			if gotCheck.SQL != wantCheck {
				t.Fatalf("check adapter SQL = %q, want core SQL %q", gotCheck.SQL, wantCheck)
			}
		})
	}
}

func toDriverColumns(columns []schema.Column) []driver.Column {
	out := make([]driver.Column, len(columns))
	for i, column := range columns {
		out[i] = driver.Column{
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
	return out
}
