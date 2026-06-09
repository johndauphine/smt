package ddl

import (
	"strings"
	"testing"

	"smt/internal/driver"
)

func TestRenderer_CreateTableMSSQL(t *testing.T) {
	renderer, err := NewRenderer("mssql", "dbo", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	table := crmCompanyTable()

	sql, columnTypes, err := renderer.CreateTableDDL(&table)
	if err != nil {
		t.Fatalf("CreateTableDDL: %v", err)
	}
	for _, want := range []string{
		`CREATE TABLE [dbo].[Companies]`,
		`[CompanyId] INT IDENTITY(1,1) NOT NULL`,
		`[Name] VARCHAR(80) NOT NULL`,
		`[IsActive] BIT NOT NULL DEFAULT 1`,
		`[CreatedAt] DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()`,
		`CONSTRAINT [pk_Companies] PRIMARY KEY ([CompanyId])`,
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected SQL to contain %q, got:\n%s", want, sql)
		}
	}
	if columnTypes["Name"] != "VARCHAR(80)" {
		t.Fatalf("Name column type = %q", columnTypes["Name"])
	}
}

func TestRenderer_CreateTableMySQL(t *testing.T) {
	renderer, err := NewRenderer("mysql", "crm", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	table := crmCompanyTable()

	sql, columnTypes, err := renderer.CreateTableDDL(&table)
	if err != nil {
		t.Fatalf("CreateTableDDL: %v", err)
	}
	for _, want := range []string{
		"CREATE TABLE `crm`.`Companies`",
		"`CompanyId` INT AUTO_INCREMENT NOT NULL",
		"`Name` VARCHAR(80) NOT NULL",
		"`IsActive` TINYINT(1) NOT NULL DEFAULT 1",
		"`CreatedAt` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP",
		"CONSTRAINT `pk_Companies` PRIMARY KEY (`CompanyId`)",
		"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected SQL to contain %q, got:\n%s", want, sql)
		}
	}
	if columnTypes["Name"] != "VARCHAR(80)" {
		t.Fatalf("Name column type = %q", columnTypes["Name"])
	}
}

func TestRenderer_FinalizationMSSQL(t *testing.T) {
	renderer, err := NewRenderer("mssql", "dbo", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	table := crmCompanyTable()

	idxSQL, err := renderer.CreateIndexDDL(&table, &driver.Index{Name: "IX_Companies_Name", Columns: []string{"Name"}, IsUnique: true})
	if err != nil {
		t.Fatalf("CreateIndexDDL: %v", err)
	}
	fkSQL, err := renderer.CreateForeignKeyDDL(&driver.Table{Name: "Contacts"}, &driver.ForeignKey{
		Name: "FK_Contacts_Companies", Columns: []string{"CompanyId"}, RefTable: "Companies", RefColumns: []string{"CompanyId"}, OnDelete: "CASCADE",
	})
	if err != nil {
		t.Fatalf("CreateForeignKeyDDL: %v", err)
	}
	checkSQL, err := renderer.CreateCheckConstraintDDL(&table, &driver.CheckConstraint{Name: "CK_Companies_Active", Definition: "([IsActive]=(1))"})
	if err != nil {
		t.Fatalf("CreateCheckConstraintDDL: %v", err)
	}

	assertEqualSQL(t, idxSQL, `CREATE UNIQUE INDEX [IX_Companies_Name] ON [dbo].[Companies] ([Name])`)
	assertEqualSQL(t, fkSQL, `ALTER TABLE [dbo].[Contacts] ADD CONSTRAINT [FK_Contacts_Companies] FOREIGN KEY ([CompanyId]) REFERENCES [dbo].[Companies] ([CompanyId]) ON DELETE CASCADE`)
	assertEqualSQL(t, checkSQL, `ALTER TABLE [dbo].[Companies] ADD CONSTRAINT [CK_Companies_Active] CHECK ([IsActive]=1)`)
}

func TestRenderer_FinalizationMySQL(t *testing.T) {
	renderer, err := NewRenderer("mysql", "crm", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	table := crmCompanyTable()

	idxSQL, err := renderer.CreateIndexDDL(&table, &driver.Index{Name: "IX_Companies_Name", Columns: []string{"Name"}, IsUnique: true})
	if err != nil {
		t.Fatalf("CreateIndexDDL: %v", err)
	}
	fkSQL, err := renderer.CreateForeignKeyDDL(&driver.Table{Name: "Contacts"}, &driver.ForeignKey{
		Name: "FK_Contacts_Companies", Columns: []string{"CompanyId"}, RefTable: "Companies", RefColumns: []string{"CompanyId"}, OnDelete: "CASCADE",
	})
	if err != nil {
		t.Fatalf("CreateForeignKeyDDL: %v", err)
	}
	checkSQL, err := renderer.CreateCheckConstraintDDL(&table, &driver.CheckConstraint{Name: "CK_Companies_Active", Definition: "([IsActive]=(1))"})
	if err != nil {
		t.Fatalf("CreateCheckConstraintDDL: %v", err)
	}

	assertEqualSQL(t, idxSQL, "CREATE UNIQUE INDEX `IX_Companies_Name` ON `crm`.`Companies` (`Name`)")
	assertEqualSQL(t, fkSQL, "ALTER TABLE `crm`.`Contacts` ADD CONSTRAINT `FK_Contacts_Companies` FOREIGN KEY (`CompanyId`) REFERENCES `crm`.`Companies` (`CompanyId`) ON DELETE CASCADE")
	assertEqualSQL(t, checkSQL, "ALTER TABLE `crm`.`Companies` ADD CONSTRAINT `CK_Companies_Active` CHECK (`IsActive`=1)")
}

func TestRenderer_DropIndexPostgresWithoutSchema(t *testing.T) {
	renderer, err := NewRenderer("postgres", "", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}

	got := renderer.DropIndexDDL("users", "ix_users_email")

	assertEqualSQL(t, got, `DROP INDEX "ix_users_email"`)
}

func TestRenderer_DropColumnDefaultMSSQL(t *testing.T) {
	renderer, err := NewRenderer("mssql", "dbo", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}

	got := renderer.DropColumnDefaultDDL("Users", "CreatedAt")

	for _, want := range []string{
		"DECLARE @constraintName sysname",
		"sys.default_constraints",
		"s.name = N'dbo'",
		"t.name = N'Users'",
		"c.name = N'CreatedAt'",
		"ALTER TABLE [dbo].[Users] DROP CONSTRAINT",
		"QUOTENAME(@constraintName)",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected SQL to contain %q, got:\n%s", want, got)
		}
	}
}

func TestRenderer_MySQLEnumSetTypesPreserveValues(t *testing.T) {
	renderer, err := NewRenderer("mysql", "crm", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}

	enumType, err := renderer.ColumnType(driver.Column{
		Name:       "CustomerType",
		DataType:   "enum",
		EnumValues: []string{"individual", "company", "owner's"},
	})
	if err != nil {
		t.Fatalf("ColumnType enum: %v", err)
	}
	assertEqualSQL(t, enumType, "ENUM('individual','company','owner''s')")

	setType, err := renderer.ColumnType(driver.Column{
		Name:       "Flags",
		DataType:   "set",
		EnumValues: []string{"vip", "wholesale"},
	})
	if err != nil {
		t.Fatalf("ColumnType set: %v", err)
	}
	assertEqualSQL(t, setType, "SET('vip','wholesale')")
}

func TestRenderer_EnumSetTypesMapTextForOtherTargets(t *testing.T) {
	mssqlRenderer, err := NewRenderer("mssql", "dbo", "fail")
	if err != nil {
		t.Fatalf("NewRenderer mssql: %v", err)
	}
	mssqlType, err := mssqlRenderer.ColumnType(driver.Column{
		Name:       "CustomerType",
		DataType:   "enum",
		EnumValues: []string{"individual", "government"},
	})
	if err != nil {
		t.Fatalf("ColumnType mssql enum: %v", err)
	}
	assertEqualSQL(t, mssqlType, "NVARCHAR(10)")

	postgresRenderer, err := NewRenderer("postgres", "public", "fail")
	if err != nil {
		t.Fatalf("NewRenderer postgres: %v", err)
	}
	postgresType, err := postgresRenderer.ColumnType(driver.Column{
		Name:       "CustomerType",
		DataType:   "enum",
		EnumValues: []string{"individual", "government"},
	})
	if err != nil {
		t.Fatalf("ColumnType postgres enum: %v", err)
	}
	assertEqualSQL(t, postgresType, "text")
}

func TestRenderer_PostgresArrayAliasesMapForMSSQLAndMySQL(t *testing.T) {
	mssqlRenderer, err := NewRenderer("mssql", "dbo", "fail")
	if err != nil {
		t.Fatalf("NewRenderer mssql: %v", err)
	}
	mssqlType, err := mssqlRenderer.ColumnType(driver.Column{Name: "Tags", DataType: "_text"})
	if err != nil {
		t.Fatalf("ColumnType mssql array: %v", err)
	}
	assertEqualSQL(t, mssqlType, "NVARCHAR(MAX)")

	mysqlRenderer, err := NewRenderer("mysql", "crm", "fail")
	if err != nil {
		t.Fatalf("NewRenderer mysql: %v", err)
	}
	mysqlType, err := mysqlRenderer.ColumnType(driver.Column{Name: "Tags", DataType: "_text"})
	if err != nil {
		t.Fatalf("ColumnType mysql array: %v", err)
	}
	assertEqualSQL(t, mysqlType, "JSON")
}

func TestRenderer_MySQLTextAliasesMapForPostgresAndMSSQL(t *testing.T) {
	postgresRenderer, err := NewRenderer("postgres", "public", "fail")
	if err != nil {
		t.Fatalf("NewRenderer postgres: %v", err)
	}
	postgresType, err := postgresRenderer.ColumnType(driver.Column{Name: "Notes", DataType: "mediumtext"})
	if err != nil {
		t.Fatalf("ColumnType postgres mediumtext: %v", err)
	}
	assertEqualSQL(t, postgresType, "text")

	mssqlRenderer, err := NewRenderer("mssql", "dbo", "fail")
	if err != nil {
		t.Fatalf("NewRenderer mssql: %v", err)
	}
	mssqlType, err := mssqlRenderer.ColumnType(driver.Column{Name: "Notes", DataType: "mediumtext"})
	if err != nil {
		t.Fatalf("ColumnType mssql mediumtext: %v", err)
	}
	assertEqualSQL(t, mssqlType, "NVARCHAR(MAX)")
}

func TestRenderer_MySQLTargetPreservesLargeTextAndOnUpdate(t *testing.T) {
	renderer, err := NewRenderer("mysql", "crm", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}

	for _, tc := range []struct {
		name string
		col  driver.Column
		want string
	}{
		{name: "nvarchar max", col: driver.Column{Name: "Notes", DataType: "nvarchar", MaxLength: -1}, want: "LONGTEXT"},
		{name: "unbounded varchar", col: driver.Column{Name: "Notes", DataType: "varchar", MaxLength: 0}, want: "TEXT"},
		{name: "mediumtext", col: driver.Column{Name: "Notes", DataType: "mediumtext"}, want: "MEDIUMTEXT"},
		{name: "longtext", col: driver.Column{Name: "Notes", DataType: "longtext"}, want: "LONGTEXT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := renderer.ColumnType(tc.col)
			if err != nil {
				t.Fatalf("ColumnType: %v", err)
			}
			assertEqualSQL(t, got, tc.want)
		})
	}

	def, _, err := renderer.ColumnDefinition(driver.Column{
		Name:               "UpdatedAt",
		DataType:           "datetime",
		IsNullable:         false,
		DefaultExpression:  "CURRENT_TIMESTAMP",
		OnUpdateExpression: "CURRENT_TIMESTAMP",
	})
	if err != nil {
		t.Fatalf("ColumnDefinition: %v", err)
	}
	assertEqualSQL(t, def, "`UpdatedAt` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)")
}

func TestRenderer_RowversionAndUnsignedTypes(t *testing.T) {
	mssqlRenderer, err := NewRenderer("mssql", "dbo", "fail")
	if err != nil {
		t.Fatalf("NewRenderer mssql: %v", err)
	}
	mssqlRowversion, err := mssqlRenderer.ColumnType(driver.Column{Name: "Version", DataType: "timestamp", MaxLength: 8})
	if err != nil {
		t.Fatalf("ColumnType mssql rowversion: %v", err)
	}
	assertEqualSQL(t, mssqlRowversion, "ROWVERSION")

	mssqlUnsigned, err := mssqlRenderer.ColumnType(driver.Column{Name: "Count", DataType: "int", IsUnsigned: true})
	if err != nil {
		t.Fatalf("ColumnType mssql unsigned: %v", err)
	}
	assertEqualSQL(t, mssqlUnsigned, "BIGINT")

	mysqlRenderer, err := NewRenderer("mysql", "crm", "fail")
	if err != nil {
		t.Fatalf("NewRenderer mysql: %v", err)
	}
	mysqlRowversion, err := mysqlRenderer.ColumnType(driver.Column{Name: "Version", DataType: "timestamp", MaxLength: 8})
	if err != nil {
		t.Fatalf("ColumnType mysql rowversion: %v", err)
	}
	assertEqualSQL(t, mysqlRowversion, "BINARY(8)")

	mysqlUnsigned, err := mysqlRenderer.ColumnType(driver.Column{Name: "Count", DataType: "int", IsUnsigned: true})
	if err != nil {
		t.Fatalf("ColumnType mysql unsigned: %v", err)
	}
	assertEqualSQL(t, mysqlUnsigned, "INT UNSIGNED")

	postgresRenderer, err := NewRenderer("postgres", "public", "fail")
	if err != nil {
		t.Fatalf("NewRenderer postgres: %v", err)
	}
	postgresUnsigned, err := postgresRenderer.ColumnType(driver.Column{Name: "Count", DataType: "int", IsUnsigned: true})
	if err != nil {
		t.Fatalf("ColumnType postgres unsigned: %v", err)
	}
	assertEqualSQL(t, postgresUnsigned, "bigint")

	postgresUnsignedBigint, err := postgresRenderer.ColumnType(driver.Column{Name: "Count", DataType: "bigint", IsUnsigned: true})
	if err != nil {
		t.Fatalf("ColumnType postgres unsigned bigint: %v", err)
	}
	assertEqualSQL(t, postgresUnsignedBigint, "numeric(20,0)")

	postgresUnsignedBigintIdentity, err := postgresRenderer.ColumnType(driver.Column{Name: "ID", DataType: "bigint", IsUnsigned: true, IsIdentity: true})
	if err != nil {
		t.Fatalf("ColumnType postgres unsigned bigint identity: %v", err)
	}
	assertEqualSQL(t, postgresUnsignedBigintIdentity, "bigint GENERATED BY DEFAULT AS IDENTITY")
}

func TestRenderer_MySQLExpressionNormalizesForMSSQL(t *testing.T) {
	renderer, err := NewRenderer("mssql", "dbo", "fail")
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}

	got, err := renderer.Expression("((`color` is null) or regexp_like(`color`,_utf8mb4'^#[0-9A-Fa-f]{6}$'))", nil)
	if err != nil {
		t.Fatalf("Expression: %v", err)
	}
	assertEqualSQL(t, got, "([color] is null) or ([color] LIKE '#[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]')")
}

func TestRenderer_PostgresExpressionsNormalizeForMSSQLAndMySQL(t *testing.T) {
	mssqlRenderer, err := NewRenderer("mssql", "dbo", "fail")
	if err != nil {
		t.Fatalf("NewRenderer mssql: %v", err)
	}
	mssqlConcat, err := mssqlRenderer.Expression("((first_name || ' ') || last_name)", nil)
	if err != nil {
		t.Fatalf("Expression mssql concat: %v", err)
	}
	assertEqualSQL(t, mssqlConcat, "first_name + ' ' + last_name")

	mysqlRenderer, err := NewRenderer("mysql", "crm", "fail")
	if err != nil {
		t.Fatalf("NewRenderer mysql: %v", err)
	}
	mysqlConcat, err := mysqlRenderer.Expression("((first_name || ' ') || last_name)", nil)
	if err != nil {
		t.Fatalf("Expression mysql concat: %v", err)
	}
	assertEqualSQL(t, mysqlConcat, "CONCAT(first_name, ' ', last_name)")

	mysqlCase, err := mysqlRenderer.Expression("CASE WHEN cost_price IS NULL THEN NULL::numeric ELSE unit_price END", nil)
	if err != nil {
		t.Fatalf("Expression mysql case: %v", err)
	}
	assertEqualSQL(t, mysqlCase, "CASE WHEN cost_price IS NULL THEN NULL ELSE unit_price END")

	anyArray := "((address_type)::text = ANY ((ARRAY['billing'::character varying, 'shipping'::character varying])::text[]))"
	mssqlAny, err := mssqlRenderer.Expression(anyArray, nil)
	if err != nil {
		t.Fatalf("Expression mssql any array: %v", err)
	}
	assertEqualSQL(t, mssqlAny, "address_type IN ('billing', 'shipping')")

	mysqlAny, err := mysqlRenderer.Expression(anyArray, nil)
	if err != nil {
		t.Fatalf("Expression mysql any array: %v", err)
	}
	assertEqualSQL(t, mysqlAny, "address_type IN ('billing', 'shipping')")

	mssqlEmail, err := mssqlRenderer.Expression("((email) ~ '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$')", nil)
	if err != nil {
		t.Fatalf("Expression mssql email regex: %v", err)
	}
	assertEqualSQL(t, mssqlEmail, "(email LIKE '%_@_%._%' AND email NOT LIKE '% %')")

	mysqlEmail, err := mysqlRenderer.Expression("((email) ~ '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$')", nil)
	if err != nil {
		t.Fatalf("Expression mysql email regex: %v", err)
	}
	assertEqualSQL(t, mysqlEmail, "(email REGEXP '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$')")

	mssqlColor, err := mssqlRenderer.Expression("((color) ~ '^#[0-9A-Fa-f]{6}$')", nil)
	if err != nil {
		t.Fatalf("Expression mssql color regex: %v", err)
	}
	assertEqualSQL(t, mssqlColor, "(color LIKE '#[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]')")

	identityCheck := "((((customer_type)::text = 'individual'::text) AND (first_name IS NOT NULL) AND (last_name IS NOT NULL)) OR (((customer_type)::text = ANY ((ARRAY['company'::character varying, 'government'::character varying])::text[])) AND (company_name IS NOT NULL)))"
	mssqlIdentity, err := mssqlRenderer.Expression(identityCheck, nil)
	if err != nil {
		t.Fatalf("Expression mssql identity check: %v", err)
	}
	assertEqualSQL(t, mssqlIdentity, "(((customer_type) = 'individual') AND (first_name IS NOT NULL) AND (last_name IS NOT NULL)) OR (customer_type IN ('company', 'government')) AND (company_name IS NOT NULL)")
}

func TestRenderer_DefaultsNormalizePostgresCastsAndTimestampPrecision(t *testing.T) {
	mssqlRenderer, err := NewRenderer("mssql", "dbo", "fail")
	if err != nil {
		t.Fatalf("NewRenderer mssql: %v", err)
	}
	mssqlJSON, err := mssqlRenderer.ColumnDefault(driver.Column{Name: "Settings", DataType: "jsonb", DefaultExpression: "'{}'::jsonb"})
	if err != nil {
		t.Fatalf("ColumnDefault mssql json: %v", err)
	}
	assertEqualSQL(t, mssqlJSON, "'{}'")

	mssqlText, err := mssqlRenderer.ColumnDefault(driver.Column{Name: "CustomerType", DataType: "character varying", DefaultExpression: "'individual'::character varying"})
	if err != nil {
		t.Fatalf("ColumnDefault mssql text: %v", err)
	}
	assertEqualSQL(t, mssqlText, "'individual'")

	mssqlTime, err := mssqlRenderer.ColumnDefault(driver.Column{Name: "CreatedAt", DataType: "datetime", DefaultExpression: "current_timestamp(6)"})
	if err != nil {
		t.Fatalf("ColumnDefault mssql timestamp: %v", err)
	}
	assertEqualSQL(t, mssqlTime, "SYSUTCDATETIME()")

	mysqlRenderer, err := NewRenderer("mysql", "crm", "fail")
	if err != nil {
		t.Fatalf("NewRenderer mysql: %v", err)
	}
	mysqlJSON, err := mysqlRenderer.ColumnDefault(driver.Column{Name: "Settings", DataType: "jsonb", DefaultExpression: "'{}'::jsonb"})
	if err != nil {
		t.Fatalf("ColumnDefault mysql json: %v", err)
	}
	assertEqualSQL(t, mysqlJSON, "(JSON_OBJECT())")

	mysqlArray, err := mysqlRenderer.ColumnDefault(driver.Column{Name: "Skills", DataType: "_text", DefaultExpression: "'{}'::text[]"})
	if err != nil {
		t.Fatalf("ColumnDefault mysql array: %v", err)
	}
	assertEqualSQL(t, mysqlArray, "(JSON_ARRAY())")

	mysqlTime, err := mysqlRenderer.ColumnDefault(driver.Column{Name: "CreatedAt", DataType: "datetime2", DefaultExpression: "SYSUTCDATETIME()"})
	if err != nil {
		t.Fatalf("ColumnDefault mysql timestamp: %v", err)
	}
	assertEqualSQL(t, mysqlTime, "CURRENT_TIMESTAMP(6)")
}

func crmCompanyTable() driver.Table {
	return driver.Table{
		Name:       "Companies",
		PrimaryKey: []string{"CompanyId"},
		Columns: []driver.Column{
			{Name: "CompanyId", DataType: "int", IsNullable: false, IsIdentity: true},
			{Name: "Name", DataType: "varchar", MaxLength: 80, IsNullable: false},
			{Name: "IsActive", DataType: "bit", IsNullable: false, DefaultExpression: "((1))"},
			{Name: "CreatedAt", DataType: "datetime2", IsNullable: false, DefaultExpression: "SYSUTCDATETIME()"},
		},
	}
}

func assertEqualSQL(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Fatalf("SQL = %q, want %q", got, want)
	}
}
