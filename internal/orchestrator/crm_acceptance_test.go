package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"smt/internal/config"
	"smt/internal/driver"
	"smt/internal/pool"
)

type crmAcceptanceCase struct {
	Name       string
	Source     config.SourceConfig
	Target     config.TargetConfig
	SourceType string
	TargetType string
}

type crmMatrixReport struct {
	Cases    []crmCaseReport `json:"cases"`
	PassedAt string          `json:"passed_at,omitempty"`
}

type crmCaseReport struct {
	Name   string       `json:"name"`
	Source string       `json:"source"`
	Target string       `json:"target"`
	Report verifyReport `json:"report"`
}

func TestCRM_DeterministicAcceptanceMatrix(t *testing.T) {
	if os.Getenv("SMT_E2E_CRM") == "" {
		t.Skip("set SMT_E2E_CRM=1 with live CRM fixtures loaded to run")
	}
	if testing.Short() {
		t.Skip("skipping live CRM acceptance under -short")
	}
	// A host secrets file must not influence the no-AI CRM release gate.
	t.Setenv("SMT_SECRETS_FILE", os.DevNull+".missing")

	cases := crmAcceptanceCases(t)
	matrix := crmMatrixReport{}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			dataDir := t.TempDir()
			cfg := &config.Config{
				Source: tc.Source,
				Target: tc.Target,
				Migration: config.MigrationConfig{
					MaxSourceConnections:   2,
					MaxTargetConnections:   2,
					CreateIndexes:          true,
					CreateForeignKeys:      true,
					CreateCheckConstraints: true,
					DataDir:                dataDir,
				},
				SchemaGeneration: config.SchemaGenerationConfig{
					Mode:              driver.SchemaGenerationDeterministic,
					UnknownTypePolicy: "fail",
				},
				AIReview: config.AIReviewConfig{Enabled: boolPtr(false)},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()

			cleanCRMTarget(t, ctx, cfg)

			orch, err := NewWithOptions(cfg, Options{StateFile: filepath.Join(dataDir, "state.db")})
			if err != nil {
				t.Fatalf("NewWithOptions: %v", err)
			}
			defer orch.Close()

			if err := orch.Run(ctx); err != nil {
				t.Fatalf("orchestrator Run (CRM %s): %v", tc.Name, err)
			}
			assertManifestWritten(t, dataDir)

			srcTables := extractAll(t, ctx, &cfg.Source, cfg.Source.Schema)
			tgtTables := extractAll(t, ctx, sourceForTarget(cfg), cfg.Target.Schema)
			report := verifyCRMRoundTrip(t, tc.Name, srcTables, tgtTables, cfg.Target.Schema, tc.SourceType, tc.TargetType)
			matrix.Cases = append(matrix.Cases, crmCaseReport{
				Name:   tc.Name,
				Source: tc.SourceType,
				Target: tc.TargetType,
				Report: report,
			})
		})
	}

	if !t.Failed() {
		matrix.PassedAt = time.Now().UTC().Format(time.RFC3339)
	}
	writeCRMMatrixReport(t, matrix)
}

func crmAcceptanceCases(t *testing.T) []crmAcceptanceCase {
	t.Helper()

	dialects := []string{"mssql", "postgres", "mysql"}
	cases := make([]crmAcceptanceCase, 0, len(dialects)*len(dialects))
	for _, sourceType := range dialects {
		for _, targetType := range dialects {
			name := sourceType + "-to-" + targetType
			cases = append(cases, crmAcceptanceCase{
				Name:       name,
				Source:     crmSourceConfig(t, sourceType),
				Target:     crmTargetConfig(t, targetType, name),
				SourceType: sourceType,
				TargetType: targetType,
			})
		}
	}
	return cases
}

func crmSourceConfig(t *testing.T, dialect string) config.SourceConfig {
	t.Helper()
	switch dialect {
	case "mssql":
		return config.SourceConfig{
			Type:            "mssql",
			Host:            env("CRM_MSSQL_HOST", "localhost"),
			Port:            envInt(t, "CRM_MSSQL_PORT", 1433),
			Database:        env("CRM_MSSQL_DB", "CRM_MSSQL"),
			User:            env("CRM_MSSQL_USER", "sa"),
			Password:        env("CRM_MSSQL_PASS", "TestPass2024"),
			Schema:          env("CRM_MSSQL_SCHEMA", "dbo"),
			TrustServerCert: true,
		}
	case "postgres":
		return config.SourceConfig{
			Type:     "postgres",
			Host:     env("CRM_PG_HOST", "localhost"),
			Port:     envInt(t, "CRM_PG_PORT", 5432),
			Database: env("CRM_PG_DB", "crm_pg"),
			User:     env("CRM_PG_USER", "postgres"),
			Password: env("CRM_PG_PASS", "TestPass2024"),
			Schema:   env("CRM_PG_SCHEMA", "public"),
			SSLMode:  "disable",
		}
	case "mysql":
		return config.SourceConfig{
			Type:     "mysql",
			Host:     env("CRM_MYSQL_HOST", "localhost"),
			Port:     envInt(t, "CRM_MYSQL_PORT", 3306),
			Database: env("CRM_MYSQL_DB", "crm_mysql"),
			User:     env("CRM_MYSQL_USER", "root"),
			Password: env("CRM_MYSQL_PASS", "TestPass2024"),
			Schema:   env("CRM_MYSQL_SCHEMA", "crm_mysql"),
		}
	default:
		t.Fatalf("unsupported CRM source dialect %q", dialect)
		return config.SourceConfig{}
	}
}

func crmTargetConfig(t *testing.T, dialect, caseName string) config.TargetConfig {
	t.Helper()
	schema := crmTargetSchema(caseName)
	switch dialect {
	case "mssql":
		return config.TargetConfig{
			Type:            "mssql",
			Host:            env("CRM_MSSQL_HOST", "localhost"),
			Port:            envInt(t, "CRM_MSSQL_PORT", 1433),
			Database:        env("CRM_MSSQL_TARGET_DB", "tempdb"),
			User:            env("CRM_MSSQL_USER", "sa"),
			Password:        env("CRM_MSSQL_PASS", "TestPass2024"),
			Schema:          schema,
			TrustServerCert: true,
		}
	case "postgres":
		return config.TargetConfig{
			Type:     "postgres",
			Host:     env("CRM_PG_HOST", "localhost"),
			Port:     envInt(t, "CRM_PG_PORT", 5432),
			Database: env("CRM_PG_TARGET_DB", "postgres"),
			User:     env("CRM_PG_USER", "postgres"),
			Password: env("CRM_PG_PASS", "TestPass2024"),
			Schema:   schema,
			SSLMode:  "disable",
		}
	case "mysql":
		return config.TargetConfig{
			Type:     "mysql",
			Host:     env("CRM_MYSQL_HOST", "localhost"),
			Port:     envInt(t, "CRM_MYSQL_PORT", 3306),
			Database: env("CRM_MYSQL_ADMIN_DB", "mysql"),
			User:     env("CRM_MYSQL_USER", "root"),
			Password: env("CRM_MYSQL_PASS", "TestPass2024"),
			Schema:   schema,
		}
	default:
		t.Fatalf("unsupported CRM target dialect %q", dialect)
		return config.TargetConfig{}
	}
}

func crmTargetSchema(caseName string) string {
	envName := "CRM_" + strings.ToUpper(strings.ReplaceAll(caseName, "-", "_")) + "_TARGET_SCHEMA"
	return env(envName, "crm_accept_"+strings.ReplaceAll(caseName, "-", "_"))
}

func cleanCRMTarget(t *testing.T, ctx context.Context, cfg *config.Config) {
	t.Helper()
	w, err := pool.NewTargetPool(&cfg.Target, 2, cfg.Source.Type, cfg.SchemaGeneration.UnknownTypePolicy)
	if err != nil {
		t.Fatalf("opening target for cleanup: %v", err)
	}
	defer w.Close()

	schema := cfg.Target.Schema
	switch cfg.Target.Type {
	case "postgres":
		if _, err := w.DB().ExecContext(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", quoteDoubleIdent(schema))); err != nil {
			t.Fatalf("dropping postgres target schema %s: %v", schema, err)
		}
	case "mysql":
		if _, err := w.DB().ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", quoteMySQLIdent(schema))); err != nil {
			t.Fatalf("dropping mysql target database %s: %v", schema, err)
		}
	case "mssql":
		if _, err := w.DB().ExecContext(ctx, mssqlDropSchemaSQL(schema)); err != nil {
			t.Fatalf("dropping mssql target schema %s: %v", schema, err)
		}
	default:
		t.Fatalf("unsupported CRM target type %q", cfg.Target.Type)
	}
}

func verifyCRMRoundTrip(t *testing.T, name string, src, tgt []driver.Table, schema, sourceDialect, targetDialect string) verifyReport {
	t.Helper()
	rep := verifyReport{Schema: schema, SourceTables: len(src), TargetTables: len(tgt), Counts: map[string]int{}}

	tgtByName := map[string]driver.Table{}
	for _, tt := range tgt {
		tgtByName[strings.ToLower(tt.Name)] = tt
	}
	srcByName := map[string]bool{}
	for _, s := range src {
		srcByName[strings.ToLower(s.Name)] = true
	}
	for _, tt := range tgt {
		if !srcByName[strings.ToLower(tt.Name)] {
			rep.Failures = append(rep.Failures, fmt.Sprintf("unexpected extra table %s on target", tt.Name))
		}
	}

	totalPK := 0
	totalFK := 0
	totalChecks := 0
	for _, s := range src {
		key := strings.ToLower(s.Name)
		tt, ok := tgtByName[key]
		if !ok {
			rep.Failures = append(rep.Failures, fmt.Sprintf("table %s missing on target", s.Name))
			continue
		}
		tr := tableReport{Name: key, Columns: len(tt.Columns), ForeignKeys: len(tt.ForeignKeys), Indexes: len(tt.Indexes)}
		if len(tt.PrimaryKey) > 0 {
			tr.PrimaryKey = strings.Join(lowerSorted(tt.PrimaryKey), ",")
		}

		if len(s.Columns) != len(tt.Columns) {
			rep.Failures = append(rep.Failures,
				fmt.Sprintf("%s column count: source=%d target=%d", s.Name, len(s.Columns), len(tt.Columns)))
		}
		for _, d := range driver.CompareColumns(s.Columns, tt.Columns, sourceDialect, targetDialect) {
			rep.Failures = append(rep.Failures, fmt.Sprintf("%s.%s", s.Name, d.String()))
		}
		if !sameColsCI(s.PrimaryKey, tt.PrimaryKey) {
			rep.Failures = append(rep.Failures,
				fmt.Sprintf("%s primary key: source=%v target=%v", s.Name, s.PrimaryKey, tt.PrimaryKey))
		}

		tgtIdx := map[string]bool{}
		for _, idx := range tt.Indexes {
			tgtIdx[colSetKey(idx.Columns)] = true
		}
		for _, idx := range s.Indexes {
			if !tgtIdx[colSetKey(idx.Columns)] {
				rep.Failures = append(rep.Failures,
					fmt.Sprintf("%s index on (%s) missing on target", s.Name, strings.Join(lowerSorted(idx.Columns), ",")))
			}
		}

		tgtFK := map[string]bool{}
		for _, fk := range tt.ForeignKeys {
			tgtFK[fkKey(fk)] = true
		}
		for _, fk := range s.ForeignKeys {
			if !tgtFK[fkKey(fk)] {
				rep.Failures = append(rep.Failures,
					fmt.Sprintf("%s FK on (%s)->%s missing on target", s.Name, strings.Join(lowerSorted(fk.Columns), ","), strings.ToLower(fk.RefTable)))
			}
		}

		if len(tt.CheckConstraints) < len(s.CheckConstraints) {
			rep.Failures = append(rep.Failures,
				fmt.Sprintf("%s check constraints: source=%d target=%d", s.Name, len(s.CheckConstraints), len(tt.CheckConstraints)))
		}

		totalPK += len(tt.PrimaryKey)
		totalFK += len(tt.ForeignKeys)
		totalChecks += len(tt.CheckConstraints)
		rep.Tables = append(rep.Tables, tr)
	}
	rep.Counts["source_pk_tables"] = countWithPK(src)
	rep.Counts["target_pk_columns"] = totalPK
	rep.Counts["target_fk_count"] = totalFK
	rep.Counts["target_check_count"] = totalChecks

	for _, f := range rep.Failures {
		t.Errorf("CRM %s verify: %s", name, f)
	}
	if len(rep.Failures) == 0 {
		rep.PassedAt = time.Now().UTC().Format(time.RFC3339)
	}
	return rep
}

func sourceForTarget(cfg *config.Config) *config.SourceConfig {
	return &config.SourceConfig{
		Type:            cfg.Target.Type,
		Host:            cfg.Target.Host,
		Port:            cfg.Target.Port,
		Database:        cfg.Target.Database,
		User:            cfg.Target.User,
		Password:        cfg.Target.Password,
		Schema:          cfg.Target.Schema,
		SSLMode:         cfg.Target.SSLMode,
		TrustServerCert: cfg.Target.TrustServerCert,
		Encrypt:         cfg.Target.Encrypt,
		PacketSize:      cfg.Target.PacketSize,
	}
}

func writeCRMMatrixReport(t *testing.T, report crmMatrixReport) {
	t.Helper()
	reportDir := os.Getenv("CRM_REPORT_DIR")
	if reportDir == "" {
		return
	}
	if err := os.MkdirAll(reportDir, 0700); err != nil {
		t.Fatalf("creating CRM report dir: %v", err)
	}
	blob, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("marshaling CRM report: %v", err)
	}
	path := filepath.Join(reportDir, "crm_acceptance_matrix.json")
	if err := os.WriteFile(path, blob, 0600); err != nil {
		t.Fatalf("writing CRM report: %v", err)
	}
	t.Logf("CRM acceptance report written to %s", path)
}

func quoteDoubleIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func quoteMySQLIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func mssqlDropSchemaSQL(schema string) string {
	schemaLiteral := "N'" + strings.ReplaceAll(schema, "'", "''") + "'"
	return fmt.Sprintf(`
DECLARE @schema sysname = %s;
DECLARE @sql nvarchar(max) = N'';

SELECT @sql = @sql + N'ALTER TABLE ' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + N'.' + QUOTENAME(t.name) + N' DROP CONSTRAINT ' + QUOTENAME(fk.name) + N';'
FROM sys.foreign_keys fk
JOIN sys.tables t ON t.object_id = fk.parent_object_id
WHERE SCHEMA_NAME(t.schema_id) = @schema;
IF @sql <> N'' EXEC sp_executesql @sql;

SET @sql = N'';
SELECT @sql = @sql + N'DROP TABLE ' + QUOTENAME(SCHEMA_NAME(schema_id)) + N'.' + QUOTENAME(name) + N';'
FROM sys.tables
WHERE SCHEMA_NAME(schema_id) = @schema;
IF @sql <> N'' EXEC sp_executesql @sql;

IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = @schema)
BEGIN
  SET @sql = N'DROP SCHEMA ' + QUOTENAME(@schema);
  EXEC sp_executesql @sql;
END
`, schemaLiteral)
}

func boolPtr(v bool) *bool {
	return &v
}
