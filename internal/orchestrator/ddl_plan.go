package orchestrator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"smt/internal/driver"
	pgddl "smt/internal/driver/postgres"
	"smt/internal/logging"
	"smt/internal/schemadiff"
	"smt/internal/source"
)

// GenerateDDL renders the full create-schema plan without opening or applying
// to a target database. The target block is treated as a dialect descriptor:
// target.type and target.schema influence the SQL, while host/database/user are
// only needed by apply paths. If outputPath is non-empty, it is written before
// the run is marked successful.
func (o *Orchestrator) GenerateDDL(ctx context.Context, outputPath string) (schemadiff.Plan, string, error) {
	runID := o.opts.RunID
	if runID == "" {
		runID = uuid.NewString()
	}

	if err := o.state.CreateRun(runID, o.config.Source.Schema, o.config.Target.Schema, o.config.Sanitized(), o.runProfile, o.runConfig); err != nil {
		return schemadiff.Plan{}, "", fmt.Errorf("recording run start: %w", err)
	}

	start := time.Now()
	_ = o.notifier.MigrationStarted(runID, o.config.Source.Type, o.config.Target.Type, 0)

	plan, err := o.renderDDLPlan(ctx, runID)
	if err != nil {
		_ = o.state.CompleteRun(runID, "failed", err.Error())
		_ = o.notifier.MigrationFailed(runID, err, time.Since(start))
		return schemadiff.Plan{}, runID, err
	}

	sql := plan.SQL()
	if err := o.writeSQLArtifact(runID, "schema.sql", sql); err != nil {
		_ = o.state.CompleteRun(runID, "failed", err.Error())
		_ = o.notifier.MigrationFailed(runID, err, time.Since(start))
		return schemadiff.Plan{}, runID, fmt.Errorf("writing DDL artifact: %w", err)
	}
	if strings.TrimSpace(outputPath) != "" {
		if err := os.WriteFile(outputPath, []byte(sql), 0600); err != nil {
			_ = o.state.CompleteRun(runID, "failed", err.Error())
			_ = o.notifier.MigrationFailed(runID, err, time.Since(start))
			return schemadiff.Plan{}, runID, fmt.Errorf("writing output file: %w", err)
		}
	}

	dur := time.Since(start)
	_ = o.state.CompleteRun(runID, "success", "")
	_ = o.notifier.MigrationCompleted(runID, start, dur, len(o.tables), 0, 0)
	logging.Info("DDL generation complete in %s (%d tables)", dur.Round(time.Millisecond), len(o.tables))
	return plan, runID, nil
}

func (o *Orchestrator) renderDDLPlan(ctx context.Context, runID string) (schemadiff.Plan, error) {
	if err := o.ExtractSchema(ctx, runID); err != nil {
		return schemadiff.Plan{}, err
	}

	renderer, err := o.newCreateDDLRenderer()
	if err != nil {
		return schemadiff.Plan{}, err
	}

	plan := schemadiff.Plan{}
	if ddl, err := renderCreateSchemaDDL(renderer.targetType, renderer.targetSchema); err != nil {
		return schemadiff.Plan{}, err
	} else if ddl != "" {
		plan.Statements = append(plan.Statements, schemadiff.Statement{
			Description: fmt.Sprintf("create target schema %s", renderer.targetSchema),
			SQL:         stripTrailingSemicolons(ddl),
			Risk:        schemadiff.RiskSafe,
		})
	}

	tableStatements, tableDDLs, err := o.renderCreateTableStatements(ctx, runID, renderer)
	if err != nil {
		return schemadiff.Plan{}, err
	}
	plan.Statements = append(plan.Statements, tableStatements...)

	if o.config.Migration.CreateIndexes {
		stmts, err := o.renderCreateIndexStatements(ctx, runID, renderer, tableDDLs)
		if err != nil {
			return schemadiff.Plan{}, err
		}
		plan.Statements = append(plan.Statements, stmts...)
	}
	if o.config.Migration.CreateForeignKeys {
		stmts, err := o.renderCreateForeignKeyStatements(ctx, runID, renderer, tableDDLs)
		if err != nil {
			return schemadiff.Plan{}, err
		}
		plan.Statements = append(plan.Statements, stmts...)
	}
	if o.config.Migration.CreateCheckConstraints {
		stmts, err := o.renderCreateCheckStatements(ctx, runID, renderer, tableDDLs)
		if err != nil {
			return schemadiff.Plan{}, err
		}
		plan.Statements = append(plan.Statements, stmts...)
	}

	return plan, nil
}

type createDDLRenderer struct {
	sourceType           string
	targetType           string
	targetSchema         string
	sourceContext        *driver.DatabaseContext
	unknownTypePolicy    string
	aiReviewEnabled      bool
	aiReviewMode         string
	tableVerifier        driver.TableTypeMapper
	finalizationVerifier driver.FinalizationDDLMapper
}

func (o *Orchestrator) newCreateDDLRenderer() (createDDLRenderer, error) {
	targetType := canonicalDBType(o.config.Target.Type)
	sourceType := canonicalDBType(o.config.Source.Type)
	if targetType != "postgres" {
		return createDDLRenderer{}, fmt.Errorf("deterministic DDL generation currently supports postgres targets; %s target renderer is not implemented", targetType)
	}

	aiReviewEnabled := o.config.AIReview.Enabled != nil && *o.config.AIReview.Enabled

	var verifier driver.TypeMapper
	if aiReviewEnabled {
		if name := strings.TrimSpace(o.config.AIReview.Model); name != "" {
			vm, err := driver.NewAITypeMapperByName(name)
			if err != nil {
				return createDDLRenderer{}, fmt.Errorf("loading AI review provider %q: %w", name, err)
			}
			verifier = vm
		} else {
			vm, err := driver.GetAITypeMapper()
			if err != nil {
				return createDDLRenderer{}, fmt.Errorf("AI review requires an AI provider; configure one in ~/.secrets/smt-config.yaml: %w", err)
			}
			verifier = vm
		}
	}

	var tableVerifier driver.TableTypeMapper
	var finalizationVerifier driver.FinalizationDDLMapper
	if verifier != nil {
		tableVerifier, _ = verifier.(driver.TableTypeMapper)
		finalizationVerifier, _ = verifier.(driver.FinalizationDDLMapper)
	}

	return createDDLRenderer{
		sourceType:           sourceType,
		targetType:           targetType,
		targetSchema:         o.config.Target.Schema,
		sourceContext:        o.source.DatabaseContext(),
		unknownTypePolicy:    o.config.SchemaGeneration.UnknownTypePolicy,
		aiReviewEnabled:      aiReviewEnabled,
		aiReviewMode:         o.config.AIReview.Mode,
		tableVerifier:        tableVerifier,
		finalizationVerifier: finalizationVerifier,
	}, nil
}

func (o *Orchestrator) renderCreateTableStatements(ctx context.Context, runID string, renderer createDDLRenderer) ([]schemadiff.Statement, map[string]string, error) {
	_ = o.state.UpdatePhase(runID, string(TaskCreateTables))
	total := len(o.tables)
	logging.Info("[%s] rendering %d CREATE TABLE statement(s) (concurrency=%d)", TaskCreateTables, total, o.aiConcurrency())

	statements := make([]schemadiff.Statement, total)
	tableDDLs := make([]string, total)
	var done atomic.Int64
	if err := runParallel(ctx, o.tables, o.aiConcurrency(), func(ctx context.Context, i int, t source.Table) error {
		ddl, err := renderer.renderTable(ctx, &t)
		if err != nil {
			return fmt.Errorf("rendering table %s: %w", t.Name, err)
		}
		ddl = stripTrailingSemicolons(ddl)
		tableDDLs[i] = ddl
		statements[i] = schemadiff.Statement{
			Table:       driver.NormalizeIdentifier(renderer.targetType, t.Name),
			Description: fmt.Sprintf("create table %s", t.Name),
			SQL:         ddl,
			Risk:        schemadiff.RiskSafe,
		}
		n := done.Add(1)
		logging.Info("  ✓ [%d/%d] %s.%s", n, total, o.config.Source.Schema, t.Name)
		return nil
	}); err != nil {
		return nil, nil, err
	}

	ddlByTable := make(map[string]string, len(o.tables))
	for i, t := range o.tables {
		ddlByTable[t.Name] = tableDDLs[i]
	}
	return statements, ddlByTable, nil
}

func (o *Orchestrator) renderCreateIndexStatements(ctx context.Context, runID string, renderer createDDLRenderer, tableDDLs map[string]string) ([]schemadiff.Statement, error) {
	_ = o.state.UpdatePhase(runID, string(TaskCreateIndexes))
	logging.Info("[%s] loading and rendering indexes (concurrency=%d)", TaskCreateIndexes, o.aiConcurrency())
	results := make([][]schemadiff.Statement, len(o.tables))

	if err := runParallel(ctx, o.tables, o.aiConcurrency(), func(ctx context.Context, i int, t source.Table) error {
		if err := o.source.LoadIndexes(ctx, &t); err != nil {
			return fmt.Errorf("loading indexes for %s: %w", t.Name, err)
		}
		stmts := make([]schemadiff.Statement, 0, len(t.Indexes))
		for j := range t.Indexes {
			idx := t.Indexes[j]
			ddl, err := renderer.renderIndex(ctx, &t, &idx, tableDDLs[t.Name])
			if err != nil {
				return fmt.Errorf("rendering index %s: %w", idx.Name, err)
			}
			stmts = append(stmts, schemadiff.Statement{
				Table:       driver.NormalizeIdentifier(renderer.targetType, t.Name),
				Description: fmt.Sprintf("create index %s on %s", idx.Name, t.Name),
				SQL:         stripTrailingSemicolons(ddl),
				Risk:        schemadiff.RiskSafe,
			})
		}
		results[i] = stmts
		return nil
	}); err != nil {
		return nil, err
	}
	return flattenStatements(results), nil
}

func (o *Orchestrator) renderCreateForeignKeyStatements(ctx context.Context, runID string, renderer createDDLRenderer, tableDDLs map[string]string) ([]schemadiff.Statement, error) {
	_ = o.state.UpdatePhase(runID, string(TaskCreateFKs))
	logging.Info("[%s] loading and rendering foreign keys (concurrency=%d)", TaskCreateFKs, o.aiConcurrency())
	results := make([][]schemadiff.Statement, len(o.tables))

	if err := runParallel(ctx, o.tables, o.aiConcurrency(), func(ctx context.Context, i int, t source.Table) error {
		if err := o.source.LoadForeignKeys(ctx, &t); err != nil {
			return fmt.Errorf("loading FKs for %s: %w", t.Name, err)
		}
		stmts := make([]schemadiff.Statement, 0, len(t.ForeignKeys))
		for j := range t.ForeignKeys {
			fk := t.ForeignKeys[j]
			fkForTarget := fk
			fkForTarget.RefSchema = renderer.targetSchema
			ddl, err := renderer.renderForeignKey(ctx, &t, &fkForTarget, tableDDLs[t.Name])
			if err != nil {
				return fmt.Errorf("rendering FK %s: %w", fk.Name, err)
			}
			stmts = append(stmts, schemadiff.Statement{
				Table:       driver.NormalizeIdentifier(renderer.targetType, t.Name),
				Description: fmt.Sprintf("create foreign key %s on %s", fk.Name, t.Name),
				SQL:         stripTrailingSemicolons(ddl),
				Risk:        schemadiff.RiskSafe,
			})
		}
		results[i] = stmts
		return nil
	}); err != nil {
		return nil, err
	}
	return flattenStatements(results), nil
}

func (o *Orchestrator) renderCreateCheckStatements(ctx context.Context, runID string, renderer createDDLRenderer, tableDDLs map[string]string) ([]schemadiff.Statement, error) {
	_ = o.state.UpdatePhase(runID, string(TaskCreateChecks))
	logging.Info("[%s] loading and rendering check constraints (concurrency=%d)", TaskCreateChecks, o.aiConcurrency())
	results := make([][]schemadiff.Statement, len(o.tables))

	if err := runParallel(ctx, o.tables, o.aiConcurrency(), func(ctx context.Context, i int, t source.Table) error {
		if err := o.source.LoadCheckConstraints(ctx, &t); err != nil {
			return fmt.Errorf("loading checks for %s: %w", t.Name, err)
		}
		stmts := make([]schemadiff.Statement, 0, len(t.CheckConstraints))
		for j := range t.CheckConstraints {
			chk := t.CheckConstraints[j]
			ddl, err := renderer.renderCheck(ctx, &t, &chk, tableDDLs[t.Name])
			if err != nil {
				return fmt.Errorf("rendering check %s: %w", chk.Name, err)
			}
			stmts = append(stmts, schemadiff.Statement{
				Table:       driver.NormalizeIdentifier(renderer.targetType, t.Name),
				Description: fmt.Sprintf("create check constraint %s on %s", chk.Name, t.Name),
				SQL:         stripTrailingSemicolons(ddl),
				Risk:        schemadiff.RiskSafe,
			})
		}
		results[i] = stmts
		return nil
	}); err != nil {
		return nil, err
	}
	return flattenStatements(results), nil
}

func (r createDDLRenderer) renderTable(ctx context.Context, t *driver.Table) (string, error) {
	ddl, _, err := pgddl.RenderCreateTableDDLWithPolicy(t, r.targetSchema, false, r.unknownTypePolicy)
	if err != nil {
		return "", err
	}
	if err := r.reviewTable(ctx, t, ddl); err != nil {
		return "", err
	}
	return ddl, nil
}

func (r createDDLRenderer) renderIndex(ctx context.Context, t *driver.Table, idx *driver.Index, _ string) (string, error) {
	ddl, err := pgddl.RenderCreateIndexDDL(t, idx, r.targetSchema)
	if err != nil {
		return "", err
	}
	if err := r.reviewFinalization(ctx, driver.DDLTypeIndex, t, idx, nil, nil, ddl); err != nil {
		return "", err
	}
	return ddl, nil
}

func (r createDDLRenderer) renderForeignKey(ctx context.Context, t *driver.Table, fk *driver.ForeignKey, _ string) (string, error) {
	ddl, err := pgddl.RenderCreateForeignKeyDDL(t, fk, r.targetSchema)
	if err != nil {
		return "", err
	}
	if err := r.reviewFinalization(ctx, driver.DDLTypeForeignKey, t, nil, fk, nil, ddl); err != nil {
		return "", err
	}
	return ddl, nil
}

func (r createDDLRenderer) renderCheck(ctx context.Context, t *driver.Table, chk *driver.CheckConstraint, _ string) (string, error) {
	ddl, err := pgddl.RenderCreateCheckConstraintDDL(t, chk, r.targetSchema)
	if err != nil {
		return "", err
	}
	if err := r.reviewFinalization(ctx, driver.DDLTypeCheckConstraint, t, nil, nil, chk, ddl); err != nil {
		return "", err
	}
	return ddl, nil
}

func (r createDDLRenderer) reviewTable(ctx context.Context, t *driver.Table, ddl string) error {
	if !r.aiReviewEnabled {
		return nil
	}
	if r.tableVerifier == nil {
		return fmt.Errorf("AI review enabled but no table reviewer is configured")
	}
	verdict, err := r.tableVerifier.VerifyTableDDL(ctx, driver.VerifyTableDDLRequest{
		SourceDBType:  r.sourceType,
		TargetDBType:  r.targetType,
		SourceTable:   t,
		TargetSchema:  r.targetSchema,
		SourceContext: r.sourceContext,
		ProposedDDL:   ddl,
	})
	if err != nil {
		return fmt.Errorf("AI review failed for table %s: %w", t.FullName(), err)
	}
	return handleReviewVerdict(r.aiReviewMode, fmt.Sprintf("table %s", t.FullName()), verdict)
}

func (r createDDLRenderer) reviewFinalization(ctx context.Context, ddlType driver.DDLType, t *driver.Table, idx *driver.Index, fk *driver.ForeignKey, chk *driver.CheckConstraint, ddl string) error {
	if !r.aiReviewEnabled {
		return nil
	}
	if r.finalizationVerifier == nil {
		return fmt.Errorf("AI review enabled but no finalization reviewer is configured")
	}
	verdict, err := r.finalizationVerifier.VerifyFinalizationDDL(ctx, driver.VerifyFinalizationDDLRequest{
		Type:            ddlType,
		SourceDBType:    r.sourceType,
		TargetDBType:    r.targetType,
		Table:           t,
		TargetSchema:    r.targetSchema,
		Index:           idx,
		ForeignKey:      fk,
		CheckConstraint: chk,
		ProposedDDL:     ddl,
	})
	if err != nil {
		return fmt.Errorf("AI review failed for %s on %s: %w", ddlType, t.FullName(), err)
	}
	return handleReviewVerdict(r.aiReviewMode, fmt.Sprintf("%s on %s", ddlType, t.FullName()), verdict)
}

func handleReviewVerdict(mode, label string, verdict *driver.VerifyResult) error {
	if verdict == nil || verdict.OK {
		logging.Debug("AI review OK: %s", label)
		return nil
	}
	msg := strings.Join(verdict.Issues, "\n  ")
	if strings.EqualFold(mode, "fail") {
		return fmt.Errorf("AI review flagged %d issue(s) on %s:\n  %s", len(verdict.Issues), label, msg)
	}
	logging.Warn("AI review flagged %d issue(s) on %s:\n  %s", len(verdict.Issues), label, msg)
	return nil
}

func renderCreateSchemaDDL(targetType, schema string) (string, error) {
	schema = strings.TrimSpace(schema)
	if schema == "" {
		return "", nil
	}
	dialect := driver.GetDialect(targetType)
	if dialect == nil {
		return "", fmt.Errorf("unsupported target type %q", targetType)
	}

	switch targetType {
	case "postgres":
		return fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", dialect.QuoteIdentifier(schema)), nil
	case "mssql":
		escapedName := strings.ReplaceAll(schema, "'", "''")
		escapedDDL := strings.ReplaceAll(fmt.Sprintf("CREATE SCHEMA %s", dialect.QuoteIdentifier(schema)), "'", "''")
		return fmt.Sprintf("IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'%s') EXEC(N'%s')", escapedName, escapedDDL), nil
	case "mysql":
		return fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dialect.QuoteIdentifier(schema)), nil
	default:
		return "", fmt.Errorf("unsupported target type %q", targetType)
	}
}

func (o *Orchestrator) writeSQLArtifact(runID, name, sql string) error {
	dir := o.ddlArtifactDir(runID)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, name), []byte(sql), 0600)
}

func flattenStatements(grouped [][]schemadiff.Statement) []schemadiff.Statement {
	var total int
	for _, group := range grouped {
		total += len(group)
	}
	out := make([]schemadiff.Statement, 0, total)
	for _, group := range grouped {
		out = append(out, group...)
	}
	return out
}

func stripTrailingSemicolons(sql string) string {
	sql = strings.TrimSpace(sql)
	for strings.HasSuffix(sql, ";") {
		sql = strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	}
	return sql
}

func canonicalDBType(dbType string) string {
	if d, err := driver.Get(dbType); err == nil {
		return d.Name()
	}
	return strings.ToLower(strings.TrimSpace(dbType))
}
