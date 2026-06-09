package orchestrator

// This file holds the schema phases as small named methods. The public
// entry point Run() drives them in order; each phase method is independent
// and can be called individually by the schema-diff/sync commands.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"smt/internal/checkpoint"
	"smt/internal/driver"
	"smt/internal/logging"
	"smt/internal/schemadiff"
	"smt/internal/source"
)

// defaultAIConcurrency is the number of concurrent AI calls used when
// the user has not set Migration.AIConcurrency. Picked as a middle
// ground: ~8× speedup vs serial against cloud providers (well under
// rate limits), no harm against local providers (which serialize
// through one GPU anyway). Override in config for warehouse-scale
// schemas (try 16-32) or for local LM Studio / Ollama (set to 1).
const defaultAIConcurrency = 8

// runParallel calls fn concurrently for each item in items, with at
// most n calls in flight at once. First non-nil return cancels the
// rest via the shared context. Same error semantics as the previous
// serial loop: first failure aborts and is propagated.
func runParallel[T any](ctx context.Context, items []T, n int, fn func(context.Context, int, T) error) error {
	if n <= 0 {
		n = defaultAIConcurrency
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(n)
	for i, item := range items {
		g.Go(func() error { return fn(gctx, i, item) })
	}
	return g.Wait()
}

// TaskType names a single phase of a schema run. Stored in the run record
// so a partial run reports which phase it reached before failing.
type TaskType string

const (
	TaskExtractSchema TaskType = "extract_schema"
	TaskCreateTables  TaskType = "create_tables"
	TaskCreateIndexes TaskType = "create_indexes"
	TaskCreateFKs     TaskType = "create_fks"
	TaskCreateChecks  TaskType = "create_checks"
	TaskExecuteDDL    TaskType = "execute_ddl"
)

// Run executes the full schema build sequence in order. Each phase is its
// own method below; Run is the orchestration. If any phase fails, the run
// is marked failed in state and the error is surfaced.
func (o *Orchestrator) Run(ctx context.Context) error {
	runID := o.opts.RunID
	if runID == "" {
		runID = uuid.NewString()
	}

	if err := o.state.CreateRun(runID, checkpoint.RunKindApply, o.config.Source.Schema, o.config.Target.Schema, o.config.Sanitized(), o.runProfile, o.runConfig); err != nil {
		return fmt.Errorf("recording run start: %w", err)
	}

	start := time.Now()
	_ = o.notifier.MigrationStarted(runID, o.config.Source.Type, o.config.Target.Type, 0)

	if err := o.runAllPhases(ctx, runID); err != nil {
		_ = o.state.CompleteRun(runID, "failed", err.Error())
		_ = o.notifier.MigrationFailed(runID, err, time.Since(start))
		return err
	}

	dur := time.Since(start)
	_ = o.state.CompleteRun(runID, "success", "")
	_ = o.notifier.MigrationCompleted(runID, start, dur, len(o.tables), 0, 0)
	logging.Info("Schema build complete in %s (%d tables)", dur.Round(time.Millisecond), len(o.tables))
	return nil
}

// runAllPhases renders the full DDL plan — the exact same path `smt create`
// (preview) uses, including optional AI review — and then executes it against
// the target. One render pipeline means the schema.sql a user reviewed is the
// SQL apply executes (#87).
func (o *Orchestrator) runAllPhases(ctx context.Context, runID string) error {
	plan, err := o.renderDDLPlan(ctx, runID)
	if err != nil {
		return err
	}
	if err := o.writeSQLArtifact(runID, "schema.sql", plan.SQL()); err != nil {
		return fmt.Errorf("writing DDL artifact: %w", err)
	}
	return o.executePlan(ctx, runID, plan)
}

// executePlan runs the rendered statements in order. Statements whose target
// object already exists are skipped, so re-runs after a partial failure are
// idempotent (the pre-#87 writer paths had the same semantics). Execution is
// sequential: rendering (the expensive part) already happened concurrently,
// plan order encodes dependencies, and serial DDL sidesteps the InnoDB
// metadata-lock deadlocks concurrent CHECK creation used to hit (#25).
func (o *Orchestrator) executePlan(ctx context.Context, runID string, plan schemadiff.Plan) error {
	_ = o.state.UpdatePhase(runID, string(TaskExecuteDDL))
	total := len(plan.Statements)
	logging.Info("[%s] executing %d DDL statement(s)", TaskExecuteDDL, total)
	for i, stmt := range plan.Statements {
		exists, err := o.planObjectExists(ctx, stmt)
		if err != nil {
			return fmt.Errorf("checking existence for %s: %w", stmt.Description, err)
		}
		if exists {
			logging.Info("  ✓ [%d/%d] %s — already exists, skipping", i+1, total, stmt.Description)
			continue
		}
		if _, err := o.target.ExecRaw(ctx, stmt.SQL); err != nil {
			return fmt.Errorf("%s: %w\nSQL: %s", stmt.Description, err, stmt.SQL)
		}
		logging.Info("  ✓ [%d/%d] %s", i+1, total, stmt.Description)
	}
	return nil
}

// planObjectExists consults the target catalog for the object a statement
// creates. Schema statements render with IF NOT EXISTS semantics on every
// target, so they never need gating.
func (o *Orchestrator) planObjectExists(ctx context.Context, stmt schemadiff.Statement) (bool, error) {
	schema := o.config.Target.Schema
	switch stmt.Kind {
	case schemadiff.StatementKindTable:
		return o.target.TableExists(ctx, schema, stmt.Object)
	case schemadiff.StatementKindIndex:
		return o.target.IndexExists(ctx, schema, stmt.Table, stmt.Object)
	case schemadiff.StatementKindForeignKey:
		return o.target.ForeignKeyExists(ctx, schema, stmt.Table, stmt.Object)
	case schemadiff.StatementKindCheck:
		return o.target.CheckConstraintExists(ctx, schema, stmt.Table, stmt.Object)
	default:
		return false, nil
	}
}

// ExtractSchema reads the source schema, applies include/exclude filters,
// and stores the result on the orchestrator for subsequent phases.
func (o *Orchestrator) ExtractSchema(ctx context.Context, runID string) error {
	_ = o.state.UpdatePhase(runID, string(TaskExtractSchema))
	logging.Info("[%s] extracting source schema (%s)", TaskExtractSchema, o.config.Source.Schema)

	tables, err := o.source.ExtractSchema(ctx, o.config.Source.Schema)
	if err != nil {
		return fmt.Errorf("extracting source schema: %w", err)
	}

	o.tables = o.filterTables(tables)
	logging.Info("[%s] %d tables in scope", TaskExtractSchema, len(o.tables))
	if err := o.writeJSONArtifact(runID, "source_schema.json", o.tables); err != nil {
		return fmt.Errorf("writing source schema artifact: %w", err)
	}
	return nil
}

// aiConcurrency returns the configured per-phase concurrency limit, or
// defaultAIConcurrency if unset.
func (o *Orchestrator) aiConcurrency() int {
	n := o.config.Migration.AIConcurrency
	if n <= 0 {
		n = defaultAIConcurrency
	}
	return n
}

// filterTables drops tables matching exclude_tables and (if include_tables
// is set) drops anything not matching it. Patterns use filepath.Match
// glob syntax.
func (o *Orchestrator) filterTables(tables []source.Table) []source.Table {
	include := o.config.Migration.IncludeTables
	exclude := o.config.Migration.ExcludeTables
	if len(include) == 0 && len(exclude) == 0 {
		return tables
	}
	out := tables[:0]
	for _, t := range tables {
		if matchesAny(t.Name, exclude) {
			continue
		}
		if len(include) > 0 && !matchesAny(t.Name, include) {
			continue
		}
		out = append(out, t)
	}
	return out
}

func matchesAny(name string, patterns []string) bool {
	for _, p := range patterns {
		if ok, _ := filepath.Match(p, name); ok {
			return true
		}
	}
	return false
}

func (o *Orchestrator) runArtifactDir(runID string) string {
	return filepath.Join(o.config.Migration.DataDir, "runs", runID)
}

func (o *Orchestrator) ddlArtifactDir(runID string) string {
	return filepath.Join(o.runArtifactDir(runID), "ddl")
}

func (o *Orchestrator) writeJSONArtifact(runID, name string, value any) error {
	dir := o.runArtifactDir(runID)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(filepath.Join(dir, name), data, 0600)
}

func artifactName(name string) string {
	normalized := driver.NormalizeIdentifier("postgres", name)
	if normalized == "" {
		return "unnamed"
	}
	return normalized
}
