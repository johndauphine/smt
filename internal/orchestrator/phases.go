package orchestrator

// This file holds the schema phases as small named methods. The public
// entry point Run() drives them in order; each phase method is independent
// and can be called individually by the schema-diff/sync commands.

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"smt/internal/driver"
	"smt/internal/logging"
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
	TaskCreateSchema  TaskType = "create_schema"
	TaskCreateTables  TaskType = "create_tables"
	TaskCreateIndexes TaskType = "create_indexes"
	TaskCreateFKs     TaskType = "create_fks"
	TaskCreateChecks  TaskType = "create_checks"
)

// Run executes the full schema build sequence in order. Each phase is its
// own method below; Run is the orchestration. If any phase fails, the run
// is marked failed in state and the error is surfaced.
func (o *Orchestrator) Run(ctx context.Context) error {
	runID := o.opts.RunID
	if runID == "" {
		runID = uuid.NewString()
	}

	if err := o.state.CreateRun(runID, o.config.Source.Schema, o.config.Target.Schema, o.config.Sanitized(), o.runProfile, o.runConfig); err != nil {
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

// runAllPhases drives the per-phase methods in order. Optional phases
// (indexes, FKs, checks) are gated by config flags.
func (o *Orchestrator) runAllPhases(ctx context.Context, runID string) error {
	if err := o.ExtractSchema(ctx, runID); err != nil {
		return err
	}
	if err := o.CreateTargetSchema(ctx, runID); err != nil {
		return err
	}
	if err := o.CreateTables(ctx, runID); err != nil {
		return err
	}
	if o.config.Migration.CreateIndexes {
		if err := o.CreateIndexes(ctx, runID); err != nil {
			return err
		}
	}
	if o.config.Migration.CreateForeignKeys {
		if err := o.CreateForeignKeys(ctx, runID); err != nil {
			return err
		}
	}
	if o.config.Migration.CreateCheckConstraints {
		if err := o.CreateCheckConstraints(ctx, runID); err != nil {
			return err
		}
	}
	return nil
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
	return nil
}

// CreateTargetSchema ensures the target schema (namespace) exists.
func (o *Orchestrator) CreateTargetSchema(ctx context.Context, runID string) error {
	_ = o.state.UpdatePhase(runID, string(TaskCreateSchema))
	logging.Info("[%s] ensuring target schema %q exists", TaskCreateSchema, o.config.Target.Schema)
	if err := o.target.CreateSchema(ctx, o.config.Target.Schema); err != nil {
		return fmt.Errorf("creating target schema: %w", err)
	}
	return nil
}

// CreateTables issues a CREATE TABLE for each table in scope. Calls run
// concurrently up to Migration.AIConcurrency in flight at once; logging
// is per-completion (so output order may differ from source order, but
// each line clearly identifies the table).
func (o *Orchestrator) CreateTables(ctx context.Context, runID string) error {
	_ = o.state.UpdatePhase(runID, string(TaskCreateTables))
	total := len(o.tables)
	logging.Info("[%s] creating %d tables (concurrency=%d)", TaskCreateTables, total, o.aiConcurrency())
	var done atomic.Int64
	// Gather source DB context once up front (Reader caches; this primes the
	// cache so the per-table goroutines all read the same value without racing
	// on the sync.Once inside the Reader implementation). The result feeds the
	// AI prompt's SOURCE DATABASE block — see issue #13.
	sourceCtx := o.source.DatabaseContext()
	return runParallel(ctx, o.tables, o.aiConcurrency(), func(ctx context.Context, _ int, t source.Table) error {
		opts := driver.TableOptions{SourceContext: sourceCtx}
		if err := o.target.CreateTableWithOptions(ctx, &t, o.config.Target.Schema, opts); err != nil {
			return fmt.Errorf("creating table %s: %w", t.Name, err)
		}
		n := done.Add(1)
		logging.Info("  ✓ [%d/%d] %s.%s", n, total, o.config.Source.Schema, t.Name)
		return nil
	})
}

// CreateIndexes loads each table's indexes from the source and creates
// them on the target. Each table's load+create work runs in its own
// goroutine; indexes within one table are still applied sequentially
// (typically only a handful per table, and they share AI cache hits).
func (o *Orchestrator) CreateIndexes(ctx context.Context, runID string) error {
	_ = o.state.UpdatePhase(runID, string(TaskCreateIndexes))
	logging.Info("[%s] loading and creating indexes (concurrency=%d)", TaskCreateIndexes, o.aiConcurrency())
	return runParallel(ctx, o.tables, o.aiConcurrency(), func(ctx context.Context, _ int, t source.Table) error {
		if err := o.source.LoadIndexes(ctx, &t); err != nil {
			return fmt.Errorf("loading indexes for %s: %w", t.Name, err)
		}
		for j := range t.Indexes {
			idx := t.Indexes[j]
			if err := o.target.CreateIndex(ctx, &t, &idx, o.config.Target.Schema); err != nil {
				return fmt.Errorf("creating index %s: %w", idx.Name, err)
			}
		}
		return nil
	})
}

// CreateForeignKeys loads each table's foreign keys from the source and
// creates them on the target. Same parallelism shape as CreateIndexes:
// per-table goroutines, sequential FKs within a table.
func (o *Orchestrator) CreateForeignKeys(ctx context.Context, runID string) error {
	_ = o.state.UpdatePhase(runID, string(TaskCreateFKs))
	logging.Info("[%s] loading and creating foreign keys (concurrency=%d)", TaskCreateFKs, o.aiConcurrency())
	return runParallel(ctx, o.tables, o.aiConcurrency(), func(ctx context.Context, _ int, t source.Table) error {
		if err := o.source.LoadForeignKeys(ctx, &t); err != nil {
			return fmt.Errorf("loading FKs for %s: %w", t.Name, err)
		}
		for j := range t.ForeignKeys {
			fk := t.ForeignKeys[j]
			if err := o.target.CreateForeignKey(ctx, &t, &fk, o.config.Target.Schema); err != nil {
				return fmt.Errorf("creating FK %s: %w", fk.Name, err)
			}
		}
		return nil
	})
}

// CreateCheckConstraints loads each table's check constraints from the
// source and creates them on the target. Same parallelism shape as the
// other constraint phases.
func (o *Orchestrator) CreateCheckConstraints(ctx context.Context, runID string) error {
	_ = o.state.UpdatePhase(runID, string(TaskCreateChecks))
	logging.Info("[%s] loading and creating check constraints (concurrency=%d)", TaskCreateChecks, o.aiConcurrency())
	return runParallel(ctx, o.tables, o.aiConcurrency(), func(ctx context.Context, _ int, t source.Table) error {
		if err := o.source.LoadCheckConstraints(ctx, &t); err != nil {
			return fmt.Errorf("loading checks for %s: %w", t.Name, err)
		}
		for j := range t.CheckConstraints {
			chk := t.CheckConstraints[j]
			if err := o.target.CreateCheckConstraint(ctx, &t, &chk, o.config.Target.Schema); err != nil {
				return fmt.Errorf("creating check %s: %w", chk.Name, err)
			}
		}
		return nil
	})
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
