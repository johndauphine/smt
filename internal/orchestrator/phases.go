package orchestrator

// This file holds the schema phases as small named methods. The public
// entry point Run() drives them in order; each phase method is independent
// and can be called individually by the schema-diff/sync commands.

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"smt/internal/logging"
	"smt/internal/source"
)

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

// CreateTables issues a CREATE TABLE for each table in scope.
func (o *Orchestrator) CreateTables(ctx context.Context, runID string) error {
	_ = o.state.UpdatePhase(runID, string(TaskCreateTables))
	logging.Info("[%s] creating %d tables", TaskCreateTables, len(o.tables))
	for i := range o.tables {
		if err := ctx.Err(); err != nil {
			return err
		}
		t := &o.tables[i]
		logging.Info("  [%d/%d] %s.%s", i+1, len(o.tables), o.config.Source.Schema, t.Name)
		if err := o.target.CreateTable(ctx, t, o.config.Target.Schema); err != nil {
			return fmt.Errorf("creating table %s: %w", t.Name, err)
		}
	}
	return nil
}

// CreateIndexes loads each table's indexes from the source and creates
// them on the target.
func (o *Orchestrator) CreateIndexes(ctx context.Context, runID string) error {
	_ = o.state.UpdatePhase(runID, string(TaskCreateIndexes))
	logging.Info("[%s] loading and creating indexes", TaskCreateIndexes)
	for i := range o.tables {
		t := &o.tables[i]
		if err := o.source.LoadIndexes(ctx, t); err != nil {
			return fmt.Errorf("loading indexes for %s: %w", t.Name, err)
		}
		for j := range t.Indexes {
			idx := t.Indexes[j]
			if err := o.target.CreateIndex(ctx, t, &idx, o.config.Target.Schema); err != nil {
				return fmt.Errorf("creating index %s: %w", idx.Name, err)
			}
		}
	}
	return nil
}

// CreateForeignKeys loads each table's foreign keys from the source and
// creates them on the target.
func (o *Orchestrator) CreateForeignKeys(ctx context.Context, runID string) error {
	_ = o.state.UpdatePhase(runID, string(TaskCreateFKs))
	logging.Info("[%s] loading and creating foreign keys", TaskCreateFKs)
	for i := range o.tables {
		t := &o.tables[i]
		if err := o.source.LoadForeignKeys(ctx, t); err != nil {
			return fmt.Errorf("loading FKs for %s: %w", t.Name, err)
		}
		for j := range t.ForeignKeys {
			fk := t.ForeignKeys[j]
			if err := o.target.CreateForeignKey(ctx, t, &fk, o.config.Target.Schema); err != nil {
				return fmt.Errorf("creating FK %s: %w", fk.Name, err)
			}
		}
	}
	return nil
}

// CreateCheckConstraints loads each table's check constraints from the
// source and creates them on the target.
func (o *Orchestrator) CreateCheckConstraints(ctx context.Context, runID string) error {
	_ = o.state.UpdatePhase(runID, string(TaskCreateChecks))
	logging.Info("[%s] loading and creating check constraints", TaskCreateChecks)
	for i := range o.tables {
		t := &o.tables[i]
		if err := o.source.LoadCheckConstraints(ctx, t); err != nil {
			return fmt.Errorf("loading checks for %s: %w", t.Name, err)
		}
		for j := range t.CheckConstraints {
			chk := t.CheckConstraints[j]
			if err := o.target.CreateCheckConstraint(ctx, t, &chk, o.config.Target.Schema); err != nil {
				return fmt.Errorf("creating check %s: %w", chk.Name, err)
			}
		}
	}
	return nil
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
