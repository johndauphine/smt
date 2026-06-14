package orchestrator

import (
	"context"
	"fmt"

	"smt/internal/driver"
	"smt/internal/logging"
)

// diagnoseSchemaFailure is the opt-in AI advisory hook (ai_review.
// diagnose_failures). When a schema extraction or DDL-render bug aborts the run
// before any DDL exists — which the verifier-only ai_review never sees — it asks
// the configured AI provider for user-facing guidance (cause + suggestions) on
// how to resolve it.
//
// It is strictly advisory: it never generates, patches, or retries DDL, and it
// never changes the run's outcome. The caller still returns the original error;
// a provider/network failure here is swallowed (logged at debug) so diagnosis
// can never mask or replace the real error.
func (o *Orchestrator) diagnoseSchemaFailure(ctx context.Context, table, schema, operation string, cause error) {
	if o.config == nil || !o.config.AIReview.DiagnoseFailures || cause == nil {
		return
	}
	diag := o.errorDiagnoser()
	if diag == nil {
		return
	}
	diagnosis, err := diag.Diagnose(ctx, &driver.ErrorContext{
		ErrorMessage: fmt.Sprintf("%s: %v", operation, cause),
		TableName:    table,
		TableSchema:  schema,
		SourceDBType: canonicalDBType(o.config.Source.Type),
		TargetDBType: canonicalDBType(o.config.Target.Type),
		TargetMode:   o.config.SchemaGeneration.Mode,
	})
	if err != nil {
		logging.Debug("AI failure diagnosis unavailable: %v", err)
		return
	}
	driver.EmitDiagnosis(diagnosis)
}

// errorDiagnoser lazily resolves the AI failure-diagnosis advisor from the
// ai_review.model provider on first use, so no AI client is constructed unless a
// failure actually occurs. A resolution error is logged once and yields nil
// (diagnosis is best-effort).
func (o *Orchestrator) errorDiagnoser() *driver.AIErrorDiagnoser {
	o.diagnoserOnce.Do(func() {
		d, err := driver.NewErrorDiagnoserByName(o.config.AIReview.Model)
		if err != nil {
			logging.Debug("AI failure diagnosis disabled: no usable provider: %v", err)
			return
		}
		o.diagnoser = d
	})
	return o.diagnoser
}
