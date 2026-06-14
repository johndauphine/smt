package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

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

// splicedFix is the outcome of translating one failing expression and
// re-rendering the table with it overridden.
type splicedFix struct {
	exprErr      *driver.ExpressionRenderError
	fix          *driver.ExpressionFix
	ddl          string // SMT's deterministic DDL with the one expression spliced
	classMatched bool   // deterministic class-equivalence verdict
}

// handleRenderFailure runs the failure advisories for a single-table render
// error and, when --apply-suggested is set, splices the AI fix into the plan.
// It returns the (marked) spliced DDL and true when the table was fixed
// in-place; false means the caller must return the original error.
//
// AI-authored content reaches the plan / schema.sql ONLY here, and only under
// the explicit --apply-suggested flag — loudly logged and marked inline.
func (o *Orchestrator) handleRenderFailure(ctx context.Context, runID string, renderer createDDLRenderer, t *driver.Table, cause error) (string, bool) {
	if o.config == nil || t == nil {
		return "", false
	}
	o.diagnoseSchemaFailure(ctx, t.Name, t.Schema, "rendering CREATE TABLE DDL", cause)

	// Only do the (one) AI splice call if a suggestion or apply is actually
	// requested.
	if !aiSuggestFixesEnabled(o.config) && !o.opts.ApplySuggested {
		return "", false
	}
	sf, ok := o.spliceFix(ctx, renderer, t, cause)
	if !ok {
		return "", false
	}
	if aiSuggestFixesEnabled(o.config) {
		o.writeSuggestion(runID, t.Name, sf)
	}
	if !o.opts.ApplySuggested {
		return "", false
	}

	verdict := "OK: default class matches source"
	if !sf.classMatched {
		verdict = "REVIEW: class NOT mechanically confirmed"
	}
	logging.Warn("APPLYING AI-ASSISTED FIX (--apply-suggested): table %s, column %s DEFAULT  %s -> %s  [%s]. This expression was authored by an AI model.",
		oneLine(t.Name), oneLine(sf.exprErr.Column), oneLine(sf.exprErr.SourceExpr), oneLine(sf.fix.Expression), verdict)
	marker := fmt.Sprintf("-- AI-ASSISTED FIX (--apply-suggested): %s DEFAULT  %s -> %s  [%s]\n",
		oneLine(sf.exprErr.Column), oneLine(sf.exprErr.SourceExpr), oneLine(sf.fix.Expression), verdict)
	return marker + sf.ddl, true
}

// spliceFix attempts the AI splice for a single-expression render failure:
// translate the one failing expression and re-render the table with it
// overridden. ok=false (debug-logged) when the failure isn't a single
// splice-able default, no provider is available, the AI expression is malformed,
// or the re-render fails. Performs exactly one AI call.
func (o *Orchestrator) spliceFix(ctx context.Context, renderer createDDLRenderer, t *driver.Table, cause error) (*splicedFix, bool) {
	if o.config == nil || cause == nil || t == nil {
		return nil, false
	}
	var exprErr *driver.ExpressionRenderError
	if !errors.As(cause, &exprErr) || exprErr.Kind != "default" {
		return nil, false // not a single splice-able expression — diagnosis-only
	}
	idx := -1
	for i := range t.Columns {
		if t.Columns[i].Name == exprErr.Column {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil, false
	}
	suggester := o.errorDiagnoser()
	if suggester == nil {
		return nil, false
	}
	fix, err := suggester.SuggestExpressionFix(ctx, driver.FixRequest{
		Kind:          exprErr.Kind,
		SourceExpr:    exprErr.SourceExpr,
		ColumnName:    exprErr.Column,
		ColumnType:    t.Columns[idx].DataType,
		SourceDialect: canonicalDBType(o.config.Source.Type),
		TargetDialect: canonicalDBType(o.config.Target.Type),
	})
	if err != nil {
		logging.Debug("AI expression fix unavailable: %v", err)
		return nil, false
	}
	// Structural guard: a malformed expression must not be spliced verbatim
	// (it could inject extra columns/statements into the DDL).
	if err := driver.ValidateTargetExpression(fix.Expression); err != nil {
		logging.Debug("AI expression fix rejected (not a single value expression): %v", err)
		return nil, false
	}

	// Re-render the whole table deterministically with ONLY the failing
	// expression overridden by the AI translation.
	spliced := *t
	spliced.Columns = append([]driver.Column(nil), t.Columns...)
	spliced.Columns[idx].DefaultExpressionOverride = fix.Expression
	ddl, rerr := renderer.renderTable(ctx, &spliced)
	if rerr != nil {
		logging.Debug("re-render with AI expression failed: %v", rerr)
		return nil, false
	}
	return &splicedFix{
		exprErr:      exprErr,
		fix:          fix,
		ddl:          ddl,
		classMatched: driver.DefaultExpressionsEquivalent(exprErr.SourceExpr, fix.Expression),
	}, true
}

// writeSuggestion writes the AI-assisted suggestion to schema.suggested.sql,
// once even under concurrent failures (suggest_fixes path). It never writes to
// schema.sql and the run still fails — review-only.
func (o *Orchestrator) writeSuggestion(runID, table string, sf *splicedFix) {
	o.suggestOnce.Do(func() {
		content := renderSuggestionFile(table, sf.exprErr, sf.fix, sf.ddl, sf.classMatched)
		if werr := o.writeSQLArtifact(runID, "schema.suggested.sql", content); werr != nil {
			logging.Debug("writing schema.suggested.sql: %v", werr)
			return
		}
		verdict := "review the translated expression (class not mechanically confirmed)"
		if sf.classMatched {
			verdict = "translated default matches the source default class"
		}
		logging.Warn("AI-assisted fix written to %s — %s. Review before applying; SMT did NOT apply it (use --apply-suggested to apply).",
			filepath.Join(o.ddlArtifactDir(runID), "schema.suggested.sql"), verdict)
	})
}

// renderSuggestionFile wraps SMT's deterministic DDL (with one AI-translated
// expression spliced in) in a banner that makes the provenance — which single
// expression came from the AI — unmistakable.
func renderSuggestionFile(table string, exprErr *driver.ExpressionRenderError, fix *driver.ExpressionFix, ddl string, classMatched bool) string {
	var b strings.Builder
	b.WriteString("-- ============================================================\n")
	b.WriteString("-- AI-ASSISTED FIX · review before applying\n")
	b.WriteString("--\n")
	b.WriteString("-- This is SMT's deterministic DDL with ONE expression translated by\n")
	b.WriteString("-- an AI model. SMT did not and will not apply it automatically.\n")
	b.WriteString(fmt.Sprintf("-- Table:  %s\n", oneLine(table)))
	b.WriteString(fmt.Sprintf("-- Column: %s (%s)\n", oneLine(exprErr.Column), oneLine(exprErr.Kind)))
	b.WriteString(fmt.Sprintf("-- AI-translated: %s  ->  %s\n", oneLine(exprErr.SourceExpr), oneLine(fix.Expression)))
	if strings.TrimSpace(fix.Explanation) != "" {
		b.WriteString("-- Note: " + oneLine(fix.Explanation) + "\n")
	}
	b.WriteString(fmt.Sprintf("-- Confidence: %s (AI)\n", fix.Confidence))
	b.WriteString("--\n")
	b.WriteString("-- Verification (deterministic): every column type, length, nullability,\n")
	b.WriteString("-- identity, and all other defaults are SMT's deterministic output — only\n")
	b.WriteString("-- the one DEFAULT above is AI-authored.\n")
	if classMatched {
		b.WriteString("--   [OK] the translated default matches the source's default class.\n")
	} else {
		b.WriteString("--   [REVIEW] SMT could not mechanically confirm the translated default\n")
		b.WriteString("--   is equivalent to the source — review it before applying.\n")
	}
	b.WriteString("-- ============================================================\n\n")
	b.WriteString(strings.TrimSpace(ddl))
	b.WriteString(";\n")
	return b.String()
}

// oneLine collapses all whitespace (including newlines) to single spaces so an
// AI/source string can't break out of a single-line "-- " banner comment.
func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
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
