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

const maxAIExpressionFixesPerObject = 10

type expressionFixSuggester interface {
	SuggestExpressionFix(context.Context, driver.FixRequest) (*driver.ExpressionFix, error)
}

// splicedExpressionFix records one AI-authored expression that was validated
// and spliced into an otherwise deterministic object render.
type splicedExpressionFix struct {
	exprErr      *driver.ExpressionRenderError
	fix          *driver.ExpressionFix
	classMatched bool // deterministic class-equivalence verdict
}

// splicedFix is the outcome of translating one or more failing expressions and
// re-rendering the object with those expressions overridden.
type splicedFix struct {
	expressions []splicedExpressionFix
	ddl         string // SMT's deterministic DDL with the expression overrides spliced
}

// handleRenderFailure runs the failure advisories for a single-table render
// error and, when --apply-suggested is set, splices the AI fix into the plan.
// It returns the (marked) spliced DDL and true when the table was fixed
// in-place; false means the caller must return the original error.
//
// AI-authored content reaches the plan / schema.sql ONLY here, and only under
// the explicit --apply-suggested flag — loudly logged and marked inline.
func (o *Orchestrator) handleRenderFailure(ctx context.Context, runID, operation string, renderer createDDLRenderer, t *driver.Table, cause error) (string, bool) {
	if o.config == nil || t == nil {
		return "", false
	}
	o.diagnoseSchemaFailure(ctx, t.Name, t.Schema, operation, cause)

	// Only do AI splice calls if a suggestion or apply is actually requested.
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

	var markers strings.Builder
	for _, expr := range sf.expressions {
		verdict := expressionFixVerdict(expr.classMatched)
		logging.Warn("APPLYING AI-ASSISTED FIX (--apply-suggested): table %s, %s %s:  %s -> %s  [%s]. This expression was authored by an AI model.",
			oneLine(t.Name), oneLine(expr.exprErr.Kind), oneLine(expr.exprErr.Column), oneLine(expr.exprErr.SourceExpr), oneLine(expr.fix.Expression), verdict)
		fmt.Fprintf(&markers, "-- AI-ASSISTED FIX (--apply-suggested): %s %s  %s -> %s  [%s]\n",
			oneLine(expr.exprErr.Kind), oneLine(expr.exprErr.Column), oneLine(expr.exprErr.SourceExpr), oneLine(expr.fix.Expression), verdict)
	}
	return markers.String() + sf.ddl, true
}

// spliceFix attempts the AI splice loop for structured expression render
// failures: translate the failing expression (a column DEFAULT or a CHECK
// predicate), re-render that object with the expression overridden, and repeat
// while the same object exposes another spliceable expression failure. Everything
// outside the overridden expressions stays SMT's deterministic output. The loop
// is bounded by the number of expression-bearing objects on the table, capped at
// maxAIExpressionFixesPerObject, and aborts if the same expression fails twice.
func (o *Orchestrator) spliceFix(ctx context.Context, renderer createDDLRenderer, t *driver.Table, cause error) (*splicedFix, bool) {
	if o.config == nil || cause == nil || t == nil {
		return nil, false
	}
	var exprErr *driver.ExpressionRenderError
	if !errors.As(cause, &exprErr) || (exprErr.Kind != "default" && exprErr.Kind != "check") {
		return nil, false // not a single splice-able expression — diagnosis-only
	}

	suggester := o.expressionFixSuggester()
	if suggester == nil {
		return nil, false
	}

	spliced := cloneTableForExpressionSplice(t)
	mode := exprErr.Kind
	limit := expressionFixLimit(&spliced)
	seen := map[string]bool{}
	fixes := make([]splicedExpressionFix, 0, limit)

	for {
		if exprErr.Kind != "default" && exprErr.Kind != "check" {
			logging.Warn("AI-assisted expression repair aborted for table %s: unsupported expression kind %q after %d fix(es)",
				oneLine(t.Name), oneLine(exprErr.Kind), len(fixes))
			return nil, false
		}
		if exprErr.Kind != mode {
			logging.Warn("AI-assisted expression repair aborted for table %s: re-render switched from %s to %s after %d fix(es)",
				oneLine(t.Name), oneLine(mode), oneLine(exprErr.Kind), len(fixes))
			return nil, false
		}
		if len(fixes) >= limit {
			logging.Warn("AI-assisted expression repair aborted for table %s: reached safety limit of %d expression fix(es)",
				oneLine(t.Name), limit)
			return nil, false
		}
		key := expressionFailureKey(exprErr)
		if seen[key] {
			logging.Warn("AI-assisted expression repair aborted for table %s: repeated %s expression failure for %s",
				oneLine(t.Name), oneLine(exprErr.Kind), oneLine(exprErr.Column))
			return nil, false
		}
		seen[key] = true

		colIdx, chkIdx, colType, ok := expressionLocation(&spliced, exprErr)
		if !ok {
			logging.Debug("AI expression fix unavailable: failed %s %q not found on table %s",
				exprErr.Kind, exprErr.Column, t.Name)
			return nil, false
		}

		fix, err := suggester.SuggestExpressionFix(ctx, driver.FixRequest{
			Kind:          exprErr.Kind,
			SourceExpr:    exprErr.SourceExpr,
			ColumnName:    exprErr.Column,
			ColumnType:    colType,
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

		fixRecord := splicedExpressionFix{exprErr: exprErr, fix: fix}
		var ddl string
		var rerr error
		switch exprErr.Kind {
		case "default":
			spliced.Columns[colIdx].DefaultExpressionOverride = strings.TrimSpace(fix.Expression)
			ddl, rerr = renderer.renderTable(ctx, &spliced)
			fixRecord.classMatched = driver.DefaultExpressionsEquivalent(exprErr.SourceExpr, fix.Expression)
		case "check":
			spliced.CheckConstraints[chkIdx].DefinitionOverride = strings.TrimSpace(fix.Expression)
			chk := spliced.CheckConstraints[chkIdx]
			ddl, rerr = renderer.renderCheck(ctx, &spliced, &chk)
			// No deterministic class check for a boolean CHECK predicate — always
			// flagged for review.
		}

		fixes = append(fixes, fixRecord)
		if rerr == nil {
			return &splicedFix{expressions: fixes, ddl: ddl}, true
		}
		var next *driver.ExpressionRenderError
		if !errors.As(rerr, &next) {
			logging.Warn("AI-assisted expression repair aborted for table %s: re-render failed after %d fix(es): %v",
				oneLine(t.Name), len(fixes), rerr)
			return nil, false
		}
		logging.Debug("re-render with %d AI expression fix(es) found another expression failure: %v", len(fixes), rerr)
		exprErr = next
	}
}

// writeSuggestion writes the AI-assisted suggestion to schema.suggested.sql,
// once even under concurrent failures (suggest_fixes path). It never writes to
// schema.sql and the run still fails — review-only.
func (o *Orchestrator) writeSuggestion(runID, table string, sf *splicedFix) {
	o.suggestOnce.Do(func() {
		content := renderSuggestionFile(table, sf)
		if werr := o.writeSQLArtifact(runID, "schema.suggested.sql", content); werr != nil {
			logging.Debug("writing schema.suggested.sql: %v", werr)
			return
		}
		verdict := "review translated expression(s) that were not mechanically confirmed"
		if allExpressionFixClassesMatched(sf) {
			verdict = "translated expression(s) match the source class"
		}
		logging.Warn("AI-assisted fix written to %s — %s. Review before applying; SMT did NOT apply it (use --apply-suggested to apply).",
			filepath.Join(o.ddlArtifactDir(runID), "schema.suggested.sql"), verdict)
	})
}

// renderSuggestionFile wraps SMT's deterministic DDL (with AI-translated
// expression(s) spliced in) in a banner that makes provenance unmistakable.
func renderSuggestionFile(table string, sf *splicedFix) string {
	var b strings.Builder
	b.WriteString("-- ============================================================\n")
	b.WriteString("-- AI-ASSISTED FIX · review before applying\n")
	b.WriteString("--\n")
	b.WriteString("-- This is SMT's deterministic DDL with the expression(s) below\n")
	b.WriteString("-- translated by an AI model. SMT did not and will not apply it\n")
	b.WriteString("-- automatically.\n")
	b.WriteString(fmt.Sprintf("-- Table:  %s\n", oneLine(table)))
	for i, expr := range sf.expressions {
		b.WriteString(fmt.Sprintf("-- Object %d: %s (%s)\n", i+1, oneLine(expr.exprErr.Column), oneLine(expr.exprErr.Kind)))
		b.WriteString(fmt.Sprintf("-- AI-translated %d: %s  ->  %s\n", i+1, oneLine(expr.exprErr.SourceExpr), oneLine(expr.fix.Expression)))
		if strings.TrimSpace(expr.fix.Explanation) != "" {
			b.WriteString(fmt.Sprintf("-- Note %d: %s\n", i+1, oneLine(expr.fix.Explanation)))
		}
		b.WriteString(fmt.Sprintf("-- Confidence %d: %s (AI)\n", i+1, expr.fix.Confidence))
	}
	b.WriteString("--\n")
	b.WriteString("-- Verification (deterministic): every column type, length, nullability,\n")
	b.WriteString("-- identity, and all defaults/constraints not listed above are SMT's\n")
	b.WriteString("-- deterministic output — only the expression(s) above are AI-authored.\n")
	if allExpressionFixClassesMatched(sf) {
		b.WriteString("--   [OK] the translated expression(s) match the source class.\n")
	} else {
		b.WriteString("--   [REVIEW] SMT could not mechanically confirm every translated\n")
		b.WriteString("--   expression is equivalent to the source — review before applying.\n")
	}
	b.WriteString("-- ============================================================\n\n")
	b.WriteString(strings.TrimSpace(sf.ddl))
	b.WriteString(";\n")
	return b.String()
}

// oneLine collapses all whitespace (including newlines) to single spaces so an
// AI/source string can't break out of a single-line "-- " banner comment.
func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func expressionFixVerdict(classMatched bool) string {
	if classMatched {
		return "OK: expression class matches source"
	}
	return "REVIEW: class NOT mechanically confirmed"
}

func allExpressionFixClassesMatched(sf *splicedFix) bool {
	if sf == nil || len(sf.expressions) == 0 {
		return false
	}
	for _, expr := range sf.expressions {
		if !expr.classMatched {
			return false
		}
	}
	return true
}

func cloneTableForExpressionSplice(t *driver.Table) driver.Table {
	spliced := *t
	spliced.Columns = append([]driver.Column(nil), t.Columns...)
	spliced.CheckConstraints = append([]driver.CheckConstraint(nil), t.CheckConstraints...)
	return spliced
}

func expressionFixLimit(t *driver.Table) int {
	n := 0
	for _, col := range t.Columns {
		if strings.TrimSpace(col.DefaultExpression) != "" {
			n++
		}
	}
	for _, chk := range t.CheckConstraints {
		if strings.TrimSpace(chk.Definition) != "" {
			n++
		}
	}
	if n < 1 {
		return 1
	}
	if n > maxAIExpressionFixesPerObject {
		return maxAIExpressionFixesPerObject
	}
	return n
}

func expressionLocation(t *driver.Table, exprErr *driver.ExpressionRenderError) (colIdx, chkIdx int, colType string, ok bool) {
	colIdx, chkIdx = -1, -1
	switch exprErr.Kind {
	case "default":
		for i := range t.Columns {
			if t.Columns[i].Name == exprErr.Column {
				return i, -1, t.Columns[i].DataType, true
			}
		}
	case "check":
		for i := range t.CheckConstraints {
			if t.CheckConstraints[i].Name == exprErr.Column {
				return -1, i, "", true
			}
		}
	}
	return colIdx, chkIdx, "", false
}

func expressionFailureKey(exprErr *driver.ExpressionRenderError) string {
	if exprErr == nil {
		return ""
	}
	return exprErr.Kind + "\x00" + exprErr.Column
}

func (o *Orchestrator) expressionFixSuggester() expressionFixSuggester {
	if o.fixSuggester != nil {
		return o.fixSuggester
	}
	return o.errorDiagnoser()
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
