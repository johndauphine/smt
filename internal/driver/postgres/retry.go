package postgres

import (
	"context"
	"errors"
	"fmt"

	"smt/internal/driver"
	"smt/internal/logging"
)

// retryFinalize generates DDL via the finalization mapper and executes it,
// retrying up to maxRetries times. Each retry feeds the prior failed DDL plus
// the verbatim database error back into the AI prompt via PreviousAttempt;
// the prompt invites the AI to either return corrected DDL or, if it judges
// retry futile, emit NOT_RETRYABLE — which surfaces here as ErrNotRetryable
// and breaks the loop early with the original DB error preserved.
//
// This replaced the per-driver isRetryableDDLError SQLSTATE allowlist (#29
// PR B follow-up): the allowlist needed manual maintenance and false negatives
// kept appearing (e.g. PG 42883 undefined_function on mysql→pg). Delegating
// the classification to the AI removes the list. See driver/retry.go for the
// sentinel + parsing helper.
//
// label is used for logging and the wrapping error message (e.g. "index
// Orders.idx_customer", "FK Orders.fk_customer"). The finalization mapper
// has no cache, so a successful retry doesn't need any re-prime step.
func (w *Writer) retryFinalize(ctx context.Context, req driver.FinalizationDDLRequest, maxRetries int, label string) error {
	// Defensive clamp: a negative budget would skip the loop body entirely
	// (no AI call, no exec) and surface a confusing wrapped-nil error.
	// Orchestrator.aiMaxRetries already maps negatives to 0; this guard
	// exists for direct WithOptions callers. (Copilot review on PR #31.)
	if maxRetries < 0 {
		maxRetries = 0
	}
	var (
		lastDDL string
		lastErr error
	)
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			req.PreviousAttempt = &driver.FinalizationDDLAttempt{
				DDL:   lastDDL,
				Error: lastErr.Error(),
			}
			logging.Info("retry attempt %d/%d for %s after DDL error: %v",
				attempt, maxRetries, label, lastErr)
		}

		ddl, err := w.finalizationMapper.GenerateFinalizationDDL(ctx, req)
		if err != nil {
			// AI examined the prior error and classified it as non-retryable.
			// Surface the original DB error — that's what the user can act on.
			if errors.Is(err, driver.ErrNotRetryable) {
				logging.Info("%s: AI classified DB error as non-retryable (%v); surfacing original error", label, err)
				return fmt.Errorf("%s: %w\nDDL: %s", label, lastErr, lastDDL)
			}
			return fmt.Errorf("AI DDL generation failed for %s: %w", label, err)
		}

		if _, execErr := w.pool.Exec(ctx, ddl); execErr == nil {
			if attempt > 0 {
				logging.Info("%s succeeded on retry attempt %d/%d", label, attempt, maxRetries)
			}
			return nil
		} else {
			// Short-circuit on cancellation — without this guard the next
			// iteration would re-prompt the AI to "fix" a Ctrl-C against an
			// already-canceled context, surfacing an AI wrapper error instead
			// of the cancellation. (codex review on PR #31.)
			if driver.IsCanceled(ctx, execErr) {
				return fmt.Errorf("%s: %w", label, execErr)
			}
			lastDDL = ddl
			lastErr = execErr
			// No classifier — let the next iteration ask the AI. If we've
			// exhausted maxRetries the for condition exits the loop naturally.
		}
	}
	return fmt.Errorf("%s: %w\nDDL: %s", label, lastErr, lastDDL)
}
