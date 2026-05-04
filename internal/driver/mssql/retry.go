package mssql

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
// retry futile, emit NOT_RETRYABLE — which surfaces as ErrNotRetryable here
// and breaks the loop early with the original DB error preserved.
//
// This replaced isRetryableDDLError (#29 PR B follow-up) — see postgres
// equivalent for the full rationale.
func (w *Writer) retryFinalize(ctx context.Context, req driver.FinalizationDDLRequest, maxRetries int, label string) error {
	// Defensive clamp — see postgres equivalent for rationale.
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
			if errors.Is(err, driver.ErrNotRetryable) {
				logging.Info("%s: AI classified DB error as non-retryable (%v); surfacing original error", label, err)
				return fmt.Errorf("%s: %w\nDDL: %s", label, lastErr, lastDDL)
			}
			return fmt.Errorf("AI DDL generation failed for %s: %w", label, err)
		}

		if _, execErr := w.db.ExecContext(ctx, ddl); execErr == nil {
			if attempt > 0 {
				logging.Info("%s succeeded on retry attempt %d/%d", label, attempt, maxRetries)
			}
			return nil
		} else {
			// Short-circuit on cancellation — see postgres equivalent for rationale.
			if driver.IsCanceled(ctx, execErr) {
				return fmt.Errorf("%s: %w", label, execErr)
			}
			lastDDL = ddl
			lastErr = execErr
		}
	}
	return fmt.Errorf("%s: %w\nDDL: %s", label, lastErr, lastDDL)
}
