package mssql

import (
	"context"
	"errors"
	"fmt"

	mssql "github.com/microsoft/go-mssqldb"

	"smt/internal/driver"
	"smt/internal/logging"
)

// isAlreadyExists reports whether err is a SQL Server "object already exists"
// error class. See postgres equivalent for the rationale; this is a small,
// focused detection by error number, not a return to the broad allowlist
// removed in #29 PR B follow-up.
//
//	2714 = There is already an object named '...' in the database.
//	1779 = Table 'X' already has a primary key defined on it.
//	1781 = Column 'X' is already constrained.
//	1913 = There is already an object named 'idx' (creating same-name index).
//	2705 = Column names in each table must be unique.
func isAlreadyExists(err error) bool {
	var mErr mssql.Error
	if !errors.As(err, &mErr) {
		return false
	}
	switch mErr.Number {
	case 2714, 1779, 1781, 1913, 2705:
		return true
	}
	return false
}

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
			// Cache the validated DDL post-exec — see postgres equivalent / #32.
			w.finalizationMapper.CacheFinalizationDDL(req, ddl)
			if attempt > 0 {
				logging.Info("%s succeeded on retry attempt %d/%d", label, attempt, maxRetries)
			}
			return nil
		} else if isAlreadyExists(execErr) {
			// See postgres equivalent.
			logging.Info("  ✓ %s already exists (post-exec catch); treating as no-op", label)
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
