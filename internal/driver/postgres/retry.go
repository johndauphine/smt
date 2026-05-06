package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"smt/internal/driver"
	"smt/internal/logging"
)

// isAlreadyExists reports whether err is a PostgreSQL "object already exists"
// error class. Used by the create paths to treat re-run idempotency failures
// as no-op success — the pre-exec existence probes catch most cases, but the
// AI sometimes renames constraint identifiers in a way our normalizer doesn't
// predict (e.g. CK_X → chk_x for some inputs and ck_x for others), so the
// probe misses and we land here. Detection is by SQLSTATE so it's stable
// across pgx versions and PG releases.
//
// 42710 = duplicate_object — covers indexes, foreign keys, check constraints,
//
//	primary keys, sequences, and most other relation-bound objects.
//
// 42P07 = duplicate_table.
// 42P06 = duplicate_schema (caught for completeness; CreateSchema already
//
//	uses IF NOT EXISTS).
func isAlreadyExists(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code {
	case "42710", "42P07", "42P06":
		return true
	}
	return false
}

// retryFinalize generates DDL via the finalization mapper and executes it,
// retrying up to opts.MaxRetries times. Each retry feeds the prior failed
// DDL plus the verbatim database error back into the AI prompt via
// PreviousAttempt; the prompt invites the AI to either return corrected DDL
// or, if it judges retry futile, emit NOT_RETRYABLE — which surfaces here
// as ErrNotRetryable and breaks the loop early with the original DB error
// preserved.
//
// When opts.AIVerify is true, an AI self-check pass runs between Generate
// and Exec. Verifier ISSUES are fed back as PreviousAttempt.Error in the
// next iteration, sharing the same retry budget as exec failures.
//
// This replaced the per-driver isRetryableDDLError SQLSTATE allowlist (#29
// PR B follow-up): the allowlist needed manual maintenance and false negatives
// kept appearing (e.g. PG 42883 undefined_function on mysql→pg). Delegating
// the classification to the AI removes the list. See driver/retry.go for the
// sentinel + parsing helper.
//
// label is used for logging and the wrapping error message (e.g. "index
// Orders.idx_customer", "FK Orders.fk_customer").
func (w *Writer) retryFinalize(ctx context.Context, req driver.FinalizationDDLRequest, opts driver.FinalizeOptions, label string) error {
	// Defensive clamp: a negative budget would skip the loop body entirely
	// (no AI call, no exec) and surface a confusing wrapped-nil error.
	// Orchestrator.aiMaxRetries already maps negatives to 0; this guard
	// exists for direct WithOptions callers. (Copilot review on PR #31.)
	maxRetries := opts.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	var (
		lastDDL          string
		lastErr          error
		lastFromVerifier bool
	)
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			req.PreviousAttempt = &driver.FinalizationDDLAttempt{
				DDL:          lastDDL,
				Error:        lastErr.Error(),
				FromVerifier: lastFromVerifier,
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

		// AI self-check between gen and exec (opt-in via opts.AIVerify).
		// On ISSUES, retry generation with the verifier's complaints fed in.
		if opts.AIVerify {
			vReq := driver.VerifyFinalizationDDLRequest{
				Type: req.Type, SourceDBType: req.SourceDBType, TargetDBType: req.TargetDBType,
				Table: req.Table, TargetSchema: req.TargetSchema, TargetContext: req.TargetContext,
				Index: req.Index, ForeignKey: req.ForeignKey, CheckConstraint: req.CheckConstraint,
				ProposedDDL: ddl,
			}
			verdict, vErr := w.finalizationMapper.VerifyFinalizationDDL(ctx, vReq)
			if vErr != nil {
				return fmt.Errorf("AI verify failed for %s: %w", label, vErr)
			}
			if !verdict.OK {
				logging.Warn("verify flagged %d issue(s) on %s, retrying:\n  %s",
					len(verdict.Issues), label, strings.Join(verdict.Issues, "\n  "))
				lastDDL = ddl
				lastErr = fmt.Errorf("%s", strings.Join(verdict.Issues, "\n"))
				lastFromVerifier = true
				continue
			}
			logging.Debug("verify OK: %s", label)
		}

		if _, execErr := w.pool.Exec(ctx, ddl); execErr == nil {
			// Cache the validated DDL post-exec — same #32 pattern as
			// CacheTableDDL. Only validated DDL ever reaches the cache, so a
			// failed-but-not-corrected DDL can't poison subsequent runs.
			w.finalizationMapper.CacheFinalizationDDL(req, ddl)
			if attempt > 0 {
				logging.Info("%s succeeded on retry attempt %d/%d", label, attempt, maxRetries)
			}
			return nil
		} else if isAlreadyExists(execErr) {
			// Pre-exec existence probes catch most re-run cases, but the AI
			// sometimes renames identifiers in ways the normalizer doesn't
			// predict — the probe by name then misses, and we land here. Treat
			// as no-op success rather than letting the AI retry into the same
			// failure. Don't cache the DDL: the catalog name diverges from
			// what we'd compute for this request, so there's no guarantee
			// future runs would land here for the same key.
			logging.Info("  ✓ %s already exists (post-exec catch); treating as no-op", label)
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
			lastFromVerifier = false
			// No classifier — let the next iteration ask the AI. If we've
			// exhausted maxRetries the for condition exits the loop naturally.
		}
	}
	return fmt.Errorf("%s: %w\nDDL: %s", label, lastErr, lastDDL)
}
