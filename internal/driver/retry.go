package driver

import (
	"errors"
	"fmt"
	"strings"
)

// ErrNotRetryable is returned by GenerateTableDDL / GenerateFinalizationDDL on
// retry calls when the AI examines the prior attempt's database error and
// decides that retrying won't help — for example, the object already exists,
// an FK references a missing parent, the user lacks permission, or the error
// is a real data-integrity violation rather than a fixable AI mistake.
//
// Writers that wrap the mapper in a retry loop (CreateTableWithOptions,
// retryFinalize) should errors.Is-check for this and break out of the loop,
// surfacing the ORIGINAL database error to the user — not the AI's
// classification wrapper, which is bookkeeping. The wrapping ErrNotRetryable
// chain still carries the AI's brief reason string for logging.
//
// This sentinel replaced per-driver isRetryableDDLError SQLSTATE allowlists
// (see #29 PR B follow-up). The allowlist needed manual maintenance — each
// new dialect-class error required a code change — and false negatives kept
// surfacing in matrix runs (e.g. PG SQLSTATE 42883 undefined_function on
// MySQL→PG migrations). Delegating the classification to the AI removes the
// list entirely; the AI already understands SQL error semantics across all
// dialects better than any hand-curated table.
//
// Cost shape vs the old allowlist:
//   - Successful first try: identical (1 AI call, no classifier consulted)
//   - Successful retry: identical (N+1 AI calls)
//   - Non-retryable failure: 2 AI calls (initial + 1 to classify) instead of
//     1 AI call + 0 retries (allowlist) or potentially 1 + max_retries (allowlist
//     false negative). Bounded waste, predictable behavior.
var ErrNotRetryable = errors.New("AI classified database error as non-retryable")

// notRetryableMarker is the literal token the AI must emit on the first line
// when it determines retry won't help. The retry-corrective prompt blocks in
// ai_typemapper.go instruct the model to use exactly this string.
const notRetryableMarker = "NOT_RETRYABLE"

// classifyRetryResponse inspects an AI response and reports whether the AI
// signalled NOT_RETRYABLE. Returns (true, reason) when the response begins
// with the NOT_RETRYABLE marker; the reason is whatever follows the marker
// on the first line (with the optional ": " separator stripped). Returns
// (false, "") for any other response shape — the caller should then proceed
// with normal DDL validation.
func classifyRetryResponse(response string) (notRetryable bool, reason string) {
	trimmed := strings.TrimSpace(response)
	if !strings.HasPrefix(strings.ToUpper(trimmed), notRetryableMarker) {
		return false, ""
	}
	// Strip the marker; whatever remains on the first line is the reason.
	after := strings.TrimSpace(trimmed[len(notRetryableMarker):])
	after = strings.TrimPrefix(after, ":")
	after = strings.TrimSpace(after)
	if i := strings.Index(after, "\n"); i >= 0 {
		after = strings.TrimSpace(after[:i])
	}
	return true, after
}

// WrapNotRetryable produces an error that wraps ErrNotRetryable plus the AI's
// brief reason. Callers can errors.Is(err, ErrNotRetryable) to detect.
func WrapNotRetryable(reason string) error {
	if reason == "" {
		return ErrNotRetryable
	}
	return fmt.Errorf("%w: %s", ErrNotRetryable, reason)
}
