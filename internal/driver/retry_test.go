package driver

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestClassifyRetryResponse pins the parser for the AI's retry-classification
// response shape. The model is instructed to emit NOT_RETRYABLE on the first
// line (optionally followed by ": <reason>") when it judges retry futile;
// any other shape is treated as a normal DDL response and falls through to
// the existing prefix validators in parseTableDDLResponse / GenerateFinalizationDDL.
func TestClassifyRetryResponse(t *testing.T) {
	tests := []struct {
		name         string
		response     string
		wantNotRetry bool
		wantReason   string
	}{
		{"empty string", "", false, ""},
		{"plain DDL response (CREATE TABLE)", "CREATE TABLE foo (id int);", false, ""},
		{"plain DDL response (ALTER TABLE)", "ALTER TABLE foo ADD CONSTRAINT chk_foo CHECK (x > 0);", false, ""},
		{"plain DDL with leading whitespace", "  \n\tCREATE INDEX idx_foo ON foo (id);", false, ""},
		// The marker, alone or with various separators
		{"marker alone", "NOT_RETRYABLE", true, ""},
		{"marker with colon-space and reason", "NOT_RETRYABLE: object already exists", true, "object already exists"},
		{"marker with just colon (no space)", "NOT_RETRYABLE:permission denied", true, "permission denied"},
		{"marker with leading whitespace", "  NOT_RETRYABLE: deadlock", true, "deadlock"},
		// Multi-line: only the first line is the reason
		{
			"multi-line, marker on first line",
			"NOT_RETRYABLE: FK target missing\nadditional commentary the model added",
			true,
			"FK target missing",
		},
		// Case insensitivity (the prompt asks for uppercase, but be tolerant)
		{"lowercase marker", "not_retryable: just in case", true, "just in case"},
		// Marker not at start: NOT a classification, just normal DDL that mentions it
		{"marker mid-string is not classification", "CREATE TABLE x (y int); -- NOT_RETRYABLE comment", false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotNotRetry, gotReason := classifyRetryResponse(tt.response)
			if gotNotRetry != tt.wantNotRetry {
				t.Errorf("classifyRetryResponse(%q) notRetryable = %v, want %v",
					tt.response, gotNotRetry, tt.wantNotRetry)
			}
			if gotReason != tt.wantReason {
				t.Errorf("classifyRetryResponse(%q) reason = %q, want %q",
					tt.response, gotReason, tt.wantReason)
			}
		})
	}
}

// TestWrapNotRetryable confirms the sentinel is errors.Is-detectable through
// the wrapping helper, which is the contract every writer's retry loop
// depends on for the early-exit branch.
func TestWrapNotRetryable(t *testing.T) {
	t.Run("with reason", func(t *testing.T) {
		err := WrapNotRetryable("object already exists")
		if !errors.Is(err, ErrNotRetryable) {
			t.Errorf("errors.Is should detect ErrNotRetryable through wrap")
		}
		if got := err.Error(); got != "AI classified database error as non-retryable: object already exists" {
			t.Errorf("unexpected formatted error: %q", got)
		}
	})

	t.Run("without reason", func(t *testing.T) {
		err := WrapNotRetryable("")
		if !errors.Is(err, ErrNotRetryable) {
			t.Errorf("errors.Is should detect ErrNotRetryable for empty reason")
		}
		// Empty reason returns the sentinel directly, not a wrap.
		if err != ErrNotRetryable {
			t.Errorf("empty reason should return the sentinel directly, got %v", err)
		}
	})
}

// TestIsCanceled pins the cancellation guard the writer retry loops use to
// short-circuit when the user hits Ctrl-C (or a deadline expires) mid-DDL,
// instead of feeding the cancellation back to the AI as if it were a fixable
// error. The earlier SQLSTATE allowlist guarded against this incidentally
// (context.Canceled isn't a *pgconn.PgError); the AI-classifier conversion
// removed that incidental guard, so we need an explicit one. See codex
// review on PR #31.
func TestIsCanceled(t *testing.T) {
	t.Run("nil ctx error and nil err → false", func(t *testing.T) {
		if IsCanceled(context.Background(), nil) {
			t.Errorf("IsCanceled(Background, nil) should be false")
		}
	})

	t.Run("nil ctx error, real DDL error → false", func(t *testing.T) {
		if IsCanceled(context.Background(), errors.New("relation already exists")) {
			t.Errorf("IsCanceled with non-cancel error and live ctx should be false")
		}
	})

	t.Run("canceled ctx → true regardless of err", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if !IsCanceled(ctx, nil) {
			t.Errorf("IsCanceled with canceled ctx should be true even when err is nil")
		}
		if !IsCanceled(ctx, errors.New("any other error")) {
			t.Errorf("IsCanceled with canceled ctx should be true regardless of err")
		}
	})

	t.Run("deadline-exceeded ctx → true", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		if !IsCanceled(ctx, nil) {
			t.Errorf("IsCanceled with expired deadline should be true")
		}
	})

	t.Run("context.Canceled wrapped in driver error → true", func(t *testing.T) {
		// Simulates the case where the parent context's cancellation isn't
		// reflected on this leaf ctx (rare, but possible) but the DB driver
		// surfaces the canceled error from a connection pool.
		err := fmt.Errorf("driver-level wrap: %w", context.Canceled)
		if !IsCanceled(context.Background(), err) {
			t.Errorf("IsCanceled should detect context.Canceled through error wrap")
		}
	})

	t.Run("context.DeadlineExceeded wrapped → true", func(t *testing.T) {
		err := fmt.Errorf("driver-level wrap: %w", context.DeadlineExceeded)
		if !IsCanceled(context.Background(), err) {
			t.Errorf("IsCanceled should detect context.DeadlineExceeded through error wrap")
		}
	})
}

// TestParseTableDDLResponse_NotRetryable wires the parser through to confirm
// the AI's NOT_RETRYABLE classification flows out as ErrNotRetryable from the
// table-DDL parsing path that GenerateTableDDL calls. This is the integration
// point the writer's retry loop relies on for early exit.
func TestParseTableDDLResponse_NotRetryable(t *testing.T) {
	mapper := testMapperWithTempCache(t, "anthropic", testProvider("test-key"))

	resp, err := mapper.parseTableDDLResponse("NOT_RETRYABLE: relation \"foo\" already exists", &Table{Name: "foo"})
	if !errors.Is(err, ErrNotRetryable) {
		t.Fatalf("expected ErrNotRetryable, got err=%v resp=%v", err, resp)
	}
	if resp != nil {
		t.Errorf("expected nil response on NOT_RETRYABLE, got %v", resp)
	}
}
