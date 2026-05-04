package driver

import (
	"errors"
	"testing"
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
