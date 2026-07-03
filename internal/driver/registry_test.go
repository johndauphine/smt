package driver

import (
	"strings"
	"testing"
)

func TestGetUnknownDriverReportsAvailableWithoutNestedAvailableCall(t *testing.T) {
	_, err := Get("__definitely_missing__")
	if err == nil {
		t.Fatal("expected unknown driver error")
	}
	if got := err.Error(); !strings.Contains(got, "available:") {
		t.Fatalf("unknown driver error should include available drivers, got %q", got)
	}
}
