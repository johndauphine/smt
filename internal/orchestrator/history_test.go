package orchestrator

import (
	"testing"
	"time"
)

func TestFmtTimeUsesUTCLabel(t *testing.T) {
	loc := time.FixedZone("offset", -5*60*60)
	ts := time.Date(2026, 7, 3, 10, 30, 0, 0, loc)

	if got := fmtTime(&ts); got != "2026-07-03T15:30:00Z" {
		t.Fatalf("fmtTime() = %q, want UTC RFC3339", got)
	}
}
