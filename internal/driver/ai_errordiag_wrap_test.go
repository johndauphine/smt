package driver

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestWrapText_NoTruncationWithinWidth(t *testing.T) {
	long := "Replace the MSSQL default expression dateadd(year,1,getdate()) with a Postgres-compatible equivalent such as CURRENT_TIMESTAMP + INTERVAL '1 year' on the target."
	lines := wrapText(long, 68)
	for _, l := range lines {
		if utf8.RuneCountInString(l) > 68 {
			t.Errorf("line exceeds width: %q (%d)", l, utf8.RuneCountInString(l))
		}
	}
	if joined := strings.Join(lines, " "); joined != strings.Join(strings.Fields(long), " ") {
		t.Errorf("wrap lost/altered content:\n got: %q", joined)
	}
}

func TestWrapText_HardSplitsLongWord(t *testing.T) {
	w := strings.Repeat("x", 150)
	lines := wrapText(w, 70)
	if len(lines) < 3 {
		t.Fatalf("expected >=3 lines for a 150-char word at width 70, got %d", len(lines))
	}
	if strings.Join(lines, "") != w {
		t.Error("hard-split altered content")
	}
}

func TestFormatBox_NoEllipsisTruncation(t *testing.T) {
	d := &ErrorDiagnosis{
		Cause:       strings.Repeat("a very long cause that should wrap rather than be cut ", 4),
		Suggestions: []string{strings.Repeat("a long actionable suggestion that must not be clipped ", 3)},
		Confidence:  "high", Category: "type_mismatch",
	}
	box := d.FormatBox()
	if strings.Contains(box, "...") {
		t.Errorf("FormatBox still truncates with ellipsis:\n%s", box)
	}
}
