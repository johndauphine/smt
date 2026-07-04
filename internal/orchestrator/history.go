package orchestrator

// History rendering for `smt history`. Pulls run records from the
// checkpoint state backend and prints them as a simple table.

import (
	"fmt"
	"io"
	"strings"
	"time"

	"smt/internal/checkpoint"
)

// ShowHistory renders all known runs (most recent first) into w as a small
// fixed-width table. Callers pass os.Stdout for the CLI or a buffer for the
// TUI — the renderer never touches a global stream (see #228).
func (o *Orchestrator) ShowHistory(w io.Writer) error {
	runs, err := o.state.GetAllRuns()
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		fmt.Fprintln(w, "No runs recorded yet.")
		return nil
	}

	fmt.Fprintf(w, "%-36s %-9s %-10s %-20s %-20s %s\n", "RUN ID", "KIND", "STATUS", "STARTED", "ENDED", "PHASE")
	fmt.Fprintln(w, strings.Repeat("-", 117))
	for _, r := range runs {
		fmt.Fprintf(w, "%-36s %-9s %-10s %-20s %-20s %s\n",
			r.ID, runKindLabel(r.Kind), r.Status, fmtTime(&r.StartedAt), fmtTime(r.CompletedAt), r.Phase)
	}
	return nil
}

// ShowRunDetails renders one run's record plus its task list into w.
func (o *Orchestrator) ShowRunDetails(w io.Writer, runID string) error {
	r, err := o.state.GetRunByID(runID)
	if err != nil {
		return err
	}
	if r == nil {
		fmt.Fprintf(w, "No run with id %s\n", runID)
		return nil
	}
	fmt.Fprintf(w, "Run:        %s\n", r.ID)
	fmt.Fprintf(w, "Kind:       %s\n", runKindLabel(r.Kind))
	fmt.Fprintf(w, "Status:     %s\n", r.Status)
	fmt.Fprintf(w, "Phase:      %s\n", r.Phase)
	fmt.Fprintf(w, "Source:     %s\n", r.SourceSchema)
	fmt.Fprintf(w, "Target:     %s\n", r.TargetSchema)
	fmt.Fprintf(w, "Started:    %s\n", fmtTime(&r.StartedAt))
	fmt.Fprintf(w, "Ended:      %s\n", fmtTime(r.CompletedAt))
	if r.Error != "" {
		fmt.Fprintf(w, "Error:      %s\n", r.Error)
	}
	return nil
}

func runKindLabel(kind string) string {
	if kind == "" {
		return checkpoint.RunKindApply
	}
	return kind
}

func fmtTime(t *time.Time) string {
	if t == nil || t.IsZero() {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

// asserts checkpoint.Run is the type we expect (compile-time check).
var _ = (*checkpoint.Run)(nil)
