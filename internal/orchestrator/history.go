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

// ShowHistory writes all known runs (most recent first) to w in a small
// fixed-width table.
func (o *Orchestrator) ShowHistory(w io.Writer) error {
	runs, err := o.state.GetAllRuns()
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		fmt.Fprintln(w, "No runs recorded yet.")
		return nil
	}

	fmt.Fprintf(w, "%-36s %-10s %-19s %-19s %s\n", "RUN ID", "STATUS", "STARTED", "ENDED", "PHASE")
	fmt.Fprintln(w, strings.Repeat("-", 105))
	for _, r := range runs {
		fmt.Fprintf(w, "%-36s %-10s %-19s %-19s %s\n",
			r.ID, r.Status, fmtTime(&r.StartedAt), fmtTime(r.CompletedAt), r.Phase)
	}
	return nil
}

// ShowRunDetails writes one run's record plus its task list to w.
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
	fmt.Fprintf(w, "Status:     %s\n", r.Status)
	fmt.Fprintf(w, "Phase:      %s\n", r.Phase)
	fmt.Fprintf(w, "Source:     %s\n", r.SourceSchema)
	fmt.Fprintf(w, "Target:     %s\n", r.TargetSchema)
	fmt.Fprintf(w, "Started:    %s\n", fmtTime(&r.StartedAt))
	fmt.Fprintf(w, "Ended:      %s\n", fmtTime(r.CompletedAt))
	if r.Error != "" {
		fmt.Fprintf(w, "Error:      %s\n", r.Error)
	}

	tasks, err := o.state.GetTasksWithProgress(r.ID)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return nil
	}
	fmt.Fprintln(w, "\nTasks:")
	for _, t := range tasks {
		fmt.Fprintf(w, "  %-30s %s\n", t.TaskKey, t.Status)
	}
	return nil
}

func fmtTime(t *time.Time) string {
	if t == nil || t.IsZero() {
		return "-"
	}
	return t.Format("2006-01-02 15:04:05")
}

// asserts checkpoint.Run is the type we expect (compile-time check).
var _ = (*checkpoint.Run)(nil)
