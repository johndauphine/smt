package orchestrator

// History rendering for `smt history`. Pulls run records from the
// checkpoint state backend and prints them as a simple table.

import (
	"fmt"
	"strings"
	"time"

	"smt/internal/checkpoint"
)

// ShowHistory prints all known runs (most recent first) in a small
// fixed-width table.
func (o *Orchestrator) ShowHistory() error {
	runs, err := o.state.GetAllRuns()
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		fmt.Println("No runs recorded yet.")
		return nil
	}

	fmt.Printf("%-36s %-10s %-19s %-19s %s\n", "RUN ID", "STATUS", "STARTED", "ENDED", "PHASE")
	fmt.Println(strings.Repeat("-", 105))
	for _, r := range runs {
		fmt.Printf("%-36s %-10s %-19s %-19s %s\n",
			r.ID, r.Status, fmtTime(&r.StartedAt), fmtTime(r.CompletedAt), r.Phase)
	}
	return nil
}

// ShowRunDetails prints one run's record plus its task list.
func (o *Orchestrator) ShowRunDetails(runID string) error {
	r, err := o.state.GetRunByID(runID)
	if err != nil {
		return err
	}
	if r == nil {
		fmt.Printf("No run with id %s\n", runID)
		return nil
	}
	fmt.Printf("Run:        %s\n", r.ID)
	fmt.Printf("Status:     %s\n", r.Status)
	fmt.Printf("Phase:      %s\n", r.Phase)
	fmt.Printf("Source:     %s\n", r.SourceSchema)
	fmt.Printf("Target:     %s\n", r.TargetSchema)
	fmt.Printf("Started:    %s\n", fmtTime(&r.StartedAt))
	fmt.Printf("Ended:      %s\n", fmtTime(r.CompletedAt))
	if r.Error != "" {
		fmt.Printf("Error:      %s\n", r.Error)
	}

	tasks, err := o.state.GetTasksWithProgress(r.ID)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return nil
	}
	fmt.Println("\nTasks:")
	for _, t := range tasks {
		fmt.Printf("  %-30s %s\n", t.TaskKey, t.Status)
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
