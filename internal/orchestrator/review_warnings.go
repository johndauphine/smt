package orchestrator

import "sync"

type aiReviewWarning struct {
	Label  string   `json:"label"`
	Issues []string `json:"issues"`
}

type reviewWarningRecorder struct {
	mu       sync.Mutex
	warnings []aiReviewWarning
}

func (r *reviewWarningRecorder) Record(label string, issues []string) {
	if r == nil {
		return
	}
	copied := append([]string(nil), issues...)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.warnings = append(r.warnings, aiReviewWarning{Label: label, Issues: copied})
}

func (r *reviewWarningRecorder) Snapshot() []aiReviewWarning {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]aiReviewWarning, len(r.warnings))
	for i := range r.warnings {
		out[i] = aiReviewWarning{
			Label:  r.warnings[i].Label,
			Issues: append([]string(nil), r.warnings[i].Issues...),
		}
	}
	return out
}
