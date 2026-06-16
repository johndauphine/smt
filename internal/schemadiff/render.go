package schemadiff

import (
	"fmt"
	"strings"
)

// Risk classifies how dangerous a single statement is.
type Risk string

const (
	RiskUnknown       Risk = "unknown"
	RiskSafe          Risk = "safe"           // Backwards-compatible online change
	RiskBlocking      Risk = "blocking"       // Online but takes a long lock or table scan
	RiskRebuildNeeded Risk = "rebuild"        // Requires table rewrite (slow on large tables)
	RiskDataLoss      Risk = "data-loss-risk" // Drops data — operator must confirm
)

// Statement is one DDL statement plus its metadata.
type Statement struct {
	Table       string   `json:"table"`
	Description string   `json:"description"`
	SQL         string   `json:"sql"`
	Risk        Risk     `json:"risk"`
	RiskNotes   string   `json:"risk_notes,omitempty"`
	Warnings    []string `json:"warnings,omitempty"`
	// Kind and Object classify the created object so `create --apply` can
	// gate execution on target existence (idempotent re-runs, #87). Object
	// is the target-normalized object name (table/index/FK/check). Sync-
	// rendered plans leave both empty — sync statements run unconditionally.
	Kind   StatementKind `json:"kind,omitempty"`
	Object string        `json:"object,omitempty"`
}

// StatementKind classifies a plan statement for execution-time gating.
type StatementKind string

const (
	StatementKindSchema     StatementKind = "schema"
	StatementKindTable      StatementKind = "table"
	StatementKindIndex      StatementKind = "index"
	StatementKindForeignKey StatementKind = "foreign_key"
	StatementKindCheck      StatementKind = "check"
)

// Plan is the ordered list of statements that, applied in order, brings
// the target schema in line with the current source schema.
type Plan struct {
	Statements []Statement `json:"statements"`
}

// IsEmpty returns true if the plan has no statements.
func (p Plan) IsEmpty() bool { return len(p.Statements) == 0 }

// SQL returns the plan as a single semicolon-terminated SQL script with
// one comment per statement showing what it does and the classified risk.
// This is what `smt sync` writes to a file when --apply is not set.
func (p Plan) SQL() string {
	var b strings.Builder
	for _, s := range p.Statements {
		fmt.Fprintf(&b, "-- [%s] %s\n", s.Risk, s.Description)
		if s.RiskNotes != "" {
			fmt.Fprintf(&b, "-- note: %s\n", s.RiskNotes)
		}
		for _, warning := range s.Warnings {
			fmt.Fprintf(&b, "-- warning: %s\n", warning)
		}
		b.WriteString(s.SQL)
		b.WriteString(";\n\n")
	}
	return b.String()
}

// FilterByRisk returns a copy of the plan keeping only statements whose
// risk level is at or below maxRisk. Used by `smt sync --apply` to bail
// before executing data-loss statements unless the operator explicitly
// opted in with --allow-data-loss.
func (p Plan) FilterByRisk(maxRisk Risk) Plan {
	rank := map[Risk]int{
		RiskSafe:          0,
		RiskBlocking:      1,
		RiskRebuildNeeded: 2,
		RiskDataLoss:      3,
		RiskUnknown:       3,
	}
	limit := rank[maxRisk]
	out := Plan{}
	for _, s := range p.Statements {
		if rank[s.Risk] <= limit {
			out.Statements = append(out.Statements, s)
		}
	}
	return out
}
