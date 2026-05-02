package schemadiff

// AI-driven SQL rendering. Per the user's directive that SMT relies on
// AI at every turn, we do not maintain hand-coded ALTER syntax tables.
// Instead, the structural Diff is described to an LLM and the LLM emits
// the dialect-appropriate SQL plus a risk classification per statement.
//
// The whole diff goes in one prompt so the AI sees the full context
// (e.g. an added column and a new index that covers it). The response
// shape is constrained to JSON so we can parse it deterministically.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// Risk classifies how dangerous a single statement is. The AI fills it
// in based on the change itself (column drop = data-loss, type change on
// large table = rebuild, etc.).
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
	Table       string `json:"table"`
	Description string `json:"description"`
	SQL         string `json:"sql"`
	Risk        Risk   `json:"risk"`
	RiskNotes   string `json:"risk_notes,omitempty"`
}

// Plan is the ordered list of statements that, applied in order, brings
// the target schema in line with the current source schema.
type Plan struct {
	Statements []Statement `json:"statements"`
}

// IsEmpty returns true if the plan has no statements.
func (p Plan) IsEmpty() bool { return len(p.Statements) == 0 }

// SQL returns the plan as a single semicolon-terminated SQL script with
// one comment per statement showing what it does and the AI-judged risk.
// This is what `smt sync` writes to a file when --apply is not set.
func (p Plan) SQL() string {
	var b strings.Builder
	for _, s := range p.Statements {
		fmt.Fprintf(&b, "-- [%s] %s\n", s.Risk, s.Description)
		if s.RiskNotes != "" {
			fmt.Fprintf(&b, "-- note: %s\n", s.RiskNotes)
		}
		b.WriteString(s.SQL)
		b.WriteString(";\n\n")
	}
	return b.String()
}

// Asker abstracts the AI provider. Anything that takes a prompt and
// returns a response satisfies it. *driver.AITypeMapper.Ask is the
// production implementation; tests can pass a stub.
type Asker interface {
	Ask(ctx context.Context, prompt string) (string, error)
}

// Render asks the AI to convert a structural Diff into executable SQL
// for the given target dialect. The whole diff goes in one prompt so the
// AI can reason about ordering and cross-statement dependencies. If the
// diff is empty, no API call is made.
func Render(ctx context.Context, ai Asker, diff Diff, targetSchema, targetDialect string) (Plan, error) {
	if diff.IsEmpty() {
		return Plan{}, nil
	}
	if ai == nil {
		return Plan{}, fmt.Errorf("schema diff renderer requires an AI provider; configure one in ~/.secrets/smt-config.yaml")
	}

	prompt := buildRenderPrompt(diff, targetSchema, targetDialect)
	raw, err := ai.Ask(ctx, prompt)
	if err != nil {
		return Plan{}, fmt.Errorf("AI render failed: %w", err)
	}

	plan, err := parsePlanResponse(raw)
	if err != nil {
		return Plan{}, fmt.Errorf("AI returned unparseable response: %w\nraw response:\n%s", err, raw)
	}
	return plan, nil
}

// buildRenderPrompt constructs the JSON-shaped prompt that asks the LLM
// to emit one statement per change with a risk classification. Keeping
// the input format compact reduces token cost; the output format is
// strict JSON so parsing is reliable.
func buildRenderPrompt(diff Diff, targetSchema, targetDialect string) string {
	payload, _ := json.MarshalIndent(diff, "", "  ")

	var b strings.Builder
	b.WriteString("You are a database schema migration expert. Convert the following ")
	b.WriteString("structural schema diff into ALTER / CREATE / DROP statements for ")
	b.WriteString(targetDialect)
	b.WriteString(" against target schema \"")
	b.WriteString(targetSchema)
	b.WriteString("\".\n\n")

	b.WriteString("Rules:\n")
	b.WriteString("- Quote identifiers per the dialect's convention.\n")
	b.WriteString("- Order statements safely: drop FKs/checks/indexes referencing changed columns first; ")
	b.WriteString("add columns before any new constraint that uses them; drop columns last.\n")
	b.WriteString("- Use the dialect's idiomatic syntax (e.g. PostgreSQL ALTER COLUMN ... TYPE ..., ")
	b.WriteString("MySQL MODIFY COLUMN, SQL Server ALTER COLUMN). Map source types to target types ")
	b.WriteString("conservatively when the dialect changed.\n")
	b.WriteString("- Classify each statement's risk as one of: safe, blocking, rebuild, data-loss-risk.\n")
	b.WriteString("  * safe: backwards-compatible, no lock contention\n")
	b.WriteString("  * blocking: takes a long lock or full table scan\n")
	b.WriteString("  * rebuild: requires table rewrite (slow on large tables)\n")
	b.WriteString("  * data-loss-risk: drops data (column drop, table drop, narrowing type)\n")
	b.WriteString("- For risky statements, include a short risk_notes explaining why.\n\n")

	b.WriteString("Respond with ONLY a JSON object of this exact shape — no markdown fences, ")
	b.WriteString("no commentary, no leading prose:\n")
	b.WriteString("{\n")
	b.WriteString("  \"statements\": [\n")
	b.WriteString("    {\"table\": \"...\", \"description\": \"...\", \"sql\": \"ALTER TABLE ...\", \"risk\": \"safe|blocking|rebuild|data-loss-risk\", \"risk_notes\": \"...\"}\n")
	b.WriteString("  ]\n")
	b.WriteString("}\n\n")

	b.WriteString("Each sql value must be one statement without a trailing semicolon (the caller appends it).\n\n")
	b.WriteString("Schema diff:\n")
	b.Write(payload)

	return b.String()
}

// parsePlanResponse extracts the JSON Plan from the model's response.
// LLMs sometimes wrap responses in ```json fences or prefix them with
// "Here is the plan:" — we strip those before parsing.
func parsePlanResponse(raw string) (Plan, error) {
	cleaned := strings.TrimSpace(raw)
	cleaned = stripCodeFence(cleaned)

	// Find the first { and last } so leading/trailing prose can be ignored.
	start := strings.Index(cleaned, "{")
	end := strings.LastIndex(cleaned, "}")
	if start < 0 || end <= start {
		return Plan{}, fmt.Errorf("no JSON object found in response")
	}
	cleaned = cleaned[start : end+1]

	var plan Plan
	if err := json.Unmarshal([]byte(cleaned), &plan); err != nil {
		return Plan{}, fmt.Errorf("unmarshal: %w", err)
	}
	return plan, nil
}

// stripCodeFence removes ```json ... ``` (or plain ``` ... ```) wrappers.
func stripCodeFence(s string) string {
	if !strings.HasPrefix(s, "```") {
		return s
	}
	// Drop the opening fence (first newline) and any trailing fence.
	if nl := strings.Index(s, "\n"); nl >= 0 {
		s = s[nl+1:]
	}
	s = strings.TrimSuffix(s, "```")
	return strings.TrimSpace(s)
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
