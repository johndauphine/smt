package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// ExpressionFix is an AI-proposed translation of a SINGLE source-dialect
// expression (a DEFAULT/CHECK that SMT's deterministic renderer couldn't map)
// into the target dialect. SMT splices just this one expression into its own
// otherwise-deterministic DDL — the AI never authors a whole table (#134).
type ExpressionFix struct {
	// Expression is the target-dialect replacement expression.
	Expression string `json:"expression"`

	// Explanation is a short note on the translation.
	Explanation string `json:"explanation"`

	// Confidence is high | medium | low.
	Confidence string `json:"confidence"`
}

// FixRequest describes the one expression that needs translating.
type FixRequest struct {
	Kind          string // "default" | "check"
	SourceExpr    string // raw source-dialect expression
	ColumnName    string
	ColumnType    string // source column type, for context
	SourceDialect string
	TargetDialect string
}

// SuggestExpressionFix asks the AI to translate one expression to the target
// dialect. The result is advisory and is spliced into SMT's deterministic DDL,
// never applied automatically.
func (d *AIErrorDiagnoser) SuggestExpressionFix(ctx context.Context, req FixRequest) (*ExpressionFix, error) {
	if d.aiMapper == nil {
		return nil, fmt.Errorf("AI mapper not configured")
	}
	response, err := d.aiMapper.CallAI(ctx, buildExpressionFixPrompt(req))
	if err != nil {
		return nil, fmt.Errorf("AI expression-fix suggestion failed: %w", err)
	}
	return parseExpressionFix(response)
}

func buildExpressionFixPrompt(req FixRequest) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Translate a single SQL %s expression from %s to %s. SMT's deterministic renderer could not map it.\n\n", strings.ToUpper(req.Kind), req.SourceDialect, req.TargetDialect))
	sb.WriteString("STRICT RULES:\n")
	sb.WriteString(fmt.Sprintf("- Return ONLY the replacement %s expression for %s, valid in %s.\n", req.Kind, req.TargetDialect, req.TargetDialect))
	sb.WriteString("- Preserve the exact semantics; do not add casts, columns, or surrounding DDL.\n")
	sb.WriteString("- Do not include the DEFAULT/CHECK keyword, the column name, or a trailing semicolon — just the expression.\n\n")
	if req.ColumnName != "" {
		sb.WriteString(fmt.Sprintf("Column: %s", req.ColumnName))
		if req.ColumnType != "" {
			sb.WriteString(fmt.Sprintf(" (%s)", req.ColumnType))
		}
		sb.WriteString("\n")
	}
	sb.WriteString(fmt.Sprintf("Source %s expression: %s\n\n", req.Kind, req.SourceExpr))
	sb.WriteString("Respond with ONLY a JSON object (no markdown):\n")
	sb.WriteString(`{
  "expression": "<target-dialect expression>",
  "explanation": "what you changed and why (1 sentence)",
  "confidence": "high|medium|low"
}`)
	return sb.String()
}

func parseExpressionFix(response string) (*ExpressionFix, error) {
	response = strings.TrimSpace(response)
	response = strings.TrimPrefix(response, "```json")
	response = strings.TrimPrefix(response, "```")
	response = strings.TrimSuffix(response, "```")
	response = strings.TrimSpace(response)

	var fix ExpressionFix
	if err := json.Unmarshal([]byte(response), &fix); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w (response: %s)", err, truncateString(response, 120))
	}
	if strings.TrimSpace(fix.Expression) == "" {
		return nil, fmt.Errorf("expression-fix suggestion missing expression")
	}
	switch strings.ToLower(fix.Confidence) {
	case "high", "medium", "low":
		fix.Confidence = strings.ToLower(fix.Confidence)
	default:
		fix.Confidence = "medium"
	}
	return &fix, nil
}
