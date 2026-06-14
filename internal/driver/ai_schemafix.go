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

// ValidateTargetExpression rejects an AI-proposed expression that isn't a single
// self-contained value expression, so splicing it verbatim into a column's
// DEFAULT clause can't break out and inject extra columns or statements into the
// CREATE TABLE. It checks: non-empty, balanced parens/quotes, and no top-level
// (unquoted, unparenthesized) comma or semicolon. It is a structural guard, not
// a semantic or dialect validator.
func ValidateTargetExpression(expr string) error {
	s := strings.TrimSpace(expr)
	if s == "" {
		return fmt.Errorf("empty expression")
	}
	depth := 0
	inSingle, inDouble := false, false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inSingle:
			// Backslash escaping is dialect-dependent (off in PG with
			// standard_conforming_strings, on in MySQL/E-strings), so a string
			// containing one could terminate early on some targets and break
			// out of the literal — reject rather than guess.
			if c == '\\' {
				return fmt.Errorf("backslash escape in string literal (dialect-dependent)")
			}
			if c == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' { // escaped ''
					i++
					continue
				}
				inSingle = false
			}
		case inDouble:
			if c == '"' {
				inDouble = false
			}
		case c == '\'':
			inSingle = true
		case c == '"':
			inDouble = true
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth < 0 {
				return fmt.Errorf("unbalanced parentheses")
			}
		case c == ';':
			return fmt.Errorf("contains a statement separator %q", ";")
		case c == ',' && depth == 0:
			return fmt.Errorf("contains a top-level comma (would inject a column)")
		case c == '-' && i+1 < len(s) && s[i+1] == '-':
			// A line comment would comment out the rest of the DDL line (the
			// separating comma or closing paren).
			return fmt.Errorf("contains a line comment %q", "--")
		case c == '/' && i+1 < len(s) && s[i+1] == '*':
			return fmt.Errorf("contains a block comment %q", "/*")
		}
	}
	if depth != 0 {
		return fmt.Errorf("unbalanced parentheses")
	}
	if inSingle || inDouble {
		return fmt.Errorf("unterminated quoted string")
	}
	return nil
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
