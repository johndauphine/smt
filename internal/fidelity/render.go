package fidelity

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/johndauphine/smt/internal/driver"
)

// Check is an exact generated restriction, not ownership inferred from a name
// prefix. Expression is the predicate for driver.CheckConstraint.Definition;
// Definition is the complete inline CONSTRAINT clause.
type Check struct {
	Requirement Requirement `json:"requirement"`
	Name        string      `json:"name"`
	Expression  string      `json:"expression"`
	Definition  string      `json:"definition"`
}

// RenderChecks renders only typed, validated requirements. Callers must supply
// every source constraint name sharing the target namespace, including names
// owned by primary/foreign keys and indexes. Use the same source list for create
// and desired-state construction; adding live target names would rename checks
// on every introspection cycle. No SQL is accepted from findings or source text.
func RenderChecks(assessment Assessment, reservedNames []string) ([]Check, error) {
	result := Assessment{Requirements: append([]Requirement(nil), assessment.Requirements...)}
	sortAssessment(&result)
	reserved := make(map[string]bool, len(reservedNames)+len(result.Requirements))
	for _, name := range reservedNames {
		reserved[driver.NormalizeIdentifier("postgres", name)] = true
	}
	checks := make([]Check, 0, len(result.Requirements))
	seenRequirements := make(map[string]bool, len(result.Requirements))
	for _, req := range result.Requirements {
		expression, err := renderExpression(req)
		if err != nil {
			return nil, err
		}
		// Create sees original names; desired-state construction may already
		// have normalized and retargeted them. Table-local names must agree
		// across both paths and remain stable when a byte bound changes.
		table := driver.NormalizeIdentifier("postgres", req.Object.Table)
		column := driver.NormalizeIdentifier("postgres", req.Object.Column)
		identity := table + "\x00" + column + "\x00" + req.Code
		if seenRequirements[identity] {
			return nil, fmt.Errorf("duplicate fidelity requirement %q for %s.%s", req.Code, table, column)
		}
		seenRequirements[identity] = true
		hash := sha256.Sum256([]byte(identity))
		base := fmt.Sprintf("smt_fidelity_%s_%x", column, hash[:8])
		name := driver.NormalizeIdentifier("postgres", base)
		for suffix := 1; reserved[name]; suffix++ {
			name = driver.NormalizeIdentifier("postgres", fmt.Sprintf("%s_%d", base, suffix))
		}
		reserved[name] = true
		checks = append(checks, Check{Requirement: req, Name: name, Expression: expression,
			Definition: "CONSTRAINT " + quote(name) + " CHECK (" + expression + ")"})
	}
	return checks, nil
}

func renderExpression(req Requirement) (string, error) {
	if dialect(req.SourceDialect) != "mssql" || dialect(req.TargetDialect) != "postgres" {
		return "", &UnsupportedError{SourceDialect: req.SourceDialect, TargetDialect: req.TargetDialect, Reason: "requirement renderer supports SQL Server to PostgreSQL only"}
	}
	if req.Object.Column == "" || strings.ContainsRune(req.Object.Column, '\x00') {
		return "", fmt.Errorf("fidelity requirement requires a nonempty column identifier without NUL")
	}
	column := quote(driver.NormalizeIdentifier("postgres", req.Object.Column))
	switch req.Code {
	case TinyIntRange:
		if req.Min != 0 || req.Max != 255 || req.MaxBytes != 0 {
			return "", fmt.Errorf("invalid SQL Server tinyint fidelity requirement")
		}
		// SQL CHECK accepts UNKNOWN, preserving nullable source columns.
		return fmt.Sprintf("%s >= 0 AND %s <= 255", column, column), nil
	case VarBinaryLength:
		if req.MaxBytes <= 0 || req.MaxBytes > 8000 || req.Min != 0 || req.Max != 0 {
			return "", fmt.Errorf("invalid SQL Server bounded varbinary fidelity requirement")
		}
		return fmt.Sprintf("octet_length(%s) <= %d", column, req.MaxBytes), nil
	default:
		return "", &UnsupportedError{SourceDialect: req.SourceDialect, TargetDialect: req.TargetDialect, Reason: fmt.Sprintf("unknown fidelity requirement %q", req.Code)}
	}
}

func quote(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }
