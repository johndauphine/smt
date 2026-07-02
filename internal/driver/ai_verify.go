package driver

import (
	"context"
	"fmt"

	"smt/internal/logging"
)

// VerifyTableDDL audits a generated CREATE TABLE statement against the
// source introspection facts. The flow has two stages:
//
//  1. AI parse — the proposed target DDL is sent to the AI with a parser
//     prompt that returns Column[] JSON. This is the AI's only job; it does
//     not judge or compare.
//  2. Deterministic compare — Go-side CompareColumns runs the six
//     per-column criteria (max_length, precision/scale, nullability,
//     identity, TZ class, default class) against the source. Any deltas
//     become Issues in the returned VerifyResult.
//
// The split eliminates the prose-drift / lexical-vs-class failure modes the
// free-text auditor (#47, #51, #53) hit at multi-table cross-dialect scale —
// see #55. The AI's parse step is what LLMs are best at; the comparison is
// what they were worst at, so it moves to deterministic Go.
//
// Failure handling:
//   - AI parse error (network / API / etc.) → returned as error; writer
//     surfaces and retries generation.
//   - Bad JSON / zero columns from the AI → returned as a verify-fail
//     verdict with the parse error in Issues. The writer's retry loop will
//     re-prompt with the parse failure as PreviousAttempt.Error, the same
//     way it would for an exec-fail or auditor-fail in the legacy path.
//   - Comparison deltas → verdict.OK=false with one Issue per delta in the
//     same string form #53's prompt produced (`column: criterion — src vs
//     tgt`), so the writer's PreviousAttempt feedback path is unchanged.
//
// Cross-model verify (#48) still works — the parser model is whatever's
// configured. Since the comparison is deterministic, the cheap model is
// fine for parse; cross-model becomes an opt-in optimization rather than a
// recommended safety net.
func (m *AITypeMapper) VerifyTableDDL(ctx context.Context, req VerifyTableDDLRequest) (*VerifyResult, error) {
	if req.SourceTable == nil {
		return nil, fmt.Errorf("SourceTable is required")
	}
	if req.ProposedDDL == "" {
		return nil, fmt.Errorf("ProposedDDL is required")
	}

	logging.Debug("AI verify table DDL (parse + compare): %s.%s (%s -> %s)",
		req.SourceTable.Schema, req.SourceTable.Name, req.SourceDBType, req.TargetDBType)

	parsedCols, err := m.parseTargetDDLToColumns(ctx, req.ProposedDDL, req.TargetDBType)
	if err != nil {
		// Bad JSON / no columns / no JSON object — retryable signal. Surface
		// as a verdict the writer can feed back into the next prompt rather
		// than as a hard error (which would abort the table immediately).
		// The cause is in the message; the generator gets concrete corrective
		// context the same as it would for a comparison delta.
		logging.Debug("AI verify parse failed on %s.%s: %v",
			req.SourceTable.Schema, req.SourceTable.Name, err)
		return &VerifyResult{
			OK:     false,
			Issues: []string{fmt.Sprintf("verifier could not parse target DDL: %v", err)},
		}, nil
	}

	deltas := CompareColumns(req.SourceTable.Columns, parsedCols, req.SourceDBType, req.TargetDBType)
	if len(deltas) == 0 {
		logging.Debug("AI verify OK: %s.%s (deterministic compare)",
			req.SourceTable.Schema, req.SourceTable.Name)
		return &VerifyResult{OK: true}, nil
	}

	issues := make([]string, 0, len(deltas))
	for _, d := range deltas {
		issues = append(issues, d.String())
	}
	logging.Debug("AI verify flagged %d delta(s) on %s.%s (deterministic compare)",
		len(deltas), req.SourceTable.Schema, req.SourceTable.Name)
	return &VerifyResult{OK: false, Issues: issues}, nil
}

// VerifyFinalizationDDL audits a generated CREATE INDEX / FOREIGN KEY /
// CHECK CONSTRAINT statement against the source metadata. Unlike table review,
// no AI parse step is needed: SMT controls these finalization DDL shapes, so a
// narrow deterministic parser can extract the attributes and compare them in Go.
func (m *AITypeMapper) VerifyFinalizationDDL(ctx context.Context, req VerifyFinalizationDDLRequest) (*VerifyResult, error) {
	_ = ctx
	return VerifyFinalizationDDLDeterministic(req)
}
