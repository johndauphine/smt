package driver

import (
	"context"
	"fmt"
	"strings"

	"smt/internal/logging"
)

// VerifyTableDDL audits a generated CREATE TABLE statement against the
// source introspection facts. The auditor checks six per-column criteria:
// max_length / precision / scale, nullability, identity, timezone-awareness,
// default-class, and data_type semantic equivalence.
//
// Phase 1 uses the same model for generation and verification — same
// dispatch path, same provider settings. Cross-model verify is Phase 2.
//
// On a malformed AI response (neither parseable as OK nor as ISSUES) the
// parser fails-closed: returns OK=false with the raw response as the issue.
// The writer then retries generation, which is safer than letting possibly-
// bad DDL slip through on an unparseable verdict.
func (m *AITypeMapper) VerifyTableDDL(ctx context.Context, req VerifyTableDDLRequest) (*VerifyResult, error) {
	if req.SourceTable == nil {
		return nil, fmt.Errorf("SourceTable is required")
	}
	if req.ProposedDDL == "" {
		return nil, fmt.Errorf("ProposedDDL is required")
	}

	prompt := m.buildVerifyTableDDLPrompt(req)

	logging.Debug("AI verify table DDL: %s.%s (%s -> %s)",
		req.SourceTable.Schema, req.SourceTable.Name, req.SourceDBType, req.TargetDBType)

	result, err := m.CallAI(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("AI verify call failed for %s.%s: %w",
			req.SourceTable.Schema, req.SourceTable.Name, err)
	}

	verdict := parseVerifyResponse(result)
	if verdict.OK {
		logging.Debug("AI verify OK: %s.%s", req.SourceTable.Schema, req.SourceTable.Name)
	} else {
		logging.Debug("AI verify flagged %d issue(s) on %s.%s",
			len(verdict.Issues), req.SourceTable.Schema, req.SourceTable.Name)
	}
	return verdict, nil
}

// VerifyFinalizationDDL audits a generated CREATE INDEX / FOREIGN KEY /
// CHECK CONSTRAINT statement against the source metadata. Mirrors
// VerifyTableDDL.
func (m *AITypeMapper) VerifyFinalizationDDL(ctx context.Context, req VerifyFinalizationDDLRequest) (*VerifyResult, error) {
	if req.Table == nil {
		return nil, fmt.Errorf("Table is required")
	}
	if req.ProposedDDL == "" {
		return nil, fmt.Errorf("ProposedDDL is required")
	}

	var prompt string
	switch req.Type {
	case DDLTypeIndex:
		if req.Index == nil {
			return nil, fmt.Errorf("Index is required for DDLTypeIndex verify")
		}
		prompt = m.buildVerifyIndexDDLPrompt(req)
	case DDLTypeForeignKey:
		if req.ForeignKey == nil {
			return nil, fmt.Errorf("ForeignKey is required for DDLTypeForeignKey verify")
		}
		prompt = m.buildVerifyForeignKeyDDLPrompt(req)
	case DDLTypeCheckConstraint:
		if req.CheckConstraint == nil {
			return nil, fmt.Errorf("CheckConstraint is required for DDLTypeCheckConstraint verify")
		}
		prompt = m.buildVerifyCheckConstraintDDLPrompt(req)
	default:
		return nil, fmt.Errorf("unsupported DDL type for verify: %s", req.Type)
	}

	result, err := m.CallAI(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("AI verify call failed for %s on %s: %w",
			req.Type, req.Table.Name, err)
	}
	return parseVerifyResponse(result), nil
}

// parseVerifyResponse interprets the auditor's reply into a VerifyResult.
//
// Output contract (set by the verify prompt):
//   - "OK" alone (any case) -> verdict.OK = true
//   - "ISSUES" header followed by one issue per line -> verdict.OK = false
//
// Defensive against:
//   - Markdown fences wrapping the response (PR #38 stripper)
//   - Trailing prose after OK ("OK\n\nNotes: ..." passes)
//   - Missing ISSUES header but lines look like issues -> treat as ISSUES
//   - Wholly malformed responses -> fail-closed with raw text as the issue
//
// Implementation note: scan lines and use strings.EqualFold for the ISSUES
// header detection. We previously substring-searched on strings.ToUpper(text)
// and then sliced the original text by the matched index, but ToUpper can
// change byte length on some Unicode inputs (e.g. ß -> SS), so the index
// could mis-align with the original byte-string. Line-based scanning avoids
// that class of bug entirely.
func parseVerifyResponse(response string) *VerifyResult {
	trimmed := strings.TrimSpace(stripMarkdownFence(response))
	if trimmed == "" {
		return &VerifyResult{OK: false, Issues: []string{"verifier returned empty response"}}
	}

	lines := strings.Split(trimmed, "\n")

	// First-line OK detection. Match "OK" as a full word at the start so
	// "OK" / "OK." / "OK — looks fine" all pass, but "OKAY" doesn't.
	firstLine := strings.TrimSpace(lines[0])
	if isOKLine(firstLine) {
		return &VerifyResult{OK: true}
	}

	// Look for an ISSUES header line (case-insensitive, the line must START
	// with the word ISSUES — no substring matches inside other content).
	headerIdx := -1
	for i, line := range lines {
		t := strings.TrimSpace(line)
		// Allow "ISSUES", "ISSUES:", or the literal followed by content on
		// the same line — but the line must start with the keyword.
		if t == "" {
			continue
		}
		// Find the first whitespace/punctuation; check the leading token.
		token := t
		for j := 0; j < len(token); j++ {
			c := token[j]
			if c == ' ' || c == '\t' || c == ':' || c == ',' || c == '.' {
				token = token[:j]
				break
			}
		}
		if strings.EqualFold(token, "ISSUES") {
			headerIdx = i
			break
		}
	}

	if headerIdx >= 0 {
		issues := []string{}
		for _, line := range lines[headerIdx+1:] {
			line = strings.TrimSpace(line)
			line = strings.TrimPrefix(line, "- ")
			line = strings.TrimPrefix(line, "* ")
			if line == "" {
				continue
			}
			issues = append(issues, line)
		}
		if len(issues) == 0 {
			issues = []string{"verifier reported ISSUES with no enumerated detail"}
		}
		return &VerifyResult{OK: false, Issues: issues}
	}

	// Neither OK nor ISSUES — fail closed with the raw response as context.
	// Truncate to keep the next-attempt PreviousAttempt.Error short.
	return &VerifyResult{
		OK:     false,
		Issues: []string{fmt.Sprintf("verifier response did not match OK/ISSUES contract: %s", truncateString(trimmed, 300))},
	}
}

// isOKLine reports whether a line is the literal OK verdict (any case)
// optionally followed by punctuation or prose. "OKAY" is rejected — only
// the two-character word.
func isOKLine(line string) bool {
	if !strings.HasPrefix(strings.ToUpper(line), "OK") {
		return false
	}
	// Verify "OK" is followed by end-of-line, whitespace, or punctuation —
	// not by a letter (which would make it OKAY, OKEYDOKEY, etc.).
	if len(line) == 2 {
		return true
	}
	c := line[2]
	switch c {
	case ' ', '\t', '.', ',', ':', ';', '!', '?', '-':
		return true
	}
	// Em-dash / en-dash are multi-byte; accept any non-ASCII byte after OK
	// as a separator (covers — and –).
	if c >= 0x80 {
		return true
	}
	return false
}

// buildVerifyTableDDLPrompt asks the AI to compare a proposed CREATE TABLE
// against the source introspection block. The criteria list is exhaustive
// for column-level metadata and uses imperative phrasing so the model
// returns a structured verdict rather than discursive prose.
func (m *AITypeMapper) buildVerifyTableDDLPrompt(req VerifyTableDDLRequest) string {
	var sb strings.Builder

	sb.WriteString("You are a database migration auditor. Compare source column metadata against the proposed target DDL and report any attribute that fails to preserve source semantics.\n\n")

	sb.WriteString("=== SOURCE METADATA ===\n")
	sb.WriteString(buildSourceIntrospectionBlock(req.SourceTable, req.SourceDBType))
	sb.WriteString("\n")

	sb.WriteString("=== TARGET ===\n")
	sb.WriteString(fmt.Sprintf("target_dialect: %s\n", req.TargetDBType))
	if req.TargetSchema != "" {
		sb.WriteString(fmt.Sprintf("target_schema: %s\n", req.TargetSchema))
	}
	sb.WriteString("\n")

	// REQUIRED TARGET COLUMN NAMES — the generator was instructed to use these
	// exact names in the DDL it produced (e.g. PG lowercases identifiers via
	// driver.NormalizeIdentifier). Without showing the mapping, the auditor
	// would see source `Customer ID` and target `customer_id` and could
	// falsely flag a missing/changed column. Hand the auditor the same
	// authoritative source→target name table the generator received.
	sb.WriteString("=== REQUIRED TARGET COLUMN NAMES ===\n")
	sb.WriteString("The generator was instructed to use these exact target column names. A casing/quoting/identifier-normalization difference between source and target name is NOT a failure — only flag when a column is missing or its attributes diverge.\n")
	for _, col := range req.SourceTable.Columns {
		tgt := targetIdentifier(col.Name, req.TargetDBType)
		sb.WriteString(fmt.Sprintf("  %s -> %s\n", col.Name, tgt))
	}
	sb.WriteString("\n")

	sb.WriteString("=== PROPOSED TARGET DDL ===\n")
	sb.WriteString(req.ProposedDDL)
	sb.WriteString("\n\n")

	writeVerifyAuditCriteria(&sb)
	writeVerifyOutputFormat(&sb)

	return sb.String()
}

// buildVerifyIndexDDLPrompt audits a CREATE INDEX statement.
func (m *AITypeMapper) buildVerifyIndexDDLPrompt(req VerifyFinalizationDDLRequest) string {
	var sb strings.Builder
	sb.WriteString("You are a database migration auditor. Compare the source index metadata against the proposed CREATE INDEX statement.\n\n")

	sb.WriteString("=== SOURCE INDEX ===\n")
	sb.WriteString(fmt.Sprintf("table: %s\n", req.Table.Name))
	sb.WriteString(fmt.Sprintf("name: %s\n", req.Index.Name))
	sb.WriteString(fmt.Sprintf("columns: %s\n", strings.Join(req.Index.Columns, ", ")))
	sb.WriteString(fmt.Sprintf("is_unique: %v\n", req.Index.IsUnique))
	if len(req.Index.IncludeCols) > 0 {
		sb.WriteString(fmt.Sprintf("include_cols: %s\n", strings.Join(req.Index.IncludeCols, ", ")))
	}
	if req.Index.Filter != "" {
		sb.WriteString(fmt.Sprintf("filter: %s\n", req.Index.Filter))
	}
	sb.WriteString("\n")

	sb.WriteString(fmt.Sprintf("target_dialect: %s\n", req.TargetDBType))
	if req.TargetSchema != "" {
		sb.WriteString(fmt.Sprintf("target_schema: %s\n", req.TargetSchema))
	}
	sb.WriteString("\n")

	sb.WriteString("=== PROPOSED TARGET DDL ===\n")
	sb.WriteString(req.ProposedDDL)
	sb.WriteString("\n\n")

	sb.WriteString("=== AUDIT CRITERIA ===\n")
	sb.WriteString("Verify that the proposed DDL preserves:\n")
	sb.WriteString("  1. The exact column list and ordering.\n")
	sb.WriteString("  2. UNIQUE-ness — is_unique=true must produce a UNIQUE index.\n")
	sb.WriteString("  3. INCLUDE columns (if listed) — covering-index columns must be carried through where the target dialect supports them (PostgreSQL 11+, SQL Server). Acceptable to drop on dialects that lack the feature (MySQL).\n")
	sb.WriteString("  4. Filter / WHERE clause (if listed) — must be carried through where supported (PostgreSQL, SQL Server filtered indexes).\n")
	sb.WriteString("  5. Index name — translated identifier should still uniquely identify the index; minor casing/quoting differences are acceptable.\n\n")

	writeVerifyOutputFormat(&sb)
	return sb.String()
}

// buildVerifyForeignKeyDDLPrompt audits an ADD FOREIGN KEY statement.
func (m *AITypeMapper) buildVerifyForeignKeyDDLPrompt(req VerifyFinalizationDDLRequest) string {
	var sb strings.Builder
	sb.WriteString("You are a database migration auditor. Compare the source foreign-key metadata against the proposed ALTER TABLE statement.\n\n")

	sb.WriteString("=== SOURCE FOREIGN KEY ===\n")
	sb.WriteString(fmt.Sprintf("table: %s\n", req.Table.Name))
	sb.WriteString(fmt.Sprintf("name: %s\n", req.ForeignKey.Name))
	sb.WriteString(fmt.Sprintf("columns: %s\n", strings.Join(req.ForeignKey.Columns, ", ")))
	sb.WriteString(fmt.Sprintf("ref_schema: %s\n", req.ForeignKey.RefSchema))
	sb.WriteString(fmt.Sprintf("ref_table: %s\n", req.ForeignKey.RefTable))
	sb.WriteString(fmt.Sprintf("ref_columns: %s\n", strings.Join(req.ForeignKey.RefColumns, ", ")))
	sb.WriteString(fmt.Sprintf("on_delete: %s\n", req.ForeignKey.OnDelete))
	sb.WriteString(fmt.Sprintf("on_update: %s\n", req.ForeignKey.OnUpdate))
	sb.WriteString("\n")

	sb.WriteString(fmt.Sprintf("target_dialect: %s\n", req.TargetDBType))
	if req.TargetSchema != "" {
		sb.WriteString(fmt.Sprintf("target_schema: %s\n", req.TargetSchema))
	}
	sb.WriteString("\n")

	sb.WriteString("=== PROPOSED TARGET DDL ===\n")
	sb.WriteString(req.ProposedDDL)
	sb.WriteString("\n\n")

	sb.WriteString("=== AUDIT CRITERIA ===\n")
	sb.WriteString("Verify that the proposed DDL preserves:\n")
	sb.WriteString("  1. The exact local column list (order matters for composite FKs).\n")
	sb.WriteString("  2. The exact referenced column list (order must match local columns position-by-position).\n")
	sb.WriteString("  3. The referenced table name. The referenced schema should resolve to the target schema, not the source schema.\n")
	sb.WriteString("  4. ON DELETE action (CASCADE / SET NULL / SET DEFAULT / RESTRICT / NO ACTION). Treat NO ACTION and RESTRICT as equivalent (they are semantically equivalent in most engines).\n")
	sb.WriteString("  5. ON UPDATE action — same rule as ON DELETE.\n\n")

	writeVerifyOutputFormat(&sb)
	return sb.String()
}

// buildVerifyCheckConstraintDDLPrompt audits an ADD CHECK statement.
func (m *AITypeMapper) buildVerifyCheckConstraintDDLPrompt(req VerifyFinalizationDDLRequest) string {
	var sb strings.Builder
	sb.WriteString("You are a database migration auditor. Compare the source CHECK-constraint metadata against the proposed ALTER TABLE statement.\n\n")

	sb.WriteString("=== SOURCE CHECK ===\n")
	sb.WriteString(fmt.Sprintf("table: %s\n", req.Table.Name))
	sb.WriteString(fmt.Sprintf("name: %s\n", req.CheckConstraint.Name))
	sb.WriteString(fmt.Sprintf("definition: %s\n", req.CheckConstraint.Definition))
	sb.WriteString("\n")

	sb.WriteString(fmt.Sprintf("target_dialect: %s\n", req.TargetDBType))
	if req.TargetSchema != "" {
		sb.WriteString(fmt.Sprintf("target_schema: %s\n", req.TargetSchema))
	}
	sb.WriteString("\n")

	sb.WriteString("=== PROPOSED TARGET DDL ===\n")
	sb.WriteString(req.ProposedDDL)
	sb.WriteString("\n\n")

	sb.WriteString("=== AUDIT CRITERIA ===\n")
	sb.WriteString("Verify that the proposed DDL preserves the CHECK predicate:\n")
	sb.WriteString("  1. The semantics of the source predicate. Cross-dialect function translations are acceptable when they are semantically equivalent — for example: ISNULL(a,b) (MSSQL) ≡ COALESCE(a,b); UPPER(x) and LOWER(x) work identically; LIKE patterns are portable; date arithmetic mappings (DATEADD vs INTERVAL) are acceptable when correct.\n")
	sb.WriteString("  2. The constraint name (minor casing/quoting differences are acceptable).\n")
	sb.WriteString("  3. The same column references — every column that appears in the source predicate must appear in the target predicate (modulo casing).\n\n")

	writeVerifyOutputFormat(&sb)
	return sb.String()
}

// writeVerifyAuditCriteria appends the six column-level audit criteria
// shared by the table-DDL verify prompt. Same criteria the verify_columns.sh
// harness applies — keeps the in-loop and end-to-end checks aligned.
//
// Critical phrasing notes (informed by an early run that produced false
// positives on Sonnet → Sonnet):
//   - The "acceptable" lists are not optional — list them explicitly so the
//     model doesn't flag known-equivalent type-name pairs as failures.
//   - The "what is NOT a failure" section is a hard rule, not a hint.
//   - The output contract has examples — without them, models tend to write
//     a checklist with PASS/FAIL annotations per criterion instead of the
//     terse OK / ISSUES form the parser expects.
func writeVerifyAuditCriteria(sb *strings.Builder) {
	sb.WriteString("=== AUDIT CRITERIA ===\n")
	sb.WriteString("For each source column, compare its target column. A FAILURE exists ONLY if one of these strict checks fails:\n")
	sb.WriteString("  1. max_length / precision / scale not preserved exactly. Source max_length=20 must produce target VARCHAR(20). Source precision=18, scale=4 must produce target NUMERIC(18,4). No bucket-rounding, halving, doubling, substituting, or relying on the target default.\n")
	sb.WriteString("  2. nullability differs. Source nullable=false → target NOT NULL. Source nullable=true → target allows NULL.\n")
	sb.WriteString("  3. identity / auto-increment dropped. Source identity=true → target identity equivalent (PG GENERATED IDENTITY / sequence default, MSSQL IDENTITY, MySQL AUTO_INCREMENT).\n")
	sb.WriteString("  4. timezone-awareness CLASS changed. TZ-naive source MUST map to TZ-naive target; TZ-aware source MUST map to TZ-aware target.\n")
	sb.WriteString("  5. default expression dropped (target has no default, source did) or replaced with something semantically different.\n")
	sb.WriteString("  6. data_type is not a semantic equivalent in the target dialect.\n\n")

	sb.WriteString("=== ACCEPTABLE — DO NOT FLAG THESE ===\n")
	sb.WriteString("Type-name equivalences (cross-dialect):\n")
	sb.WriteString("  - PG `varchar(N)` / `character varying(N)` / MSSQL `VARCHAR(N)` / MSSQL `NVARCHAR(N)` / MySQL `VARCHAR(N)` — all preserve a max_length of N CHARACTERS.\n")
	sb.WriteString("  - PG `integer` / `int4` / MSSQL `INT` / MySQL `INT` — all 32-bit signed integer.\n")
	sb.WriteString("  - PG `bigint` / `int8` / MSSQL `BIGINT` / MySQL `BIGINT`.\n")
	sb.WriteString("  - PG `boolean` / MSSQL `BIT` / MySQL `TINYINT(1)`.\n")
	sb.WriteString("  - PG `text` for unbounded source `nvarchar(MAX)` / `varchar(MAX)` / MySQL `TEXT`.\n")
	sb.WriteString("  - PG `uuid` / MSSQL `UNIQUEIDENTIFIER` / MySQL `CHAR(36)`.\n")
	sb.WriteString("  - PG `timestamp` ≡ `timestamp without time zone`; PG `timestamptz` ≡ `timestamp with time zone`.\n")
	sb.WriteString("  - PG `numeric(P,S)` / MSSQL `NUMERIC(P,S)` / `DECIMAL(P,S)` / MySQL `DECIMAL(P,S)`.\n")
	sb.WriteString("Default-expression equivalences:\n")
	sb.WriteString("  - GETUTCDATE() ≡ GETDATE() ≡ SYSDATETIME() ≡ SYSDATETIMEOFFSET() ≡ CURRENT_TIMESTAMP ≡ NOW(). All are 'current time' defaults; pick whichever is dialect-idiomatic on the target. Do NOT flag GETUTCDATE → CURRENT_TIMESTAMP as a failure.\n")
	sb.WriteString("  - NEWID() ≡ gen_random_uuid() ≡ UUID() — all generate a fresh UUID.\n")
	sb.WriteString("  - ISNULL(a,b) ≡ COALESCE(a,b) — semantically identical for two arguments.\n")
	sb.WriteString("  - MSSQL outer-paren default stripping: ((0)) ≡ 0; ((1)) ≡ 1; (('pending')) ≡ 'pending'.\n")
	sb.WriteString("  - PG `bit(0)`/`bit(1)` literals from MSSQL `((0))`/`((1))` mapped to PG `false`/`true`.\n")
	sb.WriteString("Casing / quoting differences are NEVER a failure (e.g. `name` vs `\"name\"`, `INT` vs `int`, `NULL` vs `null`).\n\n")
}

// writeVerifyOutputFormat appends the OK / ISSUES output contract. Shared by
// every verify prompt so the parser sees the same shape regardless of which
// DDL type was audited.
//
// Few-shot examples are critical here — without them, models tend to enumerate
// each criterion with PASS/FAIL annotations rather than emit the terse output
// the parser expects. Two positive and two negative examples match the
// patterns we hit in real Sonnet runs.
func writeVerifyOutputFormat(sb *strings.Builder) {
	sb.WriteString("=== OUTPUT FORMAT ===\n")
	sb.WriteString("Your response goes to an automated retry loop. Output rules are STRICT:\n")
	sb.WriteString("- If you find ZERO failures, respond with EXACTLY the literal text: OK\n")
	sb.WriteString("  Nothing else. No prose, no checklist, no bullets, no PASS annotations.\n")
	sb.WriteString("- If you find one or more failures, respond with:\n")
	sb.WriteString("    ISSUES\n")
	sb.WriteString("    <column>: <attribute> — <source value> vs <target value>\n")
	sb.WriteString("    <column>: <attribute> — <source value> vs <target value>\n")
	sb.WriteString("  One failure per line. List ONLY failures — do not include passing criteria.\n")
	sb.WriteString("- Do NOT wrap in markdown, code fences, JSON, or explanatory prose.\n")
	sb.WriteString("- Do NOT discuss what you are doing, what passes, or alternatives. Only enumerate failures.\n\n")

	sb.WriteString("=== EXAMPLES ===\n")
	sb.WriteString("Each example shows the inputs, the EXACT response you must emit, and (after the response) a one-line rationale that is NOT part of the response. Your actual output is only the lines under \"Your response:\" — never the rationale.\n\n")

	sb.WriteString("Example A — all attributes preserved:\n")
	sb.WriteString("  Source: code varchar(20) NOT NULL\n")
	sb.WriteString("  Target: code VARCHAR(20) NOT NULL\n")
	sb.WriteString("  Your response:\n")
	sb.WriteString("    OK\n")
	sb.WriteString("  Rationale (not part of your output): every attribute matches.\n\n")

	sb.WriteString("Example B — equivalent dialect translation:\n")
	sb.WriteString("  Source: created_at datetime2 NOT NULL DEFAULT (getutcdate())\n")
	sb.WriteString("  Target: created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP\n")
	sb.WriteString("  Your response:\n")
	sb.WriteString("    OK\n")
	sb.WriteString("  Rationale (not part of your output): datetime2 is TZ-naive, PG TIMESTAMP is TZ-naive — match. GETUTCDATE ≡ CURRENT_TIMESTAMP per the ACCEPTABLE list.\n\n")

	sb.WriteString("Example C — halved varchar (real failure):\n")
	sb.WriteString("  Source: code varchar(20) NOT NULL\n")
	sb.WriteString("  Target: code VARCHAR(10) NOT NULL\n")
	sb.WriteString("  Your response:\n")
	sb.WriteString("    ISSUES\n")
	sb.WriteString("    code: max_length — 20 vs 10\n")
	sb.WriteString("  Rationale (not part of your output): max_length=20 must produce VARCHAR(20).\n\n")

	sb.WriteString("Example D — TZ semantics added (real failure):\n")
	sb.WriteString("  Source: created_at datetime2 NOT NULL\n")
	sb.WriteString("  Target: created_at TIMESTAMP WITH TIME ZONE NOT NULL\n")
	sb.WriteString("  Your response:\n")
	sb.WriteString("    ISSUES\n")
	sb.WriteString("    created_at: tz-awareness — TZ-naive (datetime2) vs TZ-aware (timestamp with time zone)\n")
	sb.WriteString("  Rationale (not part of your output): TZ class flipped naive_dt → tzaware_dt.\n\n")
}
