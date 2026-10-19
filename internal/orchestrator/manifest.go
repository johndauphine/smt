package orchestrator

// Run manifest (#64): a small JSON artifact written next to schema.sql that
// records exactly which deterministic inputs produced the plan — SMT version,
// renderer version, dialects, type policy, and content fingerprints of both
// the source schema and the rendered SQL. Two purposes:
//
//   - inspection: a user can see which renderer logic and source schema a
//     given schema.sql came from, and whether either changed between runs;
//   - invalidation: a downstream artifact cache can key on RendererVersion +
//     SourceFingerprint so a renderer bump or source change misses stale
//     entries instead of replaying them.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/johndauphine/smt/internal/ddl"
	"github.com/johndauphine/smt/internal/driver"
	"github.com/johndauphine/smt/internal/fidelity"
	"github.com/johndauphine/smt/internal/version"
)

// manifestArtifactName is the filename of the run manifest. It lives in the
// same ddl/ directory as schema.sql so the two travel together.
const manifestArtifactName = "manifest.json"

type runManifest struct {
	SMTVersion               string                 `json:"smt_version"`
	RendererVersion          string                 `json:"renderer_version"`
	SourceDialect            string                 `json:"source_dialect"`
	TargetDialect            string                 `json:"target_dialect"`
	TargetSchema             string                 `json:"target_schema"`
	UnknownTypePolicy        string                 `json:"unknown_type_policy"`
	AIReviewEnabled          bool                   `json:"ai_review_enabled"`
	AIReviewMode             string                 `json:"ai_review_mode,omitempty"`
	AIReviewWarnings         []aiReviewWarning      `json:"ai_review_warnings,omitempty"`
	MappingWarnings          []mappingWarning       `json:"mapping_warnings,omitempty"`
	TableCount               int                    `json:"table_count"`
	SourceFingerprint        string                 `json:"source_schema_fingerprint"`
	PlanFingerprint          string                 `json:"plan_fingerprint"`
	FidelityProfile          fidelity.Profile       `json:"fidelity_profile,omitempty"`
	FidelitySemanticsVersion string                 `json:"fidelity_semantics_version,omitempty"`
	FidelityFindings         []fidelity.Finding     `json:"fidelity_findings,omitempty"`
	FidelityRequirements     []fidelity.Requirement `json:"fidelity_requirements,omitempty"`
}

// writeRunManifest fingerprints the source schema and the rendered SQL and
// writes the manifest into the ddl/ run directory beside schema.sql.
func (o *Orchestrator) writeRunManifest(ctx context.Context, runID string, r createDDLRenderer, planSQL string) error {
	snap, err := o.canonicalSourceSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("building source fingerprint snapshot: %w", err)
	}
	srcFP, err := fingerprintJSON(snap)
	if err != nil {
		return err
	}
	m := runManifest{
		SMTVersion:        version.Version,
		RendererVersion:   ddl.RendererVersion,
		SourceDialect:     r.sourceType,
		TargetDialect:     r.targetType,
		TargetSchema:      r.targetSchema,
		UnknownTypePolicy: r.unknownTypePolicy,
		AIReviewEnabled:   r.aiReviewEnabled,
		AIReviewMode:      r.aiReviewMode,
		AIReviewWarnings:  snapshotReviewWarnings(r.reviewWarnings),
		MappingWarnings:   collectMappingWarnings(o.tables, r.sourceType, r.targetType),
		TableCount:        len(o.tables),
		SourceFingerprint: srcFP,
		PlanFingerprint:   fingerprintBytes([]byte(planSQL)),
	}
	if r.fidelityProfile != "" && r.fidelityProfile != fidelity.Legacy {
		m.FidelityProfile = r.fidelityProfile
		m.FidelitySemanticsVersion = fidelity.SemanticsVersion
		m.SourceFingerprint = fidelityFingerprint(srcFP, r.fidelityProfile)
		for i := range o.tables {
			a, err := r.ddlRenderer.AssessTableFidelity(&o.tables[i], r.fidelityProfile)
			if err != nil {
				return err
			}
			m.FidelityFindings = append(m.FidelityFindings, a.Findings...)
			m.FidelityRequirements = append(m.FidelityRequirements, a.Requirements...)
		}
		// Include policy even when two profiles happen to emit identical SQL.
		m.PlanFingerprint = fidelityFingerprint(planSQL, r.fidelityProfile)
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	dir := o.ddlArtifactDir(runID)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, manifestArtifactName), append(data, '\n'), 0600)
}

func snapshotReviewWarnings(r *reviewWarningRecorder) []aiReviewWarning {
	if r == nil {
		return nil
	}
	return r.Snapshot()
}

// canonicalSourceSnapshot returns a copy of the in-scope source tables loaded
// with exactly the constraint metadata that drives the rendered plan — the
// per-table render helpers load indexes/FKs/checks into goroutine-local
// copies, so o.tables alone carries only columns/PKs. Non-DDL statistics
// (row counts, sample values) are zeroed so the fingerprint tracks schema
// shape, not table contents. Only the enabled create_* categories are loaded,
// keeping the fingerprint aligned with what the plan actually contains.
func (o *Orchestrator) canonicalSourceSnapshot(ctx context.Context) ([]driver.Table, error) {
	out := make([]driver.Table, len(o.tables))
	for i := range o.tables {
		t := o.tables[i] // value copy; Load* mutate the copy, not o.tables
		if o.config.Migration.CreateIndexes {
			t.Indexes = nil
			if err := o.source.LoadIndexes(ctx, &t); err != nil {
				return nil, fmt.Errorf("loading indexes for %s: %w", t.Name, err)
			}
		}
		if o.config.Migration.CreateForeignKeys {
			t.ForeignKeys = nil
			if err := o.source.LoadForeignKeys(ctx, &t); err != nil {
				return nil, fmt.Errorf("loading FKs for %s: %w", t.Name, err)
			}
		}
		if o.config.Migration.CreateCheckConstraints {
			t.CheckConstraints = nil
			if err := o.source.LoadCheckConstraints(ctx, &t); err != nil {
				return nil, fmt.Errorf("loading checks for %s: %w", t.Name, err)
			}
		}
		canonicalizeForFingerprint(&t)
		out[i] = t
	}
	return out, nil
}

// canonicalizeForFingerprint zeroes the non-DDL fields so the fingerprint
// reflects schema shape rather than table data or transient stats.
func canonicalizeForFingerprint(t *driver.Table) {
	t.RowCount = 0
	t.EstimatedRowSize = 0
	t.Columns = slices.Clone(t.Columns)
	for j := range t.Columns {
		t.Columns[j].SampleValues = nil
	}
	// Validation evidence is inspected by the opt-in target comparison, not
	// part of the legacy source schema's renderer input contract.
	t.CheckConstraints = slices.Clone(t.CheckConstraints)
	for j := range t.CheckConstraints {
		t.CheckConstraints[j].Validated = nil
	}
}

// fingerprintJSON returns "sha256:<hex>" over the stable JSON encoding of v.
func fingerprintJSON(v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return fingerprintBytes(data), nil
}

// fidelityFingerprint separates opt-in policies in both input cache keys and
// executable-plan identities. Legacy callers retain their existing hash path.
func fidelityFingerprint(input string, profile fidelity.Profile) string {
	return fingerprintBytes([]byte(string(profile) + "\x00" + fidelity.SemanticsVersion + "\x00" + input))
}

func fingerprintBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}
