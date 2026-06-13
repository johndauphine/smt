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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"smt/internal/ddl"
	"smt/internal/version"
)

// manifestArtifactName is the filename of the run manifest within the run dir.
const manifestArtifactName = "manifest.json"

type runManifest struct {
	SMTVersion        string `json:"smt_version"`
	RendererVersion   string `json:"renderer_version"`
	SourceDialect     string `json:"source_dialect"`
	TargetDialect     string `json:"target_dialect"`
	TargetSchema      string `json:"target_schema"`
	UnknownTypePolicy string `json:"unknown_type_policy"`
	AIReviewEnabled   bool   `json:"ai_review_enabled"`
	AIReviewMode      string `json:"ai_review_mode,omitempty"`
	TableCount        int    `json:"table_count"`
	SourceFingerprint string `json:"source_schema_fingerprint"`
	PlanFingerprint   string `json:"plan_fingerprint"`
}

// writeRunManifest fingerprints the source schema and the rendered SQL and
// writes the manifest into the run directory.
func (o *Orchestrator) writeRunManifest(runID string, r createDDLRenderer, planSQL string) error {
	srcFP, err := fingerprintJSON(o.tables)
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
		TableCount:        len(o.tables),
		SourceFingerprint: srcFP,
		PlanFingerprint:   fingerprintBytes([]byte(planSQL)),
	}
	return o.writeJSONArtifact(runID, manifestArtifactName, m)
}

// fingerprintJSON returns "sha256:<hex>" over the stable JSON encoding of v.
func fingerprintJSON(v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return fingerprintBytes(data), nil
}

func fingerprintBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}
