# SMT v1 Persisted Artifact Compatibility

This document is the v1 compatibility policy for artifacts that outlive a
single SMT process: the SQLite state DB, schema snapshots, run manifests, and
deterministic renderer versions.

## State DB

The state DB lives under `migration.data_dir` as `migrate.db` and contains the
v1-active tables `runs`, `profiles`, and `schema_snapshots`.

SMT 1.x must open state DBs written by SMT 1.0 and later 1.x releases. On open,
`migrate()` creates missing active tables and adds missing active columns with
idempotent `ALTER TABLE` steps. Newer 1.x binaries may add nullable columns,
new tables, or new indexes, but existing documented columns should remain
readable.

Opening a DB written by a newer SMT minor or patch with an older binary is not
part of the v1 guarantee. Operators should upgrade the binary rather than
expect old releases to understand new state.

SMT 1.0 also opens pre-1.0 state DBs that still contain removed DMT-era tables
such as `tasks`, `task_outputs`, `transfer_progress`, or
`table_sync_timestamps`. Those orphan tables are left in place as harmless
legacy data; fresh DBs do not create them.

## Snapshots

`internal/schemadiff.CurrentSnapshotVersion` is the JSON schema version stamped
into newly written snapshots. SMT 1.0 writes snapshot version `4`.

SMT 1.x must read every snapshot written by SMT 1.0 and later 1.x releases. A
pinned version-4 fixture is kept in the test suite so future changes prove they
still deserialize the v1 snapshot format.

SMT 1.0 reads known 0.x snapshot versions through the existing backfill logic:
unversioned/0-1 snapshots, version 2 snapshots, and version 3 snapshots are
normalized before diffing so fields added later do not create false drift. If a
0.x snapshot is corrupt or outside the known formats, the supported migration
step is to run `smt snapshot` again from the live source schema and use that as
the new baseline.

Snapshots written by a future major version are not guaranteed to be readable by
v1 binaries.

## Run Manifests

Run manifests are written beside generated DDL as `manifest.json`. The following
fields are part of the v1 reader contract for downstream tooling:

- `smt_version`
- `renderer_version`
- `source_dialect`
- `target_dialect`
- `target_schema`
- `unknown_type_policy`
- `ai_review_enabled`
- `ai_review_mode`
- `ai_review_warnings` with `label`, `method`, and `issues` entries
- `mapping_warnings`
- `table_count`
- `source_schema_fingerprint`
- `plan_fingerprint`

1.x releases may add new manifest fields. Downstream readers should ignore
unknown fields and treat missing optional arrays as empty.

For `ai_review_warnings`, `method` is part of the v1 review contract. Current
table DDL review and side-object review for indexes, foreign keys, and check
constraints record `deterministic_comparator`. Downstream readers should still
tolerate historical v1.0 manifests that recorded side-object findings as
`free_text_auditor`.

## RendererVersion

`internal/ddl.RendererVersion` identifies the deterministic renderer and type
mapping output contract. It is bumped when the same source schema and config can
produce different target DDL because SMT learned a new type mapping, expression
rewrite, identifier rule, default rendering rule, or similar behavior.

When `renderer_version` changes, generated artifacts should be treated as
stale. Regenerate `schema.sql` or `migration.sql`, inspect the new manifest, and
compare the old and new DDL before applying. A renderer bump does not by itself
require re-baselining the source snapshot; re-run `smt snapshot` when the live
source schema is the new intended baseline.
