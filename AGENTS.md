# Repository Agent Guide

## Project orientation

SMT is a schema migration tool. It introspects a source database, renders
target-dialect schema DDL, reports drift, and plans incremental schema changes.
It is schema-only: do not add DMT-style row transfer, chunking, worker pools, or
runtime transfer tuning.

The released v1 architecture is deterministic. PostgreSQL, SQL Server, and
MySQL are supported as CLI sources and targets. The public `schema` package also
exposes explicitly capability-gated DDL APIs for additional dialects.

Important areas:

- `cmd/smt/`: CLI entry point.
- `internal/driver/`: driver interfaces, registry, introspection, and execution.
- `schema/canonical/`: source type to canonical type mapping.
- `internal/expr/`: dialect-neutral default and check expression handling.
- `internal/ddl/`: deterministic create-plan rendering.
- `internal/schemadiff/`: snapshots, drift, structural diffs, and sync plans.
- `internal/orchestrator/`: plan construction, optional review, and execution.
- `schema/`: stable public deterministic DDL API.
- `testdata/crm/`: cross-database fidelity fixtures and acceptance tooling.
- `docs/`: CLI, sync, recovery, public API, compatibility, and release contracts.

Read `README.md` for user-facing behavior and `CLAUDE.md` for deeper
architecture notes. Treat current code and focused contract docs as authoritative
if historical design notes disagree.

## Build and verification

The module requires the Go toolchain version declared in `go.mod` (currently
Go 1.25.7).

```text
make build       Build ./smt from ./cmd/smt
make test        Run the full Go test suite verbosely
make test-short  Run tests with -short
make fmt         Format all Go packages
make lint        Run golangci-lint
make check       Run formatting, then the full test suite
```

For a focused test, use:

```text
go test ./path/to/package -run TestName
```

CI runs the hermetic unit suite, race detector, lint, CLI build, and live CRM
database matrix. Prefer the smallest relevant test while iterating, then broaden
verification in proportion to the change. The remaining live database and live
AI suites are explicit acceptance gates, not ordinary unit tests; follow
`docs/live-acceptance.md` and run them only when the task requires those
external systems.

Go code must be `gofmt`-clean. Tests normally live beside the package they
exercise. Update golden files only for an intentional, reviewed behavior change.

## Architecture guardrails

- Executable DDL belongs to deterministic Go renderers. AI may parse or review
  a completed plan and diagnose failures, but it must not author, patch, or
  silently rewrite executable SQL.
- Keep preview and apply on the same rendered plan. Generated `schema.sql` and
  `migration.sql` are operational review artifacts, not alternate sources of
  renderer behavior.
- Put type translation in the canonical mapping pipeline and expression
  translation in `internal/expr`; do not scatter dialect string rewrites through
  the CLI or orchestrator.
- Preserve source-dialect context and exact schema metadata: length,
  precision/scale, nullability, identity, defaults, timezone semantics,
  computed columns, constraints, and index details.
- Fail explicitly for unknown or unsupported operations. Do not emit
  plausible-looking SQL that weakens fidelity or bypasses capability checks.
- Keep identifier normalization centralized through the existing driver helper.
- Driver registration uses `init()` plus the applicable blank imports in
  `internal/pool/factory.go` and `internal/config/config.go`; a new CLI database
  driver needs both pieces.

When deterministic output changes for the same input, assess
`internal/ddl.RendererVersion`, manifests, compatibility tests, and SQL goldens.
The v1 persistence policy in `docs/v1-compatibility.md` applies to state,
snapshots, manifests, and renderer versions.

## Contribution and safety principles

- Preserve public `schema` API compatibility. Additive capability interfaces are
  preferred; unsupported dialect features should return typed errors.
- Keep schema diffing structural and deterministic. Risk labels and
  `--allow-data-loss` gating are part of the sync contract.
- Add regression tests for renderer and mapping fixes, including source/target
  dialect combinations affected by the change. Counts alone are not sufficient
  for schema fidelity; use the CRM metadata checks when relevant.
- Treat apply commands, data-loss flags, fixture loading, database teardown,
  snapshot/profile deletion, and other external state changes as explicit
  operations. Do not run them unless the task authorizes that scope.
- Never commit credentials, local configuration, state databases, runtime
  `schema.sql`/`migration.sql` artifacts, or acceptance outputs. Follow
  `.gitignore` and update tracked examples when configuration contracts change.
- Keep changes scoped and preserve unrelated user work. Do not commit, push,
  publish, or merge unless explicitly requested.
