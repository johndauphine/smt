# SMT v1.4.0 Release Checklist

## Current Release Status

SMT v1.4.0 is the current release target. Treat it as published only after the
annotated tag, release assets, checksums, and stable GitHub Release are present.

| Item | Status |
|------|--------|
| Release | Target: stable GitHub Release `v1.4.0`; not complete until published |
| Tag | Create annotated tag `v1.4.0` at the commit that passed every release gate |
| CI | Unit Tests, Race Tests, Lint, and Build CLI must pass on the release commit |
| Release blockers | Confirm there are no open release-blocking issues before tagging |
| Assets | `smt-linux-amd64`, `smt-darwin-arm64`, `smt-windows-amd64.exe`, `checksums.txt` |
| Checksums | Generate locally, verify before upload, then verify the published `checksums.txt` |
| Acceptance artifacts | Generate `so2010_verification.json`, `crm_acceptance_matrix.json`, and `live_ai_smoke.json` under `.acceptance-artifacts/` during release validation |

## v1.4 Scope

The stable CLI surface is `init`, `create`, `sync`, `drift`, `snapshot`,
`snapshot list`, `health-check`, `profile`, `init-secrets`, and `history`.
Preview remains the default for create and sync; applying DDL is explicit. See
[`docs/cli.md`](cli.md) for the command and flag contract.

The public [`schema`](public-ddl-api.md) package covers deterministic creation
of schemas/databases, tables, columns, indexes, standalone primary keys,
foreign keys, named unique constraints, and check constraints. `PlanCreate`
orders schema and table statements; callers render and order standalone side
objects explicitly.

The public evolution API renders typed, ordered batches for
schema/table/index/constraint drops, column add/drop/type/nullability/default
changes, and table truncation. PostgreSQL supports the complete current
evolution surface including explicit cascade; SQL Server and MySQL support the
non-cascade surface. SQLite and ClickHouse advertise their supported subsets.
Every unsupported request returns `*schema.UnsupportedFeatureError` rather
than silently dropping behavior or proposing a rebuild.

## Supported Artifacts

`make release-artifacts` builds:

- `smt-linux-amd64`
- `smt-darwin-arm64`
- `smt-windows-amd64.exe`

`make release-checksums` writes `dist/checksums.txt` with SHA-256 checksums for
the built artifacts.

Other platforms can build from source with Go 1.25.7 or later:

```bash
go build -trimpath -o smt ./cmd/smt
```

## First Run

Create a config and secrets file:

```bash
smt init
smt init-secrets
```

Required setup:

- `config.yaml` defines source/target database connection metadata.
- `~/.secrets/smt-config.yaml` stores AI provider keys, Slack webhook URL, and
  `encryption.master_key`.
- Generate the profile encryption key with `openssl rand -base64 32`.
- Keep real credentials out of the repository.

The default schema path does not use AI:

```yaml
schema_generation:
  mode: deterministic
ai_review:
  enabled: false
```

Optional AI review/advisory behavior is configured with `ai_review.enabled`,
`ai_review.model`, `ai_review.diagnose_failures`, and `ai_review.suggest_fixes`.
The deprecated 0.x `migration.ai_verify` and `migration.ai_verifier_model`
aliases are removed in v1.

## Install And Update

Download the artifact for the host OS/architecture, verify its checksum, mark it
executable on Unix-like systems, and put it on `PATH`:

```bash
shasum -a 256 -c checksums.txt
chmod +x smt-darwin-arm64
mv smt-darwin-arm64 /usr/local/bin/smt
```

To update, replace the existing binary with the newer artifact after verifying
the checksum. Config files, secrets, state DBs, snapshots, and run artifacts are
kept outside the binary and are not overwritten by installation.

## Release Commands

Run hermetic local gates:

```bash
go test ./... -count=1
go test -race ./... -count=1
golangci-lint run
go build -trimpath -o /tmp/smt-ci-build ./cmd/smt
```

Run live release gates:

```bash
make test-so2010
make test-crm-acceptance
make test-live-ai
```

Build artifacts and checksums:

```bash
VERSION=v1.4.0 make release-checksums
```

Archive:

- `dist/*`
- `.acceptance-artifacts/so2010/so2010_verification.json`
- `.acceptance-artifacts/crm/crm_acceptance_matrix.json`
- `.acceptance-artifacts/ai/live_ai_smoke.json`

## GitHub Release Notes

Use the `CHANGELOG.md` `1.4.0` section as the release body. It records the
public schema-evolution API and the deterministic CLI/rendering changes shipped
since v1.0.0 without reconstructing notes manually.
