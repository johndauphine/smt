# SMT v1 Live Acceptance Gates

The default `go test ./...` suite is hermetic. Live database and live AI checks
are release gates that must be invoked explicitly.

All release-gate artifacts are written under `.acceptance-artifacts/` so CI can
archive the directory.

## StackOverflow2010 No-AI Gate

This gate proves the historical SQL Server to PostgreSQL no-AI path.

Required services:

- SQL Server with the `StackOverflow2010` database loaded.
- PostgreSQL with an existing target database, default `postgres`.

Command:

```bash
make test-so2010
```

Important environment variables:

```bash
SO2010_MSSQL_HOST=localhost
SO2010_MSSQL_PORT=1433
SO2010_MSSQL_DB=StackOverflow2010
SO2010_MSSQL_USER=sa
SO2010_MSSQL_PASS=TestPass2024
SO2010_PG_HOST=localhost
SO2010_PG_PORT=5432
SO2010_PG_DB=postgres
SO2010_PG_USER=postgres
SO2010_PG_PASS=TestPass2024
SO2010_PG_SCHEMA=so2010_accept
```

Artifact:

```text
.acceptance-artifacts/so2010/so2010_verification.json
```

## CRM Matrix Gate

This gate exercises each supported engine at least once as a source and once as
a target:

| Case | Source | Target |
|------|--------|--------|
| `mssql-to-postgres` | SQL Server | PostgreSQL |
| `postgres-to-mysql` | PostgreSQL | MySQL |
| `mysql-to-mssql` | MySQL | SQL Server |

Start local disposable services:

```bash
make test-dbs-up
make mysql-test-up
```

Load the CRM fixtures from `testdata/crm/README.md`, then run:

```bash
make test-crm-acceptance
```

The Go acceptance harness verifies table presence plus columns, max length,
precision/scale, nullability, identity, timezone class, default-expression
class, primary keys, secondary indexes, foreign keys, and check-constraint
presence. Default expressions use the same deterministic class comparison as AI
review. Computed-column expression and storage-class equivalence is not a v1
release claim; the legacy shell helper remains a smoke tool for the SQL Server
to PostgreSQL path.

Manual CLI configs for the same three paths live in `testdata/crm/configs/`.

Artifact:

```text
.acceptance-artifacts/crm/crm_acceptance_matrix.json
```

## Live AI Smoke Gate

This gate is optional for normal development but required before publishing a
v1 release candidate that advertises live AI review/advisory behavior.

Configure `~/.secrets/smt-config.yaml` or point `SMT_SECRETS_FILE` at an
equivalent secrets file with a valid default provider:

```yaml
ai:
  default_provider: anthropic
  providers:
    anthropic:
      api_key: "..."
      model: claude-sonnet-4-6
```

Command:

```bash
make test-live-ai
```

The test runs only when `SMT_LIVE_AI=1` is set by the make target. It verifies:

- `AITypeMapper.CallAI` reaches the configured provider and returns parseable
  JSON.
- The AI DDL parser returns structured columns for a small PostgreSQL table.
- AI failure diagnosis returns `cause`, `suggestions`, `confidence`, and
  `category`.
- AI expression-fix suggestion returns parseable JSON and passes
  `ValidateTargetExpression`.

The report records provider name, model, and pass metadata only. It does not
record prompts, raw responses, API keys, or full provider error bodies.

Artifact:

```text
.acceptance-artifacts/ai/live_ai_smoke.json
```
