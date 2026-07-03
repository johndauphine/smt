# SMT v1 Apply Failure and Recovery Contract

SMT writes DDL artifacts before applying them. Operators should treat
`schema.sql`, `migration.sql`, and the run `manifest.json` as the recovery
record for a failed apply.

## `smt create --apply`

`create --apply` renders the same deterministic `schema.sql` that `smt create`
preview writes, persists it under the run artifact directory, then executes the
statements against the target in dependency order.

If a statement fails:

- execution stops at the first failed statement;
- the run is marked `failed` in the state DB;
- the error includes the statement description, database error, and SQL text;
- later statements are not attempted.

Create re-runs are intended to be idempotent for objects SMT can identify by
name in the target catalog. Before executing create statements, SMT checks
whether tables, indexes, foreign keys, and check constraints already exist and
skips existing objects. Existing-object skips are logged as shape-unverified;
run `smt drift` after recovery to confirm the target matches `schema.sql`.
Schema statements are rendered with `IF NOT EXISTS` semantics where the target
dialect supports it.

## `smt sync --apply`

`sync --apply` creates a run record and writes the rendered `migration.sql`
plus a run `manifest.json` under `migration.data_dir/runs/<run-id>/ddl/`
before executing anything. It refuses unsupported changes before executing
DDL, and refuses `data-loss-risk` statements unless `--allow-data-loss` is set.

Once execution starts, sync statements run sequentially and stop at the first
failure. The error includes the failed statement number, description, database
error, and SQL text. The run is marked `failed` with its current phase in the
state DB. `--save-snapshot` writes a new baseline only after every statement
succeeds.

For `sync --against snapshot --apply`, a partial failure leaves the saved
snapshot baseline unchanged. If the latest baseline still predates that failed
partial apply, SMT refuses a blind snapshot-mode rerun before executing DDL.
Recover by inspecting the run artifact, making the target correct, then running
`smt sync --against target --apply` or capturing a new baseline with
`smt snapshot`.

## Transactions

SMT v1 does not wrap whole create or sync plans in a cross-dialect transaction.
DDL transaction behavior differs across PostgreSQL, SQL Server, and MySQL; some
statements auto-commit, some take long metadata locks, and some cannot be safely
rolled back across engines. The v1 contract is explicit stop-at-first-failure
behavior with durable artifacts, not all-or-nothing online schema migration.

Operators who need transactional DDL for a specific engine can review the
generated SQL and apply it manually inside their own engine-specific transaction
or migration framework.

## Release Smoke

Automated coverage pins the v1 failure behavior:

```bash
go test ./cmd/smt -run TestApplyPlan_StopsAtFirstFailure
go test ./cmd/smt -run TestSyncApplyRecordsRunAndArtifactsOnFailure
go test ./internal/orchestrator -run TestExecutePlanStopsAtFirstFailure
```

For manual release validation, run a small `create --apply` or `sync --apply`
against a disposable target with an intentionally conflicting object, confirm
the command stops at the failed statement, inspect the artifact SQL, fix or drop
the conflicting object, and rerun.
