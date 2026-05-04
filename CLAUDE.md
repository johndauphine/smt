# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

SMT is a schema migration tool. It extracts the schema of a source database, generates the matching DDL on a target database, and applies incremental schema changes (ALTER TABLE, CREATE INDEX, ...) detected by diffing the current source schema against a stored snapshot.

The mental model is: **SMT = DMT (the data migration tool at ~/repos/dmt) minus data transfer, plus a schema-diff/sync feature**. SMT shares DMT's driver model, AI-assisted type mapping, TUI scaffolding, encrypted profile storage, and SQLite state DB. SMT does not move rows — there are no goroutine pools, no chunking, no progress trackers, no parallel workers, no read-ahead/write-ahead buffers.

## Build / test / lint

```
make build         # builds ./smt from ./cmd/smt
make test          # go test -v ./...
make test-short    # skips integration tests
make lint          # golangci-lint run
make fmt           # go fmt ./...
make check         # fmt + test
make test-dbs-up   # docker postgres + mssql for integration
make mysql-test-up # docker mysql for integration
```

Single test:
```
go test -run TestCompute_AddedColumn ./internal/schemadiff/
```

Pre-commit hook (`make setup-hooks`) runs `gofmt -l` and `go test ./... -short` and blocks on either failing.

Go 1.25 required. CGO is off; SQLite uses the `modernc.org/sqlite` pure-Go driver.

### End-to-end test fixtures

`testdata/crm/` holds three native-dialect 3NF CRM source schemas (`crm_mssql.sql`, `crm_postgres.sql`, `crm_mysql.sql`) — same logical 14-table shape across all three but each in its own dialect (MSSQL: IDENTITY+DATETIMEOFFSET+UNIQUEIDENTIFIER+PERSISTED; PG: GENERATED IDENTITY+TIMESTAMPTZ+JSONB+arrays+STORED; MySQL: AUTO_INCREMENT+ENUM+SET+JSON+VIRTUAL/STORED). Use these to drive any source × target permutation; they're the canonical test surface for column-metadata fidelity (NOT NULL, defaults, computed columns, FK actions). See `testdata/crm/README.md` for load commands.

## Architecture

### Pluggable driver model (preserved verbatim from DMT)

Database engines are pluggable via `init()` registration plus a blank import:

- `internal/driver/registry.go` — central registry, case-insensitive name lookup
- `internal/driver/{postgres,mssql,mysql}/driver.go` — each calls `driver.Register(...)` in `init()`
- `internal/pool/factory.go` — `NewSourcePool` and `NewTargetPool` blank-import every driver to trigger registration; the orchestrator opens connections through these factories

To add a new database engine: drop a package under `internal/driver/foo/` implementing `driver.Driver`/`driver.Reader`/`driver.Writer`, add a blank import to `internal/pool/factory.go`. No changes to orchestrator, config, or TUI.

### Orchestrator (split for review)

`internal/orchestrator/` is intentionally small — schema runs are short and synchronous, no parallelism:

- `orchestrator.go` — struct + lifecycle + accessors (`New`, `NewWithOptions`, `Close`, `Source`, `Target`, `State`)
- `phases.go` — one named method per phase (`ExtractSchema`, `CreateTargetSchema`, `CreateTables`, `CreateIndexes`, `CreateForeignKeys`, `CreateCheckConstraints`) plus `Run` which calls them in order
- `healthcheck.go` — connection ping + table count
- `history.go` — `ShowHistory` / `ShowRunDetails` rendering of past runs from the state DB

Optional phases (indexes, FKs, checks) are gated by `cfg.Migration.Create*` booleans. There is no transfer phase, no validate-row-counts phase, no chunk-level resume.

### Philosophy: SMT feeds the AI, the AI does the work

SMT's job is to give the AI the context it needs and execute what comes back. Translation, generation, and judgment belong to the AI:

- **type mapping** (source dialect → target dialect): AI, via `MapType`
- **DDL generation** for new schemas: AI, via `tableMapper.GenerateTableDDL` (CREATE TABLE) and `finalizationMapper.GenerateFinalizationDDL` (indexes, FKs, checks) inside each driver's Writer
- **Identifier naming convention** for the target: `driver.NormalizeIdentifier(targetType, name)` — single source of truth shared by `create`'s pre-sanitization and `sync`'s `Diff.Normalize`
- **ALTER generation** in `sync`: AI, via `Render` in `schemadiff/render.go`
- **Risk judgment** for ALTERs (safe / blocking / rebuild / data-loss-risk): AI, in the same prompt
- **Error diagnosis** when DDL fails: AI, via `internal/driver/ai_errordiag.go`

What stays deterministic in SMT code:

- Schema introspection (`ExtractSchema`, `LoadIndexes`, etc.) — well-defined queries against `information_schema` / `sys.*`. Cheap and reliable, no AI needed.
- Structural diffing (`schemadiff.Compute`) — pure data comparison, no SQL knowledge.
- Statement execution (`Writer.ExecRaw`) — passes the AI's output to the database.
- CLI / TUI / config / state DB plumbing — non-AI work that has nothing to translate.

If you find yourself writing SQL syntax in Go code (a new ALTER variant, a new dialect quirk), stop — that's AI work. Add it to the prompt instead.

### AI infrastructure

The full multi-provider HTTP plumbing (Anthropic / OpenAI / Gemini / Ollama / LM Studio) lives in `internal/driver/ai_typemapper.go`:

- `MapType` / `MapTypeWithError` — cached type-mapping API, used by every driver's `CreateTable` for source-to-target type inference
- `Ask(ctx, prompt)` — generic free-form prompt entrypoint, used by `internal/schemadiff/render.go` for SQL rendering
- `dispatch` — single switch over providers; adding a new provider is a one-place edit

The table-DDL prompt (`buildTableDDLPrompt` → `buildSourceIntrospectionBlock`) hands the AI **raw introspection facts** (data_type, max_length, precision, scale, nullable, default_expression, computed/computed_expression/computed_storage), not a synthesized source DDL string. Earlier versions assembled a per-dialect CREATE TABLE in Go and asked the AI to translate it; PR #16 dropped that intermediate step because Go-side synthesis was duplicating work the AI already does well and was hiding metadata behind dialect quoting. If you need to give the AI more source context, extend the introspection block — don't rebuild the source-DDL synthesizer.

The source side of that prompt (the `=== SOURCE DATABASE ===` block) is populated via `Reader.DatabaseContext()` — each driver's Reader caches a `*DatabaseContext` (sync.Once) and the orchestrator passes it through `TableOptions.SourceContext` on the first `CreateTableWithOptions` call. PR #24 (closes #13) added this; before that the source block had only `Type:` and the prompt was a one-sided ask.

### AI-DDL validate-and-retry

`internal/driver/retry.go` is the shared retry primitive that wraps every AI-rendered DDL phase (CREATE TABLE + the three finalize phases). The flow inside each writer's `Create*WithOptions` is: ask AI → exec → on failure, feed the verbatim prior DDL + verbatim DB error back into the next prompt → up to `migration.ai_max_retries` retries. There is **no SQLSTATE allowlist** — instead the AI itself classifies hopeless cases by emitting the literal `NOT_RETRYABLE` marker (parsed by `classifyRetryResponse` into `ErrNotRetryable`), in which case the loop exits and the original DB error surfaces to the user. `IsCanceled` short-circuits ctx cancellation/deadline so Ctrl-C doesn't get re-prompted as a "fix this" round-trip.

Cache discipline matters here: the type-mapper's cache is **read-only on the AI path**. Only the writer caches DDL, and only after a successful exec — so a structurally-valid-but-semantically-wrong first attempt can't poison the cache for future runs (PR #33 closes #32). Postgres's `Unlogged` rewrite is applied to `execDDL` while `aiDDL` is what gets cached, so future calls with `Unlogged=false` get the un-rewritten form back.

Config: `migration.ai_max_retries` is `*int` — omitted defaults to 3, `0` is the explicit opt-out, negative values clamp to 0. Resolved in `orchestrator.aiMaxRetries`.

`internal/driver/ai_errordiag.go` diagnoses DDL failures (used outside the retry loop, e.g. for surfacing a final error). All AI prompts are in-source string builders, not template files. Provider config and API keys live in `~/.secrets/smt-config.yaml` (env var override: `SMT_SECRETS_FILE`).

### Schema diff + sync (the new SMT-specific feature)

`internal/schemadiff/` is the new functionality on top of DMT's schema layer:

- `snapshot.go` — `Snapshot` is a serializable point-in-time view (tables + columns + indexes + FKs + checks + captured_at)
- `diff.go` — `Compute(prev, curr)` returns added/removed/changed tables and per-table column/index/FK/check deltas. Pure data, no SQL knowledge.
- `render.go` — `Render(ctx, ai, diff, schema, dialect)` asks the LLM to convert the structural diff into one JSON statement per change, each with a SQL string + description + Risk classification (`safe` / `blocking` / `rebuild` / `data-loss-risk`). The whole diff goes in one prompt so the AI sees cross-statement context.

There is no hand-coded ALTER syntax table — the AI renders SQL. `Plan.FilterByRisk` lets `sync --apply` refuse data-loss without `--allow-data-loss`.

Storage: `internal/checkpoint/snapshots.go` adds a `schema_snapshots` table to the SQLite state DB (`SaveSnapshot` / `GetLatestSnapshot` / `ListSnapshots`).

CLI flow:
- `smt snapshot` — extract source, save to state DB
- `smt sync` — diff vs latest snapshot, AI renders SQL, write to `migration.sql` (default) or apply with `--apply`

### Config + secrets split

- `~/.smt/state.db` — run history, encrypted profiles, schema snapshots
- `~/.smt/type-cache.json` — AI type-mapping cache
- `~/.secrets/smt-config.yaml` — AI keys, encryption master key, Slack webhook (mode 0600, env var `SMT_SECRETS_FILE` overrides)
- `config.yaml` — per-migration settings (source/target connection, schema names, include/exclude tables, create_indexes/FKs/checks flags). See `config.yaml.example`.

Profile encryption uses `SMT_MASTER_KEY` env var (generate with `openssl rand -base64 32`).

### What was removed from DMT (and why)

If you find yourself looking for these in SMT, they're intentionally gone:

- `internal/transfer/` — row-by-row transfer pipeline. SMT runs DDL, not COPY/INSERT.
- `internal/monitor/` — AI-driven runtime tuning of chunk_size/workers. No data transfer to tune.
- `internal/progress/` — chunk-level progress tracking. SMT logs each phase and DDL statement directly.
- `internal/pool/writer_pool.go` — parallel writer goroutines.
- `internal/driver/dbtuning/` — database-level data-transfer parameter tuning.
- `internal/driver/ai_smartconfig.go` — AI suggestions for data-transfer parameters.
- `cmd/migrate/...` paths — SMT's binary is `cmd/smt/`.
- `MigrationDefaults` workers/chunk_size/buffers fields are still defined in `internal/secrets/secrets.go` (kept to avoid breaking the secrets file format) but unused.
- `validator.go` — DMT's row-count validator. Schema validation lives in `schemadiff`.

## Reading the source

Some files preserve DMT's original code shape unchanged (verbatim copy with module-path rewrite from `github.com/johndauphine/dmt` to `smt`). That is by design — for review, anything matching `~/repos/dmt/internal/<path>` is the same in `~/repos/smt/internal/<path>`. Files in `internal/orchestrator/`, `internal/schemadiff/`, `internal/checkpoint/snapshots.go`, and `cmd/smt/` are SMT-specific.

## Open follow-ups

Active work is tracked in GitHub issues. Run `gh issue list --state open` and read the bodies — each carries Symptom + Root cause + Proposed fix:

- **#25** — MySQL InnoDB CHECK-constraint creation deadlocks (Error 1213) at `ai_concurrency: 16`. InnoDB takes metadata locks on parent + child tables when adding a CHECK; SMT fans out across goroutines and the lock graph deadlocks on related tables. Mssql/pg targets tolerate it. Cleanest fix: cap CHECK-phase concurrency at 1 for MySQL targets in `Orchestrator.CreateCheckConstraints`. Surfaced only after #17 was fixed (no PG-source CHECKs to create before that).
- **#26** — MySQL reader doesn't extract ENUM/SET value lists. `loadColumns` reads `DATA_TYPE` (`enum`) but not `COLUMN_TYPE` (`enum('billing','shipping','physical','mailing')`), so the AI sees no values and on `*→mysql` fabricates `ENUM('','')` (Error 1291: duplicated empty value). Fix: add `EnumValues []string` to `driver.Column`, parse `COLUMN_TYPE`, surface in the introspection block. Same shape as #18.
- **#28** — Documentation-only: gpt-oss-20b → MSSQL whack-a-mole. Pre-#27 prompt rules and post-#27 prompt rules give the same 5/9 net score on the matrix but redistribute which pairs fail (rule-following inconsistency, not missing rules). Standing rule "no per-model AI workarounds" applies — the issue exists to document the model-selection guidance, not as a fix target. Note: the validate-and-retry work landed since (#29 family) helps materially on local models, partially answering this issue's "out of scope" suggestion.

Older non-issue follow-ups:

- `smt validate` and `smt analyze` are stubs. validate is a small reuse of `schemadiff.Compute` against the target's introspection rather than a stored snapshot. analyze reuses the AI plumbing for schema-relevant suggestions (risky type mappings, tables to exclude, missing indexes).
- TUI `/sync` and `/snapshot` print "lands in a later phase" instead of dispatching to the new commands; wire them to call the same handlers as the CLI.
- `MigrationDefaults` in `internal/secrets/secrets.go` carries unused workers/chunk_size/buffer fields. Safe to drop once we're confident no DMT secrets file in the wild needs them.

### Cross-engine coverage status (as of 2026-05-04)

The CRM fixture (`testdata/crm/`) supports all three engines as both source and target. The full 9-pair matrix is functional with the project default (Sonnet 4.6) — every source × target pair lands 14/22/33 (tables / FKs / CHECKs) on the CRM fixture.

Caveats:
- **`pg → mysql` at high `ai_concurrency`** can hit MySQL InnoDB CHECK deadlocks (#25). Workaround: drop `ai_concurrency` to 1 for MySQL targets, or run only the affected phase serially. Other 8 pairs are unaffected.
- **`* → mysql` ENUM/SET columns** rely on the AI inferring values from column names because the reader doesn't expose `COLUMN_TYPE` (#26). Sonnet usually infers plausibly; gpt-oss-20b emits empty strings and trips Error 1291.
- **Local models** (gpt-oss-20b, qwen3-coder-30b) score around 3-5/9 with model-specific failure patterns; see #28 for the documented analysis. The validate-and-retry feature (`migration.ai_max_retries`, default 3) materially helps local-model reliability — gpt-oss-20b mssql→mssql went 6/14 → 14/22/31 at temperature=0 with retries fixing parser-rejected DDL on the second attempt.

Default model is `claude-sonnet-4-6`. Haiku was tried and reverted historically because of `pg → mssql` PERSISTED computed-column issues; with #13 plumbed (full SourceContext in the prompt) Haiku now lands mssql↔pg correctly, useful for cost-sensitive workloads — though mssql↔mysql with Haiku is still flaky.

### Resolved (kept here as decision log)

- `create` and `sync` now agree on identifier naming. Both go through `driver.NormalizeIdentifier(targetType, name)` — a single source of truth that matches PostgreSQL's case-folding (lowercases + slugs non-alphanumeric) and passes MSSQL/MySQL through. Earlier I misread the codebase and thought `create` was hand-coding ~3000 lines of DDL string assembly per driver; in fact the per-driver `Writer.CreateTable` / `CreateIndex` / `CreateForeignKey` / `CreateCheckConstraint` already use AI rendering (via `tableMapper.GenerateTableDDL` and `finalizationMapper.GenerateFinalizationDDL`). The actual divergence was a small per-driver pre-sanitization step that `sync` skipped. See `internal/driver/identifiers.go` and the `Diff.Normalize` call in `cmd/smt/sync.go`.
- **#13 (PR #24)** — SourceContext now populated. `Reader.DatabaseContext()` returns a sync.Once-cached `*DatabaseContext`; `Orchestrator.CreateTables` passes it via `TableOptions.SourceContext`. The `=== SOURCE DATABASE ===` prompt block is now symmetric to TARGET (version, charset, collation, identifier case, varchar semantics, version-gated features). The "No source context available" string is gone.
- **#17 (PR #22)** — Postgres `LoadForeignKeys` / `LoadCheckConstraints` are real implementations against `pg_constraint`. Composite FKs handled via `LATERAL UNNEST(c.conkey) WITH ORDINALITY` indexed into `c.confkey`. CHECK predicates via `pg_get_expr(c.conbin, ...)`. Action keywords mapped to the same uppercase strings the mssql/mysql readers produce.
- **#18 (PR #21)** — MySQL `EXTRA` parsing via `parseGeneratedColumnExtra` matches `VIRTUAL GENERATED` / `STORED GENERATED` explicitly, no longer false-matching `DEFAULT_GENERATED`.
- **#19 (PR #23)** — MySQL `DATETIME(N)` precision rule lives in `mysql/dialect.go::AIPromptAugmentation` (engine-specific rules belong in the per-driver Dialect, not in `writeMigrationRules`).
- **#29 family (PRs #30, #31, #33)** — AI-DDL validate-and-retry across all four DDL phases. Cache writes are writer-controlled (post-exec only). AI classifies futile retries via `NOT_RETRYABLE` marker rather than per-driver SQLSTATE allowlists. `IsCanceled` short-circuit on context cancellation/deadline.
- **#27** — MySQL function-call default parens (`DEFAULT (UUID())`, `DEFAULT (JSON_OBJECT())`) and MSSQL PERSISTED-implicit-nullability are explicit prompt rules in the per-driver Dialect.

## Common gotchas

- `*.yaml` is in `.gitignore`. Whitelist new config files explicitly (e.g. `!config.yaml.example`). `testdata/**/*.sql` is whitelisted similarly so the CRM fixtures stay tracked even though `*.sql` is gitignored.
- The `smt` binary line in `.gitignore` is anchored to `/smt` so it doesn't shadow `cmd/smt/`.
- `gofmt` re-sorts imports after the module-path rewrite. The pre-commit hook will catch any drift.
- The driver registry depends on blank imports in `internal/pool/factory.go`. If a new driver isn't being found, that's the file to check.
- Tests under `internal/driver/{postgres,mssql,mysql}/` include integration tests behind build tags that need live databases (`make test-dbs-up`); `-short` skips them.
- OpenAI reasoning models (o-series, gpt-5.x) reject the default `temperature: 0` with HTTP 400. SMT's `Provider.ModelTemperature` (yaml `model_temperature`) lets the user override per provider — set `model_temperature: 1` in the openai block of the secrets file to use them. There is no model-name list in code (intentional; see PR #11).
- AI prompts are sensitive to wording — small phrasing changes can flip whether the AI preserves NOT NULL / DEFAULT / generated columns. PR #9 fixed a regression where the prompt's `OUTPUT REQUIREMENTS` section had a DMT-era line telling the AI to drop NOT NULL "for data migration flexibility." When editing prompts, re-run the OLTP3NF/CRM fixtures end-to-end and verify the column-attribute counts match (tables/fks/checks/identity/computed/notnull/defaults).
