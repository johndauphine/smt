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

`testdata/crm/` holds three native-dialect 3NF CRM source schemas (`crm_mssql.sql`, `crm_postgres.sql`, `crm_mysql.sql`) — same logical 14-table shape across all three but each in its own dialect (MSSQL: IDENTITY+DATETIMEOFFSET+UNIQUEIDENTIFIER+PERSISTED; PG: GENERATED IDENTITY+TIMESTAMPTZ+JSONB+arrays+STORED; MySQL: AUTO_INCREMENT+ENUM+SET+JSON+VIRTUAL/STORED), plus a standalone `type_smoke` table per fixture (#46) covering boundary lengths, legacy LOB types, explicit fsp, time-only, sized binary/blob tiers, and numeric precision extremes. Use these to drive any source × target permutation; they're the canonical test surface for column-metadata fidelity (NOT NULL, defaults, computed columns, FK actions). See `testdata/crm/README.md` for load commands.

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
- `phases.go` — `Run` renders the full DDL plan (the same `renderDDLPlan` path `smt create` preview uses, in `ddl_plan.go`) and then `executePlan` runs each statement through `Writer.ExecRaw`, skipping objects that already exist on the target (idempotent re-runs). One render pipeline means schema.sql is exactly what apply executes (#87). Execution is sequential; rendering is concurrent (`runParallel`)
- `healthcheck.go` — connection ping + table count
- `history.go` — `ShowHistory` / `ShowRunDetails` rendering of past runs from the state DB

Optional phases (indexes, FKs, checks) are gated by `cfg.Migration.Create*` booleans. There is no transfer phase, no validate-row-counts phase, no chunk-level resume.

### Philosophy: deterministic first, AI optional

SMT must be able to generate schema DDL without an AI model. AI is an optional enhancement for review and diagnostics, not the source of executable DDL.

Deterministic SMT code owns:

- **Type mapping and DDL generation** for supported source/target pairs through `internal/ddl.Renderer` and per-driver deterministic helpers.
- **Identifier naming convention** for the target: `driver.NormalizeIdentifier(targetType, name)` — single source of truth shared by `create` and `sync`.
- **ALTER generation** in `sync`: deterministic rendering from `schemadiff.Compute`.
- **Risk labels** for sync plans: deterministic classification in `schemadiff`.
- Schema introspection (`ExtractSchema`, `LoadIndexes`, etc.) through catalog queries against `information_schema` / `sys.*`.
- Structural diffing (`schemadiff.Compute`) and statement execution (`Writer.ExecRaw`).

AI may still be used for optional review (`ai_review.enabled`) and diagnostics, but it must not generate or patch executable DDL.

### AI infrastructure

The full multi-provider HTTP plumbing (Anthropic / OpenAI / Google / Ollama / LM Studio) lives in `internal/driver/ai_typemapper.go`:

- `MapType` / `MapTypeWithError` — legacy cached type-mapping API retained for optional callers, not required by core schema DDL generation.
- `Ask(ctx, prompt)` / `CallAI(ctx, prompt)` — generic prompt entrypoints used by optional review and diagnostics.
- `dispatch` — single switch over providers; adding a new provider is a one-place edit.

`internal/driver/ai_verify.go` can parse/review deterministic DDL when `ai_review.enabled` is on. Review may warn or fail, but it does not rewrite DDL. `internal/driver/ai_errordiag.go` can diagnose DDL failures for user-facing context. Provider config and API keys live in `~/.secrets/smt-config.yaml` (env var override: `SMT_SECRETS_FILE`).

### Schema diff + sync (the new SMT-specific feature)

`internal/schemadiff/` is the new functionality on top of DMT's schema layer:

- `snapshot.go` — `Snapshot` is a serializable point-in-time view (tables + columns + indexes + FKs + checks + captured_at)
- `diff.go` — `Compute(prev, curr)` returns added/removed/changed tables and per-table column/index/FK/check deltas. Pure data, no SQL knowledge.
- `render_deterministic.go` — converts the structural diff into deterministic target-dialect DDL statements plus Risk classification (`safe` / `blocking` / `rebuild` / `data-loss-risk`).

`Plan.FilterByRisk` lets `sync --apply` refuse data-loss without `--allow-data-loss`.

Storage: `internal/checkpoint/snapshots.go` adds a `schema_snapshots` table to the SQLite state DB (`SaveSnapshot` / `GetLatestSnapshot` / `ListSnapshots`).

CLI flow:
- `smt snapshot` — extract source, save to state DB
- `smt sync` — diff vs latest snapshot, deterministically render SQL, write to `migration.sql` (default) or apply with `--apply`
- `smt drift` — introspect the live **target** and report drift between the source-derived (desired) schema and the existing target (#69). Read-only; cross-dialect column equivalence via `driver.CompareColumns`, so `varchar(20)` ≡ `character varying(20)`. Classifies missing / extra / changed tables and columns (`schemadiff.ComputeDrift`). Exit 0 = in sync, 3 = drift; `--fail-on-destructive-only` exits 0 for additive-only drift (useful as a CI gate).

### Config + secrets split

- `~/.smt/state.db` — run history, encrypted profiles, schema snapshots
- `~/.smt/type-cache.json` — legacy AI type-mapping cache for the optional `MapType` API; **not** consulted by the deterministic executable-DDL path
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

Active work is tracked in GitHub issues. Run `gh issue list --state open` and read the bodies — each carries Symptom + Root cause + Proposed fix. The remaining open work clusters under three epics:

- **#57 (deterministic DDL generation)** — core is done (no-AI schema path, deterministic renderer, repeat-stable). Remaining children: **#62** (UVG-style canonical type layer — largely behavioral-equivalent today; a refactor question), **#64** (renderer/mapper version fingerprints on persisted artifacts), **#65** (SO2010 MSSQL→PG no-AI acceptance test).
- **#58 (optional AI review)** — review is optional, default-off, inspect-only; the verifier-feedback retry loop is gone. Remaining child: **#68** (reviewer contract + provider-failure tests).
- **#59 (deterministic sync)** — snapshot diff, deterministic ALTERs, dry-run + risk gating, golden tests all landed. Remaining child: **#69** (target-side introspection + three-way source/desired/existing diff — the substantive open feature).

Older non-issue follow-ups:

- `MigrationDefaults` in `internal/secrets/secrets.go` carries unused workers/chunk_size/buffer fields. Safe to drop once we're confident no DMT secrets file in the wild needs them.

### Cross-engine coverage status

The CRM fixture (`testdata/crm/`) supports all three engines as both source and target. The full 9-pair matrix runs through the **deterministic renderer** (`internal/ddl.Renderer` + `internal/driver/postgres/deterministic.go`) — there is no model in the executable-DDL path, so matrix fidelity is a property of the Go renderer, not of a model choice or temperature. Re-run after any renderer change with the column-diff harness in `testdata/crm/verify_columns.sh`; do not trust count-only checks.

#### Pass criteria for matrix runs

Counts (tables / FKs / CHECKs match source) are necessary but **not sufficient**. A "successful" matrix pass must also satisfy column-level metadata equivalence:

1. **Per-column `max_length` matches exactly.** Source `varchar(20)` must land as target `VARCHAR(20)` (or dialect equivalent), not `varchar(10)`/`varchar(50)`/`text`. Halving / bucket-rounding / size-substitution silently breaks insert paths on real workloads.
2. **Per-column `precision` and `scale` match exactly** for numeric/decimal columns. `NUMERIC(18,4)` source → `NUMERIC(18,4)` target.
3. **Per-column nullability matches exactly.** A source NOT NULL column must remain NOT NULL on the target.
4. **Per-column timezone-awareness preserved.** `datetime2` (no TZ) → `timestamp without time zone`; `datetimeoffset` / `timestamptz` (with TZ) → `timestamp with time zone`. Adding TZ where the source lacks it (or stripping it where it has it) is a fidelity failure.
5. **Identity / auto-increment preserved.** Source IDENTITY column → target's identity equivalent (PG `GENERATED BY DEFAULT AS IDENTITY` or sequence default; MySQL `AUTO_INCREMENT`).
6. **Default-expression class preserved.** Source `GETUTCDATE()` → target `CURRENT_TIMESTAMP`; source `NEWID()` → target's UUID generator. Drop / substitute / hardcode is a fail.
7. **Computed columns preserved** (storage class included: STORED vs VIRTUAL where applicable).

The harness in `testdata/crm/verify_columns.sh` applies criteria 1–6 column-by-column (criterion 7, computed-column presence + storage class, is a harness TODO — needs cross-dialect expression normalization). Counts alone once hid a regression where `varchar` lengths were silently halved while the matrix still showed ✓ — the column-diff harness exists specifically to catch that class. `testdata/crm/type_smoke` (a 15th standalone table, #46) extends coverage to boundary lengths, legacy LOB types, explicit fsp, time-only, sized binary vs blob tiers, and numeric precision extremes.

#### Optional AI review (`ai_review`, default off)

`ai_review.enabled: true` adds an AI **parse + deterministic-compare** pass that inspects the already-rendered deterministic DDL before apply. It does **not** author or rewrite executable DDL — review is advisory (`ai_review.mode: warn`) or a hard gate (`mode: fail`), never a generator. The reviewer model parses the proposed target DDL into structured `Column[]` JSON; Go-side `CompareColumns` (`internal/driver/verify_compare.go`) runs the per-column criteria mechanically (max_length, precision/scale, nullability, identity, TZ class, default class) against the source and surfaces any deltas as `Issues`. Pair a cheap parser with a stronger one via `ai_review.model` (a provider entry in the secrets file). The legacy `migration.ai_verify` / `ai_verifier_model` keys are accepted as deprecated aliases (warned at load — see #67).

The AI-parses / Go-compares split is deliberate (#55). An earlier free-text auditor asked one model to both parse the DDL and judge equivalence per criterion; LLMs are good at the first and inconsistent at the second, and cross-dialect runs hit prose-drift and lexical-vs-class confusion that prompt iteration could not fix (#53). Keeping the AI on the parse step and moving comparison to deterministic Go makes lexical/class distinctions unambiguous.

The deterministic comparator handles class-equivalence cases the free-text auditor used to false-positive on: `nvarchar(N) ≡ varchar(N)`, `datetime2 ≡ timestamp`, `datetimeoffset ≡ timestamptz`, `GETUTCDATE() ≡ CURRENT_TIMESTAMP`, MSSQL `((0))/((1))` ≡ pg `false/true`, `uniqueidentifier ≡ uuid ≡ char(36) ≡ binary(16)`, MSSQL MAX-sentinel max_length=-1 ≡ unbounded targets max_length=0, MySQL LOB-tier ranks (LONGTEXT ≠ TEXT same-dialect, but either ≡ pg text cross-dialect). Computed columns short-circuit length/precision/scale checks (the engine synthesizes those from the expression). All equivalence rules live in `internal/driver/verify_compare.go` with unit tests pinning each cross-dialect case — adding a rule means editing Go, not a prompt.

Scope note: the deterministic comparator covers TABLE DDL. CREATE INDEX / FOREIGN KEY / CHECK CONSTRAINT review still goes through the legacy free-text auditor (`buildVerifyIndexDDLPrompt` etc.); index/FK shapes are simple enough that prose-drift rarely fires there, while CHECK predicates remain the rough surface (regex→LIKE-class translation likely needs AST-level normalization, a separate effort).

Criterion 6 is currently a binary "has-default Y/N" check, not full expression equivalence — it catches the common dropped-default regression but a wrong-but-plausible translation (`now()` vs `CURRENT_TIMESTAMP`) would pass. Tightening needs per-dialect normalization tables (#64-adjacent follow-up).

**Reviewer model sizing.** Because review only parses DDL (it no longer judges equivalence), small models are far less fragile than under the old free-text auditor — but a too-small reviewer can still misparse and false-positive, prompting noise. Above ~20B parameters reviewing is reliable (gpt-oss-20b, qwen3-coder-30b, gemma-4-26b verified clean on the CRM Companies fixture). For sub-20B local models, leave `ai_review.enabled: false` or point `ai_review.model` at a cloud provider; the same cross-model pattern suits platforms with poor local-AI performance (ARM Windows / Snapdragon X without CUDA). Standing rule: the fix for a misbehaving model is a different model, not per-model compensating code in SMT.

### Resolved (kept here as decision log)

- **#101 (PR #103)** — Source-dialect awareness in the renderer (`Renderer.WithSource`). mysql→mysql preserves `TIMESTAMP` and `tinyint(1)` verbatim; cross-dialect mappings unchanged. Added `Column.DisplayWidth` (tinyint(1) only) and snapshot version 4 with backfill.
- **#42 / #43 (PR #104)** — Removed the dead DMT-era AI tuning config (`ai:` section, `ai_adjust`) and the checkpoint AI-adjustment/tuning-history tables and APIs — none had a consumer after the runtime-tuning monitor was dropped. Incidentally closed an unredacted-`ai.api_key`-in-run-history leak.
- **#46 (PR #105)** — `testdata/crm/type_smoke` boundary-type table per fixture, plus pg blob→bytea mapping and LOB/binary length rules in both the comparator and `verify_columns.sh`.
- **#71 (PR #106)** — Golden + stability + risk-gating tests for deterministic sync plans (`internal/schemadiff/golden_test.go`, regenerate with `UPDATE_GOLDEN=1`). The golden run caught and fixed a pg `CHECK`-parenthesization bug.
- **#54 (PR #107)** — Un-configured render concurrency drops from 8 to 2 when `ai_review` is enabled, so cold-cache review runs stay under provider rate limits. Explicit `ai_concurrency` still passes through.
- **#108 (PR #109)** — Stopped downgrading LOB capacity on MySQL targets: foreign `text`→`LONGTEXT`, `image`→`LONGBLOB`, unbounded binary→`LONGBLOB`, oversized varchar→`MEDIUMTEXT`/`LONGTEXT`; MySQL's own blob tiers preserved.
- **#67 (PR #110)** — `migration.ai_verify` / `ai_verifier_model` now warn at config load as deprecated aliases for `ai_review.*`. Drive-by: `logging.SetOutput(nil)` resets to the default instead of panicking on the next write.
- **#25 (PRs #96, #99)** — MySQL InnoDB CHECK-constraint deadlocks (Error 1213) under concurrent creation. First mitigated by serializing the CHECK phase for MySQL targets, then mooted entirely when #99 made all DDL execution sequential (rendering stays concurrent).

- `create` and `sync` now agree on identifier naming. Both go through `driver.NormalizeIdentifier(targetType, name)` — a single source of truth that matches PostgreSQL's case-folding (lowercases + slugs non-alphanumeric) and passes MSSQL/MySQL through. The deterministic DDL renderer also uses this path for create and sync statements.
- **#13 (PR #24)** — SourceContext now populated. `Reader.DatabaseContext()` returns a sync.Once-cached `*DatabaseContext`; `Orchestrator.CreateTables` passes it via `TableOptions.SourceContext`. The `=== SOURCE DATABASE ===` prompt block is now symmetric to TARGET (version, charset, collation, identifier case, varchar semantics, version-gated features). The "No source context available" string is gone.
- **#17 (PR #22)** — Postgres `LoadForeignKeys` / `LoadCheckConstraints` are real implementations against `pg_constraint`. Composite FKs handled via `LATERAL UNNEST(c.conkey) WITH ORDINALITY` indexed into `c.confkey`. CHECK predicates via `pg_get_expr(c.conbin, ...)`. Action keywords mapped to the same uppercase strings the mssql/mysql readers produce.
- **#18 (PR #21)** — MySQL `EXTRA` parsing via `parseGeneratedColumnExtra` matches `VIRTUAL GENERATED` / `STORED GENERATED` explicitly, no longer false-matching `DEFAULT_GENERATED`.
- **#19 (PR #23)** — MySQL `DATETIME(N)` precision was originally handled in prompt augmentation; deterministic DDL now owns this through the renderer.
- **#26** — MySQL `ENUM` / `SET` values are captured deterministically. `loadColumns` reads `COLUMN_TYPE`, parses the literal list into `driver.Column.EnumValues`, and the deterministic renderer preserves native `ENUM(...)` / `SET(...)` on MySQL targets.
- **#29 family (PRs #30, #31, #33)** — superseded by deterministic DDL generation. The old AI-DDL validate-and-retry path and retry-classification marker were removed when executable DDL stopped being model-generated.
- **#27** — MySQL function-call default parens (`DEFAULT (UUID())`, `DEFAULT (JSON_OBJECT())`) and MSSQL PERSISTED-implicit-nullability were explicit prompt rules; deterministic DDL now handles these as renderer rules.
- **#48** — `migration.ai_verifier_model` / `ai_review.model` select the AI review provider. Originally plumbed through `pool.NewTargetPool` → `WriterOptions.VerifierTypeMapper` into per-writer reviewer fields; since the preview/apply unification (#87) the reviewer is resolved once in `orchestrator.newCreateDDLRenderer` (`ddl_plan.go`) — writers execute pre-rendered statements and hold no AI mappers. Same PR added `secrets.Provider.Type` (`provider:` YAML field) so multiple entries can share one backend.

## Common gotchas

- `*.yaml` is in `.gitignore`. Whitelist new config files explicitly (e.g. `!config.yaml.example`). `testdata/**/*.sql` is whitelisted similarly so the CRM fixtures stay tracked even though `*.sql` is gitignored.
- The `smt` binary line in `.gitignore` is anchored to `/smt` so it doesn't shadow `cmd/smt/`.
- `gofmt` re-sorts imports after the module-path rewrite. The pre-commit hook will catch any drift.
- The driver registry depends on blank imports in `internal/pool/factory.go`. If a new driver isn't being found, that's the file to check.
- Tests under `internal/driver/{postgres,mssql,mysql}/` include integration tests behind build tags that need live databases (`make test-dbs-up`); `-short` skips them.
- OpenAI reasoning models (o-series, gpt-5.x) reject the default `temperature: 0` with HTTP 400. SMT's `Provider.ModelTemperature` (yaml `model_temperature`) lets the user override per provider — set `model_temperature: 1` in the openai block of the secrets file to use them. There is no model-name list in code (intentional; see PR #11).
- Executable DDL is **deterministic** — column fidelity (NOT NULL / DEFAULT / generated columns / `max_length` / precision / TZ class) is owned by `internal/ddl.Renderer` and the per-driver renderers, not by any prompt. The fidelity bar lives in code and is pinned by unit tests plus the `testdata/crm/verify_columns.sh` end-to-end harness; re-run it after any renderer change (criteria 1–6; criterion 7 is harness-TODO) and do not trust count-only checks. (History: model-authored DDL was sensitive to prompt wording — weak models would drop NOT NULL or bucket-round `varchar` lengths — which is precisely why the executable path was moved off the model in #77/#97. The optional AI **review** path still uses prompts, but only to *parse* DDL for the deterministic comparator, never to author it; review-prompt wording is far less load-bearing as a result.)
- The renderer carries the **source dialect** (`Renderer.WithSource`, plumbed from the orchestrator and sync). Same-dialect runs pass types through verbatim where the generic cross-dialect mapping would lose semantics — MySQL `TIMESTAMP` (UTC-normalized) and `tinyint(1)` (boolean convention) on mysql→mysql (#101), and MySQL LOB tiers preserved rather than flattened (#108). When adding a type rule that only makes sense same-dialect, gate it on `r.source`.
