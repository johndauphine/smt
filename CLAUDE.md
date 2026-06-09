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

- **#28** — Documentation-only: gpt-oss-20b → MSSQL whack-a-mole. Pre-#27 prompt rules and post-#27 prompt rules give the same 5/9 net score on the matrix but redistribute which pairs fail (rule-following inconsistency, not missing rules). Standing rule "no per-model AI workarounds" applies — the issue exists to document the model-selection guidance, not as a fix target. Note: the validate-and-retry work landed since (#29 family) helps materially on local models, partially answering this issue's "out of scope" suggestion.

Older non-issue follow-ups:

- `MigrationDefaults` in `internal/secrets/secrets.go` carries unused workers/chunk_size/buffer fields. Safe to drop once we're confident no DMT secrets file in the wild needs them.

### Cross-engine coverage status (as of 2026-05-06)

The CRM fixture (`testdata/crm/`) supports all three engines as both source and target. The full 9-pair matrix is functional with the project default (Sonnet 4.6).

#### Pass criteria for matrix runs

Counts (tables / FKs / CHECKs match source) are necessary but **not sufficient**. A "successful" matrix pass must also satisfy column-level metadata equivalence:

1. **Per-column `max_length` matches exactly.** Source `varchar(20)` must land as target `VARCHAR(20)` (or dialect equivalent), not `varchar(10)`/`varchar(50)`/`text`. Halving / bucket-rounding / size-substitution silently breaks insert paths on real workloads.
2. **Per-column `precision` and `scale` match exactly** for numeric/decimal columns. `NUMERIC(18,4)` source → `NUMERIC(18,4)` target.
3. **Per-column nullability matches exactly.** A source NOT NULL column must remain NOT NULL on the target.
4. **Per-column timezone-awareness preserved.** `datetime2` (no TZ) → `timestamp without time zone`; `datetimeoffset` / `timestamptz` (with TZ) → `timestamp with time zone`. Adding TZ where the source lacks it (or stripping it where it has it) is a fidelity failure.
5. **Identity / auto-increment preserved.** Source IDENTITY column → target's identity equivalent (PG `GENERATED BY DEFAULT AS IDENTITY` or sequence default; MySQL `AUTO_INCREMENT`).
6. **Default-expression class preserved.** Source `GETUTCDATE()` → target `CURRENT_TIMESTAMP`; source `NEWID()` → target's UUID generator. Drop / substitute / hardcode is a fail.
7. **Computed columns preserved** (storage class included: STORED vs VIRTUAL where applicable).

Counts alone hid the regression that surfaced after PR #16 (introspection-facts migration), where local models silently halved `varchar` lengths and the matrix still showed ✓. The harness in `testdata/crm/verify_columns.sh` applies criteria 1–6 column-by-column. Criterion 7 (computed-column presence + storage class) is a TODO — needs cross-dialect expression normalization.

#### In-loop verification (opt-in)

`migration.ai_verify: true` adds an AI **parse + deterministic-compare** pass between DDL generation and exec (#55). The verifier model parses the proposed target DDL into structured `Column[]` JSON; Go-side `CompareColumns` runs the six per-column criteria mechanically (max_length, precision/scale, nullability, identity, TZ class, default class) against the source. Any deltas surface as `Issues` and feed back into the next generation attempt as `PreviousAttempt`, sharing the `ai_max_retries` budget with exec-fail retries. Cache hits skip verify (cached DDL was already verified and executed). By default, generation and verification use the same provider; set `migration.ai_verifier_model` to a different provider entry in the secrets file to pair a cheap generator with a stronger parser (recommended pattern when the generator is Haiku-class — Sonnet parses more reliably than Haiku does). Opt-in default — set the flag to enable. To force re-verification of cached entries after enabling, clear `~/.smt/type-cache.json`.

The split is deliberate. The earlier free-text auditor (PR #47, abandoned in #53) asked one model to do two cognitive jobs in one response — parse the DDL AND judge equivalence per criterion. LLMs are good at the first and inconsistent at the second; cross-dialect runs hit prose-drift and lexical-vs-class confusion that prompt iteration could not fix (see #53's closing comment for evidence across Haiku+Sonnet, Sonnet+Opus pairings). #55 keeps the AI on the parse step it's good at and moves comparison to deterministic Go where lexical/class distinctions are unambiguous.

The deterministic comparator handles class-equivalence cases the auditor used to false-positive on: `nvarchar(N) ≡ varchar(N)`, `datetime2 ≡ timestamp`, `datetimeoffset ≡ timestamptz`, `GETUTCDATE() ≡ CURRENT_TIMESTAMP`, MSSQL `((0))/((1))` ≡ pg `false/true`, `uniqueidentifier ≡ uuid ≡ char(36) ≡ binary(16)`, MSSQL MAX-sentinel max_length=-1 ≡ unbounded targets max_length=0. Computed columns short-circuit length/precision/scale checks (the engine synthesizes those from the expression). All equivalence rules live in `internal/driver/verify_compare.go` with unit tests pinning each cross-dialect case — adding a new rule means editing Go, not editing prompts.

Scope note: #55 v1 covers TABLE DDL only. CREATE INDEX / FOREIGN KEY / CHECK CONSTRAINT verify still go through the legacy free-text auditor (`buildVerifyIndexDDLPrompt` etc.). Index and FK shapes are simple enough that the prose-drift class doesn't fire often there; CHECK predicates remain the rough surface — predicate-translation work (regex → LIKE etc.) probably needs AST-level normalization, a separate effort.

**Same-model verify size threshold (empirical).** Same-model verify is reliable above roughly 20B parameters. End-to-end on the CRM Companies fixture, mssql→pg, with `ai_verify: true`:

| Model | Verify retries | External harness |
|---|---|---|
| Sonnet 4.6 | 0 | PASS |
| `openai/gpt-oss-20b` | 0 | PASS |
| `qwen/qwen3-coder-30b` | 0 | PASS |
| `gemma-4-26b-a4b-it-mlx` (MLX, 26B/4B-active MoE) | 0 | PASS |
| `gemma-4-e4b-it-mlx` (MLX, ~4B effective) | 2 | **FAIL** — auditor false-positives on the prompt's ACCEPTABLE list (MSSQL `((1))` → PG `true` for bit→boolean defaults, `IDENTITY ≡ GENERATED BY DEFAULT AS IDENTITY`, `GETUTCDATE() ≡ CURRENT_TIMESTAMP`); the generator "corrects" by dropping a real default to silence the auditor. Net: introduces a regression worse than verify-off. |

Below the threshold the auditor doesn't reliably hold the equivalence rules in working memory. For sub-20B local models, either upgrade the local model, leave `ai_verify: false`, or use cross-model verify (`ai_verifier_model: <cloud-provider>`) — a cloud auditor over a cheap local generator avoids the correlated-bias trap entirely. Cross-model verify is also the recommended pattern on platforms with poor local-AI performance (e.g. ARM Windows / Snapdragon X without CUDA): point generation at a cheap cloud provider (Haiku) and verification at a strong one (Sonnet); cost is essentially the same as same-model Sonnet verify since the generator handles the bulk of the tokens.

Criterion 6 is currently a binary "has-default Y/N" check, not full expression equivalence. It catches the most common regression (dropped default) but a target that translates `GETUTCDATE()` to a wrong-but-plausible target expression (e.g. `now()` instead of `CURRENT_TIMESTAMP`) would pass. Tightening to expression equivalence requires per-dialect normalization tables and is a follow-up.

When editing prompts, re-run the CRM fixture end-to-end with the column-diff harness; do not trust count-only checks.

Caveats:
- **Local models** (gpt-oss-20b, qwen3-coder-30b) score around 3-5/9 with model-specific failure patterns; see #28 for the documented analysis. The validate-and-retry feature (`migration.ai_max_retries`, default 3) materially helps local-model reliability — gpt-oss-20b mssql→mssql went 6/14 → 14/22/31 at temperature=0 with retries fixing parser-rejected DDL on the second attempt.
- **Local models + `ai_verify: true`** — works cleanly at 20B+ parameters (gpt-oss-20b, qwen3-coder-30b, gemma-4-26b-a4b-it-mlx all verified). Sub-20B local (gemma-4-e4b-it-mlx) hits same-model correlated-bias false positives that net-regress the schema; either upgrade the local or use cross-model verify via `ai_verifier_model` (Phase 2 — see below).

Default model is `claude-sonnet-4-6`. Haiku was tried and reverted historically because of `pg → mssql` PERSISTED computed-column issues; with #13 plumbed (full SourceContext in the prompt) Haiku now lands mssql↔pg correctly, useful for cost-sensitive workloads — though mssql↔mysql with Haiku is still flaky.

### Resolved (kept here as decision log)

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
- AI prompts are sensitive to wording — small phrasing changes can flip whether the AI preserves NOT NULL / DEFAULT / generated columns / `max_length`. PR #9 fixed a regression where the prompt's `OUTPUT REQUIREMENTS` section had a DMT-era line telling the AI to drop NOT NULL "for data migration flexibility." A separate regression introduced by PR #16 (May 2026) — moving from synthesized source-DDL strings to raw introspection facts — silently broke `max_length` fidelity on sub-Sonnet models because the prompt didn't explicitly demand exact preservation; the AI had to *compose* `VARCHAR(20)` from `data_type=varchar` + `max_length=20`, and weaker models reached for "round to a friendly bucket" heuristics. Fixed by adding an emphatic length/precision/scale rule. When editing prompts, re-run the CRM fixture end-to-end with the column-diff harness in `testdata/crm/verify_columns.sh` (criteria 1–6 above; criterion 7 is harness-TODO); do not trust count-only checks.
