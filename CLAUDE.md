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

`internal/driver/ai_errordiag.go` diagnoses DDL failures. All AI prompts are in-source string builders, not template files. Provider config and API keys live in `~/.secrets/smt-config.yaml` (env var override: `SMT_SECRETS_FILE`).

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

- **#13** — SourceContext is never populated. The AI prompt's `=== SOURCE DATABASE ===` block has only `Type:` (no version, charset, varchar semantics). Fix is structural: add `DatabaseContext()` to the `Reader` interface and plumb through `WriterOptions` / `TableOptions`. Independent of #16.
- **#17** — `internal/driver/postgres/reader.go` `LoadForeignKeys` and `LoadCheckConstraints` are literal `return nil` stubs. Every postgres-as-source migration produces 0 FKs on the target. Implement against `pg_constraint` / `information_schema.check_constraints`, mirror the mssql/mysql readers.
- **#18** — `internal/driver/mysql/reader.go` substring-matches `"GENERATED"` on the `EXTRA` column, which also matches `DEFAULT_GENERATED` (MySQL 8.0.13+ marker for any expression default). Misclassifies default-valued TIMESTAMP/DATETIME columns as computed. Tighten the match to `"VIRTUAL GENERATED"` / `"STORED GENERATED"`.
- **#19** — AI prompt-coverage gap: mssql → mysql for `DATETIME2(N)` columns with `DEFAULT GETUTCDATE()` produces invalid `DATETIME(N) DEFAULT CURRENT_TIMESTAMP` (MySQL needs the precision argument on the default). One-line addition to migration rules.

Older non-issue follow-ups:

- `smt validate` and `smt analyze` are stubs. validate is a small reuse of `schemadiff.Compute` against the target's introspection rather than a stored snapshot. analyze reuses the AI plumbing for schema-relevant suggestions (risky type mappings, tables to exclude, missing indexes).
- TUI `/sync` and `/snapshot` print "lands in a later phase" instead of dispatching to the new commands; wire them to call the same handlers as the CLI.
- `MigrationDefaults` in `internal/secrets/secrets.go` carries unused workers/chunk_size/buffer fields. Safe to drop once we're confident no DMT secrets file in the wild needs them.

### Cross-engine coverage status (as of 2026-05-03)

The CRM fixture (`testdata/crm/`) supports all three engines as both source and target. Coverage of the 9-pair matrix:

- **mssql sources:** mssql→pg ✓, mssql→mssql ✓, mssql→mysql blocked by #19.
- **pg sources:** tables phase works on all targets; FKs/CHECKs blocked by #17 on every pair.
- **mysql sources:** all pairs blocked by #18 at the first table with a `DEFAULT CURRENT_TIMESTAMP` column.

Default model is `claude-sonnet-4-6`. Haiku was tried and reverted because it regressed on `pg → mssql` for tables with computed columns (emitted invalid `<type> AS (...) PERSISTED` where MSSQL forbids the type before AS). The prompt now contains an explicit rule for this case (see PR #16) but Sonnet remains the safer default.

### Resolved (kept here as decision log)

- `create` and `sync` now agree on identifier naming. Both go through `driver.NormalizeIdentifier(targetType, name)` — a single source of truth that matches PostgreSQL's case-folding (lowercases + slugs non-alphanumeric) and passes MSSQL/MySQL through. Earlier I misread the codebase and thought `create` was hand-coding ~3000 lines of DDL string assembly per driver; in fact the per-driver `Writer.CreateTable` / `CreateIndex` / `CreateForeignKey` / `CreateCheckConstraint` already use AI rendering (via `tableMapper.GenerateTableDDL` and `finalizationMapper.GenerateFinalizationDDL`). The actual divergence was a small per-driver pre-sanitization step that `sync` skipped. See `internal/driver/identifiers.go` and the `Diff.Normalize` call in `cmd/smt/sync.go`.

## Common gotchas

- `*.yaml` is in `.gitignore`. Whitelist new config files explicitly (e.g. `!config.yaml.example`).
- The `smt` binary line in `.gitignore` is anchored to `/smt` so it doesn't shadow `cmd/smt/`.
- `gofmt` re-sorts imports after the module-path rewrite. The pre-commit hook will catch any drift.
- The driver registry depends on blank imports in `internal/pool/factory.go`. If a new driver isn't being found, that's the file to check.
- Tests under `internal/driver/{postgres,mssql,mysql}/` include integration tests behind build tags that need live databases (`make test-dbs-up`); `-short` skips them.
