# Changelog

All notable changes to SMT are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.12.1] - 2026-06-21

### Fixed

- **TZ-aware timestamps no longer flatten to naive `DATETIME` on MySQL targets**
  ([#169]). A PostgreSQL `timestamptz` or SQL Server `datetimeoffset` source now
  renders to MySQL `TIMESTAMP` (which stores UTC and converts on read) instead of
  timezone-naive `DATETIME`. The canonical Timestamp mapper gated on
  `UTCNormalized` rather than `WithTZ`, so the same canonical `tzaware_dt` stayed
  TZ-aware on PostgreSQL/SQL Server targets but lost it on MySQL — a fidelity
  regression the deterministic `tz_class` comparator flagged. The comparator is
  reconciled to accept MySQL `TIMESTAMP` for a TZ-aware source (MySQL `DATETIME`
  still flags a genuine loss). `RendererVersion` 3 → 4.
- **AI-review false positives from an incomplete parser contract** ([#168],
  [#170]). The AI-review comparator read four `Column` attributes the parser
  prompt never asked the model to emit, so every affected column false-positived
  regardless of reviewer model (local and cloud alike): fractional-seconds
  precision (`timestamp(6)` vs `timestamp`, also `TIME(N)` / `DATETIME2(N)` /
  `DATETIMEOFFSET(N)`) ([#168]); MySQL `UNSIGNED` (`INT UNSIGNED` vs `INT`); the
  `tinyint(1)` boolean display width (`TINYINT(1)` vs `TINYINT`); and same-dialect
  `ENUM`/`SET` member lists (`max_length 7 vs 0`) ([#170]). The parser contract now
  emits `datetime_precision`, `is_unsigned`, `display_width`, and `enum_values`,
  and the dead ENUM length proxy is replaced by the rendered `ENUM(...)`
  comparison (with a guard that flags an enum parsed without its members).

## [0.12.0] - 2026-06-17

### Added

- **Config wizard** ([#160]) — `smt init` (and TUI `/init`) for guided
  `config.yaml` creation, replacing copy-and-hand-edit of the example. A
  UI-agnostic core (`internal/wizard`) defines the step/field list once;
  `RenderYAML()` emits clean commented YAML, and the written file is validated as
  the same artifact it produces.
- **Live-target sync planner** ([#69]) — the drift/sync path introspects the
  live target for a three-way source / desired / existing diff.
- **Canonical type follow-ups** — a first-class **Spatial** kind for
  geography/geometry ([#121]), warnings for approximate/lossy type conversions
  ([#122]), and routing the drift / AI-review comparator through the canonical
  type IR ([#123]).
- **`smt snapshot list`** ([#159]) — list stored source-schema snapshots (id,
  source type, schema, table count, captured-at), newest first, with `--limit`
  (default 50). Surfaces the existing `schema_snapshots` baselines that `smt
  snapshot` writes and `smt sync` consumes; read-only.

### Fixed

- Multi-expression AI splice loop ([#141]).

### Removed

- **Dead DMT-era state-DB tables and checkpoint APIs** ([#158]). The SQLite
  state DB no longer creates the `tasks`, `task_outputs`, `transfer_progress`,
  or `table_sync_timestamps` tables — they backed DMT's row-transfer pipeline
  (parallel workers, chunk-level resume, date-based incremental sync) and were
  never written by SMT, which runs DDL. The corresponding `StateBackend` /
  `State` / `FileState` methods (task tracking, transfer progress, run-resume,
  sync watermarks) and the `Task` / `TransferProgress` / `TaskWithProgress`
  types are removed. `smt history <run>` is unchanged (its task section was
  always empty). Forward-compat: existing `~/.smt` state DBs keep the orphan
  tables as harmless empties — there is no DROP migration, and a pre-existing DB
  opens and works unchanged.
- **DMT-era `migration_defaults` keys dropped from the secrets file** ([#156]).
  The global `migration_defaults` block in `~/.secrets/smt-config.yaml` no longer
  carries data-transfer tuning that SMT (a schema tool) never consumed:
  `workers`, `max_memory_mb`, `read_ahead_buffers`, `write_ahead_writers`,
  `parallel_readers`, `strict_consistency`, `sample_validation`, `sample_size`,
  `checkpoint_frequency`, `max_retries`, `history_retention_days`, `ai_adjust`,
  and `ai_adjust_interval`. The v1-supported shape is `max_source_connections`,
  `max_target_connections`, `create_indexes`, `create_foreign_keys`,
  `create_check_constraints`, and `data_dir`. Existing secrets files that still
  list a removed key load fine — the key is ignored and a single warning names
  the dropped keys (warn-and-ignore, not a hard failure). `smt init-secrets`
  emits only the supported shape.

## [0.11.0] - 2026-06-14

Headline: **optional AI failure assistance** — when SMT's deterministic renderer
hits something it can't translate, the AI can now help, while never authoring
the DDL SMT applies. All features are opt-in and advisory; executable DDL stays
deterministic.

### Added

- **AI failure diagnosis** ([#131]) — `ai_review.diagnose_failures`. On a schema
  extraction or DDL-render failure (which aborts before any DDL exists, so the
  verifier never sees it), the AI prints user-facing guidance (cause +
  suggestions). Strictly advisory: it never generates, patches, or retries DDL,
  and never changes the run's outcome.
- **AI-assisted fix suggestions** ([#134]) — `ai_review.suggest_fixes` (opt-out;
  defaults to `diagnose_failures`). On a render failure caused by one
  unsupported expression (a column `DEFAULT` or a `CHECK` predicate), the AI
  translates *only that expression* and SMT splices it into its own
  deterministic DDL — the AI never authors a whole table. Written to a clearly
  labeled `schema.suggested.sql` for review; never to `schema.sql`.
  - Deterministic verification: a structural injection guard plus a
    default-class equivalence check stamp each suggestion `[OK]` or `[REVIEW]`.
  - `--apply-suggested` (loud, off by default) splices the fix into the plan and
    continues instead of aborting — the only path by which AI-authored content
    reaches `schema.sql` / the applied DDL, and it is marked inline.

### Changed

- AI diagnosis box output wraps instead of truncating, so long guidance is
  readable ([#133]).

## [0.10.1] - 2026-06-14

### Fixed

- **MSSQL→PostgreSQL render no longer fails on the `CONVERT(date, <now>)` date
  default** ([#127]). A valid, common SQL Server "today's date" default such as
  `DEFAULT (CONVERT(date, GETDATE()))` previously aborted deterministic
  rendering with `unsupported SQL expression function "CONVERT"` — before any
  DDL existed, so `ai_review` could not help. It now maps to `CURRENT_DATE`
  (UTC now-family to `(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date`), and the
  drift / AI-review comparator classifies the idiom as the `current_date` class
  so `create --apply` and `drift` are clean. Any other `CONVERT(...)` form still
  fails rather than being silently reinterpreted.

### Changed

- Dropped the `darwin-amd64` (Intel Mac) build target ([#126]); macOS binaries
  are Apple Silicon (arm64) only.

## [0.10.0] - 2026-06-14

Headline: **SMT's executable DDL is now fully deterministic — generated by Go,
not an AI model.** AI is optional and advisory only (parse + deterministic
compare); it never authors or patches executable DDL.

### Added

- **Deterministic DDL generation** for MSSQL / PostgreSQL / MySQL ([#57]).
  `create` preview and `apply` share one render pipeline, so `schema.sql` is
  exactly what apply executes.
- **Canonical type IR** ([#62]) — one `source → canonical → target` type mapper
  (`internal/canonical`) shared by all three renderer targets, replacing the
  per-target type switches.
- **Schema drift detection** — `smt drift` introspects the live target and
  reports a three-way source / desired / existing diff with cross-dialect column
  equivalence ([#69]).
- **Deterministic run manifest** with renderer/source version fingerprints
  ([#64]); golden + stability tests for sync plans ([#71]); a `type_smoke`
  boundary-type fixture per dialect ([#46]).
- **Type fidelity**: end-to-end fractional-second datetime precision, preserved
  MySQL LOB capacity, MySQL `TIMESTAMP` / `tinyint(1)` same-dialect, and pg
  `timestamptz` → MSSQL `DATETIMEOFFSET`.

### Changed

- **Optional AI review reframed** ([#58]): AI parses the already-rendered DDL
  and a deterministic Go comparator judges equivalence — it no longer authors
  DDL. Cross-model review via `ai_review.model`; provider failure fails closed;
  deprecated `ai_verify` config keys warn at load.
- Migrated the lint config to golangci-lint v2 (now tracked in the repo).

### Removed

- AI-authored schema DDL paths and the validate-and-retry loop around them.
- Dead DMT-era AI tuning config and checkpoint tuning-history plumbing.

Releases before 0.10.0 are listed on the
[GitHub releases page](https://github.com/johndauphine/smt/releases). Full
history since v0.9.0:
[`v0.9.0...v0.10.0`](https://github.com/johndauphine/smt/compare/v0.9.0...v0.10.0).

[0.12.1]: https://github.com/johndauphine/smt/releases/tag/v0.12.1
[0.12.0]: https://github.com/johndauphine/smt/releases/tag/v0.12.0
[0.11.0]: https://github.com/johndauphine/smt/releases/tag/v0.11.0
[#141]: https://github.com/johndauphine/smt/issues/141
[#160]: https://github.com/johndauphine/smt/issues/160
[#121]: https://github.com/johndauphine/smt/issues/121
[#122]: https://github.com/johndauphine/smt/issues/122
[#123]: https://github.com/johndauphine/smt/issues/123
[0.10.1]: https://github.com/johndauphine/smt/releases/tag/v0.10.1
[0.10.0]: https://github.com/johndauphine/smt/releases/tag/v0.10.0
[#131]: https://github.com/johndauphine/smt/issues/131
[#133]: https://github.com/johndauphine/smt/pull/133
[#134]: https://github.com/johndauphine/smt/issues/134
[#156]: https://github.com/johndauphine/smt/issues/156
[#158]: https://github.com/johndauphine/smt/issues/158
[#159]: https://github.com/johndauphine/smt/issues/159
[#46]: https://github.com/johndauphine/smt/issues/46
[#57]: https://github.com/johndauphine/smt/issues/57
[#58]: https://github.com/johndauphine/smt/issues/58
[#62]: https://github.com/johndauphine/smt/issues/62
[#64]: https://github.com/johndauphine/smt/issues/64
[#69]: https://github.com/johndauphine/smt/issues/69
[#71]: https://github.com/johndauphine/smt/issues/71
[#126]: https://github.com/johndauphine/smt/pull/126
[#127]: https://github.com/johndauphine/smt/issues/127
[#168]: https://github.com/johndauphine/smt/issues/168
[#169]: https://github.com/johndauphine/smt/issues/169
[#170]: https://github.com/johndauphine/smt/issues/170
