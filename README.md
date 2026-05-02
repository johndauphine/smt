# smt — Schema Migration Tool

SMT extracts the schema of a source database, builds matching DDL on a target database, and applies incremental schema changes (ALTER TABLE / CREATE INDEX / ...) detected by diffing the current source schema against a stored snapshot. The diff is rendered to dialect-appropriate SQL by an AI provider.

SMT is the schema-only counterpart to [DMT](https://github.com/johndauphine/dmt) (the data migration tool): same pluggable driver model, same AI-assisted type mapping, same TUI scaffolding, same encrypted profile storage. It does not move rows.

## Supported databases

PostgreSQL, SQL Server, MySQL — both as source and target. New engines are added by dropping a package under `internal/driver/foo/` that implements `driver.Driver`/`Reader`/`Writer` and registers itself in `init()`.

## Quick start

```bash
make build
./smt init-secrets                  # create ~/.secrets/smt-config.yaml; add your AI key
cp config.yaml.example config.yaml  # edit source/target connection
./smt health-check
./smt create                        # build target schema from source
./smt snapshot                      # capture source schema as a baseline
# ...time passes, source schema evolves...
./smt sync                          # diff vs snapshot, AI renders ALTERs to migration.sql
./smt sync --apply                  # execute the ALTERs against the target
```

Run `./smt` with no arguments to launch the TUI. See `./smt --help` for the full command list.

## Commands

| command | what it does |
|---------|--------------|
| `smt create` | extract source schema, run CREATE TABLE / CREATE INDEX / etc on target |
| `smt snapshot` | save the current source schema as a baseline for future diffing |
| `smt sync` | diff source against last snapshot; AI generates target-dialect SQL; emit to `migration.sql` for review |
| `smt sync --apply` | also execute the generated SQL against the target |
| `smt sync --apply --allow-data-loss` | permit column/table drops |
| `smt sync --apply --save-snapshot` | save the new schema as the next baseline after success |
| `smt validate` | (planned) compare source vs target, report drift |
| `smt analyze` | (planned) AI suggestions for schema-relevant config |
| `smt health-check` | ping both databases, count source tables |
| `smt history` | list past schema runs |
| `smt profile {save,list,delete,export}` | encrypted profile storage |
| `smt init` / `smt init-secrets` | create the config / secrets files |

## AI configuration

SMT relies on an AI provider for type mapping and for SQL rendering in `sync`. Configure one in `~/.secrets/smt-config.yaml` (run `smt init-secrets` for the template):

```yaml
ai:
  provider: anthropic           # anthropic | openai | gemini | ollama | lmstudio
  api_key: ${env:ANTHROPIC_API_KEY}
  model: claude-haiku-4-5-20251001
encryption:
  master_key: ""                # openssl rand -base64 32, used for profile encryption
notifications:
  slack:
    webhook_url: ""
```

The same secrets file is read by both the CLI and the TUI. File mode 0600 is enforced.

## Philosophy

SMT's job is to give the AI the context it needs and execute what comes back. Translation, generation, and judgment belong to the AI — type mapping, ALTER statement generation, risk classification, and error diagnosis all flow through the configured provider. SMT itself does the deterministic parts: introspect schemas, compute structural diffs, run SQL.

## How `sync` works

1. `smt snapshot` extracts the current source schema (tables + columns + indexes + FKs + check constraints) and stores the JSON in the SQLite state DB at `~/.smt/state.db`.
2. `smt sync` extracts the current source schema again, loads the latest snapshot, and computes a structural diff (added / removed / changed tables and per-table column/index/FK/check deltas).
3. The whole structural diff is sent to the configured AI provider in one prompt. The AI returns a JSON list of statements, each containing the SQL text, a one-line description, and a risk classification: `safe`, `blocking`, `rebuild`, or `data-loss-risk`.
4. By default the SQL is written to `migration.sql` for review. With `--apply` the statements are executed against the target in order. `data-loss-risk` statements (column drops, table drops) are refused unless `--allow-data-loss` is also passed.

There is no hand-coded ALTER syntax table — the AI is the renderer. This keeps the supported-dialect surface as wide as the AI provider's training data.

## Configuration

`config.yaml` (per-migration):

```yaml
source: { type, host, port, database, user, password, schema }
target: { type, host, port, database, user, password, schema }
migration:
  include_tables: []           # optional glob patterns
  exclude_tables: ["__*", "temp_*"]
  create_indexes: true
  create_foreign_keys: true
  create_check_constraints: true
  data_dir: ~/.smt
slack: { ... }                 # optional
```

Passwords support `${env:VAR}`, `${file:/path}`, and literal forms.

`~/.secrets/smt-config.yaml` (global): AI provider keys, profile encryption master key, Slack webhook. Never put these in `config.yaml`.

## Heritage

SMT started as a schema-feature carve-out from DMT. The driver layer, AI plumbing (multi-provider HTTP for Anthropic / OpenAI / Gemini / Ollama / LM Studio), encrypted profile storage, setup wizard, and Bubble Tea TUI all carry over. The data-transfer machinery (parallel workers, chunking, write-ahead writers, runtime AI tuning) is gone. The schema-diff + AI-rendered ALTER feature is new in SMT.

## License

MIT.
