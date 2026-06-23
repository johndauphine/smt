# SMT CLI Reference

This is the v1 CLI surface contract. Items marked `stable` will not be removed
or renamed in 1.x without a deprecation cycle. Items marked `experimental` may
change in any 1.x release.

## Global Flags

| flag | status | description |
|---|---|---|
| `--config`, `-c` | stable | Path to `config.yaml`. |
| `--profile` | stable | Profile name stored in SQLite; overrides `--config`. |
| `--state-file` | stable | Use a YAML state file instead of SQLite for headless runs. |
| `--log-format` | stable | `text` or `json`. |
| `--verbosity` | stable | `debug`, `info`, `warn`, or `error`. |
| `--shutdown-timeout` | stable | Grace period for signal-triggered shutdown. |
| `--help`, `-h` | stable | Show help. |
| `--version`, `-v` | stable | Show the binary version. |

## Commands

| command | status | description |
|---|---|---|
| `smt init` | stable | Create a `config.yaml` with the guided wizard. |
| `smt create` | stable | Extract source schema and generate matching target DDL. |
| `smt sync` | stable | Diff source schema against the target and optionally apply ALTERs. |
| `smt drift` | stable | Report read-only drift between desired and live target schema. |
| `smt snapshot` | stable | Capture the current source schema as a sync baseline. |
| `smt snapshot list`, `smt snapshot ls` | stable | List stored source-schema snapshots. |
| `smt health-check` | stable | Test configured database connections. |
| `smt profile save` | stable | Save an encrypted profile from a config file. |
| `smt profile list` | stable | List saved encrypted profiles. |
| `smt profile delete` | stable | Delete a saved encrypted profile. |
| `smt profile export` | stable | Export a saved profile to a config file. |
| `smt init-secrets` | stable | Create the global secrets-file template. |
| `smt history` | stable | List schema operation history or show one run. |

## Command Flags

### `smt init`

| flag | status | description |
|---|---|---|
| `--out`, `-o` | stable | Output path; default `config.yaml`. |
| `--force`, `-f` | stable | Overwrite an existing file. |
| `--print` | stable | Write config YAML to stdout instead of a file. |
| `--non-interactive`, `-y` | stable | Do not prompt; use flags and defaults. |
| `--health-check` | stable | Test connections after writing. |
| `--save-profile` | stable | Save the result as an encrypted profile. |
| `--source.type` | stable | Source engine. |
| `--source.host` | stable | Source host. |
| `--source.port` | stable | Source port. |
| `--source.database` | stable | Source database. |
| `--source.user` | stable | Source user. |
| `--source.password_mode` | stable | Source password storage mode. |
| `--source.password` | stable | Source password value or reference. |
| `--source.schema` | stable | Source schema. |
| `--target.type` | stable | Target engine. |
| `--target.schema` | stable | Target schema. |
| `--target.configure` | stable | Whether to include target connection fields. |
| `--target.host` | stable | Target host. |
| `--target.port` | stable | Target port. |
| `--target.database` | stable | Target database. |
| `--target.user` | stable | Target user. |
| `--target.password_mode` | stable | Target password storage mode. |
| `--target.password` | stable | Target password value or reference. |
| `--unknown_type_policy` | stable | Unknown source type policy. |
| `--ai_review` | stable | Enable optional AI review. |
| `--ai_review.mode` | stable | AI review mode. |
| `--ai_review.model` | stable | Secrets-file provider entry for AI review. |
| `--ai_review.diagnose_failures` | stable | Enable AI failure diagnosis. |
| `--ai_review.suggest_fixes` | stable | Enable AI expression-fix suggestions. |
| `--migration` | stable | Include migration override fields. |
| `--migration.include_tables` | stable | Include-table globs. |
| `--migration.exclude_tables` | stable | Exclude-table globs. |
| `--migration.create_indexes` | stable | Create non-PK indexes. |
| `--migration.create_foreign_keys` | stable | Create foreign keys. |
| `--migration.create_check_constraints` | stable | Create CHECK constraints. |
| `--slack` | stable | Enable Slack notifications in config. |
| `--slack.webhook_var` | stable | Environment variable holding the Slack webhook URL. |
| `--slack.channel` | stable | Slack channel. |
| `--slack.username` | stable | Slack username. |
| `--profile.name` | stable | Profile name in generated config. |
| `--profile.description` | stable | Profile description in generated config. |

### `smt create`

| flag | status | description |
|---|---|---|
| `--apply` | stable | Execute generated DDL against the target. |
| `--out`, `-o` | stable | Output file when not applying; default `schema.sql`. |
| `--source-schema` | stable | Override source schema from config. |
| `--target-schema` | stable | Override target schema from config. |
| `--apply-suggested` | experimental | Splice a single AI-translated expression fix into the plan and continue. |

### `smt sync`

| flag | status | description |
|---|---|---|
| `--apply` | stable | Execute ALTERs against the target. |
| `--out`, `-o` | stable | Output file when not applying; default `migration.sql`. |
| `--allow-data-loss` | stable | Permit data-loss-risk statements while applying. |
| `--save-snapshot` | stable | Save the source schema as the next baseline after successful sync. |

### `smt drift`

| flag | status | description |
|---|---|---|
| `--fail-on-destructive-only` | stable | Exit non-zero only for destructive drift. |

### `smt snapshot`

| flag | status | description |
|---|---|---|
| `--out`, `-o` | stable | Also write snapshot JSON to this path. |

### `smt snapshot list`

| flag | status | description |
|---|---|---|
| `--limit`, `-n` | stable | Maximum snapshots to show; default `50`. |

### `smt profile`

| subcommand | flag | status | description |
|---|---|---|---|
| `save` | `--name`, `-n` | stable | Profile name; defaults to config filename. |
| `delete` | `--name`, `-n` | stable | Profile name to delete. |
| `export` | `--name`, `-n` | stable | Profile name to export. |
| `export` | `--out`, `-o` | stable | Output path; default `config.yaml`. |

### `smt init-secrets`

| flag | status | description |
|---|---|---|
| `--force`, `-f` | stable | Overwrite an existing secrets file. |

### `smt health-check` and `smt history`

| command | flag | status | description |
|---|---|---|---|
| `health-check` | none | stable | Uses global flags only. |
| `history` | `--run` | stable | Show details for a specific run ID. |

## Exit Codes

| code | meaning |
|---:|---|
| `0` | Success. |
| `1` | Configuration, YAML, JSON, or command-line contract error. |
| `2` | Connection, authentication, pool, DNS, or network error. |
| `3` | Schema operation failed. For `smt drift`, `3` specifically means drift was detected. |
| `4` | Validation error. |
| `5` | Cancelled by signal or context cancellation. |
| `6` | State DB, checkpoint, profile, or run-history error. |
| `7` | File I/O error. |

`smt drift` is the only stable command that uses a non-error status for a
domain result: `0` means no drift, `3` means drift detected. Other commands map
errors through the shared exit-code classifier above.
