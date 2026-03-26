# Design

Architecture decisions and the reasoning behind them.

## System Architecture

```
                    ┌─────────────┐
                    │   cli.py    │  Click CLI: parse args, setup logging
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │ pipeline.py │  Orchestrate steps, error handling, timing
                    └──────┬──────┘
                           │
          ┌────────────────┼────────────────┐
          │                │                │
   ┌──────▼──────┐ ┌──────▼──────┐ ┌───────▼───────┐
   │  models.py  │ │migration.py │ │  database.py  │
   │             │ │             │ │               │
   │ Reflect src │ │ Alembic API │ │ Schema DDL    │
   │ Generate .py│ │ init/create │ │ Table listing │
   │             │ │ apply/roll  │ │ Connection    │
   └──────┬──────┘ └──────┬──────┘ └───────┬───────┘
          │                │                │
   ┌──────▼──────┐ ┌──────▼──────┐ ┌───────▼───────┐
   │  Source DB  │ │  Target DB  │ │  Target DB    │
   │  (inspect)  │ │  (alembic)  │ │  (DDL/query)  │
   └─────────────┘ └─────────────┘ └───────────────┘
```

### Module Dependency Graph

```
cli.py → config.py
cli.py → pipeline.py → models.py → (SQLAlchemy inspect)
                      → migration.py → (Alembic command API)
                      → database.py → (SQLAlchemy engine + text)
config.py → (PyYAML, SQLAlchemy URL)
```

No circular dependencies. Each module has a single responsibility.

## Key Design Decisions

### 1. SQLAlchemy inspect() Over sqlacodegen

**Decision**: Replace sqlacodegen with direct use of `sqlalchemy.inspect()`.

**Alternatives considered**:
- Keep sqlacodegen as a dependency and shell out to it
- Use sqlacodegen's internal API (undocumented)
- Use SQLAlchemy's `automap_base()` for runtime models

**Why inspect()**:
- sqlacodegen is CLI-only with no stable Python API
- Calling it via subprocess reintroduces the shell dependency problem
- `inspect()` returns raw metadata dicts that we can map directly to the output we need
- We control the output format entirely, eliminating the 7-pass regex post-processing
- Collations are never emitted (vs. generating them then stripping)
- Type mapping is explicit and auditable (a dictionary, not implicit sqlacodegen behavior)

**Why not automap**:
- Alembic's autogenerate requires actual model files on disk to import `Base.metadata`
- automap creates runtime classes, not files
- We need generated Python source code, not runtime objects

### 2. Alembic Python API Over CLI

**Decision**: Drive Alembic through `alembic.command.*` and `alembic.config.Config`.

**Why**:
- Eliminates subprocess calls, PATH issues, and shell escaping
- `command.revision(autogenerate=True)` is a single function call
- DDL generation uses `config.output_buffer = StringIO()` to capture SQL in-process
- Error handling is native Python exceptions, not exit code parsing

**Trade-off**: Alembic's Python API is less documented than its CLI. We accept this because the API surface we use is small and stable (init, revision, upgrade, downgrade).

### 3. YAML Over .env for Configuration

**Decision**: Use YAML configuration with environment variable overrides for secrets.

**Alternatives considered**:
- Keep `config.env` (flat key-value)
- TOML (Python 3.11+ native)
- Pure environment variables

**Why YAML**:
- Nested structure naturally represents source/target databases
- Lists are native (tables), unlike .env where comma-separated strings need parsing
- Widely understood, good editor support
- PyYAML is a stable, lightweight dependency
- Environment variable overrides cover the CI/CD secret injection use case

**Why not TOML**: Requires Python 3.11+ for `tomllib`. Since we target 3.12+, TOML would work, but YAML was preferred for its wider adoption and simpler list syntax.

### 4. Dataclasses Over Pydantic

**Decision**: Use standard library dataclasses for configuration, not Pydantic.

**Why**:
- Fewer dependencies (Pydantic is a heavy transitive dependency tree)
- Config validation is simple enough for manual checks
- `URL.create()` handles the complex part (password encoding)
- Schema name validation is two regex checks
- We don't need Pydantic's serialization, JSON schema, or type coercion

### 5. Dialect Dispatch for Schema DDL

**Decision**: Use a dictionary mapping dialect names to DDL templates, with `str.format()` interpolation.

**Alternatives considered**:
- SQLAlchemy's `CreateSchema` DDL construct
- Subclass per dialect (PostgresManager, MssqlManager)

**Why dispatch dictionary**:
- Two dialects, two operations each = 4 DDL strings total
- A class hierarchy would be over-engineering for 4 strings
- The dictionary is readable, testable, and easy to extend
- Identifier validation before `format()` prevents injection

**Why not SQLAlchemy DDL constructs**: `CreateSchema` works for PostgreSQL but MSSQL's `IF NOT EXISTS` + `EXEC` pattern has no SQLAlchemy equivalent. Raw SQL with validation is the pragmatic choice.

### 6. Models Always Regenerated

**Decision**: Every pipeline run reflects the source database and regenerates `models.py`, even if it hasn't changed.

**Why**:
- The source database is the single source of truth
- Detecting "has the source changed?" requires reflecting anyway
- Alembic is the diff engine — it compares models against the target database
- If models haven't changed, Alembic produces an empty migration, which we detect and remove
- Previous models are backed up automatically

**Trade-off**: Marginally slower than caching, but eliminates an entire class of staleness bugs.

### 7. PascalCase Attributes, Lowercase DB Names

**Decision**: Python class attributes preserve the original source column names (e.g., `DisplayName`), while database identifiers are lowercased (e.g., `displayname`).

**Why**:
- Preserving source names makes model code recognizable to people who know the source schema
- Lowercase DB names are the PostgreSQL convention (unquoted identifiers are lowercased)
- The explicit column name in `mapped_column('displayname', ...)` makes the mapping clear
- This matches the behavior of the original bash scripts

### 8. No Workspace venv Management

**Decision**: The Python CLI is installed into the user's environment. The workspace directory only contains models.py, Alembic config, and migration files.

**Why**:
- The bash scripts created a venv per workspace and installed sqlacodegen/alembic into it
- This was necessary because the scripts had no packaging
- As a pip-installable package, SMT's dependencies are managed by the package manager
- Eliminating the venv step simplifies the pipeline and removes `pip install` from the critical path

## Data Flow

### Full Pipeline (`smt migrate`)

```
1. Load smt.yaml
   ├── Parse YAML
   ├── Apply env var overrides (SMT_SOURCE_PASSWORD, SMT_TARGET_PASSWORD)
   ├── Validate schema name chars + length
   └── Build SQLAlchemy URLs

2. Generate models (Step 1)
   ├── Connect to source database
   ├── inspect() → get_table_names, get_columns, get_pk_constraint, get_foreign_keys
   ├── For each table:
   │   ├── Map column types (reflected → generic SA types)
   │   ├── Log collation warnings
   │   ├── Lowercase all DB identifiers
   │   └── Rewrite FK references to target schema
   ├── Generate imports, Base class, table classes
   ├── Backup existing models.py
   └── Write new models.py with header

3. Initialize Alembic (Step 2)
   ├── Create workspace directory
   ├── alembic.command.init() (if not exists)
   ├── Write env.py from template
   ├── Update alembic.ini with target URL
   └── CREATE SCHEMA IF NOT EXISTS on target DB

4. Create migration (Step 3)
   ├── Check current revision vs head
   ├── Apply pending migrations if behind
   ├── alembic.command.revision(autogenerate=True)
   ├── Check if migration is empty → remove if so
   ├── Strip collation parameters from migration file
   └── Log operation counts

5. Apply migration (Step 4)
   ├── Check if already at head → skip if so
   ├── Generate DDL SQL file (offline mode)
   ├── alembic.command.upgrade("head")
   └── List tables in target schema for verification
```

## Error Handling Strategy

| Layer | Error Type | Behavior |
|-------|-----------|----------|
| Config | `ConfigError` | CLI exits with message, no stack trace |
| Database | `DatabaseError` | Pipeline reports step number + recovery command |
| Migration | Alembic exceptions | Pipeline catches, wraps with step context |
| Pipeline | `PipelineError` | CLI shows step, cause, retry command, rollback hint |
| CLI | `SystemExit(1)` | Clean exit with error message |

## Security Considerations

- **Passwords**: Never logged. URLs use `hide_password=True` by default; only `get_url_string()` exposes them (for alembic.ini, which is in .gitignore).
- **Identifier injection**: Schema names validated against `[A-Za-z0-9_]+` before any DDL interpolation.
- **Config files**: `smt.yaml` is in `.gitignore` to prevent credential commits.
- **Environment variables**: Preferred over YAML for passwords in CI/CD.
