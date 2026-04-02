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
   │  (wrapper)  │ │             │ │               │
   │ Reflect src │ │ Alembic API │ │ Schema DDL    │
   │ Backup, I/O │ │ init/create │ │ Table listing │
   └──────┬──────┘ │ apply/roll  │ │ Connection    │
          │        └──────┬──────┘ └───────┬───────┘
   ┌──────▼──────┐        │                │
   │sqlacodegen  │ ┌──────▼──────┐ ┌───────▼───────┐
   │  _smt/      │ │  Target DB  │ │  Target DB    │
   │SmtGenerator │ │  (alembic)  │ │  (DDL/query)  │
   │ Gen models/ │ └─────────────┘ └───────────────┘
   └──────┬──────┘
   ┌──────▼──────┐
   │  Source DB  │
   │  (reflect)  │
   └─────────────┘
```

### Module Dependency Graph

```
cli.py → config.py
cli.py → pipeline.py → models.py → sqlacodegen_smt.generator (SmtGenerator)
                                  → (SQLAlchemy MetaData.reflect)
                      → migration.py → (Alembic command API)
                      → database.py → (SQLAlchemy engine + text)
config.py → (PyYAML, SQLAlchemy URL)
sqlacodegen_smt → sqlacodegen (DeclarativeGenerator)
```

No circular dependencies. Each module has a single responsibility.

## Key Design Decisions

### 1. sqlacodegen Subclass Over Direct inspect()

**Decision**: Subclass sqlacodegen's `DeclarativeGenerator` as `SmtGenerator` rather than using `sqlalchemy.inspect()` directly.

**History**: SMT originally used a custom 637-line `ModelGenerator` that called `inspect()` directly with a hand-maintained type map. This was replaced with `SmtGenerator` (in `src/sqlacodegen_smt/generator.py`), which subclasses sqlacodegen's `DeclarativeGenerator` and overrides ~12 methods.

**Why subclass (not direct fork)**:
- sqlacodegen's `DeclarativeGenerator` has clean method boundaries — every SMT customization maps to an overridable method
- No code duplication — we don't maintain a copy of sqlacodegen's ~2000-line `generators.py`
- Upstream bug fixes and new SQLAlchemy version compatibility flow in automatically
- ~350 lines of overrides vs. 637 lines of custom reflection + code generation

**Why not automap**:
- Alembic's autogenerate requires actual model files on disk to import `Base.metadata`
- automap creates runtime classes, not files
- We need generated Python source code, not runtime objects

**Trade-off**: Coupling to sqlacodegen's internal API (method signatures, import/state management). Mitigated by pinning `>=4.0,<5.0` in pyproject.toml.

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

**Decision**: Every pipeline run reflects the source database via `MetaData.reflect()` and regenerates the `models/` package (one file per table) via `SmtGenerator`, even if it hasn't changed.

**Why**:
- The source database is the single source of truth
- Detecting "has the source changed?" requires reflecting anyway
- Alembic is the diff engine — it compares models against the target database
- If models haven't changed, Alembic produces an empty migration, which we detect and remove
- Previous models are backed up automatically (entire `models/` directory)

**Trade-off**: Marginally slower than caching, but eliminates an entire class of staleness bugs.

### 7. PascalCase Attributes, Lowercase DB Names

**Decision**: Python class attributes preserve the original source column names (e.g., `DisplayName`), while database identifiers are lowercased (e.g., `displayname`).

**Why**:
- Preserving source names makes model code recognizable to people who know the source schema
- Lowercase DB names are the PostgreSQL convention (unquoted identifiers are lowercased)
- The explicit column name in `mapped_column('displayname', ...)` makes the mapping clear
- This matches the behavior of the original bash scripts

### 8. No Workspace venv Management

**Decision**: The Python CLI is installed into the user's environment. The workspace directory only contains the `models/` package, Alembic config, migration files, and per-table DDL files.

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
   ├── MetaData.reflect() → populate Table/Column objects
   ├── Backup existing models/ directory (or legacy models.py)
   ├── SmtGenerator.generate() → dict[filename, content]:
   │   ├── fix_column_types() → adapt dialect types, strip collations
   │   ├── get_adapted_type() → MSSQL overrides + String fallback
   │   ├── generate_models() → ModelClass objects (no relationships)
   │   ├── Per table: render with lowercase identifiers, target schema, Identity()
   │   └── Per-file import tracking (save/restore self.imports)
   └── Write models/ package:
       ├── base.py (DeclarativeBase)
       ├── <table>.py per table (with per-file imports, headers)
       └── __init__.py (re-exports Base + all models)

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
   ├── Split DDL into per-table files in ddl/ directory
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
