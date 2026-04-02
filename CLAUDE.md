# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Purpose

Schema migration toolkit supporting PostgreSQL and MSSQL. Migrates schemas between databases using SQLAlchemy reflection + Alembic migrations, with automatic lowercase naming and derived target schema names.

## Common Commands

```bash
# Install (requires Python 3.12+)
uv venv --python 3.12 .venv && uv pip install -e ".[dev,postgres]"

# Full migration pipeline
smt migrate -c smt.yaml              # Interactive (prompts for confirmation)
smt migrate -c smt.yaml --yes        # Non-interactive

# Individual steps (must run in order on first use)
smt generate -c smt.yaml             # Reflect source -> models/ package
smt init -c smt.yaml                 # Init Alembic + create target schema
smt create -c smt.yaml               # Autogenerate migration (removes empty ones)
smt apply -c smt.yaml                # Generate DDL, apply, verify tables
smt apply -c smt.yaml --dry-run      # Generate DDL only, don't apply

# Rollback
smt rollback -c smt.yaml             # Rollback all (to base)
smt rollback -c smt.yaml -n 1        # Rollback one revision
smt rollback -c smt.yaml --drop-schema  # Rollback all + drop target schema

# Status and history
smt status -c smt.yaml               # Current revision, tables
smt history -c smt.yaml              # Migration history

# Run tests
.venv/bin/pytest tests/ -v                         # All tests (~86, no DB required)
.venv/bin/pytest tests/test_models.py -v           # Single test file
.venv/bin/pytest tests/test_config.py::test_name -v  # Single test function

# Lint
.venv/bin/ruff check src/ tests/        # Check only
.venv/bin/ruff check --fix src/ tests/  # Auto-fix
```

## Configuration

Copy `config.example.yaml` to `smt.yaml` and edit. Key fields:

```yaml
source:
  dialect: postgresql | mssql
  host, port, user, password, database, schema
target:
  dialect: postgresql | mssql
  host, port, user, password, database
tables: [Users, Posts] or "all"
workspace: ./migration_workspace
```

- **Target schema**: Auto-derived as `dw__{source.database}__{source.schema}` (lowercase)
- **Env var overrides**: `SMT_SOURCE_PASSWORD` and `SMT_TARGET_PASSWORD` override YAML passwords
- **Schema name validation**: Characters (`[A-Za-z0-9_]+`) and length (PG: 63, MSSQL: 128) validated at load time
- **Default ports**: postgresql=5432, mssql=1433
- **Default drivers**: postgresql=psycopg2, mssql=pyodbc

## Architecture

### Package Structure (`src/smt/` + `src/sqlacodegen_smt/`)

```
smt/
  cli.py          Click CLI entry point (group + subcommands)
  config.py       YAML loading, DatabaseConfig/SmtConfig dataclasses, URL building, validation
  database.py     DatabaseManager: engine creation, dialect-specific schema DDL, table listing
  models.py       ModelGenerator: thin wrapper around SmtGenerator (backup, reflection, file I/O)
  migration.py    MigrationManager: Alembic programmatic API wrapper
  pipeline.py     Pipeline orchestrator (composes the above)
  templates/
    env.py.mako   Alembic env.py template (imports Base from models package)

sqlacodegen_smt/
  __init__.py
  generator.py    SmtGenerator(DeclarativeGenerator): sqlacodegen fork with SMT customizations
```

Entry point: `smt = "smt.cli:cli"` (defined in pyproject.toml). Ruff config: line-length 100, target py312.

### Pipeline Flow

```
Source DB -> MetaData.reflect() -> SmtGenerator -> models/ package (one .py per table, lowercase transforms)
                                                        |
Target DB <- alembic upgrade <- migration file <- alembic autogenerate (diff models vs target) <-+
                                    |
                              ddl/<table>.sql (per-table DDL snapshots)
```

### Key Module Details

**`models.py`** (wrapper): Thin wrapper around `SmtGenerator`. Handles `MetaData.reflect()`, backup of existing output, and file I/O. The `ModelGenerator` interface (`__init__` + `write()`) is unchanged from the pipeline's perspective.

**`sqlacodegen_smt/generator.py`** (core): `SmtGenerator` subclasses sqlacodegen's `DeclarativeGenerator` with 11 customizations:
- **Multi-file output**: `generate()` returns `dict[str, str]` (filename -> content) for a models/ package.
- **Naming**: `__tablename__` = lowercase, column DB names = lowercase, constraint names = lowercase. Python attribute names preserve original PascalCase.
- **Target schema rewriting**: `__table_args__` and FK refs use the derived target schema.
- **Collation stripping**: Detected, warned, stripped from reflected types before rendering.
- **Type fallback**: MSSQL types mapped via `_MSSQL_TYPE_OVERRIDES`; remaining dialect types fall back to `String`.
- **Identity()**: Autoincrement PKs emit `Identity()`.
- **Keyword escaping**: Python reserved words get `_` suffix in attribute names.
- **No relationships**: `generate_relationships()` returns empty list.
- **File headers**: Each table file has a metadata header (timestamp, source, target).

**`migration.py`**: Drives Alembic via `alembic.config.Config` + `alembic.command.*` (no subprocess).
- **Empty migration detection**: Regex `_OP_PATTERNS` searches for actual operations (create_table, add_column, etc.); empty migrations are auto-removed.
- **DDL splitting**: `_split_ddl_by_table()` uses regex to extract table names from CREATE TABLE, CREATE INDEX, ALTER TABLE statements (handles unquoted, double-quoted, and bracket-quoted identifiers) to generate per-table `ddl/<table>.sql` files.
- **DDL generation uses Alembic offline mode**: `command.upgrade(sql=True)` with `config.output_buffer = StringIO()` to capture SQL without applying.

**`database.py`**: Dialect-specific DDL dispatch for schema creation/drop. Schema names validated against `[A-Za-z0-9_]+` regex before DDL string interpolation (prevents injection).

**`config.py`**: Loads YAML, validates, builds `URL.create()`. MSSQL+pyodbc URLs automatically include ODBC Driver 18, TrustServerCertificate, and Encrypt query params. `get_url_string()` renders URL with visible password (for alembic.ini).

**`pipeline.py`**: Orchestrates generate->init->create->apply. `PipelineError` includes step number and suggests the recovery command (e.g., "resume from step 3 with `smt create`").

### Design Decisions

- **sqlacodegen fork** — `sqlacodegen_smt` subclasses `DeclarativeGenerator` (not a direct fork; upstream as dependency)
- **No subprocess calls** — Alembic and DB operations are all Python API
- **No venv management** — tool is pip-installed; workspace only has models/ package + alembic artifacts
- **Idempotent pipeline** — running twice with no source changes reports "Already in sync"
- **Models always regenerated** — source DB is single source of truth; Alembic diffs handle the rest
- **env.py uses `include_schemas=True`** — required for multi-schema Alembic support
- **env.py rewritten on every init** — ensures template is current
- **Duplicate handler guard** — `_setup_logging()` checks `root.handlers` to avoid duplicate log output

## Testing

SmtGenerator tests use in-memory SQLite with real SQLAlchemy Table objects. ModelGenerator wrapper tests mock `MetaData.reflect()`. No external database connection needed.

**Key fixtures** (in `conftest.py`):
- `tmp_workspace` — temp directory with `migration_workspace/` subdir
- `sample_config_yaml` — writes a sample `smt.yaml` with 2 tables (Users, Posts)

**Test files map to source modules**: `test_config.py`, `test_models.py`, `test_migration.py`, `test_database.py`, `test_cli.py`, `test_sqlacodegen_smt.py`.

**Mocking pattern**: SmtGenerator tests create real `Table`/`Column` objects in a `MetaData` and pass to the generator with an in-memory SQLite engine. ModelGenerator wrapper tests patch `MetaData.reflect()` to inject table definitions. See `test_sqlacodegen_smt.py` and `test_models.py`.

## Legacy Bash Scripts

The original bash scripts remain in `scripts/` for reference. They are not used by the Python CLI.
