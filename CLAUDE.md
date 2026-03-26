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
smt generate -c smt.yaml             # Reflect source -> models.py
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
.venv/bin/pytest tests/ -v

# Lint
.venv/bin/ruff check src/ tests/
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

### Package Structure (`src/smt/`)

```
cli.py          Click CLI entry point (group + subcommands)
config.py       YAML loading, DatabaseConfig/SmtConfig dataclasses, URL building, validation
database.py     DatabaseManager: engine creation, dialect-specific schema DDL, table listing
models.py       ModelGenerator: SQLAlchemy inspect() -> model code generation (replaces sqlacodegen)
migration.py    MigrationManager: Alembic programmatic API wrapper
pipeline.py     Pipeline orchestrator (composes the above)
templates/
  env.py.mako   Alembic env.py template (imports Base from models.py)
```

### Pipeline Flow

```
Source DB -> inspect() -> ModelGenerator -> models.py (with lowercase transforms baked in)
                                                |
Target DB <- alembic upgrade <- migration file <- alembic autogenerate (diff models vs target) <-+
```

### Key Module Details

**`models.py`** (core module): Replaces sqlacodegen + regex post-processing. Uses `sqlalchemy.inspect()` to reflect columns, PKs, FKs per table. Generates SQLAlchemy 2.0 declarative code directly with correct naming:
- `__tablename__` = lowercase, column DB names = lowercase, constraint names = lowercase
- Python attribute names = original PascalCase from source
- FK references rewritten to `target_schema.table.col`
- Collations detected during reflection are logged as warnings and skipped
- Unrecognized dialect-specific types fall back to `String` with a warning
- Backs up previous models.py as `models_<timestamp>.py.bak`

**`migration.py`**: Drives Alembic via `alembic.config.Config` + `alembic.command.*` (no subprocess). Handles:
- Init: `command.init()`, writes env.py from template, updates alembic.ini URL
- Create: applies pending migrations first, `command.revision(autogenerate=True)`, removes empty migrations, strips collation params
- Apply: checks if at head, generates DDL SQL file, `command.upgrade("head")`. Supports `--dry-run`.
- Rollback: `command.downgrade(target)` where target is "base", "-N", or revision hash

**`database.py`**: Dialect-specific DDL dispatch for schema creation/drop. Schema names validated against `[A-Za-z0-9_]+` before DDL interpolation.
- PostgreSQL: `CREATE SCHEMA IF NOT EXISTS` / `DROP SCHEMA IF EXISTS ... CASCADE`
- MSSQL: `IF NOT EXISTS (SELECT FROM sys.schemas ...) EXEC('CREATE SCHEMA ...')` / `IF EXISTS ... EXEC('DROP SCHEMA ...')`

**`config.py`**: Loads YAML, applies env var overrides for passwords, validates schema name characters (`[A-Za-z0-9_]+`) and length against dialect limits, builds SQLAlchemy `URL.create()` (handles password encoding). MSSQL+pyodbc URLs automatically include ODBC Driver 18, TrustServerCertificate, and Encrypt params. `get_url_string()` renders the URL with password visible (for alembic.ini).

### Design Decisions

- **No sqlacodegen dependency** — replaced with SQLAlchemy `inspect()` + custom code generator
- **No subprocess calls** — Alembic and DB operations are all Python API
- **No venv management** — tool is pip-installed; workspace only has models.py + alembic artifacts
- **Idempotent pipeline** — running twice with no source changes reports "Already in sync"
- **PascalCase Python attributes, lowercase DB identifiers** — `Users.DisplayName` -> column `displayname`
- **env.py uses `include_schemas=True`** — required for multi-schema Alembic support
- **env.py rewritten on every init** — ensures template is current
- **Identifier validation** — schema names checked against `[A-Za-z0-9_]+` before DDL interpolation
- **Duplicate handler guard** — `_setup_logging()` checks `root.handlers` to avoid duplicate log output

### Generated Artifacts (in workspace directory)

- `models.py` — SQLAlchemy 2.0 models with timestamp header
- `models_*.py.bak` — Backups of previous models
- `migration_*.sql` — DDL snapshots for review
- `alembic/versions/` — Migration files
- `alembic.ini` — Target DB URL (%-escaped)

## Testing

```bash
.venv/bin/pytest tests/ -v          # All tests (56 tests, no DB required)
.venv/bin/pytest tests/test_config.py -v    # Config module only
.venv/bin/pytest tests/test_models.py -v    # Model generator only
```

Tests use mocked SQLAlchemy inspectors — no database connection needed.

## Prerequisites

- Python 3.12+
- Database drivers: `pip install smt[postgres]` for PostgreSQL, `pip install smt[mssql]` for MSSQL
- MSSQL requires Microsoft ODBC Driver for SQL Server installed on the system (for pyodbc)

## Documentation

- `docs/SETUP.md` — Installation, ODBC driver setup, Docker testing, troubleshooting
- `docs/DESIGN.md` — Architecture, module dependencies, design decisions with rationale
- `docs/TECH_SPEC.md` — Module API, config schema, type mappings, Alembic integration details
- `docs/PHILOSOPHY.md` — Guiding principles, what SMT is and is not

## Legacy Bash Scripts

The original bash scripts remain in `scripts/` for reference. They are not used by the Python CLI.
