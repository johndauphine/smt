# SMT - Schema Migration Toolkit

Migrates database schemas between PostgreSQL and MSSQL databases using SQLAlchemy and Alembic. Installable Python CLI with YAML configuration.

## Features

- **Multi-database**: PostgreSQL and MSSQL as source or target
- **Automatic model generation**: Reflects source schema using a sqlacodegen-based generator (`SmtGenerator` subclass)
- **Incremental migrations**: Alembic detects and migrates only what changed
- **Lowercase normalization**: All database identifiers lowercased in target
- **Foreign key preservation**: FK relationships transformed to target schema
- **Collation handling**: Source collations logged as warnings, not emitted in target
- **DBA-friendly**: `--dry-run` generates DDL SQL for review without applying
- **Idempotent**: Re-running reports "Already in sync" when nothing changed
- **Rollback support**: Revert to any revision or drop the target schema entirely

## Quick Start

```bash
# Install
pip install -e ".[postgres]"       # for PostgreSQL
pip install -e ".[mssql]"          # for MSSQL (requires ODBC Driver 18)
pip install -e ".[postgres,mssql]" # for both

# Configure
cp config.example.yaml smt.yaml
# Edit smt.yaml with your database credentials

# Migrate
smt -c smt.yaml migrate
```

## CLI Commands

```bash
smt -c smt.yaml migrate [--yes]           # Full pipeline (generate -> init -> create -> apply)
smt -c smt.yaml generate                  # Reflect source -> models/ package
smt -c smt.yaml init                      # Initialize Alembic + create target schema
smt -c smt.yaml create [-m "message"]     # Autogenerate migration
smt -c smt.yaml apply [--dry-run]         # Apply migration (or just generate DDL)
smt -c smt.yaml rollback [-n 1]           # Rollback one revision
smt -c smt.yaml rollback --drop-schema    # Rollback all + drop target schema
smt -c smt.yaml status                    # Current revision + tables
smt -c smt.yaml history                   # Migration history
```

Global options: `--log-level DEBUG|INFO|WARNING|ERROR` (DEBUG shows SQL queries).

## Configuration

```yaml
source:
  dialect: mssql                   # postgresql | mssql
  host: localhost
  port: 1433
  user: sa
  password: YourPassword           # or set SMT_SOURCE_PASSWORD env var
  database: StackOverflow2010
  schema: dbo

target:
  dialect: postgresql
  host: localhost
  port: 5432
  user: postgres
  password: YourPassword           # or set SMT_TARGET_PASSWORD env var
  database: stackoverflow

tables:                            # list of tables, or "all"
  - Users
  - Posts
  - Comments

workspace: ./migration_workspace   # where artifacts are generated
```

**Target schema naming**: Automatically derived as `dw__<source_database>__<source_schema>` (lowercase).
Example: `StackOverflow2010.dbo` becomes `dw__stackoverflow2010__dbo`.

**Environment variables**: `SMT_SOURCE_PASSWORD` and `SMT_TARGET_PASSWORD` override YAML passwords for CI/CD.

## Identifier Transformations

| Source (MSSQL/PostgreSQL) | Target |
|---------------------------|--------|
| Schema `dbo` | Schema `dw__<database>__<schema>` |
| Table `Users` | Table `users` |
| Column `DisplayName` | Column `displayname` |
| Constraint `PK_Users` | Constraint `pk_users` |
| FK `dbo.Users.Id` | FK `dw__<db>__<schema>.users.id` |

Python attribute names preserve the original casing (e.g., `Users.DisplayName`) while all database identifiers are lowercase.

## Schema Change Detection

| Scenario | Behavior |
|----------|----------|
| No changes | Models regenerated, empty migration removed, "Already in sync" |
| New table | Migration creates the table |
| New column | Migration adds the column |
| Dropped column | Migration removes the column |
| Type change | Migration alters the column type |

Models are always regenerated from source. Alembic compares models against the target and creates incremental migrations.

## Generated Artifacts

After migration, the workspace contains:

```
migration_workspace/
├── models/                # SQLAlchemy 2.0 models package (one file per table)
│   ├── __init__.py        # Re-exports Base and all model classes
│   ├── base.py            # Base(DeclarativeBase) class
│   ├── users.py           # Per-table model file
│   └── posts.py           # Per-table model file
├── models_*.bak/          # Backups of previous models package
├── ddl/                   # Per-table DDL files
│   ├── users.sql          # DDL for users table
│   └── posts.sql          # DDL for posts table
├── migration_*.sql        # Full DDL snapshot for review
├── alembic.ini            # Target DB connection
└── alembic/
    ├── env.py             # Imports Base from models package
    └── versions/          # Migration files
```

## Docker Setup (MSSQL to PostgreSQL)

```bash
# Start databases
docker run -d --name smt-mssql \
  -e 'ACCEPT_EULA=Y' -e 'MSSQL_SA_PASSWORD=SmtTestPass1' \
  -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest

docker run -d --name smt-postgres \
  -e 'POSTGRES_PASSWORD=SmtTestPass1' \
  -p 5433:5432 postgres:15

# Create target database
docker exec smt-postgres psql -U postgres -c "CREATE DATABASE stackoverflow;"

# Create smt.yaml pointing source at MSSQL:1433, target at PG:5433
# Then run migration
smt -c smt.yaml migrate --yes

# Verify
docker exec smt-postgres psql -U postgres -d stackoverflow \
  -c "\dt dw__stackoverflow2010__dbo.*"

# Cleanup
docker stop smt-mssql smt-postgres && docker rm smt-mssql smt-postgres
```

## Prerequisites

- Python 3.12+
- PostgreSQL driver: `pip install smt[postgres]` (installs psycopg2-binary)
- MSSQL driver: `pip install smt[mssql]` (installs pyodbc; requires [Microsoft ODBC Driver 18](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server) on the system)

## Development

```bash
# Setup
uv venv --python 3.12 .venv
uv pip install -e ".[dev,postgres,mssql]"

# Test (no database required, ~86 tests)
.venv/bin/pytest tests/ -v

# Lint
.venv/bin/ruff check src/ tests/
```

## Architecture

SMT delegates model generation to `sqlacodegen_smt`, a subclass of sqlacodegen's `DeclarativeGenerator`:

```
Source DB → MetaData.reflect() → SmtGenerator → models/ package
                                                     ↓
Target DB ← alembic upgrade ← migration ← alembic autogenerate (diff models vs target)
```

`SmtGenerator` overrides ~12 methods to produce SMT-compatible output (lowercase identifiers, target schema rewriting, per-table files, MSSQL type mapping, etc.). sqlacodegen is a pip dependency, not a fork — upstream fixes flow in automatically.

See `src/sqlacodegen_smt/generator.py` for the implementation.

## Documentation

- [Design](docs/DESIGN.md) - Architecture and design decisions
- [Technical Specification](docs/TECH_SPEC.md) - Module API, type mappings, config schema
- [Philosophy](docs/PHILOSOPHY.md) - Why this approach, guiding principles
- [sqlacodegen Fork Spec](docs/SQLACODEGEN_FORK.md) - Original specification and implementation status

## Legacy

The original bash scripts remain in `scripts/` for reference. They are not used by the Python CLI.
