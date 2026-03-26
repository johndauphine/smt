# Philosophy

The guiding principles behind SMT's design.

## Why Schema Migration?

Operational databases evolve constantly — tables are added, columns renamed, types changed. When downstream systems (data warehouses, analytics platforms, dev environments) depend on that schema, they need a reliable way to stay in sync.

Manual DDL scripts are error-prone, pg_dump is all-or-nothing, and GUI tools aren't repeatable. SMT exists to make schema migration automated, version-controlled, and safe.

## Core Principles

### 1. Reflect, Don't Guess

SMT reflects the source database at runtime using SQLAlchemy's `inspect()` API. It never asks you to hand-write models or maintain a separate schema definition. The source database is always the single source of truth.

This means:
- No schema drift between what you think exists and what actually exists
- A 50-column table takes the same effort as a 2-column table
- New columns, dropped columns, and type changes are detected automatically

### 2. Transform During Generation, Not After

The original bash implementation generated models with source naming, then applied 7 regex passes to lowercase everything and rewrite schema references. This was fragile — each regex assumed a specific code shape that could break with sqlacodegen version changes.

SMT generates the correct output on the first pass. Lowercase names, target schema references, and collation omissions are baked into the code generator. There is no post-processing step.

### 3. No External CLI Dependencies

The bash scripts shelled out to `sqlacodegen`, `alembic`, `psql`, `sed`, and `grep`. Each was a potential failure point — wrong version, wrong PATH, wrong flags, macOS vs GNU incompatibilities.

SMT uses only Python APIs:
- `sqlalchemy.inspect()` replaces sqlacodegen
- `alembic.command.*` replaces the alembic CLI
- `sqlalchemy.text()` replaces psql for schema DDL
- Python's `re` module replaces sed/grep

### 4. Fail Early, Fail Clearly

Configuration errors should surface before any database connection is attempted:
- Schema name character validation (`[A-Za-z0-9_]+` only)
- Schema name length validation against dialect limits (PostgreSQL: 63 chars, MSSQL: 128)
- Missing required fields reported with the field name and section
- Unsupported dialects rejected at config parse time

Runtime errors should say what step failed and how to recover:
- "Pipeline failed at step 3 (Create migration): ..."
- "You can retry from this step with: smt create"

### 5. Idempotent by Default

Running `smt migrate` twice with no source changes should be safe and produce no side effects. This is achieved by:
- Always regenerating models from source (catches drift)
- Alembic comparing models against target state (not against previous models)
- Empty migrations detected and removed
- "Already at head" check before applying

This makes SMT safe to run in cron jobs or CI pipelines.

### 6. Reviewable Before Executable

Every migration generates a DDL SQL file before applying. The `--dry-run` flag generates DDL without executing. This respects DBA workflows where no tool runs DDL directly against production — a human reviews the SQL first.

### 7. Dialect-Aware, Not Dialect-Locked

The core reflection and model generation is dialect-agnostic (SQLAlchemy handles it). Only schema-level DDL (CREATE/DROP SCHEMA) and connection parameters require dialect-specific code, isolated in `database.py` and `config.py`.

Adding a new database means:
1. Add a DDL template to `_CREATE_SCHEMA_DDL` and `_DROP_SCHEMA_DDL`
2. Add a default driver and port to `config.py`
3. Add an optional dependency group to `pyproject.toml`

The model generator, migration manager, pipeline, and CLI require zero changes.

## What SMT Is Not

- **Not a data migration tool** — SMT migrates schema (DDL), not data (DML). Use separate ETL tools for data.
- **Not an ORM** — The generated models exist only to drive Alembic. They're not intended for application use.
- **Not a schema design tool** — SMT replicates what exists in the source. It doesn't optimize indexes, add partitioning, or suggest improvements.
- **Not a replacement for Alembic** — SMT wraps Alembic for a specific use case (cross-database schema replication). For application-driven migrations, use Alembic directly.
