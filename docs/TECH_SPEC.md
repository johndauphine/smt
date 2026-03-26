# Technical Specification

Detailed technical reference for SMT internals.

## Module Overview

| Module | Purpose | Key Classes |
|--------|---------|-------------|
| `config.py` | YAML loading, validation, URL building | `DatabaseConfig`, `SmtConfig` |
| `database.py` | Connection management, schema DDL | `DatabaseManager` |
| `models.py` | Source reflection, per-table model generation | `ModelGenerator` |
| `migration.py` | Alembic programmatic wrapper | `MigrationManager` |
| `pipeline.py` | Step orchestration | `Pipeline` |
| `cli.py` | Click CLI entry point | `cli` group |

## Configuration Schema

### YAML Structure

```yaml
source:                          # Required
  dialect: postgresql | mssql    # Required
  driver: psycopg2               # Optional (default per dialect)
  host: localhost                 # Required
  port: 5432                     # Optional (default per dialect)
  user: postgres                 # Required
  password: secret               # Required (overridable via env var)
  database: SourceDB             # Required
  schema: dbo                    # Required for source

target:                          # Required
  dialect: postgresql | mssql    # Required
  driver: psycopg2               # Optional
  host: localhost                 # Required
  port: 5432                     # Optional
  user: postgres                 # Required
  password: secret               # Required (overridable via env var)
  database: TargetDB             # Required

tables: [Users, Posts] | all     # Optional (default: all)
workspace: ./migration_workspace # Optional (default: ./migration_workspace)
```

### Defaults

| Dialect | Default Driver | Default Port |
|---------|---------------|--------------|
| `postgresql` | `psycopg2` | `5432` |
| `mssql` | `pyodbc` | `1433` |

### Environment Variable Overrides

| Variable | Overrides |
|----------|-----------|
| `SMT_SOURCE_PASSWORD` | `source.password` |
| `SMT_TARGET_PASSWORD` | `target.password` |

### Derived Values

**Target schema**: `dw__{source.database.lower()}__{source.schema.lower()}`

### Validation Rules

| Rule | When | Error |
|------|------|-------|
| Dialect must be `postgresql` or `mssql` | Config parse | `Unsupported dialect` |
| Source must have `schema` field | Config parse | `source: 'schema' is required` |
| Target schema chars `[A-Za-z0-9_]+` | Config parse | `contains invalid characters` |
| Target schema length <= dialect limit | Config parse | `exceeding identifier limit` |
| Schema name validated before DDL | DDL execution | `Invalid schema name` |

Identifier length limits: PostgreSQL = 63 chars, MSSQL = 128 chars.

### URL Construction

URLs are built using `sqlalchemy.engine.URL.create()` which handles password encoding automatically. For MSSQL+pyodbc, the following query parameters are added automatically:

```
?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes&Encrypt=yes
```

`get_url_string()` renders the URL with the password visible (required for Alembic's `alembic.ini`).

## Type Mapping

The model generator maps reflected SQLAlchemy type class names to generic types:

### Integer Family

| Source Type | Generic SA Type | Python Hint |
|------------|----------------|-------------|
| `INTEGER`, `INT` | `Integer` | `int` |
| `SMALLINT`, `TINYINT` | `SmallInteger` | `int` |
| `BIGINT` | `BigInteger` | `int` |

### String Family

| Source Type | Generic SA Type | Python Hint |
|------------|----------------|-------------|
| `VARCHAR`, `NVARCHAR`, `CHAR`, `NCHAR` | `String` | `str` |
| `TEXT`, `NTEXT`, `CLOB` | `Text` | `str` |

### Numeric Family

| Source Type | Generic SA Type | Python Hint |
|------------|----------------|-------------|
| `NUMERIC`, `DECIMAL`, `MONEY`, `SMALLMONEY` | `Numeric` | `decimal.Decimal` |
| `FLOAT`, `REAL`, `DOUBLE` | `Float` | `float` |

### Boolean

| Source Type | Generic SA Type | Python Hint |
|------------|----------------|-------------|
| `BOOLEAN`, `BIT` | `Boolean` | `bool` |

### Date/Time Family

| Source Type | Generic SA Type | Python Hint |
|------------|----------------|-------------|
| `DATETIME`, `DATETIME2`, `TIMESTAMP`, `SMALLDATETIME`, `DATETIMEOFFSET` | `DateTime` | `datetime.datetime` |
| `DATE` | `Date` | `datetime.date` |
| `TIME` | `Time` | `datetime.time` |

### Binary Family

| Source Type | Generic SA Type | Python Hint |
|------------|----------------|-------------|
| `BLOB`, `BYTEA`, `VARBINARY`, `BINARY`, `IMAGE` | `LargeBinary` | `bytes` |

### UUID

| Source Type | Generic SA Type | Python Hint |
|------------|----------------|-------------|
| `UUID`, `UNIQUEIDENTIFIER` | `Uuid` | `uuid.UUID` |

### Fallback

Unrecognized types are mapped to `String` / `str` with a warning logged:

```
Column 'geom': unrecognized type 'GEOMETRY', falling back to String
```

### Type Parameters

- **Length types** (`String`, `LargeBinary`): Length is preserved, e.g., `String(40)`
- **Precision types** (`Numeric`): Precision and scale preserved, e.g., `Numeric(10, 2)`
- **Collations**: Detected but never emitted. A warning is logged per column.

## Collation Handling

### During Model Generation

When `inspect()` reflects a column with a `collation` attribute (common on MSSQL `NVARCHAR` columns), a warning is logged:

```
Column 'DisplayName': skipping collation 'SQL_Latin1_General_CP1_CI_AS'
```

The collation is not included in the generated model. This is intentional — the target database uses its own default collation.

### During Migration Creation

If Alembic autogenerate produces migration files containing `collation=` parameters (from target schema drift), they are stripped using three regex patterns:

| Pattern | Handles |
|---------|---------|
| `, collation='...'` | Comma-prefixed collation |
| `collation='...', ` | Comma-suffixed collation |
| `(collation='...')` | Sole parameter (becomes empty parens) |

The count of stripped collations is logged as a warning.

## Schema DDL Dispatch

### CREATE SCHEMA

| Dialect | DDL |
|---------|-----|
| PostgreSQL | `CREATE SCHEMA IF NOT EXISTS {schema}` |
| MSSQL | `IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}') EXEC('CREATE SCHEMA [{schema}]')` |

### DROP SCHEMA

| Dialect | DDL |
|---------|-----|
| PostgreSQL | `DROP SCHEMA IF EXISTS {schema} CASCADE` |
| MSSQL | `IF EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}') EXEC('DROP SCHEMA [{schema}]')` |

All schema names are validated against `[A-Za-z0-9_]+` before interpolation.

## Alembic Integration

SMT drives Alembic entirely through the Python API (`alembic.command` module):

| Operation | API Call |
|-----------|----------|
| Initialize | `command.init(config, alembic_dir)` |
| Create migration | `command.revision(config, autogenerate=True, message=msg)` |
| Apply | `command.upgrade(config, "head")` |
| Rollback | `command.downgrade(config, target)` |
| Generate DDL | `command.upgrade(config, "head", sql=True)` with `output_buffer` |
| History | `command.history(config, verbose=True)` with `output_buffer` |

### Current/Head Revision

Current revision is determined by connecting to the target database and querying `MigrationContext.get_current_heads()`. Head revision is determined from the script directory (`ScriptDirectory.get_heads()`).

### env.py Template

The template at `src/smt/templates/env.py.mako` is written to the workspace on every `init` call. It:
- Adds the workspace parent directory to `sys.path`
- Imports `Base` from the `models` package (`models/__init__.py` re-exports it)
- Sets `target_metadata = Base.metadata`
- Uses `include_schemas=True` in both online and offline modes
- Uses `NullPool` for online mode connections

### Empty Migration Detection

After autogenerate, the migration file is scanned for Alembic operation patterns:

```
op.create_table, op.drop_table, op.add_column, op.drop_column,
op.create_index, op.drop_index, op.alter_column,
op.create_unique_constraint, op.drop_constraint,
op.create_foreign_key, op.create_check_constraint
```

If none are found, the migration file is deleted and "Already in sync" is reported.

## Pipeline Steps

| Step | Module | Action | On Failure |
|------|--------|--------|------------|
| 1 | `models.py` | Reflect source, generate `models/` package (one file per table) | Retry: `smt generate` |
| 2 | `migration.py` + `database.py` | Init Alembic, create target schema | Retry: `smt init` |
| 3 | `migration.py` | Create autogenerated migration | Retry: `smt create` |
| 4 | `migration.py` + `database.py` | Generate DDL (full + per-table), apply, verify tables | Retry: `smt apply` |

Each step reports the step number and recovery command on failure.

## CLI Options

### Global Options (before subcommand)

| Option | Default | Description |
|--------|---------|-------------|
| `-c, --config PATH` | `smt.yaml` | YAML config file |
| `--log-level` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

### Subcommand Options

| Command | Option | Description |
|---------|--------|-------------|
| `migrate` | `--yes, -y` | Skip confirmation prompt |
| `create` | `--message, -m` | Migration message (default: "Schema migration") |
| `apply` | `--dry-run` | Generate DDL without applying |
| `rollback` | `--steps, -n INT` | Rollback N revisions |
| `rollback` | `--revision, -r TEXT` | Rollback to specific revision |
| `rollback` | `--drop-schema` | Drop target schema after rollback |
