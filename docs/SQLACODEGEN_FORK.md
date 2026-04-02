# sqlacodegen Fork Specification for SMT

> **Status: IMPLEMENTED** — All 11 changes are implemented in `src/sqlacodegen_smt/generator.py` as a subclass of sqlacodegen's `DeclarativeGenerator` (Option 2 from below, not Option 1). The original `ModelGenerator` in `src/smt/models.py` is now a thin ~120-line wrapper. See "Implementation Notes" at the end for details on approach differences.

Original specification for replacing SMT's custom `ModelGenerator` (`src/smt/models.py`) with a modified sqlacodegen that produces equivalent output.

## Background

SMT originally used a custom 637-line `ModelGenerator` that called `sqlalchemy.inspect()` directly and emitted per-table model files with lowercase DB identifiers, PascalCase Python attributes, and target schema rewriting. This document specified the changes needed to a sqlacodegen fork to produce the same output.

### sqlacodegen Baseline

- **Repository**: https://github.com/agronholm/sqlacodegen
- **Version to fork**: 4.0.x (latest stable)
- **License**: MIT
- **Python**: >= 3.10
- **Dependencies**: SQLAlchemy >= 2.0.29, inflect >= 4.0.0
- **Key file**: `src/sqlacodegen/generators.py` (~2000 lines) contains all four generator classes

### sqlacodegen Generator Hierarchy

```
CodeGenerator (abstract base)
  TablesGenerator            -- Table() objects, no ORM
    DeclarativeGenerator     -- SQLAlchemy 2.0 Mapped[] classes (our starting point)
      DataclassGenerator     -- MappedAsDataclass variant
      SQLModelGenerator      -- SQLModel variant
```

`DeclarativeGenerator` is the class to subclass. It already produces SQLAlchemy 2.0 declarative models with `Mapped[]` type annotations and `mapped_column()`.

### sqlacodegen Extension Point

sqlacodegen uses setuptools entry points for generator discovery:

```toml
[project.entry-points."sqlacodegen.generators"]
smt = "sqlacodegen_smt.generator:SmtGenerator"
```

This means the fork can be structured as either:
1. A direct fork of sqlacodegen with modifications to `DeclarativeGenerator`
2. A separate package that subclasses `DeclarativeGenerator` and registers via entry point

Option 2 is cleaner but may require more method overrides. Option 1 gives full control. **Recommendation: option 1** (direct fork) since the changes touch many internal methods and single-file-to-multi-file output requires restructuring `generate()`.

## Required Changes

### Change 1: Multi-File Package Output

**Current sqlacodegen behavior**: `generate()` returns a single string written to one file or stdout.

**Required behavior**: Generate a `models/` package directory:
```
models/
  base.py        -- Base(DeclarativeBase) class
  <table>.py     -- One file per table (filename = table name lowercased)
  __init__.py    -- Re-exports Base and all model classes
```

**Where to modify**:
- Override `DeclarativeGenerator.generate()` to return a dict of `{filename: content}` or write files directly
- The CLI (`cli.py`) must be updated to handle directory output instead of single-file output (new `--outdir` flag)
- `generate_base()` output goes to `base.py`
- Each model class rendered by `render_class()` goes to its own `<table>.py`
- A new `generate_init_file()` method produces `__init__.py`

**Per-table file structure**:
```python
# base.py
"""SQLAlchemy declarative base."""
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase):
    pass
```

```python
# <table>.py -- each table gets its own file
# Header with timestamp, source DB, target schema
from typing import Optional    # only if needed
import datetime                # only if needed by column types

from sqlalchemy import Integer, String, PrimaryKeyConstraint, ...
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base

class Users(Base):
    __tablename__ = 'users'
    ...
```

```python
# __init__.py
"""Auto-generated models package."""
from .base import Base  # noqa: F401
from .users import Users  # noqa: F401
from .posts import Posts  # noqa: F401
```

**Import tracking**: The current `collect_imports()` and `group_imports()` methods accumulate imports globally. These must be changed to track imports **per table**, since each table file needs its own import block. The simplest approach: call `collect_imports()` and `group_imports()` once per table model instead of once for all models.

### Change 2: Lowercase Database Identifiers

**Current sqlacodegen behavior**: Uses the original table/column/constraint names from the source database as-is.

**Required behavior**:
- `__tablename__` value = lowercase of source table name
- Column DB names (first arg to `mapped_column()`) = lowercase
- PK constraint names = lowercase
- FK constraint names = lowercase
- FK referred column references = lowercase (`target_schema.tablename.colname`)

**Where to modify**:
- `render_class_variables()` — lowercase the `__tablename__` value
- `render_column()` — the `show_name` parameter controls whether the column name appears in `mapped_column()`. Force `show_name=True` always (since DB name differs from Python attr name) and lowercase the name string
- `render_constraint()` — lowercase constraint names in `PrimaryKeyConstraint(name=...)` and `ForeignKeyConstraint(name=...)`
- `render_table_args()` — lowercase any constraint names in the table args tuple

### Change 3: PascalCase Python Attribute Names

**Current sqlacodegen behavior**: Python attribute names match the database column names. sqlacodegen may apply `re.sub` to convert to snake_case with `use_inflect` option.

**Required behavior**: Python attribute names preserve the **original casing** from the source database. If the source has `DisplayName`, the Python attribute is `DisplayName` (not `display_name`, not `displayname`).

**Where to modify**:
- `generate_column_attr_name()` on `DeclarativeGenerator` — return the original column name unchanged (no snake_case conversion)
- Ensure `render_column_attribute()` renders as:
  ```python
  DisplayName: Mapped[Optional[str]] = mapped_column('displayname', String(100))
  ```
  The attribute name is original case, the first arg to `mapped_column()` is lowercase.

### Change 4: Target Schema Rewriting

**Current sqlacodegen behavior**: If a table has a schema, it appears as `schema='source_schema'` in `__table_args__`. FK references use the source schema.

**Required behavior**:
- `__table_args__` must use the **target schema** (e.g., `dw__sourcedb__dbo`), not the source schema
- FK `referred_table` references must use target schema: `'dw__sourcedb__dbo.tablename.colname'`
- The target schema is passed in as a parameter (not reflected from any database)

**Where to modify**:
- New constructor parameter: `target_schema: str` on the generator class
- `render_table_args()` — replace `table.schema` with `self.target_schema` in the schema dict
- `render_constraint()` for `ForeignKeyConstraint` — rewrite `referred_schema` to `self.target_schema` and lowercase the table/column names in references

**New CLI parameter**: `--target-schema` to pass the derived schema name.

### Change 5: Collation Stripping

**Current sqlacodegen behavior**: Emits `collation='...'` parameters on String/Text columns when reflected from source.

**Required behavior**: Never emit collation parameters. Log a warning when a collation is detected.

**Where to modify**:
- `render_column_type()` — after introspecting the type's constructor args, remove `collation` from the rendered kwargs
- Or strip collation from the reflected type object in `fix_column_types()` before rendering

### Change 6: Type Fallback to String

**Current sqlacodegen behavior**: Uses `get_adapted_type()` to walk the type's MRO and adapt dialect-specific types to generic SQLAlchemy types. If adaptation fails, keeps the dialect-specific type (e.g., `UNIQUEIDENTIFIER`, `MONEY`).

**Required behavior**: If a type cannot be mapped to a generic SQLAlchemy type, fall back to `String` and log a warning. This ensures generated models are dialect-independent.

SMT's current explicit type map for reference (types that need specific handling):

| Source Type | Target SA Type | Notes |
|---|---|---|
| `TINYINT` | `SmallInteger` | No `TinyInteger` in SA |
| `NVARCHAR`, `NCHAR`, `NTEXT` | `String` / `Text` | Drop the N-prefix distinction |
| `MONEY`, `SMALLMONEY` | `Numeric` | Map to generic numeric |
| `DATETIME2`, `SMALLDATETIME`, `DATETIMEOFFSET` | `DateTime` | MSSQL-specific variants |
| `IMAGE` | `LargeBinary` | Legacy MSSQL blob type |
| `UNIQUEIDENTIFIER` | `Uuid` | MSSQL UUID type |
| `BIT` | `Boolean` | MSSQL boolean |

**Where to modify**:
- `get_adapted_type()` — add a final fallback: if no generic type is found after walking the MRO, return `String` and emit a warning
- Verify the types in the table above are handled correctly by sqlacodegen's existing adaptation. If not, add explicit mappings.

### Change 7: Identity() for Autoincrement PKs

**Current sqlacodegen behavior**: May emit `autoincrement=True` or `Identity()` depending on dialect and version.

**Required behavior**: Autoincrement primary key columns must use `Identity()`:
```python
Id: Mapped[int] = mapped_column('id', Integer, Identity(), primary_key=True)
```

**Where to modify**:
- `render_column()` — detect autoincrement on PK columns and include `Identity()` in the rendered args
- Add `Identity` to the imports when used

### Change 8: Python Keyword Escaping

**Current sqlacodegen behavior**: May or may not handle Python reserved words as attribute names.

**Required behavior**: If a table name or column name is a Python keyword (e.g., `class`, `type`, `import`), append `_` to the Python attribute name:
```python
class_: Mapped[str] = mapped_column('class', String(50))
```

**Where to modify**:
- `generate_column_attr_name()` — check `keyword.iskeyword()` and append `_`
- `generate_model_name()` (for class names) — same check

### Change 9: Disable Relationship Generation

**Current sqlacodegen behavior**: `DeclarativeGenerator` detects FK relationships and generates `relationship()` attributes with back_populates.

**Required behavior**: Do not generate relationship attributes. SMT models are for schema migration only, not ORM querying. Relationships add complexity and import issues in per-table files.

**Where to modify**:
- Override `generate_relationships()` to be a no-op (return empty list)
- Or pass `--options nobidi` to disable bidirectional relationships — but this still generates forward relationships. A full override is cleaner.

### Change 10: File Header with Metadata

**Current sqlacodegen behavior**: No header comment.

**Required behavior**: Each generated file should have a header:
```python
# =============================================================================
# Auto-generated SQLAlchemy model: <TableName>
# Generated: <timestamp>
# Source: <database>.<schema>
# Target: <target_schema>
# =============================================================================
```

**Where to modify**:
- New method `generate_file_header(table_name: str | None) -> str`
- Called at the top of each file's content generation
- Requires new constructor parameters: `source_database`, `source_schema`

**New CLI parameters**: `--source-database`, `--source-schema` (or infer from the connection URL).

### Change 11: Backup Previous Output

**Current sqlacodegen behavior**: Overwrites output file or writes to stdout.

**Required behavior**: Before writing, if the output `models/` directory already exists:
1. Copy it to `models_<YYYYMMDD_HHMMSS>.bak/`
2. If a legacy `models.py` file exists in the parent directory, copy it to `models_<timestamp>.py.bak` and delete the original

**Where to modify**: This is output-layer logic. Add to the CLI or a new `write_output()` method that handles backup before writing the package directory.

## Integration with SMT

After the fork is ready, SMT's `models.py` (`ModelGenerator` class) gets replaced with a thin wrapper that calls the forked sqlacodegen:

```python
# Conceptual integration — src/smt/models.py after migration
from sqlacodegen_smt.generator import SmtGenerator
from sqlalchemy import MetaData

class ModelGenerator:
    def __init__(self, source_engine, target_schema, source_schema, source_database, tables=None):
        self.source_engine = source_engine
        self.target_schema = target_schema
        self.source_schema = source_schema
        self.source_database = source_database
        self.tables = tables

    def write(self, output_dir):
        # Reflect
        metadata = MetaData()
        metadata.reflect(self.source_engine, schema=self.source_schema,
                         only=self.tables)

        # Generate
        generator = SmtGenerator(
            metadata=metadata,
            bind=self.source_engine,
            target_schema=self.target_schema,
            source_database=self.source_database,
            source_schema=self.source_schema,
            options=set(),
        )
        files = generator.generate()  # returns dict[str, str]

        # Backup and write
        self._backup(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        for filename, content in files.items():
            (output_dir / filename).write_text(content)
```

The rest of SMT (`migration.py`, `pipeline.py`, `cli.py`, `database.py`, `config.py`) remains unchanged. The generated model files must be identical in structure so Alembic autogenerate continues to work.

## Acceptance Criteria

The fork is complete when the following are true:

1. **Output equivalence**: Given the same source database, the fork produces model files that are functionally equivalent to the current `ModelGenerator` output. "Functionally equivalent" means:
   - Same `__tablename__` values (lowercase)
   - Same column definitions (lowercase DB names, original-case Python attrs, same SA types)
   - Same `__table_args__` (target schema, lowercase constraint names)
   - Same FK references (target schema, lowercase table/column names)
   - Same `Identity()` on autoincrement PKs
   - Same `Optional[]` on nullable non-PK columns
   - No collation parameters
   - No relationship attributes
   - Per-table file package structure with `base.py`, `__init__.py`, per-table files

2. **Alembic compatibility**: The generated models work with SMT's existing Alembic pipeline — `alembic revision --autogenerate` produces correct migrations against a target database.

3. **Type safety**: All MSSQL-specific types (`UNIQUEIDENTIFIER`, `MONEY`, `BIT`, `DATETIME2`, `IMAGE`, `NVARCHAR`, etc.) resolve to generic SQLAlchemy types, not dialect-specific ones.

4. **Test parity**: Existing SMT tests in `tests/test_models.py` pass with the new generator (may need fixture updates for the new constructor signature).

## Testing Strategy

### Unit Tests

Port the existing SMT `tests/test_models.py` tests. These use a mock `Inspector` — the fork should work with the same pattern or with sqlacodegen's preferred in-memory SQLite approach.

Key test cases to preserve:
- `BasicTable` — class structure, `__tablename__` lowercase, `__table_args__` with target schema, PK constraint, column definitions with correct types and hints
- `ForeignKey` — FK constraint with target schema qualified references
- `TableFilter` — only specified tables are generated
- `BackupOnWrite` — previous `models/` backed up before overwrite
- `LegacyModelsPyBackup` — legacy `models.py` file backed up and removed

### Integration Test

A roundtrip test against a real database (SQLite or PostgreSQL):
1. Create source tables with representative column types
2. Run the fork to generate models
3. Run Alembic autogenerate against an empty target
4. Verify the migration creates the expected tables with correct types

### Diff Test

Run both the current `ModelGenerator` and the fork against the same reflected metadata. Diff the output files — they should be identical (modulo timestamp in headers).

## Summary of Files to Modify in the Fork

| File | Changes |
|---|---|
| `generators.py` | Subclass or modify `DeclarativeGenerator`: multi-file output, lowercase identifiers, PascalCase attrs, target schema, collation stripping, type fallback, Identity(), keyword escaping, no relationships, file headers |
| `cli.py` | Add `--outdir`, `--target-schema`, `--source-database`, `--source-schema` flags; handle directory output |
| `models.py` | No changes expected (internal dataclasses) |
| `utils.py` | Possible minor changes for lowercase naming helpers |
| `pyproject.toml` | Rename package, update entry points, add SMT-specific metadata |
| New: `tests/test_smt_output.py` | Tests for SMT-specific output format |

## Implementation Notes

The spec recommended **Option 1** (direct fork) assuming the changes would require modifying many internal methods. After reading the actual sqlacodegen source, **Option 2** (subclass) proved feasible — every change mapped to a clean method override.

### What was implemented

| File | Description |
|---|---|
| `src/sqlacodegen_smt/__init__.py` | Package init |
| `src/sqlacodegen_smt/generator.py` | `SmtGenerator(DeclarativeGenerator)` — ~350 lines, ~12 method overrides |
| `src/smt/models.py` | Thin wrapper (~120 lines): reflection, backup, file I/O |
| `tests/test_sqlacodegen_smt.py` | 21 unit tests using in-memory SQLite with real SA objects |
| `tests/test_models.py` | 9 wrapper tests (updated to mock `MetaData.reflect()`) |

### Method override mapping

| Change | Methods Overridden |
|---|---|
| 1. Multi-file output | `generate()`, `_generate_base_file()`, `_generate_table_file()`, `_generate_init_file()` |
| 2. Lowercase identifiers | `render_class_variables()`, `render_column_attribute()`, `render_constraint()` |
| 3. PascalCase attrs | `generate_column_attr_name()` |
| 4. Target schema | `render_table_args()`, `render_constraint()` |
| 5. Collation stripping | `fix_column_types()` |
| 6. Type fallback | `get_adapted_type()` + `_MSSQL_TYPE_OVERRIDES` dict |
| 7. Identity() | `render_column_attribute()` |
| 8. Keyword escaping | `generate_column_attr_name()`, `generate_model_name()` |
| 9. No relationships | `generate_relationships()` |
| 10. File headers | `_generate_file_header()` |
| 11. Backup | Stays in `ModelGenerator` wrapper (not in generator) |

### Advantages of subclass over direct fork

- **No code duplication** — sqlacodegen's 2000-line `generators.py` stays upstream
- **Upstream fixes flow in** — pinned `>=4.0,<5.0` in pyproject.toml
- **Less code** — 350 lines of overrides vs. maintaining a full fork

### E2E verification

Both PG-to-PG and MSSQL-to-PG pipelines verified end-to-end with Docker containers. MSSQL type mappings confirmed: `NVARCHAR` → `String`, `NTEXT` → `Text`, `MONEY` → `Numeric`, `BIT` → `Boolean`, `DATETIME2` → `DateTime`. Collation stripping confirmed for `SQL_Latin1_General_CP1_CI_AS`.
