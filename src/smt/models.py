"""Model code generator — replaces sqlacodegen + regex transforms.

Uses SQLAlchemy inspect() to reflect source database schema and generates
SQLAlchemy 2.0 declarative model code with lowercase database identifiers.
"""

from __future__ import annotations

import datetime
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import inspect as sa_inspect
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type mapping: SQLAlchemy reflected type class names -> generic SA type code
# ---------------------------------------------------------------------------

# Map from reflected type class name to (generic SA type name, Python type hint)
_TYPE_MAP: dict[str, tuple[str, str]] = {
    # Integer family
    "INTEGER": ("Integer", "int"),
    "INT": ("Integer", "int"),
    "SMALLINT": ("SmallInteger", "int"),
    "SMALLINTEGER": ("SmallInteger", "int"),
    "BIGINT": ("BigInteger", "int"),
    "BIGINTEGER": ("BigInteger", "int"),
    "TINYINT": ("SmallInteger", "int"),
    # String family
    "VARCHAR": ("String", "str"),
    "NVARCHAR": ("String", "str"),
    "CHAR": ("String", "str"),
    "NCHAR": ("String", "str"),
    "TEXT": ("Text", "str"),
    "NTEXT": ("Text", "str"),
    "STRING": ("String", "str"),
    "CLOB": ("Text", "str"),
    # Numeric family
    "NUMERIC": ("Numeric", "decimal.Decimal"),
    "DECIMAL": ("Numeric", "decimal.Decimal"),
    "FLOAT": ("Float", "float"),
    "REAL": ("Float", "float"),
    "DOUBLE": ("Float", "float"),
    "DOUBLE_PRECISION": ("Float", "float"),
    "MONEY": ("Numeric", "decimal.Decimal"),
    "SMALLMONEY": ("Numeric", "decimal.Decimal"),
    # Boolean
    "BOOLEAN": ("Boolean", "bool"),
    "BIT": ("Boolean", "bool"),
    # Date/Time family
    "DATETIME": ("DateTime", "datetime.datetime"),
    "DATETIME2": ("DateTime", "datetime.datetime"),
    "TIMESTAMP": ("DateTime", "datetime.datetime"),
    "DATE": ("Date", "datetime.date"),
    "TIME": ("Time", "datetime.time"),
    "SMALLDATETIME": ("DateTime", "datetime.datetime"),
    "DATETIMEOFFSET": ("DateTime", "datetime.datetime"),
    # Binary
    "BLOB": ("LargeBinary", "bytes"),
    "BYTEA": ("LargeBinary", "bytes"),
    "VARBINARY": ("LargeBinary", "bytes"),
    "BINARY": ("LargeBinary", "bytes"),
    "IMAGE": ("LargeBinary", "bytes"),
    # UUID
    "UUID": ("Uuid", "uuid.UUID"),
    "UNIQUEIDENTIFIER": ("Uuid", "uuid.UUID"),
}

# SA types that take a length parameter
_LENGTH_TYPES = {"String", "LargeBinary"}

# SA types that take precision/scale parameters
_PRECISION_TYPES = {"Numeric"}

# Python modules that need importing based on type hints used
_HINT_MODULES: dict[str, str] = {
    "datetime.datetime": "datetime",
    "datetime.date": "datetime",
    "datetime.time": "datetime",
    "decimal.Decimal": "decimal",
    "uuid.UUID": "uuid",
}


@dataclass
class ColumnInfo:
    """Reflected column metadata."""

    name: str
    type_name: str  # uppercase class name of the SA type
    sa_type_code: str  # code string for SA type, e.g. "String(40)"
    python_hint: str  # e.g. "int", "str", "datetime.datetime"
    nullable: bool
    is_primary_key: bool
    autoincrement: bool
    collation: str | None = None


@dataclass
class ForeignKeyInfo:
    """Reflected foreign key constraint."""

    name: str | None
    constrained_columns: list[str]
    referred_schema: str | None
    referred_table: str
    referred_columns: list[str]


@dataclass
class TableInfo:
    """Reflected table metadata."""

    name: str
    columns: list[ColumnInfo]
    pk_constraint_name: str | None
    pk_columns: list[str]
    foreign_keys: list[ForeignKeyInfo]


class ModelGenerator:
    """Generates SQLAlchemy 2.0 model code from a reflected database schema."""

    def __init__(
        self,
        source_engine: Engine,
        target_schema: str,
        source_schema: str,
        source_database: str,
        tables: list[str] | None = None,
    ):
        self.source_engine = source_engine
        self.target_schema = target_schema
        self.source_schema = source_schema
        self.source_database = source_database
        self.tables = tables  # None means all

    def generate(self) -> str:
        """Reflect source database and generate models.py content."""
        table_infos = self._reflect_tables()
        if not table_infos:
            raise RuntimeError(
                f"No tables found in schema '{self.source_schema}'"
                + (f" matching {self.tables}" if self.tables else "")
            )

        header = self._generate_header(table_infos)
        imports = self._generate_imports(table_infos)
        base_class = "\nclass Base(DeclarativeBase):\n    pass\n"

        class_defs = []
        for table in table_infos:
            class_defs.append(self._generate_table_class(table))

        return header + imports + base_class + "\n" + "\n\n".join(class_defs) + "\n"

    def write(self, output_path: Path) -> None:
        """Generate models and write to file, backing up any existing version."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if output_path.exists():
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = output_path.with_name(f"models_{timestamp}.py.bak")
            shutil.copy2(output_path, backup_path)
            logger.info("Backed up existing models.py to %s", backup_path.name)

        content = self.generate()
        output_path.write_text(content)
        logger.info("Generated models.py with %d table(s)", content.count("class ") - 1)

    # ------------------------------------------------------------------
    # Reflection
    # ------------------------------------------------------------------

    def _reflect_tables(self) -> list[TableInfo]:
        """Use SQLAlchemy inspect() to reflect all requested tables."""
        inspector = sa_inspect(self.source_engine)
        available = inspector.get_table_names(schema=self.source_schema)

        if self.tables:
            # Case-insensitive match against available tables
            available_lower = {t.lower(): t for t in available}
            selected = []
            for t in self.tables:
                actual = available_lower.get(t.lower())
                if actual:
                    selected.append(actual)
                else:
                    logger.warning("Table '%s' not found in schema '%s'", t, self.source_schema)
            table_names = selected
        else:
            table_names = available

        table_infos = []
        for table_name in sorted(table_names):
            table_infos.append(self._reflect_table(inspector, table_name))

        return table_infos

    def _reflect_table(self, inspector: Any, table_name: str) -> TableInfo:
        """Reflect a single table's metadata."""
        columns_raw = inspector.get_columns(table_name, schema=self.source_schema)
        pk_info = inspector.get_pk_constraint(table_name, schema=self.source_schema)
        fk_info = inspector.get_foreign_keys(table_name, schema=self.source_schema)

        pk_columns = [c.lower() for c in (pk_info.get("constrained_columns") or [])]
        pk_name = pk_info.get("name")

        columns = []
        for col in columns_raw:
            col_info = self._map_column(col, pk_columns)
            columns.append(col_info)

        foreign_keys = []
        for fk in fk_info:
            foreign_keys.append(
                ForeignKeyInfo(
                    name=fk.get("name"),
                    constrained_columns=fk["constrained_columns"],
                    referred_schema=fk.get("referred_schema"),
                    referred_table=fk["referred_table"],
                    referred_columns=fk["referred_columns"],
                )
            )

        return TableInfo(
            name=table_name,
            columns=columns,
            pk_constraint_name=pk_name,
            pk_columns=pk_columns,
            foreign_keys=foreign_keys,
        )

    def _map_column(self, col: dict[str, Any], pk_columns: list[str]) -> ColumnInfo:
        """Map a reflected column dict to ColumnInfo with generic SA types."""
        sa_type = col["type"]
        type_class_name = type(sa_type).__name__.upper()

        # Check for collation
        collation = getattr(sa_type, "collation", None)
        if collation:
            logger.warning(
                "Column '%s': skipping collation '%s' (source-specific, not emitted in target)",
                col["name"],
                collation,
            )

        mapped = _TYPE_MAP.get(type_class_name)
        if mapped:
            sa_type_name, python_hint = mapped
        else:
            logger.warning(
                "Column '%s': unrecognized type '%s', falling back to String",
                col["name"],
                type_class_name,
            )
            sa_type_name = "String"
            python_hint = "str"

        # Build SA type code string with parameters
        sa_type_code = self._build_type_code(sa_type_name, sa_type)

        col_name_lower = col["name"].lower()
        is_pk = col_name_lower in pk_columns

        return ColumnInfo(
            name=col["name"],
            type_name=type_class_name,
            sa_type_code=sa_type_code,
            python_hint=python_hint,
            nullable=col.get("nullable", True),
            is_primary_key=is_pk,
            autoincrement=bool(col.get("autoincrement", False)),
            collation=collation,
        )

    def _build_type_code(self, sa_type_name: str, sa_type: Any) -> str:
        """Build the SA type constructor code, e.g. 'String(40)' or 'Integer'."""
        if sa_type_name in _LENGTH_TYPES:
            length = getattr(sa_type, "length", None)
            if length is not None:
                return f"{sa_type_name}({length})"
        elif sa_type_name in _PRECISION_TYPES:
            precision = getattr(sa_type, "precision", None)
            scale = getattr(sa_type, "scale", None)
            if precision is not None and scale is not None:
                return f"{sa_type_name}({precision}, {scale})"
            elif precision is not None:
                return f"{sa_type_name}({precision})"
        return sa_type_name

    # ------------------------------------------------------------------
    # Code generation
    # ------------------------------------------------------------------

    def _generate_header(self, tables: list[TableInfo]) -> str:
        """Generate timestamp header comment block."""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        table_names = ", ".join(t.name for t in tables)
        return (
            "# =============================================================================\n"
            "# Auto-generated SQLAlchemy models\n"
            f"# Generated: {now}\n"
            f"# Source: {self.source_database}.{self.source_schema}\n"
            f"# Target: {self.target_schema}\n"
            f"# Tables: {table_names}\n"
            "# =============================================================================\n"
            "\n"
        )

    def _generate_imports(self, tables: list[TableInfo]) -> str:
        """Generate import statements based on types used across all tables."""
        # Collect all SA types and Python hints used
        sa_types_used: set[str] = set()
        hints_used: set[str] = set()
        has_optional = False
        has_fks = False
        has_pks = False
        has_identity = False

        for table in tables:
            if table.pk_columns:
                has_pks = True
            if table.foreign_keys:
                has_fks = True
            for col in table.columns:
                # Extract the base type name (before parentheses)
                base_type = col.sa_type_code.split("(")[0]
                sa_types_used.add(base_type)
                hints_used.add(col.python_hint)
                if col.nullable and not col.is_primary_key:
                    has_optional = True
                if col.autoincrement and col.is_primary_key:
                    has_identity = True

        # Build imports
        lines = []

        # Standard library imports
        typing_imports = []
        if has_optional:
            typing_imports.append("Optional")
        if typing_imports:
            lines.append(f"from typing import {', '.join(sorted(typing_imports))}")

        # Module imports for type hints
        modules_needed = set()
        for hint in hints_used:
            mod = _HINT_MODULES.get(hint)
            if mod:
                modules_needed.add(mod)
        for mod in sorted(modules_needed):
            lines.append(f"import {mod}")

        if lines:
            lines.append("")

        # SQLAlchemy imports
        sa_imports = sorted(sa_types_used)
        if has_identity:
            sa_imports.append("Identity")
        if has_fks:
            sa_imports.append("ForeignKeyConstraint")
        if has_pks:
            sa_imports.append("PrimaryKeyConstraint")

        sa_imports_sorted = sorted(set(sa_imports))
        lines.append(f"from sqlalchemy import {', '.join(sa_imports_sorted)}")
        lines.append(
            "from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column"
        )

        return "\n".join(lines) + "\n"

    def _generate_table_class(self, table: TableInfo) -> str:
        """Generate a single table class definition."""
        # Class name: preserve original case from source
        class_name = table.name
        table_name_lower = table.name.lower()

        lines = [f"class {class_name}(Base):"]
        lines.append(f"    __tablename__ = '{table_name_lower}'")

        # Build __table_args__
        table_args = self._build_table_args(table)
        lines.extend(table_args)

        lines.append("")

        # Generate column definitions
        for col in table.columns:
            lines.append(self._generate_column(col))

        return "\n".join(lines)

    def _build_table_args(self, table: TableInfo) -> list[str]:
        """Build __table_args__ tuple with constraints and schema."""
        args: list[str] = []

        # Primary key constraint
        if table.pk_columns:
            pk_cols = ", ".join(f"'{c}'" for c in table.pk_columns)
            pk_name = table.pk_constraint_name
            if pk_name:
                pk_name = pk_name.lower()
                args.append(f"        PrimaryKeyConstraint({pk_cols}, name='{pk_name}')")
            else:
                args.append(f"        PrimaryKeyConstraint({pk_cols})")

        # Foreign key constraints
        for fk in table.foreign_keys:
            args.append(self._generate_fk_constraint(fk))

        schema_dict = f"{{'schema': '{self.target_schema}'}}"

        if args:
            lines = ["    __table_args__ = ("]
            for arg in args:
                lines.append(f"{arg},")
            lines.append(f"        {schema_dict}")
            lines.append("    )")
        else:
            lines = [f"    __table_args__ = {schema_dict}"]

        return lines

    def _generate_fk_constraint(self, fk: ForeignKeyInfo) -> str:
        """Generate a ForeignKeyConstraint entry."""
        # Source columns (lowercase)
        src_cols = ", ".join(f"'{c.lower()}'" for c in fk.constrained_columns)

        # Reference columns: target_schema.table.column (all lowercase)
        ref_parts = []
        for ref_col in fk.referred_columns:
            ref_parts.append(
                f"'{self.target_schema}.{fk.referred_table.lower()}.{ref_col.lower()}'"
            )
        ref_cols = ", ".join(ref_parts)

        name_part = ""
        if fk.name:
            name_part = f", name='{fk.name.lower()}'"

        return f"        ForeignKeyConstraint([{src_cols}], [{ref_cols}]{name_part})"

    def _generate_column(self, col: ColumnInfo) -> str:
        """Generate a single column definition line."""
        col_name_lower = col.name.lower()

        # Python type hint
        if col.nullable and not col.is_primary_key:
            hint = f"Mapped[Optional[{col.python_hint}]]"
        else:
            hint = f"Mapped[{col.python_hint}]"

        # mapped_column arguments
        args = [f"'{col_name_lower}'"]

        # Add Identity() for autoincrement PKs
        if col.is_primary_key and col.autoincrement:
            args.append(f"{col.sa_type_code}")
            args.append("Identity()")
            args.append("primary_key=True")
        elif col.is_primary_key:
            args.append(f"{col.sa_type_code}")
            args.append("primary_key=True")
        else:
            args.append(f"{col.sa_type_code}")
            if not col.nullable:
                args.append("nullable=False")

        args_str = ", ".join(args)
        return f"    {col.name}: {hint} = mapped_column({args_str})"
