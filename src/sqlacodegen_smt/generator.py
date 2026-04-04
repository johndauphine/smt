"""SMT generator — subclass of sqlacodegen's DeclarativeGenerator.

Produces a models/ package with per-table files, lowercase DB identifiers,
PascalCase Python attributes, target schema rewriting, and no relationships.
"""

from __future__ import annotations

import datetime
import logging
from collections import defaultdict
from collections.abc import Sequence
from keyword import iskeyword
from typing import Any, ClassVar

from sqlalchemy import (
    Computed,
    Constraint,
    ForeignKey,
    ForeignKeyConstraint,
    Identity,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.schema import DefaultClause
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.sql.type_api import UserDefinedType, TypeDecorator

from sqlacodegen.generators import DeclarativeGenerator
from sqlacodegen.models import (
    ColumnAttribute,
    Model,
    ModelClass,
    RelationshipAttribute,
)
from sqlacodegen.utils import (
    render_callable,
    uses_default_name,
)

logger = logging.getLogger(__name__)

# MSSQL type class names that sqlacodegen's MRO walk may not adapt correctly.
# Maps reflected type class name (uppercase) -> generic SA type class name to import.
_MSSQL_TYPE_OVERRIDES: dict[str, str] = {
    "UNIQUEIDENTIFIER": "Uuid",
    "MONEY": "Numeric",
    "SMALLMONEY": "Numeric",
    "BIT": "Boolean",
    "TINYINT": "SmallInteger",
    "NVARCHAR": "String",
    "NCHAR": "String",
    "NTEXT": "Text",
    "IMAGE": "LargeBinary",
    "DATETIME2": "DateTime",
    "SMALLDATETIME": "DateTime",
    "DATETIMEOFFSET": "DateTime",
}


class SmtGenerator(DeclarativeGenerator):
    """sqlacodegen generator customized for SMT output.

    Changes from DeclarativeGenerator:
    1.  Multi-file package output (dict[str, str] from generate())
    2.  Lowercase DB identifiers
    3.  PascalCase Python attribute names (preserve original)
    4.  Target schema rewriting
    5.  Collation stripping
    6.  Type fallback to String
    7.  Identity() for autoincrement PKs
    8.  Python keyword escaping
    9.  No relationship generation
    10. File headers with metadata
    11. Backup (handled externally in ModelGenerator wrapper)
    """

    valid_options: ClassVar[set[str]] = DeclarativeGenerator.valid_options

    def __init__(
        self,
        metadata: MetaData,
        bind: Connection | Engine,
        options: Sequence[str] = (),
        *,
        indentation: str = "    ",
        base_class_name: str = "Base",
        target_schema: str,
        source_database: str,
        source_schema: str,
    ):
        super().__init__(
            metadata,
            bind,
            options,
            indentation=indentation,
            base_class_name=base_class_name,
        )
        self.target_schema = target_schema
        self.source_database = source_database
        self.source_schema = source_schema

    # ------------------------------------------------------------------
    # Change 1 + 10: Multi-file package output with headers
    # ------------------------------------------------------------------

    def generate(self) -> dict[str, str]:  # type: ignore[override]
        """Generate a dict of {filename: content} for a models/ package."""
        self.generate_base()

        # Remove unwanted tables, fix column types (from parent)
        for table in list(self.metadata.tables.values()):
            if self.should_ignore_table(table):
                self.metadata.remove(table)
                continue

            if "noindexes" in self.options:
                table.indexes.clear()
            if "noconstraints" in self.options:
                table.constraints.clear()
            if "nocomments" in self.options:
                table.comment = None
                for column in table.columns:
                    column.comment = None

        for table in self.metadata.tables.values():
            self.fix_column_types(table)

        # Generate model objects (handles relationships=none, naming, etc.)
        models: list[Model] = self.generate_models()

        files: dict[str, str] = {}

        # base.py
        files["base.py"] = self._generate_base_file()

        # Per-table files
        model_classes = [m for m in models if isinstance(m, ModelClass)]
        for model in model_classes:
            filename = self._get_table_module_name(model.table.name) + ".py"
            files[filename] = self._generate_table_file(model)

        # __init__.py
        files["__init__.py"] = self._generate_init_file(model_classes)

        return files

    def _generate_base_file(self) -> str:
        return (
            '"""SQLAlchemy declarative base."""\n'
            "\n"
            "from sqlalchemy.orm import DeclarativeBase\n"
            "\n"
            "\n"
            f"class {self.base_class_name}(DeclarativeBase):\n"
            f"{self.indentation}pass\n"
        )

    def _generate_table_file(self, model: ModelClass) -> str:
        # Save and reset imports for per-file collection
        saved_imports = self.imports
        saved_module_imports = self.module_imports
        self.imports = defaultdict(set)
        self.module_imports = set()

        # Collect imports for this model only
        self._collect_imports_for_single_model(model)

        # Render the class
        class_code = self.render_class(model)

        # Group imports into sections
        groups = self.group_imports()
        import_block = "\n\n".join(
            "\n".join(line for line in group) for group in groups
        )

        # Add base import
        base_import = f"from .base import {self.base_class_name}"

        # Header
        header = self._generate_file_header(model.table.name)

        # Assemble
        parts = [header]
        if import_block:
            parts.append(import_block)
        parts.append(base_import)
        parts.append("")
        parts.append("")
        parts.append(class_code)
        parts.append("")

        # Restore global imports
        self.imports = saved_imports
        self.module_imports = saved_module_imports

        return "\n".join(parts)

    def _collect_imports_for_single_model(self, model: ModelClass) -> None:
        """Collect imports needed for a single model's columns and constraints."""
        # Always need Mapped and mapped_column
        self.add_literal_import("sqlalchemy.orm", "Mapped")
        self.add_literal_import("sqlalchemy.orm", "mapped_column")

        # Collect column imports
        for column_attr in model.columns:
            self.collect_imports_for_column(column_attr.column)

        # Collect constraint imports — always emit PK and FK constraints explicitly
        for constraint in model.table.constraints:
            if isinstance(constraint, PrimaryKeyConstraint):
                self.add_literal_import("sqlalchemy", "PrimaryKeyConstraint")
            elif isinstance(constraint, ForeignKeyConstraint):
                self.add_literal_import("sqlalchemy", "ForeignKeyConstraint")
            elif isinstance(constraint, UniqueConstraint):
                if len(constraint.columns) > 1 or not uses_default_name(constraint):
                    self.add_literal_import("sqlalchemy", "UniqueConstraint")

        # Check for server_default imports
        for column_attr in model.columns:
            col = column_attr.column
            if isinstance(col.server_default, Identity):
                self.add_literal_import("sqlalchemy", "Identity")
            elif isinstance(col.server_default, Computed):
                self.add_literal_import("sqlalchemy", "Computed")
            elif col.primary_key and getattr(col, "autoincrement", False):
                self.add_literal_import("sqlalchemy", "Identity")
            elif isinstance(col.server_default, DefaultClause):
                text_val = (
                    col.server_default.arg.text
                    if isinstance(col.server_default.arg, TextClause)
                    else str(col.server_default.arg)
                )
                if not text_val.startswith("nextval("):
                    self.add_literal_import("sqlalchemy", "text")

        # Add Optional if any nullable non-PK column
        for column_attr in model.columns:
            col = column_attr.column
            if col.nullable and not col.primary_key:
                self.add_literal_import("typing", "Optional")
                break

    def _get_table_module_name(self, table_name: str) -> str:
        """Get a safe Python module name for a table (lowercase, keyword-escaped)."""
        module_name = table_name.lower()
        if iskeyword(module_name):
            module_name += "_"
        return module_name

    def _generate_init_file(self, models: list[ModelClass]) -> str:
        lines = ['"""Auto-generated models package."""']
        lines.append("")
        lines.append(f"from .base import {self.base_class_name}  # noqa: F401")
        lines.append("")
        for model in sorted(models, key=lambda m: self._get_table_module_name(m.table.name)):
            module_name = self._get_table_module_name(model.table.name)
            lines.append(f"from .{module_name} import {model.name}  # noqa: F401")
        lines.append("")
        return "\n".join(lines)

    def _generate_file_header(self, table_name: str) -> str:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return (
            "# =============================================================================\n"
            f"# Auto-generated SQLAlchemy model: {table_name}\n"
            f"# Generated: {now}\n"
            f"# Source: {self.source_database}.{self.source_schema}\n"
            f"# Target: {self.target_schema}\n"
            "# =============================================================================\n"
        )

    # ------------------------------------------------------------------
    # Change 9: No relationships
    # ------------------------------------------------------------------

    def generate_relationships(
        self,
        source: ModelClass,
        models_by_table_name: dict[str, Model],
        association_tables: list[Model],
    ) -> list[RelationshipAttribute]:
        return []

    # ------------------------------------------------------------------
    # Change 3 + 8: PascalCase attrs + keyword escaping
    # ------------------------------------------------------------------

    def generate_column_attr_name(
        self,
        column_attr: ColumnAttribute,
        global_names: set[str],
        local_names: set[str],
    ) -> None:
        name = column_attr.column.name  # preserve original case
        if iskeyword(name) or iskeyword(name.lower()):
            name = name + "_"
        # Ensure uniqueness within the model (handles case-only collisions)
        original = name
        suffix = 1
        while name in local_names or name in global_names:
            name = f"{original}_{suffix}"
            suffix += 1
        column_attr.name = name

    # ------------------------------------------------------------------
    # Change 8: Model name = original table name + keyword escaping
    # ------------------------------------------------------------------

    def generate_model_name(self, model: Model, global_names: set[str]) -> None:
        if isinstance(model, ModelClass):
            name = model.table.name
            if iskeyword(name) or iskeyword(name.lower()):
                name = name + "_"
            model.name = name

            # Fill in column attribute names
            local_names: set[str] = set()
            for column_attr in model.columns:
                self.generate_column_attr_name(column_attr, global_names, local_names)
                local_names.add(column_attr.name)
            # No relationship names to generate (change 9)
        else:
            super().generate_model_name(model, global_names)

    # ------------------------------------------------------------------
    # Change 2: Lowercase __tablename__
    # ------------------------------------------------------------------

    def render_class_variables(self, model: ModelClass) -> str:
        variables = [f"__tablename__ = '{model.table.name.lower()}'"]

        table_args = self.render_table_args(model.table)
        if table_args:
            variables.append(f"__table_args__ = {table_args}")

        return "\n".join(variables)

    # ------------------------------------------------------------------
    # Change 2 + 4: Target schema + lowercase constraints in table_args
    # ------------------------------------------------------------------

    def render_table_args(self, table: Table) -> str:
        args: list[str] = []

        # Render constraints — always include PK and FK with explicit names
        for constraint in sorted(table.constraints, key=_constraint_sort_key):
            if isinstance(constraint, PrimaryKeyConstraint):
                args.append(self.render_constraint(constraint))
            elif isinstance(constraint, ForeignKeyConstraint):
                args.append(self.render_constraint(constraint))
            elif isinstance(constraint, UniqueConstraint):
                if len(constraint.columns) > 1 or not uses_default_name(constraint):
                    args.append(self.render_constraint(constraint))

        # Always include target schema
        schema_dict = f"{{'schema': '{self.target_schema}'}}"

        if args:
            rendered_args = f",\n{self.indentation}".join(args)
            return (
                f"(\n{self.indentation}{rendered_args},\n"
                f"{self.indentation}{schema_dict}\n)"
            )
        else:
            return schema_dict

    # ------------------------------------------------------------------
    # Change 2 + 4: Lowercase constraint names, target schema FK refs
    # ------------------------------------------------------------------

    def render_constraint(self, constraint: Constraint | ForeignKey) -> str:
        if isinstance(constraint, PrimaryKeyConstraint):
            # Lowercase column names and constraint name
            col_args = ", ".join(
                repr(col.name.lower()) for col in constraint.columns
            )
            name = constraint.name
            if name:
                return render_callable(
                    "PrimaryKeyConstraint", col_args, kwargs={"name": repr(name.lower())}
                )
            else:
                return render_callable("PrimaryKeyConstraint", col_args)

        elif isinstance(constraint, ForeignKeyConstraint):
            # Local columns (lowercase)
            local_cols = [col.name.lower() for col in constraint.columns]
            # Remote columns: target_schema.table.column (all lowercase)
            remote_cols = []
            for fk in constraint.elements:
                ref_table = fk.column.table.name.lower()
                ref_col = fk.column.name.lower()
                remote_cols.append(f"{self.target_schema}.{ref_table}.{ref_col}")

            kwargs: dict[str, Any] = {}
            if constraint.name:
                kwargs["name"] = repr(constraint.name.lower())

            # Add FK options
            for attr in "ondelete", "onupdate", "deferrable", "initially", "match":
                value = getattr(constraint, attr, None)
                if value:
                    kwargs[attr] = repr(value)

            return render_callable(
                "ForeignKeyConstraint",
                repr(local_cols),
                repr(remote_cols),
                kwargs=kwargs,
            )

        elif isinstance(constraint, ForeignKey):
            # Single FK reference — rewrite to target schema
            ref_table = constraint.column.table.name.lower()
            ref_col = constraint.column.name.lower()
            remote = f"{self.target_schema}.{ref_table}.{ref_col}"
            return render_callable("ForeignKey", repr(remote))

        else:
            # Fall back to parent for other constraints
            return super().render_constraint(constraint)

    # ------------------------------------------------------------------
    # Change 2 + 7: Lowercase column name, Identity() for autoincrement
    # ------------------------------------------------------------------

    def render_column_attribute(self, column_attr: ColumnAttribute) -> str:
        column = column_attr.column
        col_name_lower = column.name.lower()

        # Render Python type hint
        rendered_python_type = self.render_column_python_type(column)

        # Build mapped_column args
        args: list[str] = [repr(col_name_lower)]  # always show name (lowercase)
        kwargs: dict[str, Any] = {}

        # Column type
        args.append(self.render_column_type(column))

        # server_default handling (Identity, Computed, DefaultClause)
        if isinstance(column.server_default, Identity):
            identity_kwargs = self._render_identity_kwargs(column.server_default)
            args.append(render_callable("Identity", kwargs=identity_kwargs))
            self.add_literal_import("sqlalchemy", "Identity")
        elif isinstance(column.server_default, Computed):
            expression = str(column.server_default.sqltext)
            computed_kwargs: dict[str, Any] = {}
            if column.server_default.persisted is not None:
                computed_kwargs["persisted"] = column.server_default.persisted
            args.append(render_callable("Computed", repr(expression), kwargs=computed_kwargs))
            self.add_literal_import("sqlalchemy", "Computed")
        elif column.primary_key and getattr(column, "autoincrement", False):
            # Autoincrement PK without explicit Identity — emit Identity()
            args.append("Identity()")
            self.add_literal_import("sqlalchemy", "Identity")
        elif isinstance(column.server_default, DefaultClause):
            text_val = (
                column.server_default.arg.text
                if isinstance(column.server_default.arg, TextClause)
                else str(column.server_default.arg)
            )
            # Skip sequence defaults (handled by Identity above for autoincrement PKs)
            if not text_val.startswith("nextval("):
                kwargs["server_default"] = render_callable("text", repr(text_val))
                self.add_literal_import("sqlalchemy", "text")

        # Primary key
        if column.primary_key:
            kwargs["primary_key"] = True
        elif not column.nullable:
            kwargs["nullable"] = False

        rendered = render_callable("mapped_column", *args, kwargs=kwargs)
        return f"{column_attr.name}: Mapped[{rendered_python_type}] = {rendered}"

    @staticmethod
    def _render_identity_kwargs(identity: Identity) -> dict[str, Any]:
        """Extract non-default kwargs from an Identity object."""
        import inspect
        from decimal import Decimal
        from inspect import Parameter

        identity_kwargs: dict[str, Any] = {}
        for name, param in inspect.signature(Identity).parameters.items():
            if name == "self" or param.kind in (
                Parameter.VAR_POSITIONAL,
                Parameter.VAR_KEYWORD,
            ):
                continue
            value = getattr(identity, name, None)
            if value is None:
                continue
            if isinstance(value, Decimal):
                value = int(value)
            if param.default is not Parameter.empty and value == param.default:
                continue
            identity_kwargs[name] = value
        return identity_kwargs

    # ------------------------------------------------------------------
    # Change 5: Collation stripping
    # ------------------------------------------------------------------

    def fix_column_types(self, table: Any) -> None:
        super().fix_column_types(table)
        for column in table.c:
            collation = getattr(column.type, "collation", None)
            if collation:
                logger.warning(
                    "Column '%s': skipping collation '%s'",
                    column.name,
                    collation,
                )
                column.type.collation = None

    # ------------------------------------------------------------------
    # Change 6: Type fallback to String for unmapped dialect types
    # ------------------------------------------------------------------

    def get_adapted_type(self, coltype: Any) -> Any:
        type_name = type(coltype).__name__.upper()

        # Check MSSQL overrides first
        if type_name in _MSSQL_TYPE_OVERRIDES:
            target_name = _MSSQL_TYPE_OVERRIDES[type_name]
            import sqlalchemy as sa

            target_cls = getattr(sa, target_name)
            # Preserve length/precision if applicable
            if target_name == "String":
                length = getattr(coltype, "length", None)
                return target_cls(length) if length else target_cls()
            elif target_name == "Numeric":
                precision = getattr(coltype, "precision", None)
                scale = getattr(coltype, "scale", None)
                if precision is not None and scale is not None:
                    return target_cls(precision, scale)
                elif precision is not None:
                    return target_cls(precision)
                return target_cls()
            elif target_name == "LargeBinary":
                length = getattr(coltype, "length", None)
                return target_cls(length) if length else target_cls()
            else:
                return target_cls()

        # Try parent adaptation
        result = super().get_adapted_type(coltype)

        # If still dialect-specific after adaptation, fall back to String
        if result.__class__.__module__.startswith("sqlalchemy.dialects."):
            if isinstance(result, (UserDefinedType, TypeDecorator)):
                logger.warning(
                    "Unmapped type '%s', falling back to String",
                    type(result).__name__,
                )
                return String()
            logger.warning(
                "Unmapped type '%s', falling back to String",
                type(result).__name__,
            )
            return String()

        return result

    # ------------------------------------------------------------------
    # Override render_class to skip relationship section
    # ------------------------------------------------------------------

    def render_class(self, model: ModelClass) -> str:
        sections: list[str] = []

        # Render class variables
        class_vars = self.render_class_variables(model)
        if class_vars:
            sections.append(class_vars)

        # Render column attributes (non-nullable first, then nullable)
        rendered_columns: list[str] = []
        for nullable in (False, True):
            for column_attr in model.columns:
                if column_attr.column.nullable is nullable:
                    rendered_columns.append(
                        self.render_column_attribute(column_attr)
                    )

        if rendered_columns:
            sections.append("\n".join(rendered_columns))

        # No relationships (change 9)

        from textwrap import indent

        declaration = self.render_class_declaration(model)
        rendered_sections = "\n\n".join(
            indent(section, self.indentation) for section in sections
        )
        return f"{declaration}\n{rendered_sections}"


def _constraint_sort_key(constraint: Any) -> str:
    """Sort key for constraints — PK first, then FK, then others."""
    if isinstance(constraint, PrimaryKeyConstraint):
        return "0"
    elif isinstance(constraint, ForeignKeyConstraint):
        return "1" + (constraint.name or "")
    else:
        return "2" + (getattr(constraint, "name", "") or "")
