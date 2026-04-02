"""Model code generator — delegates to sqlacodegen_smt fork.

Thin wrapper around SmtGenerator that handles reflection, backup, and file I/O.
"""

from __future__ import annotations

import datetime
import logging
import shutil
from pathlib import Path

from sqlalchemy import MetaData, inspect as sa_inspect
from sqlalchemy.engine import Engine

from sqlacodegen_smt.generator import SmtGenerator

logger = logging.getLogger(__name__)


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

    def write(self, output_dir: Path) -> None:
        """Generate per-table model files in a models/ directory.

        Creates:
            output_dir/base.py       - DeclarativeBase class
            output_dir/<table>.py    - One file per table model
            output_dir/__init__.py   - Re-exports Base and all models
        """
        output_dir = Path(output_dir)
        self._backup(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Reflect metadata
        metadata = MetaData()
        table_names = self._resolve_table_names()
        metadata.reflect(
            bind=self.source_engine,
            schema=self.source_schema,
            only=table_names or None,
        )

        if not metadata.tables:
            raise RuntimeError(
                f"No tables found in schema '{self.source_schema}'"
                + (f" matching {self.tables}" if self.tables else "")
            )

        # Generate via SmtGenerator
        generator = SmtGenerator(
            metadata=metadata,
            bind=self.source_engine,
            options=(),
            target_schema=self.target_schema,
            source_database=self.source_database,
            source_schema=self.source_schema,
        )
        files = generator.generate()

        # Write files
        for filename, content in files.items():
            (output_dir / filename).write_text(content)

        model_count = sum(1 for f in files if f not in ("base.py", "__init__.py"))
        logger.info("Generated %d model file(s) in models/", model_count)

    def _resolve_table_names(self) -> list[str] | None:
        """Resolve requested table names against source database."""
        if not self.tables:
            return None

        inspector = sa_inspect(self.source_engine)
        available = inspector.get_table_names(schema=self.source_schema)
        available_lower = {t.lower(): t for t in available}

        selected = []
        for t in self.tables:
            actual = available_lower.get(t.lower())
            if actual:
                selected.append(actual)
            else:
                logger.warning(
                    "Table '%s' not found in schema '%s'", t, self.source_schema
                )

        return selected or None

    def _backup(self, output_dir: Path) -> None:
        """Back up existing models/ directory and legacy models.py."""
        workspace = output_dir.parent
        dir_name = output_dir.name
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        # Back up existing models/ directory
        if output_dir.is_dir():
            backup_dir = workspace / f"{dir_name}_{timestamp}.bak"
            shutil.copytree(output_dir, backup_dir)
            logger.info("Backed up existing %s/ to %s", dir_name, backup_dir.name)
            shutil.rmtree(output_dir)
        elif output_dir.exists():
            backup_path = workspace / f"{dir_name}_{timestamp}.file.bak"
            shutil.copy2(output_dir, backup_path)
            output_dir.unlink()
            logger.info("Backed up existing file %s to %s", dir_name, backup_path.name)

        # Back up and remove legacy models.py if present
        if dir_name == "models":
            legacy_file = workspace / "models.py"
            if legacy_file.is_file():
                backup_path = workspace / f"models_{timestamp}.py.bak"
                shutil.copy2(legacy_file, backup_path)
                legacy_file.unlink()
                logger.info("Backed up legacy models.py to %s", backup_path.name)
