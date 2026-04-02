"""Tests for smt.models module (ModelGenerator wrapper)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    DateTime,
    ForeignKeyConstraint,
    create_engine,
)

from smt.models import ModelGenerator


def _patch_reflect(tables: dict, source_schema: str = "dbo"):
    """Create a patched MetaData.reflect that populates tables from a dict.

    tables = {
        "Users": {
            "columns": [Column("Id", Integer, primary_key=True, autoincrement=True), ...],
            "constraints": [PrimaryKeyConstraint("Id", name="PK_Users"), ...],
        }
    }
    """
    def side_effect(*, bind, schema=None, only=None):
        """Mock MetaData.reflect — adds real Table objects to the metadata."""
        # Get the metadata instance (self) that reflect was called on
        md = reflect_mock._metadata_ref

        for table_name, defn in tables.items():
            if only and table_name not in only:
                continue
            Table(
                table_name,
                md,
                *defn.get("columns", []),
                *defn.get("constraints", []),
                schema=schema,
            )

    reflect_mock = MagicMock(side_effect=side_effect)
    return reflect_mock


class TestModelGenerator:
    def _make_gen_and_write(self, tables, tmp_workspace, *, table_filter=None):
        """Helper: create ModelGenerator, mock reflection, call write()."""
        engine = create_engine("sqlite://")

        gen = ModelGenerator(
            source_engine=engine,
            target_schema="dw__testdb__dbo",
            source_schema="dbo",
            source_database="TestDB",
            tables=table_filter,
        )

        models_dir = tmp_workspace / "models"

        # Patch MetaData.reflect to inject our table definitions
        def patched_reflect(self_md, *, bind=None, schema=None, only=None, **kw):
            for table_name, defn in tables.items():
                if only and table_name not in only:
                    continue
                Table(
                    table_name,
                    self_md,
                    *defn.get("columns", []),
                    *defn.get("constraints", []),
                    schema=schema,
                )

        with patch.object(MetaData, "reflect", patched_reflect):
            gen.write(models_dir)

        return models_dir

    def test_basic_table(self, tmp_workspace: Path):
        models_dir = self._make_gen_and_write({
            "Users": {
                "columns": [
                    Column("Id", Integer, primary_key=True, autoincrement=True),
                    Column("DisplayName", String(40), nullable=False),
                    Column("AboutMe", Text(), nullable=True),
                ],
                "constraints": [
                    PrimaryKeyConstraint("Id", name="PK_Users"),
                ],
            }
        }, tmp_workspace)

        # Check file structure
        assert (models_dir / "base.py").exists()
        assert (models_dir / "users.py").exists()
        assert (models_dir / "__init__.py").exists()

        content = (models_dir / "users.py").read_text()

        # Check class structure
        assert "class Users(Base):" in content
        assert "__tablename__ = 'users'" in content
        assert "'schema': 'dw__testdb__dbo'" in content

        # Check PK constraint is lowercase
        assert "'pk_users'" in content

        # Check column definitions
        assert "Id:" in content
        assert "'id'" in content
        assert "Identity()" in content
        assert "primary_key=True" in content
        assert "DisplayName:" in content
        assert "'displayname'" in content
        assert "AboutMe:" in content
        assert "Optional[" in content

        # Check imports
        assert "from sqlalchemy import" in content
        assert "from .base import Base" in content

        # Check header
        assert "Auto-generated SQLAlchemy model: Users" in content
        assert "Source: TestDB.dbo" in content
        assert "Target: dw__testdb__dbo" in content

    def test_foreign_key(self, tmp_workspace: Path):
        models_dir = self._make_gen_and_write({
            "Users": {
                "columns": [Column("Id", Integer, primary_key=True)],
                "constraints": [PrimaryKeyConstraint("Id", name="PK_Users")],
            },
            "Posts": {
                "columns": [
                    Column("Id", Integer, primary_key=True),
                    Column("OwnerUserId", Integer, nullable=True),
                ],
                "constraints": [
                    PrimaryKeyConstraint("Id", name="PK_Posts"),
                    ForeignKeyConstraint(
                        ["OwnerUserId"],
                        ["dbo.Users.Id"],
                        name="FK_Posts_Users",
                    ),
                ],
            },
        }, tmp_workspace)

        content = (models_dir / "posts.py").read_text()
        assert "dw__testdb__dbo.users.id" in content
        assert "fk_posts_users" in content.lower()

    def test_table_filter(self, tmp_workspace: Path):
        """Only requested tables are generated."""
        engine = create_engine("sqlite://")

        gen = ModelGenerator(
            source_engine=engine,
            target_schema="dw__testdb__dbo",
            source_schema="dbo",
            source_database="TestDB",
            tables=["Users"],
        )

        models_dir = tmp_workspace / "models"

        tables = {
            "Users": {
                "columns": [Column("Id", Integer, primary_key=True, autoincrement=True)],
                "constraints": [PrimaryKeyConstraint("Id", name="PK_Users")],
            },
            "Posts": {
                "columns": [Column("Id", Integer, primary_key=True, autoincrement=True)],
                "constraints": [PrimaryKeyConstraint("Id", name="PK_Posts")],
            },
        }

        # Mock both sa_inspect (for _resolve_table_names) and MetaData.reflect
        mock_inspector = MagicMock()
        mock_inspector.get_table_names.return_value = ["Users", "Posts"]

        def patched_reflect(self_md, *, bind=None, schema=None, only=None, **kw):
            for table_name, defn in tables.items():
                if only and table_name not in only:
                    continue
                Table(
                    table_name,
                    self_md,
                    *defn.get("columns", []),
                    *defn.get("constraints", []),
                    schema=schema,
                )

        with (
            patch("smt.models.sa_inspect", return_value=mock_inspector),
            patch.object(MetaData, "reflect", patched_reflect),
        ):
            gen.write(models_dir)

        assert (models_dir / "users.py").exists()
        assert not (models_dir / "posts.py").exists()

    def test_backup_on_write(self, tmp_workspace: Path):
        models_dir = tmp_workspace / "models"
        models_dir.mkdir()
        (models_dir / "old_file.py").write_text("# old content")

        self._make_gen_and_write({
            "T": {
                "columns": [Column("Id", Integer, primary_key=True, autoincrement=True)],
                "constraints": [PrimaryKeyConstraint("Id", name="PK_T")],
            },
        }, tmp_workspace)

        # New files should exist
        assert (models_dir / "base.py").exists()
        assert (models_dir / "t.py").exists()
        assert (models_dir / "__init__.py").exists()

        # Old file should be gone
        assert not (models_dir / "old_file.py").exists()

        # Backup directory should exist
        backups = list(tmp_workspace.glob("models_*.bak"))
        assert len(backups) == 1
        assert (backups[0] / "old_file.py").read_text() == "# old content"

    def test_legacy_models_py_backup(self, tmp_workspace: Path):
        """Legacy models.py is backed up when writing models/ directory."""
        # Create legacy models.py
        legacy = tmp_workspace / "models.py"
        legacy.write_text("# legacy models")

        self._make_gen_and_write({
            "T": {
                "columns": [Column("Id", Integer, primary_key=True, autoincrement=True)],
                "constraints": [PrimaryKeyConstraint("Id", name="PK_T")],
            },
        }, tmp_workspace)

        # Legacy file should be gone
        assert not legacy.exists()

        # Backup should exist
        backups = list(tmp_workspace.glob("models_*.py.bak"))
        assert len(backups) == 1
        assert backups[0].read_text() == "# legacy models"

    def test_per_table_files(self, tmp_workspace: Path):
        """write() produces per-table model files in a models/ directory."""
        models_dir = self._make_gen_and_write({
            "Users": {
                "columns": [
                    Column("Id", Integer, primary_key=True, autoincrement=True),
                    Column("DisplayName", String(40), nullable=False),
                ],
                "constraints": [PrimaryKeyConstraint("Id", name="PK_Users")],
            },
            "Posts": {
                "columns": [Column("Id", Integer, primary_key=True, autoincrement=True)],
                "constraints": [PrimaryKeyConstraint("Id", name="PK_Posts")],
            },
        }, tmp_workspace)

        # Check directory structure
        assert (models_dir / "base.py").exists()
        assert (models_dir / "__init__.py").exists()
        assert (models_dir / "users.py").exists()
        assert (models_dir / "posts.py").exists()

        # Check base.py content
        base_content = (models_dir / "base.py").read_text()
        assert "class Base(DeclarativeBase):" in base_content

        # Check users.py content
        users_content = (models_dir / "users.py").read_text()
        assert "class Users(Base):" in users_content
        assert "from .base import Base" in users_content
        assert "__tablename__ = 'users'" in users_content
        assert "'pk_users'" in users_content

        # Check posts.py content
        posts_content = (models_dir / "posts.py").read_text()
        assert "class Posts(Base):" in posts_content

        # Check __init__.py content
        init_content = (models_dir / "__init__.py").read_text()
        assert "from .base import Base" in init_content
        assert "from .users import Users" in init_content
        assert "from .posts import Posts" in init_content

    def test_keyword_escaping(self, tmp_workspace: Path):
        """Python keywords in column names get underscore suffix."""
        models_dir = self._make_gen_and_write({
            "Badges": {
                "columns": [
                    Column("Id", Integer, primary_key=True, autoincrement=True),
                    Column("Class", Integer, nullable=False),
                ],
                "constraints": [PrimaryKeyConstraint("Id", name="PK_Badges")],
            },
        }, tmp_workspace)

        content = (models_dir / "badges.py").read_text()
        # 'Class' is a keyword — attribute should be escaped, DB column name preserved
        assert "Class_:" in content
        assert "'class'" in content
        assert "class Badges(Base):" in content

    def test_no_tables_found(self, tmp_workspace: Path):
        engine = create_engine("sqlite://")
        gen = ModelGenerator(
            source_engine=engine,
            target_schema="dw__testdb__dbo",
            source_schema="dbo",
            source_database="TestDB",
        )

        models_dir = tmp_workspace / "models"

        def patched_reflect(self_md, *, bind=None, schema=None, only=None, **kw):
            pass  # No tables added

        with patch.object(MetaData, "reflect", patched_reflect):
            with pytest.raises(RuntimeError, match="No tables found"):
                gen.write(models_dir)

    def test_datetime_columns(self, tmp_workspace: Path):
        models_dir = self._make_gen_and_write({
            "Events": {
                "columns": [
                    Column("Id", Integer, primary_key=True, autoincrement=True),
                    Column("CreatedAt", DateTime(), nullable=False),
                ],
                "constraints": [PrimaryKeyConstraint("Id", name="PK_Events")],
            },
        }, tmp_workspace)

        content = (models_dir / "events.py").read_text()
        assert "import datetime" in content
        assert "datetime.datetime" in content
        assert "DateTime" in content
