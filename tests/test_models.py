"""Tests for smt.models module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.types import DateTime, Integer, String, Text

from smt.models import ModelGenerator


def _mock_inspector(tables: dict):
    """Create a mock inspector with given table definitions.

    tables = {
        "Users": {
            "columns": [
                {"name": "Id", "type": Integer(), "nullable": False, "autoincrement": True},
                {"name": "DisplayName", "type": String(40), "nullable": False, "autoincrement": False},
            ],
            "pk": {"constrained_columns": ["Id"], "name": "PK_Users"},
            "fks": [],
        }
    }
    """
    inspector = MagicMock()
    inspector.get_table_names.return_value = list(tables.keys())

    def get_columns(table_name, schema=None):
        return tables[table_name]["columns"]

    def get_pk_constraint(table_name, schema=None):
        return tables[table_name]["pk"]

    def get_foreign_keys(table_name, schema=None):
        return tables[table_name].get("fks", [])

    inspector.get_columns = get_columns
    inspector.get_pk_constraint = get_pk_constraint
    inspector.get_foreign_keys = get_foreign_keys

    return inspector


class TestModelGenerator:
    @patch("smt.models.sa_inspect")
    def test_basic_table(self, mock_inspect):
        mock_inspect.return_value = _mock_inspector({
            "Users": {
                "columns": [
                    {"name": "Id", "type": Integer(), "nullable": False, "autoincrement": True},
                    {"name": "DisplayName", "type": String(40), "nullable": False, "autoincrement": False},
                    {"name": "AboutMe", "type": Text(), "nullable": True, "autoincrement": False},
                ],
                "pk": {"constrained_columns": ["Id"], "name": "PK_Users"},
                "fks": [],
            }
        })

        engine = MagicMock()
        gen = ModelGenerator(
            source_engine=engine,
            target_schema="dw__testdb__dbo",
            source_schema="dbo",
            source_database="TestDB",
        )
        output = gen.generate()

        # Check class structure
        assert "class Users(Base):" in output
        assert "__tablename__ = 'users'" in output
        assert "'schema': 'dw__testdb__dbo'" in output

        # Check PK constraint is lowercase
        assert "PrimaryKeyConstraint('id', name='pk_users')" in output

        # Check column definitions
        assert "Id: Mapped[int] = mapped_column('id', Integer, Identity(), primary_key=True)" in output
        assert "DisplayName: Mapped[str] = mapped_column('displayname', String(40), nullable=False)" in output
        assert "AboutMe: Mapped[Optional[str]] = mapped_column('aboutme', Text)" in output

        # Check imports
        assert "from sqlalchemy import" in output
        assert "DeclarativeBase" in output
        assert "from typing import Optional" in output

        # Check header
        assert "Auto-generated SQLAlchemy models" in output
        assert "Source: TestDB.dbo" in output
        assert "Target: dw__testdb__dbo" in output

    @patch("smt.models.sa_inspect")
    def test_foreign_key(self, mock_inspect):
        mock_inspect.return_value = _mock_inspector({
            "Posts": {
                "columns": [
                    {"name": "Id", "type": Integer(), "nullable": False, "autoincrement": True},
                    {"name": "OwnerUserId", "type": Integer(), "nullable": True, "autoincrement": False},
                ],
                "pk": {"constrained_columns": ["Id"], "name": "PK_Posts"},
                "fks": [
                    {
                        "constrained_columns": ["OwnerUserId"],
                        "referred_schema": "dbo",
                        "referred_table": "Users",
                        "referred_columns": ["Id"],
                        "name": "FK_Posts_Users",
                    }
                ],
            }
        })

        engine = MagicMock()
        gen = ModelGenerator(
            source_engine=engine,
            target_schema="dw__testdb__dbo",
            source_schema="dbo",
            source_database="TestDB",
        )
        output = gen.generate()

        assert "ForeignKeyConstraint(['owneruserid'], ['dw__testdb__dbo.users.id'], name='fk_posts_users')" in output

    @patch("smt.models.sa_inspect")
    def test_table_filter(self, mock_inspect):
        mock_inspect.return_value = _mock_inspector({
            "Users": {
                "columns": [
                    {"name": "Id", "type": Integer(), "nullable": False, "autoincrement": True},
                ],
                "pk": {"constrained_columns": ["Id"], "name": "PK_Users"},
            },
            "Posts": {
                "columns": [
                    {"name": "Id", "type": Integer(), "nullable": False, "autoincrement": True},
                ],
                "pk": {"constrained_columns": ["Id"], "name": "PK_Posts"},
            },
        })

        engine = MagicMock()
        gen = ModelGenerator(
            source_engine=engine,
            target_schema="dw__testdb__dbo",
            source_schema="dbo",
            source_database="TestDB",
            tables=["Users"],
        )
        output = gen.generate()

        assert "class Users(Base):" in output
        assert "class Posts(Base):" not in output

    @patch("smt.models.sa_inspect")
    def test_backup_on_write(self, mock_inspect, tmp_workspace: Path):
        mock_inspect.return_value = _mock_inspector({
            "T": {
                "columns": [
                    {"name": "Id", "type": Integer(), "nullable": False, "autoincrement": True},
                ],
                "pk": {"constrained_columns": ["Id"], "name": "PK_T"},
            },
        })

        models_path = tmp_workspace / "models.py"
        models_path.write_text("# old content")

        engine = MagicMock()
        gen = ModelGenerator(
            source_engine=engine,
            target_schema="dw__testdb__dbo",
            source_schema="dbo",
            source_database="TestDB",
        )
        gen.write(models_path)

        # Original should be regenerated
        assert "class T(Base):" in models_path.read_text()

        # Backup should exist
        backups = list(tmp_workspace.glob("models_*.py.bak"))
        assert len(backups) == 1
        assert backups[0].read_text() == "# old content"

    @patch("smt.models.sa_inspect")
    def test_no_tables_found(self, mock_inspect):
        mock_inspect.return_value = _mock_inspector({})

        engine = MagicMock()
        gen = ModelGenerator(
            source_engine=engine,
            target_schema="dw__testdb__dbo",
            source_schema="dbo",
            source_database="TestDB",
        )

        with pytest.raises(RuntimeError, match="No tables found"):
            gen.generate()

    @patch("smt.models.sa_inspect")
    def test_datetime_columns(self, mock_inspect):
        mock_inspect.return_value = _mock_inspector({
            "Events": {
                "columns": [
                    {"name": "Id", "type": Integer(), "nullable": False, "autoincrement": True},
                    {"name": "CreatedAt", "type": DateTime(), "nullable": False, "autoincrement": False},
                ],
                "pk": {"constrained_columns": ["Id"], "name": "PK_Events"},
            },
        })

        engine = MagicMock()
        gen = ModelGenerator(
            source_engine=engine,
            target_schema="dw__testdb__dbo",
            source_schema="dbo",
            source_database="TestDB",
        )
        output = gen.generate()

        assert "import datetime" in output
        assert "Mapped[datetime.datetime]" in output
        assert "DateTime" in output
