"""Tests for smt.database module."""

from __future__ import annotations

from smt.config import DatabaseConfig
from smt.database import DatabaseManager, _CREATE_SCHEMA_DDL, _DROP_SCHEMA_DDL


class TestSchemaDialectDDL:
    def test_pg_create_schema_ddl(self):
        ddl = _CREATE_SCHEMA_DDL["postgresql"].format(schema="test_schema")
        assert ddl == "CREATE SCHEMA IF NOT EXISTS test_schema"

    def test_pg_drop_schema_ddl(self):
        ddl = _DROP_SCHEMA_DDL["postgresql"].format(schema="test_schema")
        assert ddl == "DROP SCHEMA IF EXISTS test_schema CASCADE"

    def test_mssql_create_schema_ddl(self):
        ddl = _CREATE_SCHEMA_DDL["mssql"].format(schema="test_schema")
        assert "sys.schemas" in ddl
        assert "CREATE SCHEMA [test_schema]" in ddl

    def test_mssql_drop_schema_ddl(self):
        ddl = _DROP_SCHEMA_DDL["mssql"].format(schema="test_schema")
        assert "sys.schemas" in ddl
        assert "DROP SCHEMA [test_schema]" in ddl

    def test_all_supported_dialects_have_ddl(self):
        from smt.config import SUPPORTED_DIALECTS

        for dialect in SUPPORTED_DIALECTS:
            assert dialect in _CREATE_SCHEMA_DDL, f"Missing CREATE DDL for {dialect}"
            assert dialect in _DROP_SCHEMA_DDL, f"Missing DROP DDL for {dialect}"


class TestDatabaseManager:
    def test_engine_created_lazily(self):
        cfg = DatabaseConfig(
            dialect="postgresql", host="localhost", port=5432,
            user="u", password="p", database="db",
        )
        mgr = DatabaseManager(cfg)
        assert mgr._engine is None

    def test_list_tables_returns_sorted(self):
        """Verify list_tables sorts results (using mock)."""
        from unittest.mock import MagicMock, patch, PropertyMock

        cfg = DatabaseConfig(
            dialect="postgresql", host="localhost", port=5432,
            user="u", password="p", database="db",
        )
        mgr = DatabaseManager(cfg)

        mock_inspector = MagicMock()
        mock_inspector.get_table_names.return_value = ["zebra", "alpha", "middle"]

        with patch("smt.database.inspect", return_value=mock_inspector):
            with patch.object(type(mgr), "engine", new_callable=PropertyMock):
                tables = mgr.list_tables("test_schema")

        assert tables == ["alpha", "middle", "zebra"]
