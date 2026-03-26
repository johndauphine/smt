"""Tests for smt.config module."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from smt.config import ConfigError, DatabaseConfig, SmtConfig, load_config


class TestDatabaseConfig:
    def test_default_driver_postgresql(self):
        cfg = DatabaseConfig(
            dialect="postgresql", host="localhost", port=5432,
            user="pg", password="pw", database="db",
        )
        assert cfg.driver == "psycopg2"

    def test_default_driver_mssql(self):
        cfg = DatabaseConfig(
            dialect="mssql", host="localhost", port=1433,
            user="sa", password="pw", database="db",
        )
        assert cfg.driver == "pyodbc"

    def test_unsupported_dialect(self):
        with pytest.raises(ConfigError, match="Unsupported dialect"):
            DatabaseConfig(
                dialect="mysql", host="localhost", port=3306,
                user="u", password="p", database="db",
            )

    def test_custom_driver(self):
        cfg = DatabaseConfig(
            dialect="postgresql", host="localhost", port=5432,
            user="pg", password="pw", database="db", driver="asyncpg",
        )
        assert cfg.driver == "asyncpg"

    def test_get_url(self):
        cfg = DatabaseConfig(
            dialect="postgresql", host="myhost", port=5432,
            user="pg", password="p@ss#word", database="testdb",
        )
        url = cfg.get_url()
        assert "postgresql+psycopg2" in str(url)
        assert "myhost" in str(url)
        assert "5432" in str(url)
        assert "testdb" in str(url)


class TestSmtConfig:
    def test_target_schema_derived(self):
        source = DatabaseConfig(
            dialect="postgresql", host="h", port=5432,
            user="u", password="p", database="StackOverflow2010", schema="dbo",
        )
        target = DatabaseConfig(
            dialect="postgresql", host="h", port=5432,
            user="u", password="p", database="targetdb",
        )
        cfg = SmtConfig(source=source, target=target, tables="all", workspace=Path("."))
        assert cfg.target_schema == "dw__stackoverflow2010__dbo"

    def test_schema_name_too_long_for_pg(self):
        source = DatabaseConfig(
            dialect="postgresql", host="h", port=5432,
            user="u", password="p",
            database="A" * 50, schema="B" * 20,
        )
        target = DatabaseConfig(
            dialect="postgresql", host="h", port=5432,
            user="u", password="p", database="targetdb",
        )
        with pytest.raises(ConfigError, match="identifier limit"):
            SmtConfig(source=source, target=target, tables="all", workspace=Path("."))


class TestLoadConfig:
    def test_load_valid(self, sample_config_yaml: Path):
        cfg = load_config(sample_config_yaml)
        assert cfg.source.dialect == "postgresql"
        assert cfg.source.database == "SourceDB"
        assert cfg.source.schema == "dbo"
        assert cfg.target.database == "TargetDB"
        assert cfg.tables == ["Users", "Posts"]
        assert cfg.target_schema == "dw__sourcedb__dbo"

    def test_missing_file(self, tmp_path: Path):
        with pytest.raises(ConfigError, match="not found"):
            load_config(tmp_path / "nonexistent.yaml")

    def test_missing_source_section(self, tmp_path: Path):
        cfg_file = tmp_path / "smt.yaml"
        cfg_file.write_text("target:\n  dialect: postgresql\n  host: h\n  user: u\n  password: p\n  database: d\n")
        with pytest.raises(ConfigError, match="source"):
            load_config(cfg_file)

    def test_missing_source_schema(self, tmp_path: Path):
        cfg_file = tmp_path / "smt.yaml"
        cfg_file.write_text("""\
source:
  dialect: postgresql
  host: h
  user: u
  password: p
  database: d
target:
  dialect: postgresql
  host: h
  user: u
  password: p
  database: d
""")
        with pytest.raises(ConfigError, match="schema"):
            load_config(cfg_file)

    def test_env_var_override(self, sample_config_yaml: Path, monkeypatch):
        monkeypatch.setenv("SMT_SOURCE_PASSWORD", "env_secret")
        monkeypatch.setenv("SMT_TARGET_PASSWORD", "env_target_secret")
        cfg = load_config(sample_config_yaml)
        assert cfg.source.password == "env_secret"
        assert cfg.target.password == "env_target_secret"

    def test_tables_all_string(self, tmp_path: Path):
        cfg_file = tmp_path / "smt.yaml"
        cfg_file.write_text("""\
source:
  dialect: postgresql
  host: h
  user: u
  password: p
  database: d
  schema: public
target:
  dialect: postgresql
  host: h
  user: u
  password: p
  database: d
tables: all
""")
        cfg = load_config(cfg_file)
        assert cfg.tables == "all"

    def test_default_port_postgresql(self, tmp_path: Path):
        cfg_file = tmp_path / "smt.yaml"
        cfg_file.write_text("""\
source:
  dialect: postgresql
  host: h
  user: u
  password: p
  database: d
  schema: s
target:
  dialect: postgresql
  host: h
  user: u
  password: p
  database: d
""")
        cfg = load_config(cfg_file)
        assert cfg.source.port == 5432

    def test_default_port_mssql(self, tmp_path: Path):
        cfg_file = tmp_path / "smt.yaml"
        cfg_file.write_text("""\
source:
  dialect: mssql
  host: h
  user: u
  password: p
  database: d
  schema: dbo
target:
  dialect: mssql
  host: h
  user: u
  password: p
  database: d
""")
        cfg = load_config(cfg_file)
        assert cfg.source.port == 1433
