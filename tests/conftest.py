"""Shared test fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def tmp_workspace(tmp_path: Path) -> Path:
    """Provide a temporary workspace directory."""
    ws = tmp_path / "migration_workspace"
    ws.mkdir()
    return ws


@pytest.fixture
def sample_config_yaml(tmp_path: Path) -> Path:
    """Write a sample smt.yaml and return its path."""
    config = tmp_path / "smt.yaml"
    config.write_text("""\
source:
  dialect: postgresql
  host: localhost
  port: 5432
  user: postgres
  password: testpass
  database: SourceDB
  schema: dbo

target:
  dialect: postgresql
  host: localhost
  port: 5433
  user: postgres
  password: testpass
  database: TargetDB

tables:
  - Users
  - Posts

workspace: ./migration_workspace
""")
    return config
