"""Tests for smt.migration module."""

from __future__ import annotations

from pathlib import Path

import pytest

from smt.migration import MigrationManager, _COLLATION_PATTERNS, _OP_PATTERNS


class TestOpPatterns:
    @pytest.mark.parametrize("line", [
        "    op.create_table('users',",
        "    op.drop_table('users',",
        "    op.add_column('users',",
        "    op.drop_column('users', 'email')",
        "    op.create_index('ix_users_name',",
        "    op.drop_index('ix_users_name',",
        "    op.alter_column('users', 'name',",
        "    op.create_foreign_key('fk_posts_users',",
    ])
    def test_detects_operations(self, line: str):
        assert _OP_PATTERNS.search(line) is not None

    def test_empty_migration_detected(self):
        content = """
def upgrade():
    pass

def downgrade():
    pass
"""
        assert _OP_PATTERNS.search(content) is None

    def test_non_empty_migration_detected(self):
        content = """
def upgrade():
    op.create_table('users',
        sa.Column('id', sa.Integer(), nullable=False),
    )
"""
        assert _OP_PATTERNS.search(content) is not None


class TestCollationStripping:
    def test_strip_comma_prefix_collation(self):
        text = "sa.String(40), collation='SQL_Latin1'"
        for pattern, replacement in _COLLATION_PATTERNS:
            text = pattern.sub(replacement, text)
        assert "collation" not in text
        assert text == "sa.String(40)"

    def test_strip_comma_suffix_collation(self):
        text = "collation='SQL_Latin1', nullable=False"
        for pattern, replacement in _COLLATION_PATTERNS:
            text = pattern.sub(replacement, text)
        assert "collation" not in text
        assert "nullable=False" in text

    def test_strip_parenthesized_collation(self):
        text = "sa.Unicode(collation='Latin1')"
        for pattern, replacement in _COLLATION_PATTERNS:
            text = pattern.sub(replacement, text)
        assert text == "sa.Unicode()"


class TestMigrationManager:
    def test_init_creates_workspace(self, tmp_path: Path):
        ws = tmp_path / "new_workspace"
        mgr = MigrationManager(
            workspace=ws,
            target_url="sqlite:///test.db",
            target_schema="test_schema",
        )
        # workspace not created until init_alembic
        assert not ws.exists()

    def test_get_current_revision_no_ini(self, tmp_path: Path):
        mgr = MigrationManager(
            workspace=tmp_path / "noexist",
            target_url="sqlite:///test.db",
            target_schema="test_schema",
        )
        assert mgr.get_current_revision() is None

    def test_get_head_revision_no_ini(self, tmp_path: Path):
        mgr = MigrationManager(
            workspace=tmp_path / "noexist",
            target_url="sqlite:///test.db",
            target_schema="test_schema",
        )
        assert mgr.get_head_revision() is None

    def test_get_history_no_ini(self, tmp_path: Path):
        mgr = MigrationManager(
            workspace=tmp_path / "noexist",
            target_url="sqlite:///test.db",
            target_schema="test_schema",
        )
        assert mgr.get_history() == []

    def test_is_migration_empty(self, tmp_path: Path):
        mgr = MigrationManager(
            workspace=tmp_path,
            target_url="sqlite:///test.db",
            target_schema="test_schema",
        )

        empty_file = tmp_path / "empty_migration.py"
        empty_file.write_text("def upgrade():\n    pass\n\ndef downgrade():\n    pass\n")
        assert mgr._is_migration_empty(empty_file)

        nonempty_file = tmp_path / "real_migration.py"
        nonempty_file.write_text("def upgrade():\n    op.create_table('t')\n")
        assert not mgr._is_migration_empty(nonempty_file)

    def test_strip_collations(self, tmp_path: Path):
        mgr = MigrationManager(
            workspace=tmp_path,
            target_url="sqlite:///test.db",
            target_schema="test_schema",
        )

        migration = tmp_path / "migration.py"
        migration.write_text(
            "    sa.Column('name', sa.String(40), collation='SQL_Latin1', nullable=False),\n"
            "    sa.Column('desc', sa.Unicode(collation='Latin1')),\n"
        )

        stripped = mgr._strip_collations(migration)
        assert stripped == 2

        content = migration.read_text()
        assert "collation" not in content
        assert "nullable=False" in content

    def test_count_operations(self, tmp_path: Path):
        mgr = MigrationManager(
            workspace=tmp_path,
            target_url="sqlite:///test.db",
            target_schema="test_schema",
        )

        migration = tmp_path / "migration.py"
        migration.write_text("""\
def upgrade():
    op.create_table('users')
    op.create_table('posts')
    op.add_column('users', sa.Column('email'))
    op.create_index('ix_users_email', 'users')
""")

        counts = mgr._count_operations(migration)
        assert counts["create_table"] == 2
        assert counts["add_column"] == 1
        assert counts["create_index"] == 1
