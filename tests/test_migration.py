"""Tests for smt.migration module."""

from __future__ import annotations

from pathlib import Path

import pytest

from smt.migration import MigrationManager, _COLLATION_PATTERNS, _OP_PATTERNS


class TestDdlSplitting:
    def test_split_ddl_by_table(self, tmp_path: Path):
        mgr = MigrationManager(
            workspace=tmp_path,
            target_url="sqlite:///test.db",
            target_schema="test_schema",
        )

        ddl = """\
-- Running upgrade  -> abc123

CREATE TABLE test_schema.users (
    id INTEGER NOT NULL,
    displayname VARCHAR(40) NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE test_schema.posts (
    id INTEGER NOT NULL,
    owneruserid INTEGER,
    PRIMARY KEY (id),
    FOREIGN KEY(owneruserid) REFERENCES test_schema.users (id)
);

CREATE INDEX ix_posts_owneruserid ON test_schema.posts (owneruserid);

UPDATE alembic_version SET version_num='abc123' WHERE alembic_version.version_num = 'prev';

"""
        result = mgr._split_ddl_by_table(ddl)

        assert "users" in result
        assert "posts" in result
        assert "CREATE TABLE" in result["users"]
        assert "CREATE TABLE" in result["posts"]
        assert "CREATE INDEX" in result["posts"]
        assert len(result) == 2  # no alembic_version

    def test_extract_table_from_create(self, tmp_path: Path):
        extract = MigrationManager._extract_table_from_ddl
        # Bare identifiers
        assert extract("CREATE TABLE schema.users (") == "users"
        assert extract("CREATE TABLE users (") == "users"
        # Double-quoted identifiers
        assert extract('CREATE TABLE "schema"."users" (') == "users"
        # Bracket-quoted identifiers (MSSQL)
        assert extract("CREATE TABLE [dbo].[users] (") == "users"
        assert extract("CREATE TABLE [users] (") == "users"

    def test_extract_table_from_index(self, tmp_path: Path):
        extract = MigrationManager._extract_table_from_ddl
        assert extract("CREATE INDEX ix_users_name ON schema.users (name)") == "users"
        assert extract('CREATE INDEX ix ON "schema"."users" (name)') == "users"
        assert extract("CREATE UNIQUE INDEX ix ON [dbo].[users] (name)") == "users"

    def test_extract_table_from_alter(self, tmp_path: Path):
        extract = MigrationManager._extract_table_from_ddl
        assert extract("ALTER TABLE schema.users ADD COLUMN email VARCHAR") == "users"
        assert extract('ALTER TABLE "schema"."users" ADD COLUMN email') == "users"
        assert extract("ALTER TABLE [dbo].[users] ADD COLUMN email") == "users"

    def test_generate_per_table_ddl_creates_files(self, tmp_path: Path):
        mgr = MigrationManager(
            workspace=tmp_path,
            target_url="sqlite:///test.db",
            target_schema="test_schema",
        )

        ddl = (
            "CREATE TABLE test_schema.users (\n"
            "    id INTEGER NOT NULL\n"
            ");\n\n"
            "CREATE TABLE test_schema.posts (\n"
            "    id INTEGER NOT NULL\n"
            ");\n"
        )
        mgr._generate_per_table_ddl(ddl)

        ddl_dir = tmp_path / "ddl"
        assert ddl_dir.exists()
        assert (ddl_dir / "users.sql").exists()
        assert (ddl_dir / "posts.sql").exists()
        assert "CREATE TABLE" in (ddl_dir / "users.sql").read_text()
        assert "CREATE TABLE" in (ddl_dir / "posts.sql").read_text()

    def test_generate_per_table_ddl_clears_stale_files(self, tmp_path: Path):
        mgr = MigrationManager(
            workspace=tmp_path,
            target_url="sqlite:///test.db",
            target_schema="test_schema",
        )

        # Create stale file from a previous run
        ddl_dir = tmp_path / "ddl"
        ddl_dir.mkdir()
        (ddl_dir / "old_table.sql").write_text("-- stale")

        ddl = (
            "CREATE TABLE test_schema.users (\n"
            "    id INTEGER NOT NULL\n"
            ");\n"
        )
        mgr._generate_per_table_ddl(ddl)

        assert (ddl_dir / "users.sql").exists()
        assert not (ddl_dir / "old_table.sql").exists()


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
