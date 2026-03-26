"""Tests for smt.cli module."""

from __future__ import annotations

from click.testing import CliRunner

from smt.cli import cli


class TestCliGroup:
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Schema Migration Toolkit" in result.output

    def test_subcommands_listed(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        for cmd in ("migrate", "generate", "init", "create", "apply", "rollback", "status", "history"):
            assert cmd in result.output

    def test_migrate_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["migrate", "--help"])
        assert result.exit_code == 0
        assert "--yes" in result.output

    def test_apply_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["apply", "--help"])
        assert result.exit_code == 0
        assert "--dry-run" in result.output

    def test_rollback_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["rollback", "--help"])
        assert result.exit_code == 0
        assert "--steps" in result.output
        assert "--revision" in result.output
        assert "--drop-schema" in result.output

    def test_create_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["create", "--help"])
        assert result.exit_code == 0
        assert "--message" in result.output

    def test_missing_config_exits_with_error(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["-c", "nonexistent.yaml", "status"])
        assert result.exit_code != 0

    def test_log_level_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--log-level", "DEBUG", "--help"])
        assert result.exit_code == 0
