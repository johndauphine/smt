"""Pipeline orchestrator — replaces migrate-all.sh."""

from __future__ import annotations

import logging
import time

import click

from smt.config import SmtConfig
from smt.database import DatabaseManager
from smt.migration import MigrationManager
from smt.models import ModelGenerator

logger = logging.getLogger(__name__)


class PipelineError(Exception):
    """Raised when a pipeline step fails."""

    def __init__(self, step: int, step_name: str, cause: Exception):
        self.step = step
        self.step_name = step_name
        self.cause = cause
        super().__init__(f"Pipeline failed at step {step} ({step_name}): {cause}")


class Pipeline:
    """Orchestrates the full migration pipeline."""

    def __init__(self, config: SmtConfig):
        self.config = config
        self.source_db = DatabaseManager(config.source)
        self.target_db = DatabaseManager(config.target)
        self.migration_mgr = MigrationManager(
            workspace=config.workspace,
            target_url=config.target.get_url_string(),
            target_schema=config.target_schema,
        )
        self.model_gen = ModelGenerator(
            source_engine=self.source_db.engine,
            target_schema=config.target_schema,
            source_schema=config.source.schema,
            source_database=config.source.database,
            tables=config.tables if config.tables != "all" else None,
        )

    def run(self, confirm: bool = True) -> None:
        """Run the full migration pipeline."""
        self._show_summary()

        if confirm:
            if not click.confirm("Proceed with migration?", default=False):
                click.echo("Migration cancelled")
                return

        start = time.time()
        steps = [
            (1, "Generate models", self._step_generate),
            (2, "Initialize Alembic", self._step_init),
            (3, "Create migration", self._step_create),
            (4, "Apply migration", self._step_apply),
        ]

        try:
            for num, name, func in steps:
                click.echo(f"\n{'=' * 42}")
                click.echo(f"Step {num}/4: {name}")
                click.echo(f"{'=' * 42}")
                func()
        except PipelineError as e:
            click.echo(f"\n{'=' * 42}")
            click.echo(f"ERROR: Pipeline failed at step {e.step} ({e.step_name})")
            click.echo(f"{'=' * 42}")
            click.echo(f"\nCause: {e.cause}")
            click.echo(f"\nYou can retry from this step with: smt {_STEP_COMMANDS[e.step]}")
            click.echo("Or rollback with: smt rollback")
            raise SystemExit(1) from e
        finally:
            self.source_db.dispose()
            self.target_db.dispose()

        duration = time.time() - start
        click.echo(f"\n{'=' * 42}")
        click.echo("Migration Complete")
        click.echo(f"{'=' * 42}")
        click.echo(f"\nDuration: {duration:.1f}s")
        click.echo(f"Workspace: {self.config.workspace}")

    def _show_summary(self) -> None:
        src = self.config.source
        tgt = self.config.target
        click.echo(f"{'=' * 42}")
        click.echo("Schema Migration Pipeline")
        click.echo(f"{'=' * 42}")
        click.echo(f"\nSource ({src.dialect}):")
        click.echo(f"  Host: {src.host}:{src.port}")
        click.echo(f"  Database: {src.database}")
        click.echo(f"  Schema: {src.schema}")
        click.echo(f"\nTarget ({tgt.dialect}):")
        click.echo(f"  Host: {tgt.host}:{tgt.port}")
        click.echo(f"  Database: {tgt.database}")
        click.echo(f"  Schema: {self.config.target_schema}")
        tables = self.config.tables
        if isinstance(tables, list):
            click.echo(f"\nTables: {', '.join(tables)}")
        else:
            click.echo("\nTables: all")
        click.echo(f"Workspace: {self.config.workspace}\n")

    def _step_generate(self) -> None:
        try:
            models_path = self.config.workspace / "models.py"
            self.model_gen.write(models_path)
        except Exception as e:
            raise PipelineError(1, "Generate models", e) from e

    def _step_init(self) -> None:
        try:
            self.migration_mgr.init_alembic()
            self.target_db.create_schema(self.config.target_schema)
        except Exception as e:
            raise PipelineError(2, "Initialize Alembic", e) from e

    def _step_create(self) -> None:
        try:
            self.migration_mgr.create_migration()
        except Exception as e:
            raise PipelineError(3, "Create migration", e) from e

    def _step_apply(self) -> None:
        try:
            self.migration_mgr.apply_migration()
            tables = self.target_db.list_tables(self.config.target_schema)
            if tables:
                click.echo(f"\nTables in {self.config.target_schema}:")
                for t in tables:
                    click.echo(f"  - {t}")
        except Exception as e:
            raise PipelineError(4, "Apply migration", e) from e


_STEP_COMMANDS = {
    1: "generate",
    2: "init",
    3: "create",
    4: "apply",
}
