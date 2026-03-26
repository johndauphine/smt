"""Click CLI entry point for SMT."""

from __future__ import annotations

import logging
import sys

import click

from smt.config import ConfigError, load_config


def _setup_logging(log_level: str) -> None:
    """Configure logging with timestamped output."""
    level = getattr(logging, log_level.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)

    # Guard against duplicate handlers when called multiple times
    if not root.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        root.addHandler(handler)

    # SQLAlchemy echo at DEBUG
    if level <= logging.DEBUG:
        logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
        logging.getLogger("alembic").setLevel(logging.DEBUG)


@click.group()
@click.option(
    "--config", "-c",
    default="smt.yaml",
    type=click.Path(),
    help="Path to YAML config file.",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    default="INFO",
    help="Logging verbosity.",
)
@click.pass_context
def cli(ctx: click.Context, config: str, log_level: str) -> None:
    """SMT — Schema Migration Toolkit."""
    _setup_logging(log_level)
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config


def _load(ctx: click.Context):
    """Load config from context, exit on error."""
    try:
        return load_config(ctx.obj["config_path"])
    except ConfigError as e:
        click.echo(f"Configuration error: {e}", err=True)
        raise SystemExit(1) from e


# ---------------------------------------------------------------------------
# migrate — full pipeline
# ---------------------------------------------------------------------------
@cli.command()
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt.")
@click.pass_context
def migrate(ctx: click.Context, yes: bool) -> None:
    """Run the full migration pipeline."""
    config = _load(ctx)
    from smt.pipeline import Pipeline

    pipeline = Pipeline(config)
    pipeline.run(confirm=not yes)


# ---------------------------------------------------------------------------
# generate — step 1
# ---------------------------------------------------------------------------
@cli.command()
@click.pass_context
def generate(ctx: click.Context) -> None:
    """Generate SQLAlchemy models from source database."""
    config = _load(ctx)
    from smt.database import DatabaseManager
    from smt.models import ModelGenerator

    source_db = DatabaseManager(config.source)
    try:
        gen = ModelGenerator(
            source_engine=source_db.engine,
            target_schema=config.target_schema,
            source_schema=config.source.schema,
            source_database=config.source.database,
            tables=config.tables if config.tables != "all" else None,
        )
        gen.write(config.workspace / "models")
    finally:
        source_db.dispose()


# ---------------------------------------------------------------------------
# init — step 2
# ---------------------------------------------------------------------------
@cli.command("init")
@click.pass_context
def init_cmd(ctx: click.Context) -> None:
    """Initialize Alembic and create target schema."""
    config = _load(ctx)
    from smt.database import DatabaseManager
    from smt.migration import MigrationManager

    mgr = MigrationManager(
        workspace=config.workspace,
        target_url=config.target.get_url_string(),
        target_schema=config.target_schema,
    )
    mgr.init_alembic()

    target_db = DatabaseManager(config.target)
    try:
        target_db.create_schema(config.target_schema)
    finally:
        target_db.dispose()


# ---------------------------------------------------------------------------
# create — step 3
# ---------------------------------------------------------------------------
@cli.command()
@click.option("--message", "-m", default="Schema migration", help="Migration message.")
@click.pass_context
def create(ctx: click.Context, message: str) -> None:
    """Create an Alembic migration from model differences."""
    config = _load(ctx)
    from smt.migration import MigrationManager

    mgr = MigrationManager(
        workspace=config.workspace,
        target_url=config.target.get_url_string(),
        target_schema=config.target_schema,
    )
    result = mgr.create_migration(message)
    if result is None:
        click.echo("Already in sync — no migration needed")
    else:
        click.echo(f"Migration created: {result}")


# ---------------------------------------------------------------------------
# apply — step 4
# ---------------------------------------------------------------------------
@cli.command()
@click.option("--dry-run", is_flag=True, help="Generate DDL SQL without applying.")
@click.pass_context
def apply(ctx: click.Context, dry_run: bool) -> None:
    """Apply pending migrations to the target database."""
    config = _load(ctx)
    from smt.database import DatabaseManager
    from smt.migration import MigrationManager

    mgr = MigrationManager(
        workspace=config.workspace,
        target_url=config.target.get_url_string(),
        target_schema=config.target_schema,
    )
    ddl_path = mgr.apply_migration(dry_run=dry_run)

    if ddl_path and not dry_run:
        target_db = DatabaseManager(config.target)
        try:
            tables = target_db.list_tables(config.target_schema)
            if tables:
                click.echo(f"\nTables in {config.target_schema}:")
                for t in tables:
                    click.echo(f"  - {t}")
        finally:
            target_db.dispose()


# ---------------------------------------------------------------------------
# rollback
# ---------------------------------------------------------------------------
@cli.command()
@click.option("--steps", "-n", type=int, help="Number of revisions to rollback.")
@click.option("--revision", "-r", type=str, help="Target revision hash.")
@click.option("--drop-schema", is_flag=True, help="Drop target schema after rollback.")
@click.pass_context
def rollback(ctx: click.Context, steps: int | None, revision: str | None, drop_schema: bool) -> None:
    """Rollback migrations."""
    config = _load(ctx)
    from smt.database import DatabaseManager
    from smt.migration import MigrationManager

    mgr = MigrationManager(
        workspace=config.workspace,
        target_url=config.target.get_url_string(),
        target_schema=config.target_schema,
    )

    if revision:
        target = revision
    elif steps:
        target = f"-{steps}"
    else:
        target = "base"

    mgr.rollback(target)

    if drop_schema:
        target_db = DatabaseManager(config.target)
        try:
            target_db.drop_schema(config.target_schema)
            click.echo(f"Schema '{config.target_schema}' dropped")
        finally:
            target_db.dispose()


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------
@cli.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Show current migration status."""
    config = _load(ctx)
    from smt.database import DatabaseManager
    from smt.migration import MigrationManager

    mgr = MigrationManager(
        workspace=config.workspace,
        target_url=config.target.get_url_string(),
        target_schema=config.target_schema,
    )

    current = mgr.get_current_revision()
    head = mgr.get_head_revision()

    click.echo(f"Target schema: {config.target_schema}")
    click.echo(f"Current revision: {current or '(none)'}")
    click.echo(f"Head revision: {head or '(none)'}")

    if current == head and head is not None:
        click.echo("Status: Up to date")
    elif head is None:
        click.echo("Status: No migrations")
    else:
        click.echo("Status: Pending migrations")

    target_db = DatabaseManager(config.target)
    try:
        tables = target_db.list_tables(config.target_schema)
        if tables:
            click.echo(f"\nTables ({len(tables)}):")
            for t in tables:
                click.echo(f"  - {t}")
        else:
            click.echo("\nNo tables in target schema")
    finally:
        target_db.dispose()


# ---------------------------------------------------------------------------
# history
# ---------------------------------------------------------------------------
@cli.command()
@click.pass_context
def history(ctx: click.Context) -> None:
    """Show migration history."""
    config = _load(ctx)
    from smt.migration import MigrationManager

    mgr = MigrationManager(
        workspace=config.workspace,
        target_url=config.target.get_url_string(),
        target_schema=config.target_schema,
    )

    entries = mgr.get_history()
    if entries:
        for entry in entries:
            click.echo(entry)
    else:
        click.echo("No migration history")
