"""Database abstraction layer for schema operations."""

from __future__ import annotations

import logging
import re

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from smt.config import DatabaseConfig

logger = logging.getLogger(__name__)

_VALID_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")


class DatabaseError(Exception):
    """Raised when a database operation fails."""


class DatabaseManager:
    """Manages database connections and schema-level DDL operations."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._engine: Engine | None = None

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(self.config.get_url())
        return self._engine

    def dispose(self):
        """Close all connections."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    def verify_connection(self) -> bool:
        """Test database connectivity."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error("Connection failed: %s", e)
            return False

    def create_schema(self, schema_name: str) -> None:
        """Create a schema if it does not exist (dialect-aware)."""
        _validate_identifier(schema_name)
        dialect = self.config.dialect
        logger.info("Creating schema '%s' (%s)...", schema_name, dialect)

        ddl = _CREATE_SCHEMA_DDL.get(dialect)
        if ddl is None:
            raise DatabaseError(f"Schema creation not supported for dialect: {dialect}")

        sql = ddl.format(schema=schema_name)
        with self.engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()

        logger.info("Schema '%s' ready", schema_name)

    def drop_schema(self, schema_name: str) -> None:
        """Drop a schema if it exists (dialect-aware)."""
        _validate_identifier(schema_name)
        dialect = self.config.dialect
        logger.info("Dropping schema '%s' (%s)...", schema_name, dialect)

        ddl = _DROP_SCHEMA_DDL.get(dialect)
        if ddl is None:
            raise DatabaseError(f"Schema drop not supported for dialect: {dialect}")

        sql = ddl.format(schema=schema_name)
        with self.engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()

        logger.info("Schema '%s' dropped", schema_name)

    def list_tables(self, schema_name: str) -> list[str]:
        """List all tables in a schema."""
        inspector = inspect(self.engine)
        return sorted(inspector.get_table_names(schema=schema_name))


def _validate_identifier(name: str) -> None:
    """Validate that a schema name contains only safe identifier characters."""
    if not _VALID_IDENTIFIER.match(name):
        raise DatabaseError(
            f"Invalid schema name '{name}': "
            f"only alphanumeric characters and underscores are allowed"
        )


_CREATE_SCHEMA_DDL: dict[str, str] = {
    "postgresql": "CREATE SCHEMA IF NOT EXISTS {schema}",
    "mssql": (
        "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}') "
        "EXEC('CREATE SCHEMA [{schema}]')"
    ),
}

_DROP_SCHEMA_DDL: dict[str, str] = {
    "postgresql": "DROP SCHEMA IF EXISTS {schema} CASCADE",
    "mssql": (
        "IF EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}') "
        "EXEC('DROP SCHEMA [{schema}]')"
    ),
}
