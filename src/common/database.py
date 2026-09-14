"""Database connection and session lifecycle management."""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import duckdb
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.common.config import settings
from src.common.logger import get_logger

logger = get_logger("common.database")


class DatabaseManager:
    """Manages connections to DuckDB or PostgreSQL based on application settings."""

    def __init__(self, engine_type: str | None = None) -> None:
        self.engine_type = engine_type or settings.DB_ENGINE
        self._pg_engine: Engine | None = None

    def get_pg_engine(self) -> Engine:
        """Lazily creates and returns a SQLAlchemy Engine for PostgreSQL."""
        if self._pg_engine is None:
            self._pg_engine = create_engine(
                settings.postgres_connection_uri,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
            )
        return self._pg_engine

    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        """Context manager yielding an active database connection."""
        if self.engine_type == "duckdb":
            duck_conn = duckdb.connect(str(settings.resolved_duckdb_path))
            try:
                yield duck_conn
            finally:
                duck_conn.close()
        elif self.engine_type == "postgres":
            engine = self.get_pg_engine()
            with engine.connect() as pg_conn:
                yield pg_conn
        else:
            raise ValueError(f"Unsupported database engine: {self.engine_type}")

    def execute_ddl(self, sql_script: str) -> None:
        """Executes a DDL SQL script across the active database engine."""
        logger.info(f"Executing DDL against target engine: {self.engine_type}")
        with self.get_connection() as conn:
            if self.engine_type == "duckdb":
                conn.execute(sql_script)
            elif self.engine_type == "postgres":
                # Split and execute statements or execute as a single script block
                conn.execute(text(sql_script))
                conn.commit()
        logger.info("DDL executed successfully.")


db_manager = DatabaseManager()
