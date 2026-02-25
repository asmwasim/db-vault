"""Database engine registry."""

from __future__ import annotations

from db_vault.core.exceptions import EngineNotFoundError
from db_vault.core.models import DatabaseConfig, DatabaseType
from db_vault.engines.base import BaseEngine


def get_engine(config: DatabaseConfig) -> BaseEngine:
    """Instantiate the appropriate engine for the given database config.

    Raises:
        EngineNotFoundError: If the database type is not supported.
    """
    if config.type == DatabaseType.POSTGRES:
        from db_vault.engines.postgres import PostgresEngine

        return PostgresEngine(config)

    if config.type == DatabaseType.MYSQL:
        from db_vault.engines.mysql import MySQLEngine

        return MySQLEngine(config)

    if config.type == DatabaseType.MONGODB:
        from db_vault.engines.mongodb import MongoDBEngine

        return MongoDBEngine(config)

    if config.type == DatabaseType.SQLITE:
        from db_vault.engines.sqlite import SQLiteEngine

        return SQLiteEngine(config)

    raise EngineNotFoundError(f"Unsupported database type: {config.type}")


__all__ = ["BaseEngine", "get_engine"]
