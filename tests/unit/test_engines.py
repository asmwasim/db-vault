"""Tests for the engine registry."""

from __future__ import annotations

from db_vault.core.models import DatabaseConfig, DatabaseType
from db_vault.engines import get_engine
from db_vault.engines.mongodb import MongoDBEngine
from db_vault.engines.mysql import MySQLEngine
from db_vault.engines.postgres import PostgresEngine
from db_vault.engines.sqlite import SQLiteEngine


class TestEngineRegistry:
    def test_postgres(self) -> None:
        config = DatabaseConfig(type=DatabaseType.POSTGRES)
        engine = get_engine(config)
        assert isinstance(engine, PostgresEngine)

    def test_mysql(self) -> None:
        config = DatabaseConfig(type=DatabaseType.MYSQL)
        engine = get_engine(config)
        assert isinstance(engine, MySQLEngine)

    def test_mongodb(self) -> None:
        config = DatabaseConfig(type=DatabaseType.MONGODB)
        engine = get_engine(config)
        assert isinstance(engine, MongoDBEngine)

    def test_sqlite(self) -> None:
        config = DatabaseConfig(type=DatabaseType.SQLITE, database="test.db")
        engine = get_engine(config)
        assert isinstance(engine, SQLiteEngine)

    def test_supported_types(self) -> None:
        assert SQLiteEngine.supported_backup_types() == [
            __import__("db_vault.core.models", fromlist=["BackupType"]).BackupType.FULL
        ]
