"""Tests for the SQLite engine (no external dependencies needed)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from db_vault.core.exceptions import BackupError, ConnectionError, RestoreError
from db_vault.core.models import BackupType, DatabaseConfig, DatabaseType, RestoreRequest
from db_vault.engines.sqlite import SQLiteEngine


class TestSQLiteConnection:
    def test_connection_ok(self, sqlite_config: DatabaseConfig) -> None:
        engine = SQLiteEngine(sqlite_config)
        assert engine.test_connection() is True

    def test_connection_missing_file(self, tmp_path: Path) -> None:
        config = DatabaseConfig(type=DatabaseType.SQLITE, database=str(tmp_path / "nope.db"))
        engine = SQLiteEngine(config)
        with pytest.raises(ConnectionError, match="not found"):
            engine.test_connection()

    def test_connection_invalid_file(self, tmp_path: Path) -> None:
        bad_file = tmp_path / "bad.db"
        bad_file.write_text("this is not a database")
        config = DatabaseConfig(type=DatabaseType.SQLITE, database=str(bad_file))
        engine = SQLiteEngine(config)
        with pytest.raises(ConnectionError):
            engine.test_connection()


class TestSQLiteIntrospection:
    def test_list_databases(self, sqlite_config: DatabaseConfig) -> None:
        engine = SQLiteEngine(sqlite_config)
        dbs = engine.list_databases()
        assert len(dbs) == 1
        assert sqlite_config.database in dbs[0]

    def test_list_tables(self, sqlite_config: DatabaseConfig) -> None:
        engine = SQLiteEngine(sqlite_config)
        tables = engine.list_tables()
        assert "users" in tables
        assert "orders" in tables


class TestSQLiteBackup:
    def test_full_backup(self, sqlite_config: DatabaseConfig, tmp_path: Path) -> None:
        engine = SQLiteEngine(sqlite_config)
        output_dir = tmp_path / "backup_output"

        result = engine.backup(output_dir)

        assert result.exists()
        assert result.stat().st_size > 0
        assert result.name.startswith("sqlite_")
        assert result.suffix == ".db"

    def test_selective_backup(self, sqlite_config: DatabaseConfig, tmp_path: Path) -> None:
        engine = SQLiteEngine(sqlite_config)
        output_dir = tmp_path / "backup_output"

        result = engine.backup(output_dir, tables=["users"])

        assert result.exists()
        # Verify only the users table is in the backup
        conn = sqlite3.connect(str(result))
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cur.fetchall()]
        conn.close()
        assert "users" in tables
        assert "orders" not in tables

    def test_incremental_not_supported(self, sqlite_config: DatabaseConfig, tmp_path: Path) -> None:
        engine = SQLiteEngine(sqlite_config)
        with pytest.raises(BackupError, match="only supports full"):
            engine.backup(tmp_path, backup_type=BackupType.INCREMENTAL)

    def test_backup_missing_db(self, tmp_path: Path) -> None:
        config = DatabaseConfig(type=DatabaseType.SQLITE, database=str(tmp_path / "nope.db"))
        engine = SQLiteEngine(config)
        with pytest.raises(BackupError, match="not found"):
            engine.backup(tmp_path / "out")


class TestSQLiteRestore:
    def test_full_restore(self, sqlite_config: DatabaseConfig, tmp_path: Path) -> None:
        engine = SQLiteEngine(sqlite_config)

        # Backup
        backup_file = engine.backup(tmp_path / "backups")

        # Restore to a new location
        restored_db = tmp_path / "restored.db"
        request = RestoreRequest(
            backup_file=backup_file,
            target_database=str(restored_db),
        )
        engine.restore(request)

        # Verify
        assert restored_db.exists()
        conn = sqlite3.connect(str(restored_db))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        assert cur.fetchone()[0] == 3
        cur.execute("SELECT COUNT(*) FROM orders")
        assert cur.fetchone()[0] == 4
        conn.close()

    def test_selective_restore(self, sqlite_config: DatabaseConfig, tmp_path: Path) -> None:
        engine = SQLiteEngine(sqlite_config)

        # Backup
        backup_file = engine.backup(tmp_path / "backups")

        # Restore only the users table
        restored_db = tmp_path / "restored.db"
        request = RestoreRequest(
            backup_file=backup_file,
            target_database=str(restored_db),
            tables=["users"],
        )
        engine.restore(request)

        # Verify
        conn = sqlite3.connect(str(restored_db))
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cur.fetchall()]
        assert "users" in tables
        # orders should NOT be present
        assert "orders" not in tables
        conn.close()

    def test_dry_run(self, sqlite_config: DatabaseConfig, tmp_path: Path) -> None:
        engine = SQLiteEngine(sqlite_config)
        backup_file = engine.backup(tmp_path / "backups")

        restored_db = tmp_path / "dry_run.db"
        request = RestoreRequest(
            backup_file=backup_file,
            target_database=str(restored_db),
            dry_run=True,
        )
        engine.restore(request)
        # Should not create the file
        assert not restored_db.exists()

    def test_restore_missing_backup(self, sqlite_config: DatabaseConfig, tmp_path: Path) -> None:
        engine = SQLiteEngine(sqlite_config)
        request = RestoreRequest(
            backup_file=tmp_path / "nonexistent.db",
            target_database=str(tmp_path / "restored.db"),
        )
        with pytest.raises(RestoreError, match="not found"):
            engine.restore(request)
