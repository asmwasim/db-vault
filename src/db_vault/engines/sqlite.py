"""SQLite backup/restore engine using Python's built-in sqlite3 module."""

from __future__ import annotations

import contextlib
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

from db_vault.core.exceptions import BackupError, ConnectionError, RestoreError
from db_vault.core.models import BackupType, DatabaseConfig, RestoreRequest
from db_vault.engines.base import BaseEngine
from db_vault.logging import get_logger

log = get_logger(__name__)


class SQLiteEngine(BaseEngine):
    """Engine for SQLite databases."""

    def __init__(self, config: DatabaseConfig) -> None:
        super().__init__(config)

    @property
    def _db_path(self) -> Path:
        return Path(self.config.database)

    # ────────────── Connection ──────────────

    def test_connection(self) -> bool:
        db_path = self._db_path
        if not db_path.exists():
            raise ConnectionError(f"SQLite database file not found: {db_path}")
        try:
            conn = sqlite3.connect(str(db_path), timeout=10)
            # Verify it's a valid SQLite database by reading its schema
            conn.execute("PRAGMA integrity_check")
            conn.execute("SELECT count(*) FROM sqlite_master")
            conn.close()
            log.info("sqlite_connection_ok", database=str(db_path))
            return True
        except sqlite3.Error as exc:
            raise ConnectionError(f"SQLite connection failed: {exc}") from exc

    # ────────────── Introspection ───────────

    def list_databases(self) -> list[str]:
        """SQLite is a single-file database; return the configured file path."""
        return [str(self._db_path)]

    def list_tables(self, database: str | None = None) -> list[str]:
        db_path = Path(database) if database else self._db_path
        try:
            conn = sqlite3.connect(str(db_path), timeout=10)
            cur = conn.cursor()
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
            tables = [row[0] for row in cur.fetchall()]
            cur.close()
            conn.close()
            return tables
        except sqlite3.Error as exc:
            raise ConnectionError(f"Failed to list tables: {exc}") from exc

    # ────────────── Backup ──────────────────

    def backup(
            self,
            output_dir: Path,
            backup_type: BackupType = BackupType.FULL,
            tables: list[str] | None = None,
    ) -> Path:
        if backup_type != BackupType.FULL:
            raise BackupError(
                f"SQLite engine only supports full backups. "
                f"Got: {backup_type.value}."
            )

        db_path = self._db_path
        if not db_path.exists():
            raise BackupError(f"SQLite database file not found: {db_path}")

        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        db_name = db_path.stem
        filename = f"sqlite_{db_name}_{timestamp}.db"
        output_file = output_dir / filename

        log.info("sqlite_backup_start", database=str(db_path), file=str(output_file))

        try:
            if tables:
                # Selective backup: create a new DB with only the requested tables
                self._backup_tables(db_path, output_file, tables)
            else:
                # Full backup using the sqlite3 backup API (safe, handles locking)
                source = sqlite3.connect(str(db_path))
                dest = sqlite3.connect(str(output_file))
                source.backup(dest)
                dest.close()
                source.close()
        except sqlite3.Error as exc:
            output_file.unlink(missing_ok=True)
            raise BackupError(f"SQLite backup failed: {exc}") from exc

        log.info(
            "sqlite_backup_complete",
            file=str(output_file),
            size=output_file.stat().st_size,
        )
        return output_file

    @classmethod
    def supported_backup_types(cls) -> list[BackupType]:
        return [BackupType.FULL]

    # ────────────── Restore ─────────────────

    def restore(self, request: RestoreRequest) -> None:
        target = Path(request.target_database) if request.target_database else self._db_path
        backup_path = request.backup_file

        if not backup_path.exists():
            raise RestoreError(f"Backup file not found: {backup_path}")

        if request.dry_run:
            log.info(
                "sqlite_restore_dry_run",
                source=str(backup_path),
                target=str(target),
                tables=request.tables,
            )
            return

        log.info("sqlite_restore_start", source=str(backup_path), target=str(target))

        try:
            if request.tables:
                self._restore_tables(backup_path, target, request.tables, request.drop_existing)
            else:
                # Full restore: copy the backup file over the target
                if target.exists() and request.drop_existing:
                    target.unlink()
                if target.exists():
                    # Use the backup API in reverse
                    source = sqlite3.connect(str(backup_path))
                    dest = sqlite3.connect(str(target))
                    source.backup(dest)
                    dest.close()
                    source.close()
                else:
                    shutil.copy2(backup_path, target)
        except sqlite3.Error as exc:
            raise RestoreError(f"SQLite restore failed: {exc}") from exc

        log.info("sqlite_restore_complete", target=str(target))

    # ────────────── Helpers ─────────────────

    @staticmethod
    def _backup_tables(source_path: Path, dest_path: Path, tables: list[str]) -> None:
        """Selectively backup specific tables into a new SQLite database."""
        source = sqlite3.connect(str(source_path))
        dest = sqlite3.connect(str(dest_path))

        for table in tables:
            # Get the CREATE TABLE statement
            cur = source.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            row = cur.fetchone()
            if row is None:
                log.warning("sqlite_table_not_found", table=table)
                continue

            create_sql = row[0]
            dest.execute(create_sql)

            # Copy data
            rows = source.execute(f"SELECT * FROM [{table}]").fetchall()
            if rows:
                placeholders = ", ".join("?" * len(rows[0]))
                dest.executemany(
                    f"INSERT INTO [{table}] VALUES ({placeholders})", rows
                )

        dest.commit()
        dest.close()
        source.close()

    @staticmethod
    def _restore_tables(
            backup_path: Path,
            target_path: Path,
            tables: list[str],
            drop_existing: bool,
    ) -> None:
        """Selectively restore specific tables from a backup."""
        backup_conn = sqlite3.connect(str(backup_path))
        target_conn = sqlite3.connect(str(target_path))

        for table in tables:
            cur = backup_conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            row = cur.fetchone()
            if row is None:
                log.warning("sqlite_table_not_found_in_backup", table=table)
                continue

            if drop_existing:
                target_conn.execute(f"DROP TABLE IF EXISTS [{table}]")

            create_sql = row[0]
            with contextlib.suppress(sqlite3.OperationalError):
                target_conn.execute(create_sql)

            rows = backup_conn.execute(f"SELECT * FROM [{table}]").fetchall()
            if rows:
                placeholders = ", ".join("?" * len(rows[0]))
                target_conn.executemany(
                    f"INSERT INTO [{table}] VALUES ({placeholders})", rows
                )

        target_conn.commit()
        target_conn.close()
        backup_conn.close()
