"""MySQL backup/restore engine using mysqldump / mysql client."""

from __future__ import annotations

import os
import subprocess
from datetime import datetime
from pathlib import Path

from db_vault.core.exceptions import BackupError, ConnectionError, RestoreError
from db_vault.core.models import BackupType, DatabaseConfig, RestoreRequest
from db_vault.engines.base import BaseEngine
from db_vault.logging import get_logger

log = get_logger(__name__)


class MySQLEngine(BaseEngine):
    """Engine for MySQL / MariaDB databases."""

    def __init__(self, config: DatabaseConfig) -> None:
        super().__init__(config)

    # ────────────── Connection ──────────────

    def test_connection(self) -> bool:
        try:
            import pymysql

            conn = pymysql.connect(
                host=self.config.host,
                port=self.config.port or 3306,
                user=self.config.username,
                password=(
                    self.config.password.get_secret_value() if self.config.password else ""
                ),
                database=self.config.database or None,
                connect_timeout=10,
                ssl={"ssl": True} if self.config.ssl else None,
            )
            conn.close()
            log.info("mysql_connection_ok", host=self.config.host, database=self.config.database)
            return True
        except Exception as exc:
            raise ConnectionError(f"MySQL connection failed: {exc}") from exc

    # ────────────── Introspection ───────────

    def list_databases(self) -> list[str]:
        import pymysql

        try:
            conn = pymysql.connect(
                host=self.config.host,
                port=self.config.port or 3306,
                user=self.config.username,
                password=(
                    self.config.password.get_secret_value() if self.config.password else ""
                ),
                connect_timeout=10,
            )
            cur = conn.cursor()
            cur.execute("SHOW DATABASES")
            databases = [row[0] for row in cur.fetchall()]
            cur.close()
            conn.close()
            # Filter system databases
            system_dbs = {"information_schema", "performance_schema", "mysql", "sys"}
            return [db for db in databases if db not in system_dbs]
        except Exception as exc:
            raise ConnectionError(f"Failed to list databases: {exc}") from exc

    def list_tables(self, database: str | None = None) -> list[str]:
        import pymysql

        db = database or self.config.database
        if not db:
            raise ConnectionError("No database specified.")
        try:
            conn = pymysql.connect(
                host=self.config.host,
                port=self.config.port or 3306,
                user=self.config.username,
                password=(
                    self.config.password.get_secret_value() if self.config.password else ""
                ),
                database=db,
                connect_timeout=10,
            )
            cur = conn.cursor()
            cur.execute("SHOW TABLES")
            tables = [row[0] for row in cur.fetchall()]
            cur.close()
            conn.close()
            return tables
        except Exception as exc:
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
                f"MySQL engine only supports full backups via mysqldump. "
                f"Got: {backup_type.value}. "
                f"Incremental backups require binary log (server-side config) "
                f"or Percona XtraBackup."
            )

        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        db_name = self.config.database or "all"
        filename = f"mysql_{db_name}_{timestamp}.sql"
        output_file = output_dir / filename

        cmd = [
            "mysqldump",
            f"--host={self.config.host}",
            f"--port={self.config.port or 3306}",
            f"--user={self.config.username}",
            "--single-transaction",
            "--routines",
            "--triggers",
            "--events",
            f"--result-file={output_file}",
        ]

        if self.config.ssl:
            cmd.append("--ssl")

        if self.config.database:
            cmd.append(self.config.database)
            # Add specific tables if requested
            if tables:
                cmd.extend(tables)
        else:
            cmd.append("--all-databases")

        env = os.environ.copy()
        if self.config.password:
            env["MYSQL_PWD"] = self.config.password.get_secret_value()

        log.info("mysql_backup_start", database=db_name, file=str(output_file))

        try:
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=3600,
            )
            if result.returncode != 0:
                raise BackupError(
                    f"mysqldump failed (exit {result.returncode}): {result.stderr}"
                )
        except FileNotFoundError:
            raise BackupError(
                "mysqldump not found. Install mysql-client: "
                "apt install default-mysql-client / brew install mysql-client"
            )
        except subprocess.TimeoutExpired:
            raise BackupError("mysqldump timed out after 1 hour")

        log.info(
            "mysql_backup_complete",
            file=str(output_file),
            size=output_file.stat().st_size,
        )
        return output_file

    @classmethod
    def supported_backup_types(cls) -> list[BackupType]:
        return [BackupType.FULL]

    # ────────────── Restore ─────────────────

    def restore(self, request: RestoreRequest) -> None:
        target_db = request.target_database or self.config.database
        if not target_db:
            raise RestoreError("No target database specified for restore.")

        cmd = [
            "mysql",
            f"--host={self.config.host}",
            f"--port={self.config.port or 3306}",
            f"--user={self.config.username}",
            target_db,
        ]

        env = os.environ.copy()
        if self.config.password:
            env["MYSQL_PWD"] = self.config.password.get_secret_value()

        if request.dry_run:
            log.info("mysql_restore_dry_run", command=" ".join(cmd))
            return

        log.info("mysql_restore_start", database=target_db, file=str(request.backup_file))

        try:
            with open(request.backup_file) as f:
                sql_content = f.read()

            # If selective restore, filter SQL for specific tables
            if request.tables:
                sql_content = self._filter_tables(sql_content, request.tables)

            result = subprocess.run(
                cmd,
                input=sql_content,
                env=env,
                capture_output=True,
                text=True,
                timeout=7200,
            )
            if result.returncode != 0:
                raise RestoreError(
                    f"mysql restore failed (exit {result.returncode}): {result.stderr}"
                )
        except FileNotFoundError:
            raise RestoreError("mysql client not found. Install mysql-client.")
        except subprocess.TimeoutExpired:
            raise RestoreError("mysql restore timed out after 2 hours")

        log.info("mysql_restore_complete", database=target_db)

    @staticmethod
    def _filter_tables(sql: str, tables: list[str]) -> str:
        """Very basic filter to extract table-specific statements from a mysqldump.

        This is a best-effort approach — for reliable selective restore,
        dump individual tables during backup.
        """
        lines = sql.split("\n")
        result_lines: list[str] = []
        in_target_table = False
        table_set = set(tables)

        for line in lines:
            # Detect table definition blocks
            if line.startswith("-- Table structure for table") or line.startswith(
                    "-- Dumping data for table"
            ):
                table_name = line.split("`")[1] if "`" in line else ""
                in_target_table = table_name in table_set

            if in_target_table or line.startswith("--") or not line.strip():
                result_lines.append(line)

        return "\n".join(result_lines)
