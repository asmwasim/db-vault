"""PostgreSQL backup/restore engine using pg_dump / pg_restore."""

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


class PostgresEngine(BaseEngine):
    """Engine for PostgreSQL databases."""

    def __init__(self, config: DatabaseConfig) -> None:
        super().__init__(config)

    # ────────────── Connection ──────────────

    def test_connection(self) -> bool:
        try:
            import psycopg2

            conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.username,
                password=(
                    self.config.password.get_secret_value() if self.config.password else None
                ),
                dbname=self.config.database or "postgres",
                connect_timeout=10,
                sslmode="require" if self.config.ssl else "prefer",
            )
            conn.close()
            log.info("postgres_connection_ok", host=self.config.host, database=self.config.database)
            return True
        except Exception as exc:
            raise ConnectionError(f"PostgreSQL connection failed: {exc}") from exc

    # ────────────── Introspection ───────────

    def list_databases(self) -> list[str]:
        import psycopg2

        try:
            conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.username,
                password=(
                    self.config.password.get_secret_value() if self.config.password else None
                ),
                dbname="postgres",
                connect_timeout=10,
            )
            cur = conn.cursor()
            cur.execute(
                "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"
            )
            databases = [row[0] for row in cur.fetchall()]
            cur.close()
            conn.close()
            return databases
        except Exception as exc:
            raise ConnectionError(f"Failed to list databases: {exc}") from exc

    def list_tables(self, database: str | None = None) -> list[str]:
        import psycopg2

        db = database or self.config.database
        try:
            conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.username,
                password=(
                    self.config.password.get_secret_value() if self.config.password else None
                ),
                dbname=db,
                connect_timeout=10,
            )
            cur = conn.cursor()
            cur.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
            )
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
                f"PostgreSQL engine only supports full backups via pg_dump. "
                f"Got: {backup_type.value}. "
                f"Incremental/differential require WAL archiving (server-side config)."
            )

        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        db_name = self.config.database or "all"
        filename = f"postgres_{db_name}_{timestamp}.dump"
        output_file = output_dir / filename

        cmd = [
            "pg_dump",
            f"--host={self.config.host}",
            f"--port={self.config.port}",
            f"--username={self.config.username}",
            "--format=custom",
            "--no-password",
            f"--file={output_file}",
        ]

        # Add specific tables if requested
        if tables:
            for table in tables:
                cmd.extend(["--table", table])

        cmd.append(self.config.database)

        env = os.environ.copy()
        if self.config.password:
            env["PGPASSWORD"] = self.config.password.get_secret_value()
        if self.config.ssl:
            env["PGSSLMODE"] = "require"

        log.info("postgres_backup_start", database=self.config.database, file=str(output_file))

        try:
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout
            )
            if result.returncode != 0:
                raise BackupError(f"pg_dump failed (exit {result.returncode}): {result.stderr}")
        except FileNotFoundError:
            raise BackupError(
                "pg_dump not found. Install postgresql-client: "
                "apt install postgresql-client / brew install libpq"
            )
        except subprocess.TimeoutExpired:
            raise BackupError("pg_dump timed out after 1 hour")

        log.info(
            "postgres_backup_complete",
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
            "pg_restore",
            f"--host={self.config.host}",
            f"--port={self.config.port}",
            f"--username={self.config.username}",
            f"--dbname={target_db}",
            "--no-password",
            "--verbose",
        ]

        if request.drop_existing:
            cmd.append("--clean")
            cmd.append("--if-exists")

        if request.tables:
            for table in request.tables:
                cmd.extend(["--table", table])

        cmd.append(str(request.backup_file))

        env = os.environ.copy()
        if self.config.password:
            env["PGPASSWORD"] = self.config.password.get_secret_value()

        if request.dry_run:
            log.info("postgres_restore_dry_run", command=" ".join(cmd))
            return

        log.info("postgres_restore_start", database=target_db, file=str(request.backup_file))

        try:
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=7200,
            )
            # pg_restore returns non-zero for warnings too, so we check stderr
            if result.returncode != 0 and "error" in result.stderr.lower():
                raise RestoreError(
                    f"pg_restore failed (exit {result.returncode}): {result.stderr}"
                )
        except FileNotFoundError:
            raise RestoreError(
                "pg_restore not found. Install postgresql-client."
            )
        except subprocess.TimeoutExpired:
            raise RestoreError("pg_restore timed out after 2 hours")

        log.info("postgres_restore_complete", database=target_db)
