"""MongoDB backup/restore engine using mongodump / mongorestore."""

from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path

from db_vault.core.exceptions import BackupError, ConnectionError, RestoreError
from db_vault.core.models import BackupType, DatabaseConfig, RestoreRequest
from db_vault.engines.base import BaseEngine
from db_vault.logging import get_logger

log = get_logger(__name__)


class MongoDBEngine(BaseEngine):
    """Engine for MongoDB databases."""

    def __init__(self, config: DatabaseConfig) -> None:
        super().__init__(config)

    @property
    def _uri(self) -> str:
        """Build a MongoDB connection URI."""
        user = self.config.username or ""
        password = (
            self.config.password.get_secret_value() if self.config.password else ""
        )
        host = f"{self.config.host}:{self.config.port or 27017}"
        auth = f"{user}:{password}@" if user else ""
        scheme = "mongodb+srv" if self.config.ssl else "mongodb"
        return f"{scheme}://{auth}{host}/{self.config.database}"

    # ────────────── Connection ──────────────

    def test_connection(self) -> bool:
        try:
            from pymongo import MongoClient
            from pymongo.errors import ConnectionFailure

            client = MongoClient(
                self._uri,
                serverSelectionTimeoutMS=10_000,
                tls=self.config.ssl,
            )
            # Force a connection attempt
            client.admin.command("ping")
            client.close()
            log.info(
                "mongodb_connection_ok",
                host=self.config.host,
                database=self.config.database,
            )
            return True
        except (ConnectionFailure, Exception) as exc:
            raise ConnectionError(f"MongoDB connection failed: {exc}") from exc

    # ────────────── Introspection ───────────

    def list_databases(self) -> list[str]:
        from pymongo import MongoClient

        try:
            client = MongoClient(self._uri, serverSelectionTimeoutMS=10_000)
            db_names = client.list_database_names()
            client.close()
            system_dbs = {"admin", "config", "local"}
            return [d for d in db_names if d not in system_dbs]
        except Exception as exc:
            raise ConnectionError(f"Failed to list databases: {exc}") from exc

    def list_tables(self, database: str | None = None) -> list[str]:
        from pymongo import MongoClient

        db_name = database or self.config.database
        if not db_name:
            raise ConnectionError("No database specified.")
        try:
            client = MongoClient(self._uri, serverSelectionTimeoutMS=10_000)
            db = client[db_name]
            collections = db.list_collection_names()
            client.close()
            return sorted(collections)
        except Exception as exc:
            raise ConnectionError(f"Failed to list collections: {exc}") from exc

    # ────────────── Backup ──────────────────

    def backup(
            self,
            output_dir: Path,
            backup_type: BackupType = BackupType.FULL,
            tables: list[str] | None = None,
    ) -> Path:
        if backup_type != BackupType.FULL:
            raise BackupError(
                f"MongoDB engine only supports full backups via mongodump. "
                f"Got: {backup_type.value}. "
                f"Incremental backups require oplog on a replica set."
            )

        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        db_name = self.config.database or "all"
        dump_dir = output_dir / f"mongodb_{db_name}_{timestamp}"

        cmd = ["mongodump", f"--uri={self._uri}", f"--out={dump_dir}"]

        # Specific collections
        if tables and self.config.database:
            # mongodump can only do one collection at a time with --collection
            # For multiple, we do separate dumps or use --nsInclude
            for collection in tables:
                ns = f"{self.config.database}.{collection}"
                cmd.extend([f"--nsInclude={ns}"])

        if self.config.ssl:
            cmd.append("--ssl")

        log.info("mongodb_backup_start", database=db_name, dir=str(dump_dir))

        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=3600
            )
            if result.returncode != 0:
                raise BackupError(
                    f"mongodump failed (exit {result.returncode}): {result.stderr}"
                )
        except FileNotFoundError:
            raise BackupError(
                "mongodump not found. Install mongodb-database-tools: "
                "https://www.mongodb.com/try/download/database-tools"
            )
        except subprocess.TimeoutExpired:
            raise BackupError("mongodump timed out after 1 hour")

        # Create a tar archive of the dump directory for easier handling
        archive_path = output_dir / f"mongodb_{db_name}_{timestamp}.archive"
        self._tar_directory(dump_dir, archive_path)

        # Clean up raw dump directory
        import shutil

        shutil.rmtree(dump_dir, ignore_errors=True)

        log.info(
            "mongodb_backup_complete",
            file=str(archive_path),
            size=archive_path.stat().st_size,
        )
        return archive_path

    @classmethod
    def supported_backup_types(cls) -> list[BackupType]:
        return [BackupType.FULL]

    # ────────────── Restore ─────────────────

    def restore(self, request: RestoreRequest) -> None:
        target_db = request.target_database or self.config.database

        # First, extract the archive if it's a tar
        restore_dir = self._maybe_untar(request.backup_file)

        cmd = ["mongorestore", f"--uri={self._uri}"]

        if target_db:
            cmd.extend([f"--db={target_db}"])

        if request.drop_existing:
            cmd.append("--drop")

        if request.tables:
            for collection in request.tables:
                ns = f"{target_db}.{collection}" if target_db else collection
                cmd.extend([f"--nsInclude={ns}"])

        cmd.append(str(restore_dir))

        if request.dry_run:
            cmd.append("--dryRun")
            log.info("mongodb_restore_dry_run", command=" ".join(cmd))

        log.info(
            "mongodb_restore_start",
            database=target_db,
            file=str(request.backup_file),
        )

        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=7200
            )
            if result.returncode != 0:
                raise RestoreError(
                    f"mongorestore failed (exit {result.returncode}): {result.stderr}"
                )
        except FileNotFoundError:
            raise RestoreError("mongorestore not found. Install mongodb-database-tools.")
        except subprocess.TimeoutExpired:
            raise RestoreError("mongorestore timed out after 2 hours")

        log.info("mongodb_restore_complete", database=target_db)

    # ────────────── Helpers ─────────────────

    @staticmethod
    def _tar_directory(source_dir: Path, archive_path: Path) -> None:
        """Create a tar archive from a directory."""
        import tarfile

        with tarfile.open(archive_path, "w") as tar:
            tar.add(source_dir, arcname=source_dir.name)

    @staticmethod
    def _maybe_untar(file_path: Path) -> Path:
        """If the file is a tar archive, extract it and return the dir."""
        import tarfile

        if tarfile.is_tarfile(file_path):
            extract_dir = file_path.parent / file_path.stem
            with tarfile.open(file_path, "r") as tar:
                tar.extractall(path=extract_dir)
            return extract_dir
        return file_path
