"""Abstract base class for database backup/restore engines."""

from __future__ import annotations

import abc
from pathlib import Path

from db_vault.core.models import BackupType, DatabaseConfig, RestoreRequest


class BaseEngine(abc.ABC):
    """Interface that every database engine must implement."""

    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config

    # ────────────── Connection ──────────────

    @abc.abstractmethod
    def test_connection(self) -> bool:
        """Validate database connectivity and credentials.

        Returns:
            True if the connection is successful.

        Raises:
            db_vault.core.exceptions.ConnectionError on failure.
        """

    # ────────────── Introspection ───────────

    @abc.abstractmethod
    def list_databases(self) -> list[str]:
        """Return a list of database names accessible with the current credentials."""

    @abc.abstractmethod
    def list_tables(self, database: str | None = None) -> list[str]:
        """Return a list of tables/collections in the given (or configured) database."""

    # ────────────── Backup ──────────────────

    @abc.abstractmethod
    def backup(
            self,
            output_dir: Path,
            backup_type: BackupType = BackupType.FULL,
            tables: list[str] | None = None,
    ) -> Path:
        """Execute a backup and write the dump file to *output_dir*.

        Args:
            output_dir: Directory to write the backup file into.
            backup_type: Full, incremental, or differential.
            tables: Optional list of specific tables/collections to back up.

        Returns:
            Path to the raw (uncompressed) backup file.

        Raises:
            db_vault.core.exceptions.BackupError on failure.
        """

    @classmethod
    @abc.abstractmethod
    def supported_backup_types(cls) -> list[BackupType]:
        """Return backup types supported by this engine."""

    # ────────────── Restore ─────────────────

    @abc.abstractmethod
    def restore(self, request: RestoreRequest) -> None:
        """Restore a database from a backup file.

        Args:
            request: A RestoreRequest describing what to restore and where.

        Raises:
            db_vault.core.exceptions.RestoreError on failure.
        """

    # ────────────── Helpers ─────────────────

    @property
    def engine_name(self) -> str:
        """Human-readable engine name."""
        return self.config.type.value

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} db={self.config.connection_string}>"
