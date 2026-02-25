"""Abstract base class for storage backends."""

from __future__ import annotations

import abc
from pathlib import Path


class BaseStorage(abc.ABC):
    """Interface for backup file storage backends."""

    @abc.abstractmethod
    def upload(self, local_path: Path, remote_key: str) -> str:
        """Upload a backup file to the storage backend.

        Args:
            local_path: Path to the local file to upload.
            remote_key: Destination key/path in the backend.

        Returns:
            The full location URI or path where the file was stored.
        """

    @abc.abstractmethod
    def download(self, remote_key: str, local_path: Path) -> Path:
        """Download a backup file from the storage backend.

        Args:
            remote_key: Key/path of the file in the backend.
            local_path: Local destination path.

        Returns:
            Path to the downloaded file.
        """

    @abc.abstractmethod
    def list_backups(self, prefix: str = "") -> list[dict[str, str]]:
        """List available backups in the storage backend.

        Args:
            prefix: Optional prefix/directory to filter by.

        Returns:
            List of dicts with at least 'key', 'size', 'last_modified'.
        """

    @abc.abstractmethod
    def delete(self, remote_key: str) -> None:
        """Delete a backup file from the storage backend.

        Args:
            remote_key: Key/path of the file to delete.
        """

    @abc.abstractmethod
    def exists(self, remote_key: str) -> bool:
        """Check if a file exists in the backend."""
