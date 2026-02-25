"""Local filesystem storage backend."""

from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

from db_vault.core.exceptions import StorageError
from db_vault.logging import get_logger
from db_vault.storage.base import BaseStorage

log = get_logger(__name__)


class LocalStorage(BaseStorage):
    """Store backup files on the local filesystem."""

    def __init__(self, base_path: Path) -> None:
        self.base_path = base_path.expanduser().resolve()
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _full_path(self, remote_key: str) -> Path:
        return self.base_path / remote_key

    def upload(self, local_path: Path, remote_key: str) -> str:
        """Copy a backup file to the local storage directory."""
        dest = self._full_path(remote_key)
        dest.parent.mkdir(parents=True, exist_ok=True)

        try:
            if local_path.resolve() == dest.resolve():
                log.debug("local_upload_skip_same_path", path=str(dest))
                return str(dest)

            shutil.copy2(local_path, dest)
            log.info(
                "local_upload_complete",
                source=str(local_path),
                destination=str(dest),
                size=dest.stat().st_size,
            )
            return str(dest)
        except OSError as exc:
            raise StorageError(f"Failed to copy backup to {dest}: {exc}") from exc

    def download(self, remote_key: str, local_path: Path) -> Path:
        """Copy a backup file from local storage to the designated path."""
        source = self._full_path(remote_key)
        if not source.exists():
            raise StorageError(f"Backup not found: {source}")

        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            shutil.copy2(source, local_path)
            log.info("local_download_complete", source=str(source), destination=str(local_path))
            return local_path
        except OSError as exc:
            raise StorageError(f"Failed to download backup: {exc}") from exc

    def list_backups(self, prefix: str = "") -> list[dict[str, str]]:
        """List backup files under the base path, optionally filtered by prefix."""
        search_dir = self._full_path(prefix) if prefix else self.base_path
        if not search_dir.exists():
            return []

        results: list[dict[str, str]] = []
        for file_path in sorted(search_dir.rglob("*")):
            if file_path.is_file():
                stat = file_path.stat()
                relative = file_path.relative_to(self.base_path)
                results.append({
                    "key": str(relative),
                    "size": str(stat.st_size),
                    "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                })
        return results

    def delete(self, remote_key: str) -> None:
        """Delete a backup file from local storage."""
        target = self._full_path(remote_key)
        if not target.exists():
            raise StorageError(f"File not found: {target}")
        try:
            target.unlink()
            log.info("local_delete_complete", path=str(target))
        except OSError as exc:
            raise StorageError(f"Failed to delete {target}: {exc}") from exc

    def exists(self, remote_key: str) -> bool:
        return self._full_path(remote_key).exists()
