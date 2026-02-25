"""Tests for local storage backend."""

from __future__ import annotations

from pathlib import Path

import pytest

from db_vault.core.exceptions import StorageError
from db_vault.storage.local import LocalStorage


class TestLocalStorage:
    def test_upload(self, sample_file: Path, tmp_path: Path) -> None:
        storage = LocalStorage(tmp_path / "store")
        location = storage.upload(sample_file, "test/backup.dat")
        assert Path(location).exists()

    def test_upload_same_path(self, tmp_path: Path) -> None:
        """Uploading to the same path should be a no-op."""
        store_dir = tmp_path / "store"
        store_dir.mkdir()
        f = store_dir / "file.txt"
        f.write_text("hello")
        storage = LocalStorage(store_dir)
        location = storage.upload(f, "file.txt")
        assert location == str(f)

    def test_download(self, sample_file: Path, tmp_path: Path) -> None:
        storage = LocalStorage(tmp_path / "store")
        storage.upload(sample_file, "test/backup.dat")

        dest = tmp_path / "downloaded.dat"
        storage.download("test/backup.dat", dest)
        assert dest.exists()
        assert dest.read_bytes() == sample_file.read_bytes()

    def test_download_missing(self, tmp_path: Path) -> None:
        storage = LocalStorage(tmp_path / "store")
        with pytest.raises(StorageError, match="not found"):
            storage.download("nonexistent.dat", tmp_path / "out.dat")

    def test_list_backups(self, sample_file: Path, tmp_path: Path) -> None:
        storage = LocalStorage(tmp_path / "store")
        storage.upload(sample_file, "db/backup1.dat")
        storage.upload(sample_file, "db/backup2.dat")
        storage.upload(sample_file, "other/backup3.dat")

        all_backups = storage.list_backups()
        assert len(all_backups) == 3

        db_backups = storage.list_backups("db")
        assert len(db_backups) == 2

    def test_list_empty(self, tmp_path: Path) -> None:
        storage = LocalStorage(tmp_path / "store")
        assert storage.list_backups() == []

    def test_delete(self, sample_file: Path, tmp_path: Path) -> None:
        storage = LocalStorage(tmp_path / "store")
        storage.upload(sample_file, "test/backup.dat")
        assert storage.exists("test/backup.dat")

        storage.delete("test/backup.dat")
        assert not storage.exists("test/backup.dat")

    def test_delete_missing(self, tmp_path: Path) -> None:
        storage = LocalStorage(tmp_path / "store")
        with pytest.raises(StorageError, match="not found"):
            storage.delete("nonexistent.dat")

    def test_exists(self, sample_file: Path, tmp_path: Path) -> None:
        storage = LocalStorage(tmp_path / "store")
        assert not storage.exists("test.dat")
        storage.upload(sample_file, "test.dat")
        assert storage.exists("test.dat")
