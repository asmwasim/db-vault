"""Shared pytest fixtures."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from db_vault.core.models import (
    DatabaseConfig,
    DatabaseType,
    StorageConfig,
    StorageType,
)


@pytest.fixture()
def tmp_dir(tmp_path: Path) -> Path:
    """Return a temporary directory for test outputs."""
    return tmp_path


@pytest.fixture()
def sample_file(tmp_path: Path) -> Path:
    """Create a sample file with repeating content (good for compression)."""
    f = tmp_path / "sample.dat"
    content = b"The quick brown fox jumps over the lazy dog.\n" * 10_000
    f.write_bytes(content)
    return f


@pytest.fixture()
def sqlite_db(tmp_path: Path) -> Path:
    """Create a temporary SQLite database with sample data."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute("""
                CREATE TABLE users
                (
                    id    INTEGER PRIMARY KEY,
                    name  TEXT NOT NULL,
                    email TEXT NOT NULL
                )
                """)
    cur.execute("""
                CREATE TABLE orders
                (
                    id      INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    product TEXT,
                    amount  REAL,
                    FOREIGN KEY (user_id) REFERENCES users (id)
                )
                """)

    users = [
        (1, "Alice", "alice@example.com"),
        (2, "Bob", "bob@example.com"),
        (3, "Charlie", "charlie@example.com"),
    ]
    cur.executemany("INSERT INTO users VALUES (?, ?, ?)", users)

    orders = [
        (1, 1, "Widget", 29.99),
        (2, 1, "Gadget", 49.99),
        (3, 2, "Widget", 29.99),
        (4, 3, "Thingamajig", 99.99),
    ]
    cur.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", orders)

    conn.commit()
    conn.close()
    return db_path


@pytest.fixture()
def sqlite_config(sqlite_db: Path) -> DatabaseConfig:
    """Return a DatabaseConfig pointing to the test SQLite database."""
    return DatabaseConfig(type=DatabaseType.SQLITE, database=str(sqlite_db))


@pytest.fixture()
def local_storage_config(tmp_path: Path) -> StorageConfig:
    """Return a StorageConfig for local storage in a temp directory."""
    return StorageConfig(
        type=StorageType.LOCAL,
        local_path=tmp_path / "backups",
    )
