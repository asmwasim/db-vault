"""Tests for core models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from db_vault.core.models import (
    AppConfig,
    BackupMetadata,
    BackupStatus,
    BackupType,
    CompressionAlgorithm,
    CompressionConfig,
    DatabaseConfig,
    DatabaseType,
    StorageConfig,
    StorageType,
    _human_size,
)


class TestDatabaseConfig:
    def test_default_port_postgres(self) -> None:
        config = DatabaseConfig(type=DatabaseType.POSTGRES)
        assert config.port == 5432

    def test_default_port_mysql(self) -> None:
        config = DatabaseConfig(type=DatabaseType.MYSQL)
        assert config.port == 3306

    def test_default_port_mongodb(self) -> None:
        config = DatabaseConfig(type=DatabaseType.MONGODB)
        assert config.port == 27017

    def test_default_port_sqlite(self) -> None:
        config = DatabaseConfig(type=DatabaseType.SQLITE, database="test.db")
        assert config.port is None

    def test_custom_port(self) -> None:
        config = DatabaseConfig(type=DatabaseType.POSTGRES, port=5433)
        assert config.port == 5433

    def test_connection_string_postgres(self) -> None:
        config = DatabaseConfig(
            type=DatabaseType.POSTGRES,
            host="myhost",
            port=5432,
            username="admin",
            database="mydb",
        )
        assert config.connection_string == "postgres://admin@myhost:5432/mydb"

    def test_connection_string_sqlite(self) -> None:
        config = DatabaseConfig(type=DatabaseType.SQLITE, database="/tmp/test.db")
        assert config.connection_string == "sqlite:////tmp/test.db"


class TestCompressionConfig:
    def test_default(self) -> None:
        config = CompressionConfig()
        assert config.algorithm == CompressionAlgorithm.ZSTD
        assert config.level == 3

    def test_invalid_level(self) -> None:
        with pytest.raises(ValidationError):
            CompressionConfig(level=0)

    def test_invalid_level_high(self) -> None:
        with pytest.raises(ValidationError):
            CompressionConfig(level=23)


class TestStorageConfig:
    def test_default(self) -> None:
        config = StorageConfig()
        assert config.type == StorageType.LOCAL

    def test_s3(self) -> None:
        config = StorageConfig(
            type=StorageType.S3,
            s3_bucket="my-bucket",
            s3_region="eu-west-1",
        )
        assert config.s3_bucket == "my-bucket"
        assert config.s3_region == "eu-west-1"


class TestBackupMetadata:
    def test_defaults(self) -> None:
        meta = BackupMetadata(
            database_name="testdb",
            database_type=DatabaseType.POSTGRES,
            backup_type=BackupType.FULL,
            file_name="backup.dump",
            file_path="/backups/backup.dump",
        )
        assert meta.status == BackupStatus.PENDING
        assert len(meta.id) == 16
        assert meta.timestamp is not None

    def test_compression_ratio(self) -> None:
        meta = BackupMetadata(
            database_name="testdb",
            database_type=DatabaseType.POSTGRES,
            backup_type=BackupType.FULL,
            file_name="backup.dump.zst",
            file_path="/backups/backup.dump.zst",
            file_size=1000,
            compressed_size=300,
        )
        assert meta.compression_ratio == pytest.approx(0.3)

    def test_compression_ratio_zero(self) -> None:
        meta = BackupMetadata(
            database_name="testdb",
            database_type=DatabaseType.POSTGRES,
            backup_type=BackupType.FULL,
            file_name="backup.dump",
            file_path="/backups/backup.dump",
            file_size=0,
        )
        assert meta.compression_ratio == 0.0

    def test_size_human(self) -> None:
        meta = BackupMetadata(
            database_name="testdb",
            database_type=DatabaseType.POSTGRES,
            backup_type=BackupType.FULL,
            file_name="backup.dump",
            file_path="/backups/backup.dump",
            compressed_size=1536,
        )
        assert "KB" in meta.size_human


class TestHumanSize:
    def test_bytes(self) -> None:
        assert _human_size(500) == "500.0 B"

    def test_kilobytes(self) -> None:
        assert _human_size(1024) == "1.0 KB"

    def test_megabytes(self) -> None:
        assert _human_size(1024 * 1024) == "1.0 MB"

    def test_gigabytes(self) -> None:
        assert _human_size(1024 ** 3) == "1.0 GB"


class TestAppConfig:
    def test_empty(self) -> None:
        config = AppConfig()
        assert config.databases == {}
        assert config.storage.type == StorageType.LOCAL
        assert config.compression.algorithm == CompressionAlgorithm.ZSTD
