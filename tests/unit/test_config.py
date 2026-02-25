"""Tests for configuration loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from db_vault.core.config import load_config, load_config_file, save_config_file
from db_vault.core.models import (
    AppConfig,
    CompressionAlgorithm,
    CompressionConfig,
    DatabaseConfig,
    DatabaseType,
    StorageConfig,
    StorageType,
)


class TestLoadConfigFile:
    def test_missing_file(self, tmp_path: Path) -> None:
        result = load_config_file(tmp_path / "nonexistent.toml")
        assert result == {}

    def test_valid_toml(self, tmp_path: Path) -> None:
        config_path = tmp_path / "config.toml"
        config_path.write_text("""
[storage]
type = "local"
local_path = "./my-backups"

[compression]
algorithm = "gzip"
level = 6
""")
        result = load_config_file(config_path)
        assert result["storage"]["type"] == "local"
        assert result["compression"]["algorithm"] == "gzip"
        assert result["compression"]["level"] == 6

    def test_invalid_toml(self, tmp_path: Path) -> None:
        config_path = tmp_path / "bad.toml"
        config_path.write_text("this is not valid [[[toml")
        from db_vault.core.exceptions import ConfigError

        with pytest.raises(ConfigError, match="Invalid TOML"):
            load_config_file(config_path)


class TestSaveConfigFile:
    def test_save_and_reload(self, tmp_path: Path) -> None:
        config = AppConfig(
            databases={
                "mydb": DatabaseConfig(
                    type=DatabaseType.POSTGRES,
                    host="db.example.com",
                    port=5432,
                    username="user",
                    password="secret",
                    database="production",
                )
            },
            storage=StorageConfig(type=StorageType.LOCAL, local_path=Path("./backups")),
            compression=CompressionConfig(algorithm=CompressionAlgorithm.GZIP, level=6),
        )

        saved = save_config_file(config, tmp_path / "config.toml")
        assert saved.exists()

        # Reload and verify
        raw = load_config_file(saved)
        assert raw["databases"]["mydb"]["type"] == "postgres"
        assert raw["databases"]["mydb"]["host"] == "db.example.com"
        assert raw["compression"]["algorithm"] == "gzip"

    def test_file_permissions(self, tmp_path: Path) -> None:
        config = AppConfig()
        saved = save_config_file(config, tmp_path / "config.toml")
        # On Unix, should be 600
        mode = oct(saved.stat().st_mode)[-3:]
        assert mode == "600"


class TestLoadConfig:
    def test_defaults(self, tmp_path: Path) -> None:
        """Loading with no file and no env vars should return defaults."""
        config = load_config(tmp_path / "nonexistent.toml")
        assert config.storage.type == StorageType.LOCAL
        assert config.compression.algorithm == CompressionAlgorithm.ZSTD
        assert config.databases == {}

    def test_env_override_db(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Environment variables should create a 'default' database."""
        monkeypatch.setenv("DB_VAULT_DB_TYPE", "mysql")
        monkeypatch.setenv("DB_VAULT_DB_HOST", "db.test")
        monkeypatch.setenv("DB_VAULT_DB_PORT", "3307")
        monkeypatch.setenv("DB_VAULT_DB_USERNAME", "testuser")
        monkeypatch.setenv("DB_VAULT_DB_NAME", "testdb")

        config = load_config(tmp_path / "nonexistent.toml")
        assert "default" in config.databases
        db = config.databases["default"]
        assert db.type == DatabaseType.MYSQL
        assert db.host == "db.test"
        assert db.port == 3307
        assert db.username == "testuser"
        assert db.database == "testdb"

    def test_env_override_storage(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DB_VAULT_STORAGE_TYPE", "s3")
        monkeypatch.setenv("DB_VAULT_S3_BUCKET", "my-bucket")
        monkeypatch.setenv("DB_VAULT_S3_REGION", "eu-west-1")

        config = load_config(tmp_path / "nonexistent.toml")
        assert config.storage.type == StorageType.S3
        assert config.storage.s3_bucket == "my-bucket"
        assert config.storage.s3_region == "eu-west-1"

    def test_env_override_compression(
            self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DB_VAULT_COMPRESSION", "lz4")
        monkeypatch.setenv("DB_VAULT_COMPRESSION_LEVEL", "5")

        config = load_config(tmp_path / "nonexistent.toml")
        assert config.compression.algorithm == CompressionAlgorithm.LZ4
        assert config.compression.level == 5
