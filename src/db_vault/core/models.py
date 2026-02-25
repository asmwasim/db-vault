"""Pydantic models for db-vault configuration and metadata."""

from __future__ import annotations

import enum
import uuid
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator


# ──────────────────────── Enums ──────────────────────────


class DatabaseType(enum.StrEnum):
    """Supported database management systems."""

    POSTGRES = "postgres"
    MYSQL = "mysql"
    MONGODB = "mongodb"
    SQLITE = "sqlite"


class BackupType(enum.StrEnum):
    """Supported backup strategies."""

    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"


class CompressionAlgorithm(enum.StrEnum):
    """Supported compression algorithms."""

    ZSTD = "zstd"
    GZIP = "gzip"
    LZ4 = "lz4"
    NONE = "none"


class StorageType(enum.StrEnum):
    """Supported storage backends."""

    LOCAL = "local"
    S3 = "s3"


class BackupStatus(enum.StrEnum):
    """Status of a backup operation."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class LogFormat(enum.StrEnum):
    """Structured log output format."""

    CONSOLE = "console"
    JSON = "json"


# ──────────────────── Config Models ──────────────────────


class DatabaseConfig(BaseModel):
    """Database connection parameters."""

    type: DatabaseType
    host: str = "localhost"
    port: int | None = None
    username: str | None = None
    password: SecretStr | None = None
    database: str = ""
    ssl: bool = False

    @model_validator(mode="after")
    def set_default_port(self) -> DatabaseConfig:
        if self.port is None:
            defaults = {
                DatabaseType.POSTGRES: 5432,
                DatabaseType.MYSQL: 3306,
                DatabaseType.MONGODB: 27017,
            }
            self.port = defaults.get(self.type)
        return self

    @property
    def connection_string(self) -> str:
        """Build a connection string (password masked)."""
        if self.type == DatabaseType.SQLITE:
            return f"sqlite:///{self.database}"
        user = self.username or ""
        host = f"{self.host}:{self.port}" if self.port else self.host
        return f"{self.type.value}://{user}@{host}/{self.database}"


class StorageConfig(BaseModel):
    """Storage backend configuration."""

    type: StorageType = StorageType.LOCAL
    local_path: Path = Path("./backups")

    # S3 settings
    s3_bucket: str | None = None
    s3_prefix: str = "db-vault/"
    s3_region: str = "us-east-1"
    s3_endpoint_url: str | None = None


class CompressionConfig(BaseModel):
    """Compression settings."""

    algorithm: CompressionAlgorithm = CompressionAlgorithm.ZSTD
    level: int = 3

    @field_validator("level")
    @classmethod
    def validate_level(cls, v: int) -> int:
        if v < 1 or v > 22:
            msg = "Compression level must be between 1 and 22"
            raise ValueError(msg)
        return v


class NotificationConfig(BaseModel):
    """Notification settings."""

    slack_webhook_url: SecretStr | None = None
    notify_on_success: bool = True
    notify_on_failure: bool = True


class LoggingConfig(BaseModel):
    """Logging settings."""

    level: str = "INFO"
    log_file: Path | None = None
    format: LogFormat = LogFormat.CONSOLE


class ScheduleEntry(BaseModel):
    """A single scheduled backup job."""

    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    name: str = ""
    cron: str  # cron expression, e.g. "0 2 * * *"
    database: DatabaseConfig
    backup_type: BackupType = BackupType.FULL
    compression: CompressionConfig = CompressionConfig()
    storage: StorageConfig = StorageConfig()
    notification: NotificationConfig | None = None
    enabled: bool = True


class AppConfig(BaseModel):
    """Top-level application configuration."""

    databases: dict[str, DatabaseConfig] = Field(default_factory=dict)
    storage: StorageConfig = StorageConfig()
    compression: CompressionConfig = CompressionConfig()
    notification: NotificationConfig = NotificationConfig()
    logging: LoggingConfig = LoggingConfig()
    schedules: dict[str, ScheduleEntry] = Field(default_factory=dict)


# ──────────────────── Metadata Models ────────────────────


class BackupMetadata(BaseModel):
    """Metadata describing a completed backup."""

    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:16])
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    database_name: str
    database_type: DatabaseType
    backup_type: BackupType
    file_name: str
    file_path: str
    file_size: int = 0  # bytes
    compressed_size: int = 0  # bytes
    compression: CompressionAlgorithm = CompressionAlgorithm.NONE
    checksum_sha256: str = ""
    duration_seconds: float = 0.0
    status: BackupStatus = BackupStatus.PENDING
    storage_type: StorageType = StorageType.LOCAL
    error_message: str | None = None
    tables: list[str] | None = None  # if specific tables were backed up

    @property
    def compression_ratio(self) -> float:
        """Return compression ratio (0.0-1.0). Lower is better compression."""
        if self.file_size == 0:
            return 0.0
        return self.compressed_size / self.file_size

    @property
    def size_human(self) -> str:
        """Return human-readable compressed size."""
        return _human_size(self.compressed_size or self.file_size)


class RestoreRequest(BaseModel):
    """Parameters for a restore operation."""

    backup_file: Path
    target_database: str | None = None
    tables: list[str] | None = None
    dry_run: bool = False
    drop_existing: bool = False


# ──────────────────── Helpers ────────────────────────────


def _human_size(nbytes: int) -> str:
    """Convert bytes to human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024  # type: ignore[assignment]
    return f"{nbytes:.1f} PB"
