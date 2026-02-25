"""Configuration loading and management for db-vault.

Configuration sources (highest to lowest priority):
  1. CLI arguments (passed directly)
  2. Environment variables (DB_VAULT_* prefix)
  3. Config file (~/.config/db-vault/config.toml)
  4. Defaults
"""

from __future__ import annotations

import contextlib
import os
import sys
import tomllib
from pathlib import Path
from typing import Any

import tomli_w

from db_vault.core.exceptions import ConfigError
from db_vault.core.models import (
    AppConfig,
    CompressionAlgorithm,
    CompressionConfig,
    DatabaseConfig,
    DatabaseType,
    LogFormat,
    LoggingConfig,
    NotificationConfig,
    StorageConfig,
    StorageType,
)

# ──────────────────── Paths ──────────────────────────────

_APP_NAME = "db-vault"


def _get_config_dir() -> Path:
    """Return the platform-appropriate config directory."""
    if sys.platform == "darwin":
        base = Path.home() / "Library" / "Application Support"
    elif sys.platform == "win32":
        base = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
    else:
        base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    return base / _APP_NAME


def _get_data_dir() -> Path:
    """Return the platform-appropriate data directory."""
    if sys.platform == "darwin":
        base = Path.home() / "Library" / "Application Support"
    elif sys.platform == "win32":
        base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
    else:
        base = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    return base / _APP_NAME


CONFIG_DIR = _get_config_dir()
DATA_DIR = _get_data_dir()
CONFIG_FILE = CONFIG_DIR / "config.toml"
LOG_DIR = DATA_DIR / "logs"
SCHEDULER_DB = DATA_DIR / "jobs.db"
METADATA_DIR = DATA_DIR / "metadata"

# ──────────────────── Environment Loading ────────────────

_ENV_PREFIX = "DB_VAULT_"


def _env(key: str, default: str | None = None) -> str | None:
    """Read an environment variable with the DB_VAULT_ prefix."""
    return os.environ.get(f"{_ENV_PREFIX}{key}", default)


def _load_db_from_env() -> DatabaseConfig | None:
    """Attempt to build a DatabaseConfig from environment variables."""
    db_type = _env("DB_TYPE")
    if not db_type:
        return None
    try:
        return DatabaseConfig(
            type=DatabaseType(db_type.lower()),
            host=_env("DB_HOST", "localhost"),  # type: ignore[arg-type]
            port=int(p) if (p := _env("DB_PORT")) else None,
            username=_env("DB_USERNAME"),
            password=_env("DB_PASSWORD"),
            database=_env("DB_NAME", ""),  # type: ignore[arg-type]
            ssl=(_env("DB_SSL", "false") or "false").lower() in ("true", "1", "yes"),
        )
    except (ValueError, KeyError) as exc:
        raise ConfigError(f"Invalid database config in environment: {exc}") from exc


def _load_storage_from_env() -> dict[str, Any]:
    """Load storage config overrides from environment."""
    overrides: dict[str, Any] = {}
    if st := _env("STORAGE_TYPE"):
        overrides["type"] = StorageType(st.lower())
    if lp := _env("STORAGE_LOCAL_PATH"):
        overrides["local_path"] = Path(lp)
    if sb := _env("S3_BUCKET"):
        overrides["s3_bucket"] = sb
    if sp := _env("S3_PREFIX"):
        overrides["s3_prefix"] = sp
    if sr := _env("S3_REGION"):
        overrides["s3_region"] = sr
    if se := _env("S3_ENDPOINT_URL"):
        overrides["s3_endpoint_url"] = se
    return overrides


def _load_compression_from_env() -> dict[str, Any]:
    """Load compression config overrides from environment."""
    overrides: dict[str, Any] = {}
    if ca := _env("COMPRESSION"):
        overrides["algorithm"] = CompressionAlgorithm(ca.lower())
    if cl := _env("COMPRESSION_LEVEL"):
        overrides["level"] = int(cl)
    return overrides


def _load_notification_from_env() -> dict[str, Any]:
    """Load notification config overrides from environment."""
    overrides: dict[str, Any] = {}
    if sw := _env("SLACK_WEBHOOK_URL"):
        overrides["slack_webhook_url"] = sw
    return overrides


def _load_logging_from_env() -> dict[str, Any]:
    """Load logging config overrides from environment."""
    overrides: dict[str, Any] = {}
    if ll := _env("LOG_LEVEL"):
        overrides["level"] = ll.upper()
    if lf := _env("LOG_FILE"):
        overrides["log_file"] = Path(lf)
    if fmt := _env("LOG_FORMAT"):
        overrides["format"] = LogFormat(fmt.lower())
    return overrides


# ──────────────────── TOML File Loading ──────────────────


def load_config_file(path: Path | None = None) -> dict[str, Any]:
    """Load and return the raw TOML config dict. Returns empty dict if file missing."""
    config_path = path or CONFIG_FILE
    if not config_path.exists():
        return {}
    try:
        with open(config_path, "rb") as f:
            return tomllib.load(f)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Invalid TOML in {config_path}: {exc}") from exc


def save_config_file(config: AppConfig, path: Path | None = None) -> Path:
    """Save AppConfig to a TOML file."""
    config_path = path or CONFIG_FILE
    config_path.parent.mkdir(parents=True, exist_ok=True)

    data = _config_to_toml_dict(config)
    with open(config_path, "wb") as f:
        tomli_w.dump(data, f)

    # Restrict file permissions (Unix only)
    with contextlib.suppress(OSError):
        config_path.chmod(0o600)

    return config_path


def _config_to_toml_dict(config: AppConfig) -> dict[str, Any]:
    """Convert an AppConfig to a TOML-serialisable dict."""
    data: dict[str, Any] = {}

    # Databases
    if config.databases:
        data["databases"] = {}
        for name, db in config.databases.items():
            db_dict = db.model_dump(exclude_none=True)
            db_dict["type"] = db.type.value
            if db.password:
                db_dict["password"] = db.password.get_secret_value()
            data["databases"][name] = db_dict

    # Storage
    storage_dict = config.storage.model_dump(exclude_none=True)
    storage_dict["type"] = config.storage.type.value
    storage_dict["local_path"] = str(config.storage.local_path)
    data["storage"] = storage_dict

    # Compression
    comp_dict = config.compression.model_dump()
    comp_dict["algorithm"] = config.compression.algorithm.value
    data["compression"] = comp_dict

    # Notification
    notif_dict = config.notification.model_dump(exclude_none=True)
    if config.notification.slack_webhook_url:
        notif_dict["slack_webhook_url"] = (
            config.notification.slack_webhook_url.get_secret_value()
        )
    data["notification"] = notif_dict

    # Logging
    log_dict = config.logging.model_dump(exclude_none=True)
    log_dict["format"] = config.logging.format.value
    if config.logging.log_file:
        log_dict["log_file"] = str(config.logging.log_file)
    data["logging"] = log_dict

    return data


# ──────────────────── Main Loader ────────────────────────


def load_config(config_path: Path | None = None) -> AppConfig:
    """Load the full application config (file + env overrides)."""
    raw = load_config_file(config_path)

    # Build databases from file
    databases: dict[str, DatabaseConfig] = {}
    for name, db_data in raw.get("databases", {}).items():
        databases[name] = DatabaseConfig(**db_data)

    # Add env-based database as "default" if set
    env_db = _load_db_from_env()
    if env_db:
        databases["default"] = env_db

    # Storage: file defaults + env overrides
    storage_data = raw.get("storage", {})
    storage_data.update(_load_storage_from_env())
    storage = StorageConfig(**storage_data) if storage_data else StorageConfig()

    # Compression
    comp_data = raw.get("compression", {})
    comp_data.update(_load_compression_from_env())
    compression = CompressionConfig(**comp_data) if comp_data else CompressionConfig()

    # Notification
    notif_data = raw.get("notification", {})
    notif_data.update(_load_notification_from_env())
    notification = NotificationConfig(**notif_data) if notif_data else NotificationConfig()

    # Logging
    log_data = raw.get("logging", {})
    log_data.update(_load_logging_from_env())
    logging_config = LoggingConfig(**log_data) if log_data else LoggingConfig()

    return AppConfig(
        databases=databases,
        storage=storage,
        compression=compression,
        notification=notification,
        logging=logging_config,
    )


def ensure_dirs() -> None:
    """Create required application directories if they don't exist."""
    for d in (CONFIG_DIR, DATA_DIR, LOG_DIR, METADATA_DIR):
        d.mkdir(parents=True, exist_ok=True)
