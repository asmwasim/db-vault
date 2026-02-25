"""Custom exceptions for db-vault."""


class DbVaultError(Exception):
    """Base exception for all db-vault errors."""


class ConnectionError(DbVaultError):
    """Raised when a database connection fails."""


class BackupError(DbVaultError):
    """Raised when a backup operation fails."""


class RestoreError(DbVaultError):
    """Raised when a restore operation fails."""


class StorageError(DbVaultError):
    """Raised when a storage operation fails."""


class CompressionError(DbVaultError):
    """Raised when compression or decompression fails."""


class ConfigError(DbVaultError):
    """Raised when configuration is invalid or missing."""


class SchedulerError(DbVaultError):
    """Raised when scheduler operations fail."""


class NotificationError(DbVaultError):
    """Raised when sending a notification fails."""


class EngineNotFoundError(DbVaultError):
    """Raised when a requested database engine is not available."""


class BackupNotFoundError(DbVaultError):
    """Raised when a specified backup file or ID cannot be found."""
