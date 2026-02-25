"""Storage backend registry."""

from __future__ import annotations

from db_vault.core.exceptions import StorageError
from db_vault.core.models import StorageConfig, StorageType
from db_vault.storage.base import BaseStorage


def get_storage(config: StorageConfig) -> BaseStorage:
    """Instantiate the appropriate storage backend.

    Raises:
        StorageError: If the storage type is not supported or misconfigured.
    """
    if config.type == StorageType.LOCAL:
        from db_vault.storage.local import LocalStorage

        return LocalStorage(base_path=config.local_path)

    if config.type == StorageType.S3:
        if not config.s3_bucket:
            raise StorageError("S3 bucket name is required (--s3-bucket or DB_VAULT_S3_BUCKET)")
        from db_vault.storage.s3 import S3Storage

        return S3Storage(
            bucket=config.s3_bucket,
            prefix=config.s3_prefix,
            region=config.s3_region,
            endpoint_url=config.s3_endpoint_url,
        )

    raise StorageError(f"Unsupported storage type: {config.type}")


__all__ = ["BaseStorage", "get_storage"]
