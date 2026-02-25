"""AWS S3 storage backend with multipart upload support."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from db_vault.core.exceptions import StorageError
from db_vault.logging import get_logger
from db_vault.storage.base import BaseStorage

log = get_logger(__name__)

# Multipart upload configuration
_MULTIPART_THRESHOLD = 100 * 1024 * 1024  # 100 MB
_MULTIPART_CHUNKSIZE = 50 * 1024 * 1024  # 50 MB
_MAX_CONCURRENCY = 10


class S3Storage(BaseStorage):
    """Store backup files in AWS S3."""

    def __init__(
            self,
            bucket: str,
            prefix: str = "db-vault/",
            region: str = "us-east-1",
            endpoint_url: str | None = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/" if prefix else ""
        self.region = region
        self.endpoint_url = endpoint_url

        boto_config = BotoConfig(
            region_name=region,
            retries={"max_attempts": 3, "mode": "adaptive"},
        )

        session_kwargs: dict[str, Any] = {}
        client_kwargs: dict[str, Any] = {"config": boto_config}
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url

        self._session = boto3.Session(**session_kwargs)
        self._client = self._session.client("s3", **client_kwargs)

        # TransferConfig for multipart uploads
        from boto3.s3.transfer import TransferConfig

        self._transfer_config = TransferConfig(
            multipart_threshold=_MULTIPART_THRESHOLD,
            multipart_chunksize=_MULTIPART_CHUNKSIZE,
            max_concurrency=_MAX_CONCURRENCY,
            use_threads=True,
        )

    def _full_key(self, remote_key: str) -> str:
        """Prepend the configured prefix to a key."""
        return f"{self.prefix}{remote_key}"

    def upload(self, local_path: Path, remote_key: str) -> str:
        """Upload a backup file to S3 with multipart support for large files."""
        full_key = self._full_key(remote_key)
        file_size = local_path.stat().st_size

        log.info(
            "s3_upload_start",
            bucket=self.bucket,
            key=full_key,
            size=file_size,
        )

        extra_args: dict[str, str] = {
            "ServerSideEncryption": "AES256",
        }

        try:
            self._client.upload_file(
                str(local_path),
                self.bucket,
                full_key,
                ExtraArgs=extra_args,
                Config=self._transfer_config,
                Callback=_ProgressCallback(file_size, full_key),
            )
        except ClientError as exc:
            raise StorageError(f"S3 upload failed: {exc}") from exc

        location = f"s3://{self.bucket}/{full_key}"
        log.info("s3_upload_complete", location=location, size=file_size)
        return location

    def download(self, remote_key: str, local_path: Path) -> Path:
        """Download a backup file from S3."""
        full_key = self._full_key(remote_key)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        log.info("s3_download_start", bucket=self.bucket, key=full_key)

        try:
            self._client.download_file(
                self.bucket,
                full_key,
                str(local_path),
                Config=self._transfer_config,
            )
        except ClientError as exc:
            raise StorageError(f"S3 download failed: {exc}") from exc

        log.info(
            "s3_download_complete",
            destination=str(local_path),
            size=local_path.stat().st_size,
        )
        return local_path

    def list_backups(self, prefix: str = "") -> list[dict[str, str]]:
        """List backup files in the S3 bucket under the configured prefix."""
        full_prefix = self._full_key(prefix)
        results: list[dict[str, str]] = []

        try:
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
                for obj in page.get("Contents", []):
                    # Strip the base prefix for display
                    key = obj["Key"]
                    if self.prefix and key.startswith(self.prefix):
                        key = key[len(self.prefix):]
                    results.append({
                        "key": key,
                        "size": str(obj["Size"]),
                        "last_modified": obj["LastModified"].isoformat(),
                    })
        except ClientError as exc:
            raise StorageError(f"Failed to list S3 objects: {exc}") from exc

        return results

    def delete(self, remote_key: str) -> None:
        """Delete a backup file from S3."""
        full_key = self._full_key(remote_key)

        try:
            self._client.delete_object(Bucket=self.bucket, Key=full_key)
            log.info("s3_delete_complete", bucket=self.bucket, key=full_key)
        except ClientError as exc:
            raise StorageError(f"Failed to delete S3 object: {exc}") from exc

    def exists(self, remote_key: str) -> bool:
        """Check if a file exists in S3."""
        full_key = self._full_key(remote_key)
        try:
            self._client.head_object(Bucket=self.bucket, Key=full_key)
            return True
        except ClientError:
            return False


class _ProgressCallback:
    """Callback for tracking S3 upload progress."""

    def __init__(self, total_size: int, key: str) -> None:
        self._total = total_size
        self._key = key
        self._uploaded = 0
        self._last_pct = -1

    def __call__(self, bytes_transferred: int) -> None:
        self._uploaded += bytes_transferred
        if self._total > 0:
            pct = int(self._uploaded * 100 / self._total)
            # Log every 10% to avoid flood
            if pct >= self._last_pct + 10:
                self._last_pct = pct
                log.debug("s3_upload_progress", key=self._key, percent=pct)
