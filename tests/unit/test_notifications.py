"""Tests for notification backends."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from db_vault.core.exceptions import NotificationError
from db_vault.core.models import (
    BackupMetadata,
    BackupStatus,
    BackupType,
    DatabaseType,
)
from db_vault.notifications.slack import SlackNotifier


class TestSlackNotifier:
    @pytest.fixture()
    def metadata(self) -> BackupMetadata:
        return BackupMetadata(
            database_name="testdb",
            database_type=DatabaseType.POSTGRES,
            backup_type=BackupType.FULL,
            file_name="backup.dump.zst",
            file_path="s3://bucket/backup.dump.zst",
            file_size=1000000,
            compressed_size=300000,
            duration_seconds=12.5,
            status=BackupStatus.COMPLETED,
        )

    def test_build_success_payload(self, metadata: BackupMetadata) -> None:
        payload = SlackNotifier._build_payload(metadata, success=True)
        assert "attachments" in payload
        assert payload["attachments"][0]["color"] == "#36a64f"

    def test_build_failure_payload(self, metadata: BackupMetadata) -> None:
        metadata.status = BackupStatus.FAILED
        metadata.error_message = "pg_dump not found"
        payload = SlackNotifier._build_payload(metadata, success=False)
        assert payload["attachments"][0]["color"] == "#e01e5a"
        # Should contain error block
        blocks = payload["attachments"][0]["blocks"]
        error_blocks = [b for b in blocks if b["type"] == "section" and "Error" in str(b)]
        assert len(error_blocks) > 0

    @patch("db_vault.notifications.slack.httpx.post")
    def test_notify_success(self, mock_post: MagicMock, metadata: BackupMetadata) -> None:
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        notifier = SlackNotifier("https://hooks.slack.com/test")
        notifier.notify_success(metadata)

        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        assert "https://hooks.slack.com/test" in call_kwargs.args

    @patch("db_vault.notifications.slack.httpx.post")
    def test_notify_failure_http_error(
            self, mock_post: MagicMock, metadata: BackupMetadata,
    ) -> None:
        import httpx

        mock_post.side_effect = httpx.HTTPError("Connection refused")

        notifier = SlackNotifier("https://hooks.slack.com/test")
        with pytest.raises(NotificationError, match="Slack notification failed"):
            notifier.notify_failure(metadata)
