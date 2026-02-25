"""Slack webhook notification backend."""

from __future__ import annotations

from typing import Any

import httpx

from db_vault.core.exceptions import NotificationError
from db_vault.core.models import BackupMetadata, BackupStatus
from db_vault.logging import get_logger
from db_vault.notifications.base import BaseNotifier

log = get_logger(__name__)


class SlackNotifier(BaseNotifier):
    """Send backup notifications via Slack Incoming Webhook."""

    def __init__(self, webhook_url: str, timeout: float = 30.0) -> None:
        self.webhook_url = webhook_url
        self.timeout = timeout

    def notify_success(self, metadata: BackupMetadata) -> None:
        """Send a success notification to Slack."""
        payload = self._build_payload(metadata, success=True)
        self._send(payload)

    def notify_failure(self, metadata: BackupMetadata) -> None:
        """Send a failure notification to Slack."""
        payload = self._build_payload(metadata, success=False)
        self._send(payload)

    def _send(self, payload: dict[str, Any]) -> None:
        """POST the payload to the Slack webhook URL."""
        try:
            response = httpx.post(
                self.webhook_url,
                json=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()
            log.info("slack_notification_sent")
        except httpx.HTTPError as exc:
            log.error("slack_notification_failed", error=str(exc))
            raise NotificationError(f"Slack notification failed: {exc}") from exc

    @staticmethod
    def _build_payload(metadata: BackupMetadata, *, success: bool) -> dict[str, Any]:
        """Build a Slack Block Kit message payload."""
        color = "#36a64f" if success else "#e01e5a"
        status_emoji = ":white_check_mark:" if success else ":x:"
        status_text = "Completed" if success else "Failed"

        fields = [
            {"type": "mrkdwn", "text": f"*Database:*\n{metadata.database_name}"},
            {"type": "mrkdwn", "text": f"*Type:*\n{metadata.database_type.value}"},
            {"type": "mrkdwn", "text": f"*Backup Type:*\n{metadata.backup_type.value}"},
            {"type": "mrkdwn", "text": f"*Duration:*\n{metadata.duration_seconds:.1f}s"},
            {"type": "mrkdwn", "text": f"*Size:*\n{metadata.size_human}"},
            {"type": "mrkdwn", "text": f"*Storage:*\n{metadata.storage_type.value}"},
        ]

        blocks: list[dict[str, Any]] = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{status_emoji} DB Vault Backup {status_text}",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "fields": fields,
            },
        ]

        if metadata.status == BackupStatus.FAILED and metadata.error_message:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error:*\n```{metadata.error_message}```",
                },
            })

        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Backup ID: `{metadata.id}` | {metadata.timestamp.isoformat()}",
                }
            ],
        })

        return {
            "attachments": [{"color": color, "blocks": blocks}],
        }
