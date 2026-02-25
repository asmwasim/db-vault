"""Abstract base class for notification backends."""

from __future__ import annotations

import abc

from db_vault.core.models import BackupMetadata


class BaseNotifier(abc.ABC):
    """Interface for sending backup notifications."""

    @abc.abstractmethod
    def notify_success(self, metadata: BackupMetadata) -> None:
        """Send a notification about a successful backup."""

    @abc.abstractmethod
    def notify_failure(self, metadata: BackupMetadata) -> None:
        """Send a notification about a failed backup."""
