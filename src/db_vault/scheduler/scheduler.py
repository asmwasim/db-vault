"""Built-in backup scheduler using APScheduler with persistent job store."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, JobEvent
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from db_vault.core.config import SCHEDULER_DB
from db_vault.logging import get_logger

log = get_logger(__name__)


class BackupScheduler:
    """Manages scheduled backup jobs using APScheduler.

    Jobs are persisted in a local SQLite database so they survive restarts.
    """

    def __init__(self, db_path: Path | None = None) -> None:
        db = db_path or SCHEDULER_DB
        db.parent.mkdir(parents=True, exist_ok=True)

        jobstores = {
            "default": SQLAlchemyJobStore(url=f"sqlite:///{db}"),
        }
        executors = {
            "default": ThreadPoolExecutor(max_workers=4),
        }
        job_defaults: dict[str, Any] = {
            "coalesce": True,  # Combine missed runs into one
            "max_instances": 1,  # Only one instance of each job at a time
            "misfire_grace_time": 3600,  # Allow 1 hour grace for missed jobs
        }

        self._scheduler = BlockingScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
        )

        # Register event listeners
        self._scheduler.add_listener(self._on_job_executed, EVENT_JOB_EXECUTED)
        self._scheduler.add_listener(self._on_job_error, EVENT_JOB_ERROR)

    def add_job(
            self,
            job_id: str,
            func: Callable[..., Any],
            cron_expression: str,
            kwargs: dict[str, Any] | None = None,
            name: str | None = None,
    ) -> None:
        """Add or replace a scheduled backup job.

        Args:
            job_id: Unique identifier for the job.
            func: The callable to execute (e.g. run_backup).
            cron_expression: Standard cron expression (e.g. '0 2 * * *').
            kwargs: Keyword arguments to pass to the function.
            name: Human-readable name for the job.
        """
        trigger = CronTrigger.from_crontab(cron_expression)

        self._scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            name=name or job_id,
            kwargs=kwargs or {},
            replace_existing=True,
        )
        log.info(
            "schedule_job_added",
            job_id=job_id,
            cron=cron_expression,
            name=name,
        )

    def remove_job(self, job_id: str) -> None:
        """Remove a scheduled job by ID."""
        try:
            self._scheduler.remove_job(job_id)
            log.info("schedule_job_removed", job_id=job_id)
        except Exception as exc:
            log.error("schedule_job_remove_failed", job_id=job_id, error=str(exc))
            raise

    def list_jobs(self) -> list[dict[str, Any]]:
        """Return a list of all scheduled jobs with their details."""
        jobs = self._scheduler.get_jobs()
        result = []
        for job in jobs:
            result.append({
                "id": job.id,
                "name": job.name,
                "next_run": str(job.next_run_time) if job.next_run_time else "paused",
                "trigger": str(job.trigger),
            })
        return result

    def pause_job(self, job_id: str) -> None:
        """Pause a scheduled job."""
        self._scheduler.pause_job(job_id)
        log.info("schedule_job_paused", job_id=job_id)

    def resume_job(self, job_id: str) -> None:
        """Resume a paused job."""
        self._scheduler.resume_job(job_id)
        log.info("schedule_job_resumed", job_id=job_id)

    def start(self) -> None:
        """Start the scheduler (blocking). Runs until interrupted."""
        job_count = len(self._scheduler.get_jobs())
        log.info("scheduler_starting", jobs=job_count)
        try:
            self._scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            log.info("scheduler_stopped")

    def shutdown(self) -> None:
        """Gracefully shut down the scheduler."""
        self._scheduler.shutdown(wait=True)
        log.info("scheduler_shutdown")

    @staticmethod
    def _on_job_executed(event: JobEvent) -> None:
        log.info(
            "schedule_job_executed",
            job_id=event.job_id,
            scheduled_time=str(event.scheduled_run_time),
        )

    @staticmethod
    def _on_job_error(event: JobEvent) -> None:
        log.error(
            "schedule_job_error",
            job_id=event.job_id,
            error=str(event.exception),
            traceback=str(event.traceback),
        )
