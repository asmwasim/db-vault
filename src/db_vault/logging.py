"""Structured logging configuration for db-vault using structlog."""

from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path

import structlog

from db_vault.core.models import LogFormat

# Keys whose values should be redacted in log output
_SENSITIVE_KEYS = frozenset({
    "password",
    "secret",
    "token",
    "webhook_url",
    "access_key",
    "secret_key",
    "authorization",
})


def _redact_sensitive(
        _logger: logging.Logger,
        _method: str,
        event_dict: dict,
) -> dict:
    """Redact values of keys that look like secrets."""
    for key in event_dict:
        if any(s in key.lower() for s in _SENSITIVE_KEYS):
            event_dict[key] = "***REDACTED***"
    return event_dict


def setup_logging(
        level: str = "INFO",
        log_file: Path | None = None,
        log_format: LogFormat = LogFormat.CONSOLE,
) -> None:
    """Configure structlog + stdlib logging for the application.

    Args:
        level: Log level name (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_file: Optional path to a log file. Parent dirs are created automatically.
        log_format: Output format - console (human-friendly) or json (machine-parseable).
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Shared processors for structlog
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        _redact_sensitive,
        structlog.processors.UnicodeDecoder(),
    ]

    if log_format == LogFormat.JSON:
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Build a ProcessorFormatter for stdlib handlers
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    # Console handler (always present)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(console_handler)
    root_logger.setLevel(log_level)

    # File handler (optional)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        # JSON format for file output regardless of console format
        file_formatter = structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
        )
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

    # Silence noisy third-party loggers
    for name in ("boto3", "botocore", "urllib3", "s3transfer", "pymongo"):
        logging.getLogger(name).setLevel(logging.WARNING)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a structlog bound logger."""
    return structlog.get_logger(name)
