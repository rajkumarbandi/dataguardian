"""
Structured JSON logger for the DataGuardian platform.

Every pipeline component should obtain its logger via ``get_logger()``.
The returned ``DataGuardianLogger`` carries fixed context fields
(correlation_id, entity, source_system) that appear in every log record,
making it trivial to filter a single pipeline run in any log aggregator.

Design decisions
----------------
* Standard ``logging`` module — no third-party dependency required at runtime
  on Databricks, where ``structlog`` may not be pre-installed.
* JSON formatter writes one JSON object per line, compatible with Azure Monitor
  Logs, Databricks log delivery, and any ELK/Splunk pipeline.
* ``bind()`` returns a *new* logger with additional context merged in, so the
  original logger is never mutated (safe for concurrent use).
* File handler uses ``RotatingFileHandler`` (10 MB / 5 backups) to prevent
  disk exhaustion in long-running local test runs.
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import traceback
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# JSON formatter
# ---------------------------------------------------------------------------


class JsonFormatter(logging.Formatter):
    """
    Formats a ``LogRecord`` as a single-line JSON object.

    Fixed fields emitted on every record: timestamp, level, logger,
    correlation_id, entity, source_system, message.  Any extra keyword
    arguments passed via ``extra=`` are merged into the top-level object.
    """

    def __init__(self, context: dict[str, Any] | None = None) -> None:
        super().__init__()
        self._context: dict[str, Any] = context or {}

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
        }
        payload.update(self._context)

        # Merge any extra fields supplied via extra={} on the log call
        for key, value in record.__dict__.items():
            if key not in _STDLIB_RECORD_ATTRS and not key.startswith("_"):
                payload[key] = value

        payload["message"] = record.getMessage()

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        elif record.exc_text:
            payload["exception"] = record.exc_text

        return json.dumps(payload, default=str)


# Attributes that belong to the LogRecord itself — not user-supplied extras
_STDLIB_RECORD_ATTRS: frozenset[str] = frozenset(
    {
        "args", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "message",
        "module", "msecs", "msg", "name", "pathname", "process",
        "processName", "relativeCreated", "stack_info", "thread",
        "threadName", "taskName",
    }
)


# ---------------------------------------------------------------------------
# DataGuardianLogger — thin wrapper with context binding
# ---------------------------------------------------------------------------


class DataGuardianLogger:
    """
    A logger that carries immutable context fields on every record.

    Obtain an instance via ``get_logger()`` rather than constructing directly.
    Call ``bind()`` to create a child logger with additional fields added.
    """

    def __init__(
        self,
        name: str,
        level: int = logging.INFO,
        context: dict[str, Any] | None = None,
    ) -> None:
        self._name = name
        self._context: dict[str, Any] = context or {}
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)

    # ------------------------------------------------------------------
    # Context management
    # ------------------------------------------------------------------

    def bind(self, **fields: Any) -> DataGuardianLogger:
        """Return a new logger with ``fields`` merged into the context."""
        merged = {**self._context, **fields}
        child = DataGuardianLogger(
            name=self._name,
            level=self._logger.level,
            context=merged,
        )
        # Share the underlying logging.Logger so handlers are inherited
        child._logger = self._logger
        return child

    # ------------------------------------------------------------------
    # Logging methods
    # ------------------------------------------------------------------

    def debug(self, message: str, **kwargs: Any) -> None:
        self._log(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        self._log(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        self._log(logging.WARNING, message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        self._log(logging.ERROR, message, **kwargs)

    def critical(self, message: str, **kwargs: Any) -> None:
        self._log(logging.CRITICAL, message, **kwargs)

    def exception(self, message: str, **kwargs: Any) -> None:
        """Log ERROR with the current exception traceback automatically."""
        kwargs["exception_detail"] = traceback.format_exc()
        self._log(logging.ERROR, message, **kwargs)

    def _log(self, level: int, message: str, **kwargs: Any) -> None:
        extra = {**self._context, **kwargs}
        self._logger.log(level, message, extra=extra)


# ---------------------------------------------------------------------------
# Factory function
# ---------------------------------------------------------------------------


def get_logger(
    name: str = "dataguardian",
    *,
    level: str = "INFO",
    enable_console: bool = True,
    log_file: str | None = None,
    **context: Any,
) -> DataGuardianLogger:
    """
    Create (or retrieve) a ``DataGuardianLogger`` with the given context.

    Parameters
    ----------
    name:
        Logger name — shown in the ``logger`` JSON field.  Use the module
        ``__name__`` for component-level loggers.
    level:
        Logging level string: ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``.
    enable_console:
        When ``True``, attach a ``StreamHandler`` if none is present.
    log_file:
        Optional path to a rotating log file.  Pass ``None`` to disable.
    **context:
        Arbitrary key/value pairs embedded in every log record produced by
        the returned logger (e.g. ``correlation_id``, ``entity``).
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    underlying = logging.getLogger(name)

    # Avoid adding duplicate handlers when get_logger is called multiple times
    if not underlying.handlers:
        if enable_console:
            _add_console_handler(underlying, numeric_level, context)
        if log_file:
            _add_file_handler(underlying, numeric_level, context, log_file)

    underlying.setLevel(numeric_level)
    # Prevent propagation to the root logger to avoid double-printing
    underlying.propagate = False

    return DataGuardianLogger(name=name, level=numeric_level, context=context)


def _add_console_handler(
    logger: logging.Logger,
    level: int,
    context: dict[str, Any],
) -> None:
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(JsonFormatter(context))
    logger.addHandler(handler)


def _add_file_handler(
    logger: logging.Logger,
    level: int,
    context: dict[str, Any],
    log_file: str,
) -> None:
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    handler = logging.handlers.RotatingFileHandler(
        filename=log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    handler.setLevel(level)
    handler.setFormatter(JsonFormatter(context))
    logger.addHandler(handler)


# ---------------------------------------------------------------------------
# Convenience: pipeline-run logger
# ---------------------------------------------------------------------------


def get_pipeline_logger(
    source_system: str,
    entity: str,
    batch_id: str,
    level: str | None = None,
) -> DataGuardianLogger:
    """
    Return a logger pre-bound with the standard pipeline context fields.

    Shortcut used by the ingestion engine so every log record automatically
    carries ``source_system``, ``entity``, and ``batch_id``.
    """
    effective_level = level or os.getenv("DATAGUARDIAN_LOG_LEVEL", "INFO")
    return get_logger(
        name=f"dataguardian.{source_system}.{entity}",
        level=effective_level,
        source_system=source_system,
        entity=entity,
        batch_id=batch_id,
    )
