"""Logging setup for the ``portolan`` command."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import UTC, datetime
from typing import TextIO

from portolan_cli.utils import redact_credentials
from portolan_core import trace

_DEBUG_VERBOSITY = 2

LOG_ENV = "PORTOLAN_LOG"
LOG_FILE_ENV = "PORTOLAN_LOG_FILE"
GDAL_DEBUG_ENV = "PORTOLAN_GDAL_DEBUG"

PACKAGE_LOGGERS = ("portolan_cli", "portolan_core")
THIRD_PARTY_LOGGERS = ("urllib3", "httpx", "obstore")
# Rasterio sends GDAL messages to this logger. A CPL_DEBUG message arrives at DEBUG.
GDAL_LOGGER = "rasterio._env"

_ENV_LEVELS = {"debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING}


class _StderrHandler(logging.StreamHandler):  # type: ignore[type-arg]
    """A stream handler that looks up ``sys.stderr`` at each record.

    Click's test runner and pytest replace ``sys.stderr`` after setup.
    """

    @property
    def stream(self) -> TextIO:
        return sys.stderr

    @stream.setter
    def stream(self, value: TextIO) -> None:
        pass


class _RedactingFormatter(logging.Formatter):
    """A formatter whose output holds no URL credential."""

    def format(self, record: logging.LogRecord) -> str:
        return redact_credentials(self._format(record))

    def _format(self, record: logging.LogRecord) -> str:
        return super().format(record)


class _RelativeFormatter(_RedactingFormatter):
    """Format a record as ``+3.214s DEBUG portolan_cli.add: message``."""

    def _format(self, record: logging.LogRecord) -> str:
        line = f"+{record.relativeCreated / 1000:.3f}s {record.levelname} {record.name}: {record.getMessage()}"
        if record.exc_info:
            line = f"{line}\n{self.formatException(record.exc_info)}"
        return line


class _UserFormatter(_RelativeFormatter):
    """Format a ``portolan_cli`` or ``portolan_core`` record as an output line: ``⚠ message``."""

    def _format(self, record: logging.LogRecord) -> str:
        if record.name.split(".")[0] not in PACKAGE_LOGGERS:
            return super()._format(record)
        prefix = "✗" if record.levelno >= logging.ERROR else "⚠"
        return f"{prefix} {record.getMessage()}"


class _JsonFormatter(logging.Formatter):
    """Format a record as one JSON object with the run ID."""

    def format(self, record: logging.LogRecord) -> str:
        doc = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "rel": round(record.relativeCreated / 1000, 3),
            "level": record.levelname,
            "logger": record.name,
            "msg": redact_credentials(record.getMessage()),
            "run_id": trace.run_id(),
        }
        if record.exc_info:
            doc["exc"] = redact_credentials(self.formatException(record.exc_info))
        return json.dumps(doc)


def _resolve_level(verbosity: int, quiet: bool) -> int:
    if quiet:
        return logging.ERROR
    if verbosity >= _DEBUG_VERBOSITY:
        return logging.DEBUG
    if verbosity == 1:
        return logging.INFO
    env = os.environ.get(LOG_ENV, "").strip().lower()
    return _ENV_LEVELS.get(env, logging.WARNING)


class _Saved:
    """Hold the root level that ``configure_logging`` replaced for ``--quiet``."""

    root_level: int | None = None


def reset_logging() -> None:
    """Remove the handlers and levels that ``configure_logging`` set."""
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, "_portolan", False):
            root.removeHandler(handler)
            handler.close()
    if _Saved.root_level is not None:
        root.setLevel(_Saved.root_level)
        _Saved.root_level = None
    for name in PACKAGE_LOGGERS:
        logging.getLogger(name).setLevel(logging.NOTSET)
    for name in (*THIRD_PARTY_LOGGERS, GDAL_LOGGER):
        logging.getLogger(name).setLevel(logging.NOTSET)
    logging.captureWarnings(False)


def configure_logging(*, verbosity: int = 0, quiet: bool = False) -> None:
    """Set the log level and the stderr handler for one run."""
    reset_logging()
    level = _resolve_level(verbosity, quiet)
    gdal_debug = os.environ.get(GDAL_DEBUG_ENV, "").strip() in ("1", "true", "on", "yes")
    root = logging.getLogger()

    # The handler level keeps records below it off stderr even if another handler lowers the root level.
    stderr_handler = _StderrHandler()
    stderr_handler.setLevel(logging.DEBUG if gdal_debug else level)
    stderr_handler.setFormatter(
        _UserFormatter() if level >= logging.WARNING else _RelativeFormatter()
    )
    stderr_handler._portolan = True  # type: ignore[attr-defined]
    root.addHandler(stderr_handler)

    log_file = os.environ.get(LOG_FILE_ENV)
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(_JsonFormatter())
        file_handler._portolan = True  # type: ignore[attr-defined]
        root.addHandler(file_handler)

    if level != logging.WARNING:
        for name in PACKAGE_LOGGERS:
            logging.getLogger(name).setLevel(level)
    if quiet:
        _Saved.root_level = root.level
        root.setLevel(logging.ERROR)

    if verbosity >= _DEBUG_VERBOSITY and not quiet:
        for name in THIRD_PARTY_LOGGERS:
            logging.getLogger(name).setLevel(logging.DEBUG)
        logging.captureWarnings(True)

    if gdal_debug:
        os.environ["CPL_DEBUG"] = "ON"
        logging.getLogger(GDAL_LOGGER).setLevel(logging.DEBUG)
