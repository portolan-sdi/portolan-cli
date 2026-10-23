"""Logging setup for the ``portolan`` command (issue #991).

The root ``cli`` group calls ``configure_logging`` once for each run. Log
records go to stderr only. Stdout stays for the output helpers and the JSON
envelope.

Levels:

- default: WARNING
- ``-v``: INFO for ``portolan_cli``
- ``-vv``: DEBUG for ``portolan_cli``, ``urllib3``, ``httpx``, and ``obstore``,
  and Python warnings go through the ``py.warnings`` logger
- ``--quiet``: ERROR

At WARNING and ERROR a ``portolan_cli`` record reads as an output line, such
as ``⚠ message``. ``-v`` shows the full record format.

Without a flag, ``PORTOLAN_LOG`` (``debug``, ``info``, or ``warning``) sets the level.
``PORTOLAN_LOG_FILE=<path>`` also writes one JSON object for each record.
``PORTOLAN_GDAL_DEBUG=1`` sets ``CPL_DEBUG=ON`` for GDAL and shows the GDAL
debug messages that rasterio logs, at any verbosity.

This module uses no click, so library code can call it too.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import TextIO

from portolan_cli import trace

LOG_ENV = "PORTOLAN_LOG"
LOG_FILE_ENV = "PORTOLAN_LOG_FILE"
GDAL_DEBUG_ENV = "PORTOLAN_GDAL_DEBUG"

PACKAGE_LOGGER = "portolan_cli"
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


class _RelativeFormatter(logging.Formatter):
    """Format a record as ``+3.214s DEBUG portolan_cli.add: message``."""

    def format(self, record: logging.LogRecord) -> str:
        line = f"+{record.relativeCreated / 1000:.3f}s {record.levelname} {record.name}: {record.getMessage()}"
        if record.exc_info:
            line = f"{line}\n{self.formatException(record.exc_info)}"
        return line


class _UserFormatter(_RelativeFormatter):
    """Format a ``portolan_cli`` record as an output line: ``⚠ message``.

    At the default level a library warning is a message for the operator, so
    it reads like ``output.warn``. An error reads like ``output.error``. The
    record format and the traceback need ``-v``. Other loggers keep the
    record format.
    """

    def format(self, record: logging.LogRecord) -> str:
        if record.name != PACKAGE_LOGGER and not record.name.startswith(f"{PACKAGE_LOGGER}."):
            return super().format(record)
        prefix = "✗" if record.levelno >= logging.ERROR else "⚠"
        return f"{prefix} {record.getMessage()}"


class _JsonFormatter(logging.Formatter):
    """Format a record as one JSON object with the run ID."""

    def format(self, record: logging.LogRecord) -> str:
        doc = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "rel": round(record.relativeCreated / 1000, 3),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "run_id": trace.run_id(),
        }
        if record.exc_info:
            doc["exc"] = self.formatException(record.exc_info)
        return json.dumps(doc)


def _resolve_level(verbosity: int, quiet: bool) -> int:
    if quiet:
        return logging.ERROR
    if verbosity >= 2:
        return logging.DEBUG
    if verbosity == 1:
        return logging.INFO
    env = os.environ.get(LOG_ENV, "").strip().lower()
    return _ENV_LEVELS.get(env, logging.WARNING)


_saved_root_level: int | None = None


def reset_logging() -> None:
    """Remove the handlers and levels that ``configure_logging`` set."""
    global _saved_root_level
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, "_portolan", False):
            root.removeHandler(handler)
            handler.close()
    if _saved_root_level is not None:
        root.setLevel(_saved_root_level)
        _saved_root_level = None
    logging.getLogger(PACKAGE_LOGGER).setLevel(logging.NOTSET)
    for name in (*THIRD_PARTY_LOGGERS, GDAL_LOGGER):
        logging.getLogger(name).setLevel(logging.NOTSET)
    logging.captureWarnings(False)


def configure_logging(*, verbosity: int = 0, quiet: bool = False) -> None:
    """Set the log level and the stderr handler for one run.

    Args:
        verbosity: The count of ``-v`` flags.
        quiet: True for ``--quiet``. It wins over ``verbosity``.
    """
    global _saved_root_level
    reset_logging()
    level = _resolve_level(verbosity, quiet)
    gdal_debug = os.environ.get(GDAL_DEBUG_ENV, "").strip() in ("1", "true", "on", "yes")
    root = logging.getLogger()

    # The handler level keeps records below the chosen level off stderr,
    # also when another handler (pytest's caplog) lowers the root level.
    # With GDAL debug on, the handler passes DEBUG records. The logger levels
    # below still keep the DEBUG records of other loggers off stderr.
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

    # Records from portolan_cli reach the root handlers at any root level, so
    # the root level changes only for --quiet. Other libraries stay at
    # WARNING, as with Python's fallback handler.
    if level != logging.WARNING:
        logging.getLogger(PACKAGE_LOGGER).setLevel(level)
    if quiet:
        _saved_root_level = root.level
        root.setLevel(logging.ERROR)

    if verbosity >= 2 and not quiet:
        for name in THIRD_PARTY_LOGGERS:
            logging.getLogger(name).setLevel(logging.DEBUG)
        logging.captureWarnings(True)

    if gdal_debug:
        os.environ["CPL_DEBUG"] = "ON"
        logging.getLogger(GDAL_LOGGER).setLevel(logging.DEBUG)
