"""The callback that library code calls in place of printing (#1133).

Each line a user sees goes to a ``Report`` callable with its level. The CLI
prints the line through ``portolan_cli.output``. ``Level`` and ``Report`` are
the types of ``portolan_cli/sync/report.py`` on the pull branch.

A format handler, a ``viz`` function, and the finalization helpers take no
callback, so :func:`reporting` holds one for a block, as ``formats.in_run``
holds the run state. ``trace.submit`` copies the context, so a worker thread
reports to the same callback. With no callback, :func:`emit` logs the line, so
a library caller writes nothing to the terminal through this module.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Callable, Iterator
from contextvars import ContextVar
from typing import Literal

Level = Literal["success", "info", "warn", "error", "detail"]
Report = Callable[[Level, str], None]

logger = logging.getLogger(__name__)

_LOG_LEVELS: dict[Level, int] = {
    "success": logging.INFO,
    "info": logging.INFO,
    "detail": logging.DEBUG,
    "warn": logging.WARNING,
    "error": logging.ERROR,
}

_REPORT: ContextVar[Report | None] = ContextVar("portolan_report", default=None)


@contextlib.contextmanager
def reporting(report: Report | None) -> Iterator[None]:
    """Send each :func:`emit` in the block to ``report``. None keeps the current one."""
    if report is None:
        yield
        return
    token = _REPORT.set(report)
    try:
        yield
    finally:
        _REPORT.reset(token)


def emit(level: Level, message: str) -> None:
    """Hand one line for the user to the current callback, or log it without one."""
    report = _REPORT.get()
    if report is not None:
        report(level, message)
    else:
        logger.log(_LOG_LEVELS[level], message)
