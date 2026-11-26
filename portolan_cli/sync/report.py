"""The progress callback that the sync library takes in place of printing.

pull, sync, clone, push, and the transfer modules pass each line a user sees
to a ``Report`` callable with its level. The CLI prints the line through
``portolan_cli.output``. A library caller that passes no callback gets no
output, so its stdout stays clean.

A function that takes ``report: Report | None`` sends its lines through
``tell``. Without a callback, ``tell`` writes the line to the log.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Literal

Level = Literal["success", "info", "warn", "error", "detail"]
Report = Callable[[Level, str], None]

logger = logging.getLogger(__name__)


def silent(level: Level, message: str) -> None:
    """Drop one progress line. This is the default for a library caller."""


def tell(report: Report | None, level: Level, message: str) -> None:
    """Send one line to ``report``, or to the log without one."""
    if report is not None:
        report(level, message)
    else:
        logger.log(logging.WARNING if level in ("warn", "error") else logging.DEBUG, message)
