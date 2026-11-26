"""The progress callback that the sync library takes in place of printing.

pull, sync, and clone pass each line a user sees to a ``Report`` callable with
its level. The CLI prints the line through ``portolan_cli.output``. A library
caller that passes no callback gets no output, so its stdout stays clean.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Literal

Level = Literal["success", "info", "warn", "error", "detail"]
Report = Callable[[Level, str], None]


def silent(level: Level, message: str) -> None:
    """Drop one progress line. This is the default for a library caller."""
