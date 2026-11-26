"""The callback that library code calls in place of printing (#1133).

Each line a user sees goes to a ``MessageCallback`` with its level, the
callback type of the extract path (``extract.common.progress``). The CLI prints
the line through ``portolan_cli.output``.

A format handler, a ``viz`` function, and the finalization helpers take no
callback, so :func:`reporting` holds one for a block, as ``formats.in_run``
holds the run state. ``trace.submit`` copies the context, so a worker thread
reports to the same callback. With no callback, :func:`emit` logs the line
through ``emit_message``, so a library caller writes nothing to the terminal.
"""

from __future__ import annotations

import contextlib
from contextvars import ContextVar
from typing import TYPE_CHECKING

from portolan_cli.extract.common.progress import MessageCallback, emit_message

if TYPE_CHECKING:
    from collections.abc import Iterator

_REPORT: ContextVar[MessageCallback | None] = ContextVar("portolan_report", default=None)


@contextlib.contextmanager
def reporting(on_message: MessageCallback | None) -> Iterator[None]:
    """Send each :func:`emit` in the block to ``on_message``. None keeps the current one."""
    if on_message is None:
        yield
        return
    token = _REPORT.set(on_message)
    try:
        yield
    finally:
        _REPORT.reset(token)


@contextlib.contextmanager
def recording() -> Iterator[list[tuple[str, str]]]:
    """Collect each :func:`emit` in the block as ``(level, text)``, for a worker process."""
    lines: list[tuple[str, str]] = []
    with reporting(lambda level, text: lines.append((level, text))):
        yield lines


def emit(level: str, text: str) -> None:
    """Hand one line for the user to the current callback, or log it without one.

    The level is ``info``, ``detail``, ``success``, ``warn``, or ``error``.
    """
    emit_message(_REPORT.get(), level, text)
