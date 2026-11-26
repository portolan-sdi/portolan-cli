"""The messages that push and upload report while they run.

Library code does not print (#1005). A push or an upload calls ``notice`` with
a level and a message. The caller of the run passes a ``Notify`` callback, and
``notifying`` makes that callback current for the run. The CLI prints each
message through ``portolan_cli.output``. Without a callback, a message goes to
the log.

The callback lives in a context variable, so the threads and tasks of one run
reach it without a parameter on each helper. ``trace.submit`` and
``asyncio.to_thread`` copy the context into each worker.
"""

from __future__ import annotations

import contextvars
import logging
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Literal

# "verbose" is per-file detail. The CLI prints it only with --verbose.
NoticeLevel = Literal["success", "info", "warn", "error", "detail", "verbose"]
Notify = Callable[[NoticeLevel, str], None]

logger = logging.getLogger(__name__)

_LOG_LEVELS: dict[NoticeLevel, int] = {
    "success": logging.INFO,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "error": logging.ERROR,
    "detail": logging.INFO,
    "verbose": logging.DEBUG,
}

_current: contextvars.ContextVar[Notify | None] = contextvars.ContextVar(
    "sync_notify", default=None
)

# Concurrent collection pushes share one callback. A group of lines holds the
# lock, so the lines of another collection cannot come between them.
_lock = threading.RLock()


@contextmanager
def notifying(notify: Notify | None) -> Iterator[None]:
    """Make ``notify`` the callback of the run. None keeps the current callback."""
    if notify is None:
        yield
        return
    token = _current.set(notify)
    try:
        yield
    finally:
        _current.reset(token)


@contextmanager
def notice_group() -> Iterator[None]:
    """Keep the notices sent inside the block together."""
    with _lock:
        yield


def notice(level: NoticeLevel, message: str) -> None:
    """Send ``message`` to the callback of the run, or to the log without one."""
    callback = _current.get()
    with _lock:
        if callback is None:
            logger.log(_LOG_LEVELS[level], message)
        else:
            callback(level, message)
