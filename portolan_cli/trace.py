"""Stage spans and counters for one ``portolan`` run (issue #991).

Tracing is off by default. When it is off, ``span`` returns a shared no-op
object after one flag check, and ``count`` returns at once.

Usage::

    from portolan_cli.trace import count, span

    with span("add.prepare", files=len(batch)) as s:
        ...
        s.set(bytes=total)
    count("checksum.bytes", n)

Spans nest through a ``ContextVar``. Asyncio tasks inherit the current span.
A thread pool worker does not, so submit work through ``submit``.

``PORTOLAN_TIMINGS=1`` or ``--timings`` turns tracing on.
``PORTOLAN_TIMINGS=mem`` also records the ``tracemalloc`` growth of each span: its
peak traced memory less the traced memory at span entry. Memory that an
earlier stage holds, such as a module imported before the span, does not count.
``PORTOLAN_TRACE_FILE=<path>`` turns tracing on and names the Chrome trace file.

This module imports only the standard library, so any layer may import it.
``json_io`` imports it to count the JSON writes.
"""

from __future__ import annotations

import contextlib
import contextvars
import os
import sys
import threading
import time
import tracemalloc
import uuid
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from concurrent.futures import Executor, Future
    from pathlib import Path
    from types import ModuleType, TracebackType

    from typing_extensions import Self

resource: ModuleType | None
try:
    import resource
except ImportError:  # Windows has no resource module.
    resource = None

P = ParamSpec("P")
R = TypeVar("R")

TIMINGS_ENV = "PORTOLAN_TIMINGS"
TRACE_FILE_ENV = "PORTOLAN_TRACE_FILE"

# Counter names that ``meta.counters`` always holds, at 0 when nothing counted.
# Each one counts at the single choke point of its work, so the scale gates in
# tests/perf see every call site. One has no product choke point.
# ``duckdb.connects`` counts through a patch of ``duckdb.connect`` that
# tests/perf/runner.py installs, because geoparquet-io calls it by attribute
# lookup.
KNOWN_COUNTERS: tuple[str, ...] = (
    "checksum.files",
    "checksum.bytes",
    "versions_json.reads",
    "versions_json.writes",
    "collection_json.writes",
    "items_parquet.writes",
    "config.yaml_parses",
    "config.merges",
    "versions.published",
    "thumbnail.draws",
    "tiles.fetched",
    "raster.opens",
    "parquet.opens",
    "duckdb.connects",
)

_MB = 1024 * 1024

_enabled = False
_mem = False
_lock = threading.Lock()
_roots: list[_Span] = []
_counters: dict[str, int] = {}
_current: contextvars.ContextVar[_Span | None] = contextvars.ContextVar(
    "portolan_trace_span", default=None
)
_run_id = uuid.uuid4().hex
_t0 = time.perf_counter()


def _rss_peak_mb() -> float | None:
    """Return the process RSS high-water mark in MB, or None on Windows."""
    if resource is None:
        return None
    peak: int = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux reports kilobytes. macOS reports bytes.
    divisor = _MB if sys.platform == "darwin" else 1024
    return round(peak / divisor, 1)


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    return str(value)


class _NullSpan:
    """The span that ``span`` returns when tracing is off."""

    __slots__ = ()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        return None

    def set(self, **attrs: Any) -> None:
        """Do nothing. Tracing is off."""


_NULL_SPAN = _NullSpan()


class _Span:
    """One timed stage. It records wall time, CPU time, and memory."""

    def __init__(self, name: str, attrs: dict[str, Any]) -> None:
        self.name = name
        self.attrs = attrs
        self.children: list[_Span] = []
        self.tid = 0
        self.thread_name = ""
        self.start = 0.0
        self.end: float | None = None
        self.cpu_start = 0.0
        self.cpu_end: float | None = None
        self.rss_peak_mb: float | None = None
        self.mem_peak = 0
        self.mem_base = 0
        self._parent: _Span | None = None
        self._token: contextvars.Token[_Span | None] | None = None

    def set(self, **attrs: Any) -> None:
        """Add attributes to the span, for values known after the work runs."""
        self.attrs.update(attrs)

    def __enter__(self) -> Self:
        thread = threading.current_thread()
        # The OS thread ID. Perfetto reads tid as a 32-bit value, and
        # threading.get_ident() returns a pointer that does not fit.
        self.tid = threading.get_native_id()
        self.thread_name = thread.name
        self._parent = _current.get()
        with _lock:
            if self._parent is None:
                _roots.append(self)
            else:
                self._parent.children.append(self)
        if _mem and tracemalloc.is_tracing():
            _propagate_mem_peak(self._parent)
            tracemalloc.reset_peak()
            self.mem_base = tracemalloc.get_traced_memory()[0]
        self._token = _current.set(self)
        self.cpu_start = time.process_time()
        self.start = time.perf_counter()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.end = time.perf_counter()
        # process_time() covers every thread in the process, not only this span's.
        self.cpu_end = time.process_time()
        self.rss_peak_mb = _rss_peak_mb()
        if _mem and tracemalloc.is_tracing():
            _propagate_mem_peak(self)
            if self._parent is not None:
                self._parent.mem_peak = max(self._parent.mem_peak, self.mem_peak)
        if exc_type is not None:
            self.attrs.setdefault("error", exc_type.__name__)
        if self._token is not None:
            try:
                _current.reset(self._token)
            except ValueError:
                # The span closed in a different context than it opened in.
                _current.set(self._parent)

    def to_dict(self, now: float) -> dict[str, Any]:
        end = self.end if self.end is not None else now
        cpu_end = self.cpu_end if self.cpu_end is not None else time.process_time()
        node: dict[str, Any] = {
            "name": self.name,
            "start_s": round(self.start - _t0, 6),
            "wall_s": round(end - self.start, 6),
            "cpu_s": round(cpu_end - self.cpu_start, 6),
            "rss_peak_mb": self.rss_peak_mb if self.end is not None else _rss_peak_mb(),
            "tid": self.tid,
            "attrs": _json_safe(self.attrs),
            "children": [c.to_dict(now) for c in list(self.children)],
        }
        if _mem:
            node["mem_growth_mb"] = round(max(self.mem_peak - self.mem_base, 0) / _MB, 3)
        if self.end is None:
            node["open"] = True
        return node


def _propagate_mem_peak(target: _Span | None) -> None:
    """Fold the tracemalloc peak since the last reset into ``target``."""
    if target is None:
        return
    _, peak = tracemalloc.get_traced_memory()
    target.mem_peak = max(target.mem_peak, peak)


def enable(*, mem: bool = False) -> None:
    """Turn tracing on. With ``mem``, also start ``tracemalloc``."""
    global _enabled, _mem, _t0
    with _lock:
        _enabled = True
        _mem = mem
        _t0 = time.perf_counter()
        for name in KNOWN_COUNTERS:
            _counters.setdefault(name, 0)
    if mem and not tracemalloc.is_tracing():
        tracemalloc.start()


def enable_from_env(*, timings_flag: bool = False) -> None:
    """Turn tracing on from ``--timings``, ``PORTOLAN_TIMINGS``, or ``PORTOLAN_TRACE_FILE``."""
    value = os.environ.get(TIMINGS_ENV, "").strip().lower()
    mem = value == "mem"
    env_on = value not in ("", "0", "false", "no", "off")
    if timings_flag or env_on or os.environ.get(TRACE_FILE_ENV):
        enable(mem=mem)


def reset() -> None:
    """Turn tracing off and drop every span and counter. Tests use it."""
    global _enabled, _mem, _run_id
    with _lock:
        _enabled = False
        _mem = False
        _roots.clear()
        _counters.clear()
        _run_id = uuid.uuid4().hex
    _current.set(None)
    if tracemalloc.is_tracing():
        tracemalloc.stop()


@contextlib.contextmanager
def tracing(*, mem: bool = False) -> Iterator[None]:
    """Turn tracing on for one block, then reset it. Tests and the perf harness use it."""
    enable(mem=mem)
    try:
        yield
    finally:
        reset()


def is_enabled() -> bool:
    """Return True when tracing is on."""
    return _enabled


def mem_enabled() -> bool:
    """Return True when tracing also records the tracemalloc peak."""
    return _mem


def run_id() -> str:
    """Return the ID of this run. Log records and ``meta`` carry it."""
    return _run_id


def span(name: str, **attrs: Any) -> _Span | _NullSpan:
    """Return a context manager that times one stage.

    Args:
        name: The stage name, as ``<command>.<stage>``.
        **attrs: Values to record with the span.
    """
    if not _enabled:
        return _NULL_SPAN
    return _Span(name, attrs)


def current_span() -> _Span | _NullSpan:
    """Return the open span of this context, or the no-op span."""
    if not _enabled:
        return _NULL_SPAN
    return _current.get() or _NULL_SPAN


def count(name: str, n: int = 1) -> None:
    """Add ``n`` to the process-wide counter ``name``."""
    if not _enabled:
        return
    with _lock:
        _counters[name] = _counters.get(name, 0) + n


def counters() -> dict[str, int]:
    """Return a copy of the counters."""
    with _lock:
        return dict(_counters)


def submit(
    executor: Executor, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs
) -> Future[R]:
    """Submit ``fn`` to ``executor`` in a copy of the current context.

    The worker then sees the open span as its parent. Each call copies the
    context, because one ``Context`` cannot run in two threads at once.
    """
    ctx = contextvars.copy_context()

    def run() -> R:
        return ctx.run(fn, *args, **kwargs)

    return executor.submit(run)


def _package_version() -> str:
    try:
        return version("portolan-cli")
    except PackageNotFoundError:
        return "unknown"


def meta() -> dict[str, Any] | None:
    """Return the ``meta`` block for the JSON envelope, or None when tracing is off."""
    if not _enabled:
        return None
    now = time.perf_counter()
    with _lock:
        roots = list(_roots)
    return {
        "timings": [root.to_dict(now) for root in roots],
        "counters": counters(),
        "peak_rss_mb": _rss_peak_mb(),
        "version": _package_version(),
        "run_id": _run_id,
    }


def _walk(spans: list[_Span]) -> list[_Span]:
    out: list[_Span] = []
    for s in spans:
        out.append(s)
        out.extend(_walk(list(s.children)))
    return out


def chrome_trace_events() -> list[dict[str, Any]]:
    """Return every span as a Chrome trace complete event (``"ph": "X"``).

    The list also holds one ``thread_name`` metadata event for each thread.
    """
    if not _enabled:
        return []
    now = time.perf_counter()
    pid = os.getpid()
    with _lock:
        spans = _walk(list(_roots))
    events: list[dict[str, Any]] = []
    threads: dict[int, str] = {}
    for s in spans:
        end = s.end if s.end is not None else now
        threads.setdefault(s.tid, s.thread_name)
        events.append(
            {
                "name": s.name,
                "ph": "X",
                "ts": round((s.start - _t0) * 1e6, 3),
                "dur": round((end - s.start) * 1e6, 3),
                "pid": pid,
                "tid": s.tid,
                "args": _json_safe(s.attrs),
            }
        )
    for tid, thread_name in threads.items():
        events.append(
            {
                "name": "thread_name",
                "ph": "M",
                "pid": pid,
                "tid": tid,
                "args": {"name": thread_name},
            }
        )
    return events


def write_chrome_trace(path: Path) -> None:
    """Write the spans to ``path`` as a Chrome trace file. Perfetto opens it."""
    from portolan_cli.json_io import write_json_atomic  # json_io imports this module

    write_json_atomic(path, {"traceEvents": chrome_trace_events(), "displayTimeUnit": "ms"})


def format_table() -> str:
    """Return a text table of the stages directly under each root span."""
    if not _enabled:
        return ""
    now = time.perf_counter()
    with _lock:
        roots = list(_roots)
    rows: list[tuple[str, str, str, str]] = []
    for root in roots:
        for s in [root, *root.children]:
            node = s.to_dict(now)
            rss = node["rss_peak_mb"]
            name = node["name"] if s is root else f"  {node['name']}"
            rows.append(
                (
                    name,
                    f"{node['wall_s']:.3f}",
                    f"{node['cpu_s']:.3f}",
                    "-" if rss is None else f"{rss:.1f}",
                )
            )
    header = ("span", "wall s", "cpu s", "rss MB")
    widths = [max(len(r[i]) for r in [header, *rows]) for i in range(4)]
    lines = [
        f"{r[0]:<{widths[0]}}  {r[1]:>{widths[1]}}  {r[2]:>{widths[2]}}  {r[3]:>{widths[3]}}"
        for r in [header, *rows]
    ]
    return "\n".join(lines)
