"""Run one ``portolan`` command in a child process and record its cost (#992).

The scale gates, the nightly scenario job, and ``scripts/profile.py`` all run
``portolan`` through this module, so each one counts the same work::

    python -m tests.perf.runner --out result.json [--mem] -- add . --workers 1

Before the command runs, the module installs three things:

- The network guard of ``tests/network_guard.py``. A scenario that reaches a
  remote host records it under ``network`` in the result, and the gates fail.
- A counting wrapper around ``duckdb.connect``. geoparquet-io calls the
  function by attribute lookup, so the wrapper sees each connection. The
  counter is ``duckdb.connects``. A query on DuckDB's module-level default
  connection opens no connection and does not count.
- A stub tile source for contextily. It returns one fixed 256 px tile and
  counts ``tiles.fetched``. contextily caches tiles per process, so the count
  is the number of distinct tiles that the run asks for.

The command runs in this process with tracing on. The result file holds the
exit code, the counters, the span tree, the wall time, and the peak RSS. It
also holds ``sites``: for each counter, the number of ``count`` calls from
each ``portolan_cli`` function. ``python -m tests.perf.gates --record`` prints
them as the comments of the expected constants. The child process starts cold,
so no cache of an earlier run changes a count.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
import threading
import time
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from types import FrameType

    import pytest

TILE_SIZE = 256


#: Modules that only relay a count. The site is the first frame outside them.
_RELAYS = ("portolan_cli.trace", "portolan_cli.json_io")


def _site(frame: FrameType | None) -> str:
    while frame is not None:
        module = str(frame.f_globals.get("__name__", ""))
        if module.startswith("portolan_cli") and module not in _RELAYS:
            return f"{module}:{frame.f_code.co_name}"
        frame = frame.f_back
    return "outside portolan_cli"


def _install_site_recorder(sites: dict[str, Counter[str]]) -> None:
    """Record the calling function of each count.

    It runs before any ``portolan_cli`` module imports ``count`` by name, so
    every module binds the wrapper.
    """
    from portolan_cli import trace

    count = trace.count
    lock = threading.Lock()

    def recording_count(name: str, n: int = 1) -> None:
        if trace.is_enabled():
            site = _site(sys._getframe(1))
            with lock:
                sites.setdefault(name, Counter())[site] += 1
        count(name, n)

    trace.count = recording_count


def _install_duckdb_counter() -> None:
    import duckdb

    from portolan_cli import trace

    connect = duckdb.connect

    def counted_connect(*args: Any, **kwargs: Any) -> Any:
        trace.count("duckdb.connects")
        return connect(*args, **kwargs)

    duckdb.connect = counted_connect


def _install_stub_tiles(patch: pytest.MonkeyPatch) -> None:
    try:
        import contextily.tile
        import numpy as np
    except ImportError:
        return  # no thumbnail extra, so no basemap is drawn

    from portolan_cli import trace

    # A fixed light grey RGBA tile, the shape contextily's own fetch returns.
    tile = np.full((TILE_SIZE, TILE_SIZE, 4), 235, dtype=np.uint8)
    tile[..., 3] = 255

    def stub_fetch_tile(*_args: Any, **_kwargs: Any) -> Any:
        trace.count("tiles.fetched")
        return tile.copy()

    patch.setattr(contextily.tile, "_fetch_tile", stub_fetch_tile)


def _peak_rss_mb() -> float | None:
    try:
        import resource
    except ImportError:  # Windows has no resource module
        return None
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(peak / (1024 * 1024 if sys.platform == "darwin" else 1024), 1)


def run(argv: list[str], *, mem: bool) -> dict[str, Any]:
    """Run ``portolan <argv>`` in this process and return its cost record."""
    import pytest

    from tests.network_guard import NetworkGuard, SentinelProxy

    os.environ["PORTOLAN_TIMINGS"] = "mem" if mem else "1"
    sites: dict[str, Counter[str]] = {}
    _install_site_recorder(sites)
    _install_duckdb_counter()

    from portolan_cli import trace
    from portolan_cli.cli import cli

    exit_code: int | str = 0
    with contextlib.ExitStack() as stack:
        sentinel = stack.enter_context(contextlib.contextmanager(SentinelProxy().run)())
        guard = NetworkGuard(test=" ".join(argv), sentinel=sentinel)
        patch = stack.enter_context(pytest.MonkeyPatch.context())
        _install_stub_tiles(patch)
        guard.install(patch)
        start = time.perf_counter()
        try:
            result = cli.main(args=argv, prog_name="portolan", standalone_mode=False)
            exit_code = result if isinstance(result, int) else 0
        except SystemExit as stop:
            exit_code = stop.code if isinstance(stop.code, int) else 1
        except Exception as error:  # noqa: BLE001 - the record names the error
            exit_code = f"{type(error).__name__}: {error}"
        wall_s = time.perf_counter() - start
        network = guard.collect()
    meta = trace.meta() or {}
    return {
        "argv": argv,
        "exit_code": exit_code,
        "wall_s": round(wall_s, 3),
        "peak_rss_mb": _peak_rss_mb(),
        "counters": meta.get("counters", {}),
        "timings": meta.get("timings", []),
        "network": network,
        "sites": {name: dict(calls.most_common()) for name, calls in sorted(sites.items())},
    }


def main() -> int:
    """Parse the arguments, run the command, and write the record."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", type=Path, required=True, help="Where to write the record.")
    parser.add_argument("--mem", action="store_true", help="Record tracemalloc peaks.")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="-- then the portolan args")
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    record = run(command, mem=args.mem)
    args.out.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
