"""Run one ``portolan`` command in a child process and record its cost.
It installs the network guard, the DuckDB, Parquet and tile counters, and writes counters, spans, wall time and peak RSS.
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
    from collections.abc import Callable
    from types import FrameType

    import pytest

TILE_SIZE = 256


#: Modules that only relay a count. The site is the first frame outside them.
_RELAYS = ("portolan_core.trace", "portolan_core.json_io")


def _site(frame: FrameType | None) -> str:
    while frame is not None:
        module = str(frame.f_globals.get("__name__", ""))
        if module.startswith(("portolan_cli", "portolan_core")) and module not in _RELAYS:
            return f"{module}:{frame.f_code.co_name}"
        frame = frame.f_back
    return "outside portolan_cli"


def _install_site_recorder(sites: dict[str, Counter[str]]) -> None:
    """Record the calling function of each count, before any module imports ``count``."""
    from portolan_core import trace

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

    from portolan_core import trace

    connect = duckdb.connect

    def counted_connect(*args: Any, **kwargs: Any) -> Any:
        trace.count("duckdb.connects")
        return connect(*args, **kwargs)

    duckdb.connect = counted_connect


def _install_parquet_counter(patch: pytest.MonkeyPatch) -> None:
    """Count the PyArrow footer opens of geoparquet-io, which core counters cannot see.
    Keep the class intact: footer consumers use isinstance checks.
    """
    import pyarrow.parquet as pq

    from portolan_core import trace

    original: Callable[..., None] = pq.ParquetFile.__init__

    def counted_init(self: Any, *args: Any, **kwargs: Any) -> None:
        caller: FrameType | None = sys._getframe(1)
        # read_metadata/read_schema may reach ParquetFile through PyArrow's
        # Python helpers. Attribute the open to the library that requested it.
        while caller is not None and str(caller.f_globals.get("__name__", "")).startswith(
            "pyarrow."
        ):
            caller = caller.f_back
        if caller is not None and str(caller.f_globals.get("__name__", "")).startswith(
            "geoparquet_io."
        ):
            trace.count("parquet.opens")
        original(self, *args, **kwargs)

    patch.setattr(pq.ParquetFile, "__init__", counted_init)


def install_worker_counters() -> None:
    """Install delegated-reader counters in a freshly spawned conversion worker."""
    import pytest

    _install_duckdb_counter()
    # The worker owns these patches for its entire process lifetime.
    _install_parquet_counter(pytest.MonkeyPatch())


#: The colour of every stub basemap tile. No thumbnail preset uses it, so a
#: pixel of this colour in a thumbnail came from the basemap.
STUB_TILE_RGBA = (201, 74, 203, 255)


def install_stub_tiles(patch: pytest.MonkeyPatch) -> list[str]:
    """Serve every basemap tile as one fixed tile and record each tile URL.
    Only the HTTP request in ``thumbnail._get_tile`` is replaced.
    """
    import contextily.tile  # noqa: F401 - the gate for the thumbnail extra
    import numpy as np

    from portolan_cli.viz import thumbnail

    # The shape and dtype that contextily's own fetch returns.
    tile = np.empty((TILE_SIZE, TILE_SIZE, 4), dtype=np.uint8)
    tile[...] = STUB_TILE_RGBA
    urls: list[str] = []
    lock = threading.Lock()

    def stub_get_tile(tile_url: str) -> Any:
        with lock:
            urls.append(tile_url)
        return tile.copy()

    patch.setattr(thumbnail, "_get_tile", stub_get_tile)
    return urls


def _peak_rss_mb() -> float | None:
    # Linux keeps the ru_maxrss of the parent across fork and exec.
    # VmHWM belongs to this program alone.
    status = Path("/proc/self/status")
    if status.is_file():
        for line in status.read_text(encoding="utf-8").splitlines():
            if line.startswith("VmHWM:"):
                return round(int(line.split()[1]) / 1024, 1)
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
    os.environ["PORTOLAN_PERF_WORKER_INIT"] = (
        f"tests.perf.runner:{install_worker_counters.__name__}"
    )
    sites: dict[str, Counter[str]] = {}
    _install_site_recorder(sites)
    _install_duckdb_counter()

    from portolan_cli.cli import cli
    from portolan_core import trace

    exit_code: int | str = 0
    with contextlib.ExitStack() as stack:
        sentinel = stack.enter_context(contextlib.contextmanager(SentinelProxy().run)())
        guard = NetworkGuard(test=" ".join(argv), sentinel=sentinel)
        patch = stack.enter_context(pytest.MonkeyPatch.context())
        _install_parquet_counter(patch)
        with contextlib.suppress(ImportError):  # no thumbnail extra, so no basemap
            install_stub_tiles(patch)
        guard.install(patch)
        start = time.perf_counter()
        try:
            result = cli.main(args=argv, prog_name="portolan", standalone_mode=False)
            exit_code = result if isinstance(result, int) else 0
        except SystemExit as stop:
            exit_code = stop.code if isinstance(stop.code, int) else 1
        except Exception as error:  # the record names the error
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
    parser.add_argument(
        "--mem", action="store_true", help="Record the tracemalloc growth of each span."
    )
    parser.add_argument("command", nargs=argparse.REMAINDER, help="-- then the portolan args")
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    record = run(command, mem=args.mem)
    args.out.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
