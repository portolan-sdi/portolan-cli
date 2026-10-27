"""Measure a scenario at two sizes and compare the counters with the recorded constants.

The scale gate runs each scenario at size ``n`` and ``2n`` and derives, for
each counter ``c``::

    slope     = c(2n) - c(n)
    intercept = 2 * c(n) - c(2n)

For a cost that grows linearly, ``slope`` is the work per unit of size and
``intercept`` is the fixed work per run. A per-file re-read raises the slope
at any size, so a small scenario catches it. The pairs are exact integers,
because a counter does not depend on machine load.

Each counter has one owner file in ``tests/perf/expected/``. A lane that
removes work lowers only the constants in its own file, so the diff of that
file records each improvement. A pair that no file holds is ``(0, 0)``. The
comment on each pair names the functions that made the counts at size 2n,
with their number of calls.

``checksum.bytes`` has no gate. It measures data volume, and the bytes of a
converted file change with the geoparquet-io version.

Print the measured pairs with the code that produced each count, or write
them over the owner files (then commit only the file of your lane)::

    uv run python -m tests.perf.gates --record [--write] [scenario ...]
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from tests.network_guard import install_duckdb_extensions
from tests.perf.expected import add, config, parquet, versions, viz
from tests.perf.scenarios import HISTORY_VERSIONS, SCENARIOS, Scenario, run_portolan

#: The owner file of each gated counter, by the lane that removes its work.
COUNTER_OWNER: dict[str, str] = {
    "versions_json.reads": "versions",
    "versions_json.writes": "versions",
    "versions.published": "versions",
    "checksum.files": "versions",
    "items_parquet.writes": "versions",
    "config.yaml_parses": "config",
    "config.merges": "config",
    "collection_json.writes": "add",
    "parquet.opens": "parquet",
    "raster.opens": "parquet",
    "vector.opens": "parquet",
    "duckdb.connects": "parquet",
    "thumbnail.draws": "viz",
    "tiles.fetched": "viz",
}

OWNER_FILES: dict[str, dict[tuple[str, str], tuple[int, int]]] = {
    "versions": versions.EXPECTED,
    "config": config.EXPECTED,
    "add": add.EXPECTED,
    "parquet": parquet.EXPECTED,
    "viz": viz.EXPECTED,
}

OWNER_DOCS: dict[str, str] = {
    "versions": "versions.json reads and writes, published versions, checksums, items.parquet",
    "config": "YAML parses and merged-config builds",
    "add": "collection.json writes",
    "parquet": "Parquet, raster, and vector opens, DuckDB connections",
    "viz": "thumbnail draws and basemap tiles",
}

EXPECTED_DIR = Path(__file__).parent / "expected"

#: The size ``n`` of each scale gate. The gate also runs ``2n``.
SCALE_SIZES: dict[str, int] = {
    "add-many-collections": 3,
    "add-many-collections-thumbs": 2,
    "add-one-collection": 3,
    "add-with-companions": 3,
    "add-large-vector": 1_000,
    "re-add-unchanged": 3,
    "check-metadata": 3,
    "check-fix-missing-rasters": 3,
    "check-fix-geo-assets": 3,
    "status": 3,
    "add-rasters": 3,
}


def measure(
    scenario: Scenario,
    workdir: Path,
    n: int,
    *,
    workers: int = 1,
    mem: bool = False,
    history: int = HISTORY_VERSIONS,
) -> dict[str, Any]:
    """Build ``scenario`` at size ``n`` under ``workdir``, run it, and return the record."""
    label = f"{scenario.name}-{n}-w{workers}"
    root = workdir / label
    scenario.build(root, n, history=history)
    command = list(scenario.command)
    if workers != 1:
        command[command.index("--workers") + 1] = str(workers)
    return run_portolan(root, command, workdir / f"{label}.json", mem=mem)


def scale(small: dict[str, Any], large: dict[str, Any]) -> dict[str, tuple[int, int]]:
    """Return the ``(slope, intercept)`` of each gated counter from runs at n and 2n."""
    pairs = {}
    for name in COUNTER_OWNER:
        c1, c2 = small["counters"][name], large["counters"][name]
        pairs[name] = (c2 - c1, 2 * c1 - c2)
    return pairs


def expected(scenario: str) -> dict[str, tuple[int, int]]:
    """Return the recorded ``(slope, intercept)`` of each gated counter of ``scenario``."""
    return {
        name: OWNER_FILES[owner].get((scenario, name), (0, 0))
        for name, owner in COUNTER_OWNER.items()
    }


Row = tuple[str, str, str, tuple[int, int], str]


def _record(name: str, workdir: Path) -> tuple[list[str], list[Row]]:
    scenario = SCENARIOS[name]
    n = SCALE_SIZES[name]
    small = measure(scenario, workdir, n)
    large = measure(scenario, workdir, 2 * n)
    for record in (small, large):
        if record["exit_code"] != scenario.exit_code or record["network"]:
            raise RuntimeError(
                f"{name} failed: exit {record['exit_code']}, network {record['network']}"
            )
    notes = [f"# {name}: exit {small['exit_code']}/{large['exit_code']}"]
    rows: list[Row] = []
    for counter, pair in scale(small, large).items():
        if pair == (0, 0):
            continue
        sites = ", ".join(
            f"{site.removeprefix('portolan_cli.')} x{calls}"
            for site, calls in list(large["sites"].get(counter, {}).items())[:2]
        )
        rows.append((COUNTER_OWNER[counter], name, counter, pair, sites or "no site recorded"))
    return notes, rows


def _line(row: Row) -> str:
    _, name, counter, pair, sites = row
    return f'    ("{name}", "{counter}"): {pair},  # {sites}\n'


_FMT_ON = "# fmt: on\n"


def render(owner: str, lines: list[str], trailer: str = "") -> str:
    """Return the text of ``expected/<owner>.py``.

    ``lines`` are the entries. ``trailer`` is the text after the table, such
    as a memory ceiling, which this tool does not measure.
    """
    body = "".join(sorted(lines))
    return (
        f'"""Scale-gate constants of {owner}: {OWNER_DOCS[owner]}. See gates.py."""\n\n'
        "from __future__ import annotations\n\n"
        "# fmt: off\n"
        "EXPECTED: dict[tuple[str, str], tuple[int, int]] = {\n"
        f"{body}"
        "}\n"
        f"{_FMT_ON}{trailer}"
    )


def main(argv: list[str] | None = None) -> int:
    """Print the measured constants of each scenario, or write the owner files."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--record", action="store_true", required=True)
    parser.add_argument("--write", action="store_true", help="Rewrite tests/perf/expected/.")
    parser.add_argument("scenarios", nargs="*", default=sorted(SCALE_SIZES))
    args = parser.parse_args(argv)
    install_duckdb_extensions()
    rows: list[Row] = []
    with tempfile.TemporaryDirectory(prefix="perf-record-") as tmp, ThreadPoolExecutor(4) as pool:
        for notes, scenario_rows in pool.map(lambda name: _record(name, Path(tmp)), args.scenarios):
            print("\n".join(notes), flush=True)
            rows.extend(scenario_rows)
    for owner in OWNER_FILES:
        path = EXPECTED_DIR / f"{owner}.py"
        table, _, trailer = path.read_text(encoding="utf-8").partition(_FMT_ON)
        # Keep the entries of the scenarios that this run did not measure.
        kept = [
            line + "\n"
            for line in table.splitlines()
            if line.startswith("    (") and line.split('"')[1] not in args.scenarios
        ]
        text = render(owner, kept + [_line(row) for row in rows if row[0] == owner], trailer)
        if args.write:
            path.write_text(text, encoding="utf-8")
        else:
            print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
