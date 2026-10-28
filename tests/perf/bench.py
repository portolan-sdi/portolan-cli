"""Nightly scenario job: wall time and peak RSS against a committed baseline (#992).

The job runs five scenarios at full size, each in a cold child process
through ``runner.py``. It records the wall time of the command, the peak RSS
of the process, the counters, and the stage times::

    uv run python -m tests.perf.bench --out scenario-results.json \\
        --compare .benchmarks/scenarios.json

A scenario fails the run when its wall time doubles or its peak RSS grows by
more than 15% over the baseline. The wall limit scales by the runner speed
that ``calibrate`` measures. ``WALL_THRESHOLD`` gives the measured variance. A missing baseline, a baseline from another
machine type, or a baseline at other sizes also fails the run, so the gate
cannot pass without a comparison.

Without ``--compare`` the job only records. A maintainer commits the
``scenario-results.json`` of a ``record-baseline`` run as
``.benchmarks/scenarios.json``. The baseline comes from the CI runner type and
never from a dev machine. ``--scale`` shrinks every size, for a local check of
the job itself.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from tests.network_guard import install_duckdb_extensions
from tests.perf.gates import measure
from tests.perf.scenarios import REPO_ROOT, SCENARIOS

#: Full sizes. The re-add scenario also carries 50 history versions.
FULL_SIZES: dict[str, int] = {
    "add-many-collections": 100,  # S1: 100 collections, one file each
    "add-large-vector": 1_000_000,  # S2: one file of 1M polygons
    "re-add-unchanged": 5_000,  # S3: 5,000 raster items, 50 versions, no-op add
    "check-metadata": 25_000,  # S4: check --metadata on 25,000 raster items
    "add-rasters": 3_984,  # S8: the GHSL item count of #775
}
FULL_HISTORY = 50
#: Shared runners of one type differ. After the calibration scale, the same code
#: still ran up to 1.33x slower (2026-10-28 nightlies). add-rasters alone moved
#: 37% between two fast runners. So the wall limit catches a doubling, such as
#: the quadratic check of ca668f9 (35x), and the counter gates and CodSpeed
#: catch the smaller costs. Peak RSS held within 1% on every runner.
WALL_THRESHOLD = 1.00
RSS_THRESHOLD = 0.15


def _commit() -> str:
    proc = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True, check=False
    )
    return proc.stdout.strip() or "unknown"


def run_all(scale: float, workdir: Path) -> dict[str, Any]:
    """Run every scenario at its full size times ``scale`` and return the results."""
    results: dict[str, Any] = {
        "machine": platform.machine(),
        "python": platform.python_version(),
        "commit": _commit(),
        "calibration_s": calibrate(),
        "scenarios": {},
    }
    for name, full in FULL_SIZES.items():
        n = max(2, round(full * scale))
        record = measure(SCENARIOS[name], workdir, n, history=FULL_HISTORY)
        if record["exit_code"] != SCENARIOS[name].exit_code or record["network"]:
            raise SystemExit(f"{name} failed: {record['exit_code']} {record['network']}")
        stages = [(child["name"], child["wall_s"]) for child in record["timings"][0]["children"]]
        results["scenarios"][name] = {
            "n": n,
            "wall_s": record["wall_s"],
            "peak_rss_mb": record["peak_rss_mb"],
            "counters": record["counters"],
            "stages": stages,
        }
        print(f"{name} n={n}: {record['wall_s']} s, {record['peak_rss_mb']} MB", flush=True)
    return results


def calibrate(rounds: int = 3) -> float:
    """Time a fixed workload of Python, hashing, and file I/O. Return the fastest round.

    Two runners of one type differ in speed. The nightly of 2026-10-28 ran the
    baseline code 1.5x to 2x slower than the recording run. The compare scales
    the baseline wall time by the ratio of the two calibrations, so it
    measures the code and not the runner.
    """
    record = {f"key{i}": [i, str(i), {"value": float(i)}] for i in range(100_000)}
    block = bytes(range(256)) * 4096
    best = float("inf")
    for _ in range(rounds):
        start = time.perf_counter()
        json.loads(json.dumps(record))
        digest = hashlib.sha256()
        for _ in range(256):
            digest.update(block)
        with tempfile.TemporaryDirectory(prefix="perf-calibrate-") as tmp:
            for i in range(200):
                path = Path(tmp) / f"{i}.bin"
                path.write_bytes(block)
                path.read_bytes()
        best = min(best, time.perf_counter() - start)
    return round(best, 4)


def compare(results: dict[str, Any], baseline: dict[str, Any]) -> list[str]:
    """Return one line per regression of ``results`` over ``baseline``.

    The wall-time limit scales with the calibration ratio of the two runs.
    The peak RSS limit does not scale, because a slower runner uses the same memory.
    """
    if baseline["machine"] != results["machine"]:
        return [f"baseline machine {baseline['machine']} is not {results['machine']}"]
    if "calibration_s" not in baseline:
        return ["the baseline has no calibration_s. Record a new baseline."]
    speed = results["calibration_s"] / baseline["calibration_s"]
    print(f"runner speed: this run's calibration is {speed:.2f}x the baseline's")
    failures = []
    for name, now in results["scenarios"].items():
        before = baseline["scenarios"].get(name)
        if before is None or before["n"] != now["n"]:
            failures.append(f"{name}: the baseline has no run at n={now['n']}. Record one.")
            continue
        for key, limit, scale in (
            ("wall_s", WALL_THRESHOLD, speed),
            ("peak_rss_mb", RSS_THRESHOLD, 1.0),
        ):
            ceiling = round(before[key] * scale * (1 + limit), 3)
            verdict = "FAIL" if now[key] > ceiling else "ok"
            line = f"{name} {key}: {before[key]} -> {now[key]} (limit {ceiling:.1f}) {verdict}"
            print(line)
            if verdict == "FAIL":
                failures.append(line)
    return failures


def main(argv: list[str] | None = None) -> int:
    """Run the scenarios, write the results, and compare them when asked."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", type=Path, required=True, help="Where to write the results.")
    parser.add_argument("--compare", type=Path, help="The committed baseline to compare with.")
    parser.add_argument("--scale", type=float, default=1.0, help="Fraction of the full sizes.")
    args = parser.parse_args(argv)
    if args.compare is not None and not args.compare.is_file():
        print(f"No baseline at {args.compare}. Run the job with record-baseline first.")
        return 1
    # The scenarios block the network, so a fresh runner installs the extensions first.
    install_duckdb_extensions()
    with tempfile.TemporaryDirectory(prefix="perf-bench-") as tmp:
        results = run_all(args.scale, Path(tmp))
    args.out.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    if args.compare is None:
        return 0
    failures = compare(results, json.loads(args.compare.read_text(encoding="utf-8")))
    for line in failures:
        print(f"REGRESSION {line}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
