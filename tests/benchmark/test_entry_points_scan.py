"""Cost of the entry point scan with many installed distributions (#947).

The plugin manager and the lazy command group read entry points through
``importlib.metadata.entry_points(group=...)``. That call reads every installed
distribution. This benchmark adds 300 fake distributions to this environment
and times the first scan in a fresh interpreter, which is what each
``portolan`` invocation pays.
"""

from __future__ import annotations

import os
import statistics
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest

from tests.support.fake_dist import write_fake_dist

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.benchmark

EXTRA_DISTRIBUTIONS = 300
RUNS = 5
# A ceiling well above the measured cost, to catch a regression rather than
# to describe the normal case. In October 2026 on Linux with CPython 3.12.13
# and 230 installed distributions, one scan took 7 to 9 ms. The first scan
# with 300 extra distributions (528 in total) took 24.5 ms. Compare timings
# with the same interpreter on both sides: a pyenv CPython 3.12.2 build
# imported portolan_cli.cli about 110 ms slower than uv's 3.12.13 build.
CEILING_SECONDS = 0.25

_PROBE = """
import time
from importlib.metadata import distributions, entry_points
start = time.perf_counter()
eps = entry_points(group="portolan")
elapsed = time.perf_counter() - start
print(sum(1 for _ in distributions()), len(eps), elapsed)
"""


def _scan(site: Path) -> tuple[int, int, float]:
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(site), env.get("PYTHONPATH", "")])
    out = subprocess.run(
        [sys.executable, "-c", _PROBE], capture_output=True, text=True, check=True, env=env
    ).stdout.split()
    return int(out[0]), int(out[1]), float(out[2])


def test_entry_point_scan_with_300_extra_distributions(tmp_path: Path) -> None:
    for index in range(EXTRA_DISTRIBUTIONS):
        group = {"portolan": {f"fake{index}": "fake_module"}} if index == 0 else {}
        write_fake_dist(tmp_path, f"fake-dist-{index}", {"console_scripts": {}, **group})

    runs = [_scan(tmp_path) for _ in range(RUNS)]
    seconds = statistics.median(elapsed for _, _, elapsed in runs)
    installed, found, _ = runs[0]

    print(f"\n{installed} distributions, first scan median {seconds * 1000:.1f} ms")
    assert found >= 1
    assert installed >= EXTRA_DISTRIBUTIONS
    assert seconds < CEILING_SECONDS
