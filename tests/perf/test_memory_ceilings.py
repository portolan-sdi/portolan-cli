"""tracemalloc ceiling for the 100,000-feature thumbnail (#992).

The test runs the scenario in a cold child process with
``PORTOLAN_TIMINGS=mem``. The trace records the tracemalloc peak of each span.
The ceiling is the measured peak plus 15%. The thumbnail memory is Python and
matplotlib memory, so tracemalloc sees it.

The large-vector add has no ceiling here because tracemalloc misses Arrow and
DuckDB, so tests/perf/bench.py gates its nightly RSS.

The test takes more than five seconds, so it carries the ``slow`` marker and
runs in the nightly slow job.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from tests.perf.expected.viz import THUMBNAIL_DRAWS, THUMBNAIL_TRACEMALLOC_MB
from tests.perf.gates import measure
from tests.perf.scenarios import SCENARIOS

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = [pytest.mark.integration, pytest.mark.slow]


def _span(record: dict[str, Any], name: str) -> dict[str, Any]:
    (root,) = record["timings"]
    (node,) = [child for child in root["children"] if child["name"] == name]
    return dict(node)


def test_large_thumbnail_stays_under_its_ceiling(tmp_path: Path) -> None:
    record = measure(SCENARIOS["thumbnail-large"], tmp_path, 100_000, mem=True)

    assert (record["exit_code"], record["network"]) == (0, [])
    assert _span(record, "add.thumbnails")["mem_peak_mb"] <= THUMBNAIL_TRACEMALLOC_MB
    assert record["counters"]["thumbnail.draws"] == THUMBNAIL_DRAWS
