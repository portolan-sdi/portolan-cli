"""tracemalloc ceilings for the large-vector add and the 100,000-feature thumbnail (#992).

Each test runs one scenario in a cold child process with
``PORTOLAN_TIMINGS=mem``. The trace records the tracemalloc peak of each span.
A ceiling is the measured peak plus 15%. tracemalloc counts the Python heap
and numpy arrays. It does not count Arrow or DuckDB buffers, so the nightly
scenario job gates the peak RSS of the same scenarios.

Both tests take more than five seconds, so they carry the ``slow`` marker and
run in the nightly slow job.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from tests.perf.expected.parquet import LARGE_VECTOR_TRACEMALLOC_MB
from tests.perf.expected.viz import THUMBNAIL_DRAWS, THUMBNAIL_TRACEMALLOC_MB
from tests.perf.gates import measure
from tests.perf.scenarios import SCENARIOS

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = [pytest.mark.integration, pytest.mark.slow]


def _span(record: dict[str, Any], name: str) -> dict[str, Any]:
    (root,) = record["timings"]
    if root["name"] == name:
        return dict(root)
    (node,) = [child for child in root["children"] if child["name"] == name]
    return dict(node)


def test_large_vector_add_stays_under_its_ceiling(tmp_path: Path) -> None:
    record = measure(SCENARIOS["add-large-vector"], tmp_path, 1_000_000, mem=True)

    assert (record["exit_code"], record["network"]) == (0, [])
    assert _span(record, "portolan.add")["mem_peak_mb"] <= LARGE_VECTOR_TRACEMALLOC_MB


def test_large_thumbnail_stays_under_its_ceiling(tmp_path: Path) -> None:
    record = measure(SCENARIOS["thumbnail-large"], tmp_path, 100_000, mem=True)

    assert (record["exit_code"], record["network"]) == (0, [])
    assert _span(record, "add.thumbnails")["mem_peak_mb"] <= THUMBNAIL_TRACEMALLOC_MB
    assert record["counters"]["thumbnail.draws"] == THUMBNAIL_DRAWS
