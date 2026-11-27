"""tracemalloc ceiling, measured growth plus 15%, for the 100,000-feature thumbnail span.
tracemalloc misses Arrow and DuckDB, so tests/perf/bench.py gates the large-vector RSS.
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
    assert _span(record, "add.thumbnails")["mem_growth_mb"] <= THUMBNAIL_TRACEMALLOC_MB
    assert record["counters"]["thumbnail.draws"] == THUMBNAIL_DRAWS
