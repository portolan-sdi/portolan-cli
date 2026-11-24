"""Shared fixtures for the Iceberg backend tests.

The unit tests run on ``tests.iceberg.fake_lake``: a fake REST catalog and
a fake DuckDB connection, with no network. The e2e tests under
``tests/iceberg/e2e`` add the compose stack through ``tests/iceberg_stack.py``.
A developer's ``PORTOLAN_ICEBERG_*`` exports are stripped at session start
by ``tests/conftest.py``, so a unit test never reaches a real catalog.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests.iceberg.fake_lake import Lake

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def lake(tmp_path: Path) -> Lake:
    """A backend on the fake catalog and the fake connection, rooted in ``tmp_path``."""
    return Lake(tmp_path)


@pytest.fixture
def parquet_file(tmp_path: Path) -> Path:
    """A Parquet file with three rows and no geometry."""
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["alpha", "beta", "gamma"], type=pa.string()),
            "value": pa.array([10.5, 20.0, 30.7], type=pa.float64()),
        }
    )
    path = tmp_path / "test_data.parquet"
    pq.write_table(table, path)
    return path
