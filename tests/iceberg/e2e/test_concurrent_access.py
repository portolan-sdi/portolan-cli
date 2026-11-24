"""Two writers on the stack: different collections both land, and a reader never sees nothing.

Marked ``e2e_slow``, so the nightly runs them. Each thread builds its own
backend, because a DuckDB connection serves one thread.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING

import pytest

from portolan_cli.backends.iceberg.backend import IcebergBackend
from tests.iceberg.e2e.conftest import SCHEMA, write_test_parquet

if TYPE_CHECKING:
    from pathlib import Path

    from portolan_cli.api import Version
    from tests.iceberg_stack import IcebergStack

pytestmark = [pytest.mark.e2e, pytest.mark.e2e_slow]


def test_concurrent_publishes_to_different_collections_both_succeed(
    iceberg_stack: IcebergStack, tmp_path: Path
) -> None:
    _ = iceberg_stack

    def publish_to(collection: str, rows: int) -> Version:
        (tmp_path / collection).mkdir()
        asset = write_test_parquet(tmp_path / collection / "data.parquet", rows=rows)
        backend = IcebergBackend(catalog_root=tmp_path)
        return backend.publish(collection, {"data.parquet": str(asset)}, SCHEMA, False, "c")

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {
            pool.submit(publish_to, "concurrent-a", 1): "a",
            pool.submit(publish_to, "concurrent-b", 2): "b",
        }
        results = {futures[f]: f.result() for f in as_completed(futures)}

    assert results["a"].version == "1.0.0"
    assert results["b"].version == "1.0.0"


def test_a_reader_during_a_write_sees_the_old_or_the_new_version(
    iceberg_stack: IcebergStack, tmp_path: Path
) -> None:
    _ = iceberg_stack
    (tmp_path / "read-write").mkdir()
    seed = write_test_parquet(tmp_path / "read-write" / "data.parquet", rows=1)
    IcebergBackend(catalog_root=tmp_path).publish(
        "read-write",
        {"data.parquet": str(seed)},
        SCHEMA,
        False,
        "seed",
    )

    def writer() -> Version:
        asset = write_test_parquet(tmp_path / "read-write" / "data.parquet", rows=5)
        return IcebergBackend(catalog_root=tmp_path).publish(
            "read-write",
            {"data.parquet": str(asset)},
            SCHEMA,
            False,
            "second",
        )

    def reader() -> int:
        return len(IcebergBackend(catalog_root=tmp_path).list_versions("read-write"))

    with ThreadPoolExecutor(max_workers=2) as pool:
        write = pool.submit(writer)
        read = pool.submit(reader)
        write.result()
        seen = read.result()

    assert seen in (1, 2)
    assert len(IcebergBackend(catalog_root=tmp_path).list_versions("read-write")) == 2
