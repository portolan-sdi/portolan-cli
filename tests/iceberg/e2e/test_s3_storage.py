"""The data and the metadata land in the warehouse bucket, where DuckDB reads them."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from portolan_cli.backends.iceberg.backend import IcebergBackend
from tests.iceberg.e2e.conftest import SCHEMA, write_test_parquet

if TYPE_CHECKING:
    from pathlib import Path

    from tests.iceberg_stack import IcebergStack

pytestmark = pytest.mark.e2e


def test_the_data_files_and_the_metadata_files_are_in_the_bucket(
    iceberg_stack: IcebergStack, tmp_path: Path
) -> None:
    backend = IcebergBackend(catalog_root=tmp_path)
    (tmp_path / "s3-check").mkdir()
    asset = write_test_parquet(tmp_path / "s3-check" / "data.parquet")

    backend.publish("s3-check", {"data.parquet": str(asset)}, SCHEMA, False, "S3")

    keys = iceberg_stack.objects("s3-check")
    assert any(
        key.startswith(f"{iceberg_stack.namespace}/s3-check/data/") and key.endswith(".parquet")
        for key in keys
    ), keys
    assert any(key.endswith(".metadata.json") for key in keys), keys
    assert iceberg_stack.load_table("s3-check").metadata_location.startswith(
        f"{iceberg_stack.lane.bucket_url}{iceberg_stack.namespace}/s3-check/metadata/"
    )


@pytest.mark.e2e_slow
def test_a_large_file_round_trips_through_the_bucket(
    iceberg_stack: IcebergStack, tmp_path: Path
) -> None:
    rows = 10_000
    table = pa.table(
        {
            "id": pa.array(range(rows), type=pa.int64()),
            "val": pa.array([f"row_{i}" for i in range(rows)], type=pa.string()),
        }
    )
    (tmp_path / "s3-large").mkdir()
    path = tmp_path / "s3-large" / "data.parquet"
    pq.write_table(table, path)
    backend = IcebergBackend(catalog_root=tmp_path)

    backend.publish("s3-large", {"data.parquet": str(path)}, SCHEMA, False, "large")

    assert iceberg_stack.row_count("s3-large") == rows
