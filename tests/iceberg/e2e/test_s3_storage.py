"""E2E tests for S3 storage verification via RustFS.

Verifies that Iceberg data and metadata files are correctly stored
in RustFS S3-compatible storage after publishing through the REST catalog.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests.iceberg.e2e.conftest import write_test_parquet
from tests.iceberg_stack import scan_row_count, warehouse_objects

if TYPE_CHECKING:
    from pathlib import Path

    from portolan_cli.backends.iceberg.backend import IcebergBackend
    from tests.iceberg_stack import IcebergLane, RestAdmin


@pytest.mark.e2e
def test_parquet_files_in_s3(
    rest_iceberg_backend: IcebergBackend, iceberg_lane: IcebergLane, tmp_path: Path
) -> None:
    """A DuckDB listing finds Parquet data files in the warehouse bucket."""
    asset = write_test_parquet(tmp_path / "s3_data.parquet")

    rest_iceberg_backend.publish(
        collection="s3-parquet",
        assets={"s3_data.parquet": str(asset)},
        schema={"columns": ["id", "val"], "types": {}, "hash": "h1"},
        breaking=False,
        message="S3 parquet check",
    )

    keys = warehouse_objects(iceberg_lane, "portolake/s3-parquet/")
    parquet_keys = [key for key in keys if key.endswith(".parquet")]

    assert len(parquet_keys) > 0, f"No parquet files under portolake/s3-parquet/: {keys}"


@pytest.mark.e2e
def test_iceberg_metadata_in_s3(
    rest_iceberg_backend: IcebergBackend, iceberg_lane: IcebergLane, tmp_path: Path
) -> None:
    """Iceberg metadata JSON and manifest files should exist in S3."""
    asset = write_test_parquet(tmp_path / "meta_check.parquet")

    rest_iceberg_backend.publish(
        collection="s3-metadata",
        assets={"meta_check.parquet": str(asset)},
        schema={"columns": ["id", "val"], "types": {}, "hash": "h1"},
        breaking=False,
        message="Metadata check",
    )

    keys = warehouse_objects(iceberg_lane, "portolake/s3-metadata/metadata/")

    has_metadata_json = any(k.endswith(".metadata.json") for k in keys)
    assert has_metadata_json, f"No metadata.json files found: {keys}"


@pytest.mark.e2e
@pytest.mark.e2e_slow
def test_large_dataset_s3(
    rest_iceberg_backend: IcebergBackend,
    rest_admin: RestAdmin,
    iceberg_lane: IcebergLane,
    tmp_path: Path,
) -> None:
    """10K-row table round-trips through S3, read back the way a user reads it.

    DuckDB's ``iceberg_scan`` over the metadata location, with no catalog
    server, is the reader path the collection publishes.
    """
    rows = 10_000
    table = pa.table(
        {
            "id": pa.array(range(rows), type=pa.int64()),
            "val": pa.array([f"row_{i}" for i in range(rows)], type=pa.string()),
        }
    )
    path = tmp_path / "large.parquet"
    pq.write_table(table, path)

    rest_iceberg_backend.publish(
        collection="s3-large",
        assets={"large.parquet": str(path)},
        schema={"columns": ["id", "val"], "types": {}, "hash": "h1"},
        breaking=False,
        message="Large dataset",
    )

    loaded = rest_admin.load_table("portolake", "s3-large")
    count = scan_row_count(iceberg_lane, loaded.metadata_location)
    assert count == rows, f"Expected {rows} rows, got {count}"
