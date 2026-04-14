"""E2E tests for S3 storage verification via MinIO.

Verifies that Iceberg data and metadata files are correctly stored
in MinIO S3-compatible storage after publishing through the REST catalog.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests.iceberg.e2e.conftest import write_test_parquet


@pytest.mark.e2e
def test_parquet_files_in_s3(rest_iceberg_backend, minio_client, tmp_path):
    """boto3 list_objects finds Parquet data files in warehouse bucket."""
    asset = write_test_parquet(tmp_path / "s3_data.parquet")

    rest_iceberg_backend.publish(
        collection="s3-parquet",
        assets={"s3_data.parquet": str(asset)},
        schema={"columns": ["id", "val"], "types": {}, "hash": "h1"},
        breaking=False,
        message="S3 parquet check",
    )

    response = minio_client.list_objects_v2(Bucket="warehouse", Prefix="portolake/s3-parquet/")
    contents = response.get("Contents", [])
    parquet_keys = [obj["Key"] for obj in contents if obj["Key"].endswith(".parquet")]

    assert len(parquet_keys) > 0, f"No parquet files under portolake/s3-parquet/: {contents}"


@pytest.mark.e2e
def test_iceberg_metadata_in_s3(rest_iceberg_backend, minio_client, tmp_path):
    """Iceberg metadata JSON and manifest files should exist in S3."""
    asset = write_test_parquet(tmp_path / "meta_check.parquet")

    rest_iceberg_backend.publish(
        collection="s3-metadata",
        assets={"meta_check.parquet": str(asset)},
        schema={"columns": ["id", "val"], "types": {}, "hash": "h1"},
        breaking=False,
        message="Metadata check",
    )

    response = minio_client.list_objects_v2(
        Bucket="warehouse", Prefix="portolake/s3-metadata/metadata/"
    )
    contents = response.get("Contents", [])
    keys = [obj["Key"] for obj in contents]

    has_metadata_json = any(k.endswith(".metadata.json") for k in keys)
    assert has_metadata_json, f"No metadata.json files found: {keys}"


@pytest.mark.e2e
@pytest.mark.e2e_slow
def test_large_dataset_s3(rest_iceberg_backend, rest_catalog, tmp_path):
    """10K-row dataset round-trips correctly through S3."""
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

    # Read back from Iceberg table via REST catalog
    iceberg_table = rest_catalog.load_table("portolake.s3-large")
    result = iceberg_table.scan().to_arrow()
    assert result.num_rows == rows, f"Expected {rows} rows, got {result.num_rows}"
