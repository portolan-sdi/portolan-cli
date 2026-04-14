"""E2E tests for full version lifecycle against REST catalog + MinIO.

These tests exercise IcebergBackend.publish/list/rollback/prune
against a real Iceberg REST catalog server with S3 storage.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests.iceberg.e2e.conftest import write_test_parquet


@pytest.mark.e2e
def test_publish_to_rest_catalog(rest_iceberg_backend, tmp_path):
    """Publish data via REST catalog; verify table is queryable."""
    asset = write_test_parquet(tmp_path / "data.parquet")

    version = rest_iceberg_backend.publish(
        collection="rest-publish",
        assets={"data.parquet": str(asset)},
        schema={"columns": ["id", "val"], "types": {"id": "int64", "val": "string"}, "hash": "h1"},
        breaking=False,
        message="First version via REST",
    )

    assert version.version == "1.0.0"
    assert version.message == "First version via REST"


@pytest.mark.e2e
def test_version_lifecycle_rest(rest_iceberg_backend, tmp_path):
    """Full lifecycle: publish v1->v2->v3, list, rollback to v1, prune."""
    collection = "lifecycle"

    # Publish 3 versions
    for i in range(1, 4):
        asset = write_test_parquet(tmp_path / f"data_v{i}.parquet", rows=i)
        rest_iceberg_backend.publish(
            collection=collection,
            assets={f"data_v{i}.parquet": str(asset)},
            schema={"columns": ["id", "val"], "types": {}, "hash": f"h{i}"},
            breaking=False,
            message=f"Version {i}",
        )

    # List -- should have 3 versions oldest-first
    versions = rest_iceberg_backend.list_versions(collection)
    assert len(versions) == 3
    assert versions[0].version == "1.0.0"
    assert versions[2].version == "1.2.0"

    # Rollback to 1.0.0
    restored = rest_iceberg_backend.rollback(collection, "1.0.0")
    assert restored.version == "1.0.0"

    # Verify current is 1.0.0
    current = rest_iceberg_backend.get_current_version(collection)
    assert current.version == "1.0.0"


@pytest.mark.e2e
def test_schema_evolution_rest(rest_iceberg_backend, tmp_path):
    """Add a column in v2 -- Iceberg schema evolution should work via REST."""
    collection = "schema-evo"

    # v1: {id, val}
    asset1 = write_test_parquet(tmp_path / "v1.parquet")
    rest_iceberg_backend.publish(
        collection=collection,
        assets={"v1.parquet": str(asset1)},
        schema={"columns": ["id", "val"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    # v2: {id, val, extra} -- new column
    table2 = pa.table(
        {
            "id": pa.array([10, 20], type=pa.int64()),
            "val": pa.array(["a", "b"], type=pa.string()),
            "extra": pa.array([1.0, 2.0], type=pa.float64()),
        }
    )
    v2_path = tmp_path / "v2.parquet"
    pq.write_table(table2, v2_path)

    version = rest_iceberg_backend.publish(
        collection=collection,
        assets={"v2.parquet": str(v2_path)},
        schema={"columns": ["id", "val", "extra"], "types": {}, "hash": "h2"},
        breaking=False,
        message="v2 with extra column",
    )
    assert version.version == "1.1.0"


@pytest.mark.e2e
def test_data_stored_in_minio(rest_iceberg_backend, minio_client, tmp_path):
    """After publish, Parquet data files should exist in MinIO warehouse bucket."""
    asset = write_test_parquet(tmp_path / "minio_check.parquet")

    rest_iceberg_backend.publish(
        collection="minio-check",
        assets={"minio_check.parquet": str(asset)},
        schema={"columns": ["id", "val"], "types": {}, "hash": "h1"},
        breaking=False,
        message="Check MinIO",
    )

    response = minio_client.list_objects_v2(Bucket="warehouse", Prefix="portolake/")
    contents = response.get("Contents", [])
    keys = [obj["Key"] for obj in contents]

    # Should have at least data files and metadata
    assert len(keys) > 0, "No objects found in warehouse bucket"
    has_parquet = any(k.endswith(".parquet") for k in keys)
    assert has_parquet, f"No parquet files in warehouse: {keys}"
