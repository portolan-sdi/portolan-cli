"""Tests for Iceberg-native data storage (Phase 1).

These tests verify that publish() writes actual Parquet data into Iceberg tables,
not just metadata. The Iceberg table schema is dynamic (inferred from the data).
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# --- Data ingestion ---


@pytest.mark.integration
def test_publish_writes_actual_data(iceberg_backend, iceberg_catalog, parquet_file):
    """publish() should write actual Parquet rows into the Iceberg table."""
    iceberg_backend.publish(
        collection="buildings",
        assets={"data.parquet": str(parquet_file)},
        schema={"columns": ["id", "name", "value"], "types": {}, "hash": "h1"},
        breaking=False,
        message="ingest data",
    )

    # Verify actual data exists in the Iceberg table
    table = iceberg_catalog.load_table("portolake.buildings")
    result = table.scan().to_arrow()
    assert len(result) == 3  # 3 rows from parquet_file fixture


@pytest.mark.integration
def test_publish_preserves_schema(iceberg_backend, iceberg_catalog, parquet_file):
    """Iceberg table schema should match the ingested Parquet schema."""
    iceberg_backend.publish(
        collection="buildings",
        assets={"data.parquet": str(parquet_file)},
        schema={"columns": ["id", "name", "value"], "types": {}, "hash": "h1"},
        breaking=False,
        message="ingest data",
    )

    table = iceberg_catalog.load_table("portolake.buildings")
    iceberg_schema = table.schema()

    # Should have the data columns from the parquet file
    field_names = {field.name for field in iceberg_schema.fields}
    assert "id" in field_names
    assert "name" in field_names
    assert "value" in field_names


@pytest.mark.integration
def test_publish_preserves_data_values(iceberg_backend, iceberg_catalog, parquet_file):
    """Ingested data should have correct values."""
    iceberg_backend.publish(
        collection="buildings",
        assets={"data.parquet": str(parquet_file)},
        schema={"columns": ["id", "name", "value"], "types": {}, "hash": "h1"},
        breaking=False,
        message="ingest data",
    )

    table = iceberg_catalog.load_table("portolake.buildings")
    result = table.scan().to_arrow()

    ids = sorted(result.column("id").to_pylist())
    assert ids == [1, 2, 3]

    names = sorted(result.column("name").to_pylist())
    assert names == ["alpha", "beta", "gamma"]


# --- Version metadata still works ---


@pytest.mark.integration
def test_publish_snapshot_has_version_metadata(iceberg_backend, iceberg_catalog, parquet_file):
    """Version metadata should still be stored in snapshot summary properties."""
    version = iceberg_backend.publish(
        collection="buildings",
        assets={"data.parquet": str(parquet_file)},
        schema={"columns": ["id", "name", "value"], "types": {}, "hash": "h1"},
        breaking=False,
        message="first ingest",
    )

    assert version.version == "1.0.0"
    assert version.message == "first ingest"
    assert version.breaking is False
    assert "data.parquet" in version.assets


@pytest.mark.integration
def test_publish_asset_metadata_preserved(iceberg_backend, parquet_file):
    """Asset metadata (sha256, size, href) should be in version even with data ingestion."""
    version = iceberg_backend.publish(
        collection="buildings",
        assets={"data.parquet": str(parquet_file)},
        schema={"columns": ["id", "name", "value"], "types": {}, "hash": "h1"},
        breaking=False,
        message="check assets",
    )

    asset = version.assets["data.parquet"]
    assert asset.sha256 != ""
    assert asset.size_bytes > 0
    assert asset.href == "buildings/data.parquet"


# --- Time travel ---


@pytest.mark.integration
def test_time_travel_reads_correct_data(
    iceberg_backend, iceberg_catalog, parquet_file, parquet_file_v2
):
    """Publishing v2 should not destroy v1 data -- old snapshot should have v1 rows."""
    # Publish v1 with 3 rows
    iceberg_backend.publish(
        collection="buildings",
        assets={"data.parquet": str(parquet_file)},
        schema={"columns": ["id", "name", "value"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    table = iceberg_catalog.load_table("portolake.buildings")
    v1_snapshot_id = table.current_snapshot().snapshot_id

    # Publish v2 with 5 rows
    iceberg_backend.publish(
        collection="buildings",
        assets={"data.parquet": str(parquet_file_v2)},
        schema={"columns": ["id", "name", "value"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v2",
    )

    # Current data should have v2 rows
    table = iceberg_catalog.load_table("portolake.buildings")
    current = table.scan().to_arrow()
    assert len(current) >= 5  # v2 has 5 rows

    # Time travel to v1 should have v1 rows
    v1_data = table.scan(snapshot_id=v1_snapshot_id).to_arrow()
    assert len(v1_data) == 3
    v1_ids = sorted(v1_data.column("id").to_pylist())
    assert v1_ids == [1, 2, 3]


# --- Rollback with actual data ---


@pytest.mark.integration
def test_rollback_restores_data(iceberg_backend, iceberg_catalog, parquet_file, parquet_file_v2):
    """Rollback should set current snapshot to the target, restoring its data."""
    # Publish v1 with 3 rows
    iceberg_backend.publish(
        collection="buildings",
        assets={"data.parquet": str(parquet_file)},
        schema={"columns": ["id", "name", "value"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    # Publish v2 with 5 rows
    iceberg_backend.publish(
        collection="buildings",
        assets={"data.parquet": str(parquet_file_v2)},
        schema={"columns": ["id", "name", "value"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v2",
    )

    # Rollback to v1
    rolled = iceberg_backend.rollback("buildings", "1.0.0")
    assert rolled.version == "1.0.0"

    # Current snapshot should point to v1's data (3 rows)
    table = iceberg_catalog.load_table("portolake.buildings")
    current_data = table.scan().to_arrow()
    assert len(current_data) == 3
    v1_ids = sorted(current_data.column("id").to_pylist())
    assert v1_ids == [1, 2, 3]


# --- Schema evolution ---


@pytest.mark.integration
def test_publish_handles_schema_evolution(iceberg_backend, iceberg_catalog, tmp_path):
    """Publishing data with a new column should auto-evolve the Iceberg schema."""
    # v1: id, name
    table_v1 = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["a", "b"], type=pa.string()),
        }
    )
    path_v1 = tmp_path / "v1.parquet"
    pq.write_table(table_v1, path_v1)

    iceberg_backend.publish(
        collection="evolving",
        assets={"data.parquet": str(path_v1)},
        schema={"columns": ["id", "name"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    # v2: id, name, new_col
    table_v2 = pa.table(
        {
            "id": pa.array([3, 4], type=pa.int64()),
            "name": pa.array(["c", "d"], type=pa.string()),
            "new_col": pa.array([100.0, 200.0], type=pa.float64()),
        }
    )
    path_v2 = tmp_path / "v2.parquet"
    pq.write_table(table_v2, path_v2)

    iceberg_backend.publish(
        collection="evolving",
        assets={"data.parquet": str(path_v2)},
        schema={"columns": ["id", "name", "new_col"], "types": {}, "hash": "h2"},
        breaking=True,
        message="v2 with new column",
    )

    # Iceberg schema should now have all 3 columns
    table = iceberg_catalog.load_table("portolake.evolving")
    field_names = {field.name for field in table.schema().fields}
    assert "id" in field_names
    assert "name" in field_names
    assert "new_col" in field_names


# --- Multiple assets ---


@pytest.mark.integration
def test_publish_multiple_parquet_assets(iceberg_backend, iceberg_catalog, tmp_path):
    """Publishing multiple Parquet files should concatenate their data."""
    t1 = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["a", "b"], type=pa.string()),
        }
    )
    t2 = pa.table(
        {
            "id": pa.array([3, 4], type=pa.int64()),
            "name": pa.array(["c", "d"], type=pa.string()),
        }
    )
    p1 = tmp_path / "part1.parquet"
    p2 = tmp_path / "part2.parquet"
    pq.write_table(t1, p1)
    pq.write_table(t2, p2)

    iceberg_backend.publish(
        collection="multi",
        assets={"part1.parquet": str(p1), "part2.parquet": str(p2)},
        schema={"columns": ["id", "name"], "types": {}, "hash": "h1"},
        breaking=False,
        message="two parts",
    )

    table = iceberg_catalog.load_table("portolake.multi")
    result = table.scan().to_arrow()
    assert len(result) == 4
    ids = sorted(result.column("id").to_pylist())
    assert ids == [1, 2, 3, 4]
