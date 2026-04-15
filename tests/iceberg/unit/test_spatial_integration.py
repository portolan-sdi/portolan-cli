"""Integration tests for spatial partitioning in IcebergBackend (Phase 2).

Tests that publish() adds geohash and bbox columns when the data
contains a geometry column, and creates partitioned Iceberg tables.
"""

import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def _make_wkb_point(x: float, y: float) -> bytes:
    """Create a WKB point (little-endian)."""
    return struct.pack("<BIdd", 1, 1, x, y)


def _write_geo_parquet(path, points: list[tuple[float, float]]):
    """Write a GeoParquet file with point geometries."""
    wkb_values = [_make_wkb_point(x, y) for x, y in points]
    table = pa.table(
        {
            "id": pa.array(range(len(points)), type=pa.int64()),
            "name": pa.array([f"point_{i}" for i in range(len(points))], type=pa.string()),
            "geometry": pa.array(wkb_values, type=pa.binary()),
        }
    )
    pq.write_table(table, path)
    return path


@pytest.mark.integration
def test_publish_adds_geohash_column(iceberg_backend, iceberg_catalog, tmp_path):
    """Publishing GeoParquet should add geohash column to the Iceberg table."""
    geo_file = _write_geo_parquet(
        tmp_path / "geo.parquet",
        [(2.3522, 48.8566), (-73.9857, 40.7484), (139.6917, 35.6895)],  # Paris, NYC, Tokyo
    )

    iceberg_backend.publish(
        collection="places",
        assets={"geo.parquet": str(geo_file)},
        schema={"columns": ["id", "name", "geometry"], "types": {}, "hash": "h1"},
        breaking=False,
        message="geo data",
    )

    table = iceberg_catalog.load_table("portolake.places")
    field_names = {f.name for f in table.schema().fields}

    # Should have a geohash column (precision depends on row count, but for <100K rows
    # the plan says no partitioning. However, for geo data we always add geohash for
    # spatial query support -- so this tests that geohash is added when geometry exists)
    # For small datasets, we still add the column but don't partition by it.
    has_geohash = any(name.startswith("geohash_") for name in field_names)
    assert has_geohash, f"Expected geohash column, got fields: {field_names}"


@pytest.mark.integration
def test_publish_adds_bbox_columns(iceberg_backend, iceberg_catalog, tmp_path):
    """Publishing GeoParquet should add bbox columns."""
    geo_file = _write_geo_parquet(
        tmp_path / "geo.parquet",
        [(2.3522, 48.8566), (-73.9857, 40.7484)],
    )

    iceberg_backend.publish(
        collection="places",
        assets={"geo.parquet": str(geo_file)},
        schema={"columns": ["id", "name", "geometry"], "types": {}, "hash": "h1"},
        breaking=False,
        message="geo data",
    )

    table = iceberg_catalog.load_table("portolake.places")
    field_names = {f.name for f in table.schema().fields}

    for col in ["bbox_xmin", "bbox_ymin", "bbox_xmax", "bbox_ymax"]:
        assert col in field_names, f"Expected {col} in fields: {field_names}"


@pytest.mark.integration
def test_publish_bbox_values_correct(iceberg_backend, iceberg_catalog, tmp_path):
    """Bbox values should match geometry coordinates."""
    geo_file = _write_geo_parquet(
        tmp_path / "geo.parquet",
        [(10.5, 20.3)],
    )

    iceberg_backend.publish(
        collection="places",
        assets={"geo.parquet": str(geo_file)},
        schema={"columns": ["id", "name", "geometry"], "types": {}, "hash": "h1"},
        breaking=False,
        message="single point",
    )

    table = iceberg_catalog.load_table("portolake.places")
    result = table.scan().to_arrow()

    assert result.column("bbox_xmin")[0].as_py() == pytest.approx(10.5)
    assert result.column("bbox_ymin")[0].as_py() == pytest.approx(20.3)


@pytest.mark.integration
def test_publish_skips_partitioning_small_dataset(iceberg_backend, iceberg_catalog, tmp_path):
    """Small datasets (<100K rows) should NOT have partition spec on geohash."""
    geo_file = _write_geo_parquet(
        tmp_path / "geo.parquet",
        [(2.3522, 48.8566), (-73.9857, 40.7484)],
    )

    iceberg_backend.publish(
        collection="small",
        assets={"geo.parquet": str(geo_file)},
        schema={"columns": ["id", "name", "geometry"], "types": {}, "hash": "h1"},
        breaking=False,
        message="tiny dataset",
    )

    table = iceberg_catalog.load_table("portolake.small")
    # Small dataset should have unpartitioned spec
    partition_fields = table.spec().fields
    assert len(partition_fields) == 0, f"Expected no partition fields, got {partition_fields}"


@pytest.mark.integration
def test_publish_no_geometry_skips_spatial_columns(iceberg_backend, iceberg_catalog, tmp_path):
    """Non-geometry data should not get geohash or bbox columns."""
    table_data = pa.table({"id": pa.array([1, 2, 3], type=pa.int64())})
    path = tmp_path / "plain.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="plain",
        assets={"plain.parquet": str(path)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="no geometry",
    )

    table = iceberg_catalog.load_table("portolake.plain")
    field_names = {f.name for f in table.schema().fields}

    assert not any(name.startswith("geohash_") for name in field_names)
    assert "bbox_xmin" not in field_names


@pytest.mark.integration
def test_publish_creates_partitioned_table(iceberg_catalog, tmp_path):
    """Datasets >= 100K rows should create an Iceberg table partitioned by geohash."""
    import random

    from portolan_cli.backends.iceberg.backend import IcebergBackend

    backend = IcebergBackend(catalog=iceberg_catalog)

    # Generate 100K+ points spread across Spain to trigger partitioning (precision 3)
    random.seed(42)
    n = 100_001
    points = [(random.uniform(-9.0, 3.0), random.uniform(36.0, 43.5)) for _ in range(n)]
    wkb_values = [_make_wkb_point(x, y) for x, y in points]

    table_data = pa.table(
        {
            "id": pa.array(range(n), type=pa.int64()),
            "geometry": pa.array(wkb_values, type=pa.binary()),
        }
    )
    path = tmp_path / "large_geo.parquet"
    pq.write_table(table_data, path)

    backend.publish(
        collection="large_geo",
        assets={"large_geo.parquet": str(path)},
        schema={"columns": ["id", "geometry"], "types": {}, "hash": "h1"},
        breaking=False,
        message="large geo dataset",
    )

    table = iceberg_catalog.load_table("portolake.large_geo")

    # Should have partition spec on geohash_3
    partition_fields = table.spec().fields
    assert len(partition_fields) == 1, f"Expected 1 partition field, got {partition_fields}"
    assert partition_fields[0].name == "geohash_3"

    # Verify multiple data files were created (one per partition)
    data_files = list(table.scan().plan_files())
    assert len(data_files) > 1, f"Expected multiple data files (partitions), got {len(data_files)}"


@pytest.mark.integration
def test_publish_no_partition_for_non_geo(iceberg_backend, iceberg_catalog, tmp_path):
    """Non-geometry data should never have a partition spec, regardless of size."""
    table_data = pa.table({"id": pa.array(range(1000), type=pa.int64())})
    path = tmp_path / "plain.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="plain_large",
        assets={"plain.parquet": str(path)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="plain data",
    )

    table = iceberg_catalog.load_table("portolake.plain_large")
    partition_fields = table.spec().fields
    assert len(partition_fields) == 0


@pytest.mark.integration
def test_publish_geohash_values_queryable(iceberg_backend, iceberg_catalog, tmp_path):
    """Should be able to filter by geohash value after publish."""
    geo_file = _write_geo_parquet(
        tmp_path / "geo.parquet",
        [(2.3522, 48.8566), (-73.9857, 40.7484), (139.6917, 35.6895)],
    )

    iceberg_backend.publish(
        collection="queryable",
        assets={"geo.parquet": str(geo_file)},
        schema={"columns": ["id", "name", "geometry"], "types": {}, "hash": "h1"},
        breaking=False,
        message="queryable data",
    )

    table = iceberg_catalog.load_table("portolake.queryable")
    result = table.scan().to_arrow()

    # Find which geohash column exists
    geohash_col = [c for c in result.column_names if c.startswith("geohash_")][0]
    geohash_values = result.column(geohash_col).to_pylist()

    # All values should be non-empty strings
    assert all(isinstance(v, str) and len(v) > 0 for v in geohash_values)
    # Different locations should (likely) have different geohashes
    assert len(set(geohash_values)) > 1
