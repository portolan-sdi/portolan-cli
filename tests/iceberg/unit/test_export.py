"""Tests for static GeoParquet export from Iceberg snapshots (Phase 6)."""

import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def _make_wkb_point(x: float, y: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, x, y)


@pytest.mark.integration
def test_export_current_snapshot(iceberg_backend, iceberg_catalog, tmp_path):
    """export_current_snapshot should write all current rows to a single Parquet file."""
    table_data = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["a", "b", "c"], type=pa.string()),
        }
    )
    src = tmp_path / "data.parquet"
    pq.write_table(table_data, src)

    iceberg_backend.publish(
        collection="buildings",
        assets={"data.parquet": str(src)},
        schema={"columns": ["id", "name"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolan_cli.backends.iceberg.export import export_current_snapshot

    output = tmp_path / "export.parquet"
    table = iceberg_catalog.load_table("portolake.buildings")
    export_current_snapshot(table, output)

    assert output.exists()
    result = pq.read_table(output)
    assert len(result) == 3
    assert sorted(result.column("id").to_pylist()) == [1, 2, 3]


@pytest.mark.integration
def test_export_excludes_derived_columns(iceberg_backend, iceberg_catalog, tmp_path):
    """Export should exclude portolake-derived columns (geohash_*, bbox_*)."""
    wkb_values = [_make_wkb_point(2.35, 48.85), _make_wkb_point(-73.99, 40.75)]
    table_data = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "geometry": pa.array(wkb_values, type=pa.binary()),
        }
    )
    src = tmp_path / "geo.parquet"
    pq.write_table(table_data, src)

    iceberg_backend.publish(
        collection="places",
        assets={"geo.parquet": str(src)},
        schema={"columns": ["id", "geometry"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolan_cli.backends.iceberg.export import export_current_snapshot

    output = tmp_path / "export.parquet"
    table = iceberg_catalog.load_table("portolake.places")
    export_current_snapshot(table, output)

    result = pq.read_table(output)
    col_names = result.column_names
    assert "id" in col_names
    assert "geometry" in col_names
    assert not any(c.startswith("geohash_") for c in col_names)
    assert not any(c.startswith("bbox_") for c in col_names)


@pytest.mark.integration
def test_export_after_multiple_publishes(iceberg_backend, iceberg_catalog, tmp_path):
    """Export should contain all rows from current snapshot (appended data)."""
    t1 = pa.table({"id": pa.array([1, 2], type=pa.int64())})
    p1 = tmp_path / "v1.parquet"
    pq.write_table(t1, p1)

    iceberg_backend.publish(
        collection="growing",
        assets={"data.parquet": str(p1)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    t2 = pa.table({"id": pa.array([3, 4, 5], type=pa.int64())})
    p2 = tmp_path / "v2.parquet"
    pq.write_table(t2, p2)

    iceberg_backend.publish(
        collection="growing",
        assets={"data.parquet": str(p2)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v2",
    )

    from portolan_cli.backends.iceberg.export import export_current_snapshot

    output = tmp_path / "export.parquet"
    table = iceberg_catalog.load_table("portolake.growing")
    export_current_snapshot(table, output)

    result = pq.read_table(output)
    assert len(result) == 5


@pytest.mark.integration
def test_export_empty_table(iceberg_catalog, tmp_path):
    """Export of an empty table should produce an empty Parquet file."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    iceberg_catalog.create_namespace("portolake")
    schema = Schema(NestedField(1, "id", LongType()))
    table = iceberg_catalog.create_table("portolake.empty", schema=schema)

    from portolan_cli.backends.iceberg.export import export_current_snapshot

    output = tmp_path / "export.parquet"
    export_current_snapshot(table, output)

    assert output.exists()
    result = pq.read_table(output)
    assert len(result) == 0
