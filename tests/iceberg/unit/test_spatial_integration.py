"""The table's columns are the GeoParquet's, with nothing derived added (#938).

`publish()` registered the rows by rewriting them, and the rewrite added a
`geohash_N` column and `bbox_*` columns so the table could carry an identity
partition spec over the geohash. The backend now registers the published file
instead, so the table holds that file's own columns.

`specs/incubating/iceberg.md` asks for this. A partitioned collection maps to an
identity spec over its own `partition:keys`, and the partition cell column stays
in the data files. A geohash the backend derives at ingest is not that column.
"""

from __future__ import annotations

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


_POINTS = [(2.3522, 48.8566), (-73.9857, 40.7484), (139.6917, 35.6895)]


def _publish(backend, path, collection):
    backend.publish(
        collection=collection,
        assets={"geo.parquet": str(path)},
        schema={"columns": ["id", "name", "geometry"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )


@pytest.mark.integration
def test_publish_adds_no_derived_columns(iceberg_backend, iceberg_catalog, tmp_path):
    """The table's columns are exactly the file's."""
    geo_file = _write_geo_parquet(tmp_path / "geo.parquet", _POINTS)

    _publish(iceberg_backend, geo_file, "nocols")

    columns = set(iceberg_catalog.load_table("portolake.nocols").schema().column_names)
    assert columns == {"id", "name", "geometry"}


@pytest.mark.integration
def test_publish_adds_no_geohash_column(iceberg_backend, iceberg_catalog, tmp_path):
    """A geohash the backend derives at ingest is not the convention's partition column."""
    geo_file = _write_geo_parquet(tmp_path / "geo.parquet", _POINTS)

    _publish(iceberg_backend, geo_file, "nogeohash")

    columns = iceberg_catalog.load_table("portolake.nogeohash").schema().column_names
    assert not [c for c in columns if c.startswith("geohash_")]


@pytest.mark.integration
def test_publish_adds_no_bbox_columns(iceberg_backend, iceberg_catalog, tmp_path):
    """The GeoParquet carries its own bbox covering column when it has one."""
    geo_file = _write_geo_parquet(tmp_path / "geo.parquet", _POINTS)

    _publish(iceberg_backend, geo_file, "nobbox")

    columns = iceberg_catalog.load_table("portolake.nobbox").schema().column_names
    assert not [c for c in columns if c.startswith("bbox_")]


@pytest.mark.integration
def test_publish_creates_an_unpartitioned_table(iceberg_backend, iceberg_catalog, tmp_path):
    """No partition spec, because the file carries no partition cell column."""
    geo_file = _write_geo_parquet(tmp_path / "geo.parquet", _POINTS)

    _publish(iceberg_backend, geo_file, "unpart")

    assert iceberg_catalog.load_table("portolake.unpart").spec().fields == ()


@pytest.mark.integration
def test_the_rows_are_readable_from_the_registered_file(iceberg_backend, iceberg_catalog, tmp_path):
    """Registering changes where the rows live, not whether they read back."""
    geo_file = _write_geo_parquet(tmp_path / "geo.parquet", _POINTS)

    _publish(iceberg_backend, geo_file, "readable")

    arrow = iceberg_catalog.load_table("portolake.readable").scan().to_arrow()
    assert arrow.num_rows == len(_POINTS)
    assert pq.ParquetFile(geo_file).metadata.num_rows == len(_POINTS)
