"""The table registers the published GeoParquet rather than copying its rows (#938)."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pytestmark = pytest.mark.unit

_GEO = (
    b'{"version":"1.1.0","primary_column":"geometry",'
    b'"columns":{"geometry":{"encoding":"WKB","geometry_types":[]}}}'
)


def _geoparquet(path: Path, rows: int = 3) -> Path:
    """A GeoParquet file: WKB points and the geo key a reader looks for."""
    from shapely import Point, to_wkb

    wkb = [to_wkb(Point(i * 0.01, 40 + i * 0.01)) for i in range(rows)]
    table = pa.table(
        {
            "id": pa.array(range(rows), type=pa.int64()),
            "geometry": pa.array(wkb, type=pa.binary()),
        }
    )
    table = table.replace_schema_metadata({b"geo": _GEO})
    pq.write_table(table, path)
    return path


@pytest.mark.integration
def test_publish_registers_the_file_and_writes_no_copy(iceberg_backend, iceberg_catalog, tmp_path):
    """The warehouse holds no second copy of the rows."""
    src = _geoparquet(tmp_path / "data.parquet")

    iceberg_backend.publish(
        collection="reg",
        assets={"data.parquet": str(src)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    table = iceberg_catalog.load_table("portolake.reg")
    registered = [task.file.file_path for task in table.scan().plan_files()]

    assert len(registered) == 1
    assert Path(registered[0].replace("file://", "")) == src


@pytest.mark.integration
def test_the_registered_file_keeps_its_geo_key(iceberg_backend, iceberg_catalog, tmp_path):
    """Nothing rewrites the file, so its GeoParquet metadata survives."""
    src = _geoparquet(tmp_path / "data.parquet")

    iceberg_backend.publish(
        collection="geokey",
        assets={"data.parquet": str(src)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    table = iceberg_catalog.load_table("portolake.geokey")
    path = Path(next(iter(table.scan().plan_files())).file.file_path.replace("file://", ""))
    metadata = pq.ParquetFile(path).schema_arrow.metadata or {}

    assert b"geo" in metadata
