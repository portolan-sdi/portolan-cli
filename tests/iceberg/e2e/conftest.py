"""Helpers for the e2e tests against the compose stack.

The fixtures live in ``tests/conftest.py`` (``iceberg_stack`` and the ones
it builds on), so a test in any directory can use the stack. This file
holds the file writers the e2e tests share.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from portolan_cli.backends.iceberg.backend import IcebergBackend

if TYPE_CHECKING:
    from pathlib import Path

    from portolan_cli.backends.protocol import SchemaFingerprint


def write_test_parquet(path: Path, rows: int = 3, *, start: int = 0) -> Path:
    """Write a Parquet file with ``id`` and ``val`` columns and no geometry."""
    table = pa.table(
        {
            "id": pa.array(range(start, start + rows), type=pa.int64()),
            "val": pa.array([f"row_{i}" for i in range(start, start + rows)], type=pa.string()),
        }
    )
    pq.write_table(table, path)
    return path


def write_geo_parquet(
    path: Path, *, crs: str = "OGC:CRS84", rows: int = 5, origin: tuple[float, float] = (0.0, 0.0)
) -> Path:
    """Write a GeoParquet file of points typed with ``crs``, through DuckDB spatial.

    DuckDB writes the ``geo`` key with the CRS, and reads the column back as
    ``GEOMETRY('<crs>')``, which is what the backend copies into the table.
    """
    if not re.fullmatch(r"[A-Za-z]+:[A-Za-z0-9]+", crs):
        raise ValueError(f"not an authority:code CRS: {crs!r}")
    target = str(path).replace("'", "''")
    connection = duckdb.connect()
    try:
        connection.execute("LOAD spatial")
        connection.execute(
            # The type parameter and the COPY target must be literals; the CRS
            # is checked above and the path is quoted.
            f"COPY (SELECT i AS id, ST_Point(? + i, ? + i)::GEOMETRY('{crs}') AS geometry "  # noqa: S608
            f"FROM range(?) r(i)) TO '{target}' (FORMAT parquet)",
            [origin[0], origin[1], rows],
        )
    finally:
        connection.close()
    return path


#: A schema fingerprint for the ``id``/``val`` files of ``write_test_parquet``.
SCHEMA: SchemaFingerprint = {"columns": ["id", "val"], "types": {"id": "int64"}, "hash": "h1"}


def publish_rows(
    tmp_path: Path, collection: str, rows: int, *, message: str = "m"
) -> IcebergBackend:
    """Publish ``rows`` rows under the one asset key ``data.parquet`` and return the backend."""
    backend = IcebergBackend(catalog_root=tmp_path)
    (tmp_path / collection).mkdir(exist_ok=True)
    asset = write_test_parquet(tmp_path / collection / "data.parquet", rows=rows)
    backend.publish(collection, {"data.parquet": str(asset)}, SCHEMA, False, message)
    return backend
