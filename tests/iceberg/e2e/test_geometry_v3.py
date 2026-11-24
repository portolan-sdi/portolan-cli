"""The table is format version 3 and its geometry column carries the CRS of the file.

P4 of the Iceberg specification asks for a typed geometry column. The
backend copies the type DuckDB reports for the GeoParquet file, so a
projected file lands as ``geometry(EPSG:25830)`` and a CRS84 file as
``geometry(OGC:CRS84)``, in the unquoted form of the Iceberg specification.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from portolan_cli.backends.iceberg.backend import IcebergBackend
from tests.iceberg.e2e.conftest import write_geo_parquet

if TYPE_CHECKING:
    from portolan_cli.backends.protocol import SchemaFingerprint
    from tests.iceberg_stack import IcebergStack

pytestmark = pytest.mark.e2e

SCHEMA: SchemaFingerprint = {
    "columns": ["id", "geometry"],
    "types": {"geometry": "geometry"},
    "hash": "g1",
}
REALDATA = Path(__file__).resolve().parents[2] / "fixtures" / "realdata" / "philadelphia-housing"


def _geometry_type(stack: IcebergStack, collection: str) -> str:
    fields = stack.metadata(collection)["schemas"][-1]["fields"]
    return next(str(f["type"]) for f in fields if f["name"] == "geometry")


def test_a_projected_file_gives_a_geometry_column_with_its_crs(
    iceberg_stack: IcebergStack, tmp_path: Path
) -> None:
    (tmp_path / "utm").mkdir()
    asset = write_geo_parquet(
        tmp_path / "utm" / "utm.parquet", crs="EPSG:25830", origin=(440000, 4470000)
    )

    IcebergBackend(catalog_root=tmp_path).publish(
        "utm", {"utm.parquet": str(asset)}, SCHEMA, False, "utm"
    )

    assert iceberg_stack.metadata("utm")["format-version"] == 3
    assert _geometry_type(iceberg_stack, "utm") == "geometry(EPSG:25830)"
    assert iceberg_stack.rows("utm", "SELECT ST_X(geometry) FROM t ORDER BY id LIMIT 1") == [
        (440000.0,)
    ]


@pytest.mark.realdata
def test_a_crs84_file_with_null_geometries_keeps_every_row(
    iceberg_stack: IcebergStack, tmp_path: Path
) -> None:
    """501 Philadelphia housing rows, 25 of them with no geometry."""
    (tmp_path / "housing").mkdir()
    source = REALDATA / "affordable_housing.parquet"
    target = tmp_path / "housing" / "affordable_housing.parquet"
    target.write_bytes(source.read_bytes())

    IcebergBackend(catalog_root=tmp_path).publish(
        "housing",
        {"affordable_housing.parquet": str(target)},
        SCHEMA,
        False,
        "housing",
    )

    # OGC:CRS84 is the default CRS of the Iceberg geometry type, and a catalog
    # may store it implicit or explicit. Both forms mean the same CRS.
    assert _geometry_type(iceberg_stack, "housing") in ("geometry", "geometry(OGC:CRS84)")
    assert iceberg_stack.row_count("housing") == 501
    assert iceberg_stack.rows("housing", "SELECT count(*) FROM t WHERE geometry IS NULL") == [(25,)]


def test_a_bbox_filter_over_two_disjoint_files_answers_the_rows_of_one(
    iceberg_stack: IcebergStack, tmp_path: Path
) -> None:
    """Two files with disjoint extents in one publish: a bbox filter returns one file's rows.

    DuckDB writes the rows of one publish into one data file, so the manifest
    bounds that let a reader skip a file show up across publishes, not within
    one. The read-side probe of 2026-10-29 saw one of two files read.
    """
    (tmp_path / "two").mkdir()
    near = write_geo_parquet(tmp_path / "two" / "near.parquet", rows=10, origin=(0, 0))
    far = write_geo_parquet(tmp_path / "two" / "far.parquet", rows=10, origin=(100, 100))

    IcebergBackend(catalog_root=tmp_path).publish(
        "two", {"near.parquet": str(near), "far.parquet": str(far)}, SCHEMA, False, "two"
    )

    assert iceberg_stack.row_count("two") == 20
    assert iceberg_stack.rows(
        "two", "SELECT count(*) FROM t WHERE geometry && ST_MakeEnvelope(99, 99, 111, 111)"
    ) == [(10,)]


def test_partition_keys_make_a_partitioned_table(
    iceberg_stack: IcebergStack, tmp_path: Path
) -> None:
    """A hive layout with ``partition:keys`` in collection.json gets an identity spec."""
    import json

    collection = tmp_path / "parts"
    (collection / "region=a").mkdir(parents=True)
    (collection / "region=b").mkdir(parents=True)
    (collection / "collection.json").write_text(
        json.dumps({"partition:keys": [{"name": "region", "type": "string"}]}), encoding="utf-8"
    )
    a = write_geo_parquet(collection / "region=a" / "part.parquet", rows=3, origin=(0, 0))
    b = write_geo_parquet(collection / "region=b" / "part.parquet", rows=4, origin=(50, 50))

    IcebergBackend(catalog_root=tmp_path).publish(
        "parts",
        {"region=a/part.parquet": str(a), "region=b/part.parquet": str(b)},
        SCHEMA,
        False,
        "p",
    )

    metadata = iceberg_stack.metadata("parts")
    spec = next(
        s for s in metadata["partition-specs"] if s["spec-id"] == metadata["default-spec-id"]
    )
    assert [(f["name"], f["transform"]) for f in spec["fields"]] == [("region", "identity")]
    assert iceberg_stack.rows(
        "parts", "SELECT region, count(*) FROM t GROUP BY region ORDER BY region"
    ) == [
        ("a", 3),
        ("b", 4),
    ]
    data_files = [k for k in iceberg_stack.objects("parts", "data/") if k.endswith(".parquet")]
    assert any("/region=a/" in k for k in data_files) and any("/region=b/" in k for k in data_files)
