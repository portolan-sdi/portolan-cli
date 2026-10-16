"""The table's columns come from the published file, through the full CLI flow.

`portolan add` with the iceberg backend used to rewrite the rows and add a
`geohash_N` column and `bbox_*` columns. The backend now registers the published
GeoParquet, so the table holds that file's own columns (issue #938).

The file still carries a `bbox` covering column, because geoparquet-io writes
one. That is the column `formats.md` asks for, and a reader prunes row groups
with it.
"""

from __future__ import annotations

import pytest

from tests.iceberg.integration.conftest import (
    invoke_add,
    load_test_catalog,
    place_geojson_in_collection,
)


@pytest.mark.integration
def test_add_geojson_adds_no_derived_columns(initialized_iceberg_catalog, runner):
    """No geohash and no bbox_* columns, because nothing rewrites the file."""
    catalog_root = initialized_iceberg_catalog
    geojson = place_geojson_in_collection(catalog_root, "spatial_test")

    result = invoke_add(runner, catalog_root, geojson)
    assert result.exit_code == 0, f"Add failed: {result.output}"

    catalog = load_test_catalog(catalog_root)
    table = catalog.load_table("portolake.spatial_test")
    field_names = {f.name for f in table.schema().fields}

    assert not [n for n in field_names if n.startswith("geohash_")], field_names
    assert not [n for n in field_names if n.startswith("bbox_")], field_names


@pytest.mark.integration
def test_add_geojson_keeps_the_covering_bbox_column(initialized_iceberg_catalog, runner):
    """geoparquet-io writes a bbox covering column, and registering keeps it."""
    catalog_root = initialized_iceberg_catalog
    geojson = place_geojson_in_collection(catalog_root, "bbox_vals")

    result = invoke_add(runner, catalog_root, geojson)
    assert result.exit_code == 0, f"Add failed: {result.output}"

    catalog = load_test_catalog(catalog_root)
    table = catalog.load_table("portolake.bbox_vals")
    arrow_table = table.scan().to_arrow()

    assert arrow_table.num_rows > 0
    assert "bbox" in arrow_table.column_names
    assert arrow_table.column("bbox").null_count == 0
