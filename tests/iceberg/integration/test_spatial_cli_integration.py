"""Integration tests for spatial columns through the full CLI flow.

Verifies that `portolan add` with the iceberg backend adds geohash and
bbox columns to the Iceberg table when the input has geometry.
"""

import pytest

from tests.iceberg.integration.conftest import (
    invoke_add,
    load_test_catalog,
    place_geojson_in_collection,
)


@pytest.mark.integration
def test_add_geojson_adds_spatial_columns(initialized_iceberg_catalog, runner):
    """Adding GeoJSON should produce an Iceberg table with geohash and bbox columns."""
    catalog_root = initialized_iceberg_catalog
    geojson = place_geojson_in_collection(catalog_root, "spatial_test")

    result = invoke_add(runner, catalog_root, geojson)
    assert result.exit_code == 0, f"Add failed: {result.output}"

    catalog = load_test_catalog(catalog_root)
    table = catalog.load_table("portolake.spatial_test")
    field_names = {f.name for f in table.schema().fields}

    has_geohash = any(name.startswith("geohash_") for name in field_names)
    assert has_geohash, f"Expected geohash column, got fields: {field_names}"

    for bbox_col in ("bbox_xmin", "bbox_ymin", "bbox_xmax", "bbox_ymax"):
        assert bbox_col in field_names, f"Missing {bbox_col} column, got: {field_names}"


@pytest.mark.integration
def test_add_geojson_bbox_columns_have_values(initialized_iceberg_catalog, runner):
    """Bbox columns should have non-null values after add."""
    catalog_root = initialized_iceberg_catalog
    geojson = place_geojson_in_collection(catalog_root, "bbox_vals")

    result = invoke_add(runner, catalog_root, geojson)
    assert result.exit_code == 0, f"Add failed: {result.output}"

    catalog = load_test_catalog(catalog_root)
    table = catalog.load_table("portolake.bbox_vals")
    arrow_table = table.scan().to_arrow()

    assert arrow_table.num_rows > 0
    assert arrow_table.column("bbox_xmin").null_count == 0
    assert arrow_table.column("bbox_ymin").null_count == 0
    assert arrow_table.column("bbox_xmax").null_count == 0
    assert arrow_table.column("bbox_ymax").null_count == 0
