"""Integration tests for `portolan add` with iceberg backend.

Tests the full CLI pipeline: add_cmd -> add_files -> finalize_datasets ->
IcebergBackend.publish(), using SQLite + local filesystem.
"""

import json

import pytest

from tests.iceberg.integration.conftest import (
    _POINTS_GEOJSON,
    _POINTS_GEOJSON_V2,
    invoke_add,
    load_test_catalog,
    place_geojson_in_collection,
)


@pytest.mark.integration
def test_add_geojson_creates_iceberg_table(initialized_iceberg_catalog, runner):
    """portolan add with iceberg backend creates an Iceberg table with version 1.0.0."""
    catalog_root = initialized_iceberg_catalog
    geojson = place_geojson_in_collection(catalog_root, "places")

    result = invoke_add(runner, catalog_root, geojson)
    assert result.exit_code == 0, f"Add failed: {result.output}"

    catalog = load_test_catalog(catalog_root)
    table = catalog.load_table("portolake.places")
    snap = table.current_snapshot()
    assert snap is not None
    assert snap.summary.additional_properties["portolake.version"] == "1.0.0"


@pytest.mark.integration
def test_add_same_collection_twice_increments_version(initialized_iceberg_catalog, runner):
    """Second add to same collection increments to 1.1.0."""
    catalog_root = initialized_iceberg_catalog

    # First add
    geojson1 = place_geojson_in_collection(
        catalog_root, "cities", _POINTS_GEOJSON, "cities_v1.geojson"
    )
    result = invoke_add(runner, catalog_root, geojson1)
    assert result.exit_code == 0, f"First add failed: {result.output}"

    # Second add (different file in same collection)
    geojson2 = place_geojson_in_collection(
        catalog_root, "cities", _POINTS_GEOJSON_V2, "cities_v2.geojson"
    )
    result = invoke_add(runner, catalog_root, geojson2)
    assert result.exit_code == 0, f"Second add failed: {result.output}"

    catalog = load_test_catalog(catalog_root)
    table = catalog.load_table("portolake.cities")
    snap = table.current_snapshot()
    assert snap.summary.additional_properties["portolake.version"] == "1.1.0"


@pytest.mark.integration
def test_add_creates_stac_collection_json(initialized_iceberg_catalog, runner):
    """After add, collection.json should exist with valid STAC structure."""
    catalog_root = initialized_iceberg_catalog
    geojson = place_geojson_in_collection(catalog_root, "boundaries")

    result = invoke_add(runner, catalog_root, geojson)
    assert result.exit_code == 0, f"Add failed: {result.output}"

    collection_json = catalog_root / "boundaries" / "collection.json"
    assert collection_json.exists(), "collection.json not created"

    coll = json.loads(collection_json.read_text())
    assert coll["type"] == "Collection"
    assert coll["id"] == "boundaries"


@pytest.mark.integration
def test_add_exit_code_zero_on_success(initialized_iceberg_catalog, runner):
    """CLI exits with code 0 on successful add."""
    catalog_root = initialized_iceberg_catalog
    geojson = place_geojson_in_collection(catalog_root, "demo")

    result = invoke_add(runner, catalog_root, geojson)
    assert result.exit_code == 0


@pytest.mark.integration
def test_add_stores_assets_in_version(initialized_iceberg_catalog, runner):
    """Published version should contain asset metadata."""
    catalog_root = initialized_iceberg_catalog
    geojson = place_geojson_in_collection(catalog_root, "assets_test")

    result = invoke_add(runner, catalog_root, geojson)
    assert result.exit_code == 0, f"Add failed: {result.output}"

    catalog = load_test_catalog(catalog_root)
    table = catalog.load_table("portolake.assets_test")
    snap = table.current_snapshot()

    assets_json = snap.summary.additional_properties.get("portolake.assets")
    assert assets_json is not None, "No assets stored in snapshot"
    assets = json.loads(assets_json)
    assert len(assets) > 0, "Assets dict is empty"
