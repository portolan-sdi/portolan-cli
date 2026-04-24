"""Integration tests for collection-level asset workflow (Issue #250, ADR-0031, Issue #364).

Tests the complete workflow of adding collection-level vector assets,
verifying STAC metadata generation, and ensuring correct path structure.

Per ADR-0031:
- Single vector files (GeoParquet, Shapefile, GeoPackage) are collection-level assets
- No item.json created - asset goes directly in collection.json
- items.parquet is NOT generated (no items to index)
"""

import json
from pathlib import Path

import pytest

from portolan_cli.dataset import add_dataset


def _assert_no_item_json(collection_dir: Path) -> None:
    """Assert no item.json files exist in collection (Issue #364)."""
    item_json_files = [
        f
        for f in collection_dir.rglob("*.json")
        if f.name not in ("collection.json", "versions.json", "catalog.json")
    ]
    assert len(item_json_files) == 0, (
        f"Should NOT create item.json for collection-level vector, found: {[f.name for f in item_json_files]}"
    )


def _assert_collection_assets(collection_dir: Path, expected_assets: dict[str, str]) -> None:
    """Assert collection.json has expected assets with correct hrefs."""
    collection_json = collection_dir / "collection.json"
    with open(collection_json) as f:
        collection_data = json.load(f)

    assets = collection_data.get("assets", {})
    assert len(assets) == len(expected_assets), (
        f"Expected {len(expected_assets)} assets, got {len(assets)}: {list(assets.keys())}"
    )

    for key, expected_href in expected_assets.items():
        assert key in assets, f"Missing '{key}' asset, got: {list(assets.keys())}"
        assert assets[key]["href"] == expected_href, (
            f"{key} href should be '{expected_href}', got: {assets[key]['href']}"
        )

    # Verify NO item links
    links = collection_data.get("links", [])
    item_links = [link for link in links if link.get("rel") == "item"]
    assert len(item_links) == 0, f"Should NOT have item links, got: {item_links}"


@pytest.mark.integration
class TestCollectionLevelAssetWorkflow:
    """Test complete workflow for collection-level vector assets."""

    def test_end_to_end_collection_level_asset(
        self, fresh_catalog_no_versions, fixtures_dir, tmp_path
    ):
        """Test complete workflow: add file → verify structure → verify metadata.

        This integration test covers:
        1. Adding a collection-level vector file (demographics/census.parquet)
        2. Verifying no double-nested directories (demographics/demographics/)
        3. Verifying versions.json has correct href (demographics/census.parquet)
        4. Verifying collection.json has asset in "assets" field (Issue #364)
        5. Verifying NO item.json exists (Issue #364)

        Per ADR-0031, collection-level assets should be organized as:
            demographics/
                census.parquet          # Asset at collection level
                collection.json         # Has assets.data pointing to census.parquet
                versions.json
        """
        # Setup: Create collection directory
        collection_dir = fresh_catalog_no_versions / "demographics"
        collection_dir.mkdir()

        # Copy test fixture to collection directory
        test_file = fixtures_dir / "simple.parquet"
        target_file = collection_dir / "census.parquet"
        target_file.write_bytes(test_file.read_bytes())

        # Execute: Add collection-level asset
        result = add_dataset(
            catalog_root=fresh_catalog_no_versions,
            path=target_file,
            collection_id="demographics",
            item_id=None,  # Auto-derive from file stem
            title="Census Data 2020",
            description="Population demographics from 2020 census",
        )

        # Verify: No double nesting (no demographics/demographics/ directory)
        assert not (collection_dir / "demographics").exists(), (
            "Should NOT have nested demographics/demographics/ directory (Bug #250)"
        )
        assert target_file.exists(), "Asset file should exist at collection level"

        # Verify: NO item.json exists (Issue #364)
        _assert_no_item_json(collection_dir)

        # Verify: versions.json has correct href
        versions_file = collection_dir / "versions.json"
        assert versions_file.exists(), "versions.json should exist"

        with open(versions_file) as f:
            versions_data = json.load(f)

        assert len(versions_data["versions"]) == 1, "Should have one version"
        version = versions_data["versions"][0]

        # Asset key is collection-relative, href is catalog-relative (Issue #354)
        assert "census.parquet" in version["assets"]
        assert version["assets"]["census.parquet"]["href"] == "demographics/census.parquet"

        # Verify collection.json has asset (Issue #364)
        _assert_collection_assets(collection_dir, {"census": "./census.parquet"})

        # Verify: Result contains correct info
        assert result.item_id == "census", (
            f"Item ID should be 'census' (from file stem), got '{result.item_id}'"
        )
        assert result.collection_id == "demographics"

    def test_multiple_collection_level_assets_same_collection(
        self, fresh_catalog_no_versions, fixtures_dir
    ):
        """Test adding multiple collection-level assets to same collection.

        Verifies that multiple vector files in the same collection directory
        are each tracked correctly without interference, and both appear
        in collection.json assets (Issue #364).
        """
        collection_dir = fresh_catalog_no_versions / "demographics"
        collection_dir.mkdir()

        # Add first asset
        test_file = fixtures_dir / "simple.parquet"
        census_file = collection_dir / "census.parquet"
        census_file.write_bytes(test_file.read_bytes())

        add_dataset(
            catalog_root=fresh_catalog_no_versions,
            path=census_file,
            collection_id="demographics",
            item_id=None,
            title="Census Data",
            description="Population census",
        )

        # Add second asset
        parcels_file = collection_dir / "parcels.parquet"
        parcels_file.write_bytes(test_file.read_bytes())

        add_dataset(
            catalog_root=fresh_catalog_no_versions,
            path=parcels_file,
            collection_id="demographics",
            item_id=None,
            title="Parcel Data",
            description="Land parcels",
        )

        # Verify versions.json tracks both assets
        versions_file = collection_dir / "versions.json"
        with open(versions_file) as f:
            versions_data = json.load(f)

        assert len(versions_data["versions"]) == 2, "Should have two versions"

        all_assets = {}
        for version in versions_data["versions"]:
            all_assets.update(version["assets"])

        assert "census.parquet" in all_assets
        assert "parcels.parquet" in all_assets
        assert all_assets["census.parquet"]["href"] == "demographics/census.parquet"
        assert all_assets["parcels.parquet"]["href"] == "demographics/parcels.parquet"

        # Verify no double nesting
        assert not (collection_dir / "demographics").exists()

        # Verify NO item.json and correct collection.json assets
        _assert_no_item_json(collection_dir)
        _assert_collection_assets(
            collection_dir, {"census": "./census.parquet", "parcels": "./parcels.parquet"}
        )

        # Verify items.parquet does NOT exist
        assert not (collection_dir / "items.parquet").exists()

    def test_mixed_collection_and_item_level_assets(self, fresh_catalog_no_versions, fixtures_dir):
        """Test collection with both collection-level and item-level assets.

        Verifies that a collection can have:
        - Collection-level assets (demographics/census.parquet)
        - Item-level assets (demographics/survey/responses.parquet)

        And both are tracked correctly without path conflicts.
        """
        collection_dir = fresh_catalog_no_versions / "demographics"
        collection_dir.mkdir()

        # Add collection-level asset
        test_file = fixtures_dir / "simple.parquet"
        census_file = collection_dir / "census.parquet"
        census_file.write_bytes(test_file.read_bytes())

        add_dataset(
            catalog_root=fresh_catalog_no_versions,
            path=census_file,
            collection_id="demographics",
            item_id=None,
            title="Census Data",
            description="Population census",
        )

        # Add item-level asset (traditional organization)
        item_dir = collection_dir / "survey"
        item_dir.mkdir()
        survey_file = item_dir / "responses.parquet"
        survey_file.write_bytes(test_file.read_bytes())

        add_dataset(
            catalog_root=fresh_catalog_no_versions,
            path=survey_file,
            collection_id="demographics",
            item_id="survey",
            title="Survey Responses",
            description="Survey data",
        )

        # Verify versions.json tracks both correctly
        versions_file = collection_dir / "versions.json"
        with open(versions_file) as f:
            versions_data = json.load(f)

        all_assets = {}
        for version in versions_data["versions"]:
            all_assets.update(version["assets"])

        # Collection-level asset key is filename only (per Issue #354)
        assert "census.parquet" in all_assets, (
            "Collection-level asset key should be filename (collection-relative)"
        )
        assert all_assets["census.parquet"]["href"] == "demographics/census.parquet", (
            "Collection-level asset href should be catalog-relative"
        )

        # Item-level asset key is relative to item (not full path from catalog root)
        assert "survey/responses.parquet" in all_assets, (
            "Item-level asset should be tracked (key is relative to item)"
        )

        # Verify the href is correct for item-level asset
        item_asset = all_assets["survey/responses.parquet"]
        assert item_asset["href"] == "demographics/survey/responses.parquet", (
            "Item-level asset href should be full path from catalog root"
        )

        # Verify directory structure
        assert census_file.exists(), "Collection-level file exists at collection root"
        assert survey_file.exists(), "Item-level file exists in item directory"
        assert not (collection_dir / "demographics").exists(), (
            "Should NOT have double-nested demographics/demographics/ directory"
        )
