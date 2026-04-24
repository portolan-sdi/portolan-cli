"""Integration tests for collection-level asset workflow (Issue #250, ADR-0031, Issue #364).

Tests the complete workflow of adding collection-level vector assets,
verifying STAC metadata generation, and ensuring correct path structure.

Per ADR-0031:
- Single vector files (GeoParquet, Shapefile, GeoPackage) are collection-level assets
- No item.json created - asset goes directly in collection.json
- items.parquet is NOT generated (no items to index)
"""

import json

import pytest

from portolan_cli.dataset import add_dataset


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

        # Verify: Asset file exists at collection level
        assert target_file.exists(), "Asset file should exist at collection level"

        # Verify: NO item.json exists (Issue #364)
        item_json_files = [
            f
            for f in collection_dir.rglob("*.json")
            if f.name not in ("collection.json", "versions.json", "catalog.json")
        ]
        assert len(item_json_files) == 0, (
            f"Should NOT create item.json for collection-level vector (Issue #364), "
            f"found: {[f.name for f in item_json_files]}"
        )

        # Verify: versions.json has correct href
        versions_file = collection_dir / "versions.json"
        assert versions_file.exists(), "versions.json should exist"

        with open(versions_file) as f:
            versions_data = json.load(f)

        assert len(versions_data["versions"]) == 1, "Should have one version"
        version = versions_data["versions"][0]

        # Check asset key and href (critical test for Bug #250, updated per Issue #354)
        # Asset key is collection-relative (filename only for collection-level assets)
        # href is catalog-relative (includes collection path)
        expected_key = "census.parquet"
        expected_href = "demographics/census.parquet"
        assert expected_key in version["assets"], (
            f"Asset key should be '{expected_key}' (collection-relative), got {list(version['assets'].keys())}"
        )
        assert version["assets"][expected_key]["href"] == expected_href, (
            f"Asset href should be '{expected_href}' (catalog-relative, no double nesting)"
        )

        # Verify: collection.json has asset in "assets" field (Issue #364)
        collection_json = collection_dir / "collection.json"
        assert collection_json.exists(), "collection.json should be created"

        with open(collection_json) as f:
            collection_data = json.load(f)

        # Asset should be in collection.assets with file stem as key
        assets = collection_data.get("assets", {})
        assert "census" in assets, (
            f"collection.json should have 'census' asset (file stem), got: {list(assets.keys())}"
        )
        assert assets["census"]["href"] == "./census.parquet", (
            f"Asset href should be './census.parquet', got: {assets['census']['href']}"
        )

        # Verify NO item links
        links = collection_data.get("links", [])
        item_links = [link for link in links if link.get("rel") == "item"]
        assert len(item_links) == 0, (
            f"Should NOT have item links for collection-level asset (Issue #364), got: {item_links}"
        )

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

        # Verify both assets are tracked correctly in versions.json
        versions_file = collection_dir / "versions.json"
        with open(versions_file) as f:
            versions_data = json.load(f)

        # Should have two versions (one per asset)
        assert len(versions_data["versions"]) == 2, "Should have two versions for two assets"

        # Check both assets have correct keys and hrefs (per Issue #354)
        # Keys are collection-relative (filename only), hrefs are catalog-relative
        all_assets = {}
        for version in versions_data["versions"]:
            all_assets.update(version["assets"])

        assert "census.parquet" in all_assets, "Census asset should be tracked (key is filename)"
        assert "parcels.parquet" in all_assets, "Parcels asset should be tracked (key is filename)"
        assert all_assets["census.parquet"]["href"] == "demographics/census.parquet"
        assert all_assets["parcels.parquet"]["href"] == "demographics/parcels.parquet"

        # Verify no double nesting for either asset
        assert not (collection_dir / "demographics").exists(), (
            "Should NOT have nested demographics/demographics/ directory"
        )

        # Verify NO item.json files exist (Issue #364)
        item_json_files = [
            f
            for f in collection_dir.rglob("*.json")
            if f.name not in ("collection.json", "versions.json", "catalog.json")
        ]
        assert len(item_json_files) == 0, (
            f"Should NOT create item.json for collection-level vectors, "
            f"found: {[f.name for f in item_json_files]}"
        )

        # Verify collection.json has both assets (Issue #364)
        collection_json = collection_dir / "collection.json"
        with open(collection_json) as f:
            collection_data = json.load(f)

        # Both assets should be present with distinct keys (file stems)
        assets = collection_data.get("assets", {})
        assert len(assets) == 2, (
            f"collection.json should have 2 assets (census + parcels), got: {list(assets.keys())}"
        )

        # Verify distinct keys derived from file stems
        assert "census" in assets, f"Missing 'census' asset, got: {list(assets.keys())}"
        assert "parcels" in assets, f"Missing 'parcels' asset, got: {list(assets.keys())}"

        # Verify hrefs point to correct files
        assert assets["census"]["href"] == "./census.parquet", (
            f"census href should be './census.parquet', got: {assets['census']['href']}"
        )
        assert assets["parcels"]["href"] == "./parcels.parquet", (
            f"parcels href should be './parcels.parquet', got: {assets['parcels']['href']}"
        )

        # Verify NO item links
        links = collection_data.get("links", [])
        item_links = [link for link in links if link.get("rel") == "item"]
        assert len(item_links) == 0, (
            f"Should NOT have item links for collection-level assets, got: {item_links}"
        )

        # Verify items.parquet does NOT exist (Issue #364)
        items_parquet = collection_dir / "items.parquet"
        assert not items_parquet.exists(), (
            "items.parquet should NOT exist for collection with only collection-level assets"
        )

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
