"""Integration tests for collection-level asset workflow (Issue #250, ADR-0031).

Tests the complete workflow of adding collection-level vector assets,
verifying STAC metadata generation, and ensuring correct path structure.
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
        4. Verifying collection.json is created

        Per ADR-0031, collection-level assets should be organized as:
            demographics/
                census.parquet          # Asset at collection level
                collection.json
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

        # Verify: versions.json has correct href
        versions_file = collection_dir / "versions.json"
        assert versions_file.exists(), "versions.json should exist"

        with open(versions_file) as f:
            versions_data = json.load(f)

        assert len(versions_data["versions"]) == 1, "Should have one version"
        version = versions_data["versions"][0]

        # Check asset key and href (critical test for Bug #250)
        expected_href = "demographics/census.parquet"
        assert expected_href in version["assets"], (
            f"Asset key should be '{expected_href}', got {list(version['assets'].keys())}"
        )
        assert version["assets"][expected_href]["href"] == expected_href, (
            f"Asset href should be '{expected_href}' (no double nesting)"
        )

        # Verify: collection.json exists
        collection_json = collection_dir / "collection.json"
        assert collection_json.exists(), "collection.json should be created"

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
        are each tracked correctly without interference.
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

        # Check both assets have correct hrefs
        all_assets = {}
        for version in versions_data["versions"]:
            all_assets.update(version["assets"])

        assert "demographics/census.parquet" in all_assets, "Census asset should be tracked"
        assert "demographics/parcels.parquet" in all_assets, "Parcels asset should be tracked"

        # Verify no double nesting for either asset
        assert not (collection_dir / "demographics").exists(), (
            "Should NOT have nested demographics/demographics/ directory"
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

        # Collection-level asset has full path from catalog root
        assert "demographics/census.parquet" in all_assets, (
            "Collection-level asset should be tracked with correct path"
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
