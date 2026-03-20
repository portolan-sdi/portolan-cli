"""Tests for collection-level asset handling (ADR-0031).

Issue #250: portolan add creates incorrect asset paths for collection-level assets.
"""

import json

import pytest

from portolan_cli.dataset import add_dataset


@pytest.fixture
def initialized_catalog(tmp_path):
    """Create an initialized Portolan catalog structure (per ADR-0023)."""
    # Create .portolan for internal state
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()

    # Create config.yaml (required per ADR-0029)
    config_data = "# Portolan configuration\n"
    (portolan_dir / "config.yaml").write_text(config_data)

    # catalog.json at root level (per ADR-0023)
    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "test-catalog",
        "description": "Test catalog for collection-level assets",
        "links": [],
    }
    (tmp_path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    return tmp_path


@pytest.mark.unit
class TestCollectionLevelAssets:
    """Test collection-level asset handling per ADR-0031."""

    def test_collection_level_asset_href_is_correct(self, initialized_catalog, fixtures_dir):
        """Test that collection-level assets get correct href in versions.json.

        When a file is directly in a collection directory (collection/file.parquet),
        the href in versions.json should be 'collection/file.parquet', NOT
        'collection/collection/file.parquet' (double nesting).

        This is the RED phase - test should fail initially.
        """
        # Arrange: Create collection with file at collection level
        collection_dir = initialized_catalog / "demographics"
        collection_dir.mkdir()

        # Copy a test parquet file to collection level
        test_file = fixtures_dir / "simple.parquet"
        target_file = collection_dir / "census.parquet"
        target_file.write_bytes(test_file.read_bytes())

        # Act: Add the dataset
        add_dataset(
            catalog_root=initialized_catalog,
            path=target_file,
            collection_id="demographics",
            item_id=None,  # Let it auto-derive
            title=None,
            description=None,
        )

        # Assert: Check versions.json has correct href
        versions_file = collection_dir / "versions.json"
        assert versions_file.exists(), "versions.json should be created"

        with open(versions_file) as f:
            versions_data = json.load(f)

        # Get the first version
        assert len(versions_data["versions"]) > 0, "Should have at least one version"
        version = versions_data["versions"][0]

        # Check the asset href
        assert "demographics/census.parquet" in version["assets"], (
            f"Asset key should be 'demographics/census.parquet', got: {list(version['assets'].keys())}"
        )

        asset = version["assets"]["demographics/census.parquet"]

        # The critical assertion - href should NOT have double nesting
        assert asset["href"] == "demographics/census.parquet", (
            f"Expected 'demographics/census.parquet', got '{asset['href']}' (double nesting bug)"
        )

    def test_collection_level_asset_no_duplicate_directory(self, initialized_catalog, fixtures_dir):
        """Test that collection-level assets don't create duplicate nested directories.

        Bug: portolan add creates collection/collection/ subdirectory for collection-level assets.
        Expected: No subdirectory created when file is already at collection level.
        """
        # Arrange
        collection_dir = initialized_catalog / "demographics"
        collection_dir.mkdir()

        test_file = fixtures_dir / "simple.parquet"
        target_file = collection_dir / "census.parquet"
        target_file.write_bytes(test_file.read_bytes())

        # Act
        add_dataset(
            catalog_root=initialized_catalog,
            path=target_file,
            collection_id="demographics",
            item_id=None,
            title=None,
            description=None,
        )

        # Assert: Should NOT create demographics/demographics/ subdirectory
        duplicate_dir = collection_dir / "demographics"
        assert not duplicate_dir.exists(), (
            f"Should not create duplicate nested directory {duplicate_dir}"
        )

        # Should have: collection.json, versions.json, census.parquet
        # Should NOT have: demographics/ subdirectory
        contents = list(collection_dir.iterdir())
        content_names = {p.name for p in contents}

        assert "census.parquet" in content_names, "Original file should exist"
        assert "versions.json" in content_names, "versions.json should exist"
        # Note: collection.json might not exist yet - that's OK for this test

        # Key assertion: No duplicate directory
        assert "demographics" not in content_names, (
            "Should not have duplicate 'demographics' subdirectory"
        )

    def test_collection_level_asset_item_json_location(self, initialized_catalog, fixtures_dir):
        """Test that item.json is created in correct location for collection-level assets.

        For collection-level assets, if an item.json is created, it should use a synthetic
        item ID (not the collection name) to avoid path conflicts.
        """
        # Arrange
        collection_dir = initialized_catalog / "demographics"
        collection_dir.mkdir()

        test_file = fixtures_dir / "simple.parquet"
        target_file = collection_dir / "census.parquet"
        target_file.write_bytes(test_file.read_bytes())

        # Act
        add_dataset(
            catalog_root=initialized_catalog,
            path=target_file,
            collection_id="demographics",
            item_id=None,
            title=None,
            description=None,
        )

        # Assert: If item.json exists, it should NOT be at collection/collection/item.json
        # It should either:
        # 1. Not exist (collection-level assets don't need items), OR
        # 2. Be at collection/item-id/item.json where item-id != collection-name

        # Check for the buggy path
        buggy_item_json = collection_dir / "demographics" / "demographics.json"
        assert not buggy_item_json.exists(), (
            f"Should not create item.json at buggy path {buggy_item_json}"
        )

        # If there's an item directory, it should NOT be named same as collection
        item_dirs = [d for d in collection_dir.iterdir() if d.is_dir()]
        for item_dir in item_dirs:
            assert item_dir.name != "demographics", (
                f"Item directory should not have same name as collection: {item_dir}"
            )
