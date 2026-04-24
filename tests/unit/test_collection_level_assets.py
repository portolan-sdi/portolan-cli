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

        # Check the asset key - should be collection-relative (Issue #354)
        # For collection-level assets, key is filename only; href includes collection path
        assert "census.parquet" in version["assets"], (
            f"Asset key should be 'census.parquet' (collection-relative), got: {list(version['assets'].keys())}"
        )

        asset = version["assets"]["census.parquet"]

        # The critical assertion - href should include collection path (catalog-relative)
        assert asset["href"] == "demographics/census.parquet", (
            f"Expected href 'demographics/census.parquet', got '{asset['href']}'"
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

    def test_collection_level_vector_no_item_json(self, initialized_catalog, fixtures_dir):
        """Test that collection-level vector assets do NOT create item.json (ADR-0031).

        Per ADR-0031: Single vector files (GeoParquet, Shapefile, GeoPackage) are
        collection-level assets - no item.json, asset directly in collection.json.
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

        # Assert: NO item.json should exist anywhere in collection
        item_json_files = list(collection_dir.rglob("*.json"))
        item_json_names = [f.name for f in item_json_files]

        # Only collection.json and versions.json should exist
        assert "census.json" not in item_json_names, (
            "Should NOT create item.json for collection-level vector asset"
        )

        # Verify no item subdirectory was created
        subdirs = [d for d in collection_dir.iterdir() if d.is_dir()]
        assert len(subdirs) == 0 or all(d.name.startswith(".") for d in subdirs), (
            f"Should NOT create item subdirectory, found: {[d.name for d in subdirs]}"
        )

    def test_collection_level_vector_asset_in_collection_json(
        self, initialized_catalog, fixtures_dir
    ):
        """Test that collection-level vector assets appear in collection.json assets.

        Per ADR-0031: The asset should be in collection.json's "assets" field,
        NOT as an item link.
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

        # Assert: collection.json should have asset in "assets" field
        collection_json = collection_dir / "collection.json"
        assert collection_json.exists(), "collection.json should be created"

        with open(collection_json) as f:
            collection_data = json.load(f)

        # Check assets field has our data (key is file stem, not "data")
        assets = collection_data.get("assets", {})
        assert "census" in assets, (
            f"collection.json should have 'census' asset (file stem), got: {list(assets.keys())}"
        )
        assert assets["census"]["href"] == "./census.parquet", (
            f"Asset href should be './census.parquet', got: {assets['census']['href']}"
        )

        # Verify NO item links exist for this asset
        links = collection_data.get("links", [])
        item_links = [link for link in links if link.get("rel") == "item"]
        assert len(item_links) == 0, (
            f"Should NOT have item links for collection-level asset, got: {item_links}"
        )

    def test_explicit_item_id_forces_item_level(self, initialized_catalog, fixtures_dir):
        """Test that explicit --item-id forces item-level structure (traditional).

        When user explicitly provides item_id, the asset should be item-level
        (with item.json), not collection-level.
        """
        # Arrange
        collection_dir = initialized_catalog / "demographics"
        collection_dir.mkdir()

        test_file = fixtures_dir / "simple.parquet"
        target_file = collection_dir / "census.parquet"
        target_file.write_bytes(test_file.read_bytes())

        # Act: Explicitly provide item_id
        add_dataset(
            catalog_root=initialized_catalog,
            path=target_file,
            collection_id="demographics",
            item_id="census-2020",  # Explicit item ID
            title=None,
            description=None,
        )

        # Assert: item.json SHOULD exist when item_id is explicit
        # The item.json location depends on implementation - it may be in
        # a subdirectory or at collection level. Key is that it exists.
        item_json_files = list(collection_dir.rglob("census-2020.json"))
        assert len(item_json_files) == 1, (
            "Should create item.json when explicit item_id is provided"
        )

        # And collection.json should have item link, not direct asset
        collection_json = collection_dir / "collection.json"
        with open(collection_json) as f:
            collection_data = json.load(f)

        links = collection_data.get("links", [])
        item_links = [link for link in links if link.get("rel") == "item"]
        assert len(item_links) == 1, "Should have item link when explicit item_id is provided"
