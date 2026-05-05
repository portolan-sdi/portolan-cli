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


# =============================================================================
# Issue #383: Non-geospatial parquet files with collection-level geo companions
# =============================================================================


@pytest.mark.unit
class TestCollectionLevelNonGeoCompanions:
    """Tests for issue #383: non-geo parquet files with collection-level geo companions.

    Bug: Non-geo parquet files fail to track when their companion geo file is
    collection-level because `source_to_item_dir` doesn't include collection-level
    sources.

    Fix: Add `source_to_collection_dir` mapping for collection-level sources.
    """

    def test_process_deferred_accepts_collection_dir_mapping(
        self, initialized_catalog, fixtures_dir
    ):
        """_process_deferred_non_geo_files accepts source_to_collection_dir parameter.

        TDD: Verify the function signature includes the new parameter.
        """
        from portolan_cli.dataset import _process_deferred_non_geo_files

        # Set up minimal arguments
        deferred_non_geo: list = []
        source_to_item_dir: dict = {}
        source_to_collection_dir: dict = {}  # NEW parameter
        skipped: list = []
        failures: list = []

        # Should not raise TypeError for unexpected keyword argument
        _process_deferred_non_geo_files(
            deferred_non_geo=deferred_non_geo,
            source_to_item_dir=source_to_item_dir,
            source_to_collection_dir=source_to_collection_dir,
            catalog_root=initialized_catalog,
            skipped=skipped,
            failures=failures,
        )

    def test_non_geo_with_collection_level_geo_is_tracked(self, initialized_catalog, fixtures_dir):
        """Non-geo parquet tracks when geo companion is collection-level (fix #383).

        When a geo file is added as a collection-level asset, its non-geo
        companion parquet should also be registered as a collection-level asset
        in collection.json.
        """
        from portolan_cli.dataset import _process_deferred_non_geo_files

        # Set up collection with collection.json
        collection_dir = initialized_catalog / "my-collection"
        collection_dir.mkdir()

        collection_json = collection_dir / "collection.json"
        collection_json.write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "stac_version": "1.0.0",
                    "id": "my-collection",
                    "description": "Test collection",
                    "extent": {
                        "spatial": {"bbox": [[0, 0, 1, 1]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "license": "proprietary",
                    "links": [],
                    "assets": {
                        "data": {
                            "href": "./data.parquet",
                            "type": "application/vnd.apache.parquet",
                            "roles": ["data"],
                        }
                    },
                }
            )
        )

        versions_json = collection_dir / "versions.json"
        versions_json.write_text(json.dumps({"assets": {}, "versions": []}))

        # Non-geo file in collection directory (same location as geo file)
        non_geo_file = collection_dir / "stats.parquet"
        non_geo_file.write_bytes(b"fake parquet")

        # Mappings: collection-level source
        source_to_item_dir: dict = {}
        source_to_collection_dir = {collection_dir: (collection_dir, "my-collection")}
        deferred_non_geo = [(non_geo_file, collection_dir, "my-collection")]
        skipped: list = []
        failures: list = []

        _process_deferred_non_geo_files(
            deferred_non_geo=deferred_non_geo,
            source_to_item_dir=source_to_item_dir,
            source_to_collection_dir=source_to_collection_dir,
            catalog_root=initialized_catalog,
            skipped=skipped,
            failures=failures,
        )

        # Verify: non-geo file was tracked (not failed)
        assert non_geo_file in skipped
        assert len(failures) == 0

        # Verify: asset added to collection.json
        updated = json.loads(collection_json.read_text())
        assert "stats" in updated["assets"], (
            f"Non-geo asset 'stats' should be in collection.json, got: {list(updated['assets'].keys())}"
        )
        assert updated["assets"]["stats"]["href"] == "./stats.parquet"

    def test_non_geo_without_any_companion_warns(self, initialized_catalog):
        """Non-geo file without geo companion logs warning (existing behavior)."""
        from unittest.mock import patch

        from portolan_cli.dataset import _process_deferred_non_geo_files

        # Source dir with no geo companion
        orphan_dir = initialized_catalog / "orphan"
        orphan_dir.mkdir()
        non_geo_file = orphan_dir / "lonely.parquet"
        non_geo_file.write_bytes(b"fake parquet")

        # Empty mappings - no companion found
        source_to_item_dir: dict = {}
        source_to_collection_dir: dict = {}
        deferred_non_geo = [(non_geo_file, orphan_dir, "orphan")]
        skipped: list = []
        failures: list = []

        with patch("portolan_cli.dataset.logger") as mock_logger:
            _process_deferred_non_geo_files(
                deferred_non_geo=deferred_non_geo,
                source_to_item_dir=source_to_item_dir,
                source_to_collection_dir=source_to_collection_dir,
                catalog_root=initialized_catalog,
                skipped=skipped,
                failures=failures,
            )

        # Should log warning about no geo companion
        mock_logger.warning.assert_called_once()
        warning_msg = mock_logger.warning.call_args[0][0]
        assert "no geospatial file" in warning_msg.lower()

        # File is skipped (not failed)
        assert non_geo_file in skipped
        assert len(failures) == 0
