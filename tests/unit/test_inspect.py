"""Tests for the inspect module.

TDD-first tests for file, collection, and catalog inspection functionality.
These tests verify metadata extraction and version lookup for the info command.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    pass


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def catalog_with_tracked_file(tmp_path: Path, valid_points_parquet: Path) -> Path:
    """Create a catalog with a tracked GeoParquet file and versions.json."""
    import shutil

    # Create catalog structure
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()

    # Create catalog.json
    catalog_json = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "test-catalog",
        "description": "Test catalog",
        "links": [
            {"rel": "root", "href": "./catalog.json"},
            {"rel": "child", "href": "./demographics/collection.json"},
        ],
    }
    (catalog_root / "catalog.json").write_text(json.dumps(catalog_json, indent=2))

    # Create collection directory
    collection_dir = catalog_root / "demographics"
    collection_dir.mkdir()

    # Create collection.json
    collection_json = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "demographics",
        "description": "Test collection",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": [
            {"rel": "root", "href": "../catalog.json"},
            {"rel": "self", "href": "./collection.json"},
            {"rel": "item", "href": "./census/census.json"},
        ],
    }
    (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

    # Create item directory
    item_dir = collection_dir / "census"
    item_dir.mkdir()

    # Copy test parquet file
    dest_parquet = item_dir / "census.parquet"
    shutil.copy2(valid_points_parquet, dest_parquet)

    # Create item.json (STAC item)
    item_json = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "census",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "bbox": [-180, -90, 180, 90],
        "properties": {"datetime": "2024-01-01T00:00:00Z", "title": "Census Data"},
        "assets": {
            "data": {
                "href": "./census.parquet",
                "type": "application/x-parquet",
                "roles": ["data"],
            }
        },
        "links": [],
    }
    (item_dir / "census.json").write_text(json.dumps(item_json, indent=2))

    # Create versions.json to track the file
    versions_json = {
        "spec_version": "1.0.0",
        "current_version": "1.2.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-01T00:00:00Z",
                "breaking": False,
                "assets": {
                    "census.parquet": {
                        "sha256": "abc123",
                        "size_bytes": 1000,
                        "href": "demographics/census/census.parquet",
                    }
                },
                "changes": ["census.parquet"],
            },
            {
                "version": "1.2.0",
                "created": "2024-01-15T00:00:00Z",
                "breaking": False,
                "assets": {
                    "census.parquet": {
                        "sha256": "def456",
                        "size_bytes": 1500,
                        "href": "demographics/census/census.parquet",
                    }
                },
                "changes": ["census.parquet"],
            },
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_json, indent=2))

    return catalog_root


@pytest.fixture
def catalog_with_cog(tmp_path: Path, valid_rgb_cog: Path) -> Path:
    """Create a catalog with a tracked COG file and versions.json."""
    import shutil

    # Create catalog structure
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()

    # Create catalog.json
    catalog_json = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "test-catalog",
        "description": "Test catalog",
        "links": [
            {"rel": "root", "href": "./catalog.json"},
            {"rel": "child", "href": "./imagery/collection.json"},
        ],
    }
    (catalog_root / "catalog.json").write_text(json.dumps(catalog_json, indent=2))

    # Create collection directory
    collection_dir = catalog_root / "imagery"
    collection_dir.mkdir()

    # Create collection.json
    collection_json = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "imagery",
        "description": "Test imagery collection",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": [
            {"rel": "root", "href": "../catalog.json"},
            {"rel": "self", "href": "./collection.json"},
            {"rel": "item", "href": "./satellite/satellite.json"},
        ],
    }
    (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

    # Create item directory
    item_dir = collection_dir / "satellite"
    item_dir.mkdir()

    # Copy test COG file
    dest_cog = item_dir / "satellite.tif"
    shutil.copy2(valid_rgb_cog, dest_cog)

    # Create item.json (STAC item)
    item_json = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "satellite",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "bbox": [-180, -90, 180, 90],
        "properties": {"datetime": "2024-01-01T00:00:00Z", "title": "Satellite Image"},
        "assets": {
            "data": {
                "href": "./satellite.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
            }
        },
        "links": [],
    }
    (item_dir / "satellite.json").write_text(json.dumps(item_json, indent=2))

    # Create versions.json to track the file
    versions_json = {
        "spec_version": "1.0.0",
        "current_version": "2.0.0",
        "versions": [
            {
                "version": "2.0.0",
                "created": "2024-01-15T00:00:00Z",
                "breaking": False,
                "assets": {
                    "satellite.tif": {
                        "sha256": "xyz789",
                        "size_bytes": 50000,
                        "href": "imagery/satellite/satellite.tif",
                    }
                },
                "changes": ["satellite.tif"],
            }
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_json, indent=2))

    return catalog_root


# =============================================================================
# Test: File Info - GeoParquet
# =============================================================================


class TestFileInfoGeoParquet:
    """Tests for inspect_file with GeoParquet files."""

    @pytest.mark.unit
    def test_inspect_geoparquet_returns_metadata(self, catalog_with_tracked_file: Path) -> None:
        """Test that inspect_file extracts GeoParquet metadata."""
        from portolan_cli.inspect import inspect_file

        file_path = catalog_with_tracked_file / "demographics" / "census" / "census.parquet"
        result = inspect_file(file_path, catalog_root=catalog_with_tracked_file)

        assert result.format == "GeoParquet"
        # CRS may or may not be present depending on the test fixture
        # bbox and feature_count should always be present for valid GeoParquet
        assert result.bbox is not None  # Should have bbox
        assert result.feature_count is not None
        assert result.feature_count > 0

    @pytest.mark.unit
    def test_inspect_geoparquet_includes_version_when_tracked(
        self, catalog_with_tracked_file: Path
    ) -> None:
        """Test that inspect_file includes version from versions.json."""
        from portolan_cli.inspect import inspect_file

        file_path = catalog_with_tracked_file / "demographics" / "census" / "census.parquet"
        result = inspect_file(file_path, catalog_root=catalog_with_tracked_file)

        assert result.version == "v1.2.0"

    @pytest.mark.unit
    def test_inspect_geoparquet_no_version_when_not_tracked(
        self, valid_points_parquet: Path, tmp_path: Path
    ) -> None:
        """Test that version is None when file is not tracked."""
        from portolan_cli.inspect import inspect_file

        result = inspect_file(valid_points_parquet, catalog_root=tmp_path)

        assert result.version is None

    @pytest.mark.unit
    def test_inspect_nonexistent_file_raises(self, tmp_path: Path) -> None:
        """Test that inspect_file raises FileNotFoundError for missing files."""
        from portolan_cli.inspect import inspect_file

        with pytest.raises(FileNotFoundError):
            inspect_file(tmp_path / "nonexistent.parquet", catalog_root=tmp_path)


# =============================================================================
# Test: File Info - COG
# =============================================================================


class TestFileInfoCOG:
    """Tests for inspect_file with COG files."""

    @pytest.mark.unit
    def test_inspect_cog_returns_metadata(self, catalog_with_cog: Path) -> None:
        """Test that inspect_file extracts COG metadata."""
        from portolan_cli.inspect import inspect_file

        file_path = catalog_with_cog / "imagery" / "satellite" / "satellite.tif"
        result = inspect_file(file_path, catalog_root=catalog_with_cog)

        assert result.format == "COG"
        assert result.crs is not None
        assert result.bbox is not None
        # COGs don't have feature_count, they have dimensions
        assert result.width is not None
        assert result.height is not None

    @pytest.mark.unit
    def test_inspect_cog_includes_version_when_tracked(self, catalog_with_cog: Path) -> None:
        """Test that inspect_file includes version from versions.json for COG."""
        from portolan_cli.inspect import inspect_file

        file_path = catalog_with_cog / "imagery" / "satellite" / "satellite.tif"
        result = inspect_file(file_path, catalog_root=catalog_with_cog)

        assert result.version == "v2.0.0"


# =============================================================================
# Test: Collection Info
# =============================================================================


class TestCollectionInfo:
    """Tests for inspect_collection."""

    @pytest.mark.unit
    def test_inspect_collection_returns_metadata(self, catalog_with_tracked_file: Path) -> None:
        """Test that inspect_collection extracts collection metadata."""
        from portolan_cli.inspect import inspect_collection

        collection_path = catalog_with_tracked_file / "demographics"
        result = inspect_collection(collection_path)

        assert result.collection_id == "demographics"
        assert result.description is not None
        assert result.item_count >= 1

    @pytest.mark.unit
    def test_inspect_collection_shows_total_size(self, catalog_with_tracked_file: Path) -> None:
        """Test that inspect_collection calculates total size."""
        from portolan_cli.inspect import inspect_collection

        collection_path = catalog_with_tracked_file / "demographics"
        result = inspect_collection(collection_path)

        assert result.total_size_bytes > 0

    @pytest.mark.unit
    def test_inspect_nonexistent_collection_raises(self, tmp_path: Path) -> None:
        """Test that inspect_collection raises for missing collection."""
        from portolan_cli.inspect import inspect_collection

        with pytest.raises(FileNotFoundError):
            inspect_collection(tmp_path / "nonexistent")


# =============================================================================
# Test: Catalog Info
# =============================================================================


class TestCatalogInfo:
    """Tests for inspect_catalog."""

    @pytest.mark.unit
    def test_inspect_catalog_returns_metadata(self, catalog_with_tracked_file: Path) -> None:
        """Test that inspect_catalog extracts catalog metadata."""
        from portolan_cli.inspect import inspect_catalog

        result = inspect_catalog(catalog_with_tracked_file)

        assert result.catalog_id == "test-catalog"
        assert result.description is not None
        assert result.collection_count >= 1

    @pytest.mark.unit
    def test_inspect_catalog_no_catalog_json_raises(self, tmp_path: Path) -> None:
        """Test that inspect_catalog raises for directory without catalog.json."""
        from portolan_cli.inspect import inspect_catalog

        with pytest.raises(FileNotFoundError):
            inspect_catalog(tmp_path)


# =============================================================================
# Test: FileInfo Output Format (ADR-0022 compliant)
# =============================================================================


class TestFileInfoOutputFormat:
    """Tests for FileInfo output formatting."""

    @pytest.mark.unit
    def test_file_info_to_dict_geoparquet(self, catalog_with_tracked_file: Path) -> None:
        """Test that FileInfo.to_dict returns correct structure for GeoParquet."""
        from portolan_cli.inspect import inspect_file

        file_path = catalog_with_tracked_file / "demographics" / "census" / "census.parquet"
        result = inspect_file(file_path, catalog_root=catalog_with_tracked_file)
        data = result.to_dict()

        assert "format" in data
        assert "crs" in data
        assert "bbox" in data
        assert "feature_count" in data
        assert "version" in data

    @pytest.mark.unit
    def test_file_info_format_human_readable(self, catalog_with_tracked_file: Path) -> None:
        """Test that FileInfo produces human-readable output per ADR-0022."""
        from portolan_cli.inspect import inspect_file

        file_path = catalog_with_tracked_file / "demographics" / "census" / "census.parquet"
        result = inspect_file(file_path, catalog_root=catalog_with_tracked_file)
        lines = result.format_human()

        # Check ADR-0022 output format
        assert any("Format:" in line for line in lines)
        assert any("CRS:" in line for line in lines)
        assert any("Bbox:" in line for line in lines)
        assert any("Features:" in line for line in lines)
        assert any("Version:" in line for line in lines)
