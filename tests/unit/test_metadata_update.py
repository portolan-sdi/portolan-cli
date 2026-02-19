"""Tests for metadata update functions.

Tests for Phase 2c: Update Functions
- update_item_metadata(item_path, file_path)
- create_missing_item(file_path, collection_path)
- update_collection_extent(collection_path)
- update_versions_tracking(file_path, versions_path)
"""

from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest

from portolan_cli.collection import (
    create_collection,
    read_collection_json,
    write_collection_json,
)
from portolan_cli.item import create_item, read_item_json, write_item_json
from portolan_cli.metadata.update import (
    create_missing_item,
    update_collection_extent,
    update_item_metadata,
    update_versions_tracking,
)
from portolan_cli.versions import (
    Asset,
    Version,
    VersionsFile,
    read_versions,
    write_versions,
)

# Path to test fixtures
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
REALDATA_FIXTURES = FIXTURES_DIR / "realdata"


class TestUpdateItemMetadata:
    """Tests for update_item_metadata function."""

    @pytest.mark.unit
    def test_update_item_metadata_updates_bbox(self, tmp_path: Path) -> None:
        """Test that update_item_metadata re-extracts bbox from file."""
        # Setup: Create collection directory with a GeoParquet file
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        # Use WGS84 fixture
        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        # Create initial item
        item = create_item(
            item_id="data",
            data_path=data_file,
            collection_id="my-collection",
        )
        item_path = write_item_json(item, collection_dir)

        # Read original bbox
        original_item = read_item_json(item_path)
        original_bbox = original_item.bbox.copy()

        # Update item metadata (should re-extract from file)
        updated_item = update_item_metadata(item_path, data_file)

        # Verify bbox was extracted (should match since file unchanged)
        assert updated_item.bbox is not None
        assert len(updated_item.bbox) == 4
        # Since file didn't change, bbox should be same
        assert updated_item.bbox == original_bbox

    @pytest.mark.unit
    def test_update_item_metadata_preserves_user_fields(self, tmp_path: Path) -> None:
        """Test that update_item_metadata preserves user-added fields like title, description."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        # Create initial item with user-provided title and description
        item = create_item(
            item_id="data",
            data_path=data_file,
            collection_id="my-collection",
            title="User Title",
            description="User Description",
        )
        item_path = write_item_json(item, collection_dir)

        # Update item metadata
        updated_item = update_item_metadata(item_path, data_file)

        # Verify user fields are preserved
        assert updated_item.title == "User Title"
        assert updated_item.description == "User Description"

    @pytest.mark.unit
    def test_update_item_metadata_updates_datetime(self, tmp_path: Path) -> None:
        """Test that update_item_metadata updates the datetime property."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        # Create item with old datetime
        item = create_item(
            item_id="data",
            data_path=data_file,
            collection_id="my-collection",
        )
        # Modify the datetime to an old value
        item.properties["datetime"] = "2020-01-01T00:00:00+00:00"
        item_path = write_item_json(item, collection_dir)

        # Update item metadata
        updated_item = update_item_metadata(item_path, data_file)

        # Verify datetime was updated
        new_datetime = updated_item.properties.get("datetime")
        assert new_datetime is not None
        assert new_datetime != "2020-01-01T00:00:00+00:00"

    @pytest.mark.unit
    def test_update_item_metadata_file_not_found(self, tmp_path: Path) -> None:
        """Test that update_item_metadata raises FileNotFoundError for missing file."""
        item_path = tmp_path / "nonexistent_item.json"
        file_path = tmp_path / "nonexistent.parquet"

        with pytest.raises(FileNotFoundError):
            update_item_metadata(item_path, file_path)

    @pytest.mark.unit
    def test_update_item_metadata_item_not_found(self, tmp_path: Path) -> None:
        """Test that update_item_metadata raises FileNotFoundError for missing item."""
        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = tmp_path / "data.parquet"
        shutil.copy(source_file, data_file)
        item_path = tmp_path / "nonexistent_item.json"

        with pytest.raises(FileNotFoundError):
            update_item_metadata(item_path, data_file)

    @pytest.mark.unit
    def test_update_item_metadata_writes_to_disk(self, tmp_path: Path) -> None:
        """Test that update_item_metadata writes updated item to disk."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        item = create_item(
            item_id="data",
            data_path=data_file,
            collection_id="my-collection",
        )
        item_path = write_item_json(item, collection_dir)

        # Update item metadata
        update_item_metadata(item_path, data_file)

        # Verify file was updated on disk
        updated_from_disk = read_item_json(item_path)
        assert updated_from_disk.bbox is not None


class TestCreateMissingItem:
    """Tests for create_missing_item function."""

    @pytest.mark.unit
    def test_create_missing_item_creates_item(self, tmp_path: Path) -> None:
        """Test that create_missing_item creates a new STAC item for a file."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        # Create collection first
        collection = create_collection(
            collection_id="my-collection",
            data_path=data_file,
            description="Test collection",
        )
        write_collection_json(collection, collection_dir)

        # Create item for the file
        item_path = create_missing_item(data_file, collection_dir)

        # Verify item was created
        assert item_path.exists()
        item = read_item_json(item_path)
        assert item.id == "data"
        assert item.collection == "my-collection"

    @pytest.mark.unit
    def test_create_missing_item_links_to_collection(self, tmp_path: Path) -> None:
        """Test that create_missing_item properly links to parent collection."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        collection = create_collection(
            collection_id="my-collection",
            data_path=data_file,
            description="Test collection",
        )
        write_collection_json(collection, collection_dir)

        item_path = create_missing_item(data_file, collection_dir)
        item = read_item_json(item_path)

        # Verify collection reference
        assert item.collection == "my-collection"

    @pytest.mark.unit
    def test_create_missing_item_extracts_metadata(self, tmp_path: Path) -> None:
        """Test that create_missing_item extracts metadata from file."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        collection = create_collection(
            collection_id="my-collection",
            data_path=data_file,
            description="Test collection",
        )
        write_collection_json(collection, collection_dir)

        item_path = create_missing_item(data_file, collection_dir)
        item = read_item_json(item_path)

        # Verify metadata was extracted
        assert item.bbox is not None
        assert len(item.bbox) == 4
        assert item.geometry is not None
        assert item.properties.get("datetime") is not None

    @pytest.mark.unit
    def test_create_missing_item_file_not_found(self, tmp_path: Path) -> None:
        """Test that create_missing_item raises FileNotFoundError for missing file."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()
        file_path = collection_dir / "nonexistent.parquet"

        with pytest.raises(FileNotFoundError):
            create_missing_item(file_path, collection_dir)

    @pytest.mark.unit
    def test_create_missing_item_uses_filename_as_id(self, tmp_path: Path) -> None:
        """Test that create_missing_item uses filename stem as item ID."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "my_custom_name.parquet"
        shutil.copy(source_file, data_file)

        collection = create_collection(
            collection_id="my-collection",
            data_path=data_file,
            description="Test collection",
        )
        write_collection_json(collection, collection_dir)

        item_path = create_missing_item(data_file, collection_dir)
        item = read_item_json(item_path)

        assert item.id == "my_custom_name"


class TestUpdateCollectionExtent:
    """Tests for update_collection_extent function."""

    @pytest.mark.unit
    def test_update_collection_extent_single_item(self, tmp_path: Path) -> None:
        """Test that update_collection_extent calculates extent from single item."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        # Create collection with initial extent
        collection = create_collection(
            collection_id="my-collection",
            data_path=data_file,
            description="Test collection",
        )
        write_collection_json(collection, collection_dir)

        # Create an item
        item = create_item(
            item_id="data",
            data_path=data_file,
            collection_id="my-collection",
        )
        write_item_json(item, collection_dir)

        # Update collection extent
        updated_collection = update_collection_extent(collection_dir)

        # Verify extent was updated
        assert updated_collection.extent.spatial.bbox is not None
        assert len(updated_collection.extent.spatial.bbox) >= 1

    @pytest.mark.unit
    def test_update_collection_extent_multiple_items(self, tmp_path: Path) -> None:
        """Test that update_collection_extent computes union bbox from multiple items."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        # Create first data file
        data_file1 = collection_dir / "data1.parquet"
        shutil.copy(source_file, data_file1)

        # Create second data file
        data_file2 = collection_dir / "data2.parquet"
        shutil.copy(source_file, data_file2)

        # Create collection
        collection = create_collection(
            collection_id="my-collection",
            data_path=data_file1,
            description="Test collection",
        )
        write_collection_json(collection, collection_dir)

        # Create items with different bboxes
        item1 = create_item(
            item_id="data1",
            data_path=data_file1,
            collection_id="my-collection",
        )
        # Manually modify bbox for testing union
        item1 = item1.__class__(
            id=item1.id,
            geometry=item1.geometry,
            bbox=[-10.0, -10.0, 0.0, 0.0],
            properties=item1.properties,
            assets=item1.assets,
            collection=item1.collection,
        )
        write_item_json(item1, collection_dir)

        item2 = create_item(
            item_id="data2",
            data_path=data_file2,
            collection_id="my-collection",
        )
        item2 = item2.__class__(
            id=item2.id,
            geometry=item2.geometry,
            bbox=[0.0, 0.0, 10.0, 10.0],
            properties=item2.properties,
            assets=item2.assets,
            collection=item2.collection,
        )
        write_item_json(item2, collection_dir)

        # Update collection extent
        updated_collection = update_collection_extent(collection_dir)

        # Verify union bbox
        bbox = updated_collection.extent.spatial.bbox[0]
        assert bbox[0] == -10.0  # west
        assert bbox[1] == -10.0  # south
        assert bbox[2] == 10.0  # east
        assert bbox[3] == 10.0  # north

    @pytest.mark.unit
    def test_update_collection_extent_no_items(self, tmp_path: Path) -> None:
        """Test that update_collection_extent handles empty collection."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        # Create collection with default extent (global)
        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        collection = create_collection(
            collection_id="my-collection",
            data_path=data_file,
            description="Test collection",
        )
        write_collection_json(collection, collection_dir)

        # Remove the data file to simulate empty collection
        data_file.unlink()

        # Update collection extent - should return existing extent
        updated_collection = update_collection_extent(collection_dir)

        # Collection extent should still exist (unchanged)
        assert updated_collection.extent is not None

    @pytest.mark.unit
    def test_update_collection_extent_writes_to_disk(self, tmp_path: Path) -> None:
        """Test that update_collection_extent writes updated collection to disk."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        collection = create_collection(
            collection_id="my-collection",
            data_path=data_file,
            description="Test collection",
        )
        write_collection_json(collection, collection_dir)

        item = create_item(
            item_id="data",
            data_path=data_file,
            collection_id="my-collection",
        )
        write_item_json(item, collection_dir)

        # Update collection extent
        update_collection_extent(collection_dir)

        # Verify file was updated
        collection_path = collection_dir / "collection.json"
        assert collection_path.exists()
        updated_from_disk = read_collection_json(collection_path)
        assert updated_from_disk.extent.spatial.bbox is not None

    @pytest.mark.unit
    def test_update_collection_extent_collection_not_found(self, tmp_path: Path) -> None:
        """Test that update_collection_extent raises FileNotFoundError."""
        collection_dir = tmp_path / "nonexistent"

        with pytest.raises(FileNotFoundError):
            update_collection_extent(collection_dir)


class TestUpdateVersionsTracking:
    """Tests for update_versions_tracking function."""

    @pytest.mark.unit
    def test_update_versions_tracking_updates_mtime(self, tmp_path: Path) -> None:
        """Test that update_versions_tracking updates source_mtime."""
        # Create a data file
        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = tmp_path / "data.parquet"
        shutil.copy(source_file, data_file)

        # Create versions.json
        versions_path = tmp_path / "versions.json"
        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime.now(timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(
                            sha256="abc123",
                            size_bytes=1000,
                            href="data.parquet",
                            source_mtime=1000.0,  # Old mtime
                        )
                    },
                    changes=["data.parquet"],
                )
            ],
        )
        write_versions(versions_path, versions_file)

        # Update versions tracking
        update_versions_tracking(data_file, versions_path)

        # Verify mtime was updated
        updated_versions = read_versions(versions_path)
        current_version = updated_versions.versions[-1]
        asset = current_version.assets.get("data.parquet")
        assert asset is not None
        assert asset.source_mtime != 1000.0  # Should be updated
        assert asset.source_mtime == data_file.stat().st_mtime

    @pytest.mark.unit
    def test_update_versions_tracking_file_not_found(self, tmp_path: Path) -> None:
        """Test that update_versions_tracking raises FileNotFoundError for missing file."""
        file_path = tmp_path / "nonexistent.parquet"
        versions_path = tmp_path / "versions.json"

        with pytest.raises(FileNotFoundError):
            update_versions_tracking(file_path, versions_path)

    @pytest.mark.unit
    def test_update_versions_tracking_versions_not_found(self, tmp_path: Path) -> None:
        """Test that update_versions_tracking raises FileNotFoundError for missing versions.json."""
        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = tmp_path / "data.parquet"
        shutil.copy(source_file, data_file)
        versions_path = tmp_path / "nonexistent_versions.json"

        with pytest.raises(FileNotFoundError):
            update_versions_tracking(data_file, versions_path)

    @pytest.mark.unit
    def test_update_versions_tracking_asset_not_in_versions(self, tmp_path: Path) -> None:
        """Test update_versions_tracking when asset doesn't exist in versions.json."""
        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = tmp_path / "new_data.parquet"
        shutil.copy(source_file, data_file)

        # Create versions.json without this asset
        versions_path = tmp_path / "versions.json"
        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime.now(timezone.utc),
                    breaking=False,
                    assets={
                        "other.parquet": Asset(
                            sha256="abc123",
                            size_bytes=1000,
                            href="other.parquet",
                        )
                    },
                    changes=["other.parquet"],
                )
            ],
        )
        write_versions(versions_path, versions_file)

        # Should raise KeyError for asset not found
        with pytest.raises(KeyError):
            update_versions_tracking(data_file, versions_path)

    @pytest.mark.unit
    def test_update_versions_tracking_preserves_other_fields(self, tmp_path: Path) -> None:
        """Test that update_versions_tracking preserves other asset fields."""
        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = tmp_path / "data.parquet"
        shutil.copy(source_file, data_file)

        versions_path = tmp_path / "versions.json"
        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime.now(timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(
                            sha256="abc123",
                            size_bytes=1000,
                            href="data.parquet",
                            source_path="original.geojson",
                            source_mtime=1000.0,
                        )
                    },
                    changes=["data.parquet"],
                )
            ],
        )
        write_versions(versions_path, versions_file)

        # Update versions tracking
        update_versions_tracking(data_file, versions_path)

        # Verify other fields are preserved
        updated_versions = read_versions(versions_path)
        current_version = updated_versions.versions[-1]
        asset = current_version.assets.get("data.parquet")
        assert asset is not None
        assert asset.sha256 == "abc123"
        assert asset.size_bytes == 1000
        assert asset.source_path == "original.geojson"


class TestEdgeCases:
    """Edge case tests for update functions."""

    @pytest.mark.unit
    def test_update_item_metadata_cog_file(self, tmp_path: Path) -> None:
        """Test update_item_metadata with COG file."""
        # Use real COG fixture
        cog_file = REALDATA_FIXTURES / "rapidai4eo-sample.tif"
        if not cog_file.exists():
            pytest.skip("COG fixture not available")

        collection_dir = tmp_path / "cog-collection"
        collection_dir.mkdir()

        data_file = collection_dir / "image.tif"
        shutil.copy(cog_file, data_file)

        item = create_item(
            item_id="image",
            data_path=data_file,
            collection_id="cog-collection",
        )
        item_path = write_item_json(item, collection_dir)

        # Update should work for COG files too
        updated_item = update_item_metadata(item_path, data_file)
        assert updated_item.bbox is not None

    @pytest.mark.unit
    def test_create_missing_item_collection_json_missing(self, tmp_path: Path) -> None:
        """Test create_missing_item when collection.json doesn't exist."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        # Don't create collection.json
        # Should raise FileNotFoundError
        with pytest.raises(FileNotFoundError):
            create_missing_item(data_file, collection_dir)
