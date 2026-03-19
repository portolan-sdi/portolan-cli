"""Regression tests for STAC link and asset path issues.

Bug #1: Asset hrefs were just filenames, but item JSON is in a subdirectory.
        Assets need to reference ../filename to reach parent directory.

Bug #2: is_current() looked up assets by filename alone, but versions.json
        stores keys as {item_id}/{filename}, causing duplicate processing.

Bug #3: Collection root links pointed to themselves instead of catalog root.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pystac
import pytest

from portolan_cli.catalog import init_catalog
from portolan_cli.dataset import (
    _scan_item_assets,
    add_dataset,
    is_current,
)
from portolan_cli.versions import read_versions


@pytest.fixture
def fixtures_dir() -> Path:
    """Path to the test fixtures directory."""
    return Path(__file__).parent.parent / "fixtures"


@pytest.fixture
def valid_parquet(fixtures_dir: Path) -> Path:
    """Path to a valid GeoParquet file for testing."""
    # Use the real-world fixture
    parquet_path = fixtures_dir / "realdata" / "open-buildings.parquet"
    if not parquet_path.exists():
        # Fallback to scan fixtures
        parquet_path = fixtures_dir / "scan" / "flat_collection" / "bomenrij.parquet"
    return parquet_path


@pytest.fixture
def catalog_with_nested_item(tmp_path: Path, valid_parquet: Path) -> tuple[Path, Path, str]:
    """Create a catalog with a nested item structure.

    Returns:
        Tuple of (catalog_root, data_file, collection_id)
    """
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()

    # Initialize catalog
    init_catalog(catalog_root)

    # Create nested collection structure: collection/item_dir/file.parquet
    collection_id = "test-collection"
    item_id = "test-item"
    collection_dir = catalog_root / collection_id / item_id
    collection_dir.mkdir(parents=True)

    # Copy a valid parquet file
    data_file = collection_dir / "data.parquet"
    shutil.copy(valid_parquet, data_file)

    return catalog_root, data_file, collection_id


class TestAssetPathResolution:
    """Bug #1: Asset hrefs must be relative to item JSON location."""

    @pytest.mark.unit
    def test_scan_item_assets_returns_parent_relative_hrefs(
        self, tmp_path: Path, valid_parquet: Path
    ) -> None:
        """Asset hrefs should reference parent directory (../) when item JSON
        will be placed in a subdirectory.

        Structure:
            collection/
                data.parquet       <- Asset file
                item_id/
                    item_id.json   <- Item JSON goes here

        The item JSON is one level deeper than assets, so href should be "../data.parquet"
        """
        # Arrange: create item directory with a parquet file
        item_dir = tmp_path / "collection"
        item_dir.mkdir()
        data_file = item_dir / "data.parquet"
        shutil.copy(valid_parquet, data_file)

        item_id = "test-item"

        # Act: scan for assets
        stac_assets, _, _ = _scan_item_assets(
            item_dir=item_dir,
            item_id=item_id,
            primary_file=data_file,
        )

        # Assert: href should be relative to item subdirectory (../)
        assert "data" in stac_assets, "Primary file should have 'data' key"
        data_asset = stac_assets["data"]

        # The item JSON will be at collection/test-item/test-item.json
        # The data file is at collection/data.parquet
        # So the relative path from item JSON to data file is ../data.parquet
        assert data_asset.href == "../data.parquet", (
            f"Asset href should be '../data.parquet' to reach parent directory, "
            f"but got '{data_asset.href}'"
        )

    @pytest.mark.unit
    def test_add_dataset_creates_valid_asset_paths(
        self, catalog_with_nested_item: tuple[Path, Path, str]
    ) -> None:
        """After add_dataset, asset hrefs should resolve to actual files."""
        catalog_root, data_file, collection_id = catalog_with_nested_item

        # Act: add the dataset
        result = add_dataset(
            path=data_file,
            catalog_root=catalog_root,
            collection_id=collection_id,
        )

        # Assert: load the item JSON and verify asset paths resolve
        item_json_path = catalog_root / collection_id / result.item_id / f"{result.item_id}.json"
        assert item_json_path.exists(), f"Item JSON should exist at {item_json_path}"

        item = pystac.Item.from_file(str(item_json_path))
        data_asset = item.assets.get("data")
        assert data_asset is not None, "Item should have 'data' asset"

        # Resolve the href relative to item JSON location
        resolved_path = (item_json_path.parent / data_asset.href).resolve()
        assert resolved_path.exists(), (
            f"Asset href '{data_asset.href}' should resolve to existing file. "
            f"Resolved to: {resolved_path}"
        )


class TestIsCurrentKeyLookup:
    """Bug #2: is_current() must handle item-scoped asset keys."""

    @pytest.mark.unit
    def test_is_current_finds_item_scoped_keys(
        self, catalog_with_nested_item: tuple[Path, Path, str]
    ) -> None:
        """is_current should find assets stored with {item_id}/{filename} keys."""
        catalog_root, data_file, collection_id = catalog_with_nested_item

        # Act: add the dataset (creates versions.json with item-scoped keys)
        result = add_dataset(
            path=data_file,
            catalog_root=catalog_root,
            collection_id=collection_id,
        )

        # Verify versions.json has item-scoped keys
        versions_path = catalog_root / collection_id / "versions.json"
        versions_file = read_versions(versions_path)
        current_version = versions_file.versions[-1]

        # The key format is {item_id}/{filename}
        expected_key = f"{result.item_id}/data.parquet"
        assert expected_key in current_version.assets, (
            f"versions.json should have key '{expected_key}', "
            f"but has keys: {list(current_version.assets.keys())}"
        )

        # Assert: is_current should return True (file already tracked)
        assert is_current(data_file, versions_path), (
            "is_current should return True for already-tracked file, "
            "even when versions.json uses item-scoped keys"
        )

    @pytest.mark.unit
    def test_multiple_files_same_directory_no_duplicate_processing(
        self, tmp_path: Path, valid_parquet: Path
    ) -> None:
        """Files in same directory should not be processed multiple times.

        When multiple geo files are in one directory:
        1. First file adds ALL files as assets to versions.json
        2. Subsequent files should be recognized as already-tracked
        """
        # Arrange: catalog with 2 parquet files in same directory
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        init_catalog(catalog_root)

        collection_id = "multi-file"
        item_dir = catalog_root / collection_id / "data"
        item_dir.mkdir(parents=True)

        file1 = item_dir / "file1.parquet"
        file2 = item_dir / "file2.parquet"
        shutil.copy(valid_parquet, file1)
        shutil.copy(valid_parquet, file2)

        # Act: add first file (should capture both as assets)
        add_dataset(path=file1, catalog_root=catalog_root, collection_id=collection_id)

        versions_path = catalog_root / collection_id / "versions.json"

        # Assert: BOTH files should now be recognized as current
        assert is_current(file1, versions_path), "file1 should be current"
        assert is_current(file2, versions_path), (
            "file2 should be current (added as asset when file1 was processed)"
        )


class TestCollectionRootLinks:
    """Bug #3: Collection root/parent links should point to catalog."""

    @pytest.mark.unit
    def test_collection_has_correct_root_link(
        self, catalog_with_nested_item: tuple[Path, Path, str]
    ) -> None:
        """Collection's root link should point to catalog.json, not itself."""
        catalog_root, data_file, collection_id = catalog_with_nested_item

        # Act: add dataset to create collection
        add_dataset(
            path=data_file,
            catalog_root=catalog_root,
            collection_id=collection_id,
        )

        # Load collection
        collection_path = catalog_root / collection_id / "collection.json"
        collection = pystac.Collection.from_file(str(collection_path))

        # Find root link
        root_links = [link for link in collection.links if link.rel == "root"]
        assert len(root_links) == 1, "Collection should have exactly one root link"

        root_link = root_links[0]

        # Root should NOT point to itself
        assert root_link.href != "./collection.json", (
            "Root link should not be self-referential (./collection.json)"
        )

        # Root should point to catalog.json
        resolved_root = (collection_path.parent / root_link.href).resolve()
        expected_root = (catalog_root / "catalog.json").resolve()
        assert resolved_root == expected_root, (
            f"Root link should resolve to catalog.json. "
            f"Got: {resolved_root}, Expected: {expected_root}"
        )

    @pytest.mark.unit
    def test_collection_has_parent_link(
        self, catalog_with_nested_item: tuple[Path, Path, str]
    ) -> None:
        """Collection should have a parent link to the catalog."""
        catalog_root, data_file, collection_id = catalog_with_nested_item

        # Act: add dataset to create collection
        add_dataset(
            path=data_file,
            catalog_root=catalog_root,
            collection_id=collection_id,
        )

        # Load collection
        collection_path = catalog_root / collection_id / "collection.json"
        collection = pystac.Collection.from_file(str(collection_path))

        # Find parent link
        parent_links = [link for link in collection.links if link.rel == "parent"]
        assert len(parent_links) >= 1, "Collection should have a parent link"

    @pytest.mark.unit
    def test_no_duplicate_item_links_in_collection(
        self, tmp_path: Path, valid_parquet: Path
    ) -> None:
        """Collection should not have duplicate item links.

        When multiple files in same directory are added, the collection
        should have ONE item link, not multiple duplicates.
        """
        # Arrange: catalog with 2 parquet files in same directory
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        init_catalog(catalog_root)

        collection_id = "multi-file"
        item_dir = catalog_root / collection_id / "data"
        item_dir.mkdir(parents=True)

        file1 = item_dir / "file1.parquet"
        file2 = item_dir / "file2.parquet"
        shutil.copy(valid_parquet, file1)
        shutil.copy(valid_parquet, file2)

        # Act: add both files
        add_dataset(path=file1, catalog_root=catalog_root, collection_id=collection_id)
        add_dataset(path=file2, catalog_root=catalog_root, collection_id=collection_id)

        # Load collection
        collection_path = catalog_root / collection_id / "collection.json"
        with open(collection_path) as f:
            collection_data = json.load(f)

        # Count item links
        item_links = [
            link for link in collection_data.get("links", []) if link.get("rel") == "item"
        ]

        # Should have exactly 1 item link (not 2 duplicates)
        assert len(item_links) == 1, (
            f"Collection should have exactly 1 item link, but has {len(item_links)}. "
            f"Links: {item_links}"
        )
