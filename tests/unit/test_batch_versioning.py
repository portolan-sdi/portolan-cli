"""Unit tests for batch versioning (Issue #281).

Tests the prepare/finalize separation that enables O(n) versioning instead of O(n²).

The key insight: separate GDAL-bound work (parallelizable) from file I/O (batch).
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from portolan_cli.versions import read_versions


class TestPreparedDataset:
    """Tests for the PreparedDataset dataclass."""

    @pytest.mark.unit
    def test_prepared_dataset_exists(self) -> None:
        """PreparedDataset dataclass can be imported and instantiated."""
        from portolan_cli.dataset import PreparedDataset
        from portolan_cli.formats import FormatType

        # Should be able to create with required fields
        prepared = PreparedDataset(
            item_id="test-item",
            collection_id="test-collection",
            format_type=FormatType.VECTOR,
            bbox=[-122.5, 37.5, -122.0, 38.0],
            asset_files={"data.parquet": (Path("/tmp/data.parquet"), "abc123")},
            item_json_path=Path("/tmp/item.json"),
        )

        assert prepared.item_id == "test-item"
        assert prepared.collection_id == "test-collection"
        assert prepared.format_type == FormatType.VECTOR

    @pytest.mark.unit
    def test_prepared_dataset_optional_fields(self) -> None:
        """PreparedDataset supports optional collection-level asset flag."""
        from portolan_cli.dataset import PreparedDataset
        from portolan_cli.formats import FormatType

        prepared = PreparedDataset(
            item_id="test-item",
            collection_id="test-collection",
            format_type=FormatType.RASTER,
            bbox=[0, 0, 1, 1],
            asset_files={"data.tif": (Path("/tmp/data.tif"), "def456")},
            item_json_path=Path("/tmp/item.json"),
            is_collection_level_asset=True,
        )

        assert prepared.is_collection_level_asset is True


@pytest.fixture
def initialized_catalog(tmp_path: Path) -> Path:
    """Create an initialized Portolan catalog structure (per ADR-0023)."""
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("version: 1\n")

    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "portolan-catalog",
        "description": "A Portolan-managed STAC catalog",
        "links": [],
    }
    (tmp_path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    return tmp_path


@pytest.fixture
def valid_geojson_content() -> str:
    """Minimal valid GeoJSON for testing."""
    return json.dumps(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                    "properties": {"name": "test"},
                }
            ],
        }
    )


class TestPrepareDataset:
    """Tests for prepare_dataset() function."""

    @pytest.mark.unit
    def test_prepare_dataset_does_not_write_versions_json(
        self,
        initialized_catalog: Path,
        valid_geojson_content: str,
    ) -> None:
        """prepare_dataset() extracts metadata but does NOT write versions.json.

        This is the key behavior change: prepare is side-effect-free for versioning.
        """
        from portolan_cli.dataset import prepare_dataset

        # Set up test file in catalog structure
        collection_dir = initialized_catalog / "test-collection"
        item_dir = collection_dir / "test-item"
        item_dir.mkdir(parents=True)
        test_file = item_dir / "data.geojson"
        test_file.write_text(valid_geojson_content)

        # Prepare the dataset
        prepared = prepare_dataset(
            path=test_file,
            catalog_root=initialized_catalog,
            collection_id="test-collection",
        )

        # Should return PreparedDataset with metadata
        assert prepared.item_id == "test-item"
        assert prepared.collection_id == "test-collection"
        assert len(prepared.asset_files) > 0

        # CRITICAL: versions.json should NOT exist yet
        versions_path = collection_dir / "versions.json"
        assert not versions_path.exists(), (
            "prepare_dataset() must NOT write versions.json - "
            "this is the whole point of the refactor!"
        )

    @pytest.mark.unit
    def test_prepare_dataset_does_not_update_collection_json(
        self,
        initialized_catalog: Path,
        valid_geojson_content: str,
    ) -> None:
        """prepare_dataset() should not update collection.json links.

        Collection links are also batched in finalize phase.
        """
        from portolan_cli.dataset import prepare_dataset

        collection_dir = initialized_catalog / "test-collection"
        item_dir = collection_dir / "test-item"
        item_dir.mkdir(parents=True)
        test_file = item_dir / "data.geojson"
        test_file.write_text(valid_geojson_content)

        # Create initial collection.json to verify it's not modified
        collection_json = collection_dir / "collection.json"
        initial_collection = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "test-collection",
            "description": "Test collection",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [],  # No item links initially
        }
        collection_json.write_text(json.dumps(initial_collection))
        initial_mtime = collection_json.stat().st_mtime

        prepare_dataset(
            path=test_file,
            catalog_root=initialized_catalog,
            collection_id="test-collection",
        )

        # Collection.json should not be modified by prepare
        # (links are added in finalize)
        final_mtime = collection_json.stat().st_mtime
        assert initial_mtime == final_mtime, (
            "prepare_dataset() should not modify collection.json - "
            "link updates are batched in finalize phase"
        )


class TestFinalizeDatasets:
    """Tests for finalize_datasets() function."""

    @pytest.mark.unit
    def test_finalize_datasets_writes_versions_json_once(
        self,
        initialized_catalog: Path,
    ) -> None:
        """finalize_datasets() writes versions.json in a single batch.

        Even with multiple prepared datasets, only one write per collection.
        """
        from portolan_cli.dataset import PreparedDataset, finalize_datasets
        from portolan_cli.formats import FormatType

        collection_dir = initialized_catalog / "test-collection"
        collection_dir.mkdir(parents=True)

        # Create fake asset files (finalize needs them to exist for stat)
        item1_dir = collection_dir / "item-1"
        item1_dir.mkdir()
        asset1 = item1_dir / "data1.parquet"
        asset1.write_bytes(b"fake parquet 1")

        item2_dir = collection_dir / "item-2"
        item2_dir.mkdir()
        asset2 = item2_dir / "data2.parquet"
        asset2.write_bytes(b"fake parquet 2")

        # Create two prepared datasets
        prepared1 = PreparedDataset(
            item_id="item-1",
            collection_id="test-collection",
            format_type=FormatType.VECTOR,
            bbox=[-122.5, 37.5, -122.0, 38.0],
            asset_files={"data1.parquet": (asset1, "checksum1")},
            item_json_path=item1_dir / "item-1.json",
        )
        prepared2 = PreparedDataset(
            item_id="item-2",
            collection_id="test-collection",
            format_type=FormatType.VECTOR,
            bbox=[-122.0, 37.0, -121.5, 37.5],
            asset_files={"data2.parquet": (asset2, "checksum2")},
            item_json_path=item2_dir / "item-2.json",
        )

        # Track write_versions calls
        with patch("portolan_cli.dataset.write_versions") as mock_write:
            finalize_datasets(
                catalog_root=initialized_catalog,
                prepared=[prepared1, prepared2],
            )

            # Should only write once per collection (both items in same collection)
            assert mock_write.call_count == 1, (
                f"Expected 1 write_versions call, got {mock_write.call_count}. "
                "finalize_datasets should batch all items in a collection."
            )

    @pytest.mark.unit
    def test_finalize_datasets_groups_by_collection(
        self,
        initialized_catalog: Path,
    ) -> None:
        """finalize_datasets() groups items by collection for efficient writes.

        Items in different collections get separate writes (one per collection).
        """
        from portolan_cli.dataset import PreparedDataset, finalize_datasets
        from portolan_cli.formats import FormatType

        # Set up two collections
        for coll in ["collection-a", "collection-b"]:
            coll_dir = initialized_catalog / coll
            item_dir = coll_dir / "item-1"
            item_dir.mkdir(parents=True)
            (item_dir / "data.parquet").write_bytes(b"fake")

        prepared_a = PreparedDataset(
            item_id="item-1",
            collection_id="collection-a",
            format_type=FormatType.VECTOR,
            bbox=[0, 0, 1, 1],
            asset_files={
                "data.parquet": (
                    initialized_catalog / "collection-a/item-1/data.parquet",
                    "checksum-a",
                )
            },
            item_json_path=initialized_catalog / "collection-a/item-1/item-1.json",
        )
        prepared_b = PreparedDataset(
            item_id="item-1",
            collection_id="collection-b",
            format_type=FormatType.VECTOR,
            bbox=[0, 0, 1, 1],
            asset_files={
                "data.parquet": (
                    initialized_catalog / "collection-b/item-1/data.parquet",
                    "checksum-b",
                )
            },
            item_json_path=initialized_catalog / "collection-b/item-1/item-1.json",
        )

        with patch("portolan_cli.dataset.write_versions") as mock_write:
            finalize_datasets(
                catalog_root=initialized_catalog,
                prepared=[prepared_a, prepared_b],
            )

            # Two collections = two writes
            assert mock_write.call_count == 2, (
                f"Expected 2 write_versions calls (one per collection), got {mock_write.call_count}"
            )

    @pytest.mark.unit
    def test_finalize_datasets_creates_correct_version_entries(
        self,
        initialized_catalog: Path,
    ) -> None:
        """finalize_datasets() creates proper version entries for all items."""
        from portolan_cli.dataset import PreparedDataset, finalize_datasets
        from portolan_cli.formats import FormatType

        collection_dir = initialized_catalog / "test-collection"
        collection_dir.mkdir(parents=True)

        # Create two items with real files
        for i in [1, 2]:
            item_dir = collection_dir / f"item-{i}"
            item_dir.mkdir()
            (item_dir / f"data{i}.parquet").write_bytes(b"x" * (100 * i))

        prepared = [
            PreparedDataset(
                item_id=f"item-{i}",
                collection_id="test-collection",
                format_type=FormatType.VECTOR,
                bbox=[0, 0, 1, 1],
                asset_files={
                    f"data{i}.parquet": (
                        collection_dir / f"item-{i}/data{i}.parquet",
                        f"checksum{i}",
                    )
                },
                item_json_path=collection_dir / f"item-{i}/item-{i}.json",
            )
            for i in [1, 2]
        ]

        finalize_datasets(
            catalog_root=initialized_catalog,
            prepared=prepared,
        )

        # Verify versions.json was created with both assets
        versions_path = collection_dir / "versions.json"
        assert versions_path.exists()

        versions = read_versions(versions_path)
        assert versions.current_version is not None

        # Latest version should have assets from both items
        latest = versions.versions[-1]
        asset_keys = set(latest.assets.keys())

        # Should have both items' assets (keyed by item_id/filename)
        assert "item-1/data1.parquet" in asset_keys
        assert "item-2/data2.parquet" in asset_keys


class TestAddDatasetBackwardCompatibility:
    """Ensure add_dataset() still works as a convenience wrapper."""

    @pytest.mark.integration
    def test_add_dataset_uses_prepare_and_finalize(
        self,
        initialized_catalog: Path,
        valid_geojson_content: str,
    ) -> None:
        """add_dataset() internally calls prepare_dataset + finalize_datasets."""
        from portolan_cli.dataset import add_dataset

        collection_dir = initialized_catalog / "test-collection"
        item_dir = collection_dir / "test-item"
        item_dir.mkdir(parents=True)
        test_file = item_dir / "data.geojson"
        test_file.write_text(valid_geojson_content)

        # add_dataset should still work as before
        result = add_dataset(
            path=test_file,
            catalog_root=initialized_catalog,
            collection_id="test-collection",
        )

        assert result.item_id == "test-item"
        assert result.collection_id == "test-collection"

        # And versions.json should exist (finalize was called)
        versions_path = collection_dir / "versions.json"
        assert versions_path.exists()
