"""Unit tests for dataset orchestration module.

Tests the dataset module which orchestrates the workflow for adding,
listing, and removing datasets from a Portolan catalog.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from portolan_cli.dataset import (
    IGNORED_FILES,
    DatasetInfo,
    _get_asset_role,
    _get_media_type,
    _update_versions,
    add_dataset,
    get_dataset_info,
    list_datasets,
    remove_dataset,
)
from portolan_cli.formats import FormatType
from portolan_cli.versions import read_versions

if TYPE_CHECKING:
    pass


@pytest.fixture
def initialized_catalog(tmp_path: Path) -> Path:
    """Create an initialized Portolan catalog structure (per ADR-0023)."""
    # Create .portolan for internal state
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()

    # catalog.json at root level (per ADR-0023)
    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "portolan-catalog",
        "description": "A Portolan-managed STAC catalog",
        "links": [],
    }
    (tmp_path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    return tmp_path


class TestDatasetInfo:
    """Tests for the DatasetInfo dataclass."""

    @pytest.mark.unit
    def test_dataset_info_creation(self) -> None:
        """DatasetInfo can be created with required fields."""
        info = DatasetInfo(
            item_id="test-item",
            collection_id="test-collection",
            format_type=FormatType.VECTOR,
            bbox=[-122.5, 37.5, -122.0, 38.0],
            asset_paths=["data.parquet"],
        )
        assert info.item_id == "test-item"
        assert info.collection_id == "test-collection"
        assert info.format_type == FormatType.VECTOR

    @pytest.mark.unit
    def test_dataset_info_with_metadata(self) -> None:
        """DatasetInfo accepts optional metadata fields."""
        info = DatasetInfo(
            item_id="test-item",
            collection_id="test-collection",
            format_type=FormatType.RASTER,
            bbox=[0, 0, 1, 1],
            asset_paths=["data.tif"],
            title="Test Dataset",
            description="A test dataset",
            datetime=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        assert info.title == "Test Dataset"
        assert info.description == "A test dataset"


class TestAddDataset:
    """Tests for add_dataset function."""

    @pytest.mark.unit
    def test_add_vector_dataset(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """add_dataset processes a vector file and creates STAC item."""
        # Create file INSIDE collection/item directory structure (Issue #163)
        item_dir = initialized_catalog / "test-collection" / "data"
        item_dir.mkdir(parents=True, exist_ok=True)

        geojson_path = item_dir / "data.geojson"
        geojson_data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                    "properties": {"name": "test"},
                }
            ],
        }
        geojson_path.write_text(json.dumps(geojson_data))

        # Create the output file that convert_vector would create (in same dir)
        output_file = item_dir / "data.parquet"
        output_file.write_bytes(b"fake parquet data")

        with (
            patch("portolan_cli.dataset.detect_format") as mock_detect,
            patch("portolan_cli.dataset.convert_vector") as mock_convert,
            patch("portolan_cli.dataset.extract_geoparquet_metadata") as mock_metadata,
            patch("portolan_cli.dataset.compute_checksum") as mock_checksum,
        ):
            mock_detect.return_value = FormatType.VECTOR
            mock_convert.return_value = output_file
            mock_metadata.return_value = MagicMock(
                bbox=(-122.5, 37.5, -122.0, 38.0),
                crs="EPSG:4326",
                feature_count=1,
                geometry_type="Point",
                to_stac_properties=lambda: {"geoparquet:feature_count": 1},
            )
            mock_checksum.return_value = "abc123"

            result = add_dataset(
                path=geojson_path,
                catalog_root=initialized_catalog,
                collection_id="test-collection",
            )

            assert result.collection_id == "test-collection"
            assert result.item_id == "data"  # From parent directory, not filename
            assert result.format_type == FormatType.VECTOR
            mock_detect.assert_called_once()
            mock_convert.assert_called_once()

    @pytest.mark.unit
    def test_add_raster_dataset(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """add_dataset processes a raster file and creates STAC item."""
        # Create file INSIDE collection/item directory structure (Issue #163)
        item_dir = initialized_catalog / "imagery" / "satellite"
        item_dir.mkdir(parents=True, exist_ok=True)

        tiff_path = item_dir / "data.tif"
        tiff_path.write_bytes(b"fake tiff data")

        # Output file in same directory (in-place)
        output_file = item_dir / "data.tif"

        with (
            patch("portolan_cli.dataset.detect_format") as mock_detect,
            patch("portolan_cli.dataset.convert_raster") as mock_convert,
            patch("portolan_cli.dataset.extract_cog_metadata") as mock_metadata,
            patch("portolan_cli.dataset.compute_checksum") as mock_checksum,
        ):
            mock_detect.return_value = FormatType.RASTER
            mock_convert.return_value = output_file
            mock_metadata.return_value = MagicMock(
                bbox=(0.0, 0.0, 1.0, 1.0),
                crs="EPSG:4326",
                width=64,
                height=64,
                band_count=3,
                to_stac_properties=lambda: {"raster:bands": [{"data_type": "uint8"}]},
            )
            mock_checksum.return_value = "def456"

            result = add_dataset(
                path=tiff_path,
                catalog_root=initialized_catalog,
                collection_id="imagery",
            )

            assert result.collection_id == "imagery"
            assert result.item_id == "satellite"  # From parent directory
            assert result.format_type == FormatType.RASTER

    @pytest.mark.unit
    def test_add_dataset_unknown_format_raises(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """add_dataset raises ValueError for unknown formats."""
        unknown_path = tmp_path / "data.xyz"
        unknown_path.write_text("unknown format")

        with patch("portolan_cli.dataset.detect_format") as mock_detect:
            mock_detect.return_value = FormatType.UNKNOWN

            with pytest.raises(ValueError, match="Unsupported format"):
                add_dataset(
                    path=unknown_path,
                    catalog_root=initialized_catalog,
                    collection_id="test",
                )

    @pytest.mark.unit
    def test_add_dataset_with_title_and_description(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """add_dataset accepts optional title and description."""
        # Create file INSIDE collection/item directory structure (Issue #163)
        item_dir = initialized_catalog / "test" / "mydata"
        item_dir.mkdir(parents=True, exist_ok=True)

        geojson_path = item_dir / "data.geojson"
        # Must have valid features for pre-validation
        geojson_data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "properties": {},
                }
            ],
        }
        geojson_path.write_text(json.dumps(geojson_data))

        output_file = item_dir / "data.parquet"
        output_file.write_bytes(b"fake data")

        with (
            patch("portolan_cli.dataset.detect_format") as mock_detect,
            patch("portolan_cli.dataset.convert_vector") as mock_convert,
            patch("portolan_cli.dataset.extract_geoparquet_metadata") as mock_metadata,
            patch("portolan_cli.dataset.compute_checksum") as mock_checksum,
        ):
            mock_detect.return_value = FormatType.VECTOR
            mock_convert.return_value = output_file
            mock_metadata.return_value = MagicMock(
                bbox=(-1, -1, 1, 1),
                crs="EPSG:4326",
                feature_count=1,
                geometry_type="Point",
                to_stac_properties=lambda: {},
            )
            mock_checksum.return_value = "xyz789"

            result = add_dataset(
                path=geojson_path,
                catalog_root=initialized_catalog,
                collection_id="test",
                title="My Dataset",
                description="A detailed description",
            )

            assert result.title == "My Dataset"
            assert result.description == "A detailed description"

    @pytest.mark.unit
    def test_add_dataset_creates_collection_if_not_exists(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """add_dataset creates collection when it doesn't exist."""
        # Create file INSIDE collection/item directory structure (Issue #163)
        item_dir = initialized_catalog / "new-collection" / "mydata"
        item_dir.mkdir(parents=True, exist_ok=True)

        geojson_path = item_dir / "data.geojson"
        # Must have valid features for pre-validation
        geojson_data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "properties": {},
                }
            ],
        }
        geojson_path.write_text(json.dumps(geojson_data))

        output_file = item_dir / "data.parquet"
        output_file.write_bytes(b"fake data")

        with (
            patch("portolan_cli.dataset.detect_format") as mock_detect,
            patch("portolan_cli.dataset.convert_vector") as mock_convert,
            patch("portolan_cli.dataset.extract_geoparquet_metadata") as mock_metadata,
            patch("portolan_cli.dataset.compute_checksum") as mock_checksum,
        ):
            mock_detect.return_value = FormatType.VECTOR
            mock_convert.return_value = output_file
            mock_metadata.return_value = MagicMock(
                bbox=(0, 0, 1, 1),
                crs="EPSG:4326",
                feature_count=1,
                geometry_type="Point",
                to_stac_properties=lambda: {},
            )
            mock_checksum.return_value = "new123"

            result = add_dataset(
                path=geojson_path,
                catalog_root=initialized_catalog,
                collection_id="new-collection",
            )

            # Collection should have been created
            collection_dir = initialized_catalog / "new-collection"
            assert collection_dir.exists(), "Collection directory should exist"
            assert result.collection_id == "new-collection", (
                "Result should have correct collection_id"
            )


class TestListDatasets:
    """Tests for list_datasets function."""

    @pytest.mark.unit
    def test_list_empty_catalog(self, initialized_catalog: Path) -> None:
        """list_datasets returns empty list for catalog with no datasets."""
        datasets = list_datasets(initialized_catalog)
        assert datasets == []

    @pytest.mark.unit
    def test_list_no_catalog_returns_empty(self, tmp_path: Path) -> None:
        """list_datasets returns empty for directory without catalog."""
        datasets = list_datasets(tmp_path)
        assert datasets == []

    @pytest.mark.unit
    def test_list_skips_non_directory_at_root(self, initialized_catalog: Path) -> None:
        """list_datasets skips files at root level (not directories).

        Per ADR-0023: Collections are directories at root level.
        """
        # Create a file (not directory) at root
        (initialized_catalog / "not-a-collection.txt").write_text("not a dir")

        datasets = list_datasets(initialized_catalog)
        assert datasets == []

    @pytest.mark.unit
    def test_list_skips_collection_without_json(self, initialized_catalog: Path) -> None:
        """list_datasets skips directories without collection.json.

        Per ADR-0023: Collections live at root level, identified by collection.json.
        """
        # Create directory at root without collection.json
        col_dir = initialized_catalog / "incomplete"
        col_dir.mkdir(parents=True)
        # No collection.json created

        datasets = list_datasets(initialized_catalog)
        assert datasets == []

    @pytest.mark.unit
    def test_list_skips_missing_item_files(self, initialized_catalog: Path) -> None:
        """list_datasets skips items where item.json doesn't exist.

        Per ADR-0023: Collections live at root level.
        """
        # Create collection at root (per ADR-0023)
        col_dir = initialized_catalog / "col"
        col_dir.mkdir(parents=True)

        # Collection references an item that doesn't exist
        collection_data = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "col",
            "description": "Test",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[0, 0, 1, 1]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [{"rel": "item", "href": "./missing/missing.json"}],
        }
        (col_dir / "collection.json").write_text(json.dumps(collection_data))

        datasets = list_datasets(initialized_catalog)
        assert datasets == []

    @pytest.mark.unit
    def test_list_detects_raster_format(self, initialized_catalog: Path) -> None:
        """list_datasets correctly identifies raster format from .tif assets.

        Per ADR-0023: Collections live at root level, not inside .portolan/.
        """
        # Create collection directory at root (per ADR-0023)
        col_dir = initialized_catalog / "imagery"
        col_dir.mkdir(parents=True)

        collection_data = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "imagery",
            "description": "Raster imagery",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[0, 0, 1, 1]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [{"rel": "item", "href": "./raster/raster.json"}],
        }
        (col_dir / "collection.json").write_text(json.dumps(collection_data))

        item_dir = col_dir / "raster"
        item_dir.mkdir()
        item_data = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "raster",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
            },
            "bbox": [0, 0, 1, 1],
            "properties": {"datetime": "2024-01-01T00:00:00Z"},
            "links": [],
            "assets": {"data": {"href": "image.tif"}},
        }
        (item_dir / "raster.json").write_text(json.dumps(item_data))

        datasets = list_datasets(initialized_catalog)

        assert len(datasets) == 1
        assert datasets[0].format_type == FormatType.RASTER

    @pytest.mark.unit
    def test_list_all_datasets(self, initialized_catalog: Path) -> None:
        """list_datasets returns all datasets across collections.

        Per ADR-0023: Collections live at root level, not inside .portolan/.
        """
        # Create collection directory at root (per ADR-0023)
        col1_dir = initialized_catalog / "col1"
        col1_dir.mkdir(parents=True)

        # Create a minimal collection.json
        collection_data = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "col1",
            "description": "Test collection",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [{"rel": "item", "href": "./item1/item1.json"}],
        }
        (col1_dir / "collection.json").write_text(json.dumps(collection_data))

        # Create item directory with item.json
        item_dir = col1_dir / "item1"
        item_dir.mkdir()
        item_data = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "item1",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
            },
            "bbox": [0, 0, 1, 1],
            "properties": {"datetime": "2024-01-01T00:00:00Z"},
            "links": [],
            "assets": {"data": {"href": "data.parquet"}},
        }
        (item_dir / "item1.json").write_text(json.dumps(item_data))

        # Update catalog to link to collection (catalog.json is at root per ADR-0023)
        catalog_data = json.loads((initialized_catalog / "catalog.json").read_text())
        catalog_data["links"].append({"rel": "child", "href": "./col1/collection.json"})
        (initialized_catalog / "catalog.json").write_text(json.dumps(catalog_data))

        datasets = list_datasets(initialized_catalog)

        assert len(datasets) == 1
        assert datasets[0].item_id == "item1"
        assert datasets[0].collection_id == "col1"

    @pytest.mark.unit
    def test_list_datasets_filter_by_collection(self, initialized_catalog: Path) -> None:
        """list_datasets filters by collection when specified.

        Per ADR-0023: Collections live at root level.
        """
        # Create two collections at root (per ADR-0023)
        for col_id in ["col1", "col2"]:
            col_dir = initialized_catalog / col_id
            col_dir.mkdir(parents=True)
            collection_data = {
                "type": "Collection",
                "stac_version": "1.0.0",
                "id": col_id,
                "description": f"Collection {col_id}",
                "license": "proprietary",
                "extent": {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {"interval": [[None, None]]},
                },
                "links": [{"rel": "item", "href": f"./item-{col_id}/item-{col_id}.json"}],
            }
            (col_dir / "collection.json").write_text(json.dumps(collection_data))

            item_dir = col_dir / f"item-{col_id}"
            item_dir.mkdir()
            item_data = {
                "type": "Feature",
                "stac_version": "1.0.0",
                "id": f"item-{col_id}",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
                },
                "bbox": [0, 0, 1, 1],
                "properties": {"datetime": "2024-01-01T00:00:00Z"},
                "links": [],
                "assets": {},
            }
            (item_dir / f"item-{col_id}.json").write_text(json.dumps(item_data))

        # Update catalog links (catalog.json is at root per ADR-0023)
        catalog_data = json.loads((initialized_catalog / "catalog.json").read_text())
        catalog_data["links"] = [
            {"rel": "child", "href": "./col1/collection.json"},
            {"rel": "child", "href": "./col2/collection.json"},
        ]
        (initialized_catalog / "catalog.json").write_text(json.dumps(catalog_data))

        # Filter by col1
        datasets = list_datasets(initialized_catalog, collection_id="col1")

        assert len(datasets) == 1
        assert datasets[0].collection_id == "col1"


class TestGetDatasetInfo:
    """Tests for get_dataset_info function."""

    @pytest.mark.unit
    def test_get_dataset_info_existing(self, initialized_catalog: Path) -> None:
        """get_dataset_info returns info for existing dataset.

        Per ADR-0023: Collections live at root level.
        """
        # Create collection at root (per ADR-0023)
        col_dir = initialized_catalog / "test-col"
        col_dir.mkdir(parents=True)

        collection_data = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "test-col",
            "description": "Test",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-122.5, 37.5, -122.0, 38.0]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [{"rel": "item", "href": "./my-item/my-item.json"}],
        }
        (col_dir / "collection.json").write_text(json.dumps(collection_data))

        item_dir = col_dir / "my-item"
        item_dir.mkdir()
        item_data = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "my-item",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [[-122.5, 37.5], [-122.5, 38.0], [-122.0, 38.0], [-122.0, 37.5], [-122.5, 37.5]]
                ],
            },
            "bbox": [-122.5, 37.5, -122.0, 38.0],
            "properties": {"datetime": "2024-01-01T00:00:00Z", "title": "My Item"},
            "links": [],
            "assets": {"data": {"href": "data.parquet", "type": "application/x-parquet"}},
        }
        (item_dir / "my-item.json").write_text(json.dumps(item_data))

        info = get_dataset_info(initialized_catalog, "test-col/my-item")

        assert info.item_id == "my-item"
        assert info.collection_id == "test-col"
        assert info.bbox == [-122.5, 37.5, -122.0, 38.0]

    @pytest.mark.unit
    def test_get_dataset_info_not_found(self, initialized_catalog: Path) -> None:
        """get_dataset_info raises KeyError for nonexistent dataset."""
        with pytest.raises(KeyError, match="Dataset not found"):
            get_dataset_info(initialized_catalog, "nonexistent/item")


class TestRemoveDataset:
    """Tests for remove_dataset function."""

    @pytest.mark.unit
    def test_remove_dataset_single_item(self, initialized_catalog: Path) -> None:
        """remove_dataset removes a single item from collection.

        Per ADR-0023: Collections live at root level.
        """
        # Create collection at root (per ADR-0023)
        col_dir = initialized_catalog / "test-col"
        col_dir.mkdir(parents=True)

        collection_data = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "test-col",
            "description": "Test",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[0, 0, 1, 1]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [{"rel": "item", "href": "./to-remove/to-remove.json"}],
        }
        (col_dir / "collection.json").write_text(json.dumps(collection_data))

        item_dir = col_dir / "to-remove"
        item_dir.mkdir()
        item_data = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "to-remove",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
            },
            "bbox": [0, 0, 1, 1],
            "properties": {"datetime": "2024-01-01T00:00:00Z"},
            "links": [],
            "assets": {"data": {"href": "data.parquet"}},
        }
        (item_dir / "to-remove.json").write_text(json.dumps(item_data))
        (item_dir / "data.parquet").write_bytes(b"fake parquet")

        remove_dataset(initialized_catalog, "test-col/to-remove")

        # Item directory should be removed
        assert not item_dir.exists()

    @pytest.mark.unit
    def test_remove_dataset_not_found(self, initialized_catalog: Path) -> None:
        """remove_dataset raises KeyError for nonexistent dataset."""
        with pytest.raises(KeyError, match="Dataset not found"):
            remove_dataset(initialized_catalog, "nonexistent/item")

    @pytest.mark.unit
    def test_remove_entire_collection(self, initialized_catalog: Path) -> None:
        """remove_dataset can remove entire collection.

        Per ADR-0023: Collections live at root level.
        """
        # Create collection at root (per ADR-0023)
        col_dir = initialized_catalog / "to-remove-col"
        col_dir.mkdir(parents=True)

        collection_data = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "to-remove-col",
            "description": "To be removed",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[0, 0, 1, 1]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [],
        }
        (col_dir / "collection.json").write_text(json.dumps(collection_data))

        # Update catalog to link to collection (catalog.json is at root per ADR-0023)
        catalog_data = json.loads((initialized_catalog / "catalog.json").read_text())
        catalog_data["links"].append({"rel": "child", "href": "./to-remove-col/collection.json"})
        (initialized_catalog / "catalog.json").write_text(json.dumps(catalog_data))

        remove_dataset(initialized_catalog, "to-remove-col", remove_collection=True)

        assert not col_dir.exists()


class TestAddDatasetMissingBbox:
    """Tests for add_dataset bbox validation."""

    @pytest.mark.unit
    def test_add_dataset_missing_bbox_raises(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """add_dataset raises ValueError when metadata has no bbox (Null Island prevention)."""
        geojson_path = tmp_path / "data.geojson"
        geojson_path.write_text('{"type": "FeatureCollection", "features": []}')

        # Create the output file
        output_dir = initialized_catalog / "test" / "data"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "data.parquet"
        output_file.write_bytes(b"fake data")

        with (
            patch("portolan_cli.dataset.detect_format") as mock_detect,
            patch("portolan_cli.dataset.convert_vector") as mock_convert,
            patch("portolan_cli.dataset.extract_geoparquet_metadata") as mock_metadata,
            patch("portolan_cli.dataset.compute_checksum") as mock_checksum,
        ):
            mock_detect.return_value = FormatType.VECTOR
            mock_convert.return_value = output_file
            # Simulate missing bbox (None or empty tuple)
            mock_metadata.return_value = MagicMock(
                bbox=None,  # Missing bbox!
                crs="EPSG:4326",
                feature_count=0,
                geometry_type="Point",
                to_stac_properties=lambda: {},
            )
            mock_checksum.return_value = "xyz789"

            with pytest.raises(ValueError, match="missing bounding box"):
                add_dataset(
                    path=geojson_path,
                    catalog_root=initialized_catalog,
                    collection_id="test",
                )


class TestAddDatasetItemIdDerivation:
    """Tests for item_id derivation from directory structure (Issue #163).

    Per the design, item_id should be derived from the PARENT DIRECTORY name,
    not the filename. This ensures files stay organized with their companions
    and avoids creating duplicate directories.

    Example:
        censo-2010/data/radios.parquet
                   ^^^^
                   item_id = "data" (parent directory)
                   NOT "radios" (filename stem)
    """

    @pytest.mark.unit
    def test_item_id_from_parent_directory_not_filename(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """item_id should be derived from parent directory name, not filename.

        Given: censo-2010/data/radios.parquet
        Expected item_id: "data" (parent directory)
        Wrong item_id: "radios" (filename stem)
        """
        # Set up: collection/item_dir/file.geojson structure
        collection_dir = initialized_catalog / "censo-2010"
        item_dir = collection_dir / "data"  # This should become the item_id
        item_dir.mkdir(parents=True)

        geojson_path = item_dir / "radios.geojson"
        geojson_data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-58.4, -34.6]},
                    "properties": {"name": "Buenos Aires"},
                }
            ],
        }
        geojson_path.write_text(json.dumps(geojson_data))

        with (
            patch("portolan_cli.dataset.detect_format") as mock_detect,
            patch("portolan_cli.dataset.convert_vector") as mock_convert,
            patch("portolan_cli.dataset.extract_geoparquet_metadata") as mock_metadata,
            patch("portolan_cli.dataset.compute_checksum") as mock_checksum,
        ):
            mock_detect.return_value = FormatType.VECTOR
            # Simulate conversion output in the SAME directory (in-place)
            output_file = item_dir / "radios.parquet"
            output_file.write_bytes(b"fake parquet")
            mock_convert.return_value = output_file
            mock_metadata.return_value = MagicMock(
                bbox=(-58.5, -34.7, -58.3, -34.5),
                crs="EPSG:4326",
                feature_count=1,
                geometry_type="Point",
                to_stac_properties=lambda: {"geoparquet:feature_count": 1},
            )
            mock_checksum.return_value = "abc123"

            result = add_dataset(
                path=geojson_path,
                catalog_root=initialized_catalog,
                collection_id="censo-2010",
            )

            # CRITICAL ASSERTION: item_id should be "data" (parent dir), not "radios" (filename)
            assert result.item_id == "data", (
                f"item_id should be parent directory name 'data', not filename stem '{result.item_id}'. "
                "See Issue #163: item boundaries should be directories, not filenames."
            )

    @pytest.mark.unit
    def test_add_dataset_tracks_in_place_no_copy(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """add_dataset should track files in-place, not copy them to a new directory.

        Given: collection/item_dir/file.geojson
        Expected: Converted file stays in collection/item_dir/
        Wrong: Copied to collection/filename_stem/ (creates duplicate)

        Issue #163: The current implementation copies files, causing duplication.
        """
        # Set up structure: collection/data/radios.geojson
        collection_dir = initialized_catalog / "censo-2010"
        item_dir = collection_dir / "data"
        item_dir.mkdir(parents=True)

        geojson_path = item_dir / "radios.geojson"
        geojson_data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-58.4, -34.6]},
                    "properties": {"name": "Buenos Aires"},
                }
            ],
        }
        geojson_path.write_text(json.dumps(geojson_data))

        with (
            patch("portolan_cli.dataset.detect_format") as mock_detect,
            patch("portolan_cli.dataset.convert_vector") as mock_convert,
            patch("portolan_cli.dataset.extract_geoparquet_metadata") as mock_metadata,
            patch("portolan_cli.dataset.compute_checksum") as mock_checksum,
        ):
            mock_detect.return_value = FormatType.VECTOR
            # Simulate conversion output in the SAME directory (in-place)
            output_file = item_dir / "radios.parquet"
            output_file.write_bytes(b"fake parquet")
            mock_convert.return_value = output_file
            mock_metadata.return_value = MagicMock(
                bbox=(-58.5, -34.7, -58.3, -34.5),
                crs="EPSG:4326",
                feature_count=1,
                geometry_type="Point",
                to_stac_properties=lambda: {"geoparquet:feature_count": 1},
            )
            mock_checksum.return_value = "abc123"

            add_dataset(
                path=geojson_path,
                catalog_root=initialized_catalog,
                collection_id="censo-2010",
            )

            # CRITICAL ASSERTION: convert_vector should be called with the SAME directory
            # as the source file (in-place conversion), not a new directory
            convert_call_args = mock_convert.call_args
            dest_dir = convert_call_args[0][1]  # Second positional arg is dest_dir

            assert dest_dir == item_dir, (
                f"convert_vector should output to source directory '{item_dir}', "
                f"not '{dest_dir}'. Files should be tracked in-place, not copied. "
                "See Issue #163."
            )

            # Also verify no duplicate directory was created
            wrong_dir = collection_dir / "radios"  # Would exist if using filename as item_id
            assert not wrong_dir.exists(), (
                f"Directory '{wrong_dir}' should not exist. "
                "Files should stay in their original directory, not be copied."
            )

    @pytest.mark.integration
    def test_add_dataset_no_artifacts_on_validation_failure(
        self, initialized_catalog: Path
    ) -> None:
        """Failed add_dataset leaves no partial artifacts (Issue #163 atomicity).

        When add_dataset fails due to missing geometry/bbox, it should be atomic:
        - No new directories created
        - No files copied
        - No collection.json created
        - No versions.json created

        This tests the REAL code path without mocks to verify atomicity.
        """
        # Create collection with an item directory containing non-geo parquet
        collection_dir = initialized_catalog / "demographics"
        item_dir = collection_dir / "census-data"
        item_dir.mkdir(parents=True)

        # Create a GeoJSON with no geometry (empty FeatureCollection)
        geojson_path = item_dir / "data.geojson"
        geojson_path.write_text('{"type": "FeatureCollection", "features": []}')

        # Record state before add attempt
        files_before = set(initialized_catalog.rglob("*"))

        # Attempt to add - should fail due to missing bbox
        with pytest.raises(ValueError, match="missing bounding box"):
            add_dataset(
                path=geojson_path,
                catalog_root=initialized_catalog,
                collection_id="demographics",
            )

        # Verify NO new artifacts were created beyond what we set up
        files_after = set(initialized_catalog.rglob("*"))
        new_files = files_after - files_before

        # Filter out the source files we created (they should exist)
        expected_files = {collection_dir, item_dir, geojson_path}
        unexpected_files = new_files - expected_files

        assert unexpected_files == set(), (
            f"Failed add_dataset created partial artifacts: {unexpected_files}\n"
            "Expected atomic failure with no leftover files. See Issue #163."
        )

        # Explicitly verify STAC artifacts don't exist
        collection_json = collection_dir / "collection.json"
        assert not collection_json.exists(), "collection.json should not exist after failed add"

        versions_json = collection_dir / "versions.json"
        assert not versions_json.exists(), "versions.json should not exist after failed add"


class TestPathSegmentValidation:
    """Tests for item_id and collection_id path traversal prevention."""

    # item_id must always be a single segment (no slashes)
    _UNSAFE_ITEM_IDS = [
        pytest.param("../etc", id="traversal"),
        pytest.param("a/b", id="slash"),
        pytest.param("a\\b", id="backslash"),
        pytest.param(".", id="dot"),
        pytest.param("..", id="dotdot"),
        pytest.param("", id="empty"),
    ]

    # collection_id allows slashes per ADR-0032 (nested catalogs),
    # but still rejects traversal, backslashes, and special segments
    _UNSAFE_COLLECTION_IDS = [
        pytest.param("../etc", id="traversal"),
        pytest.param("a/../b", id="traversal_in_path"),
        pytest.param("a\\b", id="backslash"),
        pytest.param(".", id="dot"),
        pytest.param("..", id="dotdot"),
        pytest.param("a/./b", id="dot_in_path"),
        pytest.param("a/../b", id="dotdot_in_path"),
        pytest.param("", id="empty"),
    ]

    _GEOJSON = (
        '{"type":"FeatureCollection","features":[{"type":"Feature",'
        '"geometry":{"type":"Point","coordinates":[0,0]},'
        '"properties":{"name":"x"}}]}'
    )

    @pytest.mark.unit
    @pytest.mark.parametrize("bad_id", _UNSAFE_ITEM_IDS)
    def test_rejects_unsafe_item_id(
        self, initialized_catalog: Path, tmp_path: Path, bad_id: str
    ) -> None:
        """add_dataset rejects item_id values with path separators or traversal."""
        geojson_path = tmp_path / "data.geojson"
        geojson_path.write_text(self._GEOJSON)

        with pytest.raises(ValueError, match="must be a single path segment"):
            add_dataset(
                path=geojson_path,
                catalog_root=initialized_catalog,
                collection_id="test",
                item_id=bad_id,
            )

    @pytest.mark.unit
    @pytest.mark.parametrize("bad_id", _UNSAFE_COLLECTION_IDS)
    def test_rejects_unsafe_collection_id(
        self, initialized_catalog: Path, tmp_path: Path, bad_id: str
    ) -> None:
        """add_dataset rejects collection_id values with traversal or backslashes.

        Per ADR-0032, forward slashes ARE allowed for nested catalog paths
        (e.g., 'climate/hittekaart'), but traversal segments are rejected.
        """
        geojson_path = tmp_path / "data.geojson"
        geojson_path.write_text(self._GEOJSON)

        with pytest.raises(ValueError, match="not allowed"):
            add_dataset(
                path=geojson_path,
                catalog_root=initialized_catalog,
                collection_id=bad_id,
            )


class TestUpdateVersionsHref:
    """Tests for _update_versions producing catalog-root-relative hrefs.

    The href in versions.json must be relative to catalog root so that
    push.py and pull.py can resolve it via `catalog_root / href`.
    """

    @pytest.mark.unit
    def test_href_is_catalog_root_relative(self, tmp_path: Path) -> None:
        """_update_versions produces href as collection_id/item_id/filename."""
        catalog_root = tmp_path / "catalog"
        collection_dir = catalog_root / "agriculture"
        item_dir = collection_dir / "census-2020"
        item_dir.mkdir(parents=True)

        # Create the output file where add_dataset would place it
        output_file = item_dir / "census-2020.parquet"
        output_file.write_bytes(b"fake parquet data")

        _update_versions(
            collection_dir=collection_dir,
            item_id="census-2020",
            catalog_root=catalog_root,
            asset_files={
                "census-2020.parquet": (output_file, "abc123"),
            },
        )

        versions = read_versions(collection_dir / "versions.json")
        # Asset key uses item-scoped format: {item_id}/{filename} (per ADR-0028)
        asset = versions.versions[0].assets["census-2020/census-2020.parquet"]

        assert asset.href == "agriculture/census-2020/census-2020.parquet"

    @pytest.mark.unit
    def test_href_resolves_to_actual_file(self, tmp_path: Path) -> None:
        """catalog_root / href resolves to the actual asset file on disk."""
        catalog_root = tmp_path / "catalog"
        collection_dir = catalog_root / "demographics"
        item_dir = collection_dir / "pop-data"
        item_dir.mkdir(parents=True)

        output_file = item_dir / "pop-data.parquet"
        output_file.write_bytes(b"fake parquet data")

        _update_versions(
            collection_dir=collection_dir,
            item_id="pop-data",
            catalog_root=catalog_root,
            asset_files={
                "pop-data.parquet": (output_file, "def456"),
            },
        )

        versions = read_versions(collection_dir / "versions.json")
        # Asset key uses item-scoped format: {item_id}/{filename} (per ADR-0028)
        asset = versions.versions[0].assets["pop-data/pop-data.parquet"]

        resolved = catalog_root / asset.href
        assert resolved.exists(), f"catalog_root / href should resolve to file: {resolved}"
        assert resolved == output_file


class TestGetMediaType:
    """Tests for _get_media_type helper function."""

    @pytest.mark.unit
    def test_parquet_media_type(self, tmp_path: Path) -> None:
        """_get_media_type returns correct type for .parquet files."""
        p = tmp_path / "data.parquet"
        p.write_bytes(b"fake")
        assert _get_media_type(p) == "application/x-parquet"

    @pytest.mark.unit
    def test_tif_media_type(self, tmp_path: Path) -> None:
        """_get_media_type returns correct type for .tif files."""
        p = tmp_path / "image.tif"
        p.write_bytes(b"fake")
        assert _get_media_type(p) == "image/tiff; application=geotiff; profile=cloud-optimized"

    @pytest.mark.unit
    def test_tiff_media_type(self, tmp_path: Path) -> None:
        """_get_media_type returns correct type for .tiff files."""
        p = tmp_path / "image.tiff"
        p.write_bytes(b"fake")
        assert _get_media_type(p) == "image/tiff; application=geotiff; profile=cloud-optimized"

    @pytest.mark.unit
    def test_geojson_media_type(self, tmp_path: Path) -> None:
        """_get_media_type returns correct type for .geojson files."""
        p = tmp_path / "data.geojson"
        p.write_bytes(b"fake")
        assert _get_media_type(p) == "application/geo+json"

    @pytest.mark.unit
    def test_json_media_type(self, tmp_path: Path) -> None:
        """_get_media_type returns correct type for .json files."""
        p = tmp_path / "item.json"
        p.write_bytes(b"fake")
        assert _get_media_type(p) == "application/json"

    @pytest.mark.unit
    def test_png_media_type(self, tmp_path: Path) -> None:
        """_get_media_type returns correct type for .png files."""
        p = tmp_path / "thumb.png"
        p.write_bytes(b"fake")
        assert _get_media_type(p) == "image/png"

    @pytest.mark.unit
    def test_jpg_media_type(self, tmp_path: Path) -> None:
        """_get_media_type returns correct type for .jpg files."""
        p = tmp_path / "thumb.jpg"
        p.write_bytes(b"fake")
        assert _get_media_type(p) == "image/jpeg"

    @pytest.mark.unit
    def test_jpeg_media_type(self, tmp_path: Path) -> None:
        """_get_media_type returns correct type for .jpeg files."""
        p = tmp_path / "thumb.jpeg"
        p.write_bytes(b"fake")
        assert _get_media_type(p) == "image/jpeg"

    @pytest.mark.unit
    def test_xml_media_type(self, tmp_path: Path) -> None:
        """_get_media_type returns correct type for .xml files."""
        p = tmp_path / "metadata.xml"
        p.write_bytes(b"fake")
        assert _get_media_type(p) == "application/xml"

    @pytest.mark.unit
    def test_csv_media_type(self, tmp_path: Path) -> None:
        """_get_media_type returns correct type for .csv files."""
        p = tmp_path / "data.csv"
        p.write_bytes(b"fake")
        assert _get_media_type(p) == "text/csv"

    @pytest.mark.unit
    def test_pmtiles_media_type(self, tmp_path: Path) -> None:
        """_get_media_type returns correct type for .pmtiles files."""
        p = tmp_path / "tiles.pmtiles"
        p.write_bytes(b"fake")
        assert _get_media_type(p) == "application/vnd.pmtiles"

    @pytest.mark.unit
    def test_unknown_extension_falls_back(self, tmp_path: Path) -> None:
        """_get_media_type returns application/octet-stream for unknown extensions."""
        p = tmp_path / "data.xyz"
        p.write_bytes(b"fake")
        assert _get_media_type(p) == "application/octet-stream"

    @pytest.mark.unit
    def test_case_insensitive(self, tmp_path: Path) -> None:
        """_get_media_type handles uppercase extensions."""
        p = tmp_path / "data.PARQUET"
        p.write_bytes(b"fake")
        assert _get_media_type(p) == "application/x-parquet"


class TestGetAssetRole:
    """Tests for _get_asset_role helper function."""

    @pytest.mark.unit
    def test_parquet_is_data(self, tmp_path: Path) -> None:
        """_get_asset_role identifies .parquet as data."""
        p = tmp_path / "data.parquet"
        p.write_bytes(b"fake")
        assert _get_asset_role(p) == "data"

    @pytest.mark.unit
    def test_tif_is_data(self, tmp_path: Path) -> None:
        """_get_asset_role identifies .tif as data."""
        p = tmp_path / "image.tif"
        p.write_bytes(b"fake")
        assert _get_asset_role(p) == "data"

    @pytest.mark.unit
    def test_png_is_thumbnail(self, tmp_path: Path) -> None:
        """_get_asset_role identifies .png as thumbnail."""
        p = tmp_path / "thumb.png"
        p.write_bytes(b"fake")
        assert _get_asset_role(p) == "thumbnail"

    @pytest.mark.unit
    def test_jpg_is_thumbnail(self, tmp_path: Path) -> None:
        """_get_asset_role identifies .jpg as thumbnail."""
        p = tmp_path / "thumb.jpg"
        p.write_bytes(b"fake")
        assert _get_asset_role(p) == "thumbnail"

    @pytest.mark.unit
    def test_xml_is_metadata(self, tmp_path: Path) -> None:
        """_get_asset_role identifies .xml as metadata."""
        p = tmp_path / "metadata.xml"
        p.write_bytes(b"fake")
        assert _get_asset_role(p) == "metadata"

    @pytest.mark.unit
    def test_json_is_metadata(self, tmp_path: Path) -> None:
        """_get_asset_role identifies .json as metadata."""
        p = tmp_path / "extra.json"
        p.write_bytes(b"fake")
        assert _get_asset_role(p) == "metadata"

    @pytest.mark.unit
    def test_csv_is_data(self, tmp_path: Path) -> None:
        """_get_asset_role identifies .csv as data."""
        p = tmp_path / "data.csv"
        p.write_bytes(b"fake")
        assert _get_asset_role(p) == "data"

    @pytest.mark.unit
    def test_unknown_extension_is_data(self, tmp_path: Path) -> None:
        """_get_asset_role defaults to data for unknown extensions."""
        p = tmp_path / "data.xyz"
        p.write_bytes(b"fake")
        assert _get_asset_role(p) == "data"


class TestIgnoredFiles:
    """Tests for IGNORED_FILES constant."""

    @pytest.mark.unit
    def test_ignored_files_contains_stac_items(self) -> None:
        """IGNORED_FILES includes STAC JSON files that should not be tracked as assets."""
        assert "collection.json" in IGNORED_FILES
        assert "catalog.json" in IGNORED_FILES

    @pytest.mark.unit
    def test_ignored_files_contains_versions(self) -> None:
        """IGNORED_FILES includes versions.json."""
        assert "versions.json" in IGNORED_FILES


class TestMultiAssetAddDataset:
    """Tests for multi-asset behavior in add_dataset."""

    # Helper to create valid GeoJSON data
    VALID_GEOJSON = json.dumps(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "properties": {},
                }
            ],
        }
    )

    @pytest.mark.unit
    def test_add_dataset_tracks_multiple_files_in_item_dir(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """add_dataset creates assets for ALL files in item_dir, not just the primary."""
        # Create file INSIDE item directory (Issue #163)
        item_dir = initialized_catalog / "test-collection" / "data"
        item_dir.mkdir(parents=True, exist_ok=True)

        geojson_path = item_dir / "data.geojson"
        geojson_path.write_text(self.VALID_GEOJSON)

        output_file = item_dir / "data.parquet"
        output_file.write_bytes(b"fake parquet data")
        # Add a thumbnail
        thumbnail = item_dir / "thumbnail.png"
        thumbnail.write_bytes(b"fake png data")
        # Add a metadata sidecar
        metadata_file = item_dir / "metadata.xml"
        metadata_file.write_bytes(b"<metadata/>")

        with (
            patch("portolan_cli.dataset.detect_format") as mock_detect,
            patch("portolan_cli.dataset.convert_vector") as mock_convert,
            patch("portolan_cli.dataset.extract_geoparquet_metadata") as mock_metadata,
            patch("portolan_cli.dataset.compute_checksum") as mock_checksum,
        ):
            mock_detect.return_value = FormatType.VECTOR
            mock_convert.return_value = output_file
            mock_metadata.return_value = MagicMock(
                bbox=(-122.5, 37.5, -122.0, 38.0),
                crs="EPSG:4326",
                feature_count=1,
                geometry_type="Point",
                to_stac_properties=lambda: {"geoparquet:feature_count": 1},
            )
            mock_checksum.return_value = "abc123"

            result = add_dataset(
                path=geojson_path,
                catalog_root=initialized_catalog,
                collection_id="test-collection",
            )

            # Should have all non-ignored files as asset paths
            # Note: includes geojson now that it's in item_dir
            assert len(result.asset_paths) >= 3
            assert str(output_file) in result.asset_paths
            assert str(thumbnail) in result.asset_paths
            assert str(metadata_file) in result.asset_paths

    @pytest.mark.unit
    def test_add_dataset_ignores_stac_files_in_item_dir(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """add_dataset ignores STAC JSON files (collection.json, etc.) in the item directory."""
        # Create file INSIDE item directory (Issue #163)
        item_dir = initialized_catalog / "test-collection" / "data"
        item_dir.mkdir(parents=True, exist_ok=True)

        geojson_path = item_dir / "data.geojson"
        geojson_path.write_text(self.VALID_GEOJSON)

        output_file = item_dir / "data.parquet"
        output_file.write_bytes(b"fake parquet data")

        with (
            patch("portolan_cli.dataset.detect_format") as mock_detect,
            patch("portolan_cli.dataset.convert_vector") as mock_convert,
            patch("portolan_cli.dataset.extract_geoparquet_metadata") as mock_metadata,
            patch("portolan_cli.dataset.compute_checksum") as mock_checksum,
        ):
            mock_detect.return_value = FormatType.VECTOR
            mock_convert.return_value = output_file
            mock_metadata.return_value = MagicMock(
                bbox=(-1, -1, 1, 1),
                crs="EPSG:4326",
                feature_count=1,
                geometry_type="Point",
                to_stac_properties=lambda: {},
            )
            mock_checksum.return_value = "xyz789"

            result = add_dataset(
                path=geojson_path,
                catalog_root=initialized_catalog,
                collection_id="test-collection",
            )

            # The item.json file is written by pystac after we scan, so it won't
            # be in the scanned assets. But the data file should be there.
            # Verify no STAC files appear in asset_paths
            for asset_path in result.asset_paths:
                basename = Path(asset_path).name
                assert basename not in IGNORED_FILES, f"STAC file {basename} should not be an asset"

    @pytest.mark.unit
    def test_add_dataset_skips_hidden_files(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """add_dataset skips hidden files (starting with dot) in item_dir."""
        # Create file INSIDE item directory (Issue #163)
        item_dir = initialized_catalog / "test-collection" / "data"
        item_dir.mkdir(parents=True, exist_ok=True)

        geojson_path = item_dir / "data.geojson"
        geojson_path.write_text(self.VALID_GEOJSON)

        output_file = item_dir / "data.parquet"
        output_file.write_bytes(b"fake parquet data")
        # Add a hidden file
        hidden_file = item_dir / ".hidden"
        hidden_file.write_bytes(b"hidden data")

        with (
            patch("portolan_cli.dataset.detect_format") as mock_detect,
            patch("portolan_cli.dataset.convert_vector") as mock_convert,
            patch("portolan_cli.dataset.extract_geoparquet_metadata") as mock_metadata,
            patch("portolan_cli.dataset.compute_checksum") as mock_checksum,
        ):
            mock_detect.return_value = FormatType.VECTOR
            mock_convert.return_value = output_file
            mock_metadata.return_value = MagicMock(
                bbox=(-1, -1, 1, 1),
                crs="EPSG:4326",
                feature_count=1,
                geometry_type="Point",
                to_stac_properties=lambda: {},
            )
            mock_checksum.return_value = "xyz789"

            result = add_dataset(
                path=geojson_path,
                catalog_root=initialized_catalog,
                collection_id="test-collection",
            )

            # Hidden file should not appear in assets
            for asset_path in result.asset_paths:
                assert not Path(asset_path).name.startswith("."), (
                    f"Hidden file should not be an asset: {asset_path}"
                )

    @pytest.mark.unit
    def test_add_dataset_skips_symlinks(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """add_dataset does not follow symlinks when scanning item_dir."""
        # Create file INSIDE item directory (Issue #163)
        item_dir = initialized_catalog / "test-collection" / "data"
        item_dir.mkdir(parents=True, exist_ok=True)

        geojson_path = item_dir / "data.geojson"
        geojson_path.write_text(self.VALID_GEOJSON)

        output_file = item_dir / "data.parquet"
        output_file.write_bytes(b"fake parquet data")
        # Create a symlink
        symlink = item_dir / "link.txt"
        symlink.symlink_to(output_file)

        with (
            patch("portolan_cli.dataset.detect_format") as mock_detect,
            patch("portolan_cli.dataset.convert_vector") as mock_convert,
            patch("portolan_cli.dataset.extract_geoparquet_metadata") as mock_metadata,
            patch("portolan_cli.dataset.compute_checksum") as mock_checksum,
        ):
            mock_detect.return_value = FormatType.VECTOR
            mock_convert.return_value = output_file
            mock_metadata.return_value = MagicMock(
                bbox=(-1, -1, 1, 1),
                crs="EPSG:4326",
                feature_count=1,
                geometry_type="Point",
                to_stac_properties=lambda: {},
            )
            mock_checksum.return_value = "xyz789"

            result = add_dataset(
                path=geojson_path,
                catalog_root=initialized_catalog,
                collection_id="test-collection",
            )

            # Symlink should not appear in assets
            for asset_path in result.asset_paths:
                assert Path(asset_path).name != "link.txt", (
                    "Symlinks should not be tracked as assets"
                )


class TestMultiAssetUpdateVersions:
    """Tests for multi-asset _update_versions behavior."""

    @pytest.mark.unit
    def test_update_versions_multiple_assets(self, tmp_path: Path) -> None:
        """_update_versions creates version entries for ALL provided assets."""
        catalog_root = tmp_path / "catalog"
        collection_dir = catalog_root / "my-collection"
        item_dir = collection_dir / "my-item"
        item_dir.mkdir(parents=True)

        # Create multiple files
        parquet_file = item_dir / "my-item.parquet"
        parquet_file.write_bytes(b"fake parquet")
        thumbnail = item_dir / "thumbnail.png"
        thumbnail.write_bytes(b"fake png")
        metadata = item_dir / "metadata.xml"
        metadata.write_bytes(b"<meta/>")

        _update_versions(
            collection_dir=collection_dir,
            item_id="my-item",
            catalog_root=catalog_root,
            asset_files={
                "my-item.parquet": (parquet_file, "hash1"),
                "thumbnail.png": (thumbnail, "hash2"),
                "metadata.xml": (metadata, "hash3"),
            },
        )

        versions = read_versions(collection_dir / "versions.json")
        assert len(versions.versions) == 1
        assets = versions.versions[0].assets

        # All three files should be tracked with item-scoped keys (per ADR-0028)
        assert "my-item/my-item.parquet" in assets
        assert "my-item/thumbnail.png" in assets
        assert "my-item/metadata.xml" in assets

        # Check hrefs are catalog-root-relative
        assert assets["my-item/my-item.parquet"].href == "my-collection/my-item/my-item.parquet"
        assert assets["my-item/thumbnail.png"].href == "my-collection/my-item/thumbnail.png"
        assert assets["my-item/metadata.xml"].href == "my-collection/my-item/metadata.xml"

        # Check checksums
        assert assets["my-item/my-item.parquet"].sha256 == "hash1"
        assert assets["my-item/thumbnail.png"].sha256 == "hash2"
        assert assets["my-item/metadata.xml"].sha256 == "hash3"

    @pytest.mark.unit
    def test_update_versions_single_asset_backward_compat(self, tmp_path: Path) -> None:
        """_update_versions still works with a single asset (backward compatibility)."""
        catalog_root = tmp_path / "catalog"
        collection_dir = catalog_root / "agriculture"
        item_dir = collection_dir / "census-2020"
        item_dir.mkdir(parents=True)

        output_file = item_dir / "census-2020.parquet"
        output_file.write_bytes(b"fake parquet data")

        _update_versions(
            collection_dir=collection_dir,
            item_id="census-2020",
            catalog_root=catalog_root,
            asset_files={
                "census-2020.parquet": (output_file, "abc123"),
            },
        )

        versions = read_versions(collection_dir / "versions.json")
        # Single asset still uses item-scoped key format (per ADR-0028)
        asset = versions.versions[0].assets["census-2020/census-2020.parquet"]
        assert asset.href == "agriculture/census-2020/census-2020.parquet"
        assert asset.sha256 == "abc123"

    @pytest.mark.unit
    def test_update_versions_collection_level_asset_no_doubled_path(
        self, tmp_path: Path
    ) -> None:
        """Collection-level assets (ADR-0031) should not double the path.

        When item_id == collection_id (file is at collection level, not in
        subdirectory), the href should NOT be collection/collection/file.
        This is a regression test for the bug where vector data at collection
        level got doubled paths in versions.json.
        """
        catalog_root = tmp_path / "catalog"
        # Vector data at collection level: parks/parks_datasd.parquet
        collection_dir = catalog_root / "parks"
        collection_dir.mkdir(parents=True)

        # File is directly in collection dir (not in item subdir)
        parquet_file = collection_dir / "parks_datasd.parquet"
        parquet_file.write_bytes(b"fake parquet")

        # item_id == collection_id (both are "parks")
        _update_versions(
            collection_dir=collection_dir,
            item_id="parks",
            catalog_root=catalog_root,
            asset_files={
                "parks_datasd.parquet": (parquet_file, "abc123"),
            },
        )

        versions = read_versions(collection_dir / "versions.json")
        asset = versions.versions[0].assets["parks/parks_datasd.parquet"]

        # href should be parks/parks_datasd.parquet, NOT parks/parks/parks_datasd.parquet
        assert asset.href == "parks/parks_datasd.parquet"
        # Verify it resolves to the actual file
        resolved = catalog_root / asset.href
        assert resolved == parquet_file
        assert resolved.exists()


class TestMultiAssetListAndInfo:
    """Tests for list_datasets and get_dataset_info with multiple assets."""

    @pytest.mark.unit
    def test_list_datasets_returns_all_asset_paths(self, initialized_catalog: Path) -> None:
        """list_datasets returns all assets, not just the data asset."""
        col_dir = initialized_catalog / "col1"
        col_dir.mkdir(parents=True)

        collection_data = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "col1",
            "description": "Test collection",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [{"rel": "item", "href": "./item1/item1.json"}],
        }
        (col_dir / "collection.json").write_text(json.dumps(collection_data))

        item_dir = col_dir / "item1"
        item_dir.mkdir()
        item_data = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "item1",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
            },
            "bbox": [0, 0, 1, 1],
            "properties": {"datetime": "2024-01-01T00:00:00Z"},
            "links": [],
            "assets": {
                "data": {"href": "data.parquet", "type": "application/x-parquet"},
                "thumbnail": {"href": "thumb.png", "type": "image/png"},
                "metadata": {"href": "extra.xml", "type": "application/xml"},
            },
        }
        (item_dir / "item1.json").write_text(json.dumps(item_data))

        datasets = list_datasets(initialized_catalog)

        assert len(datasets) == 1
        assert len(datasets[0].asset_paths) == 3
        assert "data.parquet" in datasets[0].asset_paths
        assert "thumb.png" in datasets[0].asset_paths
        assert "extra.xml" in datasets[0].asset_paths

    @pytest.mark.unit
    def test_get_dataset_info_returns_all_asset_paths(self, initialized_catalog: Path) -> None:
        """get_dataset_info returns all assets for a multi-asset item."""
        col_dir = initialized_catalog / "test-col"
        col_dir.mkdir(parents=True)

        collection_data = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "test-col",
            "description": "Test",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-1, -1, 1, 1]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [{"rel": "item", "href": "./my-item/my-item.json"}],
        }
        (col_dir / "collection.json").write_text(json.dumps(collection_data))

        item_dir = col_dir / "my-item"
        item_dir.mkdir()
        item_data = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "my-item",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
            },
            "bbox": [-1, -1, 1, 1],
            "properties": {"datetime": "2024-01-01T00:00:00Z"},
            "links": [],
            "assets": {
                "data": {"href": "data.parquet"},
                "thumbnail": {"href": "thumb.png"},
            },
        }
        (item_dir / "my-item.json").write_text(json.dumps(item_data))

        info = get_dataset_info(initialized_catalog, "test-col/my-item")

        assert len(info.asset_paths) == 2
        assert "data.parquet" in info.asset_paths
        assert "thumb.png" in info.asset_paths
