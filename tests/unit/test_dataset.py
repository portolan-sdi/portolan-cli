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
    DatasetInfo,
    add_dataset,
    get_dataset_info,
    list_datasets,
    remove_dataset,
)
from portolan_cli.formats import FormatType

if TYPE_CHECKING:
    pass


@pytest.fixture
def initialized_catalog(tmp_path: Path) -> Path:
    """Create an initialized Portolan catalog structure."""
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()

    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "portolan-catalog",
        "description": "A Portolan-managed STAC catalog",
        "links": [],
    }
    (portolan_dir / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

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
        # Create a mock GeoJSON file
        geojson_path = tmp_path / "data.geojson"
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

        # Create the output file that convert_vector would create
        output_dir = initialized_catalog / ".portolan" / "collections" / "test-collection" / "data"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "data.parquet"
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
            assert result.format_type == FormatType.VECTOR
            mock_detect.assert_called_once()
            mock_convert.assert_called_once()

    @pytest.mark.unit
    def test_add_raster_dataset(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """add_dataset processes a raster file and creates STAC item."""
        tiff_path = tmp_path / "data.tif"
        tiff_path.write_bytes(b"fake tiff data")

        # Create the output file that convert_raster would create
        output_dir = initialized_catalog / ".portolan" / "collections" / "imagery" / "data"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "data.tif"
        output_file.write_bytes(b"fake cog data")

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
        geojson_path = tmp_path / "data.geojson"
        geojson_path.write_text('{"type": "FeatureCollection", "features": []}')

        # Create the output file
        output_dir = initialized_catalog / ".portolan" / "collections" / "test" / "data"
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
            mock_metadata.return_value = MagicMock(
                bbox=(-1, -1, 1, 1),
                crs="EPSG:4326",
                feature_count=0,
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
        geojson_path = tmp_path / "data.geojson"
        geojson_path.write_text('{"type": "FeatureCollection", "features": []}')

        # Create the output file
        output_dir = initialized_catalog / ".portolan" / "collections" / "new-collection" / "data"
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
            mock_metadata.return_value = MagicMock(
                bbox=(0, 0, 1, 1),
                crs="EPSG:4326",
                feature_count=0,
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
            collection_dir = initialized_catalog / ".portolan" / "collections" / "new-collection"
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
    def test_list_skips_non_directory_in_collections(self, initialized_catalog: Path) -> None:
        """list_datasets skips files in collections directory."""
        portolan_dir = initialized_catalog / ".portolan"
        collections_dir = portolan_dir / "collections"
        collections_dir.mkdir()
        # Create a file (not directory) in collections
        (collections_dir / "not-a-collection.txt").write_text("not a dir")

        datasets = list_datasets(initialized_catalog)
        assert datasets == []

    @pytest.mark.unit
    def test_list_skips_collection_without_json(self, initialized_catalog: Path) -> None:
        """list_datasets skips collection directories without collection.json."""
        portolan_dir = initialized_catalog / ".portolan"
        col_dir = portolan_dir / "collections" / "incomplete"
        col_dir.mkdir(parents=True)
        # No collection.json created

        datasets = list_datasets(initialized_catalog)
        assert datasets == []

    @pytest.mark.unit
    def test_list_skips_missing_item_files(self, initialized_catalog: Path) -> None:
        """list_datasets skips items where item.json doesn't exist."""
        portolan_dir = initialized_catalog / ".portolan"
        col_dir = portolan_dir / "collections" / "col"
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
        """list_datasets correctly identifies raster format from .tif assets."""
        portolan_dir = initialized_catalog / ".portolan"
        col_dir = portolan_dir / "collections" / "imagery"
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
        """list_datasets returns all datasets across collections."""
        # Create mock collection structure
        portolan_dir = initialized_catalog / ".portolan"
        col1_dir = portolan_dir / "collections" / "col1"
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

        # Update catalog to link to collection
        catalog_data = json.loads((portolan_dir / "catalog.json").read_text())
        catalog_data["links"].append({"rel": "child", "href": "./collections/col1/collection.json"})
        (portolan_dir / "catalog.json").write_text(json.dumps(catalog_data))

        datasets = list_datasets(initialized_catalog)

        assert len(datasets) == 1
        assert datasets[0].item_id == "item1"
        assert datasets[0].collection_id == "col1"

    @pytest.mark.unit
    def test_list_datasets_filter_by_collection(self, initialized_catalog: Path) -> None:
        """list_datasets filters by collection when specified."""
        # Create two collections
        portolan_dir = initialized_catalog / ".portolan"

        for col_id in ["col1", "col2"]:
            col_dir = portolan_dir / "collections" / col_id
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

        # Update catalog links
        catalog_data = json.loads((portolan_dir / "catalog.json").read_text())
        catalog_data["links"] = [
            {"rel": "child", "href": "./collections/col1/collection.json"},
            {"rel": "child", "href": "./collections/col2/collection.json"},
        ]
        (portolan_dir / "catalog.json").write_text(json.dumps(catalog_data))

        # Filter by col1
        datasets = list_datasets(initialized_catalog, collection_id="col1")

        assert len(datasets) == 1
        assert datasets[0].collection_id == "col1"


class TestGetDatasetInfo:
    """Tests for get_dataset_info function."""

    @pytest.mark.unit
    def test_get_dataset_info_existing(self, initialized_catalog: Path) -> None:
        """get_dataset_info returns info for existing dataset."""
        # Create collection and item
        portolan_dir = initialized_catalog / ".portolan"
        col_dir = portolan_dir / "collections" / "test-col"
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
        """remove_dataset removes a single item from collection."""
        # Create collection with one item
        portolan_dir = initialized_catalog / ".portolan"
        col_dir = portolan_dir / "collections" / "test-col"
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
        """remove_dataset can remove entire collection."""
        portolan_dir = initialized_catalog / ".portolan"
        col_dir = portolan_dir / "collections" / "to-remove-col"
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

        # Update catalog to link to collection
        catalog_data = json.loads((portolan_dir / "catalog.json").read_text())
        catalog_data["links"].append(
            {"rel": "child", "href": "./collections/to-remove-col/collection.json"}
        )
        (portolan_dir / "catalog.json").write_text(json.dumps(catalog_data))

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
        output_dir = initialized_catalog / ".portolan" / "collections" / "test" / "data"
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
