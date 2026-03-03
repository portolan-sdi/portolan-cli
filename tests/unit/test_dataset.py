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
        output_dir = initialized_catalog / "test-collection" / "data"
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
        output_dir = initialized_catalog / "imagery" / "data"
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
        output_dir = initialized_catalog / "new-collection" / "data"
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


class TestPathSegmentValidation:
    """Tests for item_id and collection_id path traversal prevention."""

    _UNSAFE_IDS = [
        pytest.param("../etc", id="traversal"),
        pytest.param("a/b", id="slash"),
        pytest.param("a\\b", id="backslash"),
        pytest.param(".", id="dot"),
        pytest.param("..", id="dotdot"),
        pytest.param("", id="empty"),
    ]

    _GEOJSON = (
        '{"type":"FeatureCollection","features":[{"type":"Feature",'
        '"geometry":{"type":"Point","coordinates":[0,0]},'
        '"properties":{"name":"x"}}]}'
    )

    @pytest.mark.unit
    @pytest.mark.parametrize("bad_id", _UNSAFE_IDS)
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
    @pytest.mark.parametrize("bad_id", _UNSAFE_IDS)
    def test_rejects_unsafe_collection_id(
        self, initialized_catalog: Path, tmp_path: Path, bad_id: str
    ) -> None:
        """add_dataset rejects collection_id values with path separators or traversal."""
        geojson_path = tmp_path / "data.geojson"
        geojson_path.write_text(self._GEOJSON)

        with pytest.raises(ValueError, match="must be a single path segment"):
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
            asset_files={
                "census-2020.parquet": (output_file, "abc123"),
            },
        )

        versions = read_versions(collection_dir / "versions.json")
        asset = versions.versions[0].assets["census-2020.parquet"]

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
            asset_files={
                "pop-data.parquet": (output_file, "def456"),
            },
        )

        versions = read_versions(collection_dir / "versions.json")
        asset = versions.versions[0].assets["pop-data.parquet"]

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

    @pytest.mark.unit
    def test_add_dataset_tracks_multiple_files_in_item_dir(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """add_dataset creates assets for ALL files in item_dir, not just the primary."""
        geojson_path = tmp_path / "data.geojson"
        geojson_path.write_text('{"type": "FeatureCollection", "features": []}')

        # Create the output directory with multiple files
        output_dir = initialized_catalog / "test-collection" / "data"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "data.parquet"
        output_file.write_bytes(b"fake parquet data")
        # Add a thumbnail
        thumbnail = output_dir / "thumbnail.png"
        thumbnail.write_bytes(b"fake png data")
        # Add a metadata sidecar
        metadata_file = output_dir / "metadata.xml"
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
            assert len(result.asset_paths) == 3
            assert str(output_file) in result.asset_paths
            assert str(thumbnail) in result.asset_paths
            assert str(metadata_file) in result.asset_paths

    @pytest.mark.unit
    def test_add_dataset_ignores_stac_files_in_item_dir(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """add_dataset ignores STAC JSON files (collection.json, etc.) in the item directory."""
        geojson_path = tmp_path / "data.geojson"
        geojson_path.write_text('{"type": "FeatureCollection", "features": []}')

        output_dir = initialized_catalog / "test-collection" / "data"
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
        geojson_path = tmp_path / "data.geojson"
        geojson_path.write_text('{"type": "FeatureCollection", "features": []}')

        output_dir = initialized_catalog / "test-collection" / "data"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "data.parquet"
        output_file.write_bytes(b"fake parquet data")
        # Add a hidden file
        hidden_file = output_dir / ".hidden"
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
                feature_count=0,
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
        geojson_path = tmp_path / "data.geojson"
        geojson_path.write_text('{"type": "FeatureCollection", "features": []}')

        output_dir = initialized_catalog / "test-collection" / "data"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "data.parquet"
        output_file.write_bytes(b"fake parquet data")
        # Create a symlink
        symlink = output_dir / "link.txt"
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
                feature_count=0,
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
            asset_files={
                "my-item.parquet": (parquet_file, "hash1"),
                "thumbnail.png": (thumbnail, "hash2"),
                "metadata.xml": (metadata, "hash3"),
            },
        )

        versions = read_versions(collection_dir / "versions.json")
        assert len(versions.versions) == 1
        assets = versions.versions[0].assets

        # All three files should be tracked
        assert "my-item.parquet" in assets
        assert "thumbnail.png" in assets
        assert "metadata.xml" in assets

        # Check hrefs are catalog-root-relative
        assert assets["my-item.parquet"].href == "my-collection/my-item/my-item.parquet"
        assert assets["thumbnail.png"].href == "my-collection/my-item/thumbnail.png"
        assert assets["metadata.xml"].href == "my-collection/my-item/metadata.xml"

        # Check checksums
        assert assets["my-item.parquet"].sha256 == "hash1"
        assert assets["thumbnail.png"].sha256 == "hash2"
        assert assets["metadata.xml"].sha256 == "hash3"

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
            asset_files={
                "census-2020.parquet": (output_file, "abc123"),
            },
        )

        versions = read_versions(collection_dir / "versions.json")
        asset = versions.versions[0].assets["census-2020.parquet"]
        assert asset.href == "agriculture/census-2020/census-2020.parquet"
        assert asset.sha256 == "abc123"


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
