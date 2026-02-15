"""Unit tests for ItemModel and AssetModel dataclasses.

Tests cover:
- Item creation with required STAC fields
- Asset creation and media types
- Geometry and bbox handling
- Properties including datetime
- JSON serialization (to_dict/from_dict)
"""

from __future__ import annotations

from typing import Any

import pytest

from portolan_cli.models.catalog import Link

# These will be implemented - tests first!
from portolan_cli.models.item import AssetModel, ItemModel


class TestAssetModel:
    """Tests for AssetModel dataclass."""

    @pytest.mark.unit
    def test_create_asset_with_required_fields(self) -> None:
        """AssetModel can be created with only href."""
        asset = AssetModel(href="./data.parquet")
        assert asset.href == "./data.parquet"

    @pytest.mark.unit
    def test_create_asset_with_all_fields(self) -> None:
        """AssetModel can be created with all fields."""
        asset = AssetModel(
            href="./data.parquet",
            type="application/x-parquet",
            roles=["data"],
            title="GeoParquet data file",
        )

        assert asset.type == "application/x-parquet"
        assert asset.roles == ["data"]
        assert asset.title == "GeoParquet data file"

    @pytest.mark.unit
    def test_create_cog_asset(self) -> None:
        """AssetModel can represent a COG file."""
        asset = AssetModel(
            href="./image.tif",
            type="image/tiff; application=geotiff; profile=cloud-optimized",
            roles=["data"],
        )
        assert "cloud-optimized" in asset.type

    @pytest.mark.unit
    def test_asset_to_dict(self) -> None:
        """AssetModel.to_dict() returns correct dict."""
        asset = AssetModel(
            href="./data.parquet",
            type="application/x-parquet",
            roles=["data"],
        )
        data = asset.to_dict()

        assert data["href"] == "./data.parquet"
        assert data["type"] == "application/x-parquet"
        assert data["roles"] == ["data"]

    @pytest.mark.unit
    def test_asset_from_dict(self) -> None:
        """AssetModel.from_dict() creates AssetModel from dict."""
        data = {
            "href": "./thumbnail.png",
            "type": "image/png",
            "roles": ["thumbnail"],
        }
        asset = AssetModel.from_dict(data)

        assert asset.href == "./thumbnail.png"
        assert asset.roles == ["thumbnail"]


class TestItemModel:
    """Tests for ItemModel dataclass."""

    def _sample_geometry(self) -> dict[str, Any]:
        """Return a sample GeoJSON polygon geometry."""
        return {
            "type": "Polygon",
            "coordinates": [
                [
                    [-122.5, 37.5],
                    [-122.0, 37.5],
                    [-122.0, 38.0],
                    [-122.5, 38.0],
                    [-122.5, 37.5],
                ]
            ],
        }

    def _sample_bbox(self) -> list[float]:
        """Return a sample bbox."""
        return [-122.5, 37.5, -122.0, 38.0]

    @pytest.mark.unit
    def test_create_item_with_required_fields(self) -> None:
        """ItemModel can be created with required fields."""
        item = ItemModel(
            id="item-001",
            geometry=self._sample_geometry(),
            bbox=self._sample_bbox(),
            properties={"datetime": "2024-01-15T12:00:00Z"},
            assets={
                "data": AssetModel(href="./data.parquet"),
            },
            collection="my-collection",
        )

        assert item.id == "item-001"
        assert item.type == "Feature"
        assert item.stac_version == "1.0.0"
        assert item.collection == "my-collection"

    @pytest.mark.unit
    def test_create_item_with_all_fields(self) -> None:
        """ItemModel can be created with all fields."""
        item = ItemModel(
            id="full-item",
            geometry=self._sample_geometry(),
            bbox=self._sample_bbox(),
            properties={
                "datetime": "2024-01-15T12:00:00Z",
                "geoparquet:geometry_type": "Polygon",
                "geoparquet:feature_count": 1000,
            },
            assets={
                "data": AssetModel(
                    href="./data.parquet",
                    type="application/x-parquet",
                    roles=["data"],
                ),
            },
            collection="my-collection",
            title="Full Item",
            description="A fully-populated item",
            links=[Link(rel="self", href="./item.json")],
        )

        assert item.title == "Full Item"
        assert item.description == "A fully-populated item"

    @pytest.mark.unit
    def test_type_defaults_to_feature(self) -> None:
        """type field should always be 'Feature'."""
        item = ItemModel(
            id="test",
            geometry=self._sample_geometry(),
            bbox=self._sample_bbox(),
            properties={"datetime": None},
            assets={},
            collection="test",
        )
        assert item.type == "Feature"

    @pytest.mark.unit
    def test_item_with_null_datetime_and_range(self) -> None:
        """Item can have null datetime with start/end range."""
        item = ItemModel(
            id="temporal-range",
            geometry=self._sample_geometry(),
            bbox=self._sample_bbox(),
            properties={
                "datetime": None,
                "start_datetime": "2024-01-01T00:00:00Z",
                "end_datetime": "2024-12-31T23:59:59Z",
            },
            assets={},
            collection="test",
        )

        assert item.properties["datetime"] is None
        assert item.properties["start_datetime"] == "2024-01-01T00:00:00Z"

    @pytest.mark.unit
    def test_item_with_null_geometry(self) -> None:
        """Item can have null geometry (for non-spatial datasets)."""
        item = ItemModel(
            id="non-spatial",
            geometry=None,
            bbox=[-180.0, -90.0, 180.0, 90.0],
            properties={"datetime": "2024-01-15T12:00:00Z"},
            assets={},
            collection="test",
        )

        assert item.geometry is None


class TestItemValidation:
    """Tests for ItemModel validation rules."""

    @pytest.mark.unit
    def test_bbox_must_have_4_or_6_elements(self) -> None:
        """bbox must have 4 (2D) or 6 (3D) elements."""
        geometry = {"type": "Point", "coordinates": [0, 0]}

        # Valid 4-element bbox
        item = ItemModel(
            id="test",
            geometry=geometry,
            bbox=[0.0, 0.0, 1.0, 1.0],
            properties={"datetime": None},
            assets={},
            collection="test",
        )
        assert len(item.bbox) == 4

        # Valid 6-element bbox (3D)
        item_3d = ItemModel(
            id="test-3d",
            geometry=geometry,
            bbox=[0.0, 0.0, 0.0, 1.0, 1.0, 100.0],
            properties={"datetime": None},
            assets={},
            collection="test",
        )
        assert len(item_3d.bbox) == 6

    @pytest.mark.unit
    def test_invalid_bbox_length_raises_error(self) -> None:
        """Invalid bbox length should raise ValueError."""
        geometry = {"type": "Point", "coordinates": [0, 0]}

        with pytest.raises(ValueError, match="bbox"):
            ItemModel(
                id="test",
                geometry=geometry,
                bbox=[0.0, 0.0, 1.0],  # Invalid: only 3 elements
                properties={"datetime": None},
                assets={},
                collection="test",
            )


class TestItemSerialization:
    """Tests for ItemModel JSON serialization."""

    def _sample_item(self) -> ItemModel:
        """Create a sample item for serialization tests."""
        return ItemModel(
            id="serialize-test",
            geometry={
                "type": "Point",
                "coordinates": [-122.0, 37.5],
            },
            bbox=[-122.0, 37.5, -122.0, 37.5],
            properties={
                "datetime": "2024-01-15T12:00:00Z",
                "geoparquet:feature_count": 100,
            },
            assets={
                "data": AssetModel(
                    href="./data.parquet",
                    type="application/x-parquet",
                    roles=["data"],
                ),
            },
            collection="test-collection",
            title="Test Item",
            links=[Link(rel="self", href="./item.json")],
        )

    @pytest.mark.unit
    def test_to_dict_includes_required_fields(self) -> None:
        """to_dict() must include all STAC-required fields."""
        item = self._sample_item()
        data = item.to_dict()

        assert data["type"] == "Feature"
        assert data["stac_version"] == "1.0.0"
        assert data["id"] == "serialize-test"
        assert "geometry" in data
        assert "bbox" in data
        assert "properties" in data
        assert "links" in data
        assert "assets" in data

    @pytest.mark.unit
    def test_to_dict_assets_structure(self) -> None:
        """to_dict() should have correct assets structure."""
        item = self._sample_item()
        data = item.to_dict()

        assert "data" in data["assets"]
        assert data["assets"]["data"]["href"] == "./data.parquet"
        assert data["assets"]["data"]["type"] == "application/x-parquet"

    @pytest.mark.unit
    def test_from_dict_creates_item(self) -> None:
        """from_dict() should create ItemModel from dict."""
        data = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "from-dict-test",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "bbox": [0.0, 0.0, 0.0, 0.0],
            "properties": {"datetime": "2024-01-15T12:00:00Z"},
            "links": [],
            "assets": {
                "data": {"href": "./data.parquet"},
            },
            "collection": "test",
        }
        item = ItemModel.from_dict(data)

        assert item.id == "from-dict-test"
        assert "data" in item.assets

    @pytest.mark.unit
    def test_roundtrip_serialization(self) -> None:
        """to_dict -> from_dict should preserve all data."""
        original = self._sample_item()

        data = original.to_dict()
        restored = ItemModel.from_dict(data)

        assert restored.id == original.id
        assert restored.collection == original.collection
        assert restored.title == original.title
        assert len(restored.assets) == len(original.assets)
        assert restored.properties == original.properties


class TestItemProperties:
    """Tests for item properties handling."""

    @pytest.mark.unit
    def test_geoparquet_properties(self) -> None:
        """Item can have GeoParquet-specific properties."""
        item = ItemModel(
            id="geoparquet-item",
            geometry={"type": "Polygon", "coordinates": [[]]},
            bbox=[-180.0, -90.0, 180.0, 90.0],
            properties={
                "datetime": "2024-01-15T12:00:00Z",
                "geoparquet:geometry_type": "MultiPolygon",
                "geoparquet:feature_count": 50000,
            },
            assets={},
            collection="test",
        )

        assert item.properties["geoparquet:geometry_type"] == "MultiPolygon"
        assert item.properties["geoparquet:feature_count"] == 50000

    @pytest.mark.unit
    def test_cog_properties(self) -> None:
        """Item can have COG-specific properties."""
        item = ItemModel(
            id="cog-item",
            geometry={"type": "Polygon", "coordinates": [[]]},
            bbox=[-180.0, -90.0, 180.0, 90.0],
            properties={
                "datetime": "2024-01-15T12:00:00Z",
                "raster:bands": [
                    {"data_type": "uint8", "nodata": 0},
                    {"data_type": "uint8", "nodata": 0},
                    {"data_type": "uint8", "nodata": 0},
                ],
            },
            assets={},
            collection="test",
        )

        assert len(item.properties["raster:bands"]) == 3
