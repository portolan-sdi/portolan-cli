"""Unit tests for item creation (User Story 2).

Tests cover:
- create_item() returns ItemModel
- Geometry and bbox extraction
- Asset creation
- Item JSON serialization
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.models.item import AssetModel, ItemModel


class TestItemCreation:
    """Tests for create_item() function."""

    @pytest.fixture
    def sample_geoparquet(self, tmp_path: Path) -> Path:
        """Create a sample GeoParquet file for testing."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        schema = pa.schema(
            [
                ("id", pa.int64()),
                ("geometry", pa.binary()),
            ]
        )

        geo_metadata = {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Point"],
                    "bbox": [-122.5, 37.5, -122.0, 38.0],
                    "crs": {"id": {"authority": "EPSG", "code": 4326}},
                }
            },
        }

        table = pa.table(
            {"id": [1, 2, 3], "geometry": [b"WKB1", b"WKB2", b"WKB3"]},
            schema=schema,
        )

        existing_metadata = table.schema.metadata or {}
        new_metadata = {b"geo": json.dumps(geo_metadata).encode()}
        table = table.replace_schema_metadata({**existing_metadata, **new_metadata})

        file_path = tmp_path / "dataset.parquet"
        pq.write_table(table, file_path)
        return file_path

    @pytest.mark.unit
    def test_create_item_returns_model(self, sample_geoparquet: Path) -> None:
        """create_item should return an ItemModel."""
        from portolan_cli.item import create_item

        item = create_item(
            item_id="test-item",
            data_path=sample_geoparquet,
            collection_id="test-collection",
        )

        assert isinstance(item, ItemModel)
        assert item.id == "test-item"
        assert item.collection == "test-collection"

    @pytest.mark.unit
    def test_create_item_has_bbox(self, sample_geoparquet: Path) -> None:
        """Item should have bbox extracted from data."""
        from portolan_cli.item import create_item

        item = create_item(
            item_id="test-item",
            data_path=sample_geoparquet,
            collection_id="test-collection",
        )

        assert item.bbox is not None
        assert len(item.bbox) == 4  # [west, south, east, north]

    @pytest.mark.unit
    def test_create_item_has_geometry(self, sample_geoparquet: Path) -> None:
        """Item should have GeoJSON geometry (bounding polygon)."""
        from portolan_cli.item import create_item

        item = create_item(
            item_id="test-item",
            data_path=sample_geoparquet,
            collection_id="test-collection",
        )

        assert item.geometry is not None
        assert item.geometry["type"] == "Polygon"
        assert "coordinates" in item.geometry

    @pytest.mark.unit
    def test_create_item_has_asset(self, sample_geoparquet: Path) -> None:
        """Item should have data asset."""
        from portolan_cli.item import create_item

        item = create_item(
            item_id="test-item",
            data_path=sample_geoparquet,
            collection_id="test-collection",
        )

        assert "data" in item.assets
        assert item.assets["data"].href is not None

    @pytest.mark.unit
    def test_create_item_stac_fields(self, sample_geoparquet: Path) -> None:
        """Item should have required STAC fields."""
        from portolan_cli.item import create_item

        item = create_item(
            item_id="test-item",
            data_path=sample_geoparquet,
            collection_id="test-collection",
        )

        assert item.type == "Feature"
        assert item.stac_version == "1.0.0"
        assert "datetime" in item.properties


class TestAssetModel:
    """Tests for AssetModel."""

    @pytest.mark.unit
    def test_asset_to_dict(self) -> None:
        """AssetModel should serialize correctly."""
        asset = AssetModel(
            href="data/file.parquet",
            type="application/x-parquet",
            roles=["data"],
            title="Data file",
        )

        data = asset.to_dict()

        assert data["href"] == "data/file.parquet"
        assert data["type"] == "application/x-parquet"
        assert data["roles"] == ["data"]
        assert data["title"] == "Data file"

    @pytest.mark.unit
    def test_asset_from_dict(self) -> None:
        """AssetModel should deserialize correctly."""
        data = {
            "href": "data/file.parquet",
            "type": "application/x-parquet",
            "roles": ["data"],
        }

        asset = AssetModel.from_dict(data)

        assert asset.href == "data/file.parquet"
        assert asset.type == "application/x-parquet"
        assert asset.roles == ["data"]


class TestItemSerialization:
    """Tests for item JSON serialization."""

    @pytest.mark.unit
    def test_item_to_json(self) -> None:
        """Item should serialize to JSON correctly."""
        item = ItemModel(
            id="test-item",
            geometry={
                "type": "Polygon",
                "coordinates": [
                    [[-122.5, 37.5], [-122.0, 37.5], [-122.0, 38.0], [-122.5, 38.0], [-122.5, 37.5]]
                ],
            },
            bbox=[-122.5, 37.5, -122.0, 38.0],
            properties={"datetime": "2024-01-01T00:00:00Z"},
            assets={"data": AssetModel(href="data.parquet", type="application/x-parquet")},
            collection="test-collection",
        )

        data = item.to_dict()

        assert data["type"] == "Feature"
        assert data["id"] == "test-item"
        assert data["collection"] == "test-collection"
        assert "geometry" in data
        assert "bbox" in data
        assert "properties" in data
        assert "assets" in data

    @pytest.mark.unit
    def test_item_roundtrip(self) -> None:
        """Item should survive JSON roundtrip."""
        original = ItemModel(
            id="test-item",
            geometry={
                "type": "Polygon",
                "coordinates": [
                    [[-122.5, 37.5], [-122.0, 37.5], [-122.0, 38.0], [-122.5, 38.0], [-122.5, 37.5]]
                ],
            },
            bbox=[-122.5, 37.5, -122.0, 38.0],
            properties={"datetime": "2024-01-01T00:00:00Z"},
            assets={"data": AssetModel(href="data.parquet")},
            collection="test-collection",
        )

        json_str = json.dumps(original.to_dict())
        restored = ItemModel.from_dict(json.loads(json_str))

        assert restored.id == original.id
        assert restored.collection == original.collection
        assert restored.bbox == original.bbox


class TestWriteItemJson:
    """Tests for write_item_json() function."""

    @pytest.fixture
    def sample_item(self) -> ItemModel:
        """Create a sample item for testing."""
        return ItemModel(
            id="test-item",
            geometry={
                "type": "Polygon",
                "coordinates": [
                    [[-122.5, 37.5], [-122.0, 37.5], [-122.0, 38.0], [-122.5, 38.0], [-122.5, 37.5]]
                ],
            },
            bbox=[-122.5, 37.5, -122.0, 38.0],
            properties={"datetime": "2024-01-01T00:00:00Z"},
            assets={"data": AssetModel(href="data.parquet")},
            collection="test-collection",
        )

    @pytest.mark.unit
    def test_write_item_creates_file(self, tmp_path: Path, sample_item: ItemModel) -> None:
        """write_item_json should create item.json."""
        from portolan_cli.item import write_item_json

        result_path = write_item_json(sample_item, tmp_path)

        assert result_path.exists()
        assert result_path.name == f"{sample_item.id}.json"

    @pytest.mark.unit
    def test_write_item_valid_json(self, tmp_path: Path, sample_item: ItemModel) -> None:
        """Written file should be valid JSON."""
        from portolan_cli.item import write_item_json

        result_path = write_item_json(sample_item, tmp_path)

        with open(result_path) as f:
            data = json.load(f)

        assert data["type"] == "Feature"
        assert data["id"] == "test-item"
