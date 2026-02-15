"""Unit tests for collection creation (User Story 2).

Tests cover:
- create_collection() returns CollectionModel
- Extent extraction from GeoParquet
- Schema extraction and writing
- Collection JSON serialization
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.models.collection import (
    CollectionModel,
    ExtentModel,
    SpatialExtent,
    TemporalExtent,
)
from portolan_cli.models.schema import ColumnSchema, SchemaModel


class TestCollectionCreation:
    """Tests for create_collection() function."""

    @pytest.fixture
    def sample_geoparquet(self, tmp_path: Path) -> Path:
        """Create a sample GeoParquet file for testing."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        schema = pa.schema(
            [
                ("id", pa.int64()),
                ("name", pa.string()),
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
            {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
                "geometry": [b"WKB1", b"WKB2", b"WKB3"],
            },
            schema=schema,
        )

        existing_metadata = table.schema.metadata or {}
        new_metadata = {b"geo": json.dumps(geo_metadata).encode()}
        table = table.replace_schema_metadata({**existing_metadata, **new_metadata})

        file_path = tmp_path / "dataset.parquet"
        pq.write_table(table, file_path)
        return file_path

    @pytest.mark.unit
    def test_create_collection_returns_model(self, sample_geoparquet: Path) -> None:
        """create_collection should return a CollectionModel."""
        from portolan_cli.collection import create_collection

        collection = create_collection(
            collection_id="test-dataset",
            data_path=sample_geoparquet,
            description="Test dataset",
        )

        assert isinstance(collection, CollectionModel)
        assert collection.id == "test-dataset"
        assert collection.description == "Test dataset"

    @pytest.mark.unit
    def test_create_collection_extracts_extent(self, sample_geoparquet: Path) -> None:
        """create_collection should extract spatial extent from data."""
        from portolan_cli.collection import create_collection

        collection = create_collection(
            collection_id="test-dataset",
            data_path=sample_geoparquet,
            description="Test dataset",
        )

        # Should have valid extent
        assert collection.extent is not None
        assert collection.extent.spatial is not None
        assert len(collection.extent.spatial.bbox) > 0

    @pytest.mark.unit
    def test_create_collection_stac_fields(self, sample_geoparquet: Path) -> None:
        """Collection should have required STAC fields."""
        from portolan_cli.collection import create_collection

        collection = create_collection(
            collection_id="test-dataset",
            data_path=sample_geoparquet,
            description="Test dataset",
        )

        assert collection.type == "Collection"
        assert collection.stac_version == "1.0.0"
        assert collection.license is not None

    @pytest.mark.unit
    def test_create_collection_with_title(self, sample_geoparquet: Path) -> None:
        """Collection should accept optional title."""
        from portolan_cli.collection import create_collection

        collection = create_collection(
            collection_id="test-dataset",
            data_path=sample_geoparquet,
            description="Test dataset",
            title="My Test Dataset",
        )

        assert collection.title == "My Test Dataset"


class TestCollectionSerialization:
    """Tests for collection JSON serialization."""

    @pytest.mark.unit
    def test_collection_to_json(self) -> None:
        """Collection should serialize to JSON correctly."""
        extent = ExtentModel(
            spatial=SpatialExtent(bbox=[[-180.0, -90.0, 180.0, 90.0]]),
            temporal=TemporalExtent(interval=[["2024-01-01T00:00:00Z", None]]),
        )

        collection = CollectionModel(
            id="test-collection",
            description="A test collection",
            extent=extent,
            title="Test Collection",
        )

        data = collection.to_dict()

        assert data["type"] == "Collection"
        assert data["id"] == "test-collection"
        assert data["description"] == "A test collection"
        assert data["title"] == "Test Collection"
        assert "extent" in data
        assert "spatial" in data["extent"]
        assert "temporal" in data["extent"]

    @pytest.mark.unit
    def test_collection_roundtrip(self) -> None:
        """Collection should survive JSON roundtrip."""
        extent = ExtentModel(
            spatial=SpatialExtent(bbox=[[-122.5, 37.5, -122.0, 38.0]]),
            temporal=TemporalExtent(interval=[["2024-01-01T00:00:00Z", None]]),
        )

        original = CollectionModel(
            id="test-collection",
            description="A test collection",
            extent=extent,
            title="Test Collection",
            license="CC-BY-4.0",
        )

        json_str = json.dumps(original.to_dict())
        restored = CollectionModel.from_dict(json.loads(json_str))

        assert restored.id == original.id
        assert restored.description == original.description
        assert restored.title == original.title
        assert restored.license == original.license


class TestWriteCollectionJson:
    """Tests for write_collection_json() function."""

    @pytest.fixture
    def sample_collection(self) -> CollectionModel:
        """Create a sample collection for testing."""
        extent = ExtentModel(
            spatial=SpatialExtent(bbox=[[-180.0, -90.0, 180.0, 90.0]]),
            temporal=TemporalExtent(interval=[["2024-01-01T00:00:00Z", None]]),
        )
        return CollectionModel(
            id="test-collection",
            description="A test collection",
            extent=extent,
        )

    @pytest.mark.unit
    def test_write_collection_creates_file(
        self, tmp_path: Path, sample_collection: CollectionModel
    ) -> None:
        """write_collection_json should create collection.json."""
        from portolan_cli.collection import write_collection_json

        result_path = write_collection_json(sample_collection, tmp_path)

        assert result_path.exists()
        assert result_path.name == "collection.json"

    @pytest.mark.unit
    def test_write_collection_valid_json(
        self, tmp_path: Path, sample_collection: CollectionModel
    ) -> None:
        """Written file should be valid JSON."""
        from portolan_cli.collection import write_collection_json

        result_path = write_collection_json(sample_collection, tmp_path)

        with open(result_path) as f:
            data = json.load(f)

        assert data["type"] == "Collection"
        assert data["id"] == "test-collection"


class TestWriteSchemaJson:
    """Tests for write_schema_json() function."""

    @pytest.fixture
    def sample_schema(self) -> SchemaModel:
        """Create a sample schema for testing."""
        return SchemaModel(
            schema_version="1.0.0",
            format="geoparquet",
            columns=[
                ColumnSchema(
                    name="geometry",
                    type="binary",
                    nullable=False,
                    geometry_type="Point",
                    crs="EPSG:4326",
                ),
                ColumnSchema(
                    name="id",
                    type="int64",
                    nullable=False,
                ),
            ],
        )

    @pytest.mark.unit
    def test_write_schema_creates_file(self, tmp_path: Path, sample_schema: SchemaModel) -> None:
        """write_schema_json should create schema.json."""
        from portolan_cli.collection import write_schema_json

        result_path = write_schema_json(sample_schema, tmp_path)

        assert result_path.exists()
        assert result_path.name == "schema.json"

    @pytest.mark.unit
    def test_write_schema_valid_json(self, tmp_path: Path, sample_schema: SchemaModel) -> None:
        """Written file should be valid JSON."""
        from portolan_cli.collection import write_schema_json

        result_path = write_schema_json(sample_schema, tmp_path)

        with open(result_path) as f:
            data = json.load(f)

        assert data["format"] == "geoparquet"
        assert data["schema_version"] == "1.0.0"
        assert len(data["columns"]) == 2
