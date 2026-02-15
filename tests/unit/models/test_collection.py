"""Unit tests for CollectionModel dataclass.

Tests cover:
- Dataclass creation with required and optional fields
- Extent model with spatial and temporal bounds
- JSON serialization (to_dict/from_dict)
- Validation rules (license, bbox ranges)
- STAC compatibility
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from portolan_cli.models.catalog import Link

# These will be implemented - tests first!
from portolan_cli.models.collection import (
    CollectionModel,
    ExtentModel,
    Provider,
    SpatialExtent,
    TemporalExtent,
)


class TestCollectionModelCreation:
    """Tests for creating CollectionModel instances."""

    @pytest.mark.unit
    def test_create_collection_with_required_fields(self) -> None:
        """CollectionModel can be created with only required fields."""
        extent = ExtentModel(
            spatial=SpatialExtent(bbox=[[-180.0, -90.0, 180.0, 90.0]]),
            temporal=TemporalExtent(interval=[[None, None]]),
        )
        collection = CollectionModel(
            id="test-collection",
            description="Test collection",
            license="CC-BY-4.0",
            extent=extent,
        )

        assert collection.id == "test-collection"
        assert collection.description == "Test collection"
        assert collection.type == "Collection"
        assert collection.stac_version == "1.0.0"

    @pytest.mark.unit
    def test_create_collection_with_all_fields(self) -> None:
        """CollectionModel can be created with all fields."""
        now = datetime.now(timezone.utc)
        extent = ExtentModel(
            spatial=SpatialExtent(bbox=[[-122.5, 37.5, -122.0, 38.0]]),
            temporal=TemporalExtent(interval=[["2024-01-01T00:00:00Z", "2024-12-31T23:59:59Z"]]),
        )
        collection = CollectionModel(
            id="full-collection",
            description="Full test collection",
            license="MIT",
            extent=extent,
            title="Full Collection",
            summaries={"crs": ["EPSG:4326"], "geometry_types": ["Polygon"]},
            providers=[Provider(name="Test Provider", roles=["producer"])],
            keywords=["test", "demo"],
            created=now,
            updated=now,
            links=[Link(rel="self", href="./collection.json")],
        )

        assert collection.title == "Full Collection"
        assert collection.summaries["crs"] == ["EPSG:4326"]
        assert len(collection.providers) == 1
        assert collection.keywords == ["test", "demo"]

    @pytest.mark.unit
    def test_type_defaults_to_collection(self) -> None:
        """type field should always be 'Collection'."""
        extent = ExtentModel(
            spatial=SpatialExtent(bbox=[[-180.0, -90.0, 180.0, 90.0]]),
            temporal=TemporalExtent(interval=[[None, None]]),
        )
        collection = CollectionModel(
            id="test", description="Test", license="CC-BY-4.0", extent=extent
        )
        assert collection.type == "Collection"

    @pytest.mark.unit
    def test_license_defaults_to_cc_by_4_0(self) -> None:
        """license should default to 'CC-BY-4.0'."""
        extent = ExtentModel(
            spatial=SpatialExtent(bbox=[[-180.0, -90.0, 180.0, 90.0]]),
            temporal=TemporalExtent(interval=[[None, None]]),
        )
        collection = CollectionModel(id="test", description="Test", extent=extent)
        assert collection.license == "CC-BY-4.0"


class TestExtentModel:
    """Tests for ExtentModel and its components."""

    @pytest.mark.unit
    def test_create_spatial_extent(self) -> None:
        """SpatialExtent can be created with bbox."""
        spatial = SpatialExtent(bbox=[[-122.5, 37.5, -122.0, 38.0]])
        assert spatial.bbox == [[-122.5, 37.5, -122.0, 38.0]]

    @pytest.mark.unit
    def test_create_temporal_extent(self) -> None:
        """TemporalExtent can be created with interval."""
        temporal = TemporalExtent(interval=[["2024-01-01T00:00:00Z", None]])
        assert temporal.interval == [["2024-01-01T00:00:00Z", None]]

    @pytest.mark.unit
    def test_extent_with_global_bbox(self) -> None:
        """Extent can have global bbox [-180, -90, 180, 90]."""
        spatial = SpatialExtent(bbox=[[-180.0, -90.0, 180.0, 90.0]])
        assert spatial.bbox[0] == [-180.0, -90.0, 180.0, 90.0]

    @pytest.mark.unit
    def test_extent_with_antimeridian_crossing(self) -> None:
        """Extent can have antimeridian crossing (lon_min > lon_max)."""
        # Per STAC spec, antimeridian crossings have min_lon > max_lon
        spatial = SpatialExtent(bbox=[[170.0, -45.0, -170.0, -40.0]])
        bbox = spatial.bbox[0]
        assert bbox[0] > bbox[2]  # min_lon > max_lon for antimeridian

    @pytest.mark.unit
    def test_extent_with_null_temporal(self) -> None:
        """Temporal extent can have null values for open-ended ranges."""
        temporal = TemporalExtent(interval=[[None, None]])
        assert temporal.interval[0] == [None, None]


class TestCollectionValidation:
    """Tests for CollectionModel validation rules."""

    @pytest.mark.unit
    def test_invalid_id_raises_error(self) -> None:
        """Invalid collection IDs should raise ValueError."""
        extent = ExtentModel(
            spatial=SpatialExtent(bbox=[[-180.0, -90.0, 180.0, 90.0]]),
            temporal=TemporalExtent(interval=[[None, None]]),
        )
        with pytest.raises(ValueError, match="Invalid collection id"):
            CollectionModel(
                id="invalid id with spaces",
                description="Test",
                license="CC-BY-4.0",
                extent=extent,
            )

    @pytest.mark.unit
    def test_bbox_validation_longitude_range(self) -> None:
        """Bbox longitude must be in [-180, 180]."""
        with pytest.raises(ValueError, match="longitude"):
            SpatialExtent(bbox=[[-200.0, 0.0, 200.0, 0.0]])

    @pytest.mark.unit
    def test_bbox_validation_latitude_range(self) -> None:
        """Bbox latitude must be in [-90, 90]."""
        with pytest.raises(ValueError, match="latitude"):
            SpatialExtent(bbox=[[0.0, -100.0, 0.0, 100.0]])


class TestCollectionSerialization:
    """Tests for CollectionModel JSON serialization."""

    @pytest.mark.unit
    def test_to_dict_includes_required_fields(self) -> None:
        """to_dict() must include all STAC-required fields."""
        extent = ExtentModel(
            spatial=SpatialExtent(bbox=[[-180.0, -90.0, 180.0, 90.0]]),
            temporal=TemporalExtent(interval=[[None, None]]),
        )
        collection = CollectionModel(
            id="test",
            description="Test",
            license="CC-BY-4.0",
            extent=extent,
        )
        data = collection.to_dict()

        assert data["type"] == "Collection"
        assert data["stac_version"] == "1.0.0"
        assert data["id"] == "test"
        assert data["description"] == "Test"
        assert data["license"] == "CC-BY-4.0"
        assert "extent" in data
        assert "links" in data

    @pytest.mark.unit
    def test_to_dict_extent_structure(self) -> None:
        """to_dict() should have correct extent structure."""
        extent = ExtentModel(
            spatial=SpatialExtent(bbox=[[-122.5, 37.5, -122.0, 38.0]]),
            temporal=TemporalExtent(interval=[["2024-01-01T00:00:00Z", None]]),
        )
        collection = CollectionModel(id="test", description="Test", extent=extent)
        data = collection.to_dict()

        assert data["extent"]["spatial"]["bbox"] == [[-122.5, 37.5, -122.0, 38.0]]
        assert data["extent"]["temporal"]["interval"] == [["2024-01-01T00:00:00Z", None]]

    @pytest.mark.unit
    def test_from_dict_creates_collection(self) -> None:
        """from_dict() should create CollectionModel from dict."""
        data = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "test-collection",
            "description": "Test",
            "license": "MIT",
            "extent": {
                "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [],
        }
        collection = CollectionModel.from_dict(data)

        assert collection.id == "test-collection"
        assert collection.license == "MIT"
        assert collection.extent.spatial.bbox == [[-180.0, -90.0, 180.0, 90.0]]

    @pytest.mark.unit
    def test_roundtrip_serialization(self) -> None:
        """to_dict -> from_dict should preserve all data."""
        now = datetime.now(timezone.utc)
        extent = ExtentModel(
            spatial=SpatialExtent(bbox=[[-122.5, 37.5, -122.0, 38.0]]),
            temporal=TemporalExtent(interval=[["2024-01-01T00:00:00Z", "2024-12-31T23:59:59Z"]]),
        )
        original = CollectionModel(
            id="roundtrip-test",
            description="Test roundtrip",
            license="Apache-2.0",
            extent=extent,
            title="Roundtrip",
            summaries={"crs": ["EPSG:4326"]},
            keywords=["test"],
            created=now,
            updated=now,
        )

        data = original.to_dict()
        restored = CollectionModel.from_dict(data)

        assert restored.id == original.id
        assert restored.license == original.license
        assert restored.extent.spatial.bbox == original.extent.spatial.bbox


class TestProvider:
    """Tests for Provider dataclass."""

    @pytest.mark.unit
    def test_create_provider_with_name_only(self) -> None:
        """Provider can be created with only name."""
        provider = Provider(name="Test Provider")
        assert provider.name == "Test Provider"

    @pytest.mark.unit
    def test_create_provider_with_all_fields(self) -> None:
        """Provider can be created with all fields."""
        provider = Provider(
            name="Test Provider",
            roles=["producer", "host"],
            url="https://example.com",
        )
        assert provider.roles == ["producer", "host"]
        assert provider.url == "https://example.com"

    @pytest.mark.unit
    def test_provider_to_dict(self) -> None:
        """Provider.to_dict() returns correct dict."""
        provider = Provider(name="Test", roles=["producer"])
        data = provider.to_dict()

        assert data["name"] == "Test"
        assert data["roles"] == ["producer"]

    @pytest.mark.unit
    def test_provider_from_dict(self) -> None:
        """Provider.from_dict() creates Provider from dict."""
        data = {"name": "Test Provider", "roles": ["host"], "url": "https://example.com"}
        provider = Provider.from_dict(data)

        assert provider.name == "Test Provider"
        assert provider.url == "https://example.com"
