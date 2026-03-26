"""Tests for Vector extension support (STAC v0.1.0).

Tests the add_vector_extension() function that adds vector:geometry_types
to STAC items from GeoParquet metadata.

Per ADR-0037: Use experimental extensions, accept migration cost.
"""

from __future__ import annotations

import pystac
import pytest

from portolan_cli.stac import EXTENSION_URLS, add_vector_extension

# Mark all tests in this module as unit tests
pytestmark = pytest.mark.unit


class TestVectorExtensionUrl:
    """Tests for Vector extension URL registration."""

    def test_vector_extension_url_registered(self) -> None:
        """Vector extension URL should be in EXTENSION_URLS dict."""
        assert "vector" in EXTENSION_URLS
        assert (
            EXTENSION_URLS["vector"]
            == "https://stac-extensions.github.io/vector/v0.1.0/schema.json"
        )


class TestAddVectorExtension:
    """Tests for add_vector_extension() function."""

    @pytest.fixture
    def sample_item(self) -> pystac.Item:
        """Create a sample STAC item for testing."""
        return pystac.Item(
            id="test-item",
            geometry={"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]},
            bbox=[0, 0, 1, 1],
            datetime=None,
            properties={
                "start_datetime": "2024-01-01T00:00:00Z",
                "end_datetime": "2024-12-31T23:59:59Z",
            },
        )

    def test_adds_geometry_types_from_string(self, sample_item: pystac.Item) -> None:
        """Should add vector:geometry_types from single geometry type string."""

        class MockMetadata:
            geometry_type = "Polygon"

        add_vector_extension(sample_item, MockMetadata())

        assert sample_item.properties["vector:geometry_types"] == ["Polygon"]

    def test_adds_geometry_types_from_list(self, sample_item: pystac.Item) -> None:
        """Should preserve geometry_types when already a list."""

        class MockMetadata:
            geometry_type = ["Polygon", "MultiPolygon"]

        add_vector_extension(sample_item, MockMetadata())

        assert sample_item.properties["vector:geometry_types"] == ["Polygon", "MultiPolygon"]

    def test_adds_extension_url_to_stac_extensions(self, sample_item: pystac.Item) -> None:
        """Should add Vector extension URL to stac_extensions array."""

        class MockMetadata:
            geometry_type = "Point"

        add_vector_extension(sample_item, MockMetadata())

        assert EXTENSION_URLS["vector"] in sample_item.stac_extensions

    def test_does_not_duplicate_extension_url(self, sample_item: pystac.Item) -> None:
        """Should not add duplicate extension URL if already present."""
        sample_item.stac_extensions = [EXTENSION_URLS["vector"]]

        class MockMetadata:
            geometry_type = "Point"

        add_vector_extension(sample_item, MockMetadata())

        assert sample_item.stac_extensions.count(EXTENSION_URLS["vector"]) == 1

    def test_skips_when_no_geometry_type(self, sample_item: pystac.Item) -> None:
        """Should do nothing when metadata has no geometry_type."""

        class MockMetadata:
            pass

        add_vector_extension(sample_item, MockMetadata())

        assert "vector:geometry_types" not in sample_item.properties
        assert EXTENSION_URLS["vector"] not in (sample_item.stac_extensions or [])

    def test_skips_when_geometry_type_is_none(self, sample_item: pystac.Item) -> None:
        """Should do nothing when geometry_type is None."""

        class MockMetadata:
            geometry_type = None

        add_vector_extension(sample_item, MockMetadata())

        assert "vector:geometry_types" not in sample_item.properties

    def test_adds_to_existing_stac_extensions(self, sample_item: pystac.Item) -> None:
        """Should add to existing stac_extensions array."""
        # PySTAC may initialize stac_extensions to [] or None
        initial_count = len(sample_item.stac_extensions or [])

        class MockMetadata:
            geometry_type = "LineString"

        add_vector_extension(sample_item, MockMetadata())

        assert EXTENSION_URLS["vector"] in sample_item.stac_extensions
        assert len(sample_item.stac_extensions) == initial_count + 1
