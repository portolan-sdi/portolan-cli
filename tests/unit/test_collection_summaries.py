"""Tests for collection summaries functionality.

Tests the update_collection_summaries function that aggregates item properties.
Per ADR-0036: Hybrid field detection, categorical only, no numeric aggregation.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pystac
import pytest

from portolan_cli.stac import SUMMARIZED_FIELDS, update_collection_summaries


class TestSummarizedFieldsConfig:
    """Tests for SUMMARIZED_FIELDS configuration."""

    def test_summarized_fields_includes_core_fields(self) -> None:
        """SUMMARIZED_FIELDS should include core extension fields."""
        assert "proj:code" in SUMMARIZED_FIELDS
        assert "vector:geometry_types" in SUMMARIZED_FIELDS

    def test_summarized_fields_has_strategies(self) -> None:
        """SUMMARIZED_FIELDS values should be SummaryStrategy enums."""
        from pystac.summaries import SummaryStrategy

        for field, strategy in SUMMARIZED_FIELDS.items():
            assert isinstance(strategy, SummaryStrategy), f"{field} has invalid strategy"


class TestUpdateCollectionSummaries:
    """Tests for update_collection_summaries function."""

    @pytest.fixture
    def sample_collection(self) -> pystac.Collection:
        """Create a sample STAC collection."""
        return pystac.Collection(
            id="test-collection",
            description="Test collection",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent(bboxes=[[-180, -90, 180, 90]]),
                temporal=pystac.TemporalExtent(intervals=[[None, None]]),
            ),
        )

    @pytest.fixture
    def sample_items(self) -> list[pystac.Item]:
        """Create sample STAC items with various properties."""
        item1 = pystac.Item(
            id="item-1",
            geometry={"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]},
            bbox=[0, 0, 1, 1],
            datetime=datetime(2024, 1, 1, tzinfo=timezone.utc),
            properties={
                "proj:code": "EPSG:4326",
                "vector:geometry_types": ["Polygon"],
            },
        )
        item2 = pystac.Item(
            id="item-2",
            geometry={"type": "Polygon", "coordinates": [[[1, 0], [2, 0], [2, 1], [1, 1], [1, 0]]]},
            bbox=[1, 0, 2, 1],
            datetime=datetime(2024, 6, 1, tzinfo=timezone.utc),
            properties={
                "proj:code": "EPSG:32618",
                "vector:geometry_types": ["Polygon", "MultiPolygon"],
            },
        )
        return [item1, item2]

    def test_aggregates_proj_code(
        self, sample_collection: pystac.Collection, sample_items: list[pystac.Item]
    ) -> None:
        """Should aggregate proj:code as distinct values."""
        for item in sample_items:
            sample_collection.add_item(item)

        update_collection_summaries(sample_collection)

        summaries = sample_collection.summaries.to_dict()
        assert "proj:code" in summaries
        assert set(summaries["proj:code"]) == {"EPSG:4326", "EPSG:32618"}

    def test_aggregates_geometry_types(
        self, sample_collection: pystac.Collection, sample_items: list[pystac.Item]
    ) -> None:
        """Should aggregate vector:geometry_types as distinct values."""
        for item in sample_items:
            sample_collection.add_item(item)

        update_collection_summaries(sample_collection)

        summaries = sample_collection.summaries.to_dict()
        assert "vector:geometry_types" in summaries
        # Should flatten and deduplicate
        assert set(summaries["vector:geometry_types"]) == {"Polygon", "MultiPolygon"}

    def test_does_nothing_when_no_items(self, sample_collection: pystac.Collection) -> None:
        """Should not crash when collection has no items."""
        update_collection_summaries(sample_collection)

        # Summaries should be empty or None
        assert sample_collection.summaries.is_empty()

    def test_auto_detects_extension_prefixed_fields(
        self, sample_collection: pystac.Collection
    ) -> None:
        """Should auto-detect extension-prefixed fields not in explicit list."""
        item = pystac.Item(
            id="item-1",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 0, 0],
            datetime=datetime(2024, 1, 1, tzinfo=timezone.utc),
            properties={
                "custom:category": "A",
            },
        )
        sample_collection.add_item(item)

        item2 = pystac.Item(
            id="item-2",
            geometry={"type": "Point", "coordinates": [1, 1]},
            bbox=[1, 1, 1, 1],
            datetime=datetime(2024, 2, 1, tzinfo=timezone.utc),
            properties={
                "custom:category": "B",
            },
        )
        sample_collection.add_item(item2)

        update_collection_summaries(sample_collection)

        summaries = sample_collection.summaries.to_dict()
        # custom: prefix should be auto-detected
        assert "custom:category" in summaries
        assert set(summaries["custom:category"]) == {"A", "B"}


class TestUpdateCollectionExtent:
    """Tests for extent update from items."""

    @pytest.fixture
    def sample_collection(self) -> pystac.Collection:
        """Create a sample STAC collection."""
        return pystac.Collection(
            id="test-collection",
            description="Test collection",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent(bboxes=[[-180, -90, 180, 90]]),
                temporal=pystac.TemporalExtent(intervals=[[None, None]]),
            ),
        )

    def test_updates_temporal_extent_from_items(self, sample_collection: pystac.Collection) -> None:
        """Should update temporal extent from item datetimes."""
        item1 = pystac.Item(
            id="item-1",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 0, 0],
            datetime=datetime(2024, 1, 15, tzinfo=timezone.utc),
            properties={},
        )
        item2 = pystac.Item(
            id="item-2",
            geometry={"type": "Point", "coordinates": [1, 1]},
            bbox=[1, 1, 1, 1],
            datetime=datetime(2024, 6, 30, tzinfo=timezone.utc),
            properties={},
        )
        sample_collection.add_item(item1)
        sample_collection.add_item(item2)

        sample_collection.update_extent_from_items()

        interval = sample_collection.extent.temporal.intervals[0]
        assert interval[0] == datetime(2024, 1, 15, tzinfo=timezone.utc)
        assert interval[1] == datetime(2024, 6, 30, tzinfo=timezone.utc)
