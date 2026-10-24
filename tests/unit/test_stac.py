"""Unit tests for STAC generation module.

Tests the stac module which wraps pystac for creating STAC catalogs,
collections, and items following Portolan's conventions.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pystac
import pytest

from portolan_cli.stac import (
    add_item_to_collection,
    create_collection,
    create_item,
    load_catalog,
)


class TestCreateCollection:
    """Tests for creating STAC collections."""

    @pytest.mark.unit
    def test_create_collection_minimal(self) -> None:
        """create_collection creates a valid STAC collection with minimal args."""
        collection = create_collection(
            collection_id="test-collection",
            description="A test collection",
        )

        assert collection.id == "test-collection"
        assert collection.description == "A test collection"
        assert collection.license == "other"  # default (STAC 1.1, issue #568)
        assert isinstance(collection, pystac.Collection)

    @pytest.mark.unit
    def test_create_collection_with_extent(self) -> None:
        """create_collection accepts spatial and temporal extent."""
        collection = create_collection(
            collection_id="bounded-collection",
            description="Collection with extent",
            bbox=[-122.5, 37.5, -122.0, 38.0],
            temporal_extent=(
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 12, 31, tzinfo=timezone.utc),
            ),
        )

        spatial = collection.extent.spatial.bboxes[0]
        assert spatial == [-122.5, 37.5, -122.0, 38.0]

        temporal = collection.extent.temporal.intervals[0]
        assert temporal[0] == datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert temporal[1] == datetime(2024, 12, 31, tzinfo=timezone.utc)

    @pytest.mark.unit
    def test_create_collection_with_title(self) -> None:
        """create_collection accepts optional title."""
        collection = create_collection(
            collection_id="titled-collection",
            description="A collection",
            title="My Titled Collection",
        )

        assert collection.title == "My Titled Collection"

    @pytest.mark.unit
    def test_create_collection_with_license(self) -> None:
        """create_collection accepts license string."""
        collection = create_collection(
            collection_id="licensed-collection",
            description="A collection",
            license="CC-BY-4.0",
        )

        assert collection.license == "CC-BY-4.0"

    @pytest.mark.unit
    def test_create_collection_default_extent(self) -> None:
        """create_collection uses global extent when not specified."""
        collection = create_collection(
            collection_id="global-collection",
            description="No extent specified",
        )

        # Should have global bbox
        spatial = collection.extent.spatial.bboxes[0]
        assert spatial == [-180, -90, 180, 90]

        # Should have open temporal interval
        temporal = collection.extent.temporal.intervals[0]
        assert temporal == [None, None]


class TestCreateItem:
    """Tests for creating STAC items."""

    @pytest.mark.unit
    def test_create_item_minimal(self) -> None:
        """create_item creates a valid STAC item with minimal args."""
        item = create_item(
            item_id="test-item",
            bbox=[-122.5, 37.5, -122.0, 38.0],
        )

        assert item.id == "test-item"
        assert item.bbox == [-122.5, 37.5, -122.0, 38.0]
        assert isinstance(item, pystac.Item)

    @pytest.mark.unit
    def test_create_item_geometry_from_bbox(self) -> None:
        """create_item generates polygon geometry from bbox."""
        item = create_item(
            item_id="geo-item",
            bbox=[-122.5, 37.5, -122.0, 38.0],
        )

        geom = item.geometry
        assert geom["type"] == "Polygon"
        # Should be a proper polygon ring
        coords = geom["coordinates"][0]
        assert len(coords) == 5  # Closed ring has 5 points
        assert coords[0] == coords[-1]  # Closed

    @pytest.mark.unit
    def test_create_item_with_datetime(self) -> None:
        """create_item accepts datetime."""
        dt = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        item = create_item(
            item_id="dated-item",
            bbox=[0, 0, 1, 1],
            datetime=dt,
        )

        assert item.datetime == dt

    @pytest.mark.unit
    def test_create_item_default_datetime_is_null_with_sentinel_range(self) -> None:
        """create_item uses null datetime (open interval) when not specified."""
        item = create_item(
            item_id="now-item",
            bbox=[0, 0, 1, 1],
        )

        # Datetime is null, start/end use sentinel range (STAC 1.1.0 compliance)
        assert item.datetime is None
        assert item.properties["start_datetime"] == "1900-01-01T00:00:00Z"
        assert item.properties["end_datetime"] == "9999-12-31T23:59:59Z"
        # The marker that used to travel with the range is gone (issue #654).
        assert "portolan:datetime_provisional" not in item.properties

    @pytest.mark.unit
    def test_create_item_with_properties(self) -> None:
        """create_item accepts additional properties."""
        item = create_item(
            item_id="props-item",
            bbox=[0, 0, 1, 1],
            properties={"custom_field": "custom_value", "count": 42},
        )

        assert item.properties["custom_field"] == "custom_value"
        assert item.properties["count"] == 42

    @pytest.mark.unit
    def test_create_item_with_assets(self) -> None:
        """create_item accepts assets dictionary."""
        item = create_item(
            item_id="asset-item",
            bbox=[0, 0, 1, 1],
            assets={
                "data": pystac.Asset(
                    href="data.parquet",
                    media_type="application/vnd.apache.parquet",
                    roles=["data"],
                )
            },
        )

        assert "data" in item.assets
        assert item.assets["data"].href == "data.parquet"
        assert item.assets["data"].media_type == "application/vnd.apache.parquet"


class TestCatalogOperations:
    """Tests for loading and saving catalogs."""

    @pytest.mark.unit
    def test_load_catalog(self, tmp_path: Path) -> None:
        """load_catalog reads an existing STAC catalog."""
        catalog_data = {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "test-catalog",
            "description": "Test catalog",
            "links": [],
        }
        catalog_path = tmp_path / ".portolan" / "catalog.json"
        catalog_path.parent.mkdir(parents=True)
        catalog_path.write_text(json.dumps(catalog_data))

        catalog = load_catalog(catalog_path)

        assert catalog.id == "test-catalog"
        assert catalog.description == "Test catalog"
        assert isinstance(catalog, pystac.Catalog)

    @pytest.mark.unit
    def test_load_catalog_not_found(self, tmp_path: Path) -> None:
        """load_catalog raises FileNotFoundError for missing catalog."""
        with pytest.raises(FileNotFoundError):
            load_catalog(tmp_path / "nonexistent" / "catalog.json")


class TestItemManagement:
    """Tests for adding items to collections."""

    @pytest.mark.unit
    def test_add_item_to_collection(self) -> None:
        """add_item_to_collection links item to collection."""
        collection = create_collection(
            collection_id="parent-collection",
            description="Parent",
        )
        item = create_item(
            item_id="child-item",
            bbox=[0, 0, 1, 1],
        )

        add_item_to_collection(collection, item)

        items = list(collection.get_items())
        assert len(items) == 1
        assert items[0].id == "child-item"

    @pytest.mark.unit
    def test_add_item_keeps_the_item_file_where_add_wrote_it(self, tmp_path: Path) -> None:
        """#840: an --item-id item stays beside its data, not at <collection>/<id>/.

        On main a saved collection placed the item at the best-practice path,
        so the item landed twice: once beside the data, once in a new
        directory named after the id.
        """
        collection = create_collection(collection_id="imagery", description="d")
        collection.set_self_href(str(tmp_path / "imagery" / "collection.json"))
        item = create_item(item_id="custom-scene", bbox=[0, 0, 1, 1])
        written = tmp_path / "imagery" / "scene-a" / "custom-scene.json"
        item.set_self_href(str(written))

        add_item_to_collection(collection, item)

        assert Path(item.get_self_href()) == written

    @pytest.mark.unit
    def test_add_multiple_items(self) -> None:
        """add_item_to_collection can add multiple items."""
        collection = create_collection(
            collection_id="multi-item",
            description="Multiple items",
        )
        item1 = create_item(item_id="item1", bbox=[0, 0, 1, 1])
        item2 = create_item(item_id="item2", bbox=[1, 1, 2, 2])

        add_item_to_collection(collection, item1)
        add_item_to_collection(collection, item2)

        items = list(collection.get_items())
        assert len(items) == 2
        item_ids = {i.id for i in items}
        assert item_ids == {"item1", "item2"}


class TestFileStatistics:
    """The file extension is declared from the assets that use it (issue #654).

    The aggregates this used to write (``portolan:total_size_bytes``,
    ``portolan:asset_count``) are gone: the spec defines no portolan: field, and
    a consumer sums ``file:size`` itself.
    """

    @pytest.mark.unit
    def test_declare_file_extension_writes_no_aggregates(self) -> None:
        """A collection-level file:size declares the extension and nothing else."""
        from portolan_cli.stac import EXTENSION_URLS, declare_file_extension

        collection = create_collection(
            collection_id="stats-test",
            description="Test file statistics",
        )

        # Add assets with file:size
        asset1 = pystac.Asset(
            href="./data1.parquet",
            media_type="application/vnd.apache.parquet",
            roles=["data"],
            extra_fields={"file:size": 1000, "file:checksum": "sha256:abc123"},
        )
        asset2 = pystac.Asset(
            href="./data2.parquet",
            media_type="application/vnd.apache.parquet",
            roles=["data"],
            extra_fields={"file:size": 2000, "file:checksum": "sha256:def456"},
        )
        collection.add_asset("data1", asset1)
        collection.add_asset("data2", asset2)

        declare_file_extension(collection)

        assert EXTENSION_URLS["file"] in collection.stac_extensions
        assert [key for key in collection.extra_fields if key.startswith("portolan:")] == []

    @pytest.mark.unit
    def test_declare_file_extension_sees_item_assets(self) -> None:
        """An item asset alone is enough to declare the extension."""
        from portolan_cli.stac import EXTENSION_URLS, declare_file_extension

        collection = create_collection(
            collection_id="stats-items",
            description="Test item statistics",
        )

        # Add an item with assets
        item = create_item(item_id="item1", bbox=[0, 0, 1, 1])
        item_asset = pystac.Asset(
            href="./item1/data.parquet",
            media_type="application/vnd.apache.parquet",
            roles=["data"],
            extra_fields={"file:size": 5000},
        )
        item.add_asset("data", item_asset)
        add_item_to_collection(collection, item)

        declare_file_extension(collection)

        assert EXTENSION_URLS["file"] in collection.stac_extensions

    @pytest.mark.unit
    def test_declare_file_extension_no_assets(self) -> None:
        """An empty collection declares nothing."""
        from portolan_cli.stac import EXTENSION_URLS, declare_file_extension

        collection = create_collection(
            collection_id="empty",
            description="Empty collection",
        )

        declare_file_extension(collection)

        assert EXTENSION_URLS["file"] not in (collection.stac_extensions or [])
        assert [key for key in collection.extra_fields if key.startswith("portolan:")] == []

    @pytest.mark.unit
    def test_declare_file_extension_ignores_assets_without_size(self) -> None:
        """An asset with no file:size does not on its own declare the extension."""
        from portolan_cli.stac import EXTENSION_URLS, declare_file_extension

        collection = create_collection(
            collection_id="partial",
            description="Partial file sizes",
        )

        # Asset without file:size
        asset = pystac.Asset(
            href="./without_size.parquet",
            media_type="application/vnd.apache.parquet",
            roles=["data"],
        )
        collection.add_asset("without_size", asset)

        declare_file_extension(collection)

        assert EXTENSION_URLS["file"] not in (collection.stac_extensions or [])
