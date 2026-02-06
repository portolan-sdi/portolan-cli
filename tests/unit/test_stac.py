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
    add_collection_to_catalog,
    add_item_to_collection,
    create_collection,
    create_item,
    load_catalog,
    save_catalog,
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
        assert collection.license == "proprietary"  # default
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
    def test_create_item_default_datetime(self) -> None:
        """create_item uses current time when datetime not specified."""
        before = datetime.now(timezone.utc)
        item = create_item(
            item_id="now-item",
            bbox=[0, 0, 1, 1],
        )
        after = datetime.now(timezone.utc)

        assert before <= item.datetime <= after

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
                    media_type="application/x-parquet",
                    roles=["data"],
                )
            },
        )

        assert "data" in item.assets
        assert item.assets["data"].href == "data.parquet"
        assert item.assets["data"].media_type == "application/x-parquet"


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

    @pytest.mark.unit
    def test_save_catalog(self, tmp_path: Path) -> None:
        """save_catalog writes catalog to disk."""
        catalog = pystac.Catalog(
            id="save-test",
            description="Catalog to save",
        )
        catalog_path = tmp_path / ".portolan"

        save_catalog(catalog, catalog_path)

        # Should create catalog.json
        catalog_file = catalog_path / "catalog.json"
        assert catalog_file.exists()

        # Should be valid JSON with correct structure
        data = json.loads(catalog_file.read_text())
        assert data["id"] == "save-test"
        assert data["type"] == "Catalog"

    @pytest.mark.unit
    def test_save_catalog_creates_directories(self, tmp_path: Path) -> None:
        """save_catalog creates parent directories if needed."""
        catalog = pystac.Catalog(id="nested", description="test")
        catalog_path = tmp_path / "nested" / "path" / ".portolan"

        save_catalog(catalog, catalog_path)

        assert (catalog_path / "catalog.json").exists()


class TestCollectionManagement:
    """Tests for adding collections to catalogs."""

    @pytest.mark.unit
    def test_add_collection_to_catalog(self, tmp_path: Path) -> None:
        """add_collection_to_catalog links collection to catalog."""
        catalog = pystac.Catalog(id="parent", description="Parent catalog")
        collection = create_collection(
            collection_id="child-collection",
            description="Child collection",
        )

        add_collection_to_catalog(catalog, collection)

        # Collection should be a child
        children = list(catalog.get_children())
        assert len(children) == 1
        assert children[0].id == "child-collection"

    @pytest.mark.unit
    def test_add_multiple_collections(self, tmp_path: Path) -> None:
        """add_collection_to_catalog can add multiple collections."""
        catalog = pystac.Catalog(id="parent", description="Parent")
        col1 = create_collection(collection_id="col1", description="First")
        col2 = create_collection(collection_id="col2", description="Second")

        add_collection_to_catalog(catalog, col1)
        add_collection_to_catalog(catalog, col2)

        children = list(catalog.get_children())
        assert len(children) == 2
        child_ids = {c.id for c in children}
        assert child_ids == {"col1", "col2"}


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
    def test_add_item_updates_collection_extent(self) -> None:
        """add_item_to_collection updates collection's spatial extent."""
        collection = create_collection(
            collection_id="extent-test",
            description="Test extent update",
            bbox=[0, 0, 1, 1],  # Initial small extent
        )

        # Add item with larger bbox
        item = create_item(
            item_id="large-item",
            bbox=[-10, -10, 10, 10],
        )
        add_item_to_collection(collection, item, update_extent=True)

        # Collection extent should now encompass the item
        spatial = collection.extent.spatial.bboxes[0]
        assert spatial[0] <= -10  # min_x
        assert spatial[1] <= -10  # min_y
        assert spatial[2] >= 10  # max_x
        assert spatial[3] >= 10  # max_y

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
