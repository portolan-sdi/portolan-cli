"""Tests for stac_parquet module - STAC GeoParquet generation.

TDD-first tests for optional items.parquet generation per issue #319.
These tests verify generation, threshold hints, and collection link updates.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def collection_with_items(tmp_path: Path) -> Path:
    """Create a collection with multiple STAC items for testing."""
    # Create catalog structure
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()

    # Create catalog.json
    catalog_json = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "test-catalog",
        "description": "Test catalog for stac-geoparquet",
        "links": [
            {"rel": "root", "href": "./catalog.json"},
            {"rel": "child", "href": "./landsat/collection.json"},
        ],
    }
    (catalog_root / "catalog.json").write_text(json.dumps(catalog_json, indent=2))

    # Create collection directory
    collection_dir = catalog_root / "landsat"
    collection_dir.mkdir()

    # Create 5 items (below default threshold of 100, but testable)
    item_links = []
    for i in range(5):
        item_id = f"scene-{i:03d}"
        item_dir = collection_dir / item_id
        item_dir.mkdir()

        # Create item.json
        item_json = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": item_id,
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-122.5 + i * 0.1, 37.7],
                        [-122.4 + i * 0.1, 37.7],
                        [-122.4 + i * 0.1, 37.8],
                        [-122.5 + i * 0.1, 37.8],
                        [-122.5 + i * 0.1, 37.7],
                    ]
                ],
            },
            "bbox": [-122.5 + i * 0.1, 37.7, -122.4 + i * 0.1, 37.8],
            "properties": {
                "datetime": f"2024-01-{i + 1:02d}T00:00:00Z",
                "title": f"Landsat Scene {i}",
            },
            "assets": {
                "data": {
                    "href": f"./{item_id}.tif",
                    "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                    "roles": ["data"],
                }
            },
            "links": [],
            "collection": "landsat",
        }
        (item_dir / f"{item_id}.json").write_text(json.dumps(item_json, indent=2))
        item_links.append({"rel": "item", "href": f"./{item_id}/{item_id}.json"})

    # Create collection.json
    collection_json = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "landsat",
        "description": "Landsat imagery collection",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-122.5, 37.7, -122.0, 37.8]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": [
            {"rel": "root", "href": "../catalog.json"},
            {"rel": "self", "href": "./collection.json"},
            *item_links,
        ],
    }
    (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

    return collection_dir


@pytest.fixture
def collection_with_many_items(tmp_path: Path) -> Path:
    """Create a collection with 150 items (above default threshold)."""
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()

    # Create catalog.json
    catalog_json = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "test-catalog",
        "description": "Test catalog with many items",
        "links": [
            {"rel": "root", "href": "./catalog.json"},
            {"rel": "child", "href": "./eurosat/collection.json"},
        ],
    }
    (catalog_root / "catalog.json").write_text(json.dumps(catalog_json, indent=2))

    # Create collection directory
    collection_dir = catalog_root / "eurosat"
    collection_dir.mkdir()

    # Create 150 items (above default threshold of 100)
    item_links = []
    for i in range(150):
        item_id = f"tile-{i:04d}"
        item_dir = collection_dir / item_id
        item_dir.mkdir()

        # Create minimal item.json
        item_json = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": item_id,
            "geometry": {
                "type": "Point",
                "coordinates": [i * 0.01, 45.0 + i * 0.001],
            },
            "bbox": [i * 0.01, 45.0 + i * 0.001, i * 0.01, 45.0 + i * 0.001],
            "properties": {"datetime": "2024-01-01T00:00:00Z"},
            "assets": {
                "data": {
                    "href": f"./{item_id}.tif",
                    "type": "image/tiff; application=geotiff",
                    "roles": ["data"],
                }
            },
            "links": [],
            "collection": "eurosat",
        }
        (item_dir / f"{item_id}.json").write_text(json.dumps(item_json, indent=2))
        item_links.append({"rel": "item", "href": f"./{item_id}/{item_id}.json"})

    # Create collection.json
    collection_json = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "eurosat",
        "description": "EuroSAT tiles",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[0, 45, 1.5, 45.15]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": [
            {"rel": "root", "href": "../catalog.json"},
            {"rel": "self", "href": "./collection.json"},
            *item_links,
        ],
    }
    (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

    return collection_dir


# =============================================================================
# Test: Item Count and Threshold
# =============================================================================


class TestItemCountAndThreshold:
    """Tests for counting items and threshold-based suggestions."""

    @pytest.mark.unit
    def test_count_items_in_collection(self, collection_with_items: Path) -> None:
        """Test that count_items returns correct count from collection.json links."""
        from portolan_cli.stac_parquet import count_items

        count = count_items(collection_with_items)
        assert count == 5

    @pytest.mark.unit
    def test_count_items_many_items(self, collection_with_many_items: Path) -> None:
        """Test counting many items in large collection."""
        from portolan_cli.stac_parquet import count_items

        count = count_items(collection_with_many_items)
        assert count == 150

    @pytest.mark.unit
    def test_should_suggest_parquet_below_threshold(self, collection_with_items: Path) -> None:
        """Test that should_suggest_parquet returns False below threshold."""
        from portolan_cli.stac_parquet import should_suggest_parquet

        # 5 items, threshold 100 -> should NOT suggest
        result = should_suggest_parquet(collection_with_items, threshold=100)
        assert result is False

    @pytest.mark.unit
    def test_should_suggest_parquet_above_threshold(self, collection_with_many_items: Path) -> None:
        """Test that should_suggest_parquet returns True above threshold."""
        from portolan_cli.stac_parquet import should_suggest_parquet

        # 150 items, threshold 100 -> should suggest
        result = should_suggest_parquet(collection_with_many_items, threshold=100)
        assert result is True

    @pytest.mark.unit
    def test_should_suggest_parquet_custom_threshold(self, collection_with_items: Path) -> None:
        """Test that custom threshold is respected."""
        from portolan_cli.stac_parquet import should_suggest_parquet

        # 5 items, threshold 3 -> should suggest
        result = should_suggest_parquet(collection_with_items, threshold=3)
        assert result is True


# =============================================================================
# Test: Generate items.parquet
# =============================================================================


class TestGenerateItemsParquet:
    """Tests for generating items.parquet from STAC items."""

    @pytest.mark.unit
    def test_generate_items_parquet_creates_file(self, collection_with_items: Path) -> None:
        """Test that generate_items_parquet creates items.parquet file."""
        from portolan_cli.stac_parquet import generate_items_parquet

        parquet_path = generate_items_parquet(collection_with_items)

        assert parquet_path.exists()
        assert parquet_path.name == "items.parquet"
        assert parquet_path.parent == collection_with_items

    @pytest.mark.unit
    def test_generate_items_parquet_is_valid_geoparquet(self, collection_with_items: Path) -> None:
        """Test that generated file is valid GeoParquet with geometry column."""
        import pyarrow.parquet as pq

        from portolan_cli.stac_parquet import generate_items_parquet

        parquet_path = generate_items_parquet(collection_with_items)

        # Read and verify structure
        table = pq.read_table(parquet_path)
        assert "geometry" in table.column_names
        assert "id" in table.column_names
        assert len(table) == 5  # 5 items

    @pytest.mark.unit
    def test_generate_items_parquet_preserves_item_ids(self, collection_with_items: Path) -> None:
        """Test that all item IDs are preserved in parquet."""
        import pyarrow.parquet as pq

        from portolan_cli.stac_parquet import generate_items_parquet

        parquet_path = generate_items_parquet(collection_with_items)

        table = pq.read_table(parquet_path)
        ids = set(table["id"].to_pylist())
        expected_ids = {f"scene-{i:03d}" for i in range(5)}
        assert ids == expected_ids

    @pytest.mark.unit
    def test_generate_items_parquet_includes_bbox(self, collection_with_items: Path) -> None:
        """Test that bbox is included in parquet columns."""
        import pyarrow.parquet as pq

        from portolan_cli.stac_parquet import generate_items_parquet

        parquet_path = generate_items_parquet(collection_with_items)

        table = pq.read_table(parquet_path)
        # stac-geoparquet stores bbox as separate columns or struct
        assert "bbox" in table.column_names or all(
            col in table.column_names
            for col in ["bbox.xmin", "bbox.ymin", "bbox.xmax", "bbox.ymax"]
        )

    @pytest.mark.unit
    def test_generate_items_parquet_handles_empty_collection(self, tmp_path: Path) -> None:
        """Test that empty collection raises appropriate error."""
        from portolan_cli.stac_parquet import generate_items_parquet

        # Create collection with no items
        collection_dir = tmp_path / "empty-collection"
        collection_dir.mkdir()

        collection_json = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "empty",
            "description": "Empty collection",
            "license": "CC-BY-4.0",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [
                {"rel": "self", "href": "./collection.json"},
            ],
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

        with pytest.raises(ValueError, match="No items found"):
            generate_items_parquet(collection_dir)


# =============================================================================
# Test: Collection Link Management
# =============================================================================


class TestCollectionLinkManagement:
    """Tests for adding/updating items.parquet link in collection.json."""

    @pytest.mark.unit
    def test_add_parquet_link_to_collection(self, collection_with_items: Path) -> None:
        """Test that items.parquet link is added to collection.json."""
        from portolan_cli.stac_parquet import (
            add_parquet_link_to_collection,
            generate_items_parquet,
        )

        # First generate the parquet
        generate_items_parquet(collection_with_items)

        # Then add the link
        add_parquet_link_to_collection(collection_with_items)

        # Verify link was added
        collection_json = json.loads((collection_with_items / "collection.json").read_text())
        parquet_links = [
            link for link in collection_json["links"] if link.get("type") == "application/x-parquet"
        ]

        assert len(parquet_links) == 1
        assert parquet_links[0]["rel"] == "items"
        assert parquet_links[0]["href"] == "./items.parquet"

    @pytest.mark.unit
    def test_add_parquet_link_idempotent(self, collection_with_items: Path) -> None:
        """Test that calling add_parquet_link twice doesn't duplicate."""
        from portolan_cli.stac_parquet import (
            add_parquet_link_to_collection,
            generate_items_parquet,
        )

        generate_items_parquet(collection_with_items)

        # Add link twice
        add_parquet_link_to_collection(collection_with_items)
        add_parquet_link_to_collection(collection_with_items)

        # Verify only one link
        collection_json = json.loads((collection_with_items / "collection.json").read_text())
        parquet_links = [
            link for link in collection_json["links"] if link.get("type") == "application/x-parquet"
        ]

        assert len(parquet_links) == 1

    @pytest.mark.unit
    def test_has_parquet_link_returns_true_when_present(self, collection_with_items: Path) -> None:
        """Test has_parquet_link returns True when link exists."""
        from portolan_cli.stac_parquet import (
            add_parquet_link_to_collection,
            generate_items_parquet,
            has_parquet_link,
        )

        generate_items_parquet(collection_with_items)
        add_parquet_link_to_collection(collection_with_items)

        assert has_parquet_link(collection_with_items) is True

    @pytest.mark.unit
    def test_has_parquet_link_returns_false_when_missing(self, collection_with_items: Path) -> None:
        """Test has_parquet_link returns False when no parquet link."""
        from portolan_cli.stac_parquet import has_parquet_link

        assert has_parquet_link(collection_with_items) is False


# =============================================================================
# Test: Full Workflow
# =============================================================================


class TestFullWorkflow:
    """Tests for the complete stac-geoparquet generation workflow."""

    @pytest.mark.unit
    def test_generate_and_link_workflow(self, collection_with_items: Path) -> None:
        """Test the full workflow: generate parquet, add link, verify."""
        from portolan_cli.stac_parquet import (
            add_parquet_link_to_collection,
            generate_items_parquet,
            has_parquet_link,
        )

        # Initial state: no parquet
        assert not (collection_with_items / "items.parquet").exists()
        assert has_parquet_link(collection_with_items) is False

        # Generate and link
        parquet_path = generate_items_parquet(collection_with_items)
        add_parquet_link_to_collection(collection_with_items)

        # Final state: parquet exists and linked
        assert parquet_path.exists()
        assert has_parquet_link(collection_with_items) is True

    @pytest.mark.unit
    def test_regenerate_parquet_overwrites(self, collection_with_items: Path) -> None:
        """Test that regenerating parquet overwrites existing file."""
        import pyarrow.parquet as pq

        from portolan_cli.stac_parquet import generate_items_parquet

        # Generate first time
        generate_items_parquet(collection_with_items)

        # Add a new item to the collection
        item_id = "scene-new"
        item_dir = collection_with_items / item_id
        item_dir.mkdir()

        item_json = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": item_id,
            "geometry": {"type": "Point", "coordinates": [-122.0, 37.5]},
            "bbox": [-122.0, 37.5, -122.0, 37.5],
            "properties": {"datetime": "2024-01-10T00:00:00Z"},
            "assets": {
                "data": {
                    "href": f"./{item_id}.tif",
                    "type": "image/tiff; application=geotiff",
                    "roles": ["data"],
                }
            },
            "links": [],
            "collection": "landsat",
        }
        (item_dir / f"{item_id}.json").write_text(json.dumps(item_json, indent=2))

        # Update collection.json with new item link
        collection_json = json.loads((collection_with_items / "collection.json").read_text())
        collection_json["links"].append({"rel": "item", "href": f"./{item_id}/{item_id}.json"})
        (collection_with_items / "collection.json").write_text(
            json.dumps(collection_json, indent=2)
        )

        # Regenerate
        parquet_path = generate_items_parquet(collection_with_items)

        # Verify new item count (5 original + 1 new = 6)
        table = pq.read_table(parquet_path)
        assert len(table) == 6
