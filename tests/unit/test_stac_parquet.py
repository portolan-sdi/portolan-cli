"""Tests for stac_parquet module - STAC GeoParquet generation.

TDD-first tests for items.parquet generation (issue #319, reshaped by #654).
These tests verify generation, the asset-only registration, and default-on
orchestration with no item-count threshold.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

# =============================================================================
# Fixtures
# =============================================================================


def _write_version_snapshot(collection_dir: Path, version: str) -> None:
    """Write a versions.json with one snapshot, as the add flow would."""
    from datetime import datetime, timezone

    from portolan_cli.versions import Asset, Version, VersionsFile, write_versions

    write_versions(
        collection_dir / "versions.json",
        VersionsFile(
            spec_version="1.0.0",
            current_version=version,
            versions=[
                Version(
                    version=version,
                    created=datetime(2024, 1, 1, tzinfo=timezone.utc),
                    message="add",
                    breaking=False,
                    assets={
                        "scene-000.tif": Asset(
                            sha256="0" * 64,
                            size_bytes=1,
                            href="landsat/scene-000/scene-000.tif",
                            mtime=0.0,
                        )
                    },
                    changes=["scene-000.tif"],
                )
            ],
        ),
    )


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
def collection_with_organizing_catalogs(tmp_path: Path) -> Path:
    """A collection whose items hang off organizing catalogs (core.md:168-170).

    ``landsat/collection.json`` links two year catalogs by ``rel="child"``, and
    each of those owns one item. The collection itself carries no ``rel="item"``
    link, which is the shape that used to read as an empty collection.
    """
    catalog_root = tmp_path / "catalog"
    collection_dir = catalog_root / "landsat"
    collection_dir.mkdir(parents=True)

    (catalog_root / "catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "stac_version": "1.1.0",
                "id": "test-catalog",
                "description": "Test catalog",
                "links": [
                    {"rel": "root", "href": "./catalog.json"},
                    {"rel": "child", "href": "./landsat/collection.json"},
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    for index, year in enumerate(("2023", "2024")):
        year_dir = collection_dir / year
        item_id = f"scene-{year}"
        item_dir = year_dir / item_id
        item_dir.mkdir(parents=True)
        (year_dir / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "stac_version": "1.1.0",
                    "id": f"landsat-{year}",
                    "description": f"Scenes from {year}",
                    "links": [
                        {"rel": "root", "href": "../../catalog.json"},
                        {"rel": "parent", "href": "../collection.json"},
                        {"rel": "item", "href": f"./{item_id}/{item_id}.json"},
                    ],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        (item_dir / f"{item_id}.json").write_text(
            json.dumps(
                {
                    "type": "Feature",
                    "stac_version": "1.1.0",
                    "id": item_id,
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [-122.5 + index, 37.7],
                                [-122.4 + index, 37.7],
                                [-122.4 + index, 37.8],
                                [-122.5 + index, 37.8],
                                [-122.5 + index, 37.7],
                            ]
                        ],
                    },
                    "bbox": [-122.5 + index, 37.7, -122.4 + index, 37.8],
                    "properties": {"datetime": f"{year}-01-01T00:00:00Z"},
                    "assets": {
                        "data": {
                            "href": f"./{item_id}.tif",
                            "type": ("image/tiff; application=geotiff; profile=cloud-optimized"),
                            "roles": ["data"],
                        }
                    },
                    "links": [
                        {"rel": "root", "href": "../../../catalog.json"},
                        {"rel": "parent", "href": "../catalog.json"},
                        {"rel": "collection", "href": "../../collection.json"},
                    ],
                    "collection": "landsat",
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    (collection_dir / "collection.json").write_text(
        json.dumps(
            {
                "type": "Collection",
                "stac_version": "1.1.0",
                "id": "landsat",
                "description": "Landsat imagery grouped by year",
                "license": "CC-BY-4.0",
                "extent": {
                    "spatial": {"bbox": [[-122.5, 37.7, -121.4, 37.8]]},
                    "temporal": {"interval": [["2023-01-01T00:00:00Z", None]]},
                },
                "links": [
                    {"rel": "root", "href": "../catalog.json"},
                    {"rel": "parent", "href": "../catalog.json"},
                    {"rel": "child", "href": "./2023/catalog.json"},
                    {"rel": "child", "href": "./2024/catalog.json"},
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

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
# Test: Item Traversal
# =============================================================================


class TestOwnedItemHrefs:
    """The item walker is public so README generation can reuse it (#713)."""

    @pytest.mark.unit
    def test_returns_href_and_resolved_path_pairs(self, collection_with_items: Path) -> None:
        """Each pair carries the written href and the file it resolves to."""
        from portolan_cli.stac_parquet import owned_item_hrefs

        owned = owned_item_hrefs(collection_with_items / "collection.json")

        assert [href for href, _ in owned] == [
            f"./scene-{i:03d}/scene-{i:03d}.json" for i in range(5)
        ]
        assert all(path.exists() for _, path in owned)

    @pytest.mark.unit
    def test_descends_organizing_catalogs(self, collection_with_organizing_catalogs: Path) -> None:
        """Items behind an organizing catalog still belong to the collection."""
        from portolan_cli.stac_parquet import owned_item_hrefs

        assert len(owned_item_hrefs(collection_with_organizing_catalogs / "collection.json")) == 2

    @pytest.mark.unit
    def test_missing_node_returns_empty(self, tmp_path: Path) -> None:
        """A collection with no collection.json owns nothing."""
        from portolan_cli.stac_parquet import owned_item_hrefs

        assert owned_item_hrefs(tmp_path / "collection.json") == []


# =============================================================================
# Test: Item Count and Threshold
# =============================================================================


class TestItemCount:
    """Tests for counting items.

    No item-count threshold exists: the spec states "No item-count threshold
    applies" (formats.md), so the count only distinguishes empty collections
    (no mirror) from item-bearing ones (#654).
    """

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
    def test_count_items_descends_organizing_catalogs(
        self, collection_with_organizing_catalogs: Path
    ) -> None:
        """Items under an organizing catalog still belong to the collection.

        Counting only the collection's own ``rel="item"`` links returned 0 here.
        """
        from portolan_cli.stac_parquet import count_items

        assert count_items(collection_with_organizing_catalogs) == 2


# =============================================================================
# Test: Generate items.parquet
# =============================================================================


class TestGenerateItemsParquet:
    """Tests for generating items.parquet from STAC items."""

    @pytest.mark.unit
    def test_mirror_includes_items_under_organizing_catalogs(
        self, collection_with_organizing_catalogs: Path
    ) -> None:
        """The mirror must carry every item the collection owns, however nested.

        Reading only the collection's own item links raised "No items found".
        """
        import pyarrow.parquet as pq

        from portolan_cli.stac_parquet import generate_items_parquet

        parquet_path = generate_items_parquet(collection_with_organizing_catalogs)

        ids = pq.read_table(parquet_path, columns=["id"]).column("id").to_pylist()
        assert sorted(ids) == ["scene-2023", "scene-2024"]

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


class TestMirrorRegistration:
    """Tests for registering the item mirror in collection.json.

    Spec formats.md: "That single registration is the whole requirement; no
    ``rel: 'items'`` link is needed." The asset is the only registration; a
    legacy link from an older catalog is removed on refresh (#654).
    """

    @pytest.mark.unit
    def test_register_mirror_writes_no_link(self, collection_with_items: Path) -> None:
        """Registration writes the collection-level asset and no link at all."""
        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)
        register_mirror_asset(collection_with_items)

        collection_json = json.loads((collection_with_items / "collection.json").read_text())
        parquet_links = [
            link
            for link in collection_json["links"]
            if link.get("type") == "application/vnd.apache.parquet"
        ]
        assert parquet_links == []
        assert collection_json["assets"]["geoparquet-items"]["href"] == "./items.parquet"

    @pytest.mark.unit
    def test_register_mirror_removes_legacy_link(self, collection_with_items: Path) -> None:
        """A rel="items" link written by an older CLI is removed on refresh."""
        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)

        collection_json_path = collection_with_items / "collection.json"
        data = json.loads(collection_json_path.read_text())
        data["links"].append(
            {
                "rel": "items",
                "href": "./items.parquet",
                "type": "application/vnd.apache.parquet",
                "title": "STAC items as GeoParquet",
            }
        )
        collection_json_path.write_text(json.dumps(data))

        register_mirror_asset(collection_with_items)

        collection_json = json.loads(collection_json_path.read_text())
        assert all(link.get("rel") != "items" for link in collection_json["links"])

    @pytest.mark.unit
    def test_register_mirror_idempotent(self, collection_with_items: Path) -> None:
        """Registering twice keeps exactly one asset and still no link."""
        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)
        register_mirror_asset(collection_with_items)
        register_mirror_asset(collection_with_items)

        collection_json = json.loads((collection_with_items / "collection.json").read_text())
        mirror_assets = [
            asset
            for asset in collection_json["assets"].values()
            if asset.get("href") == "./items.parquet"
        ]
        assert len(mirror_assets) == 1
        assert all(link.get("rel") != "items" for link in collection_json["links"])

    @pytest.mark.unit
    def test_has_mirror_asset_returns_true_when_present(self, collection_with_items: Path) -> None:
        """has_mirror_asset returns True once the asset is registered."""
        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            has_mirror_asset,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)
        register_mirror_asset(collection_with_items)

        assert has_mirror_asset(collection_with_items) is True

    @pytest.mark.unit
    def test_has_mirror_asset_detects_legacy_link_only_catalog(
        self, collection_with_items: Path
    ) -> None:
        """A legacy catalog registered only via the link still counts as mirrored."""
        from portolan_cli.stac_parquet import has_mirror_asset

        collection_json_path = collection_with_items / "collection.json"
        data = json.loads(collection_json_path.read_text())
        data["links"].append(
            {
                "rel": "items",
                "href": "./items.parquet",
                "type": "application/vnd.apache.parquet",
            }
        )
        collection_json_path.write_text(json.dumps(data))

        assert has_mirror_asset(collection_with_items) is True

    @pytest.mark.unit
    def test_has_mirror_asset_returns_false_when_missing(self, collection_with_items: Path) -> None:
        """has_mirror_asset returns False when nothing registers the mirror."""
        from portolan_cli.stac_parquet import has_mirror_asset

        assert has_mirror_asset(collection_with_items) is False


# =============================================================================
# Test: Full Workflow
# =============================================================================


class TestFullWorkflow:
    """Tests for the complete stac-geoparquet generation workflow."""

    @pytest.mark.unit
    def test_generate_and_register_workflow(self, collection_with_items: Path) -> None:
        """Test the full workflow: generate parquet, register asset, verify."""
        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            has_mirror_asset,
            register_mirror_asset,
        )

        # Initial state: no parquet
        assert not (collection_with_items / "items.parquet").exists()
        assert has_mirror_asset(collection_with_items) is False

        # Generate and register
        parquet_path = generate_items_parquet(collection_with_items)
        register_mirror_asset(collection_with_items)

        # Final state: parquet exists and registered
        assert parquet_path.exists()
        assert has_mirror_asset(collection_with_items) is True

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


# =============================================================================
# Test: Error Handling
# =============================================================================


class TestErrorHandling:
    """Tests for error handling in stac-parquet module."""

    @pytest.mark.unit
    def test_stale_item_link_raises_error(self, tmp_path: Path) -> None:
        """Test that missing item files raise ValueError with clear message."""
        from portolan_cli.stac_parquet import generate_items_parquet

        # Create collection with item link but no actual item file
        collection_dir = tmp_path / "stale-collection"
        collection_dir.mkdir()

        collection_json = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "stale",
            "description": "Collection with stale links",
            "license": "CC-BY-4.0",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [
                {"rel": "self", "href": "./collection.json"},
                {"rel": "item", "href": "./missing-item/item.json"},  # This doesn't exist
            ],
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

        with pytest.raises(ValueError, match="stale item links"):
            generate_items_parquet(collection_dir)


# =============================================================================
# Test: Collection-Level Asset
# =============================================================================


class TestCollectionLevelAsset:
    """Tests for collection-level asset."""

    @pytest.mark.unit
    def test_register_mirror_creates_collection_asset(self, collection_with_items: Path) -> None:
        """The asset carries the spec role and media type (PORTO-FMT-041)."""
        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)
        register_mirror_asset(collection_with_items)

        collection_json = json.loads((collection_with_items / "collection.json").read_text())

        assert "assets" in collection_json
        assert "geoparquet-items" in collection_json["assets"]

        asset = collection_json["assets"]["geoparquet-items"]
        assert asset["href"] == "./items.parquet"
        assert asset["type"] == "application/vnd.apache.parquet"
        assert asset["roles"] == ["collection-mirror"]

    @pytest.mark.unit
    def test_collection_asset_idempotent(self, collection_with_items: Path) -> None:
        """Registering twice doesn't duplicate the asset."""
        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)

        register_mirror_asset(collection_with_items)
        register_mirror_asset(collection_with_items)

        collection_json = json.loads((collection_with_items / "collection.json").read_text())

        # Should have exactly one parquet asset
        parquet_assets = [
            (k, v)
            for k, v in collection_json.get("assets", {}).items()
            if v.get("href") == "./items.parquet"
        ]
        assert len(parquet_assets) == 1

    @pytest.mark.unit
    def test_legacy_asset_is_upgraded_to_the_spec_role(self, collection_with_items: Path) -> None:
        """An asset written before the spec role existed is upgraded in place.

        The undefined community role ``stac-items`` is dropped and the spec
        role ``collection-mirror`` is added; unrelated assets stay untouched.
        """
        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)
        register_mirror_asset(collection_with_items)

        collection_json_path = collection_with_items / "collection.json"
        data = json.loads(collection_json_path.read_text())
        # Simulate a catalog written by an older version: community role only,
        # plus an unrelated asset that must stay untouched.
        data["assets"]["geoparquet-items"]["roles"] = ["stac-items"]
        data["assets"]["thumbnail"] = {"href": "./thumb.png", "roles": ["thumbnail"]}
        collection_json_path.write_text(json.dumps(data, indent=2))

        register_mirror_asset(collection_with_items)

        data = json.loads(collection_json_path.read_text())
        roles = data["assets"]["geoparquet-items"]["roles"]
        assert roles == ["collection-mirror"]
        assert data["assets"]["thumbnail"]["roles"] == ["thumbnail"]

    @pytest.mark.unit
    def test_remove_mirror_removes_asset_and_legacy_link(self, collection_with_items: Path) -> None:
        """Removal drops the asset and any legacy rel="items" link."""
        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
            remove_mirror_from_collection,
        )

        generate_items_parquet(collection_with_items)
        register_mirror_asset(collection_with_items)

        # Add a legacy link so removal has to clean both registrations
        collection_json_path = collection_with_items / "collection.json"
        data = json.loads(collection_json_path.read_text())
        data["links"].append(
            {"rel": "items", "href": "./items.parquet", "type": "application/vnd.apache.parquet"}
        )
        collection_json_path.write_text(json.dumps(data))

        result = remove_mirror_from_collection(collection_with_items)
        assert result is True

        collection_json = json.loads(collection_json_path.read_text())
        assert "geoparquet-items" not in collection_json.get("assets", {})
        assert all(link.get("rel") != "items" for link in collection_json["links"])


# =============================================================================
# Test: file:size / file:checksum on the mirror asset (issue #710)
# =============================================================================


def _mirror_asset(collection_dir: Path) -> dict[str, object]:
    """The geoparquet-items asset as it stands on disk."""
    data = json.loads((collection_dir / "collection.json").read_text())
    asset: dict[str, object] = data["assets"]["geoparquet-items"]
    return asset


def _append_item(collection_dir: Path, item_id: str) -> None:
    """Add one more item so the next items.parquet differs from the last."""
    item_dir = collection_dir / item_id
    item_dir.mkdir()
    item_json = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": item_id,
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [[-121.5, 37.7], [-121.4, 37.7], [-121.4, 37.8], [-121.5, 37.8], [-121.5, 37.7]]
            ],
        },
        "bbox": [-121.5, 37.7, -121.4, 37.8],
        "properties": {"datetime": "2024-02-01T00:00:00Z", "title": f"Landsat {item_id}"},
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

    collection_path = collection_dir / "collection.json"
    data = json.loads(collection_path.read_text())
    data["links"].append({"rel": "item", "href": f"./{item_id}/{item_id}.json"})
    collection_path.write_text(json.dumps(data, indent=2))


class TestMirrorAssetFileFields:
    """The mirror asset must publish file:size and file:checksum (issue #710).

    PTL-AST-003 warns on their absence (PORTO-CORE-028 is a SHOULD), and
    ``portolan check --strict`` escalates that warning to a non-zero exit.
    PORTO-CORE-030 then makes a published value a claim about the bytes, and
    rashid's data pass — on by default — reports a mismatch as an ERROR. So a
    value left stale by a regeneration is worse than one that was never written.
    """

    @pytest.mark.unit
    def test_asset_carries_file_size_and_checksum(self, collection_with_items: Path) -> None:
        import hashlib

        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)
        register_mirror_asset(collection_with_items)

        raw = (collection_with_items / "items.parquet").read_bytes()
        asset = _mirror_asset(collection_with_items)
        assert asset["file:size"] == len(raw)
        assert asset["file:checksum"] == f"1220{hashlib.sha256(raw).hexdigest()}"

    @pytest.mark.unit
    def test_checksum_is_a_well_formed_multihash(self, collection_with_items: Path) -> None:
        # PORTO-CORE-029 / PTL-AST-004. Assert with rashid's own predicate so the
        # test cannot drift away from the rule that gates the catalog.
        from rashid.api import is_well_formed_multihash

        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)
        register_mirror_asset(collection_with_items)

        assert is_well_formed_multihash(_mirror_asset(collection_with_items)["file:checksum"])

    @pytest.mark.unit
    def test_regenerating_the_parquet_refreshes_the_checksum(
        self, collection_with_items: Path
    ) -> None:
        """The PORTO-CORE-030 guard: registration is idempotent by presence.

        Stamping only the creation branch leaves the first run's digest sitting
        against bytes the second run overwrote, which turns a warning into an error.
        """
        import hashlib

        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)
        register_mirror_asset(collection_with_items)
        first = dict(_mirror_asset(collection_with_items))

        _append_item(collection_with_items, "scene-005")
        generate_items_parquet(collection_with_items)
        register_mirror_asset(collection_with_items)

        raw = (collection_with_items / "items.parquet").read_bytes()
        asset = _mirror_asset(collection_with_items)
        assert asset["file:checksum"] != first["file:checksum"]
        assert asset["file:checksum"] == f"1220{hashlib.sha256(raw).hexdigest()}"
        assert asset["file:size"] == len(raw)

    @pytest.mark.unit
    def test_an_older_asset_without_the_fields_is_backfilled(
        self, collection_with_items: Path
    ) -> None:
        """A catalog written before this fix must gain the fields on the next run."""
        import hashlib

        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)
        collection_path = collection_with_items / "collection.json"
        data = json.loads(collection_path.read_text())
        data["assets"] = {
            "geoparquet-items": {
                "href": "./items.parquet",
                "type": "application/vnd.apache.parquet",
                "roles": ["stac-items"],
            }
        }
        collection_path.write_text(json.dumps(data, indent=2))

        register_mirror_asset(collection_with_items)

        raw = (collection_with_items / "items.parquet").read_bytes()
        asset = _mirror_asset(collection_with_items)
        assert asset["file:size"] == len(raw)
        assert asset["file:checksum"] == f"1220{hashlib.sha256(raw).hexdigest()}"

    @pytest.mark.unit
    def test_fields_are_absent_when_the_parquet_was_never_written(
        self, collection_with_items: Path
    ) -> None:
        """Never fabricate. Registration still happens; the claim does not."""
        from portolan_cli.stac_parquet import register_mirror_asset

        register_mirror_asset(collection_with_items)

        asset = _mirror_asset(collection_with_items)
        assert "file:size" not in asset
        assert "file:checksum" not in asset

    @pytest.mark.unit
    def test_stale_fields_are_stripped_when_the_parquet_disappears(
        self, collection_with_items: Path
    ) -> None:
        """Absence is a PTL-AST-003 warning; a claim about missing bytes is an error."""
        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)
        register_mirror_asset(collection_with_items)
        assert "file:size" in _mirror_asset(collection_with_items)

        (collection_with_items / "items.parquet").unlink()
        register_mirror_asset(collection_with_items)

        asset = _mirror_asset(collection_with_items)
        assert "file:size" not in asset
        assert "file:checksum" not in asset

    @pytest.mark.unit
    def test_a_foreign_href_is_not_stamped_from_items_parquet(
        self, collection_with_items: Path
    ) -> None:
        """The asset matches by key too, so stamp each asset from its own href."""
        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)
        collection_path = collection_with_items / "collection.json"
        data = json.loads(collection_path.read_text())
        data["assets"] = {
            "geoparquet-items": {
                "href": "./elsewhere.parquet",
                "type": "application/vnd.apache.parquet",
                "roles": ["stac-items"],
            }
        }
        collection_path.write_text(json.dumps(data, indent=2))

        register_mirror_asset(collection_with_items)

        asset = _mirror_asset(collection_with_items)
        assert "file:size" not in asset
        assert "file:checksum" not in asset

    @pytest.mark.unit
    def test_the_file_extension_is_declared_once(self, collection_with_items: Path) -> None:
        from portolan_cli.stac import EXTENSION_URLS
        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)
        register_mirror_asset(collection_with_items)
        register_mirror_asset(collection_with_items)

        data = json.loads((collection_with_items / "collection.json").read_text())
        assert data["stac_extensions"].count(EXTENSION_URLS["file"]) == 1

    @pytest.mark.unit
    def test_a_second_call_without_regeneration_rewrites_nothing(
        self, collection_with_items: Path
    ) -> None:
        """Refreshing must not mark the collection modified when nothing moved."""
        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)
        register_mirror_asset(collection_with_items)
        collection_path = collection_with_items / "collection.json"
        before = collection_path.read_text()

        register_mirror_asset(collection_with_items)

        assert collection_path.read_text() == before


class TestUnverifiableHrefsAreLeftAlone:
    """Only a missing local file proves a published claim is false.

    A relative href that resolves to nothing is evidence. An absolute or remote
    href is not: those bytes live somewhere this process cannot read, so the
    values there may be correct and measured by whoever wrote them. Stripping
    them would destroy true metadata. ``_local_asset_path`` in
    ``validation.fixers`` skips the same hrefs.
    """

    @staticmethod
    def _write_mirror(collection_dir: Path, asset: dict[str, object]) -> None:
        collection_path = collection_dir / "collection.json"
        data = json.loads(collection_path.read_text())
        data["assets"] = {"geoparquet-items": asset}
        collection_path.write_text(json.dumps(data, indent=2))

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "href",
        [
            "https://data.example.org/imagery/items.parquet",
            "s3://example-bucket/imagery/items.parquet",
            "/srv/catalog/imagery/items.parquet",
        ],
        ids=["https", "s3", "absolute-local"],
    )
    def test_an_unreadable_href_keeps_its_published_claim(
        self, collection_with_items: Path, href: str
    ) -> None:
        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)
        claim = f"1220{'ab' * 32}"
        self._write_mirror(
            collection_with_items,
            {
                "href": href,
                "type": "application/vnd.apache.parquet",
                "roles": ["stac-items"],
                "file:size": 4096,
                "file:checksum": claim,
            },
        )

        register_mirror_asset(collection_with_items)

        asset = _mirror_asset(collection_with_items)
        assert asset["file:size"] == 4096
        assert asset["file:checksum"] == claim

    @pytest.mark.unit
    def test_a_directory_href_keeps_its_published_claim(self, collection_with_items: Path) -> None:
        """A FileGDB asset is a directory; compute_checksum rejects one outright."""
        from portolan_cli.stac_parquet import register_mirror_asset

        (collection_with_items / "layers.gdb").mkdir()
        claim = f"1220{'cd' * 32}"
        self._write_mirror(
            collection_with_items,
            {
                "href": "./layers.gdb",
                "type": "application/vnd.apache.parquet",
                "roles": ["stac-items"],
                "file:size": 8192,
                "file:checksum": claim,
            },
        )

        register_mirror_asset(collection_with_items)

        asset = _mirror_asset(collection_with_items)
        assert asset["file:size"] == 8192
        assert asset["file:checksum"] == claim

    @pytest.mark.unit
    def test_a_null_valued_field_is_still_removed(self, collection_with_items: Path) -> None:
        """Membership decides, not the popped value: null is a field, and it must go."""
        from portolan_cli.stac_parquet import register_mirror_asset

        self._write_mirror(
            collection_with_items,
            {
                "href": "./items.parquet",
                "type": "application/vnd.apache.parquet",
                "roles": ["stac-items"],
                "file:size": None,
                "file:checksum": None,
            },
        )

        register_mirror_asset(collection_with_items)

        asset = _mirror_asset(collection_with_items)
        assert "file:size" not in asset
        assert "file:checksum" not in asset


class TestFileExtensionDeclaration:
    """The declaration follows the fields, in both directions."""

    @pytest.mark.unit
    def test_it_is_withdrawn_when_the_last_file_field_goes(
        self, collection_with_items: Path
    ) -> None:
        from portolan_cli.stac import EXTENSION_URLS
        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)
        register_mirror_asset(collection_with_items)
        collection_path = collection_with_items / "collection.json"
        assert EXTENSION_URLS["file"] in json.loads(collection_path.read_text())["stac_extensions"]

        (collection_with_items / "items.parquet").unlink()
        register_mirror_asset(collection_with_items)

        data = json.loads(collection_path.read_text())
        assert EXTENSION_URLS["file"] not in data["stac_extensions"]

    @pytest.mark.unit
    def test_it_survives_while_item_assets_still_carry_the_fields(
        self, collection_with_items: Path
    ) -> None:
        """portolan:asset_count covers items, and the statistics writer redeclares
        from that wider scope. Removing the URI here would start a write war."""
        from portolan_cli.stac import EXTENSION_URLS
        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)
        register_mirror_asset(collection_with_items)
        collection_path = collection_with_items / "collection.json"
        data = json.loads(collection_path.read_text())
        data["portolan:asset_count"] = 3
        collection_path.write_text(json.dumps(data, indent=2))

        (collection_with_items / "items.parquet").unlink()
        register_mirror_asset(collection_with_items)

        data = json.loads(collection_path.read_text())
        assert EXTENSION_URLS["file"] in data["stac_extensions"]

    @pytest.mark.unit
    def test_a_malformed_extension_list_does_not_crash(self, collection_with_items: Path) -> None:
        """stac_extensions as a string is invalid STAC; report it, do not append to it."""
        from portolan_cli.stac_parquet import (
            generate_items_parquet,
            register_mirror_asset,
        )

        generate_items_parquet(collection_with_items)
        collection_path = collection_with_items / "collection.json"
        data = json.loads(collection_path.read_text())
        data["stac_extensions"] = "https://stac-extensions.github.io/file/v2.1.0/schema.json"
        collection_path.write_text(json.dumps(data, indent=2))

        register_mirror_asset(collection_with_items)

        data = json.loads(collection_path.read_text())
        assert data["stac_extensions"] == (
            "https://stac-extensions.github.io/file/v2.1.0/schema.json"
        )


# =============================================================================
# Test: generate_parquet_mirrors (post-add orchestration, issues #620, #654)
# =============================================================================


class TestGenerateParquetMirrors:
    """Tests for the post-add mirror orchestration (#620, reshaped by #654).

    The spec applies no item-count threshold and FMT-040 is a SHOULD, so the
    mirror generates by default for every item-bearing collection. The only
    gates left are ``parquet.enabled`` (opt-out) and an item count of zero.
    """

    @pytest.mark.unit
    def test_explicit_flag_generates_parquet(self, collection_with_items: Path) -> None:
        """--stac-geoparquet (generate_parquet=True) generates items.parquet."""
        from portolan_cli.stac_parquet import generate_parquet_mirrors

        catalog_root = collection_with_items.parent
        generate_parquet_mirrors(
            catalog_root,
            {"landsat"},
            generate_parquet=True,
            verbose=False,
        )

        assert (collection_with_items / "items.parquet").exists()

    @pytest.mark.unit
    def test_default_generates_without_flag_or_config(self, collection_with_items: Path) -> None:
        """A plain add publishes the mirror: enabled by default, no threshold.

        Pre-#654 behavior gated this on parquet.enabled (default False) and a
        100-item threshold, so a 5-item collection got nothing.
        """
        from portolan_cli.stac_parquet import generate_parquet_mirrors

        catalog_root = collection_with_items.parent
        generate_parquet_mirrors(
            catalog_root,
            {"landsat"},
            generate_parquet=False,
            verbose=False,
        )

        assert (collection_with_items / "items.parquet").exists()
        collection_json = json.loads((collection_with_items / "collection.json").read_text())
        assert "geoparquet-items" in collection_json["assets"]

    @pytest.mark.unit
    def test_config_opt_out_disables_generation(self, collection_with_items: Path) -> None:
        """parquet.enabled=false suppresses the default generation."""
        from portolan_cli.stac_parquet import generate_parquet_mirrors

        catalog_root = collection_with_items.parent
        (catalog_root / ".portolan").mkdir(exist_ok=True)
        (catalog_root / ".portolan" / "config.yaml").write_text("parquet:\n  enabled: false\n")

        generate_parquet_mirrors(
            catalog_root,
            {"landsat"},
            generate_parquet=False,
            verbose=False,
        )

        assert not (collection_with_items / "items.parquet").exists()

    @pytest.mark.unit
    def test_explicit_flag_overrides_opt_out(self, collection_with_items: Path) -> None:
        """--stac-geoparquet generates even when config opts out."""
        from portolan_cli.stac_parquet import generate_parquet_mirrors

        catalog_root = collection_with_items.parent
        (catalog_root / ".portolan").mkdir(exist_ok=True)
        (catalog_root / ".portolan" / "config.yaml").write_text("parquet:\n  enabled: false\n")

        generate_parquet_mirrors(
            catalog_root,
            {"landsat"},
            generate_parquet=True,
            verbose=False,
        )

        assert (collection_with_items / "items.parquet").exists()

    @pytest.mark.unit
    def test_missing_collection_json_is_skipped(self, tmp_path: Path) -> None:
        """An affected collection without collection.json is a no-op, not an error."""
        from portolan_cli.stac_parquet import generate_parquet_mirrors

        (tmp_path / "catalog.json").write_text("{}")
        # Should not raise even though 'ghost' has no collection.json
        generate_parquet_mirrors(
            tmp_path,
            {"ghost"},
            generate_parquet=True,
            verbose=False,
        )
        assert not (tmp_path / "ghost" / "items.parquet").exists()

    @pytest.mark.unit
    def test_empty_affected_is_noop(self, tmp_path: Path) -> None:
        """No affected collections returns without touching the filesystem."""
        from portolan_cli.stac_parquet import generate_parquet_mirrors

        # No exception, nothing created
        generate_parquet_mirrors(tmp_path, set(), generate_parquet=True, verbose=False)

    @pytest.mark.unit
    def test_itemless_collection_gets_no_mirror(self, tmp_path: Path) -> None:
        """A collection with only collection-level assets publishes no mirror."""
        from portolan_cli.stac_parquet import generate_parquet_mirrors

        collection_dir = tmp_path / "vectors"
        collection_dir.mkdir()
        (collection_dir / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "stac_version": "1.1.0",
                    "id": "vectors",
                    "description": "Collection-level vector asset only",
                    "license": "CC-BY-4.0",
                    "extent": {
                        "spatial": {"bbox": [[0, 0, 1, 1]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                    "assets": {
                        "vectors": {"href": "./vectors.parquet", "roles": ["data"]},
                    },
                }
            )
        )

        generate_parquet_mirrors(tmp_path, {"vectors"}, generate_parquet=False, verbose=False)

        assert not (collection_dir / "items.parquet").exists()

    @pytest.mark.unit
    def test_mirror_folds_into_the_version_add_just_wrote(
        self, collection_with_items: Path
    ) -> None:
        """A versioned collection's mirror amends that snapshot, not a new one.

        Pre-fix behavior: track_parquet_in_versions bumped a patch version of
        its own, so one `add` produced two versions (the #683 family).
        """
        from portolan_cli.stac_parquet import generate_parquet_mirrors
        from portolan_cli.versions import read_versions

        catalog_root = collection_with_items.parent
        _write_version_snapshot(collection_with_items, "1.0.0")

        generate_parquet_mirrors(
            catalog_root,
            {"landsat"},
            generate_parquet=False,
            verbose=False,
            versioned_collections={"landsat"},
        )

        versions = read_versions(collection_with_items / "versions.json")
        assert [v.version for v in versions.versions] == ["1.0.0"]
        assert "items.parquet" in versions.versions[-1].assets

    @pytest.mark.unit
    def test_unversioned_collection_gets_its_own_version(self, collection_with_items: Path) -> None:
        """A skipped collection's backfilled mirror creates a new version.

        Amending a snapshot that may already be published would rewrite
        history, so only versions written in this run are amended.
        """
        from portolan_cli.stac_parquet import generate_parquet_mirrors
        from portolan_cli.versions import read_versions

        catalog_root = collection_with_items.parent
        _write_version_snapshot(collection_with_items, "1.0.0")

        generate_parquet_mirrors(
            catalog_root,
            {"landsat"},
            generate_parquet=False,
            verbose=False,
            versioned_collections=set(),
        )

        versions = read_versions(collection_with_items / "versions.json")
        assert [v.version for v in versions.versions] == ["1.0.0", "1.0.1"]
        assert "items.parquet" in versions.versions[-1].assets

    @pytest.mark.unit
    def test_unchanged_mirror_is_not_reversioned(self, collection_with_items: Path) -> None:
        """Re-adding with identical items records nothing new in versions.json."""
        from portolan_cli.stac_parquet import generate_parquet_mirrors
        from portolan_cli.versions import read_versions

        catalog_root = collection_with_items.parent
        _write_version_snapshot(collection_with_items, "1.0.0")

        generate_parquet_mirrors(
            catalog_root,
            {"landsat"},
            generate_parquet=False,
            verbose=False,
            versioned_collections={"landsat"},
        )
        first = read_versions(collection_with_items / "versions.json")

        # Second run: same items, regenerated mirror, no versioned snapshot
        generate_parquet_mirrors(
            catalog_root,
            {"landsat"},
            generate_parquet=False,
            verbose=False,
            versioned_collections=set(),
        )
        second = read_versions(collection_with_items / "versions.json")

        assert [v.version for v in second.versions] == [v.version for v in first.versions]

    @pytest.mark.unit
    def test_explicit_failure_reraises(self, collection_with_items: Path) -> None:
        """An explicit --stac-geoparquet generation failure propagates (fails the command)."""
        from unittest.mock import patch

        from portolan_cli.stac_parquet import generate_parquet_mirrors

        catalog_root = collection_with_items.parent
        with patch(
            "portolan_cli.stac_parquet.generate_items_parquet",
            side_effect=RuntimeError("boom"),
        ):
            with pytest.raises(RuntimeError, match="boom"):
                generate_parquet_mirrors(
                    catalog_root,
                    {"landsat"},
                    generate_parquet=True,
                    verbose=False,
                )

    @pytest.mark.unit
    def test_auto_failure_warns_not_raises(self, collection_with_items: Path) -> None:
        """A default-generation failure warns instead of raising."""
        from unittest.mock import patch

        from portolan_cli.stac_parquet import generate_parquet_mirrors

        catalog_root = collection_with_items.parent

        with patch(
            "portolan_cli.stac_parquet.generate_items_parquet",
            side_effect=RuntimeError("boom"),
        ):
            # generate_parquet=False → default path → should NOT raise
            generate_parquet_mirrors(
                catalog_root,
                {"landsat"},
                generate_parquet=False,
                verbose=False,
            )
        assert not (collection_with_items / "items.parquet").exists()
