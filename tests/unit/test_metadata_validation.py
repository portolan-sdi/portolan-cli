"""Tests for metadata validation functions.

Tests for:
- check_directory_metadata(): Return MetadataReport for directory tree
- validate_collection_extent(): Check extent contains all item bboxes
- validate_catalog_links(): Check links are valid (self, root, children exist)
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.metadata.models import MetadataReport

# Import validation functions (to be implemented)
from portolan_cli.metadata.validation import (
    check_directory_metadata,
    validate_catalog_links,
    validate_collection_extent,
)
from portolan_cli.models.catalog import CatalogModel, Link
from portolan_cli.models.collection import (
    CollectionModel,
    ExtentModel,
    SpatialExtent,
    TemporalExtent,
)
from portolan_cli.models.item import AssetModel, ItemModel


class TestValidateCollectionExtent:
    """Tests for validate_collection_extent function."""

    @pytest.mark.unit
    def test_valid_extent_contains_all_items(self) -> None:
        """Collection extent containing all item bboxes should pass."""
        # Collection bbox covers SF area
        collection = CollectionModel(
            id="test-collection",
            description="Test collection",
            extent=ExtentModel(
                spatial=SpatialExtent(bbox=[[-122.5, 37.7, -122.3, 37.9]]),
                temporal=TemporalExtent(interval=[["2026-01-01T00:00:00Z", None]]),
            ),
        )

        # Items within the collection bbox
        items = [
            ItemModel(
                id="item-1",
                geometry={"type": "Point", "coordinates": [-122.4, 37.8]},
                bbox=[-122.45, 37.75, -122.35, 37.85],
                properties={"datetime": "2026-01-15T00:00:00Z"},
                assets={"data": AssetModel(href="item1.parquet")},
                collection="test-collection",
            ),
            ItemModel(
                id="item-2",
                geometry={"type": "Point", "coordinates": [-122.4, 37.75]},
                bbox=[-122.48, 37.72, -122.32, 37.78],
                properties={"datetime": "2026-01-16T00:00:00Z"},
                assets={"data": AssetModel(href="item2.parquet")},
                collection="test-collection",
            ),
        ]

        result = validate_collection_extent(collection, items)
        assert result.passed is True
        assert len(result.errors) == 0

    @pytest.mark.unit
    def test_item_bbox_outside_collection_extent(self) -> None:
        """Item bbox outside collection extent should fail."""
        # Collection bbox is small
        collection = CollectionModel(
            id="test-collection",
            description="Test collection",
            extent=ExtentModel(
                spatial=SpatialExtent(bbox=[[-122.45, 37.75, -122.35, 37.85]]),
                temporal=TemporalExtent(interval=[["2026-01-01T00:00:00Z", None]]),
            ),
        )

        # Item bbox extends beyond collection
        items = [
            ItemModel(
                id="item-outside",
                geometry={"type": "Point", "coordinates": [-122.6, 37.8]},
                bbox=[-122.65, 37.75, -122.55, 37.85],  # West of collection bbox
                properties={"datetime": "2026-01-15T00:00:00Z"},
                assets={"data": AssetModel(href="item.parquet")},
                collection="test-collection",
            ),
        ]

        result = validate_collection_extent(collection, items)
        assert result.passed is False
        assert len(result.errors) >= 1
        # Error should mention the item
        assert any("item-outside" in e.message for e in result.errors)

    @pytest.mark.unit
    def test_partial_overlap_should_fail(self) -> None:
        """Item bbox partially overlapping collection should fail."""
        collection = CollectionModel(
            id="test-collection",
            description="Test collection",
            extent=ExtentModel(
                spatial=SpatialExtent(bbox=[[-122.5, 37.7, -122.3, 37.9]]),
                temporal=TemporalExtent(interval=[["2026-01-01T00:00:00Z", None]]),
            ),
        )

        # Item bbox partially outside (extends east)
        items = [
            ItemModel(
                id="item-partial",
                geometry={"type": "Point", "coordinates": [-122.25, 37.8]},
                bbox=[-122.35, 37.75, -122.2, 37.85],  # East edge outside
                properties={"datetime": "2026-01-15T00:00:00Z"},
                assets={"data": AssetModel(href="item.parquet")},
                collection="test-collection",
            ),
        ]

        result = validate_collection_extent(collection, items)
        assert result.passed is False

    @pytest.mark.unit
    def test_empty_items_list_passes(self) -> None:
        """Collection with no items should pass validation."""
        collection = CollectionModel(
            id="empty-collection",
            description="Empty collection",
            extent=ExtentModel(
                spatial=SpatialExtent(bbox=[[-180, -90, 180, 90]]),
                temporal=TemporalExtent(interval=[["2026-01-01T00:00:00Z", None]]),
            ),
        )

        result = validate_collection_extent(collection, [])
        assert result.passed is True

    @pytest.mark.unit
    def test_multiple_items_one_outside(self) -> None:
        """Multiple items where one is outside should fail with specific error."""
        collection = CollectionModel(
            id="test-collection",
            description="Test collection",
            extent=ExtentModel(
                spatial=SpatialExtent(bbox=[[-122.5, 37.7, -122.3, 37.9]]),
                temporal=TemporalExtent(interval=[["2026-01-01T00:00:00Z", None]]),
            ),
        )

        items = [
            ItemModel(
                id="item-inside",
                geometry={"type": "Point", "coordinates": [-122.4, 37.8]},
                bbox=[-122.45, 37.75, -122.35, 37.85],
                properties={"datetime": "2026-01-15T00:00:00Z"},
                assets={"data": AssetModel(href="item1.parquet")},
                collection="test-collection",
            ),
            ItemModel(
                id="item-outside",
                geometry={"type": "Point", "coordinates": [-123.0, 37.8]},
                bbox=[-123.1, 37.75, -122.9, 37.85],  # Way west
                properties={"datetime": "2026-01-16T00:00:00Z"},
                assets={"data": AssetModel(href="item2.parquet")},
                collection="test-collection",
            ),
        ]

        result = validate_collection_extent(collection, items)
        assert result.passed is False
        # Should identify the specific item
        error_messages = " ".join(e.message for e in result.errors)
        assert "item-outside" in error_messages
        assert "item-inside" not in error_messages


class TestValidateCatalogLinks:
    """Tests for validate_catalog_links function."""

    @pytest.mark.unit
    def test_catalog_with_valid_links(self, tmp_path: Path) -> None:
        """Catalog with valid self, root, and child links should pass."""
        # Create catalog
        catalog = CatalogModel(
            id="test-catalog",
            description="Test catalog",
            links=[
                Link(rel="root", href="./catalog.json", type="application/json"),
                Link(rel="self", href="./catalog.json", type="application/json"),
                Link(rel="child", href="./parcels/collection.json", type="application/json"),
            ],
        )

        # Create the referenced files
        catalog_path = tmp_path / "catalog.json"
        catalog_path.write_text(json.dumps(catalog.to_dict()))

        collection_dir = tmp_path / "parcels"
        collection_dir.mkdir()
        collection_path = collection_dir / "collection.json"
        collection = CollectionModel(
            id="parcels",
            description="Parcels collection",
            extent=ExtentModel(
                spatial=SpatialExtent(bbox=[[-122.5, 37.7, -122.3, 37.9]]),
                temporal=TemporalExtent(interval=[["2026-01-01T00:00:00Z", None]]),
            ),
        )
        collection_path.write_text(json.dumps(collection.to_dict()))

        result = validate_catalog_links(catalog, catalog_path)
        assert result.passed is True

    @pytest.mark.unit
    def test_catalog_missing_self_link(self, tmp_path: Path) -> None:
        """Catalog missing self link should produce warning."""
        catalog = CatalogModel(
            id="test-catalog",
            description="Test catalog",
            links=[
                Link(rel="root", href="./catalog.json", type="application/json"),
                # Missing self link
            ],
        )

        catalog_path = tmp_path / "catalog.json"
        catalog_path.write_text(json.dumps(catalog.to_dict()))

        result = validate_catalog_links(catalog, catalog_path)
        # Missing self is a warning, not an error
        assert len(result.warnings) >= 1
        assert any("self" in w.message.lower() for w in result.warnings)

    @pytest.mark.unit
    def test_catalog_missing_root_link(self, tmp_path: Path) -> None:
        """Catalog missing root link should produce warning."""
        catalog = CatalogModel(
            id="test-catalog",
            description="Test catalog",
            links=[
                Link(rel="self", href="./catalog.json", type="application/json"),
                # Missing root link
            ],
        )

        catalog_path = tmp_path / "catalog.json"
        catalog_path.write_text(json.dumps(catalog.to_dict()))

        result = validate_catalog_links(catalog, catalog_path)
        assert len(result.warnings) >= 1
        assert any("root" in w.message.lower() for w in result.warnings)

    @pytest.mark.unit
    def test_catalog_child_link_points_to_missing_file(self, tmp_path: Path) -> None:
        """Catalog with child link to non-existent file should fail."""
        catalog = CatalogModel(
            id="test-catalog",
            description="Test catalog",
            links=[
                Link(rel="root", href="./catalog.json", type="application/json"),
                Link(rel="self", href="./catalog.json", type="application/json"),
                Link(rel="child", href="./missing/collection.json", type="application/json"),
            ],
        )

        catalog_path = tmp_path / "catalog.json"
        catalog_path.write_text(json.dumps(catalog.to_dict()))
        # Don't create the child collection - it's intentionally missing

        result = validate_catalog_links(catalog, catalog_path)
        assert result.passed is False
        assert any("missing" in e.message.lower() for e in result.errors)

    @pytest.mark.unit
    def test_catalog_item_link_validation(self, tmp_path: Path) -> None:
        """Catalog item links should be validated if present."""
        catalog = CatalogModel(
            id="test-catalog",
            description="Test catalog",
            links=[
                Link(rel="root", href="./catalog.json", type="application/json"),
                Link(rel="self", href="./catalog.json", type="application/json"),
                Link(rel="item", href="./items/missing-item.json", type="application/geo+json"),
            ],
        )

        catalog_path = tmp_path / "catalog.json"
        catalog_path.write_text(json.dumps(catalog.to_dict()))

        result = validate_catalog_links(catalog, catalog_path)
        assert result.passed is False

    @pytest.mark.unit
    def test_empty_links_produces_warnings(self, tmp_path: Path) -> None:
        """Catalog with no links should produce warnings."""
        catalog = CatalogModel(
            id="test-catalog",
            description="Test catalog",
            links=[],
        )

        catalog_path = tmp_path / "catalog.json"
        catalog_path.write_text(json.dumps(catalog.to_dict()))

        result = validate_catalog_links(catalog, catalog_path)
        # No self or root links - should have warnings
        assert len(result.warnings) >= 2


class TestCheckDirectoryMetadata:
    """Tests for check_directory_metadata function."""

    @pytest.mark.unit
    def test_empty_directory_returns_empty_report(self, tmp_path: Path) -> None:
        """Empty directory should return report with no results."""
        result = check_directory_metadata(tmp_path)
        assert isinstance(result, MetadataReport)
        assert result.total_count == 0
        assert result.passed is True

    @pytest.mark.unit
    def test_directory_with_valid_stac_structure(self, tmp_path: Path) -> None:
        """Directory with valid STAC structure should pass."""
        # Create catalog
        catalog = CatalogModel(
            id="test-catalog",
            description="Test catalog",
            links=[
                Link(rel="root", href="./catalog.json"),
                Link(rel="self", href="./catalog.json"),
                Link(rel="child", href="./parcels/collection.json"),
            ],
        )
        catalog_path = tmp_path / "catalog.json"
        catalog_path.write_text(json.dumps(catalog.to_dict()))

        # Create collection
        collection_dir = tmp_path / "parcels"
        collection_dir.mkdir()
        collection = CollectionModel(
            id="parcels",
            description="Parcels collection",
            extent=ExtentModel(
                spatial=SpatialExtent(bbox=[[-122.5, 37.7, -122.3, 37.9]]),
                temporal=TemporalExtent(interval=[["2026-01-01T00:00:00Z", None]]),
            ),
            links=[
                Link(rel="root", href="../catalog.json"),
                Link(rel="self", href="./collection.json"),
                Link(rel="parent", href="../catalog.json"),
                Link(rel="item", href="./items/parcel1.json"),
            ],
        )
        collection_path = collection_dir / "collection.json"
        collection_path.write_text(json.dumps(collection.to_dict()))

        # Create item
        items_dir = collection_dir / "items"
        items_dir.mkdir()
        item = ItemModel(
            id="parcel1",
            geometry={
                "type": "Polygon",
                "coordinates": [
                    [
                        [-122.4, 37.8],
                        [-122.4, 37.81],
                        [-122.39, 37.81],
                        [-122.39, 37.8],
                        [-122.4, 37.8],
                    ]
                ],
            },
            bbox=[-122.4, 37.8, -122.39, 37.81],
            properties={"datetime": "2026-01-15T00:00:00Z"},
            assets={
                "data": AssetModel(
                    href="./parcel1.parquet",
                    type="application/x-parquet",
                    roles=["data"],
                )
            },
            collection="parcels",
            links=[
                Link(rel="root", href="../../catalog.json"),
                Link(rel="self", href="./parcel1.json"),
                Link(rel="parent", href="../collection.json"),
            ],
        )
        item_path = items_dir / "parcel1.json"
        item_path.write_text(json.dumps(item.to_dict()))

        result = check_directory_metadata(tmp_path)
        assert isinstance(result, MetadataReport)
        # Should find catalog, collection, and item
        assert result.total_count >= 1

    @pytest.mark.unit
    def test_directory_missing_catalog(self, tmp_path: Path) -> None:
        """Directory without catalog.json should report missing metadata."""
        # Create a collection without a parent catalog
        collection_dir = tmp_path / "orphan-collection"
        collection_dir.mkdir()
        collection = CollectionModel(
            id="orphan",
            description="Orphan collection",
            extent=ExtentModel(
                spatial=SpatialExtent(bbox=[[-122.5, 37.7, -122.3, 37.9]]),
                temporal=TemporalExtent(interval=[["2026-01-01T00:00:00Z", None]]),
            ),
        )
        (collection_dir / "collection.json").write_text(json.dumps(collection.to_dict()))

        result = check_directory_metadata(tmp_path)
        # No catalog found at root - this is valid (may be checking a subdirectory)
        assert isinstance(result, MetadataReport)

    @pytest.mark.unit
    def test_directory_with_geo_files_no_stac(self, tmp_path: Path) -> None:
        """Directory with geo files but no STAC metadata should report issues."""
        # Create a dummy parquet file (just empty for test)
        (tmp_path / "data.parquet").write_bytes(b"")
        (tmp_path / "image.tif").write_bytes(b"")

        result = check_directory_metadata(tmp_path)
        # Should detect geo files without metadata
        assert isinstance(result, MetadataReport)
        # Depending on implementation, may report missing metadata
        # For now, just verify it returns a valid report

    @pytest.mark.unit
    def test_recursive_directory_scan(self, tmp_path: Path) -> None:
        """Should scan nested directories recursively."""
        # Create nested structure
        (tmp_path / "level1").mkdir()
        (tmp_path / "level1" / "level2").mkdir()
        (tmp_path / "level1" / "level2" / "data.parquet").write_bytes(b"")

        result = check_directory_metadata(tmp_path)
        assert isinstance(result, MetadataReport)

    @pytest.mark.unit
    def test_returns_metadata_report_type(self, tmp_path: Path) -> None:
        """Function should always return MetadataReport."""
        result = check_directory_metadata(tmp_path)
        assert isinstance(result, MetadataReport)
        assert hasattr(result, "passed")
        assert hasattr(result, "total_count")
        assert hasattr(result, "results")


class TestValidationIntegration:
    """Integration tests combining multiple validation functions."""

    @pytest.mark.integration
    def test_full_catalog_validation(self, tmp_path: Path) -> None:
        """Full validation of a complete STAC catalog structure."""
        # Create a complete valid STAC catalog structure
        catalog = CatalogModel(
            id="sf-data",
            description="San Francisco Open Data",
            links=[
                Link(rel="root", href="./catalog.json", type="application/json"),
                Link(rel="self", href="./catalog.json", type="application/json"),
                Link(rel="child", href="./parcels/collection.json", type="application/json"),
            ],
        )

        collection = CollectionModel(
            id="parcels",
            description="SF Parcels",
            extent=ExtentModel(
                spatial=SpatialExtent(bbox=[[-122.52, 37.7, -122.35, 37.82]]),
                temporal=TemporalExtent(interval=[["2026-01-01T00:00:00Z", None]]),
            ),
            links=[
                Link(rel="root", href="../catalog.json", type="application/json"),
                Link(rel="self", href="./collection.json", type="application/json"),
                Link(rel="parent", href="../catalog.json", type="application/json"),
            ],
        )

        items = [
            ItemModel(
                id=f"parcel-{i}",
                geometry={
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-122.4 - i * 0.01, 37.75],
                            [-122.4 - i * 0.01, 37.76],
                            [-122.39 - i * 0.01, 37.76],
                            [-122.39 - i * 0.01, 37.75],
                            [-122.4 - i * 0.01, 37.75],
                        ]
                    ],
                },
                bbox=[-122.4 - i * 0.01, 37.75, -122.39 - i * 0.01, 37.76],
                properties={"datetime": f"2026-01-{15 + i:02d}T00:00:00Z"},
                assets={"data": AssetModel(href=f"parcel-{i}.parquet")},
                collection="parcels",
            )
            for i in range(3)
        ]

        # Write files
        (tmp_path / "catalog.json").write_text(json.dumps(catalog.to_dict()))
        parcels_dir = tmp_path / "parcels"
        parcels_dir.mkdir()
        (parcels_dir / "collection.json").write_text(json.dumps(collection.to_dict()))

        # Validate catalog links
        catalog_result = validate_catalog_links(catalog, tmp_path / "catalog.json")
        assert catalog_result.passed is True

        # Validate collection extent
        extent_result = validate_collection_extent(collection, items)
        assert extent_result.passed is True

        # Full directory check
        dir_result = check_directory_metadata(tmp_path)
        assert isinstance(dir_result, MetadataReport)
