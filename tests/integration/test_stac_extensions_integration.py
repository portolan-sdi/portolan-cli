"""Integration tests for STAC extension support.

Tests the full workflow integration of STAC extensions:
- Table extension added to collections for vector data
- Projection extension added to items (including proj:shape/transform for rasters)
- Bbox transformation to WGS84 with antimeridian handling
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.crs import transform_bbox_to_wgs84
from portolan_cli.metadata.cog import extract_cog_metadata
from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata
from portolan_cli.stac import (
    EXTENSION_URLS,
    add_projection_extension,
    add_table_extension,
    create_collection,
    create_item,
)


class TestGeoParquetTableExtension:
    """Integration tests for Table extension with real GeoParquet files."""

    @pytest.mark.integration
    def test_table_extension_from_open_buildings(self, open_buildings_path: Path) -> None:
        """Table extension extracts correct metadata from Open Buildings GeoParquet."""
        metadata = extract_geoparquet_metadata(open_buildings_path)
        collection = create_collection(
            collection_id="open-buildings",
            description="Open Buildings test",
        )

        add_table_extension(collection, metadata)

        # Verify table extension fields
        assert collection.extra_fields.get("table:row_count") == 1000
        assert collection.extra_fields.get("table:primary_geometry") == metadata.geometry_column
        assert "table:columns" in collection.extra_fields

        # Verify extension URL is added
        assert EXTENSION_URLS["table"] in (collection.stac_extensions or [])

    @pytest.mark.integration
    def test_table_extension_columns_match_schema(self, nwi_wetlands_path: Path) -> None:
        """Table extension columns match GeoParquet schema."""
        metadata = extract_geoparquet_metadata(nwi_wetlands_path)
        collection = create_collection(
            collection_id="nwi-wetlands",
            description="NWI Wetlands test",
        )

        add_table_extension(collection, metadata)

        columns = collection.extra_fields.get("table:columns", [])
        column_names = {col["name"] for col in columns}

        # Columns should match schema keys
        assert column_names == set(metadata.schema.keys())


class TestProjectionExtensionVector:
    """Integration tests for Projection extension with vector data."""

    @pytest.mark.integration
    def test_projection_extension_skips_when_crs_none(self, open_buildings_path: Path) -> None:
        """Projection extension does nothing when CRS is None."""
        metadata = extract_geoparquet_metadata(open_buildings_path)

        # Many GeoParquet files have CRS=None (implied WGS84)
        assert metadata.crs is None

        item = create_item(
            item_id="open-buildings-item",
            bbox=list(metadata.bbox),
        )

        add_projection_extension(item, metadata)

        # No projection extension should be added
        assert "proj:code" not in item.properties

    @pytest.mark.integration
    def test_projection_extension_with_utm_data(self, fixtures_dir: Path) -> None:
        """Projection extension extracts CRS from UTM GeoParquet."""
        utm_path = fixtures_dir / "vector" / "open-buildings-utm31n.parquet"
        if not utm_path.exists():
            pytest.skip("UTM fixture not available")

        metadata = extract_geoparquet_metadata(utm_path)
        item = create_item(item_id="utm-item", bbox=[0, 0, 1, 1])

        add_projection_extension(item, metadata)

        # Should have EPSG code for UTM Zone 31N
        assert item.properties.get("proj:code") == "EPSG:32631"
        assert "proj:bbox" in item.properties
        assert item.properties["proj:bbox"] == list(metadata.bbox)

        # Vector data should NOT have proj:shape (no width/height)
        assert "proj:shape" not in item.properties


class TestProjectionExtensionRaster:
    """Integration tests for Projection extension with raster data."""

    @pytest.mark.integration
    def test_projection_extension_from_cog(self, fixtures_dir: Path) -> None:
        """Projection extension extracts full metadata from COG."""
        cog_path = fixtures_dir / "raster" / "valid" / "singleband.tif"
        metadata = extract_cog_metadata(cog_path)

        item = create_item(
            item_id="singleband-item",
            bbox=list(metadata.bbox),
        )

        add_projection_extension(item, metadata)

        # Verify projection extension fields
        assert "proj:code" in item.properties
        assert "proj:bbox" in item.properties

        # Raster-specific fields
        assert "proj:shape" in item.properties
        assert item.properties["proj:shape"] == [metadata.height, metadata.width]

        assert "proj:transform" in item.properties
        assert len(item.properties["proj:transform"]) == 6

    @pytest.mark.integration
    def test_projection_extension_transform_is_gdal_format(self, fixtures_dir: Path) -> None:
        """proj:transform is in GDAL GeoTransform format."""
        cog_path = fixtures_dir / "raster" / "valid" / "singleband.tif"
        metadata = extract_cog_metadata(cog_path)

        item = create_item(item_id="transform-test", bbox=list(metadata.bbox))
        add_projection_extension(item, metadata)

        transform = item.properties["proj:transform"]

        # GDAL GeoTransform: [origin_x, pixel_width, rotation_x, origin_y, rotation_y, pixel_height]
        # For north-up images: rotation_x and rotation_y should be 0
        assert len(transform) == 6
        # pixel_width (index 1) should be positive
        assert transform[1] > 0
        # pixel_height (index 5) should be negative for north-up
        assert transform[5] < 0

    @pytest.mark.integration
    def test_cog_metadata_includes_transform(self, fixtures_dir: Path) -> None:
        """COGMetadata extraction includes transform attribute."""
        cog_path = fixtures_dir / "raster" / "valid" / "singleband.tif"
        metadata = extract_cog_metadata(cog_path)

        assert metadata.transform is not None
        assert len(metadata.transform) == 6

        # to_dict should include transform
        d = metadata.to_dict()
        assert "transform" in d
        assert d["transform"] == list(metadata.transform)


class TestBboxTransformation:
    """Integration tests for bbox CRS transformation."""

    @pytest.mark.integration
    def test_utm_bbox_transforms_to_wgs84(self, fixtures_dir: Path) -> None:
        """UTM bbox transforms correctly to WGS84."""
        utm_path = fixtures_dir / "vector" / "open-buildings-utm31n.parquet"
        if not utm_path.exists():
            pytest.skip("UTM fixture not available")

        metadata = extract_geoparquet_metadata(utm_path)

        # UTM bbox should have coordinates in meters (large values)
        native_bbox = metadata.bbox
        assert native_bbox is not None

        # Transform to WGS84
        crs_str = metadata.crs if isinstance(metadata.crs, str) else None
        wgs84_bbox = transform_bbox_to_wgs84(native_bbox, crs_str)

        # WGS84 bbox should be in degrees
        west, south, east, north = wgs84_bbox
        assert -180 <= west <= 180
        assert -90 <= south <= 90
        assert -180 <= east <= 180
        assert -90 <= north <= 90

    @pytest.mark.integration
    def test_wgs84_bbox_unchanged(self, open_buildings_path: Path) -> None:
        """WGS84 bbox passes through unchanged."""
        metadata = extract_geoparquet_metadata(open_buildings_path)

        # This file should be in WGS84
        crs_str = metadata.crs if isinstance(metadata.crs, str) else None
        if crs_str and "4326" in crs_str:
            wgs84_bbox = transform_bbox_to_wgs84(metadata.bbox, crs_str)
            assert wgs84_bbox == metadata.bbox


class TestAntimeridianHandling:
    """Integration tests for antimeridian handling."""

    @pytest.mark.integration
    def test_antimeridian_bbox_valid(self, fieldmaps_boundaries_path: Path) -> None:
        """Antimeridian-crossing bbox produces valid WGS84 coordinates."""
        metadata = extract_geoparquet_metadata(fieldmaps_boundaries_path)

        crs_str = metadata.crs if isinstance(metadata.crs, str) else None
        wgs84_bbox = transform_bbox_to_wgs84(metadata.bbox, crs_str)

        west, south, east, north = wgs84_bbox

        # All coordinates should be valid numbers
        assert all(isinstance(c, float) for c in wgs84_bbox)
        assert all(c == c for c in wgs84_bbox)  # Not NaN

        # Latitude should be valid and ordered
        assert -90 <= south <= 90
        assert -90 <= north <= 90
        assert south <= north

        # Longitude should be valid (may have west > east for antimeridian crossing)
        assert -180 <= west <= 180
        assert -180 <= east <= 180

    @pytest.mark.integration
    def test_antimeridian_geojson_fixture(self, fixtures_dir: Path) -> None:
        """Test antimeridian handling with dedicated fixture."""
        antimeridian_path = fixtures_dir / "edge" / "antimeridian.geojson"
        if not antimeridian_path.exists():
            pytest.skip("Antimeridian fixture not available")

        # Read the GeoJSON to get the bbox
        with open(antimeridian_path) as f:
            geojson = json.load(f)

        if "bbox" in geojson:
            bbox = tuple(geojson["bbox"])
            # Antimeridian-crossing bbox should be handled gracefully
            result = transform_bbox_to_wgs84(bbox, "EPSG:4326")

            # Result should be valid
            assert len(result) == 4
            assert all(isinstance(c, float) for c in result)


class TestEndToEndWorkflow:
    """End-to-end integration tests for the full STAC extension workflow."""

    @pytest.mark.integration
    def test_vector_workflow_adds_table_extension(self, open_buildings_path: Path) -> None:
        """Vector workflow adds table extension (projection only if CRS present)."""
        # Extract metadata (simulating dataset.py workflow)
        metadata = extract_geoparquet_metadata(open_buildings_path)

        # Transform bbox
        crs_str = metadata.crs if isinstance(metadata.crs, str) else None
        wgs84_bbox = transform_bbox_to_wgs84(metadata.bbox, crs_str)

        # Create STAC structures
        collection = create_collection(
            collection_id="test-collection",
            description="Test",
        )
        item = create_item(
            item_id="test-item",
            bbox=list(wgs84_bbox),
            properties=metadata.to_stac_properties(),
        )

        # Add extensions
        add_projection_extension(item, metadata)
        add_table_extension(collection, metadata)

        # Table extension should always be added for vector data
        assert "table:row_count" in collection.extra_fields
        assert "table:columns" in collection.extra_fields

        # Projection extension only added if CRS is present
        # (Open Buildings has CRS=None, so no projection extension)
        if metadata.crs is None:
            assert "proj:code" not in item.properties
        else:
            assert "proj:code" in item.properties

    @pytest.mark.integration
    def test_vector_workflow_with_crs_adds_all_extensions(self, fixtures_dir: Path) -> None:
        """Vector workflow with CRS adds both table and projection extensions."""
        utm_path = fixtures_dir / "vector" / "open-buildings-utm31n.parquet"
        if not utm_path.exists():
            pytest.skip("UTM fixture not available")

        metadata = extract_geoparquet_metadata(utm_path)

        # Transform bbox from UTM to WGS84
        crs_str = metadata.crs if isinstance(metadata.crs, str) else None
        wgs84_bbox = transform_bbox_to_wgs84(metadata.bbox, crs_str)

        # Create STAC structures
        collection = create_collection(
            collection_id="utm-collection",
            description="Test UTM",
        )
        item = create_item(
            item_id="utm-item",
            bbox=list(wgs84_bbox),
            properties=metadata.to_stac_properties(),
        )

        # Add extensions
        add_projection_extension(item, metadata)
        add_table_extension(collection, metadata)

        # Both extensions should be present
        assert "proj:code" in item.properties
        assert item.properties["proj:code"] == "EPSG:32631"
        assert "table:row_count" in collection.extra_fields
        assert "table:columns" in collection.extra_fields

    @pytest.mark.integration
    def test_raster_workflow_adds_projection_extension(self, fixtures_dir: Path) -> None:
        """Full raster workflow adds projection extension with shape/transform."""
        cog_path = fixtures_dir / "raster" / "valid" / "singleband.tif"
        metadata = extract_cog_metadata(cog_path)

        # Transform bbox
        wgs84_bbox = transform_bbox_to_wgs84(metadata.bbox, metadata.crs)

        # Create STAC item with raster properties
        item = create_item(
            item_id="raster-item",
            bbox=list(wgs84_bbox),
            properties=metadata.to_stac_properties(),
        )

        # Add projection extension
        add_projection_extension(item, metadata)

        # Verify raster-specific fields
        assert "proj:shape" in item.properties
        assert "proj:transform" in item.properties
        # STAC v1.1.0: unified bands array (not raster:bands)
        assert "bands" in item.properties

        # Verify extension URLs
        assert EXTENSION_URLS["projection"] in (item.stac_extensions or [])
