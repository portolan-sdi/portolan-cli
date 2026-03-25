"""Unit tests for STAC extension foundation (Issue #272).

Tests the STAC extension support including:
- STAC version 1.1.0
- stac_extensions array population
- Table extension for GeoParquet
- Projection extension fields
- Bbox WGS84 transformation
- Per-band nodata for COGs
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest


class TestStacVersion:
    """Tests for STAC version update to 1.1.0."""

    @pytest.mark.unit
    def test_stac_version_is_1_1_0(self) -> None:
        """STAC_VERSION constant should be 1.1.0."""
        from portolan_cli.stac import STAC_VERSION

        assert STAC_VERSION == "1.1.0"


class TestStacExtensionsArray:
    """Tests for stac_extensions array population."""

    @pytest.mark.unit
    def test_build_stac_extensions_empty_when_no_extension_fields(self) -> None:
        """build_stac_extensions returns empty list when no extension fields present."""
        from portolan_cli.stac import build_stac_extensions

        properties: dict[str, Any] = {"title": "Test", "description": "No extensions"}
        result = build_stac_extensions(properties)

        assert result == []

    @pytest.mark.unit
    def test_build_stac_extensions_includes_table_extension(self) -> None:
        """build_stac_extensions includes table extension when table: fields present."""
        from portolan_cli.stac import build_stac_extensions

        properties = {
            "table:row_count": 1000,
            "table:columns": [{"name": "id", "type": "int64"}],
        }
        result = build_stac_extensions(properties)

        assert any("table" in ext for ext in result)

    @pytest.mark.unit
    def test_build_stac_extensions_includes_projection_extension(self) -> None:
        """build_stac_extensions includes projection extension when proj: fields present."""
        from portolan_cli.stac import build_stac_extensions

        properties = {
            "proj:code": "EPSG:4326",
            "proj:bbox": [-180, -90, 180, 90],
        }
        result = build_stac_extensions(properties)

        assert any("projection" in ext for ext in result)

    @pytest.mark.unit
    def test_build_stac_extensions_includes_raster_extension(self) -> None:
        """build_stac_extensions includes raster extension when raster: fields present."""
        from portolan_cli.stac import build_stac_extensions

        properties = {
            "raster:bands": [{"data_type": "uint8", "nodata": 0}],
        }
        result = build_stac_extensions(properties)

        assert any("raster" in ext for ext in result)

    @pytest.mark.unit
    def test_build_stac_extensions_multiple_extensions(self) -> None:
        """build_stac_extensions returns multiple extensions when multiple prefixes present."""
        from portolan_cli.stac import build_stac_extensions

        properties = {
            "proj:code": "EPSG:32618",
            "table:row_count": 500,
        }
        result = build_stac_extensions(properties)

        assert len(result) >= 2
        assert any("projection" in ext for ext in result)
        assert any("table" in ext for ext in result)


class TestTableExtension:
    """Tests for Table extension fields on GeoParquet collections."""

    @pytest.mark.unit
    def test_add_table_extension_sets_row_count(self) -> None:
        """add_table_extension sets table:row_count from feature_count."""
        from portolan_cli.stac import add_table_extension, create_collection

        collection = create_collection(
            collection_id="test-table",
            description="Test table extension",
        )
        metadata = _make_geoparquet_metadata(feature_count=1234)

        add_table_extension(collection, metadata)

        # Table extension fields are in extra_fields for collections
        assert collection.extra_fields.get("table:row_count") == 1234

    @pytest.mark.unit
    def test_add_table_extension_sets_primary_geometry(self) -> None:
        """add_table_extension sets table:primary_geometry from geometry_column."""
        from portolan_cli.stac import add_table_extension, create_collection

        collection = create_collection(
            collection_id="test-geom",
            description="Test geometry column",
        )
        metadata = _make_geoparquet_metadata(geometry_column="geom")

        add_table_extension(collection, metadata)

        assert collection.extra_fields.get("table:primary_geometry") == "geom"

    @pytest.mark.unit
    def test_add_table_extension_sets_columns(self) -> None:
        """add_table_extension sets table:columns from schema."""
        from portolan_cli.stac import add_table_extension, create_collection

        collection = create_collection(
            collection_id="test-columns",
            description="Test columns",
        )
        metadata = _make_geoparquet_metadata(
            schema={"id": "int64", "name": "string", "geom": "binary"}
        )

        add_table_extension(collection, metadata)

        columns = collection.extra_fields.get("table:columns", [])
        assert len(columns) == 3
        column_names = {col["name"] for col in columns}
        assert column_names == {"id", "name", "geom"}


class TestProjectionExtension:
    """Tests for Projection extension fields."""

    @pytest.mark.unit
    def test_add_projection_extension_sets_proj_code(self) -> None:
        """add_projection_extension sets proj:code from CRS."""
        from portolan_cli.stac import add_projection_extension, create_item

        item = create_item(
            item_id="test-proj",
            bbox=[-122.5, 37.5, -122.0, 38.0],
        )
        metadata = _make_metadata_with_crs("EPSG:32610")

        add_projection_extension(item, metadata)

        assert item.properties.get("proj:code") == "EPSG:32610"

    @pytest.mark.unit
    def test_add_projection_extension_sets_proj_bbox(self) -> None:
        """add_projection_extension sets proj:bbox with native CRS bbox."""
        from portolan_cli.stac import add_projection_extension, create_item

        item = create_item(
            item_id="test-proj-bbox",
            bbox=[-122.5, 37.5, -122.0, 38.0],  # WGS84 bbox
        )
        native_bbox = (500000, 4150000, 510000, 4160000)  # UTM coords
        metadata = _make_metadata_with_crs("EPSG:32610", bbox=native_bbox)

        add_projection_extension(item, metadata)

        assert item.properties.get("proj:bbox") == list(native_bbox)

    @pytest.mark.unit
    def test_add_projection_extension_skips_when_no_crs(self) -> None:
        """add_projection_extension does nothing when CRS is None."""
        from portolan_cli.stac import add_projection_extension, create_item

        item = create_item(
            item_id="test-no-crs",
            bbox=[0, 0, 1, 1],
        )
        metadata = _make_metadata_with_crs(None)

        add_projection_extension(item, metadata)

        assert "proj:code" not in item.properties


class TestBboxWgs84Transformation:
    """Tests for bbox WGS84 transformation."""

    @pytest.mark.unit
    def test_transform_bbox_to_wgs84_passes_through_4326(self) -> None:
        """transform_bbox_to_wgs84 returns unchanged bbox for WGS84 input."""
        from portolan_cli.crs import transform_bbox_to_wgs84

        bbox = (-122.5, 37.5, -122.0, 38.0)
        result = transform_bbox_to_wgs84(bbox, "EPSG:4326")

        assert result == bbox

    @pytest.mark.unit
    def test_transform_bbox_to_wgs84_transforms_utm(self) -> None:
        """transform_bbox_to_wgs84 transforms UTM bbox to WGS84."""
        from portolan_cli.crs import transform_bbox_to_wgs84

        # UTM Zone 10N bbox (San Francisco area)
        utm_bbox = (545000, 4175000, 555000, 4185000)
        result = transform_bbox_to_wgs84(utm_bbox, "EPSG:32610")

        # Result should be in WGS84 range
        min_x, min_y, max_x, max_y = result
        assert -180 <= min_x <= 180
        assert -90 <= min_y <= 90
        assert -180 <= max_x <= 180
        assert -90 <= max_y <= 90
        # Should be roughly San Francisco area
        assert -123 < min_x < -121
        assert 37 < min_y < 38

    @pytest.mark.unit
    def test_transform_bbox_to_wgs84_handles_none_crs(self) -> None:
        """transform_bbox_to_wgs84 returns unchanged bbox when CRS is None."""
        from portolan_cli.crs import transform_bbox_to_wgs84

        bbox = (100, 200, 300, 400)
        result = transform_bbox_to_wgs84(bbox, None)

        assert result == bbox

    @pytest.mark.unit
    def test_transform_bbox_to_wgs84_handles_wkt_crs(self) -> None:
        """transform_bbox_to_wgs84 handles WKT CRS string."""
        from portolan_cli.crs import transform_bbox_to_wgs84

        # WKT for WGS84
        wkt = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
        bbox = (-122.5, 37.5, -122.0, 38.0)
        result = transform_bbox_to_wgs84(bbox, wkt)

        # Should pass through since it's WGS84
        assert abs(result[0] - bbox[0]) < 0.001
        assert abs(result[1] - bbox[1]) < 0.001


class TestPerBandNodata:
    """Tests for per-band nodata in COG metadata."""

    @pytest.mark.unit
    def test_cog_metadata_has_per_band_nodata(self, tmp_path: Path) -> None:
        """COGMetadata.to_stac_properties returns per-band nodata values."""
        from portolan_cli.metadata.cog import COGMetadata

        # Create metadata with multi-band nodata
        metadata = COGMetadata(
            bbox=(0, 0, 1, 1),
            crs="EPSG:4326",
            width=100,
            height=100,
            band_count=3,
            dtype="uint8",
            nodata=None,  # Will be replaced by nodatavals
            resolution=(1.0, 1.0),
            nodatavals=(0, 255, 128),  # Different nodata per band
        )

        props = metadata.to_stac_properties()
        bands = props["raster:bands"]

        assert len(bands) == 3
        assert bands[0]["nodata"] == 0
        assert bands[1]["nodata"] == 255
        assert bands[2]["nodata"] == 128

    @pytest.mark.unit
    def test_cog_metadata_handles_uniform_nodata(self, tmp_path: Path) -> None:
        """COGMetadata handles uniform nodata across bands."""
        from portolan_cli.metadata.cog import COGMetadata

        metadata = COGMetadata(
            bbox=(0, 0, 1, 1),
            crs="EPSG:4326",
            width=100,
            height=100,
            band_count=3,
            dtype="float32",
            nodata=-9999.0,
            resolution=(1.0, 1.0),
            nodatavals=(-9999.0, -9999.0, -9999.0),
        )

        props = metadata.to_stac_properties()
        bands = props["raster:bands"]

        assert all(b["nodata"] == -9999.0 for b in bands)


# Helper functions for creating test metadata objects


def _make_geoparquet_metadata(
    *,
    feature_count: int = 100,
    geometry_column: str = "geometry",
    schema: dict[str, str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    crs: str | None = "EPSG:4326",
) -> Any:
    """Create a mock GeoParquetMetadata-like object for testing."""
    from dataclasses import dataclass

    @dataclass
    class MockGeoParquetMetadata:
        bbox: tuple[float, float, float, float] | None
        crs: str | None
        geometry_type: str | None
        geometry_column: str | None
        feature_count: int
        schema: dict[str, str]

    return MockGeoParquetMetadata(
        bbox=bbox or (-180, -90, 180, 90),
        crs=crs,
        geometry_type="Polygon",
        geometry_column=geometry_column,
        feature_count=feature_count,
        schema=schema or {"id": "int64", "geometry": "binary"},
    )


def _make_metadata_with_crs(
    crs: str | None,
    bbox: tuple[float, float, float, float] | None = None,
) -> Any:
    """Create a mock metadata object with CRS for testing."""
    from dataclasses import dataclass

    @dataclass
    class MockMetadata:
        crs: str | None
        bbox: tuple[float, float, float, float] | None

    return MockMetadata(crs=crs, bbox=bbox)
