"""Tests to validate that test fixtures are correctly formed and usable.

These tests verify that:
1. Fixture files exist and are readable
2. Valid fixtures can be parsed by the relevant libraries
3. Invalid fixtures are indeed invalid (fail as expected)
"""

# mypy: disable-error-code="import-untyped"

from __future__ import annotations

import json
from pathlib import Path

import pytest


class TestVectorFixturesExist:
    """Verify that all vector fixtures exist and are readable."""

    @pytest.mark.unit
    def test_valid_points_geojson_exists(self, valid_points_geojson: Path) -> None:
        """points.geojson should exist and be readable."""
        assert valid_points_geojson.exists()
        assert valid_points_geojson.stat().st_size > 0

    @pytest.mark.unit
    def test_valid_polygons_geojson_exists(self, valid_polygons_geojson: Path) -> None:
        """polygons.geojson should exist and be readable."""
        assert valid_polygons_geojson.exists()
        assert valid_polygons_geojson.stat().st_size > 0

    @pytest.mark.unit
    def test_valid_lines_geojson_exists(self, valid_lines_geojson: Path) -> None:
        """lines.geojson should exist and be readable."""
        assert valid_lines_geojson.exists()
        assert valid_lines_geojson.stat().st_size > 0

    @pytest.mark.unit
    def test_valid_multigeom_geojson_exists(self, valid_multigeom_geojson: Path) -> None:
        """multigeom.geojson should exist and be readable."""
        assert valid_multigeom_geojson.exists()
        assert valid_multigeom_geojson.stat().st_size > 0

    @pytest.mark.unit
    def test_valid_parquet_exists(self, valid_points_parquet: Path) -> None:
        """points.parquet should exist and be readable."""
        assert valid_points_parquet.exists()
        assert valid_points_parquet.stat().st_size > 0


class TestVectorFixturesValid:
    """Verify that valid vector fixtures parse correctly."""

    @pytest.mark.unit
    def test_points_geojson_is_valid_geojson(self, valid_points_geojson: Path) -> None:
        """points.geojson should be valid GeoJSON with expected structure."""
        data = json.loads(valid_points_geojson.read_text(encoding="utf-8"))

        assert data["type"] == "FeatureCollection"
        assert len(data["features"]) == 10
        assert all(f["geometry"]["type"] == "Point" for f in data["features"])

    @pytest.mark.unit
    def test_polygons_geojson_is_valid_geojson(self, valid_polygons_geojson: Path) -> None:
        """polygons.geojson should be valid GeoJSON with expected structure."""
        data = json.loads(valid_polygons_geojson.read_text(encoding="utf-8"))

        assert data["type"] == "FeatureCollection"
        assert len(data["features"]) == 5
        assert all(f["geometry"]["type"] == "Polygon" for f in data["features"])

    @pytest.mark.unit
    def test_lines_geojson_is_valid_geojson(self, valid_lines_geojson: Path) -> None:
        """lines.geojson should be valid GeoJSON with expected structure."""
        data = json.loads(valid_lines_geojson.read_text(encoding="utf-8"))

        assert data["type"] == "FeatureCollection"
        assert len(data["features"]) == 5
        assert all(f["geometry"]["type"] == "LineString" for f in data["features"])

    @pytest.mark.unit
    def test_multigeom_has_mixed_types(self, valid_multigeom_geojson: Path) -> None:
        """multigeom.geojson should contain multiple geometry types."""
        data = json.loads(valid_multigeom_geojson.read_text(encoding="utf-8"))

        geom_types = {f["geometry"]["type"] for f in data["features"]}
        # Should have at least Point, LineString, Polygon
        assert len(geom_types) >= 3

    @pytest.mark.unit
    def test_large_properties_has_many_columns(self, valid_large_properties_geojson: Path) -> None:
        """large_properties.geojson should have 20+ property columns."""
        data = json.loads(valid_large_properties_geojson.read_text(encoding="utf-8"))

        first_feature = data["features"][0]
        assert len(first_feature["properties"]) >= 20

    @pytest.mark.unit
    def test_parquet_readable_with_geoparquet_io(self, valid_points_parquet: Path) -> None:
        """points.parquet should be readable with geoparquet-io."""
        import geoparquet_io as gpio

        # geoparquet-io should be able to read the file
        table = gpio.read(str(valid_points_parquet))
        # Access the underlying PyArrow table for row count
        assert table._table.num_rows == 10
        # Should have geometry column
        assert "geometry" in table._table.column_names


class TestVectorFixturesInvalid:
    """Verify that invalid vector fixtures fail as expected."""

    @pytest.mark.unit
    def test_malformed_geojson_fails_to_parse(self, invalid_malformed_geojson: Path) -> None:
        """malformed.geojson should fail JSON parsing."""
        with pytest.raises(json.JSONDecodeError):
            json.loads(invalid_malformed_geojson.read_text(encoding="utf-8"))

    @pytest.mark.unit
    def test_empty_geojson_has_no_features(self, invalid_empty_geojson: Path) -> None:
        """empty.geojson should parse but have zero features."""
        data = json.loads(invalid_empty_geojson.read_text(encoding="utf-8"))
        assert data["type"] == "FeatureCollection"
        assert len(data["features"]) == 0

    @pytest.mark.unit
    def test_no_geometry_json_missing_geometry(self, invalid_no_geometry_json: Path) -> None:
        """no_geometry.json should have features without geometry field."""
        data = json.loads(invalid_no_geometry_json.read_text(encoding="utf-8"))
        first_feature = data["features"][0]
        assert "geometry" not in first_feature

    @pytest.mark.unit
    def test_null_geometries_has_null_values(self, invalid_null_geometries_geojson: Path) -> None:
        """null_geometries.geojson should have at least one null geometry."""
        data = json.loads(invalid_null_geometries_geojson.read_text(encoding="utf-8"))
        null_geoms = [f for f in data["features"] if f["geometry"] is None]
        assert len(null_geoms) >= 1


class TestRasterFixturesExist:
    """Verify that all raster fixtures exist and are readable."""

    @pytest.mark.unit
    def test_valid_rgb_cog_exists(self, valid_rgb_cog: Path) -> None:
        """rgb.tif should exist and be readable."""
        assert valid_rgb_cog.exists()
        assert valid_rgb_cog.stat().st_size > 0

    @pytest.mark.unit
    def test_valid_singleband_cog_exists(self, valid_singleband_cog: Path) -> None:
        """singleband.tif should exist and be readable."""
        assert valid_singleband_cog.exists()
        assert valid_singleband_cog.stat().st_size > 0

    @pytest.mark.unit
    def test_valid_float32_cog_exists(self, valid_float32_cog: Path) -> None:
        """float32.tif should exist and be readable."""
        assert valid_float32_cog.exists()
        assert valid_float32_cog.stat().st_size > 0

    @pytest.mark.unit
    def test_valid_nodata_cog_exists(self, valid_nodata_cog: Path) -> None:
        """nodata.tif should exist and be readable."""
        assert valid_nodata_cog.exists()
        assert valid_nodata_cog.stat().st_size > 0


class TestRasterFixturesValid:
    """Verify that valid raster fixtures are proper COGs."""

    @pytest.mark.unit
    def test_rgb_cog_is_valid_cog(self, valid_rgb_cog: Path) -> None:
        """rgb.tif should be a valid Cloud-Optimized GeoTIFF."""
        from rio_cogeo.cogeo import cog_validate

        is_valid, errors, warnings = cog_validate(str(valid_rgb_cog))
        assert is_valid, f"COG validation failed: {errors}"

    @pytest.mark.unit
    def test_rgb_cog_has_correct_structure(self, valid_rgb_cog: Path) -> None:
        """rgb.tif should have 3 bands and correct dimensions."""
        import rasterio

        with rasterio.open(valid_rgb_cog) as src:
            assert src.count == 3  # RGB
            assert src.width == 64
            assert src.height == 64
            assert src.crs is not None
            assert src.crs.to_epsg() == 4326

    @pytest.mark.unit
    def test_singleband_cog_is_valid_cog(self, valid_singleband_cog: Path) -> None:
        """singleband.tif should be a valid Cloud-Optimized GeoTIFF."""
        from rio_cogeo.cogeo import cog_validate

        is_valid, errors, warnings = cog_validate(str(valid_singleband_cog))
        assert is_valid, f"COG validation failed: {errors}"

    @pytest.mark.unit
    def test_singleband_cog_has_one_band(self, valid_singleband_cog: Path) -> None:
        """singleband.tif should have exactly 1 band."""
        import rasterio

        with rasterio.open(valid_singleband_cog) as src:
            assert src.count == 1

    @pytest.mark.unit
    def test_float32_cog_has_float_dtype(self, valid_float32_cog: Path) -> None:
        """float32.tif should have float32 data type."""
        import rasterio

        with rasterio.open(valid_float32_cog) as src:
            assert src.dtypes[0] == "float32"

    @pytest.mark.unit
    def test_nodata_cog_has_nodata_value(self, valid_nodata_cog: Path) -> None:
        """nodata.tif should have a nodata value set."""
        import rasterio

        with rasterio.open(valid_nodata_cog) as src:
            assert src.nodata is not None
            assert src.nodata == 255


class TestRasterFixturesInvalid:
    """Verify that invalid raster fixtures fail as expected."""

    @pytest.mark.unit
    def test_not_georeferenced_has_no_crs(self, invalid_not_georeferenced_tif: Path) -> None:
        """not_georeferenced.tif should have no CRS."""
        import rasterio

        with rasterio.open(invalid_not_georeferenced_tif) as src:
            assert src.crs is None

    @pytest.mark.unit
    def test_truncated_tif_fails_to_read(self, invalid_truncated_tif: Path) -> None:
        """truncated.tif should fail when trying to read data."""
        import rasterio
        from rasterio.errors import RasterioIOError

        # Opening might work, but reading should fail
        with pytest.raises(RasterioIOError):
            with rasterio.open(invalid_truncated_tif) as src:
                src.read()  # This should fail on truncated file


class TestEdgeCaseFixtures:
    """Verify that edge case fixtures have expected characteristics."""

    @pytest.mark.unit
    def test_unicode_geojson_has_non_ascii(self, edge_unicode_geojson: Path) -> None:
        """unicode_properties.geojson should contain non-ASCII characters."""
        data = json.loads(edge_unicode_geojson.read_text(encoding="utf-8"))
        props = data["features"][0]["properties"]

        # Should have Chinese, Japanese, Arabic, etc.
        assert "name_zh" in props
        all_values = " ".join(str(v) for v in props.values())
        # Check for non-ASCII
        assert not all_values.isascii()

    @pytest.mark.unit
    def test_special_filename_exists(self, edge_special_filename_geojson: Path) -> None:
        """File with spaces in name should exist and be readable."""
        assert edge_special_filename_geojson.exists()
        assert " " in edge_special_filename_geojson.name
        # Should be valid JSON
        data = json.loads(edge_special_filename_geojson.read_text(encoding="utf-8"))
        assert data["type"] == "FeatureCollection"

    @pytest.mark.unit
    def test_antimeridian_crosses_dateline(self, edge_antimeridian_geojson: Path) -> None:
        """antimeridian.geojson should be a MultiPolygon split at +-180 per RFC 7946."""
        data = json.loads(edge_antimeridian_geojson.read_text(encoding="utf-8"))
        geom = data["features"][0]["geometry"]

        # Per RFC 7946 §3.1.9, antimeridian crossings should use MultiPolygon
        assert geom["type"] == "MultiPolygon", "Should be MultiPolygon per RFC 7946"
        assert len(geom["coordinates"]) == 2, "Should have 2 parts (east and west)"

        # Collect all longitudes from both polygons
        all_lons: list[float] = []
        for polygon in geom["coordinates"]:
            for ring in polygon:
                all_lons.extend(c[0] for c in ring)

        # Should have eastern (positive, near 180) and western (negative, near -180) parts
        has_east = any(lon >= 170 for lon in all_lons)
        has_west = any(lon <= -170 for lon in all_lons)
        assert has_east and has_west, "Should have parts on both sides of antimeridian"
