"""Unit tests for convertible format detection.

These tests verify that convertible formats (Shapefile, GeoJSON, GeoPackage, etc.)
are correctly classified as CONVERTIBLE per spec 002-cloud-native-warnings User Story 2.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.formats import (
    CloudNativeStatus,
    get_cloud_native_status,
)


class TestConvertibleVectorDetection:
    """Tests for convertible vector format detection."""

    @pytest.mark.unit
    def test_shapefile_returns_convertible(self, tmp_path: Path) -> None:
        """Shapefile returns CONVERTIBLE status with SHP display name."""
        shp_file = tmp_path / "test.shp"
        shp_file.write_bytes(b"\x00\x00\x00\x00")  # Dummy content
        result = get_cloud_native_status(shp_file)
        assert result.status == CloudNativeStatus.CONVERTIBLE
        assert result.display_name == "SHP"
        assert result.target_format == "GeoParquet"
        assert result.error_message is None

    @pytest.mark.unit
    def test_geojson_returns_convertible(self, valid_points_geojson: Path) -> None:
        """GeoJSON returns CONVERTIBLE status with GeoJSON display name."""
        result = get_cloud_native_status(valid_points_geojson)
        assert result.status == CloudNativeStatus.CONVERTIBLE
        assert result.display_name == "GeoJSON"
        assert result.target_format == "GeoParquet"
        assert result.error_message is None

    @pytest.mark.unit
    def test_geopackage_returns_convertible(self, tmp_path: Path) -> None:
        """GeoPackage returns CONVERTIBLE status with GPKG display name."""
        gpkg_file = tmp_path / "test.gpkg"
        gpkg_file.write_bytes(b"\x00\x00\x00\x00")  # Dummy content
        result = get_cloud_native_status(gpkg_file)
        assert result.status == CloudNativeStatus.CONVERTIBLE
        assert result.display_name == "GPKG"
        assert result.target_format == "GeoParquet"
        assert result.error_message is None

    @pytest.mark.unit
    def test_csv_returns_convertible(self, tmp_path: Path) -> None:
        """CSV returns CONVERTIBLE status."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("lat,lon,name\n37.7,-122.4,SF")
        result = get_cloud_native_status(csv_file)
        assert result.status == CloudNativeStatus.CONVERTIBLE
        assert result.display_name == "CSV"
        assert result.target_format == "GeoParquet"
        assert result.error_message is None

    @pytest.mark.unit
    def test_json_geojson_returns_convertible(self, tmp_path: Path) -> None:
        """JSON file with GeoJSON content returns CONVERTIBLE."""
        json_file = tmp_path / "test.json"
        json_file.write_text('{"type": "FeatureCollection", "features": []}')
        result = get_cloud_native_status(json_file)
        assert result.status == CloudNativeStatus.CONVERTIBLE
        assert result.display_name == "GeoJSON"
        assert result.target_format == "GeoParquet"
        assert result.error_message is None


class TestConvertibleRasterDetection:
    """Tests for convertible raster format detection."""

    @pytest.mark.unit
    def test_jp2_returns_convertible(self, tmp_path: Path) -> None:
        """JPEG2000 returns CONVERTIBLE status."""
        jp2_file = tmp_path / "test.jp2"
        jp2_file.write_bytes(b"\x00\x00\x00\x00")  # Dummy content
        result = get_cloud_native_status(jp2_file)
        assert result.status == CloudNativeStatus.CONVERTIBLE
        assert result.display_name == "JP2"
        assert result.target_format == "COG"
        assert result.error_message is None

    @pytest.mark.unit
    def test_non_cog_tiff_returns_convertible(self, tmp_path: Path) -> None:
        """Non-COG TIFF returns CONVERTIBLE status.

        Note: For small files, rio-cogeo may consider them valid COGs.
        This test creates an invalid TIFF to force CONVERTIBLE status.
        """
        # Create a file that is definitely not a valid COG
        # (not even a valid TIFF - will fail rio-cogeo validation)
        tiff_file = tmp_path / "non_cog.tif"
        tiff_file.write_text("this is not a valid tiff")
        result = get_cloud_native_status(tiff_file)
        assert result.status == CloudNativeStatus.CONVERTIBLE
        assert result.display_name == "TIFF"
        assert result.target_format == "COG"
        assert result.error_message is None


class TestWarningMessageFormat:
    """Tests for warning message format per spec."""

    @pytest.mark.unit
    def test_warning_message_format_shapefile(self, tmp_path: Path) -> None:
        """Shapefile warning follows spec format."""
        shp_file = tmp_path / "test.shp"
        shp_file.write_bytes(b"\x00")
        result = get_cloud_native_status(shp_file)

        # The warning message format per spec:
        # "⚠ {FORMAT} is not cloud-native. Converting to {TARGET}."
        expected_msg = (
            f"{result.display_name} is not cloud-native. Converting to {result.target_format}."
        )
        # The full message with prefix would be: "⚠ SHP is not cloud-native. Converting to GeoParquet."
        assert result.display_name == "SHP"
        assert result.target_format == "GeoParquet"
        # Verify the parts are available to construct the message
        assert expected_msg == "SHP is not cloud-native. Converting to GeoParquet."

    @pytest.mark.unit
    def test_warning_message_format_geojson(self, valid_points_geojson: Path) -> None:
        """GeoJSON warning follows spec format."""
        result = get_cloud_native_status(valid_points_geojson)
        expected_msg = (
            f"{result.display_name} is not cloud-native. Converting to {result.target_format}."
        )
        assert expected_msg == "GeoJSON is not cloud-native. Converting to GeoParquet."
