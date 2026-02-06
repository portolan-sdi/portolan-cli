"""Unit tests for format detection module."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from portolan_cli.formats import FormatType, detect_format


class TestDetectFormat:
    """Tests for detect_format function."""

    # =========================================================================
    # Vector formats
    # =========================================================================

    @pytest.mark.unit
    def test_detect_geojson_by_extension(self, tmp_path: Path) -> None:
        """GeoJSON files are detected as vector."""
        geojson_file = tmp_path / "test.geojson"
        geojson_file.write_text('{"type": "FeatureCollection", "features": []}')
        assert detect_format(geojson_file) == FormatType.VECTOR

    @pytest.mark.unit
    def test_detect_json_with_geojson_content(self, tmp_path: Path) -> None:
        """JSON files with GeoJSON content are detected as vector."""
        json_file = tmp_path / "test.json"
        json_file.write_text('{"type": "FeatureCollection", "features": []}')
        assert detect_format(json_file) == FormatType.VECTOR

    @pytest.mark.unit
    def test_detect_geoparquet_by_extension(self, tmp_path: Path) -> None:
        """Parquet files are detected as vector."""
        # Create empty file - detection is by extension, not content
        parquet_file = tmp_path / "test.parquet"
        parquet_file.write_bytes(b"PAR1")  # Parquet magic bytes
        assert detect_format(parquet_file) == FormatType.VECTOR

    @pytest.mark.unit
    def test_detect_shapefile_by_extension(self, tmp_path: Path) -> None:
        """Shapefiles are detected as vector."""
        shp_file = tmp_path / "test.shp"
        shp_file.write_bytes(b"")  # Empty file, detection by extension
        assert detect_format(shp_file) == FormatType.VECTOR

    @pytest.mark.unit
    def test_detect_geopackage_by_extension(self, tmp_path: Path) -> None:
        """GeoPackage files are detected as vector."""
        gpkg_file = tmp_path / "test.gpkg"
        gpkg_file.write_bytes(b"")
        assert detect_format(gpkg_file) == FormatType.VECTOR

    # =========================================================================
    # Raster formats
    # =========================================================================

    @pytest.mark.unit
    def test_detect_tiff_by_extension(self, tmp_path: Path) -> None:
        """TIFF files are detected as raster."""
        tif_file = tmp_path / "test.tif"
        tif_file.write_bytes(b"II*\x00")  # Little-endian TIFF magic
        assert detect_format(tif_file) == FormatType.RASTER

    @pytest.mark.unit
    def test_detect_tiff_alternate_extension(self, tmp_path: Path) -> None:
        """TIFF files with .tiff extension are detected as raster."""
        tiff_file = tmp_path / "test.tiff"
        tiff_file.write_bytes(b"MM\x00*")  # Big-endian TIFF magic
        assert detect_format(tiff_file) == FormatType.RASTER

    # =========================================================================
    # Unknown formats
    # =========================================================================

    @pytest.mark.unit
    def test_detect_unknown_extension(self, tmp_path: Path) -> None:
        """Unknown extensions return UNKNOWN."""
        unknown_file = tmp_path / "test.xyz"
        unknown_file.write_text("random content")
        assert detect_format(unknown_file) == FormatType.UNKNOWN

    @pytest.mark.unit
    def test_detect_plain_json_not_geojson(self, tmp_path: Path) -> None:
        """Plain JSON (not GeoJSON) returns UNKNOWN."""
        json_file = tmp_path / "test.json"
        json_file.write_text('{"name": "not geojson"}')
        assert detect_format(json_file) == FormatType.UNKNOWN

    # =========================================================================
    # Edge cases
    # =========================================================================

    @pytest.mark.unit
    def test_detect_case_insensitive_extension(self, tmp_path: Path) -> None:
        """Extension detection is case-insensitive."""
        geojson_file = tmp_path / "test.GEOJSON"
        geojson_file.write_text('{"type": "FeatureCollection", "features": []}')
        assert detect_format(geojson_file) == FormatType.VECTOR

    @pytest.mark.unit
    def test_detect_nonexistent_file_raises(self, tmp_path: Path) -> None:
        """Non-existent files raise FileNotFoundError."""
        missing_file = tmp_path / "missing.geojson"
        with pytest.raises(FileNotFoundError):
            detect_format(missing_file)

    @pytest.mark.unit
    def test_detect_directory_raises(self, tmp_path: Path) -> None:
        """Directories raise IsADirectoryError."""
        with pytest.raises(IsADirectoryError):
            detect_format(tmp_path)

    @pytest.mark.unit
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="chmod doesn't restrict read access on Windows",
    )
    def test_detect_json_with_read_error(self, tmp_path: Path) -> None:
        """JSON files that can't be read return UNKNOWN."""
        json_file = tmp_path / "test.json"
        json_file.write_text('{"type": "FeatureCollection"}')
        # Remove read permissions to trigger OSError
        json_file.chmod(0o000)
        try:
            # Should return UNKNOWN rather than raising
            assert detect_format(json_file) == FormatType.UNKNOWN
        finally:
            # Restore permissions for cleanup
            json_file.chmod(0o644)
