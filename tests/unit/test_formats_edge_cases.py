"""Unit tests for edge cases in format detection.

These tests cover edge cases mentioned in the spec:
- Ambiguous .tif extension (COG vs non-COG)
- .json file detected as GeoJSON vs plain JSON
- Unknown extension handling
- File not found handling
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.formats import (
    CloudNativeStatus,
    get_cloud_native_status,
)


class TestTifAmbiguity:
    """Tests for .tif extension which could be COG or non-COG."""

    @pytest.mark.unit
    def test_valid_cog_detected_as_cloud_native(self, valid_rgb_cog: Path) -> None:
        """Valid COG .tif returns CLOUD_NATIVE."""
        result = get_cloud_native_status(valid_rgb_cog)
        assert result.status == CloudNativeStatus.CLOUD_NATIVE
        assert result.display_name == "COG"

    @pytest.mark.unit
    def test_invalid_tif_detected_as_convertible(self, tmp_path: Path) -> None:
        """Invalid .tif (not a real TIFF) returns CONVERTIBLE."""
        invalid_tif = tmp_path / "not_a_real_tiff.tif"
        invalid_tif.write_text("this is not a valid tiff file")
        result = get_cloud_native_status(invalid_tif)
        # rio-cogeo will fail validation, so it's CONVERTIBLE
        assert result.status == CloudNativeStatus.CONVERTIBLE
        assert result.display_name == "TIFF"
        assert result.target_format == "COG"

    @pytest.mark.unit
    def test_tiff_alternate_extension(self, tmp_path: Path) -> None:
        """.tiff extension handled same as .tif."""
        tiff_file = tmp_path / "test.tiff"
        tiff_file.write_text("not a valid tiff")
        result = get_cloud_native_status(tiff_file)
        # Should be treated as non-COG TIFF
        assert result.status == CloudNativeStatus.CONVERTIBLE
        assert result.target_format == "COG"


class TestJsonAmbiguity:
    """Tests for .json files which could be GeoJSON or plain JSON."""

    @pytest.mark.unit
    def test_geojson_feature_collection_detected(self, tmp_path: Path) -> None:
        """JSON with FeatureCollection is detected as GeoJSON."""
        json_file = tmp_path / "test.json"
        json_file.write_text('{"type": "FeatureCollection", "features": []}')
        result = get_cloud_native_status(json_file)
        assert result.status == CloudNativeStatus.CONVERTIBLE
        assert result.display_name == "GeoJSON"
        assert result.target_format == "GeoParquet"

    @pytest.mark.unit
    def test_geojson_feature_detected(self, tmp_path: Path) -> None:
        """JSON with Feature type is detected as GeoJSON."""
        json_file = tmp_path / "test.json"
        json_file.write_text(
            '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [0, 0]}, "properties": {}}'
        )
        result = get_cloud_native_status(json_file)
        assert result.status == CloudNativeStatus.CONVERTIBLE
        assert result.display_name == "GeoJSON"

    @pytest.mark.unit
    def test_geojson_geometry_detected(self, tmp_path: Path) -> None:
        """JSON with geometry type is detected as GeoJSON."""
        json_file = tmp_path / "test.json"
        json_file.write_text('{"type": "Point", "coordinates": [0, 0]}')
        result = get_cloud_native_status(json_file)
        assert result.status == CloudNativeStatus.CONVERTIBLE
        assert result.display_name == "GeoJSON"

    @pytest.mark.unit
    def test_plain_json_not_detected_as_geojson(self, tmp_path: Path) -> None:
        """Plain JSON (not GeoJSON) returns UNSUPPORTED."""
        json_file = tmp_path / "test.json"
        json_file.write_text('{"name": "not geojson", "data": [1, 2, 3]}')
        result = get_cloud_native_status(json_file)
        assert result.status == CloudNativeStatus.UNSUPPORTED

    @pytest.mark.unit
    def test_geojson_extension_always_convertible(self, tmp_path: Path) -> None:
        """.geojson extension is always treated as GeoJSON (convertible)."""
        geojson_file = tmp_path / "test.geojson"
        # Even with empty/invalid content, .geojson is assumed GeoJSON
        geojson_file.write_text("{}")
        result = get_cloud_native_status(geojson_file)
        assert result.status == CloudNativeStatus.CONVERTIBLE
        assert result.display_name == "GeoJSON"


class TestFileNotFound:
    """Tests for file not found handling."""

    @pytest.mark.unit
    def test_nonexistent_file_raises_error(self, tmp_path: Path) -> None:
        """Non-existent file raises FileNotFoundError."""
        missing_file = tmp_path / "does_not_exist.parquet"
        with pytest.raises(FileNotFoundError):
            get_cloud_native_status(missing_file)


class TestDirectoryHandling:
    """Tests for directory handling."""

    @pytest.mark.unit
    def test_zarr_directory_is_cloud_native(self, tmp_path: Path) -> None:
        """Zarr directories (.zarr) are cloud-native."""
        zarr_dir = tmp_path / "data.zarr"
        zarr_dir.mkdir()
        (zarr_dir / ".zarray").write_text("{}")
        result = get_cloud_native_status(zarr_dir)
        assert result.status == CloudNativeStatus.CLOUD_NATIVE
        assert result.display_name == "Zarr"

    @pytest.mark.unit
    def test_regular_directory_raises_error(self, tmp_path: Path) -> None:
        """Regular directories (not .zarr) raise IsADirectoryError."""
        regular_dir = tmp_path / "some_directory"
        regular_dir.mkdir()
        with pytest.raises(IsADirectoryError):
            get_cloud_native_status(regular_dir)


class TestCaseInsensitivity:
    """Tests for case-insensitive extension handling."""

    @pytest.mark.unit
    def test_uppercase_extension(self, tmp_path: Path) -> None:
        """Uppercase extensions are handled correctly."""
        shp_file = tmp_path / "test.SHP"
        shp_file.write_bytes(b"\x00")
        result = get_cloud_native_status(shp_file)
        assert result.status == CloudNativeStatus.CONVERTIBLE
        assert result.display_name == "SHP"

    @pytest.mark.unit
    def test_mixed_case_extension(self, tmp_path: Path) -> None:
        """Mixed case extensions are handled correctly."""
        geojson_file = tmp_path / "test.GeoJSON"
        geojson_file.write_text("{}")
        result = get_cloud_native_status(geojson_file)
        assert result.status == CloudNativeStatus.CONVERTIBLE
        assert result.display_name == "GeoJSON"

    @pytest.mark.unit
    def test_copc_laz_case_insensitive(self, tmp_path: Path) -> None:
        """COPC detection is case-insensitive."""
        copc_file = tmp_path / "test.COPC.LAZ"
        copc_file.write_bytes(b"\x00")
        result = get_cloud_native_status(copc_file)
        assert result.status == CloudNativeStatus.CLOUD_NATIVE
        assert result.display_name == "COPC"
