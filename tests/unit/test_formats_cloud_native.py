"""Unit tests for cloud-native format detection.

These tests verify that cloud-native formats (GeoParquet, COG, FlatGeobuf, etc.)
are correctly classified as CLOUD_NATIVE per spec 002-cloud-native-warnings User Story 1.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.formats import (
    CloudNativeStatus,
    get_cloud_native_status,
)


class TestCloudNativeDetection:
    """Tests for get_cloud_native_status() with cloud-native formats."""

    @pytest.mark.unit
    def test_geoparquet_returns_cloud_native(self, valid_points_parquet: Path) -> None:
        """GeoParquet files return CLOUD_NATIVE status."""
        result = get_cloud_native_status(valid_points_parquet)
        assert result.status == CloudNativeStatus.CLOUD_NATIVE
        assert result.display_name == "GeoParquet"
        assert result.target_format is None
        assert result.error_message is None

    @pytest.mark.unit
    def test_cog_returns_cloud_native(self, valid_rgb_cog: Path) -> None:
        """Cloud-Optimized GeoTIFF files return CLOUD_NATIVE status."""
        result = get_cloud_native_status(valid_rgb_cog)
        assert result.status == CloudNativeStatus.CLOUD_NATIVE
        assert result.display_name == "COG"
        assert result.target_format is None
        assert result.error_message is None

    @pytest.mark.unit
    def test_flatgeobuf_returns_cloud_native(self, tmp_path: Path) -> None:
        """FlatGeobuf files return CLOUD_NATIVE status."""
        # Create minimal FlatGeobuf file (extension-based detection)
        fgb_file = tmp_path / "test.fgb"
        fgb_file.write_bytes(b"\x00\x00\x00\x00")  # Dummy content
        result = get_cloud_native_status(fgb_file)
        assert result.status == CloudNativeStatus.CLOUD_NATIVE
        assert result.display_name == "FlatGeobuf"
        assert result.target_format is None
        assert result.error_message is None

    @pytest.mark.unit
    def test_copc_returns_cloud_native(self, tmp_path: Path) -> None:
        """COPC (Cloud-Optimized Point Cloud) files return CLOUD_NATIVE status."""
        # COPC has .copc.laz extension
        copc_file = tmp_path / "test.copc.laz"
        copc_file.write_bytes(b"\x00\x00\x00\x00")  # Dummy content
        result = get_cloud_native_status(copc_file)
        assert result.status == CloudNativeStatus.CLOUD_NATIVE
        assert result.display_name == "COPC"
        assert result.target_format is None
        assert result.error_message is None

    @pytest.mark.unit
    def test_pmtiles_returns_cloud_native(self, tmp_path: Path) -> None:
        """PMTiles files return CLOUD_NATIVE status."""
        pmtiles_file = tmp_path / "test.pmtiles"
        pmtiles_file.write_bytes(b"\x00\x00\x00\x00")
        result = get_cloud_native_status(pmtiles_file)
        assert result.status == CloudNativeStatus.CLOUD_NATIVE
        assert result.display_name == "PMTiles"
        assert result.target_format is None
        assert result.error_message is None

    @pytest.mark.unit
    def test_zarr_returns_cloud_native(self, tmp_path: Path) -> None:
        """Zarr directories return CLOUD_NATIVE status."""
        # Zarr is a directory with .zarr extension
        zarr_dir = tmp_path / "test.zarr"
        zarr_dir.mkdir()
        # Create .zarray file to mark it as zarr
        (zarr_dir / ".zarray").write_text("{}")
        result = get_cloud_native_status(zarr_dir)
        assert result.status == CloudNativeStatus.CLOUD_NATIVE
        assert result.display_name == "Zarr"
        assert result.target_format is None
        assert result.error_message is None

    @pytest.mark.unit
    def test_raquet_returns_cloud_native(self, tmp_path: Path) -> None:
        """Raquet files return CLOUD_NATIVE status."""
        raquet_file = tmp_path / "test.raquet"
        raquet_file.write_bytes(b"\x00\x00\x00\x00")
        result = get_cloud_native_status(raquet_file)
        assert result.status == CloudNativeStatus.CLOUD_NATIVE
        assert result.display_name == "Raquet"
        assert result.target_format is None
        assert result.error_message is None


class TestIsGeoparquet:
    """Tests for is_geoparquet() helper function."""

    @pytest.mark.unit
    def test_is_geoparquet_with_geo_metadata(self, valid_points_parquet: Path) -> None:
        """Parquet with 'geo' metadata returns True."""
        from portolan_cli.formats import is_geoparquet

        assert is_geoparquet(valid_points_parquet) is True

    @pytest.mark.unit
    def test_is_geoparquet_without_geo_metadata(self, tmp_path: Path) -> None:
        """Parquet without 'geo' metadata returns False."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from portolan_cli.formats import is_geoparquet

        # Create a plain Parquet file without geo metadata
        table = pa.table({"col": [1, 2, 3]})
        parquet_file = tmp_path / "plain.parquet"
        pq.write_table(table, str(parquet_file))

        assert is_geoparquet(parquet_file) is False

    @pytest.mark.unit
    def test_is_geoparquet_with_invalid_file(self, tmp_path: Path) -> None:
        """Invalid Parquet file returns False."""
        from portolan_cli.formats import is_geoparquet

        invalid_file = tmp_path / "invalid.parquet"
        invalid_file.write_text("not a parquet file")

        assert is_geoparquet(invalid_file) is False


class TestIsCloudOptimizedGeotiff:
    """Tests for is_cloud_optimized_geotiff() helper function."""

    @pytest.mark.unit
    def test_is_cog_with_valid_cog(self, valid_rgb_cog: Path) -> None:
        """Valid COG returns True."""
        from portolan_cli.formats import is_cloud_optimized_geotiff

        assert is_cloud_optimized_geotiff(valid_rgb_cog) is True

    @pytest.mark.unit
    def test_is_cog_delegates_to_rio_cogeo(self, valid_rgb_cog: Path) -> None:
        """is_cloud_optimized_geotiff correctly delegates to rio-cogeo.

        Note: rio-cogeo considers small TIFFs (<512x512) as valid COGs even
        without explicit tiling, because they fit in a single tile. This is
        expected behavior per COG specification.
        """
        from rio_cogeo.cogeo import cog_validate

        from portolan_cli.formats import is_cloud_optimized_geotiff

        # Our function should return the same result as rio-cogeo
        rio_result, _errors, _warnings = cog_validate(str(valid_rgb_cog))
        our_result = is_cloud_optimized_geotiff(valid_rgb_cog)
        assert our_result == rio_result

    @pytest.mark.unit
    def test_is_cog_with_invalid_file(self, tmp_path: Path) -> None:
        """Invalid TIFF file returns False."""
        from portolan_cli.formats import is_cloud_optimized_geotiff

        invalid_file = tmp_path / "invalid.tif"
        invalid_file.write_text("not a tiff file")

        assert is_cloud_optimized_geotiff(invalid_file) is False
