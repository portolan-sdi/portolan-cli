"""Unit tests for unsupported format detection.

These tests verify that unsupported formats (NetCDF, HDF5, non-COPC LAS/LAZ)
are correctly classified as UNSUPPORTED per spec 002-cloud-native-warnings User Story 3.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.formats import (
    CloudNativeStatus,
    get_cloud_native_status,
)


class TestUnsupportedFormatDetection:
    """Tests for unsupported format detection."""

    @pytest.mark.unit
    def test_netcdf_returns_unsupported(self, tmp_path: Path) -> None:
        """NetCDF (.nc) returns UNSUPPORTED status with error message."""
        nc_file = tmp_path / "test.nc"
        nc_file.write_bytes(b"\x00\x00\x00\x00")  # Dummy content
        result = get_cloud_native_status(nc_file)
        assert result.status == CloudNativeStatus.UNSUPPORTED
        assert result.display_name == "NetCDF"
        assert result.target_format is None
        assert result.error_message == "NetCDF is not yet supported. Support coming soon."

    @pytest.mark.unit
    def test_netcdf_alternate_extension(self, tmp_path: Path) -> None:
        """NetCDF (.netcdf) returns UNSUPPORTED status."""
        nc_file = tmp_path / "test.netcdf"
        nc_file.write_bytes(b"\x00\x00\x00\x00")
        result = get_cloud_native_status(nc_file)
        assert result.status == CloudNativeStatus.UNSUPPORTED
        assert result.display_name == "NetCDF"
        assert "not yet supported" in result.error_message  # type: ignore

    @pytest.mark.unit
    def test_hdf5_returns_unsupported(self, tmp_path: Path) -> None:
        """HDF5 (.h5) returns UNSUPPORTED status with error message."""
        h5_file = tmp_path / "test.h5"
        h5_file.write_bytes(b"\x00\x00\x00\x00")
        result = get_cloud_native_status(h5_file)
        assert result.status == CloudNativeStatus.UNSUPPORTED
        assert result.display_name == "HDF5"
        assert result.target_format is None
        assert result.error_message == "HDF5 is not yet supported. Support coming soon."

    @pytest.mark.unit
    def test_hdf5_alternate_extension(self, tmp_path: Path) -> None:
        """HDF5 (.hdf5) returns UNSUPPORTED status."""
        hdf5_file = tmp_path / "test.hdf5"
        hdf5_file.write_bytes(b"\x00\x00\x00\x00")
        result = get_cloud_native_status(hdf5_file)
        assert result.status == CloudNativeStatus.UNSUPPORTED
        assert result.display_name == "HDF5"
        assert "not yet supported" in result.error_message  # type: ignore

    @pytest.mark.unit
    def test_las_returns_unsupported_with_copc_guidance(self, tmp_path: Path) -> None:
        """LAS (non-COPC) returns UNSUPPORTED with COPC guidance."""
        las_file = tmp_path / "test.las"
        las_file.write_bytes(b"\x00\x00\x00\x00")
        result = get_cloud_native_status(las_file)
        assert result.status == CloudNativeStatus.UNSUPPORTED
        assert result.display_name == "LAS"
        assert result.target_format is None
        assert "COPC" in result.error_message  # type: ignore
        assert "pdal" in result.error_message.lower()  # type: ignore

    @pytest.mark.unit
    def test_laz_returns_unsupported_with_copc_guidance(self, tmp_path: Path) -> None:
        """LAZ (non-COPC) returns UNSUPPORTED with COPC guidance."""
        laz_file = tmp_path / "test.laz"
        laz_file.write_bytes(b"\x00\x00\x00\x00")
        result = get_cloud_native_status(laz_file)
        assert result.status == CloudNativeStatus.UNSUPPORTED
        assert result.display_name == "LAZ"
        assert result.target_format is None
        assert "COPC" in result.error_message  # type: ignore


class TestCOPCvsLAZ:
    """Tests to ensure COPC (.copc.laz) is cloud-native but LAZ is not."""

    @pytest.mark.unit
    def test_copc_laz_is_cloud_native(self, tmp_path: Path) -> None:
        """COPC files (.copc.laz) are cloud-native."""
        copc_file = tmp_path / "test.copc.laz"
        copc_file.write_bytes(b"\x00\x00\x00\x00")
        result = get_cloud_native_status(copc_file)
        assert result.status == CloudNativeStatus.CLOUD_NATIVE
        assert result.display_name == "COPC"

    @pytest.mark.unit
    def test_regular_laz_is_unsupported(self, tmp_path: Path) -> None:
        """Regular LAZ files (not .copc.laz) are unsupported."""
        laz_file = tmp_path / "test.laz"
        laz_file.write_bytes(b"\x00\x00\x00\x00")
        result = get_cloud_native_status(laz_file)
        assert result.status == CloudNativeStatus.UNSUPPORTED
        assert result.display_name == "LAZ"


class TestUnknownFormats:
    """Tests for unknown/unrecognized formats."""

    @pytest.mark.unit
    def test_unknown_extension_returns_unsupported(self, tmp_path: Path) -> None:
        """Unknown extension returns UNSUPPORTED."""
        unknown_file = tmp_path / "test.xyz"
        unknown_file.write_text("random content")
        result = get_cloud_native_status(unknown_file)
        assert result.status == CloudNativeStatus.UNSUPPORTED
        assert result.error_message is not None
        assert "not supported" in result.error_message

    @pytest.mark.unit
    def test_plain_json_not_geojson_returns_unsupported(self, tmp_path: Path) -> None:
        """Plain JSON (not GeoJSON) returns UNSUPPORTED."""
        json_file = tmp_path / "test.json"
        json_file.write_text('{"name": "not geojson", "data": [1,2,3]}')
        result = get_cloud_native_status(json_file)
        assert result.status == CloudNativeStatus.UNSUPPORTED
        # Plain JSON is unsupported because it's not recognizable geospatial data
