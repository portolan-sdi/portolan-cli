"""Integration tests for check command with conversion config.

Tests that check_directory respects conversion config settings:
- Force-convert cloud-native formats
- Preserve convertible formats
- Path-based overrides

See GitHub Issue #75 and #103.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.check import check_directory
from portolan_cli.config import save_config
from portolan_cli.formats import CloudNativeStatus


class TestCheckWithConversionConfig:
    """Integration tests for check_directory with conversion config."""

    @pytest.fixture
    def catalog_with_config(self, tmp_path: Path) -> Path:
        """Create a catalog directory with conversion config."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        return tmp_path

    @pytest.mark.integration
    def test_check_without_config_uses_defaults(self, tmp_path: Path) -> None:
        """Without config, check uses default cloud-native status."""
        # Create FlatGeobuf file
        fgb_file = tmp_path / "data.fgb"
        fgb_file.write_bytes(b"\x00\x00\x00\x00")

        report = check_directory(tmp_path)

        # FlatGeobuf should be CLOUD_NATIVE by default
        assert len(report.files) == 1
        assert report.files[0].status == CloudNativeStatus.CLOUD_NATIVE
        assert report.files[0].display_name == "FlatGeobuf"

    @pytest.mark.integration
    def test_check_force_convert_flatgeobuf(self, catalog_with_config: Path) -> None:
        """FlatGeobuf in convert list shows as CONVERTIBLE."""
        # Create FlatGeobuf file
        fgb_file = catalog_with_config / "data.fgb"
        fgb_file.write_bytes(b"\x00\x00\x00\x00")

        # Configure to force-convert FlatGeobuf
        save_config(
            catalog_with_config,
            {"conversion": {"extensions": {"convert": ["fgb"]}}},
        )

        report = check_directory(catalog_with_config, catalog_path=catalog_with_config)

        assert len(report.files) == 1
        assert report.files[0].status == CloudNativeStatus.CONVERTIBLE
        assert report.files[0].display_name == "FlatGeobuf"
        assert report.files[0].target_format == "GeoParquet"

    @pytest.mark.integration
    def test_check_preserve_shapefile(self, catalog_with_config: Path) -> None:
        """Shapefile in preserve list shows as CLOUD_NATIVE."""
        # Create minimal shapefile
        shp_file = catalog_with_config / "data.shp"
        shx_file = catalog_with_config / "data.shx"
        dbf_file = catalog_with_config / "data.dbf"
        shp_file.write_bytes(b"\x00\x00\x00\x00")
        shx_file.write_bytes(b"\x00\x00\x00\x00")
        dbf_file.write_bytes(b"\x00\x00\x00\x00")

        # Configure to preserve Shapefiles
        save_config(
            catalog_with_config,
            {"conversion": {"extensions": {"preserve": ["shp"]}}},
        )

        report = check_directory(catalog_with_config, catalog_path=catalog_with_config)

        # Find the .shp file in results (ignoring sidecar files)
        shp_files = [f for f in report.files if f.path.suffix == ".shp"]
        assert len(shp_files) == 1
        assert shp_files[0].status == CloudNativeStatus.CLOUD_NATIVE
        assert shp_files[0].display_name == "SHP"
        assert shp_files[0].target_format is None

    @pytest.mark.integration
    def test_check_path_preserve_overrides_convert(self, catalog_with_config: Path) -> None:
        """Path preserve pattern overrides extension convert rule."""
        # Create archive directory with FlatGeobuf
        archive_dir = catalog_with_config / "archive"
        archive_dir.mkdir()
        fgb_file = archive_dir / "data.fgb"
        fgb_file.write_bytes(b"\x00\x00\x00\x00")

        # Also create FlatGeobuf outside archive
        other_fgb = catalog_with_config / "other.fgb"
        other_fgb.write_bytes(b"\x00\x00\x00\x00")

        # Configure: convert FlatGeobuf, but preserve archive/**
        save_config(
            catalog_with_config,
            {
                "conversion": {
                    "extensions": {"convert": ["fgb"]},
                    "paths": {"preserve": ["archive/**"]},
                }
            },
        )

        report = check_directory(catalog_with_config, catalog_path=catalog_with_config)

        # Find both FlatGeobuf files
        archive_fgb = next(f for f in report.files if "archive" in str(f.path))
        other_fgb_result = next(f for f in report.files if "other" in str(f.path))

        # Archive FlatGeobuf should be CLOUD_NATIVE (preserved)
        assert archive_fgb.status == CloudNativeStatus.CLOUD_NATIVE

        # Other FlatGeobuf should be CONVERTIBLE (force-convert)
        assert other_fgb_result.status == CloudNativeStatus.CONVERTIBLE

    @pytest.mark.integration
    def test_check_fix_respects_preserve(self, catalog_with_config: Path) -> None:
        """--fix should not convert preserved files."""
        # Create shapefile
        shp_file = catalog_with_config / "data.shp"
        shx_file = catalog_with_config / "data.shx"
        dbf_file = catalog_with_config / "data.dbf"
        shp_file.write_bytes(b"\x00\x00\x00\x00")
        shx_file.write_bytes(b"\x00\x00\x00\x00")
        dbf_file.write_bytes(b"\x00\x00\x00\x00")

        # Configure to preserve Shapefiles
        save_config(
            catalog_with_config,
            {"conversion": {"extensions": {"preserve": ["shp"]}}},
        )

        report = check_directory(
            catalog_with_config,
            catalog_path=catalog_with_config,
            fix=True,
            dry_run=True,
        )

        # No files should be marked for conversion
        if report.conversion_report:
            assert len(report.conversion_report.results) == 0

    @pytest.mark.integration
    def test_check_fix_converts_force_convert(self, catalog_with_config: Path) -> None:
        """--fix should convert force-convert files."""
        # Create FlatGeobuf file
        fgb_file = catalog_with_config / "data.fgb"
        fgb_file.write_bytes(b"\x00\x00\x00\x00")

        # Configure to force-convert FlatGeobuf
        save_config(
            catalog_with_config,
            {"conversion": {"extensions": {"convert": ["fgb"]}}},
        )

        report = check_directory(
            catalog_with_config,
            catalog_path=catalog_with_config,
            fix=True,
            dry_run=True,
        )

        # One file should be marked for conversion
        assert report.conversion_report is not None
        assert len(report.conversion_report.results) == 1
        assert report.conversion_report.results[0].format_from == "FlatGeobuf"
        assert report.conversion_report.results[0].format_to == "GeoParquet"
