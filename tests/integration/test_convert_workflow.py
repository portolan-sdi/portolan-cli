"""Integration tests for the full convert workflow with real fixtures.

These tests verify end-to-end conversion using real geospatial data files.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

# =============================================================================
# Task 7.7: Full Convert Workflow Integration Tests
# =============================================================================


@pytest.mark.integration
class TestConvertWorkflowIntegration:
    """Integration tests for full convert workflow with real fixtures."""

    def test_convert_geojson_to_geoparquet(
        self,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Convert real GeoJSON to GeoParquet and verify output."""
        from portolan_cli.convert import ConversionStatus, convert_file

        source = tmp_path / "points.geojson"
        shutil.copy(valid_points_geojson, source)

        result = convert_file(source)

        # Verify success
        assert result.status == ConversionStatus.SUCCESS
        assert result.output is not None
        assert result.output.exists()
        assert result.output.suffix == ".parquet"

        # Verify output is valid GeoParquet
        from portolan_cli.scan import _is_geoparquet

        assert _is_geoparquet(result.output)

    def test_convert_non_cog_to_cog(
        self,
        non_cog_tif: Path,
        tmp_path: Path,
    ) -> None:
        """Convert non-COG TIFF to COG and verify output."""
        from portolan_cli.convert import ConversionStatus, convert_file

        source = tmp_path / "input.tif"
        shutil.copy(non_cog_tif, source)

        result = convert_file(source, output_dir=tmp_path)

        # Verify success
        assert result.status == ConversionStatus.SUCCESS
        assert result.output is not None
        assert result.output.exists()
        assert result.output.suffix == ".tif"

        # Verify output is valid COG
        from rio_cogeo.cogeo import cog_validate

        is_cog, errors, warnings = cog_validate(str(result.output))
        assert is_cog, f"Output not a valid COG: {errors}"

    def test_skip_already_cloud_native_parquet(
        self,
        valid_points_parquet: Path,
        tmp_path: Path,
    ) -> None:
        """Already cloud-native GeoParquet is skipped."""
        from portolan_cli.convert import ConversionStatus, convert_file

        source = tmp_path / "data.parquet"
        shutil.copy(valid_points_parquet, source)

        result = convert_file(source)

        assert result.status == ConversionStatus.SKIPPED
        assert result.format_from == "GeoParquet"
        assert result.output is None

    def test_skip_already_cloud_native_cog(
        self,
        valid_rgb_cog: Path,
        tmp_path: Path,
    ) -> None:
        """Already cloud-native COG is skipped."""
        from portolan_cli.convert import ConversionStatus, convert_file

        source = tmp_path / "image.tif"
        shutil.copy(valid_rgb_cog, source)

        result = convert_file(source)

        assert result.status == ConversionStatus.SKIPPED
        assert result.format_from == "COG"
        assert result.output is None

    def test_convert_directory_mixed_formats(
        self,
        valid_points_geojson: Path,
        valid_points_parquet: Path,
        non_cog_tif: Path,
        valid_rgb_cog: Path,
        tmp_path: Path,
    ) -> None:
        """Convert directory with mixed formats produces correct report."""
        from portolan_cli.convert import convert_directory

        # Set up directory with mixed files
        input_dir = tmp_path / "data"
        input_dir.mkdir()

        # Convertible files
        shutil.copy(valid_points_geojson, input_dir / "vector.geojson")
        shutil.copy(non_cog_tif, input_dir / "raster.tif")

        # Already cloud-native files
        shutil.copy(valid_points_parquet, input_dir / "existing.parquet")
        shutil.copy(valid_rgb_cog, input_dir / "existing.cog.tif")

        report = convert_directory(input_dir)

        # All files should be processed
        assert report.total == 4
        # Convertible files should succeed
        assert report.succeeded == 2
        # Cloud-native files should be skipped
        assert report.skipped == 2
        # No failures
        assert report.failed == 0

    def test_convert_preserves_original_files(
        self,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Conversion creates new file, preserves original."""
        from portolan_cli.convert import ConversionStatus, convert_file

        source = tmp_path / "original.geojson"
        shutil.copy(valid_points_geojson, source)

        original_content = source.read_text()

        result = convert_file(source)

        assert result.status == ConversionStatus.SUCCESS
        # Original file should still exist
        assert source.exists()
        # Original content should be unchanged
        assert source.read_text() == original_content
        # Output file should be separate
        assert result.output != source

    def test_callback_invoked_for_progress(
        self,
        valid_points_geojson: Path,
        valid_polygons_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Progress callback is invoked for each file."""
        from portolan_cli.convert import ConversionResult, convert_directory

        input_dir = tmp_path / "data"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "a.geojson")
        shutil.copy(valid_polygons_geojson, input_dir / "b.geojson")

        progress_calls: list[ConversionResult] = []

        def on_progress(result: ConversionResult) -> None:
            progress_calls.append(result)

        report = convert_directory(input_dir, on_progress=on_progress)

        # Callback should be called for each file
        assert len(progress_calls) == 2
        assert len(progress_calls) == report.total


@pytest.mark.integration
@pytest.mark.realdata
class TestConvertWorkflowRealData:
    """Integration tests using real-world data fixtures.

    These tests use production data samples to verify conversion
    handles real-world edge cases correctly.
    """

    def test_convert_nwi_wetlands_complex_polygons(
        self,
        nwi_wetlands_path: Path,
        tmp_path: Path,
    ) -> None:
        """Convert NWI Wetlands (complex polygons with holes) - already GeoParquet."""
        from portolan_cli.convert import ConversionStatus, convert_file

        source = tmp_path / "wetlands.parquet"
        shutil.copy(nwi_wetlands_path, source)

        result = convert_file(source)

        # Should be skipped (already GeoParquet)
        assert result.status == ConversionStatus.SKIPPED
        assert result.format_from == "GeoParquet"

    def test_convert_open_buildings_bulk_data(
        self,
        open_buildings_path: Path,
        tmp_path: Path,
    ) -> None:
        """Convert Open Buildings (bulk polygons) - already GeoParquet."""
        from portolan_cli.convert import ConversionStatus, convert_file

        source = tmp_path / "buildings.parquet"
        shutil.copy(open_buildings_path, source)

        result = convert_file(source)

        # Should be skipped (already GeoParquet)
        assert result.status == ConversionStatus.SKIPPED

    def test_convert_road_detections_linestrings(
        self,
        road_detections_path: Path,
        tmp_path: Path,
    ) -> None:
        """Convert Road Detections (LineString geometries) - already GeoParquet."""
        from portolan_cli.convert import ConversionStatus, convert_file

        source = tmp_path / "roads.parquet"
        shutil.copy(road_detections_path, source)

        result = convert_file(source)

        # Should be skipped (already GeoParquet)
        assert result.status == ConversionStatus.SKIPPED

    def test_convert_rapidai4eo_cog(
        self,
        rapidai4eo_path: Path,
        tmp_path: Path,
    ) -> None:
        """Convert RapidAI4EO COG - already cloud-optimized."""
        from portolan_cli.convert import ConversionStatus, convert_file

        source = tmp_path / "satellite.tif"
        shutil.copy(rapidai4eo_path, source)

        result = convert_file(source)

        # Should be skipped (already COG)
        assert result.status == ConversionStatus.SKIPPED
        assert result.format_from == "COG"
