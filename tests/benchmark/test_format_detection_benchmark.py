"""Benchmarks for format detection to prevent performance regression.

Format detection runs on every file during dataset add, so it must be fast.
These benchmarks establish baselines and catch regressions.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.formats import FormatType, detect_format


class TestFormatDetectionBenchmarks:
    """Benchmarks for detect_format function."""

    @pytest.mark.benchmark(group="format-detection")
    def test_detect_geojson_performance(
        self,
        benchmark,  # type: ignore[no-untyped-def]
        valid_points_geojson: Path,
    ) -> None:
        """Benchmark GeoJSON detection speed."""
        result = benchmark(detect_format, valid_points_geojson)
        assert result == FormatType.VECTOR

    @pytest.mark.benchmark(group="format-detection")
    def test_detect_parquet_performance(
        self,
        benchmark,  # type: ignore[no-untyped-def]
        valid_points_parquet: Path,
    ) -> None:
        """Benchmark Parquet detection speed (extension-only, should be fastest)."""
        result = benchmark(detect_format, valid_points_parquet)
        assert result == FormatType.VECTOR

    @pytest.mark.benchmark(group="format-detection")
    def test_detect_tiff_performance(
        self,
        benchmark,  # type: ignore[no-untyped-def]
        valid_rgb_cog: Path,
    ) -> None:
        """Benchmark TIFF detection speed."""
        result = benchmark(detect_format, valid_rgb_cog)
        assert result == FormatType.RASTER

    @pytest.mark.benchmark(group="format-detection")
    def test_detect_json_needing_content_check(
        self,
        benchmark,  # type: ignore[no-untyped-def]
        tmp_path: Path,
    ) -> None:
        """Benchmark .json files that require content inspection."""
        # Create a GeoJSON with .json extension
        json_file = tmp_path / "test.json"
        json_file.write_text('{"type": "FeatureCollection", "features": []}')

        result = benchmark(detect_format, json_file)
        assert result == FormatType.VECTOR
