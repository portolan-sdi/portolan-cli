"""Benchmarks for conversion operations to track performance over time.

Conversion performance depends on upstream libraries, but we track it
to detect regressions from dependency updates or environmental changes.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_bounds


class TestGpioConversionBenchmarks:
    """Benchmarks for geoparquet-io conversion."""

    @pytest.mark.benchmark(group="gpio-conversion")
    @pytest.mark.slow  # Conversion can take > 100ms
    def test_convert_points_geojson_performance(
        self,
        benchmark,  # type: ignore[no-untyped-def]
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Benchmark GeoJSON to GeoParquet conversion (10 points)."""
        import geoparquet_io as gpio

        output = tmp_path / "output.parquet"

        def convert() -> None:
            gpio.convert(str(valid_points_geojson)).write(str(output))
            # Clean up for next iteration
            if output.exists():
                output.unlink()

        benchmark(convert)

    @pytest.mark.benchmark(group="gpio-conversion")
    @pytest.mark.slow
    def test_convert_polygons_geojson_performance(
        self,
        benchmark,  # type: ignore[no-untyped-def]
        valid_polygons_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Benchmark polygon GeoJSON conversion (5 polygons)."""
        import geoparquet_io as gpio

        output = tmp_path / "output.parquet"

        def convert() -> None:
            gpio.convert(str(valid_polygons_geojson)).write(str(output))
            if output.exists():
                output.unlink()

        benchmark(convert)


class TestRioCogeoConversionBenchmarks:
    """Benchmarks for rio-cogeo conversion."""

    @pytest.mark.benchmark(group="riocogeo-conversion")
    @pytest.mark.slow
    def test_cog_validation_performance(
        self,
        benchmark,  # type: ignore[no-untyped-def]
        valid_rgb_cog: Path,
    ) -> None:
        """Benchmark COG validation speed."""
        from rio_cogeo.cogeo import cog_validate

        def validate() -> tuple[bool, list[str], list[str]]:
            return cog_validate(str(valid_rgb_cog))

        is_valid, _, _ = benchmark(validate)
        assert is_valid

    @pytest.mark.benchmark(group="riocogeo-conversion")
    @pytest.mark.slow
    def test_cog_translate_performance(
        self,
        benchmark,  # type: ignore[no-untyped-def]
        tmp_path: Path,
    ) -> None:
        """Benchmark COG translation from non-COG source."""
        from rio_cogeo.cogeo import cog_translate
        from rio_cogeo.profiles import cog_profiles

        # Create source non-COG
        source = tmp_path / "source.tif"
        width, height = 64, 64
        transform = from_bounds(-122.5, 37.7, -122.3, 37.9, width, height)

        with rasterio.open(
            source,
            "w",
            driver="GTiff",
            height=height,
            width=width,
            count=1,
            dtype="uint8",
            crs="EPSG:4326",
            transform=transform,
        ) as dst:
            dst.write(np.ones((1, height, width), dtype=np.uint8) * 128)

        output = tmp_path / "output.tif"
        profile = cog_profiles.get("deflate")

        def translate() -> None:
            cog_translate(str(source), str(output), profile, quiet=True)
            if output.exists():
                output.unlink()

        benchmark(translate)
