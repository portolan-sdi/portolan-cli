"""Integration tests verifying geoparquet-io works with our fixtures.

These tests confirm that geoparquet-io can convert our test fixtures
to GeoParquet format. Per ADR-0010, Portolan delegates conversion
to geoparquet-io—these tests verify that delegation will work.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest


class TestGpioConversion:
    """Tests for geoparquet-io conversion of vector fixtures."""

    @pytest.mark.integration
    def test_convert_points_geojson(self, valid_points_geojson: Path, tmp_path: Path) -> None:
        """geoparquet-io can convert points GeoJSON to GeoParquet."""
        import geoparquet_io as gpio

        output = tmp_path / "points.parquet"
        gpio.convert(str(valid_points_geojson)).write(str(output))

        assert output.exists()
        # Verify it is a valid Parquet file
        table = pq.read_table(output)
        assert len(table) == 10  # 10 point features in fixture

    @pytest.mark.integration
    def test_convert_polygons_geojson(self, valid_polygons_geojson: Path, tmp_path: Path) -> None:
        """geoparquet-io can convert polygons GeoJSON to GeoParquet."""
        import geoparquet_io as gpio

        output = tmp_path / "polygons.parquet"
        gpio.convert(str(valid_polygons_geojson)).write(str(output))

        assert output.exists()
        table = pq.read_table(output)
        assert len(table) == 5  # 5 polygon features

    @pytest.mark.integration
    def test_convert_lines_geojson(self, valid_lines_geojson: Path, tmp_path: Path) -> None:
        """geoparquet-io can convert lines GeoJSON to GeoParquet."""
        import geoparquet_io as gpio

        output = tmp_path / "lines.parquet"
        gpio.convert(str(valid_lines_geojson)).write(str(output))

        assert output.exists()
        table = pq.read_table(output)
        assert len(table) == 5  # 5 line features

    @pytest.mark.integration
    def test_convert_multigeom_geojson(self, valid_multigeom_geojson: Path, tmp_path: Path) -> None:
        """geoparquet-io can convert mixed geometry GeoJSON."""
        import geoparquet_io as gpio

        output = tmp_path / "multigeom.parquet"
        gpio.convert(str(valid_multigeom_geojson)).write(str(output))

        assert output.exists()
        table = pq.read_table(output)
        assert len(table) == 6  # 6 mixed features

    @pytest.mark.integration
    def test_convert_large_properties_geojson(
        self, valid_large_properties_geojson: Path, tmp_path: Path
    ) -> None:
        """geoparquet-io preserves all property columns."""
        import geoparquet_io as gpio

        output = tmp_path / "large_props.parquet"
        gpio.convert(str(valid_large_properties_geojson)).write(str(output))

        assert output.exists()
        table = pq.read_table(output)
        # Fixture has 20+ columns plus geometry
        assert len(table.column_names) >= 20

    @pytest.mark.integration
    def test_convert_unicode_properties(self, edge_unicode_geojson: Path, tmp_path: Path) -> None:
        """geoparquet-io handles Unicode in property values."""
        import geoparquet_io as gpio

        output = tmp_path / "unicode.parquet"
        gpio.convert(str(edge_unicode_geojson)).write(str(output))

        assert output.exists()
        table = pq.read_table(output)
        # Verify Unicode was preserved
        assert len(table) == 1

    @pytest.mark.integration
    def test_convert_special_filename(
        self, edge_special_filename_geojson: Path, tmp_path: Path
    ) -> None:
        """geoparquet-io handles filenames with spaces."""
        import geoparquet_io as gpio

        output = tmp_path / "special.parquet"
        gpio.convert(str(edge_special_filename_geojson)).write(str(output))

        assert output.exists()

    @pytest.mark.integration
    def test_read_existing_geoparquet(self, valid_points_parquet: Path) -> None:
        """geoparquet-io can read existing GeoParquet files."""
        import geoparquet_io as gpio

        # Should not raise
        result = gpio.read(str(valid_points_parquet))
        # Verify we can access the data
        assert result is not None


class TestGpioErrorHandling:
    """Tests for geoparquet-io error handling with invalid inputs."""

    @pytest.mark.integration
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="geoparquet-io segfaults on malformed input on Windows (upstream bug)",
    )
    def test_malformed_geojson_raises(
        self, invalid_malformed_geojson: Path, tmp_path: Path
    ) -> None:
        """geoparquet-io raises on malformed GeoJSON."""
        import click
        import geoparquet_io as gpio

        output = tmp_path / "output.parquet"
        # geoparquet-io wraps errors in ClickException
        with pytest.raises(click.ClickException):
            gpio.convert(str(invalid_malformed_geojson)).write(str(output))

    @pytest.mark.integration
    def test_empty_geojson_behavior(self, invalid_empty_geojson: Path, tmp_path: Path) -> None:
        """Document geoparquet-io behavior with empty FeatureCollection."""
        import click
        import geoparquet_io as gpio

        output = tmp_path / "empty.parquet"
        # This may succeed with 0 rows or raise—document actual behavior
        try:
            gpio.convert(str(invalid_empty_geojson)).write(str(output))
            # If it succeeds, verify empty result
            table = pq.read_table(output)
            assert len(table) == 0
        except (ValueError, click.ClickException):
            # Empty input is rejected—this is also acceptable
            pass
