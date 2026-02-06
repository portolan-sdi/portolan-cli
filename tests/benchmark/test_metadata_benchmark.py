"""Benchmark tests for metadata extraction operations.

Establishes performance baselines for GeoParquet and COG metadata extraction.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.metadata import extract_cog_metadata, extract_geoparquet_metadata


@pytest.mark.benchmark
def test_geoparquet_metadata_extraction_performance(
    benchmark,
    valid_points_parquet: Path,  # type: ignore[no-untyped-def]
) -> None:
    """Benchmark GeoParquet metadata extraction.

    This measures the time to extract metadata from a GeoParquet file.
    Uses metadata-only reads, so should be fast regardless of file size.
    """
    result = benchmark(extract_geoparquet_metadata, valid_points_parquet)
    assert result.feature_count > 0


@pytest.mark.benchmark
def test_cog_metadata_extraction_performance(
    benchmark,
    valid_rgb_cog: Path,  # type: ignore[no-untyped-def]
) -> None:
    """Benchmark COG metadata extraction.

    This measures the time to extract metadata from a COG file.
    Uses header-only reads, so should be fast regardless of file size.
    """
    result = benchmark(extract_cog_metadata, valid_rgb_cog)
    assert result.width > 0


@pytest.mark.benchmark
def test_cog_singleband_metadata_extraction_performance(
    benchmark,
    valid_singleband_cog: Path,  # type: ignore[no-untyped-def]
) -> None:
    """Benchmark single-band COG metadata extraction.

    Compares performance between single-band and multi-band COGs.
    """
    result = benchmark(extract_cog_metadata, valid_singleband_cog)
    assert result.band_count == 1
