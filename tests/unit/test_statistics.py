"""Tests for statistics extraction module.

Tests raster band statistics (via rasterio) and parquet column statistics (via PyArrow).
Per ADR-0034: Stats enabled by default, approx mode for raster, PyArrow-only for parquet.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestBandStatistics:
    """Tests for BandStatistics dataclass."""

    def test_to_stac_dict_format(self) -> None:
        """BandStatistics.to_stac_dict returns correct format."""
        from portolan_cli.metadata.statistics import BandStatistics

        stats = BandStatistics(
            minimum=0.0,
            maximum=255.0,
            mean=127.5,
            stddev=45.2,
        )

        result = stats.to_stac_dict()

        assert result == {
            "minimum": 0.0,
            "maximum": 255.0,
            "mean": 127.5,
            "stddev": 45.2,
        }

    def test_optional_valid_percent(self) -> None:
        """BandStatistics handles optional valid_percent."""
        from portolan_cli.metadata.statistics import BandStatistics

        stats = BandStatistics(
            minimum=0.0,
            maximum=100.0,
            mean=50.0,
            stddev=10.0,
            valid_percent=98.5,
        )

        # valid_percent is stored but not in STAC output (STAC doesn't require it)
        assert stats.valid_percent == 98.5
        result = stats.to_stac_dict()
        assert "valid_percent" not in result


class TestExtractBandStatistics:
    """Tests for extract_band_statistics function."""

    @pytest.fixture
    def mock_rasterio_dataset(self) -> MagicMock:
        """Create a mock rasterio dataset."""
        mock_ds = MagicMock()
        mock_ds.count = 2  # 2 bands
        mock_ds.tags.return_value = {}  # No cached stats

        # Mock Statistics namedtuple
        @dataclass
        class MockStats:
            min: float
            max: float
            mean: float
            std: float

        mock_ds.statistics.side_effect = [
            MockStats(min=0.0, max=255.0, mean=127.5, std=45.2),
            MockStats(min=10.0, max=200.0, mean=100.0, std=30.0),
        ]
        return mock_ds

    def test_extracts_stats_approx_mode(self, mock_rasterio_dataset: MagicMock) -> None:
        """extract_band_statistics uses approx mode by default."""
        from portolan_cli.metadata.statistics import extract_band_statistics

        with patch("portolan_cli.metadata.statistics.rasterio.open") as mock_open:
            mock_open.return_value.__enter__.return_value = mock_rasterio_dataset

            results = extract_band_statistics(Path("test.tif"))

        assert len(results) == 2
        assert results[0].minimum == 0.0
        assert results[0].maximum == 255.0
        # Verify approx mode was used
        mock_rasterio_dataset.statistics.assert_called_with(2, approx=True)

    def test_uses_cached_stats_when_available(self) -> None:
        """extract_band_statistics uses cached GDAL stats from tags."""
        from portolan_cli.metadata.statistics import extract_band_statistics

        mock_ds = MagicMock()
        mock_ds.count = 1
        mock_ds.tags.return_value = {
            "STATISTICS_MINIMUM": "0.0",
            "STATISTICS_MAXIMUM": "255.0",
            "STATISTICS_MEAN": "127.5",
            "STATISTICS_STDDEV": "45.2",
            "STATISTICS_VALID_PERCENT": "100.0",
        }

        with patch("portolan_cli.metadata.statistics.rasterio.open") as mock_open:
            mock_open.return_value.__enter__.return_value = mock_ds

            results = extract_band_statistics(Path("test.tif"), mode="cached")

        assert len(results) == 1
        assert results[0].minimum == 0.0
        assert results[0].valid_percent == 100.0
        # Verify statistics() was NOT called (used cached)
        mock_ds.statistics.assert_not_called()

    def test_exact_mode_computes_full_stats(self, mock_rasterio_dataset: MagicMock) -> None:
        """extract_band_statistics uses exact mode when requested."""
        from portolan_cli.metadata.statistics import extract_band_statistics

        with patch("portolan_cli.metadata.statistics.rasterio.open") as mock_open:
            mock_open.return_value.__enter__.return_value = mock_rasterio_dataset

            extract_band_statistics(Path("test.tif"), mode="exact")

        # Verify approx=False was used
        mock_rasterio_dataset.statistics.assert_called_with(2, approx=False)


class TestColumnStatistics:
    """Tests for ColumnStatistics dataclass."""

    def test_to_stac_dict_format(self) -> None:
        """ColumnStatistics.to_stac_dict returns correct format."""
        from portolan_cli.metadata.statistics import ColumnStatistics

        stats = ColumnStatistics(
            name="population",
            min_value=0,
            max_value=1000000,
            null_count=5,
        )

        result = stats.to_stac_dict()

        assert result == {
            "minimum": 0,
            "maximum": 1000000,
            "null_count": 5,
        }

    def test_omits_none_values(self) -> None:
        """ColumnStatistics.to_stac_dict omits None values."""
        from portolan_cli.metadata.statistics import ColumnStatistics

        stats = ColumnStatistics(
            name="category",
            min_value=None,
            max_value=None,
            null_count=0,
        )

        result = stats.to_stac_dict()

        assert "minimum" not in result
        assert "maximum" not in result

    def test_omits_zero_null_count(self) -> None:
        """ColumnStatistics.to_stac_dict omits null_count when zero."""
        from portolan_cli.metadata.statistics import ColumnStatistics

        stats = ColumnStatistics(
            name="value",
            min_value=1,
            max_value=100,
            null_count=0,
        )

        result = stats.to_stac_dict()

        assert "null_count" not in result


class TestExtractParquetStatistics:
    """Tests for extract_parquet_statistics function."""

    def test_extracts_column_stats(self) -> None:
        """extract_parquet_statistics reads column stats from parquet metadata."""
        from portolan_cli.metadata.statistics import extract_parquet_statistics

        # Mock PyArrow parquet file
        mock_pf = MagicMock()
        mock_pf.metadata.num_row_groups = 1
        mock_pf.metadata.num_columns = 2

        # Mock schema
        mock_field1 = MagicMock()
        mock_field1.name = "id"
        mock_field2 = MagicMock()
        mock_field2.name = "value"
        mock_pf.schema_arrow = [mock_field1, mock_field2]

        # Mock row group column statistics
        mock_stats1 = MagicMock()
        mock_stats1.has_min_max = True
        mock_stats1.min = 1
        mock_stats1.max = 100
        mock_stats1.null_count = 0

        mock_stats2 = MagicMock()
        mock_stats2.has_min_max = True
        mock_stats2.min = 0.5
        mock_stats2.max = 99.5
        mock_stats2.null_count = 3

        mock_col1 = MagicMock()
        mock_col1.statistics = mock_stats1

        mock_col2 = MagicMock()
        mock_col2.statistics = mock_stats2

        mock_rg = MagicMock()
        mock_rg.column.side_effect = [mock_col1, mock_col2]
        mock_pf.metadata.row_group.return_value = mock_rg

        with patch("portolan_cli.metadata.statistics.pq.ParquetFile") as mock_pq:
            mock_pq.return_value = mock_pf

            results = extract_parquet_statistics(Path("test.parquet"))

        assert "id" in results
        assert results["id"].min_value == 1
        assert results["id"].max_value == 100
        assert results["value"].null_count == 3

    def test_handles_missing_stats(self) -> None:
        """extract_parquet_statistics handles columns without statistics."""
        from portolan_cli.metadata.statistics import extract_parquet_statistics

        mock_pf = MagicMock()
        mock_pf.metadata.num_row_groups = 1
        mock_pf.metadata.num_columns = 1

        mock_field = MagicMock()
        mock_field.name = "geometry"
        mock_pf.schema_arrow = [mock_field]

        mock_col = MagicMock()
        mock_col.statistics = None  # No stats for geometry columns

        mock_rg = MagicMock()
        mock_rg.column.return_value = mock_col
        mock_pf.metadata.row_group.return_value = mock_rg

        with patch("portolan_cli.metadata.statistics.pq.ParquetFile") as mock_pq:
            mock_pq.return_value = mock_pf

            results = extract_parquet_statistics(Path("test.parquet"))

        assert "geometry" in results
        assert results["geometry"].min_value is None
        assert results["geometry"].max_value is None
