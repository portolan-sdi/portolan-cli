"""Tests for progress output functionality.

Issue #203: Add progress printing for file-level operations.
"""

from io import StringIO
from pathlib import Path

import pytest

from portolan_cli.output import progress


class TestProgress:
    """Tests for the progress() output function."""

    def test_progress_basic_output(self) -> None:
        """Progress shows current/total and filename."""
        output = StringIO()
        progress("data.shp", current=1, total=5, file=output)
        result = output.getvalue()

        assert "1" in result
        assert "5" in result
        assert "data.shp" in result

    def test_progress_with_path_object(self) -> None:
        """Progress accepts Path objects."""
        output = StringIO()
        progress(Path("/some/path/census.geojson"), current=3, total=10, file=output)
        result = output.getvalue()

        assert "census.geojson" in result
        assert "3" in result
        assert "10" in result

    def test_progress_shows_arrow_prefix(self) -> None:
        """Progress uses arrow prefix like info()."""
        output = StringIO()
        progress("file.shp", current=1, total=1, file=output)
        result = output.getvalue()

        # Should use → prefix like info()
        assert "→" in result or "->" in result

    def test_progress_formats_file_n_of_m(self) -> None:
        """Progress shows 'file N of M' format."""
        output = StringIO()
        progress("test.parquet", current=2, total=7, file=output)
        result = output.getvalue()

        # Should contain "2 of 7" or similar
        assert "2" in result and "7" in result

    def test_progress_with_context(self) -> None:
        """Progress can include additional context."""
        output = StringIO()
        progress(
            "roads.shp",
            current=1,
            total=3,
            context="Converting to GeoParquet",
            file=output,
        )
        result = output.getvalue()

        assert "roads.shp" in result
        assert "Converting" in result or "GeoParquet" in result

    def test_progress_only_shows_filename_not_full_path(self) -> None:
        """Progress shows just filename, not full path."""
        output = StringIO()
        progress(
            Path("/very/long/path/to/data/census-2020.parquet"),
            current=1,
            total=1,
            file=output,
        )
        result = output.getvalue()

        assert "census-2020.parquet" in result
        assert "/very/long/path" not in result

    @pytest.mark.parametrize(
        "current,total",
        [
            (1, 1),
            (1, 100),
            (50, 100),
            (100, 100),
        ],
    )
    def test_progress_various_counts(self, current: int, total: int) -> None:
        """Progress handles various count combinations."""
        output = StringIO()
        progress("file.shp", current=current, total=total, file=output)
        result = output.getvalue()

        assert str(current) in result
        assert str(total) in result
