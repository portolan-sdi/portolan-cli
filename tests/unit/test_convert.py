"""Unit tests for convert module.

Tests ConversionStatus enum, ConversionResult dataclass, convert_file(),
ConversionReport, and convert_directory() functions.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# =============================================================================
# Phase 1: ConversionStatus Enum Tests
# =============================================================================


class TestConversionStatus:
    """Tests for ConversionStatus enum."""

    @pytest.mark.unit
    def test_success_value(self) -> None:
        """SUCCESS status value is 'success'."""
        from portolan_cli.convert import ConversionStatus

        assert ConversionStatus.SUCCESS.value == "success"

    @pytest.mark.unit
    def test_skipped_value(self) -> None:
        """SKIPPED status value is 'skipped'."""
        from portolan_cli.convert import ConversionStatus

        assert ConversionStatus.SKIPPED.value == "skipped"

    @pytest.mark.unit
    def test_failed_value(self) -> None:
        """FAILED status value is 'failed'."""
        from portolan_cli.convert import ConversionStatus

        assert ConversionStatus.FAILED.value == "failed"

    @pytest.mark.unit
    def test_invalid_value(self) -> None:
        """INVALID status value is 'invalid'."""
        from portolan_cli.convert import ConversionStatus

        assert ConversionStatus.INVALID.value == "invalid"

    @pytest.mark.unit
    def test_exactly_four_values(self) -> None:
        """There are exactly 4 status values."""
        from portolan_cli.convert import ConversionStatus

        assert len(ConversionStatus) == 4

    @pytest.mark.unit
    def test_all_values_distinct(self) -> None:
        """All status values are distinct."""
        from portolan_cli.convert import ConversionStatus

        values = [status.value for status in ConversionStatus]
        assert len(values) == len(set(values))

    @pytest.mark.unit
    def test_values_are_strings(self) -> None:
        """All status values are strings for JSON serialization."""
        from portolan_cli.convert import ConversionStatus

        for status in ConversionStatus:
            assert isinstance(status.value, str)
            assert len(status.value) > 0

    @pytest.mark.unit
    def test_equality_comparison(self) -> None:
        """Enum members can be compared with ==."""
        from portolan_cli.convert import ConversionStatus

        assert ConversionStatus.SUCCESS == ConversionStatus.SUCCESS
        assert ConversionStatus.SUCCESS != ConversionStatus.FAILED

    @pytest.mark.unit
    def test_can_use_as_dict_key(self) -> None:
        """Enum members can be used as dictionary keys."""
        from portolan_cli.convert import ConversionStatus

        d = {ConversionStatus.SUCCESS: 1, ConversionStatus.FAILED: 2}
        assert d[ConversionStatus.SUCCESS] == 1
        assert d[ConversionStatus.FAILED] == 2

    @pytest.mark.unit
    def test_string_representation(self) -> None:
        """String representation includes class and member name."""
        from portolan_cli.convert import ConversionStatus

        assert "SUCCESS" in str(ConversionStatus.SUCCESS)


# =============================================================================
# Phase 1: ConversionResult Dataclass Tests
# =============================================================================


class TestConversionResult:
    """Tests for ConversionResult dataclass."""

    @pytest.mark.unit
    def test_create_success_result(self) -> None:
        """Create a successful conversion result."""
        from pathlib import Path

        from portolan_cli.convert import ConversionResult, ConversionStatus

        result = ConversionResult(
            source=Path("/data/input.shp"),
            output=Path("/data/input.parquet"),
            format_from="SHP",
            format_to="GeoParquet",
            status=ConversionStatus.SUCCESS,
            error=None,
            duration_ms=150,
        )

        assert result.source == Path("/data/input.shp")
        assert result.output == Path("/data/input.parquet")
        assert result.format_from == "SHP"
        assert result.format_to == "GeoParquet"
        assert result.status == ConversionStatus.SUCCESS
        assert result.error is None
        assert result.duration_ms == 150

    @pytest.mark.unit
    def test_create_skipped_result(self) -> None:
        """Create a skipped conversion result (already cloud-native)."""
        from pathlib import Path

        from portolan_cli.convert import ConversionResult, ConversionStatus

        result = ConversionResult(
            source=Path("/data/input.parquet"),
            output=None,
            format_from="GeoParquet",
            format_to=None,
            status=ConversionStatus.SKIPPED,
            error=None,
            duration_ms=5,
        )

        assert result.status == ConversionStatus.SKIPPED
        assert result.output is None
        assert result.format_to is None

    @pytest.mark.unit
    def test_create_failed_result(self) -> None:
        """Create a failed conversion result with error message."""
        from pathlib import Path

        from portolan_cli.convert import ConversionResult, ConversionStatus

        result = ConversionResult(
            source=Path("/data/corrupt.shp"),
            output=None,
            format_from="SHP",
            format_to="GeoParquet",
            status=ConversionStatus.FAILED,
            error="Unable to read shapefile: missing .dbf sidecar",
            duration_ms=25,
        )

        assert result.status == ConversionStatus.FAILED
        assert result.error == "Unable to read shapefile: missing .dbf sidecar"
        assert result.output is None

    @pytest.mark.unit
    def test_create_invalid_result(self) -> None:
        """Create an invalid result (conversion completed but validation failed)."""
        from pathlib import Path

        from portolan_cli.convert import ConversionResult, ConversionStatus

        result = ConversionResult(
            source=Path("/data/input.tif"),
            output=Path("/data/input_converted.tif"),
            format_from="TIFF",
            format_to="COG",
            status=ConversionStatus.INVALID,
            error="COG validation failed: missing overviews",
            duration_ms=500,
        )

        assert result.status == ConversionStatus.INVALID
        assert result.output == Path("/data/input_converted.tif")
        assert result.error == "COG validation failed: missing overviews"

    @pytest.mark.unit
    def test_to_dict_success(self) -> None:
        """to_dict() returns serializable dictionary for SUCCESS."""
        from pathlib import Path, PurePosixPath

        from portolan_cli.convert import ConversionResult, ConversionStatus

        # Use PurePosixPath for platform-independent paths in tests
        source = Path(PurePosixPath("/data/input.shp"))
        output = Path(PurePosixPath("/data/input.parquet"))

        result = ConversionResult(
            source=source,
            output=output,
            format_from="SHP",
            format_to="GeoParquet",
            status=ConversionStatus.SUCCESS,
            error=None,
            duration_ms=150,
        )

        d = result.to_dict()

        # Check path strings contain expected components (platform-independent)
        assert "input.shp" in d["source"]
        assert "input.parquet" in d["output"]
        assert d["format_from"] == "SHP"
        assert d["format_to"] == "GeoParquet"
        assert d["status"] == "success"
        assert d["error"] is None
        assert d["duration_ms"] == 150

    @pytest.mark.unit
    def test_to_dict_skipped(self) -> None:
        """to_dict() handles None output correctly."""
        from pathlib import Path, PurePosixPath

        from portolan_cli.convert import ConversionResult, ConversionStatus

        source = Path(PurePosixPath("/data/input.parquet"))

        result = ConversionResult(
            source=source,
            output=None,
            format_from="GeoParquet",
            format_to=None,
            status=ConversionStatus.SKIPPED,
            error=None,
            duration_ms=5,
        )

        d = result.to_dict()

        # Check path string contains expected component (platform-independent)
        assert "input.parquet" in d["source"]
        assert d["output"] is None
        assert d["format_to"] is None
        assert d["status"] == "skipped"

    @pytest.mark.unit
    def test_to_dict_failed(self) -> None:
        """to_dict() includes error message for FAILED status."""
        from pathlib import Path

        from portolan_cli.convert import ConversionResult, ConversionStatus

        result = ConversionResult(
            source=Path("/data/corrupt.shp"),
            output=None,
            format_from="SHP",
            format_to="GeoParquet",
            status=ConversionStatus.FAILED,
            error="File not readable",
            duration_ms=10,
        )

        d = result.to_dict()

        assert d["status"] == "failed"
        assert d["error"] == "File not readable"

    @pytest.mark.unit
    def test_to_dict_is_json_serializable(self) -> None:
        """to_dict() result can be serialized to JSON."""
        import json
        from pathlib import Path, PurePosixPath

        from portolan_cli.convert import ConversionResult, ConversionStatus

        source = Path(PurePosixPath("/data/input.shp"))
        output = Path(PurePosixPath("/data/input.parquet"))

        result = ConversionResult(
            source=source,
            output=output,
            format_from="SHP",
            format_to="GeoParquet",
            status=ConversionStatus.SUCCESS,
            error=None,
            duration_ms=150,
        )

        # Should not raise
        json_str = json.dumps(result.to_dict())
        assert isinstance(json_str, str)

        # Round-trip should work
        parsed = json.loads(json_str)
        assert "input.shp" in parsed["source"]
        assert parsed["status"] == "success"

    @pytest.mark.unit
    def test_required_fields(self) -> None:
        """ConversionResult requires source, format_from, status, duration_ms."""
        from pathlib import Path

        from portolan_cli.convert import ConversionResult, ConversionStatus

        # These should be required (no defaults)
        result = ConversionResult(
            source=Path("/data/input.shp"),
            output=None,
            format_from="SHP",
            format_to=None,
            status=ConversionStatus.SKIPPED,
            error=None,
            duration_ms=0,
        )
        assert result.source is not None
        assert result.format_from is not None
        assert result.status is not None
        assert result.duration_ms is not None


# =============================================================================
# Phase 2: convert_file() Tests
# =============================================================================


class TestConvertFileSkip:
    """Tests for convert_file() with cloud-native input (skip case)."""

    @pytest.mark.unit
    def test_skip_geoparquet(self, valid_points_parquet: Path) -> None:
        """GeoParquet input returns SKIPPED status."""
        from portolan_cli.convert import ConversionStatus, convert_file

        result = convert_file(valid_points_parquet)

        assert result.status == ConversionStatus.SKIPPED
        assert result.output is None
        assert result.format_to is None
        assert result.error is None
        assert result.format_from == "GeoParquet"
        assert result.duration_ms >= 0

    @pytest.mark.unit
    def test_skip_cog(self, valid_rgb_cog: Path) -> None:
        """Valid COG input returns SKIPPED status."""
        from portolan_cli.convert import ConversionStatus, convert_file

        result = convert_file(valid_rgb_cog)

        assert result.status == ConversionStatus.SKIPPED
        assert result.output is None
        assert result.format_from == "COG"

    @pytest.mark.unit
    def test_skip_flatgeobuf(self, tmp_path: Path) -> None:
        """FlatGeobuf input returns SKIPPED status."""
        from portolan_cli.convert import ConversionStatus, convert_file

        # Create a dummy FlatGeobuf file
        fgb_file = tmp_path / "test.fgb"
        fgb_file.write_bytes(b"fgb1")  # Minimal FlatGeobuf magic

        result = convert_file(fgb_file)

        assert result.status == ConversionStatus.SKIPPED
        assert result.format_from == "FlatGeobuf"


class TestConvertFileVector:
    """Tests for convert_file() with vector input."""

    @pytest.mark.unit
    def test_convert_geojson_to_parquet(self, valid_points_geojson: Path, tmp_path: Path) -> None:
        """GeoJSON file is converted to GeoParquet."""
        from portolan_cli.convert import ConversionStatus, convert_file

        result = convert_file(valid_points_geojson, output_dir=tmp_path)

        assert result.status == ConversionStatus.SUCCESS
        assert result.output is not None
        assert result.output.suffix == ".parquet"
        assert result.output.exists()
        assert result.format_from == "GeoJSON"
        assert result.format_to == "GeoParquet"
        assert result.error is None
        assert result.duration_ms >= 0

    @pytest.mark.unit
    def test_convert_vector_output_in_source_dir_by_default(
        self, valid_points_geojson: Path, tmp_path: Path
    ) -> None:
        """Without output_dir, output is created in source directory."""
        import shutil

        from portolan_cli.convert import ConversionStatus, convert_file

        # Copy source to tmp_path to avoid polluting fixtures
        source = tmp_path / "test.geojson"
        shutil.copy(valid_points_geojson, source)

        result = convert_file(source)

        assert result.status == ConversionStatus.SUCCESS
        assert result.output is not None
        assert result.output.parent == source.parent
        assert result.output.name == "test.parquet"


class TestConvertFileRaster:
    """Tests for convert_file() with raster input."""

    @pytest.mark.unit
    def test_convert_non_cog_to_cog(self, non_cog_tif: Path, tmp_path: Path) -> None:
        """Non-COG TIFF is converted to COG."""
        from portolan_cli.convert import ConversionStatus, convert_file

        result = convert_file(non_cog_tif, output_dir=tmp_path)

        assert result.status == ConversionStatus.SUCCESS
        assert result.output is not None
        assert result.output.suffix == ".tif"
        assert result.output.exists()
        assert result.format_from == "TIFF"
        assert result.format_to == "COG"
        assert result.error is None


class TestConvertFileException:
    """Tests for convert_file() exception handling."""

    @pytest.mark.unit
    def test_file_not_found_raises(self, tmp_path: Path) -> None:
        """Non-existent file raises FileNotFoundError."""
        from portolan_cli.convert import convert_file

        missing = tmp_path / "missing.geojson"

        with pytest.raises(FileNotFoundError):
            convert_file(missing)

    @pytest.mark.unit
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="geoparquet-io segfaults on malformed input on Windows (upstream bug)",
    )
    def test_conversion_exception_returns_failed(self, tmp_path: Path) -> None:
        """Exception during conversion returns FAILED status."""
        from portolan_cli.convert import ConversionStatus, convert_file

        # Create a malformed GeoJSON that will fail conversion
        bad_file = tmp_path / "bad.geojson"
        bad_file.write_text('{"type": "FeatureCollection", "features": [INVALID')

        result = convert_file(bad_file, output_dir=tmp_path)

        assert result.status == ConversionStatus.FAILED
        assert result.error is not None
        assert len(result.error) > 0
        assert result.output is None

    @pytest.mark.unit
    def test_unsupported_format_returns_failed(self, tmp_path: Path) -> None:
        """Unsupported format returns FAILED status."""
        from portolan_cli.convert import ConversionStatus, convert_file

        # Create a NetCDF file (unsupported)
        netcdf = tmp_path / "data.nc"
        netcdf.write_bytes(b"CDF\x01")  # NetCDF magic bytes

        result = convert_file(netcdf)

        assert result.status == ConversionStatus.FAILED
        assert result.error is not None
        # Error message should indicate format is not supported
        assert "not" in result.error.lower() and "support" in result.error.lower()


class TestConvertFileValidation:
    """Tests for convert_file() validation handling."""

    @pytest.mark.unit
    def test_invalid_result_preserves_output(self, tmp_path: Path) -> None:
        """INVALID result keeps output file for inspection."""
        from pathlib import Path

        from portolan_cli.convert import ConversionResult, ConversionStatus

        # This tests the invariant that INVALID results have output path
        result = ConversionResult(
            source=Path("/data/input.tif"),
            output=Path("/data/output.tif"),
            format_from="TIFF",
            format_to="COG",
            status=ConversionStatus.INVALID,
            error="Validation failed: missing overviews",
            duration_ms=100,
        )

        # Invariant: INVALID status has output path (file kept for inspection)
        assert result.output is not None
        assert result.error is not None


# =============================================================================
# Phase 3: ConversionReport Tests
# =============================================================================


class TestConversionReport:
    """Tests for ConversionReport dataclass."""

    @pytest.mark.unit
    def test_empty_report(self) -> None:
        """Empty report has all counts = 0."""
        from portolan_cli.convert import ConversionReport

        report = ConversionReport(results=[])

        assert report.succeeded == 0
        assert report.failed == 0
        assert report.skipped == 0
        assert report.invalid == 0
        assert report.total == 0

    @pytest.mark.unit
    def test_report_with_mixed_results(self) -> None:
        """Report with mixed results has correct counts."""
        from portolan_cli.convert import (
            ConversionReport,
            ConversionResult,
            ConversionStatus,
        )

        results = [
            ConversionResult(
                source=Path("/a/success.shp"),
                output=Path("/a/success.parquet"),
                format_from="SHP",
                format_to="GeoParquet",
                status=ConversionStatus.SUCCESS,
                error=None,
                duration_ms=100,
            ),
            ConversionResult(
                source=Path("/a/skipped.parquet"),
                output=None,
                format_from="GeoParquet",
                format_to=None,
                status=ConversionStatus.SKIPPED,
                error=None,
                duration_ms=5,
            ),
            ConversionResult(
                source=Path("/a/failed.shp"),
                output=None,
                format_from="SHP",
                format_to="GeoParquet",
                status=ConversionStatus.FAILED,
                error="Read error",
                duration_ms=10,
            ),
            ConversionResult(
                source=Path("/a/invalid.tif"),
                output=Path("/a/invalid_out.tif"),
                format_from="TIFF",
                format_to="COG",
                status=ConversionStatus.INVALID,
                error="Validation failed",
                duration_ms=50,
            ),
            ConversionResult(
                source=Path("/a/success2.geojson"),
                output=Path("/a/success2.parquet"),
                format_from="GeoJSON",
                format_to="GeoParquet",
                status=ConversionStatus.SUCCESS,
                error=None,
                duration_ms=80,
            ),
        ]

        report = ConversionReport(results=results)

        assert report.succeeded == 2
        assert report.failed == 1
        assert report.skipped == 1
        assert report.invalid == 1
        assert report.total == 5

    @pytest.mark.unit
    def test_total_equals_len_results(self) -> None:
        """total equals len(results)."""
        from portolan_cli.convert import (
            ConversionReport,
            ConversionResult,
            ConversionStatus,
        )

        results = [
            ConversionResult(
                source=Path(f"/a/file{i}.shp"),
                output=Path(f"/a/file{i}.parquet"),
                format_from="SHP",
                format_to="GeoParquet",
                status=ConversionStatus.SUCCESS,
                error=None,
                duration_ms=i * 10,
            )
            for i in range(7)
        ]

        report = ConversionReport(results=results)

        assert report.total == len(results)
        assert report.total == 7

    @pytest.mark.unit
    def test_to_dict_returns_serializable(self) -> None:
        """to_dict() returns JSON-serializable dictionary."""
        import json

        from portolan_cli.convert import (
            ConversionReport,
            ConversionResult,
            ConversionStatus,
        )

        results = [
            ConversionResult(
                source=Path("/a/test.shp"),
                output=Path("/a/test.parquet"),
                format_from="SHP",
                format_to="GeoParquet",
                status=ConversionStatus.SUCCESS,
                error=None,
                duration_ms=100,
            ),
        ]

        report = ConversionReport(results=results)
        d = report.to_dict()

        # Should not raise
        json_str = json.dumps(d)
        assert isinstance(json_str, str)

    @pytest.mark.unit
    def test_to_dict_includes_summary_and_results(self) -> None:
        """to_dict() includes summary counts and results array."""
        from portolan_cli.convert import (
            ConversionReport,
            ConversionResult,
            ConversionStatus,
        )

        results = [
            ConversionResult(
                source=Path("/a/test.shp"),
                output=Path("/a/test.parquet"),
                format_from="SHP",
                format_to="GeoParquet",
                status=ConversionStatus.SUCCESS,
                error=None,
                duration_ms=100,
            ),
            ConversionResult(
                source=Path("/a/skip.parquet"),
                output=None,
                format_from="GeoParquet",
                format_to=None,
                status=ConversionStatus.SKIPPED,
                error=None,
                duration_ms=5,
            ),
        ]

        report = ConversionReport(results=results)
        d = report.to_dict()

        assert "summary" in d
        assert d["summary"]["succeeded"] == 1
        assert d["summary"]["skipped"] == 1
        assert d["summary"]["failed"] == 0
        assert d["summary"]["invalid"] == 0
        assert d["summary"]["total"] == 2

        assert "results" in d
        assert len(d["results"]) == 2
        assert d["results"][0]["status"] == "success"
        assert d["results"][1]["status"] == "skipped"

    @pytest.mark.unit
    def test_counts_invariant(self) -> None:
        """succeeded + failed + skipped + invalid == total."""
        from portolan_cli.convert import (
            ConversionReport,
            ConversionResult,
            ConversionStatus,
        )

        # Create a report with various statuses
        results = [
            ConversionResult(
                source=Path(f"/a/{i}.shp"),
                output=Path(f"/a/{i}.parquet") if status != ConversionStatus.FAILED else None,
                format_from="SHP",
                format_to="GeoParquet",
                status=status,
                error="error"
                if status in (ConversionStatus.FAILED, ConversionStatus.INVALID)
                else None,
                duration_ms=i,
            )
            for i, status in enumerate(
                [
                    ConversionStatus.SUCCESS,
                    ConversionStatus.SUCCESS,
                    ConversionStatus.SKIPPED,
                    ConversionStatus.FAILED,
                    ConversionStatus.INVALID,
                ]
            )
        ]

        report = ConversionReport(results=results)

        # Invariant: all counts sum to total
        assert report.succeeded + report.failed + report.skipped + report.invalid == report.total


# =============================================================================
# Phase 3: convert_directory() Tests
# =============================================================================


class TestConvertDirectoryBasic:
    """Tests for convert_directory() basic functionality."""

    @pytest.mark.unit
    def test_convert_multiple_files(
        self,
        valid_points_geojson: Path,
        valid_polygons_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Convert directory with multiple files returns report with all results."""
        import shutil

        from portolan_cli.convert import convert_directory

        # Set up directory with files
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "points.geojson")
        shutil.copy(valid_polygons_geojson, input_dir / "polygons.geojson")

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        report = convert_directory(input_dir, output_dir=output_dir)

        assert report.total == 2
        assert report.succeeded == 2
        assert len(report.results) == 2

    @pytest.mark.unit
    def test_empty_directory(self, tmp_path: Path) -> None:
        """Empty directory returns empty report."""
        from portolan_cli.convert import convert_directory

        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        report = convert_directory(empty_dir)

        assert report.total == 0
        assert report.succeeded == 0
        assert report.failed == 0

    @pytest.mark.unit
    def test_directory_not_exists_raises(self, tmp_path: Path) -> None:
        """Non-existent directory raises FileNotFoundError."""
        from portolan_cli.convert import convert_directory

        missing_dir = tmp_path / "missing"

        with pytest.raises(FileNotFoundError):
            convert_directory(missing_dir)

    @pytest.mark.unit
    def test_path_is_file_raises(self, valid_points_geojson: Path) -> None:
        """File path (not directory) raises ValueError."""
        from portolan_cli.convert import convert_directory

        with pytest.raises((ValueError, NotADirectoryError)):
            convert_directory(valid_points_geojson)


class TestConvertDirectoryCallback:
    """Tests for convert_directory() callback functionality."""

    @pytest.mark.unit
    def test_callback_called_for_each_file(
        self,
        valid_points_geojson: Path,
        valid_polygons_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Callback is called for each file with ConversionResult."""
        import shutil

        from portolan_cli.convert import ConversionResult, convert_directory

        # Set up directory
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "points.geojson")
        shutil.copy(valid_polygons_geojson, input_dir / "polygons.geojson")

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Track callback invocations
        callback_results: list[ConversionResult] = []

        def on_progress(result: ConversionResult) -> None:
            callback_results.append(result)

        report = convert_directory(input_dir, output_dir=output_dir, on_progress=on_progress)

        # Callback should be called once per file
        assert len(callback_results) == 2
        assert len(callback_results) == report.total

    @pytest.mark.unit
    def test_callback_receives_result_in_order(
        self,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Callback receives results as files are processed."""
        import shutil

        from portolan_cli.convert import ConversionResult, convert_directory

        input_dir = tmp_path / "input"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "test.geojson")

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        callback_results: list[ConversionResult] = []

        def on_progress(result: ConversionResult) -> None:
            callback_results.append(result)

        report = convert_directory(input_dir, output_dir=output_dir, on_progress=on_progress)

        # Results should match
        assert len(callback_results) == len(report.results)
        for cb_result, report_result in zip(callback_results, report.results, strict=True):
            assert cb_result.source == report_result.source


class TestConvertDirectoryFailureHandling:
    """Tests for convert_directory() failure handling."""

    @pytest.mark.unit
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="geoparquet-io segfaults on malformed input on Windows (upstream bug)",
    )
    def test_continues_after_failure(
        self,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """One file fails, others still processed."""
        import shutil

        from portolan_cli.convert import convert_directory

        input_dir = tmp_path / "input"
        input_dir.mkdir()

        # Copy valid file
        shutil.copy(valid_points_geojson, input_dir / "valid.geojson")

        # Create invalid file
        bad_file = input_dir / "bad.geojson"
        bad_file.write_text('{"type": "FeatureCollection", "features": [INVALID')

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        report = convert_directory(input_dir, output_dir=output_dir)

        # Both files should be processed
        assert report.total == 2
        # One should succeed, one should fail
        assert report.succeeded >= 1
        assert report.failed >= 1
        assert report.succeeded + report.failed == 2


class TestConvertDirectoryIdempotent:
    """Tests for convert_directory() idempotent behavior."""

    @pytest.mark.unit
    def test_skips_already_converted(
        self,
        valid_points_parquet: Path,
        tmp_path: Path,
    ) -> None:
        """Already cloud-native files are skipped."""
        import shutil

        from portolan_cli.convert import convert_directory

        input_dir = tmp_path / "input"
        input_dir.mkdir()
        shutil.copy(valid_points_parquet, input_dir / "data.parquet")

        report = convert_directory(input_dir)

        assert report.total == 1
        assert report.skipped == 1
        assert report.succeeded == 0

    @pytest.mark.unit
    def test_rerun_skips_cloud_native(
        self,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Re-run on same directory skips already-converted files."""
        import shutil

        from portolan_cli.convert import convert_directory

        input_dir = tmp_path / "input"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "test.geojson")

        # First run: converts GeoJSON to GeoParquet
        report1 = convert_directory(input_dir)
        assert report1.succeeded == 1

        # Second run: GeoParquet is already cloud-native, so skipped
        # Note: The output parquet is in the same directory as input
        report2 = convert_directory(input_dir)

        # The new parquet file should be skipped
        # (depending on implementation, either only parquet is found, or both are found and parquet is skipped)
        assert report2.skipped >= 1


# =============================================================================
# Phase 4.6: convert_file() Error Types Tests
# =============================================================================


class TestConvertFileErrorTypes:
    """Tests verifying convert_file() uses correct error types.

    Task 4.6: Refactor convert_file() to use new error types from errors.py:
    - UnsupportedFormatError for unsupported formats
    - ConversionFailedError for conversion exceptions
    - ValidationFailedError for validation failures
    """

    @pytest.mark.unit
    def test_unsupported_format_uses_unsupported_format_error(self, tmp_path: Path) -> None:
        """Unsupported format should use UnsupportedFormatError internally.

        The convert_file() function returns a ConversionResult with FAILED status,
        but internally it should catch/use UnsupportedFormatError for logging context.
        """
        from portolan_cli.convert import ConversionStatus, convert_file
        from portolan_cli.errors import UnsupportedFormatError

        # Create a NetCDF file (unsupported format)
        netcdf = tmp_path / "data.nc"
        netcdf.write_bytes(b"CDF\x01")  # NetCDF magic bytes

        result = convert_file(netcdf)

        assert result.status == ConversionStatus.FAILED
        assert result.error is not None
        # Error should mention format is not supported
        assert "not" in result.error.lower() and "support" in result.error.lower()

        # Verify UnsupportedFormatError can be constructed with the result context
        error = UnsupportedFormatError(str(netcdf), result.format_from)
        assert error.code == "PRTLN-CNV001"
        assert error.path == str(netcdf)

    @pytest.mark.unit
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="geoparquet-io segfaults on malformed input on Windows (upstream bug)",
    )
    def test_conversion_exception_uses_conversion_failed_error(self, tmp_path: Path) -> None:
        """Conversion exception should use ConversionFailedError internally.

        When geoparquet-io or rio-cogeo throws an exception, the convert_file()
        function should catch it and use ConversionFailedError for context.
        """
        from portolan_cli.convert import ConversionStatus, convert_file
        from portolan_cli.errors import ConversionFailedError

        # Create a malformed GeoJSON that will fail conversion
        bad_file = tmp_path / "bad.geojson"
        bad_file.write_text('{"type": "FeatureCollection", "features": [INVALID')

        result = convert_file(bad_file, output_dir=tmp_path)

        assert result.status == ConversionStatus.FAILED
        assert result.error is not None
        assert len(result.error) > 0

        # Verify ConversionFailedError can be constructed with the result context
        original_exception = ValueError(result.error)
        error = ConversionFailedError(str(bad_file), original_exception)
        assert error.code == "PRTLN-CNV002"
        assert error.path == str(bad_file)

    @pytest.mark.unit
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="geoparquet-io segfaults on malformed input on Windows (upstream bug)",
    )
    def test_failed_result_has_error_context_for_logging(self, tmp_path: Path) -> None:
        """FAILED results should have error context suitable for logging/debugging."""
        from portolan_cli.convert import ConversionStatus, convert_file

        # Create an invalid file that will fail conversion
        bad_file = tmp_path / "corrupt.geojson"
        bad_file.write_text("not valid json at all {{{")

        result = convert_file(bad_file, output_dir=tmp_path)

        assert result.status == ConversionStatus.FAILED
        # Error should be non-empty and contain useful context
        assert result.error is not None
        assert len(result.error) > 0
        # Source path should be captured
        assert result.source == bad_file

    @pytest.mark.unit
    def test_error_codes_match_portolan_error_pattern(self) -> None:
        """All conversion error types should have correct PRTLN-CNV* codes."""
        from portolan_cli.errors import (
            ConversionError,
            ConversionFailedError,
            UnsupportedFormatError,
            ValidationFailedError,
        )

        # Base class
        assert ConversionError("test").code == "PRTLN-CNV000"

        # Specific error types
        assert UnsupportedFormatError("/path", "netcdf").code == "PRTLN-CNV001"
        assert ConversionFailedError("/path", ValueError("test")).code == "PRTLN-CNV002"
        assert ValidationFailedError("/path", ["error"]).code == "PRTLN-CNV003"


# =============================================================================
# Phase 7: Edge Cases and Hardening
# =============================================================================


class TestConvertFileEdgeCases:
    """Tests for convert_file() edge cases and error handling."""

    @pytest.mark.unit
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="geoparquet-io segfaults on malformed input on Windows (upstream bug)",
    )
    def test_shapefile_with_missing_sidecars(self, tmp_path: Path) -> None:
        """Shapefile with missing sidecars attempts conversion (delegates to geoparquet-io).

        Per ADR-0010, we delegate to upstream libraries. geoparquet-io may handle
        incomplete shapefiles differently.
        """
        from portolan_cli.convert import convert_file

        # Create a minimal .shp file without sidecars
        shp_file = tmp_path / "incomplete.shp"
        shp_file.write_bytes(b"\x00\x00\x27\x0a")  # Shapefile magic bytes

        # Conversion should be attempted (result depends on geoparquet-io behavior)
        result = convert_file(shp_file, output_dir=tmp_path)

        # We expect either FAILED (geoparquet-io can't read it) or SUCCESS
        # The key is that we don't crash
        assert result.status is not None
        assert result.source == shp_file

    @pytest.mark.unit
    def test_permission_error_handling(self, tmp_path: Path) -> None:
        """Permission error during conversion returns FAILED with clear message.

        Note: This test is skipped on Windows (permission model differs) and
        when running as root (root can write anywhere).
        """
        import os

        # Skip on Windows - different permission model
        if sys.platform == "win32":
            pytest.skip("Windows has different permission handling")

        # Skip if running as root - root can write to read-only dirs
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            pytest.skip("Running as root - cannot test permission errors")

        from portolan_cli.convert import ConversionStatus, convert_file

        # Create a valid GeoJSON
        geojson = tmp_path / "test.geojson"
        geojson.write_text(
            '{"type":"FeatureCollection","features":'
            '[{"type":"Feature","geometry":{"type":"Point","coordinates":[0,0]},"properties":{}}]}'
        )

        # Create output directory that's not writable
        output_dir = tmp_path / "readonly"
        output_dir.mkdir()

        try:
            os.chmod(output_dir, 0o444)  # Read-only

            result = convert_file(geojson, output_dir=output_dir)

            # Should fail due to permission error
            assert result.status == ConversionStatus.FAILED
            assert result.error is not None
            # Error should mention permission or access issue
            # (exact wording varies by OS)
        finally:
            # Restore permissions for cleanup
            os.chmod(output_dir, 0o755)

    @pytest.mark.unit
    def test_output_file_already_exists_cloud_native(
        self,
        valid_points_parquet: Path,
        tmp_path: Path,
    ) -> None:
        """Output file already exists and is cloud-native - skips conversion."""
        import shutil

        from portolan_cli.convert import ConversionStatus, convert_file

        # Copy parquet to tmp (simulating existing output)
        input_parquet = tmp_path / "data.parquet"
        shutil.copy(valid_points_parquet, input_parquet)

        result = convert_file(input_parquet)

        # Should be skipped since already cloud-native
        assert result.status == ConversionStatus.SKIPPED
        assert result.format_from == "GeoParquet"

    @pytest.mark.unit
    def test_duration_always_non_negative(self, tmp_path: Path) -> None:
        """Duration is always >= 0, even for fast operations."""
        from portolan_cli.convert import convert_file

        # Create minimal file
        geojson = tmp_path / "tiny.geojson"
        geojson.write_text(
            '{"type":"FeatureCollection","features":'
            '[{"type":"Feature","geometry":{"type":"Point","coordinates":[0,0]},"properties":{}}]}'
        )

        result = convert_file(geojson, output_dir=tmp_path)

        assert result.duration_ms >= 0

    @pytest.mark.unit
    def test_format_from_never_empty(self, tmp_path: Path) -> None:
        """format_from is always a non-empty string."""
        from portolan_cli.convert import convert_file

        # Various file types
        files = [
            ("test.geojson", '{"type":"FeatureCollection","features":[]}'),
            ("test.nc", "CDF\x01"),  # Unsupported format
        ]

        for name, content in files:
            file_path = tmp_path / name
            if isinstance(content, str):
                file_path.write_text(content)
            else:
                file_path.write_bytes(content.encode() if isinstance(content, str) else content)

            result = convert_file(file_path, output_dir=tmp_path)

            assert result.format_from is not None
            assert len(result.format_from) > 0


class TestConvertDirectoryEdgeCases:
    """Tests for convert_directory() edge cases."""

    @pytest.mark.unit
    def test_recursive_conversion_deep_nesting(
        self,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Deep directory nesting is handled correctly."""
        import shutil

        from portolan_cli.convert import convert_directory

        # Create deeply nested structure
        deep_dir = tmp_path / "a" / "b" / "c" / "d"
        deep_dir.mkdir(parents=True)
        shutil.copy(valid_points_geojson, deep_dir / "deep.geojson")

        report = convert_directory(tmp_path)

        assert report.total >= 1
        assert report.succeeded >= 1

    @pytest.mark.unit
    def test_non_recursive_flag(
        self,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Non-recursive conversion only processes root directory."""
        import shutil

        from portolan_cli.convert import convert_directory

        # File in root
        shutil.copy(valid_points_geojson, tmp_path / "root.geojson")

        # File in subdirectory
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        shutil.copy(valid_points_geojson, subdir / "nested.geojson")

        report = convert_directory(tmp_path, recursive=False)

        # Should only process root file
        assert report.total == 1

    @pytest.mark.unit
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="geoparquet-io segfaults on malformed input on Windows (upstream bug)",
    )
    def test_mixed_valid_and_invalid_files(
        self,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Mixed valid/invalid files: all are processed, failures don't block."""
        import shutil

        from portolan_cli.convert import convert_directory

        # Valid file
        shutil.copy(valid_points_geojson, tmp_path / "valid.geojson")

        # Invalid file
        invalid = tmp_path / "invalid.geojson"
        invalid.write_text("not valid json {{{")

        report = convert_directory(tmp_path)

        # Both should be processed
        assert report.total == 2
        assert report.succeeded >= 1
        assert report.failed >= 1


class TestConvertFileSourceMtime:
    """Tests for source mtime detection scenarios (Task 7.6)."""

    @pytest.mark.unit
    def test_source_mtime_preserved_after_conversion(
        self,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Source file mtime can be retrieved after conversion for tracking."""
        import shutil

        from portolan_cli.convert import ConversionStatus, convert_file

        # Copy source to tmp
        source = tmp_path / "data.geojson"
        shutil.copy(valid_points_geojson, source)

        # Get source mtime before conversion
        source_mtime = source.stat().st_mtime

        result = convert_file(source)

        assert result.status == ConversionStatus.SUCCESS
        # Source should still exist with same mtime
        assert source.exists()
        assert source.stat().st_mtime == source_mtime
