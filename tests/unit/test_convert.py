"""Unit tests for convert module.

Tests ConversionStatus enum, ConversionResult dataclass, convert_file(),
ConversionReport, and convert_directory() functions.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest
from typing_extensions import Self

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
    def test_unsupported_value(self) -> None:
        """UNSUPPORTED status value is 'unsupported'."""
        from portolan_cli.convert import ConversionStatus

        assert ConversionStatus.UNSUPPORTED.value == "unsupported"

    @pytest.mark.unit
    def test_exactly_five_values(self) -> None:
        """There are exactly 5 status values."""
        from portolan_cli.convert import ConversionStatus

        assert len(ConversionStatus) == 5

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


class TestConvertFileKeepsAForeignOutput:
    """``check --fix`` never converts over a file that its source did not make."""

    @pytest.mark.unit
    def test_a_foreign_parquet_of_the_stem_stays(
        self, valid_points_geojson: Path, tmp_path: Path
    ) -> None:
        import shutil

        from portolan_cli.convert import ConversionStatus, convert_file

        source = tmp_path / "points.geojson"
        shutil.copy(valid_points_geojson, source)
        foreign = tmp_path / "points.parquet"
        foreign.write_bytes(b"operator data")

        result = convert_file(source)

        assert foreign.read_bytes() == b"operator data"
        assert result.status == ConversionStatus.SUCCESS
        assert result.output == tmp_path / "points-geojson.parquet"


class TestClaimOutput:
    """``claim_output`` picks a name that writes over no file of another source."""

    @staticmethod
    def _stamped(path: Path, source_name: str) -> None:
        import json

        import pyarrow as pa
        import pyarrow.parquet as pq

        from portolan_cli.convert import PROVENANCE_KEY

        table = pa.table({"a": [1]}).replace_schema_metadata(
            {PROVENANCE_KEY: json.dumps({"path": source_name, "sha256": "x"})}
        )
        pq.write_table(table, path)

    @pytest.mark.unit
    def test_a_free_name_is_kept(self, tmp_path: Path) -> None:
        from portolan_cli.convert import claim_output

        (tmp_path / "a.csv").write_text("x\n")
        assert claim_output(tmp_path / "a.csv", tmp_path / "a.parquet") == tmp_path / "a.parquet"

    @pytest.mark.unit
    def test_an_output_of_the_same_source_is_replaced(self, tmp_path: Path) -> None:
        from portolan_cli.convert import claim_output

        (tmp_path / "a.csv").write_text("x\n")
        self._stamped(tmp_path / "a.parquet", "a.csv")
        assert claim_output(tmp_path / "a.csv", tmp_path / "a.parquet") == tmp_path / "a.parquet"

    @pytest.mark.unit
    def test_an_output_of_another_source_moves_the_output(self, tmp_path: Path) -> None:
        from portolan_cli.convert import claim_output

        (tmp_path / "a.csv").write_text("x\n")
        self._stamped(tmp_path / "a.parquet", "a.geojson")
        assert (
            claim_output(tmp_path / "a.csv", tmp_path / "a.parquet") == tmp_path / "a-csv.parquet"
        )

    @pytest.mark.unit
    def test_a_byte_copy_of_the_source_is_the_source_output(self, tmp_path: Path) -> None:
        from portolan_cli.convert import claim_output

        (tmp_path / "a.tiff").write_bytes(b"cog bytes")
        (tmp_path / "a.tif").write_bytes(b"cog bytes")
        assert claim_output(tmp_path / "a.tiff", tmp_path / "a.tif") == tmp_path / "a.tif"

    @pytest.mark.unit
    def test_no_free_name_raises(self, tmp_path: Path) -> None:
        from portolan_cli.convert import claim_output
        from portolan_cli.errors import OutputExistsError

        (tmp_path / "a.csv").write_text("x\n")
        (tmp_path / "a.parquet").write_bytes(b"operator data")
        (tmp_path / "a-csv.parquet").write_bytes(b"more operator data")

        with pytest.raises(OutputExistsError, match="would replace a.parquet"):
            claim_output(tmp_path / "a.csv", tmp_path / "a.parquet")


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
    def test_unsupported_format_returns_unsupported(self, tmp_path: Path) -> None:
        """Unsupported format returns UNSUPPORTED status (not FAILED)."""
        from portolan_cli.convert import ConversionStatus, convert_file

        # Create a NetCDF file (unsupported)
        netcdf = tmp_path / "data.nc"
        netcdf.write_bytes(b"CDF\x01")  # NetCDF magic bytes

        result = convert_file(netcdf)

        # UNSUPPORTED is distinct from FAILED - not an error, just not convertible
        assert result.status == ConversionStatus.UNSUPPORTED
        # No error message for unsupported (it's not an error condition)
        assert result.error is None


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
        assert report.unsupported == 0
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
            ConversionResult(
                source=Path("/a/unsupported.nc"),
                output=None,
                format_from="NetCDF",
                format_to=None,
                status=ConversionStatus.UNSUPPORTED,
                error=None,
                duration_ms=1,
            ),
        ]

        report = ConversionReport(results=results)

        assert report.succeeded == 2
        assert report.failed == 1
        assert report.skipped == 1
        assert report.invalid == 1
        assert report.unsupported == 1
        assert report.total == 6

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
        assert d["summary"]["unsupported"] == 0
        assert d["summary"]["total"] == 2

        assert "results" in d
        assert len(d["results"]) == 2
        assert d["results"][0]["status"] == "success"
        assert d["results"][1]["status"] == "skipped"

    @pytest.mark.unit
    def test_counts_invariant(self) -> None:
        """succeeded + failed + skipped + invalid + unsupported == total."""
        from portolan_cli.convert import (
            ConversionReport,
            ConversionResult,
            ConversionStatus,
        )

        # Create a report with various statuses
        results = [
            ConversionResult(
                source=Path(f"/a/{i}.shp"),
                output=Path(f"/a/{i}.parquet")
                if status not in (ConversionStatus.FAILED, ConversionStatus.UNSUPPORTED)
                else None,
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
                    ConversionStatus.UNSUPPORTED,
                ]
            )
        ]

        report = ConversionReport(results=results)

        # Invariant: all counts sum to total
        assert (
            report.succeeded + report.failed + report.skipped + report.invalid + report.unsupported
            == report.total
        )


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


class TestConvertDirectoryPoolFallback:
    """Parallel conversion falls back to serial when the process pool can't start.

    A ``ProcessPoolExecutor`` can fail to spawn workers in restricted
    environments: locked-down containers with no ``/dev/shm``, seccomp filters
    that block ``clone``, or a nested multiprocessing context such as mutmut's
    mutation runner (which re-sets the start method during worker bootstrap and
    surfaces as ``BrokenProcessPool`` in the parent). Without a fallback every
    file is reported FAILED even though the inputs are valid. Conversion must
    still complete by dropping to the serial path. Regression guard for the
    mutation-testing baseline (issue #612).
    """

    @pytest.mark.unit
    def test_falls_back_to_serial_when_pool_broken(
        self,
        monkeypatch: pytest.MonkeyPatch,
        valid_points_geojson: Path,
        valid_polygons_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """A pool that can't start still yields both converted files, none FAILED."""
        import concurrent.futures
        import shutil
        from concurrent.futures.process import BrokenProcessPool

        from portolan_cli.convert import convert_directory

        input_dir = tmp_path / "input"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "points.geojson")
        shutil.copy(valid_polygons_geojson, input_dir / "polygons.geojson")

        class _BrokenPool:
            """Stand-in ProcessPoolExecutor whose workers never start."""

            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def __enter__(self) -> Self:
                return self

            def __exit__(self, *args: object) -> bool:
                return False

            def submit(self, *args: object, **kwargs: object) -> object:
                raise BrokenProcessPool("simulated worker startup failure")

        monkeypatch.setattr(concurrent.futures, "ProcessPoolExecutor", _BrokenPool)

        # workers=4 + 2 files selects the parallel path; the broken pool must not
        # sink the batch.
        report = convert_directory(input_dir, workers=4)

        assert report.succeeded == 2
        assert report.failed == 0
        assert (input_dir / "points.parquet").exists()
        assert (input_dir / "polygons.parquet").exists()

    @pytest.mark.unit
    def test_pool_dies_midrun_keeps_completed_and_fires_progress_once(
        self,
        monkeypatch: pytest.MonkeyPatch,
        valid_points_geojson: Path,
        valid_polygons_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """A mid-run pool death converts only the remainder; no file is dropped or doubled."""
        import concurrent.futures
        import shutil
        from concurrent.futures.process import BrokenProcessPool

        from portolan_cli.convert import ConversionResult, ConversionStatus, convert_directory

        input_dir = tmp_path / "input"
        input_dir.mkdir()
        # Sorted order: a_points.geojson before b_polygons.geojson.
        shutil.copy(valid_points_geojson, input_dir / "a_points.geojson")
        shutil.copy(valid_polygons_geojson, input_dir / "b_polygons.geojson")

        class _Future:
            def __init__(self, source: Path, ok: bool) -> None:
                self._source = source
                self._ok = ok

            def result(self) -> ConversionResult:
                if not self._ok:
                    raise BrokenProcessPool("pool died after first file")
                # A sentinel status the serial path never produces, so we can
                # prove this result came from the pool and was NOT re-converted.
                return ConversionResult(
                    source=self._source,
                    output=self._source.with_suffix(".parquet"),
                    format_from="GEOJSON",
                    format_to="GEOPARQUET",
                    status=ConversionStatus.SKIPPED,
                    error=None,
                    duration_ms=0,
                )

        class _PartialPool:
            """First submitted file succeeds; the pool then dies."""

            def __init__(self, *args: object, **kwargs: object) -> None:
                self._n = 0

            def __enter__(self) -> Self:
                return self

            def __exit__(self, *args: object) -> bool:
                return False

            def submit(self, _fn: object, source: Path, **_kw: object) -> _Future:
                self._n += 1
                return _Future(source, ok=self._n == 1)

        monkeypatch.setattr(concurrent.futures, "ProcessPoolExecutor", _PartialPool)
        # as_completed must yield the futures; identity keeps submission order.
        monkeypatch.setattr(concurrent.futures, "as_completed", lambda fs: list(fs))

        progressed: list[Path] = []
        report = convert_directory(
            input_dir, workers=4, on_progress=lambda r: progressed.append(r.source)
        )

        by_name = {r.source.name: r for r in report.results}
        # First file kept its pool result (SKIPPED), never re-run serially.
        assert by_name["a_points.geojson"].status == ConversionStatus.SKIPPED
        # Second file was converted by the serial fallback.
        assert by_name["b_polygons.geojson"].status == ConversionStatus.SUCCESS
        assert (input_dir / "b_polygons.parquet").exists()
        # Every file reported exactly once — no drops, no double-fires.
        assert sorted(p.name for p in progressed) == [
            "a_points.geojson",
            "b_polygons.geojson",
        ]


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
    def test_unsupported_format_returns_unsupported_status(self, tmp_path: Path) -> None:
        """Unsupported format returns UNSUPPORTED status (not FAILED).

        The convert_file() function returns UNSUPPORTED for formats that aren't
        convertible (like NetCDF). This is distinct from FAILED which indicates
        a conversion error.
        """
        from portolan_cli.convert import ConversionStatus, convert_file
        from portolan_cli.errors import UnsupportedFormatError

        # Create a NetCDF file (unsupported format)
        netcdf = tmp_path / "data.nc"
        netcdf.write_bytes(b"CDF\x01")  # NetCDF magic bytes

        result = convert_file(netcdf)

        # UNSUPPORTED is a distinct status - not an error condition
        assert result.status == ConversionStatus.UNSUPPORTED
        assert result.error is None  # No error for unsupported (it's expected)

        # Verify UnsupportedFormatError can still be constructed for logging if needed
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

        We delegate to upstream libraries. geoparquet-io may handle
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
            Path(output_dir).chmod(0o444)  # Read-only

            result = convert_file(geojson, output_dir=output_dir)

            # Should fail due to permission error
            assert result.status == ConversionStatus.FAILED
            assert result.error is not None
            # Error should mention permission or access issue
            # (exact wording varies by OS)
        finally:
            # Restore permissions for cleanup
            Path(output_dir).chmod(0o755)

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


# =============================================================================
# SQL Injection Prevention Tests (Security)
# =============================================================================


# NOTE: TestValidateLayerName and TestBuildStReadExpr removed.
# Those functions were part of the DuckDB workaround for multi-layer support.
# Now using geoparquet-io's native layer parameter instead.
# See: https://github.com/geoparquet/geoparquet-io/issues/315


# =============================================================================
# COG Thumbnail Generation Tests (Issue #372)
# =============================================================================


class TestGenerateCogThumbnail:
    """Tests for generate_cog_thumbnail() function."""

    @pytest.mark.unit
    def test_generates_jpeg_next_to_cog(
        self,
        valid_rgb_cog: Path,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
        recwarn: pytest.WarningsRecorder,
    ) -> None:
        """A thumbnail JPEG is created next to the COG file, with no GDAL or rasterio warning.

        The JPEG driver rejects a PHOTOMETRIC creation option, and GDAL logs
        that through ``rasterio._env`` on every render. A thumbnail carries no
        georeference on purpose, so ``NotGeoreferencedWarning`` is noise too.
        """
        import logging
        import shutil

        from portolan_cli.convert import generate_cog_thumbnail

        cog = tmp_path / "data.tif"
        shutil.copy(valid_rgb_cog, cog)

        with caplog.at_level(logging.WARNING, logger="rasterio"):
            thumb = generate_cog_thumbnail(cog)

        assert thumb is not None
        assert thumb.exists()
        assert thumb.suffix == ".jpg"
        assert thumb.parent == cog.parent
        assert thumb.stat().st_size > 0
        assert [r.getMessage() for r in caplog.records if r.name.startswith("rasterio")] == []
        assert [str(w.message) for w in recwarn] == []

    @pytest.mark.unit
    def test_a_single_band_cog_gives_a_single_band_thumbnail(
        self,
        valid_singleband_cog: Path,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
        recwarn: pytest.WarningsRecorder,
    ) -> None:
        """A 1-band COG renders a grayscale JPEG, with no GDAL or rasterio warning."""
        import logging
        import shutil

        import rasterio

        from portolan_cli.convert import generate_cog_thumbnail

        cog = tmp_path / "gray.tif"
        shutil.copy(valid_singleband_cog, cog)

        with caplog.at_level(logging.WARNING, logger="rasterio"):
            thumb = generate_cog_thumbnail(cog)

        assert [r.getMessage() for r in caplog.records if r.name.startswith("rasterio")] == []
        assert [str(w.message) for w in recwarn] == []
        assert thumb is not None
        with rasterio.open(thumb) as src:
            assert (src.driver, src.count) == ("JPEG", 1)

    @pytest.mark.unit
    def test_thumbnail_is_smaller_than_max_size(self, valid_rgb_cog: Path, tmp_path: Path) -> None:
        """Generated thumbnail respects max pixel size."""
        import shutil

        import rasterio

        from portolan_cli.convert import generate_cog_thumbnail

        cog = tmp_path / "data.tif"
        shutil.copy(valid_rgb_cog, cog)

        thumb = generate_cog_thumbnail(cog, max_size=128)
        assert thumb is not None

        with rasterio.open(thumb) as src:
            assert max(src.width, src.height) <= 128

    @pytest.mark.unit
    def test_returns_none_on_invalid_raster(self, tmp_path: Path) -> None:
        """Returns None if the source is not a readable raster."""
        from portolan_cli.convert import generate_cog_thumbnail

        bad = tmp_path / "not_a_raster.tif"
        bad.write_bytes(b"not a tiff")

        result = generate_cog_thumbnail(bad)
        assert result is None

    @pytest.mark.unit
    def test_convert_to_cog_emits_thumbnail_when_enabled(
        self, non_cog_tif: Path, tmp_path: Path
    ) -> None:
        """convert_file generates a thumbnail when generate_thumbnail is True."""
        from portolan_cli.conversion_config import CogSettings
        from portolan_cli.convert import ConversionStatus, convert_file

        settings = CogSettings(generate_thumbnail=True)
        result = convert_file(non_cog_tif, output_dir=tmp_path, cog_settings=settings)

        assert result.status == ConversionStatus.SUCCESS
        assert result.output is not None
        thumb = result.output.with_name(f"{result.output.stem}.thumb.jpg")
        assert thumb.exists()

    @pytest.mark.unit
    def test_convert_keeps_a_hand_made_thumbnail(self, non_cog_tif: Path, tmp_path: Path) -> None:
        """A new COG keeps the ``.thumb.jpg`` that is already next to it."""
        from portolan_cli.plugins.formats import handler_named

        handler = handler_named("cog")
        assert handler is not None
        thumb = tmp_path / f"{non_cog_tif.stem}.thumb.jpg"
        thumb.write_bytes(b"hand-made")

        output = handler.convert(non_cog_tif, tmp_path, None)

        assert output.exists()
        assert thumb.read_bytes() == b"hand-made"

    @pytest.mark.unit
    def test_convert_to_cog_skips_thumbnail_when_disabled(
        self, non_cog_tif: Path, tmp_path: Path
    ) -> None:
        """convert_file does not emit a thumbnail when generate_thumbnail is False."""
        from portolan_cli.conversion_config import CogSettings
        from portolan_cli.convert import ConversionStatus, convert_file

        settings = CogSettings(generate_thumbnail=False)
        result = convert_file(non_cog_tif, output_dir=tmp_path, cog_settings=settings)

        assert result.status == ConversionStatus.SUCCESS
        assert result.output is not None
        thumb = result.output.with_name(f"{result.output.stem}.thumb.jpg")
        assert not thumb.exists()

    @pytest.mark.unit
    def test_thumbnail_for_float32_singleband(
        self, valid_float32_cog: Path, tmp_path: Path
    ) -> None:
        """Float32 (elevation-like) singleband rasters get a stretched thumbnail."""
        import shutil

        from portolan_cli.convert import generate_cog_thumbnail

        cog = tmp_path / "elevation.tif"
        shutil.copy(valid_float32_cog, cog)

        thumb = generate_cog_thumbnail(cog)
        assert thumb is not None
        assert thumb.exists()
        assert thumb.stat().st_size > 0

    @pytest.mark.unit
    def test_thumbnail_does_not_clobber_user_jpg(self, valid_rgb_cog: Path, tmp_path: Path) -> None:
        """A user-supplied data.jpg next to data.tif must not be overwritten."""
        import shutil

        from portolan_cli.convert import generate_cog_thumbnail

        cog = tmp_path / "data.tif"
        shutil.copy(valid_rgb_cog, cog)

        # Pre-existing user thumbnail
        user_jpg = tmp_path / "data.jpg"
        user_payload = b"USER_THUMBNAIL_DO_NOT_OVERWRITE"
        user_jpg.write_bytes(user_payload)

        thumb = generate_cog_thumbnail(cog)
        assert thumb is not None
        assert thumb.name == "data.thumb.jpg"
        assert user_jpg.read_bytes() == user_payload

    @pytest.mark.unit
    def test_thumbnail_excludes_nodata_from_stretch(
        self, valid_nodata_cog: Path, tmp_path: Path
    ) -> None:
        """Nodata sentinels are masked out so they don't dominate the stretch."""
        import shutil

        from portolan_cli.convert import generate_cog_thumbnail

        cog = tmp_path / "with_nodata.tif"
        shutil.copy(valid_nodata_cog, cog)

        thumb = generate_cog_thumbnail(cog)
        assert thumb is not None
        assert thumb.exists()
        assert thumb.stat().st_size > 0

    @pytest.mark.unit
    def test_thumbnail_returns_none_for_all_nodata_raster(self, tmp_path: Path) -> None:
        """Returns None if raster contains only nodata (no valid pixels)."""
        import numpy as np
        import rasterio
        from rasterio.transform import from_bounds

        from portolan_cli.convert import generate_cog_thumbnail

        # Create a raster where ALL pixels are nodata
        cog = tmp_path / "all_nodata.tif"
        data = np.full((1, 64, 64), -9999, dtype=np.float32)
        transform = from_bounds(0, 0, 1, 1, 64, 64)

        profile = {
            "driver": "GTiff",
            "dtype": "float32",
            "width": 64,
            "height": 64,
            "count": 1,
            "crs": "EPSG:4326",
            "transform": transform,
            "nodata": -9999,
        }
        with rasterio.open(cog, "w", **profile) as dst:
            dst.write(data)

        result = generate_cog_thumbnail(cog)
        assert result is None

    @pytest.mark.unit
    def test_thumbnail_handles_rgba_4band_raster(self, tmp_path: Path) -> None:
        """4-band RGBA rasters produce valid 3-band RGB thumbnails."""
        import numpy as np
        import rasterio
        from rasterio.transform import from_bounds

        from portolan_cli.convert import generate_cog_thumbnail

        # Create a 4-band RGBA raster
        cog = tmp_path / "rgba.tif"
        data = np.random.randint(0, 255, (4, 128, 128), dtype=np.uint8)
        transform = from_bounds(0, 0, 1, 1, 128, 128)

        profile = {
            "driver": "GTiff",
            "dtype": "uint8",
            "width": 128,
            "height": 128,
            "count": 4,
            "crs": "EPSG:4326",
            "transform": transform,
            "photometric": "RGBA",
        }
        with rasterio.open(cog, "w", **profile) as dst:
            dst.write(data)

        thumb = generate_cog_thumbnail(cog)
        assert thumb is not None
        assert thumb.exists()

        # Verify thumbnail is 3-band (RGB, not RGBA)
        with rasterio.open(thumb) as src:
            assert src.count == 3

    @pytest.mark.unit
    def test_thumbnail_respects_quality_setting(self, valid_rgb_cog: Path, tmp_path: Path) -> None:
        """Lower quality produces smaller file size."""
        import shutil

        from portolan_cli.convert import generate_cog_thumbnail

        cog = tmp_path / "data.tif"
        shutil.copy(valid_rgb_cog, cog)

        # Generate with high quality
        thumb_high = generate_cog_thumbnail(cog, quality=95)
        assert thumb_high is not None
        size_high = thumb_high.stat().st_size

        # Remove and regenerate with low quality
        thumb_high.unlink()
        thumb_low = generate_cog_thumbnail(cog, quality=10)
        assert thumb_low is not None
        size_low = thumb_low.stat().st_size

        # Low quality should be smaller (unless image is trivially small)
        # Allow some tolerance for very small test images
        assert size_low <= size_high

    @pytest.mark.unit
    def test_convert_file_uses_thumbnail_quality_from_settings(
        self, non_cog_tif: Path, tmp_path: Path
    ) -> None:
        """convert_file passes thumbnail_quality from CogSettings."""
        from portolan_cli.conversion_config import CogSettings
        from portolan_cli.convert import ConversionStatus, convert_file

        settings = CogSettings(generate_thumbnail=True, thumbnail_quality=50)
        result = convert_file(non_cog_tif, output_dir=tmp_path, cog_settings=settings)

        assert result.status == ConversionStatus.SUCCESS
        assert result.output is not None
        thumb = result.output.with_name(f"{result.output.stem}.thumb.jpg")
        assert thumb.exists()


# =============================================================================
# VectorSettings Integration Tests (Issue #340)
# =============================================================================


class TestVectorSettingsIntegration:
    """Integration tests for VectorSettings in convert module."""

    @pytest.fixture
    def sample_geojson(self, tmp_path: Path) -> Path:
        """Create a simple GeoJSON file for testing."""
        geojson = tmp_path / "test.geojson"
        geojson.write_text(
            """{
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "test"},
                    "geometry": {"type": "Point", "coordinates": [-73.9857, 40.7484]}
                }
            ]
        }"""
        )
        return geojson

    @pytest.mark.unit
    def test_add_spatial_index_raises_for_unknown_type(self) -> None:
        """_add_spatial_index raises ValueError for unknown index types."""
        from unittest.mock import MagicMock

        from portolan_cli.convert import _add_spatial_index

        mock_table = MagicMock()

        with pytest.raises(ValueError, match="Unknown spatial index type: 'invalid'"):
            _add_spatial_index(mock_table, "invalid", resolution=9)

    @pytest.mark.unit
    def test_add_spatial_index_h3_with_resolution(self) -> None:
        """_add_spatial_index calls add_h3 with resolution."""
        from unittest.mock import MagicMock

        from portolan_cli.convert import _add_spatial_index

        mock_table = MagicMock()
        mock_table.add_h3.return_value = mock_table

        result = _add_spatial_index(mock_table, "h3", resolution=6)

        mock_table.add_h3.assert_called_once_with(resolution=6)
        assert result == mock_table

    @pytest.mark.unit
    def test_add_spatial_index_h3_default_resolution(self) -> None:
        """_add_spatial_index calls add_h3 without resolution when None."""
        from unittest.mock import MagicMock

        from portolan_cli.convert import _add_spatial_index

        mock_table = MagicMock()
        mock_table.add_h3.return_value = mock_table

        result = _add_spatial_index(mock_table, "h3", resolution=None)

        mock_table.add_h3.assert_called_once_with()
        assert result == mock_table

    @pytest.mark.unit
    def test_convert_vector_with_h3_adds_column(self, sample_geojson: Path, tmp_path: Path) -> None:
        """Converting with H3 spatial index adds h3_cell column."""
        import pyarrow.parquet as pq

        from portolan_cli.conversion_config import VectorSettings
        from portolan_cli.preparation import convert_vector

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        settings = VectorSettings(spatial_index="h3", resolution=6)
        output = convert_vector(sample_geojson, output_dir, settings=settings)

        assert output.exists()
        table = pq.read_table(output)
        assert "h3_cell" in table.schema.names

    @pytest.mark.unit
    def test_convert_vector_with_bbox_adds_column(
        self, sample_geojson: Path, tmp_path: Path
    ) -> None:
        """Converting with add_bbox=True adds bbox column."""
        import pyarrow.parquet as pq

        from portolan_cli.conversion_config import VectorSettings
        from portolan_cli.preparation import convert_vector

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        settings = VectorSettings(add_bbox=True)
        output = convert_vector(sample_geojson, output_dir, settings=settings)

        assert output.exists()
        table = pq.read_table(output)
        assert "bbox" in table.schema.names

    @pytest.mark.unit
    def test_convert_vector_with_hilbert_sort(self, sample_geojson: Path, tmp_path: Path) -> None:
        """Converting with sort=hilbert succeeds."""
        from portolan_cli.conversion_config import VectorSettings
        from portolan_cli.preparation import convert_vector

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        settings = VectorSettings(sort="hilbert")
        output = convert_vector(sample_geojson, output_dir, settings=settings)

        assert output.exists()

    @pytest.mark.unit
    def test_convert_vector_with_quadkey_sort_uses_resolution(
        self, sample_geojson: Path, tmp_path: Path
    ) -> None:
        """Converting with sort=quadkey and explicit resolution uses that resolution."""
        import pyarrow.parquet as pq

        from portolan_cli.conversion_config import VectorSettings
        from portolan_cli.preparation import convert_vector

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        settings = VectorSettings(sort="quadkey", resolution=10)
        output = convert_vector(sample_geojson, output_dir, settings=settings)

        assert output.exists()
        # quadkey sort adds a quadkey column
        table = pq.read_table(output)
        assert "quadkey" in table.schema.names

    @pytest.mark.unit
    def test_convert_file_passes_vector_settings(
        self, sample_geojson: Path, tmp_path: Path
    ) -> None:
        """convert_file correctly passes VectorSettings to the vector converter."""
        import pyarrow.parquet as pq

        from portolan_cli.conversion_config import VectorSettings
        from portolan_cli.convert import ConversionStatus, convert_file

        settings = VectorSettings(spatial_index="h3", resolution=8)
        result = convert_file(sample_geojson, output_dir=tmp_path, vector_settings=settings)

        assert result.status == ConversionStatus.SUCCESS
        assert result.output is not None

        table = pq.read_table(result.output)
        assert "h3_cell" in table.schema.names

    @pytest.mark.unit
    def test_convert_file_builds_an_s2_index(self, sample_geojson: Path, tmp_path: Path) -> None:
        """Portolan still offers s2 and still writes an ``s2_cell`` column."""
        import pyarrow.parquet as pq

        from portolan_cli.conversion_config import VectorSettings
        from portolan_cli.convert import ConversionStatus, convert_file

        settings = VectorSettings(spatial_index="s2", resolution=10)
        result = convert_file(sample_geojson, output_dir=tmp_path, vector_settings=settings)

        assert result.status == ConversionStatus.SUCCESS
        assert result.output is not None

        table = pq.read_table(result.output)
        assert "s2_cell" in table.schema.names

    @pytest.mark.unit
    def testvalidate_geoparquet_handles_directory(self, tmp_path: Path) -> None:
        """validate_geoparquet handles partitioned directories correctly."""
        from portolan_cli.convert import validate_geoparquet

        # Empty directory should fail
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        error = validate_geoparquet(empty_dir)
        assert error is not None
        assert "no parquet files" in error

    @pytest.mark.unit
    def test_validate_partitioned_geoparquet_with_valid_files(
        self, sample_geojson: Path, tmp_path: Path
    ) -> None:
        """validate_geoparquet succeeds for valid partitioned directory."""
        from portolan_cli.conversion_config import VectorSettings
        from portolan_cli.convert import validate_geoparquet
        from portolan_cli.preparation import convert_vector

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # First convert to single file to get valid geoparquet
        settings = VectorSettings(spatial_index="h3", resolution=6)
        single_file = convert_vector(sample_geojson, output_dir, settings=settings)

        # Create mock partitioned directory structure with valid parquet file
        partition_dir = tmp_path / "partitioned"
        partition_dir.mkdir()
        (partition_dir / "h3_cell=123").mkdir()
        import shutil

        shutil.copy(single_file, partition_dir / "h3_cell=123" / "data.parquet")

        # Validation should succeed on directory with valid parquet files
        error = validate_geoparquet(partition_dir)
        assert error is None


# =============================================================================
# Transient DuckDB "Query interrupted" retry (nightly stress-test flake)
# =============================================================================


class TestVectorConversionRetriesTransientInterrupt:
    """`convert_file` retries a transient DuckDB "Query interrupted".

    geoparquet-io runs conversions through DuckDB, which non-deterministically
    raises ``InterruptException('Query interrupted')`` (~1/1000 files) during
    bulk ``add`` when a pending signal trips its per-query check. The interrupt
    clears once raised, so a bounded retry keeps one interrupted query from
    failing a whole 1000-file add. Any other error must still fail fast.
    """

    @pytest.mark.unit
    def test_detects_transient_by_exception_name(self) -> None:
        """Any exception class named ``InterruptException`` is transient."""
        from portolan_cli.convert import _is_transient_conversion_error

        class InterruptException(Exception):
            pass

        # Message deliberately lacks "query interrupted" — name alone must match.
        assert _is_transient_conversion_error(InterruptException("boom")) is True

    @pytest.mark.unit
    def test_detects_transient_by_message(self) -> None:
        """A "Query interrupted" message is transient regardless of class."""
        from portolan_cli.convert import _is_transient_conversion_error

        assert _is_transient_conversion_error(RuntimeError("FATAL: Query interrupted")) is True

    @pytest.mark.unit
    def test_rejects_non_transient_error(self) -> None:
        """Ordinary conversion errors are not treated as transient."""
        from portolan_cli.convert import _is_transient_conversion_error

        assert _is_transient_conversion_error(ValueError("No CRS found")) is False

    @pytest.mark.unit
    def test_retries_then_succeeds(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """A single transient interrupt is retried and the conversion succeeds."""
        import geoparquet_io as gpio

        from portolan_cli.preparation import convert_vector

        class InterruptException(Exception):
            pass

        class _FakeTable:
            def add_bbox(self) -> _FakeTable:
                return self

            def sort_hilbert(self) -> _FakeTable:
                return self

            def write(self, path: str) -> None:
                Path(path).write_text("parquet-bytes")

        calls = {"n": 0}

        def fake_convert(src: str) -> _FakeTable:
            calls["n"] += 1
            if calls["n"] == 1:
                raise InterruptException("Query interrupted")
            return _FakeTable()

        monkeypatch.setattr(gpio, "convert", fake_convert)

        out = convert_vector(tmp_path / "in.geojson", tmp_path)

        assert calls["n"] == 2, "should retry exactly once after the transient interrupt"
        assert out == tmp_path / "in.parquet"
        assert out.exists()

    @pytest.mark.unit
    def test_gives_up_after_max_attempts(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A persistent interrupt eventually propagates after the attempt cap."""
        import geoparquet_io as gpio

        from portolan_cli.convert import _CONVERT_MAX_ATTEMPTS

        class InterruptException(Exception):
            pass

        calls = {"n": 0}

        def fake_convert(src: str) -> None:
            calls["n"] += 1
            raise InterruptException("Query interrupted")

        monkeypatch.setattr(gpio, "convert", fake_convert)

        from portolan_cli.preparation import convert_vector

        with pytest.raises(InterruptException):
            convert_vector(tmp_path / "in.geojson", tmp_path)

        assert calls["n"] == _CONVERT_MAX_ATTEMPTS

    @pytest.mark.unit
    def test_does_not_retry_non_transient(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Non-transient errors fail on the first attempt (no wasted retries)."""
        import geoparquet_io as gpio

        from portolan_cli.preparation import convert_vector

        calls = {"n": 0}

        def fake_convert(src: str) -> None:
            calls["n"] += 1
            raise ValueError("No CRS found")

        monkeypatch.setattr(gpio, "convert", fake_convert)

        with pytest.raises(ValueError, match="No CRS found"):
            convert_vector(tmp_path / "in.geojson", tmp_path)

        assert calls["n"] == 1


class TestRepairShapefileEncodingSidecar:
    """Encoding sidecar repair before shapefile conversion (issue #783)."""

    def _dbf(self, path: Path, records: bytes) -> None:
        """Minimal DBF: 32-byte header whose bytes 8-9 point past itself."""
        import struct

        header = bytearray(32)
        header[0] = 0x03
        struct.pack_into("<H", header, 8, 32)
        path.write_bytes(bytes(header) + records)

    def _shp(self, tmp_path: Path, records: bytes, cpg: str | None = None) -> Path:
        shp = tmp_path / "data.shp"
        shp.write_bytes(b"\x00" * 100)
        self._dbf(shp.with_suffix(".dbf"), records)
        if cpg is not None:
            shp.with_suffix(".cpg").write_text(cpg)
        return shp

    @pytest.mark.unit
    def test_latin1_records_without_cpg_get_iso_sidecar(self, tmp_path: Path) -> None:
        from portolan_cli.convert import repair_shapefile_encoding_sidecar

        shp = self._shp(tmp_path, "Tentud\xeda embalse".encode("latin-1"))
        result = repair_shapefile_encoding_sidecar(shp)

        assert result is not None
        assert shp.with_suffix(".cpg").read_text() == "ISO-8859-1"

    @pytest.mark.unit
    def test_lying_cpg_over_utf8_records_is_corrected(self, tmp_path: Path) -> None:
        from portolan_cli.convert import repair_shapefile_encoding_sidecar

        shp = self._shp(tmp_path, "Valle del Árrago".encode(), cpg="1252")
        result = repair_shapefile_encoding_sidecar(shp)

        assert result is not None
        assert shp.with_suffix(".cpg").read_text() == "UTF-8"

    @pytest.mark.unit
    def test_correct_sidecars_left_alone(self, tmp_path: Path) -> None:
        from portolan_cli.convert import repair_shapefile_encoding_sidecar

        # UTF-8 records with UTF-8 sidecar
        shp = self._shp(tmp_path, "Café".encode(), cpg="UTF-8")
        assert repair_shapefile_encoding_sidecar(shp) is None
        # Latin-1 records already declared ISO-8859-1
        sub = tmp_path / "b"
        sub.mkdir()
        shp2 = self._shp(sub, "Espa\xf1a".encode("latin-1"), cpg="ISO-8859-1")
        assert repair_shapefile_encoding_sidecar(shp2) is None

    @pytest.mark.unit
    def test_ascii_records_without_cpg_untouched(self, tmp_path: Path) -> None:
        from portolan_cli.convert import repair_shapefile_encoding_sidecar

        shp = self._shp(tmp_path, b"plain ascii only")
        assert repair_shapefile_encoding_sidecar(shp) is None
        assert not shp.with_suffix(".cpg").exists()

    @pytest.mark.unit
    def test_missing_dbf_is_noop(self, tmp_path: Path) -> None:
        from portolan_cli.convert import repair_shapefile_encoding_sidecar

        shp = tmp_path / "solo.shp"
        shp.write_bytes(b"\x00" * 100)
        assert repair_shapefile_encoding_sidecar(shp) is None


class TestApplyVectorSettingsSkipsGeometrylessTables:
    """Spatial operations need a geometry column (issue #805).

    ``gpio.convert`` reads a CSV of records as a plain table with
    ``geometry_column`` set to None. Calling ``add_bbox()`` on it makes DuckDB
    raise ``Binder Error: Column "geometry" referenced that exists in the
    SELECT clause - but this column cannot be referenced before it is
    defined``, which failed the whole ``add``.
    """

    @pytest.mark.unit
    def test_a_table_with_no_geometry_is_returned_untouched(self) -> None:
        from portolan_cli.conversion_config import VectorSettings
        from portolan_cli.convert import apply_vector_settings

        class _PlainTable:
            geometry_column = None

            def add_bbox(self) -> _PlainTable:
                raise AssertionError("add_bbox must not run without a geometry column")

            def sort_hilbert(self) -> _PlainTable:
                raise AssertionError("sort_hilbert must not run without a geometry column")

        table = _PlainTable()

        assert apply_vector_settings(table, VectorSettings()) is table

    @pytest.mark.unit
    def test_a_spatial_table_still_gets_both_operations(self) -> None:
        from portolan_cli.conversion_config import VectorSettings
        from portolan_cli.convert import apply_vector_settings

        calls: list[str] = []

        class _SpatialTable:
            geometry_column = "geometry"

            def add_bbox(self) -> _SpatialTable:
                calls.append("add_bbox")
                return self

            def sort_hilbert(self) -> _SpatialTable:
                calls.append("sort_hilbert")
                return self

        apply_vector_settings(_SpatialTable(), VectorSettings())

        assert calls == ["add_bbox", "sort_hilbert"]

    @pytest.mark.unit
    def test_a_table_that_predates_the_attribute_is_treated_as_spatial(self) -> None:
        """An older geoparquet-io Table exposes no `geometry_column`."""
        from portolan_cli.conversion_config import VectorSettings
        from portolan_cli.convert import apply_vector_settings

        calls: list[str] = []

        class _OldTable:
            def add_bbox(self) -> _OldTable:
                calls.append("add_bbox")
                return self

            def sort_hilbert(self) -> _OldTable:
                calls.append("sort_hilbert")
                return self

        apply_vector_settings(_OldTable(), VectorSettings())

        assert calls == ["add_bbox", "sort_hilbert"]


class TestPartitionedOutputStaysHive:
    """`_write_partitioned` asks for the Hive layout at every call site.

    geoparquet-io 1.4.0 flipped the Python API default for ``hive`` from True to
    False (geoparquet-io#661). Portolan needs the directories: each one becomes
    a STAC Item, and ``partitioning.build_glob_pattern`` matches
    ``<scheme>_cell=*``. These tests read the keyword arguments rather than the
    written files, so they hold whatever geoparquet-io defaults to next.
    """

    @staticmethod
    def _spy(scheme: str) -> tuple[Any, dict[str, Any]]:
        """Return a fake Table that records the partition call."""
        seen: dict[str, Any] = {}

        class _Spy:
            def __getattr__(self, name: str) -> Any:
                if not name.startswith("partition_by_"):
                    raise AttributeError(name)

                def _record(*args: Any, **kwargs: Any) -> None:
                    seen["method"] = name
                    seen["args"] = args
                    seen.update(kwargs)

                return _record

        return _Spy(), seen

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("scheme", "keep_kwarg"),
        [
            ("h3", "keep_h3_column"),
            ("s2", "keep_s2_column"),
            ("quadkey", "keep_quadkey_column"),
            ("a5", "keep_a5_column"),
            ("kdtree", "keep_kdtree_column"),
        ],
    )
    def test_every_scheme_asks_for_hive_directories(
        self, scheme: str, keep_kwarg: str, tmp_path: Path
    ) -> None:
        """Each partition call passes `hive=True` and keeps the index column."""
        from portolan_cli.conversion_config import VectorSettings
        from portolan_cli.convert import _write_partitioned

        table, seen = self._spy(scheme)
        settings = VectorSettings(spatial_index=scheme, resolution="auto", partition=True)

        _write_partitioned(table, tmp_path / "out", settings)

        assert seen["method"] == f"partition_by_{scheme}"
        assert seen["hive"] is True, f"{scheme} must request the Hive layout"
        assert seen[keep_kwarg] is True, f"{scheme} must keep its index column"

    @pytest.mark.unit
    def test_an_explicit_resolution_still_asks_for_hive(self, tmp_path: Path) -> None:
        """The explicit-resolution branch carries the same flags."""
        from portolan_cli.conversion_config import VectorSettings
        from portolan_cli.convert import _write_partitioned

        table, seen = self._spy("h3")
        settings = VectorSettings(spatial_index="h3", resolution=6, partition=True)

        _write_partitioned(table, tmp_path / "out", settings)

        assert seen["resolution"] == 6
        assert seen["hive"] is True
        assert seen["keep_h3_column"] is True


class TestPartitionedOutputWithGeoparquetIO:
    """`apply_vector_settings` and `_write_partitioned` run against geoparquet-io.

    geoparquet-io 1.5 made every partition method and ``add_kdtree`` refuse a
    call that names neither a resolution nor ``auto`` (geoparquet-io#800,
    #855). The spy tests above cannot see that, because they never call the
    library. These tests write real partitions.
    """

    _COLUMNS = {
        "h3": "h3_cell",
        "s2": "s2_cell",
        "quadkey": "quadkey_prefix",
        "a5": "a5_cell",
        "kdtree": "kdtree_cell",
    }
    # Coarse cells over a small cluster keep every partition above the
    # 100-row floor that geoparquet-io enforces ("Tiny partitions").
    _EXPLICIT = {"h3": 3, "s2": 5, "quadkey": 6, "a5": 3, "kdtree": 1}

    @staticmethod
    def _points(tmp_path: Path) -> Path:
        """Write a 400-point cluster, about 0.2 degrees wide, as a CSV."""
        source = tmp_path / "points.csv"
        rows = [f"{i},POINT({-70 + (i % 20) * 0.01} {-30 + (i // 20) * 0.01})" for i in range(400)]
        source.write_text("id,wkt\n" + "\n".join(rows) + "\n", encoding="utf-8")
        return source

    @classmethod
    def _table(cls, tmp_path: Path) -> Any:
        """Read the point cluster into a geoparquet-io Table."""
        import geoparquet_io as gpio

        return gpio.convert(str(cls._points(tmp_path)))

    @pytest.mark.integration
    @pytest.mark.parametrize("scheme", ["h3", "s2", "quadkey", "a5", "kdtree"])
    @pytest.mark.parametrize("resolution", ["auto", "explicit"])
    def test_every_scheme_writes_hive_partitions(
        self, scheme: str, resolution: str, tmp_path: Path
    ) -> None:
        """Each scheme writes `<column>=<cell>` directories with Parquet files."""
        from portolan_cli.conversion_config import VectorSettings
        from portolan_cli.convert import _write_partitioned, apply_vector_settings

        settings = VectorSettings(
            spatial_index=scheme,
            resolution=self._EXPLICIT[scheme] if resolution == "explicit" else "auto",
            partition=True,
        )
        out = tmp_path / "out"

        table = apply_vector_settings(self._table(tmp_path), settings, partitioned=True)
        _write_partitioned(table, out, settings)

        partitions = sorted(out.glob(f"{self._COLUMNS[scheme]}=*"))
        assert partitions, f"{scheme} wrote no Hive partition directories"
        assert all(any(p.glob("*.parquet")) for p in partitions)

    @pytest.mark.integration
    def test_single_file_writers_keep_the_index_column(self, tmp_path: Path) -> None:
        """`partition: true` alone still adds the column for a single-file write.

        `add` and the multilayer writer call `apply_vector_settings` and write
        one file whatever `partition` says. Only `preparation.convert_vector`
        passes `partitioned=True`, when the partition rule partitions next.
        """
        from portolan_cli.conversion_config import VectorSettings
        from portolan_cli.convert import apply_vector_settings

        settings = VectorSettings(spatial_index="h3", resolution="auto", partition=True)

        table = apply_vector_settings(self._table(tmp_path), settings)

        assert "h3_cell" in table.column_names

    @staticmethod
    def _catalog(tmp_path: Path, scheme: str = "h3") -> Path:
        """Write a catalog config that asks for partitions by ``scheme``."""
        (tmp_path / ".portolan").mkdir()
        (tmp_path / ".portolan" / "config.yaml").write_text(
            "conversion:\n  vector:\n"
            f"    spatial_index: {scheme}\n    resolution: auto\n    partition: true\n",
            encoding="utf-8",
        )
        return tmp_path

    @pytest.mark.integration
    def test_add_keeps_one_file_with_the_index_column(self, tmp_path: Path) -> None:
        """`add` writes one file with the index column, whatever `partition` says.

        The partition rule that `add` runs is the size threshold only. A
        400-row file stays whole.
        """
        import pyarrow.parquet as pq

        from portolan_cli.partitioning import partition_strategy, partition_vector_file
        from portolan_cli.preparation import convert_vector

        root = self._catalog(tmp_path)

        converted = convert_vector(self._points(tmp_path), root, root)

        assert partition_strategy(converted, root) is None
        assert partition_vector_file(converted, root) == []
        assert converted.is_file()
        assert "h3_cell" in pq.read_schema(converted).names

    @pytest.mark.integration
    def test_check_fix_partitions_each_source_in_its_own_directory(self, tmp_path: Path) -> None:
        """`check --fix` writes each source's partitions under `<stem>/`.

        Two sources in one directory share every cell. Each keeps all its rows.
        """
        import shutil

        import pyarrow.parquet as pq

        from portolan_cli.convert import ConversionStatus, convert_file

        root = self._catalog(tmp_path)
        first = self._points(root)
        second = root / "second.csv"
        shutil.copy(first, second)

        results = [convert_file(source, catalog_path=root) for source in (first, second)]

        assert [r.status for r in results] == [ConversionStatus.SUCCESS] * 2
        assert [r.output for r in results] == [root / "points", root / "second"]
        for output in (root / "points", root / "second"):
            files = sorted(output.glob("h3_cell=*/*.parquet"))
            assert files
            assert sum(pq.read_metadata(f).num_rows for f in files) == 400
        assert not (root / "points.parquet").exists()

    @pytest.mark.unit
    def test_size_partition_that_writes_nothing_keeps_the_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The converted file stays when the partition step writes no file."""
        from portolan_cli import partitioning

        converted = tmp_path / "big.parquet"
        converted.write_bytes(b"PAR1")
        monkeypatch.setattr(partitioning, "partition_strategy", lambda *_, **__: "kdtree")
        monkeypatch.setattr(partitioning, "partition_geoparquet", lambda **_: [])

        assert partitioning.partition_vector_file(converted, tmp_path) == []
        assert converted.exists()

    @pytest.mark.unit
    def test_size_partition_refuses_a_directory_with_partitions(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A second partitioned source in one directory raises and keeps its file.

        geoparquet-io does not write over a cell that exists, so a write into
        the same directory would lose the rows in each shared cell.
        """
        from portolan_cli import partitioning
        from portolan_cli.errors import PartitionCollisionError

        (tmp_path / "kdtree_cell=0000").mkdir()
        (tmp_path / "kdtree_cell=0000" / "0000.parquet").write_bytes(b"PAR1")
        converted = tmp_path / "second.parquet"
        converted.write_bytes(b"PAR1")
        called: list[str] = []
        monkeypatch.setattr(partitioning, "partition_strategy", lambda *_, **__: "kdtree")
        monkeypatch.setattr(
            partitioning, "partition_geoparquet", lambda **_: called.append("write") or []
        )

        with pytest.raises(PartitionCollisionError, match="kdtree_cell=0000"):
            partitioning.partition_vector_file(converted, tmp_path)

        assert called == []
        assert converted.exists()
        assert (tmp_path / "kdtree_cell=0000" / "0000.parquet").exists()

    @staticmethod
    def _prepared(path: Path, created_output: bool) -> Any:
        from portolan_cli.preparation import PreparedItem

        return PreparedItem(
            item_id=path.stem,
            collection_id="coll",
            format_type="vector",
            bbox=[0.0, 0.0, 1.0, 1.0],
            asset_files={path.name: (path, "x", 1)},
            item_json_path=None,
            is_collection_level_asset=True,
            format_name="geoparquet",
            created_output=created_output,
        )

    @pytest.mark.unit
    @pytest.mark.parametrize("created_output", [True, False])
    def test_add_collision_removes_only_a_new_copy(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, created_output: bool
    ) -> None:
        """A collision in `add` removes the converted file only when this run wrote it.

        A new copy goes, so the collection holds no row twice. A file that
        existed before the run stays: a `.parquet` source that is its own
        output, or an output that `add --force` kept.
        """
        from portolan_cli import partitioning
        from portolan_cli.add import _maybe_partition_large_file
        from portolan_cli.errors import PartitionCollisionError

        coll = tmp_path / "coll"
        (coll / "kdtree_cell=0000").mkdir(parents=True)
        converted = coll / "big.parquet"
        converted.write_bytes(b"PAR1")
        monkeypatch.setattr(partitioning, "partition_strategy", lambda *_, **__: "kdtree")

        with pytest.raises(PartitionCollisionError):
            _maybe_partition_large_file(self._prepared(converted, created_output), tmp_path, None)

        assert converted.exists() is not created_output

    @pytest.mark.unit
    @pytest.mark.parametrize("existed", [False, True])
    def test_multilayer_collision_removes_only_a_new_layer_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, existed: bool
    ) -> None:
        """A multilayer layer file that this run wrote goes on a collision.

        A layer file that existed before the run stays.
        """
        from portolan_cli import add, partitioning
        from portolan_cli.convert import LayerConversionResult

        coll = tmp_path / "coll"
        (coll / "kdtree_cell=0000").mkdir(parents=True)
        gpkg = coll / "multi.gpkg"
        gpkg.write_bytes(b"x")
        layer = coll / "roads.parquet"
        if existed:
            layer.write_bytes(b"PAR1")

        def _convert(source: Path, out: Path, settings: Any = None) -> list[Any]:
            layer.write_bytes(b"PAR1")
            return [LayerConversionResult(source, "roads", layer, True)]

        monkeypatch.setattr(add, "convert_multilayer_file", _convert)
        monkeypatch.setattr(
            add, "prepare_item", lambda path, **_: self._prepared(path, created_output=False)
        )
        monkeypatch.setattr(partitioning, "partition_strategy", lambda *_, **__: "kdtree")
        opts = add._PrepareOptions(
            catalog_root=tmp_path,
            item_id=None,
            item_datetime=None,
            force=True,
            reconvert=False,
            skip_partitioning=False,
            batch_exclude_names=frozenset(),
        )

        prepared, failures, skips = add._prepare_multilayer_file(gpkg, "coll", opts)

        assert prepared == []
        assert len(failures) == 1
        assert skips == {}
        assert layer.exists() is existed

    @pytest.mark.integration
    def test_prepare_item_reports_a_new_output(self, tmp_path: Path) -> None:
        """`created_output` is True for a new conversion and False for a kept output."""
        import shutil

        from portolan_cli.preparation import prepare_item

        (tmp_path / ".portolan").mkdir()
        (tmp_path / ".portolan" / "config.yaml").write_text("{}\n", encoding="utf-8")
        coll = tmp_path / "coll"
        coll.mkdir()
        fixtures = Path(__file__).parents[1] / "fixtures" / "vector" / "valid"
        source = coll / "points.geojson"
        shutil.copy(fixtures / "points.geojson", source)

        first = prepare_item(path=source, catalog_root=tmp_path, collection_id="coll")
        kept = prepare_item(path=source, catalog_root=tmp_path, collection_id="coll", force=True)

        assert first.created_output is True
        assert kept.created_output is False

    @pytest.mark.integration
    @pytest.mark.xfail(
        strict=True,
        reason=(
            "https://github.com/geoparquet/geoparquet-io/issues/1222: "
            "Table.add_kdtree raises ST_Centroid(BLOB) on a table from "
            "gpio.convert(). When a release fixes it, raise the pin and "
            "remove this marker."
        ),
    )
    def test_single_file_kdtree_index(self, tmp_path: Path) -> None:
        """A non-partitioned kdtree index adds the `kdtree_cell` column."""
        from portolan_cli.conversion_config import VectorSettings
        from portolan_cli.convert import apply_vector_settings

        settings = VectorSettings(spatial_index="kdtree", resolution=1)

        table = apply_vector_settings(self._table(tmp_path), settings)

        assert "kdtree_cell" in table.column_names
