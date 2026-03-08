"""Unit tests for --remove-legacy flag in check command.

Tests the logic for removing legacy/source files after successful conversion
to cloud-native formats. Per Issue #209:
- Only delete after converted file exists and is valid
- Only delete files converted in THIS run (not pre-existing)
- Handle shapefile sidecars (.dbf, .shx, .prj, .cpg, etc.)
- Handle FileGDB directories (.gdb)
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from portolan_cli.check import (
    check_directory,
    get_legacy_files_to_remove,
    remove_legacy_files,
)
from portolan_cli.convert import ConversionReport, ConversionResult, ConversionStatus

if TYPE_CHECKING:
    pass


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def shapefile_with_sidecars(tmp_path: Path) -> Path:
    """Create a shapefile with common sidecar files."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    # Primary .shp file
    shp = data_dir / "test.shp"
    shp.write_bytes(b"fake shapefile data")

    # Required sidecars
    (data_dir / "test.dbf").write_bytes(b"fake dbf")
    (data_dir / "test.shx").write_bytes(b"fake shx")

    # Optional sidecars
    (data_dir / "test.prj").write_text("GEOGCS[...]")
    (data_dir / "test.cpg").write_text("UTF-8")

    return shp


@pytest.fixture
def filegdb_directory(tmp_path: Path) -> Path:
    """Create a FileGDB directory structure."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    gdb = data_dir / "dataset.gdb"
    gdb.mkdir()

    # FileGDB internal files
    (gdb / "a00000001.gdbtable").write_bytes(b"fake gdbtable")
    (gdb / "a00000001.gdbtablx").write_bytes(b"fake gdbtablx")
    (gdb / "gdb").write_bytes(b"fake gdb marker")

    return gdb


@pytest.fixture
def geojson_file(tmp_path: Path) -> Path:
    """Create a simple GeoJSON file."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    geojson = data_dir / "points.geojson"
    geojson.write_text('{"type": "FeatureCollection", "features": []}')

    return geojson


@pytest.fixture
def converted_parquet(tmp_path: Path) -> Path:
    """Create a converted GeoParquet file (simulating successful conversion)."""
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)

    parquet = data_dir / "points.parquet"
    parquet.write_bytes(b"PAR1fake parquet data")

    return parquet


# =============================================================================
# Tests: get_legacy_files_to_remove
# =============================================================================


@pytest.mark.unit
class TestGetLegacyFilesToRemove:
    """Tests for identifying which legacy files should be removed."""

    def test_returns_source_file_for_successful_conversion(
        self,
        tmp_path: Path,
    ) -> None:
        """Should return the source file when conversion succeeded."""
        # Create source and output files
        source = tmp_path / "data.geojson"
        source.write_text("{}")
        output = tmp_path / "data.parquet"
        output.write_bytes(b"parquet data")

        result = ConversionResult(
            source=source,
            output=output,
            format_from="GeoJSON",
            format_to="GeoParquet",
            status=ConversionStatus.SUCCESS,
            error=None,
            duration_ms=100,
        )
        report = ConversionReport(results=[result])

        files_to_remove = get_legacy_files_to_remove(report)

        assert source in files_to_remove
        assert len(files_to_remove) == 1

    def test_returns_empty_for_failed_conversion(
        self,
        tmp_path: Path,
    ) -> None:
        """Should not return files when conversion failed."""
        source = tmp_path / "bad.geojson"
        source.write_text("{}")

        result = ConversionResult(
            source=source,
            output=None,
            format_from="GeoJSON",
            format_to="GeoParquet",
            status=ConversionStatus.FAILED,
            error="Conversion failed",
            duration_ms=50,
        )
        report = ConversionReport(results=[result])

        files_to_remove = get_legacy_files_to_remove(report)

        assert len(files_to_remove) == 0

    def test_returns_empty_for_skipped_conversion(
        self,
        tmp_path: Path,
    ) -> None:
        """Should not return files when conversion was skipped."""
        source = tmp_path / "data.parquet"
        source.write_bytes(b"already parquet")

        result = ConversionResult(
            source=source,
            output=source,
            format_from="GeoParquet",
            format_to="GeoParquet",
            status=ConversionStatus.SKIPPED,
            error=None,
            duration_ms=0,
        )
        report = ConversionReport(results=[result])

        files_to_remove = get_legacy_files_to_remove(report)

        assert len(files_to_remove) == 0

    def test_returns_empty_when_output_missing(
        self,
        tmp_path: Path,
    ) -> None:
        """Should not return files if output doesn't exist (safety check)."""
        source = tmp_path / "data.geojson"
        source.write_text("{}")
        output = tmp_path / "data.parquet"  # Does NOT exist

        result = ConversionResult(
            source=source,
            output=output,
            format_from="GeoJSON",
            format_to="GeoParquet",
            status=ConversionStatus.SUCCESS,
            error=None,
            duration_ms=100,
        )
        report = ConversionReport(results=[result])

        files_to_remove = get_legacy_files_to_remove(report)

        # Should NOT include source since output doesn't exist
        assert len(files_to_remove) == 0

    def test_multiple_conversions_mixed_results(
        self,
        tmp_path: Path,
    ) -> None:
        """Should only return sources from successful conversions."""
        # Successful conversion
        source1 = tmp_path / "good.geojson"
        source1.write_text("{}")
        output1 = tmp_path / "good.parquet"
        output1.write_bytes(b"parquet")

        # Failed conversion
        source2 = tmp_path / "bad.geojson"
        source2.write_text("{}")

        results = [
            ConversionResult(
                source=source1,
                output=output1,
                format_from="GeoJSON",
                format_to="GeoParquet",
                status=ConversionStatus.SUCCESS,
                error=None,
                duration_ms=100,
            ),
            ConversionResult(
                source=source2,
                output=None,
                format_from="GeoJSON",
                format_to="GeoParquet",
                status=ConversionStatus.FAILED,
                error="Failed",
                duration_ms=50,
            ),
        ]
        report = ConversionReport(results=results)

        files_to_remove = get_legacy_files_to_remove(report)

        assert source1 in files_to_remove
        assert source2 not in files_to_remove
        assert len(files_to_remove) == 1


# =============================================================================
# Tests: remove_legacy_files
# =============================================================================


@pytest.mark.unit
class TestRemoveLegacyFiles:
    """Tests for the file removal logic."""

    def test_removes_single_file(
        self,
        geojson_file: Path,
    ) -> None:
        """Should remove a single file."""
        assert geojson_file.exists()

        removed, errors = remove_legacy_files([geojson_file])

        assert not geojson_file.exists()
        assert geojson_file in removed
        assert len(errors) == 0

    def test_removes_shapefile_with_sidecars(
        self,
        shapefile_with_sidecars: Path,
    ) -> None:
        """Should remove shapefile and all its sidecars."""
        shp = shapefile_with_sidecars
        parent = shp.parent

        # Verify sidecars exist
        assert (parent / "test.dbf").exists()
        assert (parent / "test.shx").exists()
        assert (parent / "test.prj").exists()
        assert (parent / "test.cpg").exists()

        removed, errors = remove_legacy_files([shp])

        # All files should be gone
        assert not shp.exists()
        assert not (parent / "test.dbf").exists()
        assert not (parent / "test.shx").exists()
        assert not (parent / "test.prj").exists()
        assert not (parent / "test.cpg").exists()

        # Primary file should be in removed list
        assert shp in removed
        assert len(errors) == 0

    def test_removes_filegdb_directory(
        self,
        filegdb_directory: Path,
    ) -> None:
        """Should remove entire FileGDB directory."""
        gdb = filegdb_directory

        assert gdb.exists()
        assert gdb.is_dir()

        removed, errors = remove_legacy_files([gdb])

        assert not gdb.exists()
        assert gdb in removed
        assert len(errors) == 0

    def test_handles_missing_file_gracefully(
        self,
        tmp_path: Path,
    ) -> None:
        """Should not error if file already deleted."""
        nonexistent = tmp_path / "ghost.geojson"

        removed, errors = remove_legacy_files([nonexistent])

        # Should succeed (idempotent)
        assert len(errors) == 0
        # File wasn't actually removed (didn't exist)
        assert nonexistent not in removed

    def test_handles_permission_error(
        self,
        tmp_path: Path,
    ) -> None:
        """Should report error if file can't be deleted."""
        test_file = tmp_path / "protected.geojson"
        test_file.write_text("{}")

        # Mock unlink to raise PermissionError
        with patch.object(Path, "unlink", side_effect=PermissionError("Access denied")):
            removed, errors = remove_legacy_files([test_file])

        assert test_file not in removed
        assert len(errors) == 1
        assert test_file in errors

    def test_removes_multiple_files(
        self,
        tmp_path: Path,
    ) -> None:
        """Should remove multiple files in one call."""
        file1 = tmp_path / "a.geojson"
        file2 = tmp_path / "b.geojson"
        file1.write_text("{}")
        file2.write_text("{}")

        removed, errors = remove_legacy_files([file1, file2])

        assert not file1.exists()
        assert not file2.exists()
        assert file1 in removed
        assert file2 in removed
        assert len(errors) == 0


# =============================================================================
# Tests: check_directory with remove_legacy flag
# =============================================================================


@pytest.mark.unit
class TestCheckDirectoryRemoveLegacy:
    """Tests for check_directory with remove_legacy=True."""

    def test_remove_legacy_requires_fix(
        self,
        tmp_path: Path,
    ) -> None:
        """Should raise error if remove_legacy=True but fix=False."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.geojson").write_text("{}")

        with pytest.raises(ValueError, match="remove_legacy requires fix=True"):
            check_directory(data_dir, fix=False, remove_legacy=True)

    def test_remove_legacy_ignored_in_dry_run(
        self,
        tmp_path: Path,
    ) -> None:
        """Should not delete files when dry_run=True."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        geojson = data_dir / "test.geojson"
        geojson.write_text('{"type": "FeatureCollection", "features": []}')

        # Mock conversion to avoid actual file operations
        with patch("portolan_cli.check.convert_directory") as mock_convert:
            mock_convert.return_value = ConversionReport(results=[])

            check_directory(data_dir, fix=True, dry_run=True, remove_legacy=True)

        # File should still exist (dry run)
        assert geojson.exists()

    @patch("portolan_cli.check.remove_legacy_files")
    @patch("portolan_cli.check.convert_directory")
    def test_remove_legacy_called_after_conversion(
        self,
        mock_convert: MagicMock,
        mock_remove: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Should call remove_legacy_files after successful conversions."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        source = data_dir / "test.geojson"
        source.write_text("{}")
        output = data_dir / "test.parquet"
        output.write_bytes(b"parquet")

        mock_convert.return_value = ConversionReport(
            results=[
                ConversionResult(
                    source=source,
                    output=output,
                    format_from="GeoJSON",
                    format_to="GeoParquet",
                    status=ConversionStatus.SUCCESS,
                    error=None,
                    duration_ms=100,
                )
            ]
        )
        mock_remove.return_value = ([source], {})

        check_directory(data_dir, fix=True, remove_legacy=True)

        mock_remove.assert_called_once()
        # Verify the source file was passed to remove
        call_args = mock_remove.call_args[0][0]
        assert source in call_args


# =============================================================================
# Tests: Hypothesis-based property tests
# =============================================================================


@pytest.mark.unit
class TestRemoveLegacyHypothesis:
    """Property-based tests for remove_legacy logic."""

    # TODO: Add hypothesis tests for:
    # - Arbitrary file extensions
    # - Various sidecar combinations
    # - Race conditions (file deleted between check and removal)
    pass
