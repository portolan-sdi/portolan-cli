"""Unit tests for the scan module.

These tests follow TDD: written FIRST, verified to FAIL, then implementation added.
Tests are organized by user story for traceability to spec.md requirements.

Test fixtures are in tests/fixtures/scan/:
- clean_flat/: 3 valid files, no issues
- complete_shapefile/: Valid shapefile with all sidecars
- incomplete_shapefile/: Shapefile missing .dbf
- invalid_chars/: Files with spaces and non-ASCII
- multiple_primaries/: 3 GeoJSON files in one directory
- mixed_formats/: Raster + vector together
- nested/: Hierarchical directory structure
- duplicate_basenames/: Same filename, different case
- unsupported/: Mix of supported and unsupported formats
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

# Fixture path helper
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "scan"


@pytest.fixture
def fixtures_dir() -> Path:
    """Return the path to scan test fixtures."""
    return FIXTURES_DIR


# =============================================================================
# Phase 2: Foundational - Data Classes & Enums
# =============================================================================


@pytest.mark.unit
class TestEnums:
    """Tests for Severity, IssueType, and FormatType enums."""

    def test_severity_has_error_warning_info(self) -> None:
        """Severity enum has ERROR, WARNING, INFO values."""
        from portolan_cli.scan import Severity

        assert Severity.ERROR.value == "error"
        assert Severity.WARNING.value == "warning"
        assert Severity.INFO.value == "info"

    def test_issue_type_has_all_types(self) -> None:
        """IssueType enum has all 18 issue types (10 original + 8 new)."""
        from portolan_cli.scan import IssueType

        # Original 10 issue types
        assert IssueType.INCOMPLETE_SHAPEFILE.value == "incomplete_shapefile"
        assert IssueType.ZERO_BYTE_FILE.value == "zero_byte_file"
        assert IssueType.SYMLINK_LOOP.value == "symlink_loop"
        assert IssueType.BROKEN_SYMLINK.value == "broken_symlink"
        assert IssueType.PERMISSION_DENIED.value == "permission_denied"
        assert IssueType.INVALID_CHARACTERS.value == "invalid_characters"
        assert IssueType.MULTIPLE_PRIMARIES.value == "multiple_primaries"
        assert IssueType.LONG_PATH.value == "long_path"
        assert IssueType.DUPLICATE_BASENAME.value == "duplicate_basename"
        assert IssueType.MIXED_FORMATS.value == "mixed_formats"

        # NEW: Special format detection (4)
        assert IssueType.FILEGDB_DETECTED.value == "filegdb_detected"
        assert IssueType.HIVE_PARTITION_DETECTED.value == "hive_partition"
        assert IssueType.EXISTING_CATALOG.value == "existing_catalog"
        assert IssueType.DUAL_FORMAT.value == "dual_format"

        # NEW: Cross-platform compatibility (2)
        assert IssueType.WINDOWS_RESERVED_NAME.value == "windows_reserved_name"
        assert IssueType.PATH_TOO_LONG.value == "path_too_long"

        # NEW: Structure issues (2)
        assert IssueType.MIXED_FLAT_MULTIITEM.value == "mixed_flat_multiitem"
        assert IssueType.ORPHAN_SIDECAR.value == "orphan_sidecar"

        # Total should be 18
        assert len(IssueType) == 18

    def test_format_type_has_vector_and_raster(self) -> None:
        """FormatType enum has VECTOR and RASTER values."""
        from portolan_cli.scan import FormatType

        assert FormatType.VECTOR.value == "vector"
        assert FormatType.RASTER.value == "raster"


@pytest.mark.unit
class TestScanOptions:
    """Tests for ScanOptions dataclass."""

    def test_default_values(self) -> None:
        """ScanOptions has correct default values."""
        from portolan_cli.scan import ScanOptions

        opts = ScanOptions()
        # Original defaults
        assert opts.recursive is True
        assert opts.max_depth is None
        assert opts.include_hidden is False
        assert opts.follow_symlinks is False
        # NEW defaults
        assert opts.show_all is False
        assert opts.verbose is False
        assert opts.allow_existing_catalogs is False
        assert opts.fix is False
        assert opts.unsafe_fix is False
        assert opts.dry_run is False
        assert opts.suggest_collections is False

    def test_custom_values(self) -> None:
        """ScanOptions accepts custom values."""
        from portolan_cli.scan import ScanOptions

        opts = ScanOptions(
            recursive=False,
            max_depth=3,
            include_hidden=True,
            follow_symlinks=True,
        )
        assert opts.recursive is False
        assert opts.max_depth == 3
        assert opts.include_hidden is True
        assert opts.follow_symlinks is True

    def test_new_options(self) -> None:
        """ScanOptions accepts new option values."""
        from portolan_cli.scan import ScanOptions

        opts = ScanOptions(
            show_all=True,
            verbose=True,
            allow_existing_catalogs=True,
            fix=True,
            unsafe_fix=True,  # Valid because fix=True
            dry_run=True,
            suggest_collections=True,
        )
        assert opts.show_all is True
        assert opts.verbose is True
        assert opts.allow_existing_catalogs is True
        assert opts.fix is True
        assert opts.unsafe_fix is True
        assert opts.dry_run is True
        assert opts.suggest_collections is True

    def test_unsafe_fix_requires_fix(self) -> None:
        """ScanOptions raises ValueError if unsafe_fix=True but fix=False."""
        from portolan_cli.scan import ScanOptions

        with pytest.raises(ValueError, match="--unsafe-fix requires --fix"):
            ScanOptions(unsafe_fix=True, fix=False)

    def test_is_frozen(self) -> None:
        """ScanOptions is immutable (frozen dataclass)."""
        from portolan_cli.scan import ScanOptions

        opts = ScanOptions()
        with pytest.raises(AttributeError):
            opts.recursive = False  # type: ignore[misc]


@pytest.mark.unit
class TestScannedFile:
    """Tests for ScannedFile dataclass."""

    def test_creation(self, tmp_path: Path) -> None:
        """ScannedFile can be created with required fields."""
        from portolan_cli.scan import FormatType, ScannedFile

        sf = ScannedFile(
            path=tmp_path / "data.parquet",
            relative_path="data.parquet",
            extension=".parquet",
            format_type=FormatType.VECTOR,
            size_bytes=1024,
        )
        assert sf.path == tmp_path / "data.parquet"
        assert sf.relative_path == "data.parquet"
        assert sf.extension == ".parquet"
        assert sf.format_type == FormatType.VECTOR
        assert sf.size_bytes == 1024

    def test_basename_property(self, tmp_path: Path) -> None:
        """ScannedFile.basename returns filename without directory."""
        from portolan_cli.scan import FormatType, ScannedFile

        sf = ScannedFile(
            path=tmp_path / "subdir" / "data.parquet",
            relative_path="subdir/data.parquet",
            extension=".parquet",
            format_type=FormatType.VECTOR,
            size_bytes=1024,
        )
        assert sf.basename == "data.parquet"

    def test_is_frozen(self, tmp_path: Path) -> None:
        """ScannedFile is immutable (frozen dataclass)."""
        from portolan_cli.scan import FormatType, ScannedFile

        sf = ScannedFile(
            path=tmp_path / "data.parquet",
            relative_path="data.parquet",
            extension=".parquet",
            format_type=FormatType.VECTOR,
            size_bytes=1024,
        )
        with pytest.raises(AttributeError):
            sf.size_bytes = 2048  # type: ignore[misc]


@pytest.mark.unit
class TestScanIssue:
    """Tests for ScanIssue dataclass."""

    def test_creation(self, tmp_path: Path) -> None:
        """ScanIssue can be created with required fields."""
        from portolan_cli.scan import IssueType, ScanIssue, Severity

        issue = ScanIssue(
            path=tmp_path / "bad.shp",
            relative_path="bad.shp",
            issue_type=IssueType.INCOMPLETE_SHAPEFILE,
            severity=Severity.ERROR,
            message="Missing required sidecar: .dbf",
        )
        assert issue.path == tmp_path / "bad.shp"
        assert issue.issue_type == IssueType.INCOMPLETE_SHAPEFILE
        assert issue.severity == Severity.ERROR
        assert issue.suggestion is None

    def test_with_suggestion(self, tmp_path: Path) -> None:
        """ScanIssue can include optional suggestion."""
        from portolan_cli.scan import IssueType, ScanIssue, Severity

        issue = ScanIssue(
            path=tmp_path / "data (copy).parquet",
            relative_path="data (copy).parquet",
            issue_type=IssueType.INVALID_CHARACTERS,
            severity=Severity.WARNING,
            message="Filename contains spaces and parentheses",
            suggestion="Rename to data_copy.parquet",
        )
        assert issue.suggestion == "Rename to data_copy.parquet"

    def test_is_frozen(self, tmp_path: Path) -> None:
        """ScanIssue is immutable (frozen dataclass)."""
        from portolan_cli.scan import IssueType, ScanIssue, Severity

        issue = ScanIssue(
            path=tmp_path / "bad.shp",
            relative_path="bad.shp",
            issue_type=IssueType.INCOMPLETE_SHAPEFILE,
            severity=Severity.ERROR,
            message="Test",
        )
        with pytest.raises(AttributeError):
            issue.message = "Changed"  # type: ignore[misc]


@pytest.mark.unit
class TestScanResult:
    """Tests for ScanResult dataclass."""

    def test_creation(self, tmp_path: Path) -> None:
        """ScanResult can be created with required fields."""
        from portolan_cli.scan import ScanResult

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[],
            skipped=[],
            directories_scanned=1,
        )
        assert result.root == tmp_path
        assert result.ready == []
        assert result.issues == []
        assert result.skipped == []
        assert result.directories_scanned == 1

    def test_has_errors_property_false(self, tmp_path: Path) -> None:
        """ScanResult.has_errors is False when no ERROR issues."""
        from portolan_cli.scan import IssueType, ScanIssue, ScanResult, Severity

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[
                ScanIssue(
                    path=tmp_path / "a.parquet",
                    relative_path="a.parquet",
                    issue_type=IssueType.INVALID_CHARACTERS,
                    severity=Severity.WARNING,
                    message="Test",
                )
            ],
            skipped=[],
            directories_scanned=1,
        )
        assert result.has_errors is False

    def test_has_errors_property_true(self, tmp_path: Path) -> None:
        """ScanResult.has_errors is True when ERROR issues exist."""
        from portolan_cli.scan import IssueType, ScanIssue, ScanResult, Severity

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[
                ScanIssue(
                    path=tmp_path / "bad.shp",
                    relative_path="bad.shp",
                    issue_type=IssueType.INCOMPLETE_SHAPEFILE,
                    severity=Severity.ERROR,
                    message="Test",
                )
            ],
            skipped=[],
            directories_scanned=1,
        )
        assert result.has_errors is True

    def test_error_count_property(self, tmp_path: Path) -> None:
        """ScanResult.error_count returns count of ERROR issues."""
        from portolan_cli.scan import IssueType, ScanIssue, ScanResult, Severity

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[
                ScanIssue(
                    path=tmp_path / "a.shp",
                    relative_path="a.shp",
                    issue_type=IssueType.INCOMPLETE_SHAPEFILE,
                    severity=Severity.ERROR,
                    message="Test",
                ),
                ScanIssue(
                    path=tmp_path / "b.parquet",
                    relative_path="b.parquet",
                    issue_type=IssueType.ZERO_BYTE_FILE,
                    severity=Severity.ERROR,
                    message="Test",
                ),
                ScanIssue(
                    path=tmp_path / "c.parquet",
                    relative_path="c.parquet",
                    issue_type=IssueType.INVALID_CHARACTERS,
                    severity=Severity.WARNING,
                    message="Test",
                ),
            ],
            skipped=[],
            directories_scanned=1,
        )
        assert result.error_count == 2

    def test_warning_count_property(self, tmp_path: Path) -> None:
        """ScanResult.warning_count returns count of WARNING issues."""
        from portolan_cli.scan import IssueType, ScanIssue, ScanResult, Severity

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[
                ScanIssue(
                    path=tmp_path / "a.parquet",
                    relative_path="a.parquet",
                    issue_type=IssueType.INVALID_CHARACTERS,
                    severity=Severity.WARNING,
                    message="Test",
                ),
                ScanIssue(
                    path=tmp_path / "b.parquet",
                    relative_path="b.parquet",
                    issue_type=IssueType.LONG_PATH,
                    severity=Severity.WARNING,
                    message="Test",
                ),
            ],
            skipped=[],
            directories_scanned=1,
        )
        assert result.warning_count == 2

    def test_info_count_property(self, tmp_path: Path) -> None:
        """ScanResult.info_count returns count of INFO issues."""
        from portolan_cli.scan import IssueType, ScanIssue, ScanResult, Severity

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[
                ScanIssue(
                    path=tmp_path,
                    relative_path=".",
                    issue_type=IssueType.DUPLICATE_BASENAME,
                    severity=Severity.INFO,
                    message="Test",
                ),
            ],
            skipped=[],
            directories_scanned=1,
        )
        assert result.info_count == 1

    def test_classification_summary_empty(self, tmp_path: Path) -> None:
        """ScanResult.classification_summary returns empty dict for empty result."""
        from portolan_cli.scan import ScanResult

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[],
            skipped=[],
            directories_scanned=1,
        )
        summary = result.classification_summary
        # Empty result has geo_asset=0
        assert summary.get("geo_asset", 0) == 0

    def test_classification_summary_counts_ready_as_geo_asset(self, tmp_path: Path) -> None:
        """ScanResult.classification_summary counts ready files as geo_asset."""
        from portolan_cli.scan import FormatType, ScannedFile, ScanResult

        result = ScanResult(
            root=tmp_path,
            ready=[
                ScannedFile(
                    path=tmp_path / "a.geojson",
                    relative_path="a.geojson",
                    extension=".geojson",
                    format_type=FormatType.VECTOR,
                    size_bytes=100,
                ),
                ScannedFile(
                    path=tmp_path / "b.geojson",
                    relative_path="b.geojson",
                    extension=".geojson",
                    format_type=FormatType.VECTOR,
                    size_bytes=200,
                ),
            ],
            issues=[],
            skipped=[],
            directories_scanned=1,
        )
        summary = result.classification_summary
        assert summary["geo_asset"] == 2

    def test_classification_summary_counts_skipped_by_category(self, tmp_path: Path) -> None:
        """ScanResult.classification_summary counts skipped files by category."""
        from portolan_cli.scan import ScanResult
        from portolan_cli.scan_classify import (
            FileCategory,
            SkippedFile,
            SkipReasonType,
        )

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[],
            skipped=[
                SkippedFile(
                    path=tmp_path / "a.csv",
                    relative_path="a.csv",
                    category=FileCategory.TABULAR_DATA,
                    reason_type=SkipReasonType.NOT_GEOSPATIAL,
                    reason_message="CSV is tabular data",
                ),
                SkippedFile(
                    path=tmp_path / "b.md",
                    relative_path="b.md",
                    category=FileCategory.DOCUMENTATION,
                    reason_type=SkipReasonType.NOT_GEOSPATIAL,
                    reason_message="Markdown is documentation",
                ),
                SkippedFile(
                    path=tmp_path / "c.csv",
                    relative_path="c.csv",
                    category=FileCategory.TABULAR_DATA,
                    reason_type=SkipReasonType.NOT_GEOSPATIAL,
                    reason_message="CSV is tabular data",
                ),
            ],
            directories_scanned=1,
        )
        summary = result.classification_summary
        assert summary["tabular_data"] == 2
        assert summary["documentation"] == 1

    def test_classification_summary_handles_legacy_paths(self, tmp_path: Path) -> None:
        """ScanResult.classification_summary handles legacy Path objects in skipped."""
        from portolan_cli.scan import ScanResult

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[],
            skipped=[
                tmp_path / "unknown.xyz",  # Legacy Path object
            ],
            directories_scanned=1,
        )
        summary = result.classification_summary
        # Legacy paths are counted as unknown
        assert summary["unknown"] == 1

    def test_to_dict_returns_json_serializable(self, tmp_path: Path) -> None:
        """ScanResult.to_dict returns JSON-serializable dictionary."""
        from portolan_cli.scan import (
            FormatType,
            IssueType,
            ScanIssue,
            ScannedFile,
            ScanResult,
            Severity,
        )

        result = ScanResult(
            root=tmp_path,
            ready=[
                ScannedFile(
                    path=tmp_path / "data.parquet",
                    relative_path="data.parquet",
                    extension=".parquet",
                    format_type=FormatType.VECTOR,
                    size_bytes=1024,
                )
            ],
            issues=[
                ScanIssue(
                    path=tmp_path / "bad.shp",
                    relative_path="bad.shp",
                    issue_type=IssueType.INCOMPLETE_SHAPEFILE,
                    severity=Severity.ERROR,
                    message="Missing .dbf",
                    suggestion="Add the .dbf file",
                )
            ],
            skipped=[tmp_path / "data.csv"],
            directories_scanned=5,
        )

        d = result.to_dict()

        # Should be JSON serializable
        json_str = json.dumps(d)
        assert isinstance(json_str, str)

        # Check structure
        assert d["root"] == str(tmp_path)
        assert d["summary"]["directories_scanned"] == 5
        assert d["summary"]["ready_count"] == 1
        assert d["summary"]["issue_count"] == 1
        assert d["summary"]["skipped_count"] == 1

        assert len(d["ready"]) == 1
        assert d["ready"][0]["extension"] == ".parquet"
        assert d["ready"][0]["format_type"] == "vector"

        assert len(d["issues"]) == 1
        assert d["issues"][0]["type"] == "incomplete_shapefile"
        assert d["issues"][0]["severity"] == "error"
        assert d["issues"][0]["suggestion"] == "Add the .dbf file"

    def test_to_dict_includes_proposed_fixes(self, tmp_path: Path) -> None:
        """ScanResult.to_dict includes proposed_fixes when present."""
        from portolan_cli.scan import (
            FormatType,
            IssueType,
            ScanIssue,
            ScannedFile,
            ScanResult,
            Severity,
        )
        from portolan_cli.scan_fix import FixCategory, ProposedFix

        issue = ScanIssue(
            path=tmp_path / "bad name.geojson",
            relative_path="bad name.geojson",
            issue_type=IssueType.INVALID_CHARACTERS,
            severity=Severity.WARNING,
            message="Contains spaces",
        )

        fix = ProposedFix(
            issue=issue,
            category=FixCategory.SAFE,
            action="rename",
            details={"old": "bad name.geojson", "new": "bad_name.geojson"},
            preview="bad name.geojson -> bad_name.geojson",
        )

        result = ScanResult(
            root=tmp_path,
            ready=[
                ScannedFile(
                    path=tmp_path / "data.parquet",
                    relative_path="data.parquet",
                    extension=".parquet",
                    format_type=FormatType.VECTOR,
                    size_bytes=1024,
                )
            ],
            issues=[issue],
            skipped=[],
            directories_scanned=1,
            proposed_fixes=[fix],
        )

        d = result.to_dict()

        # Should include proposed_fixes
        assert "proposed_fixes" in d
        assert len(d["proposed_fixes"]) == 1
        assert d["proposed_fixes"][0]["category"] == "safe"
        assert d["proposed_fixes"][0]["action"] == "rename"

    def test_to_dict_includes_applied_fixes(self, tmp_path: Path) -> None:
        """ScanResult.to_dict includes applied_fixes when present."""
        from portolan_cli.scan import (
            FormatType,
            IssueType,
            ScanIssue,
            ScannedFile,
            ScanResult,
            Severity,
        )
        from portolan_cli.scan_fix import FixCategory, ProposedFix

        issue = ScanIssue(
            path=tmp_path / "bad name.geojson",
            relative_path="bad name.geojson",
            issue_type=IssueType.INVALID_CHARACTERS,
            severity=Severity.WARNING,
            message="Contains spaces",
        )

        applied = ProposedFix(
            issue=issue,
            category=FixCategory.SAFE,
            action="rename",
            details={"old": "bad name.geojson", "new": "bad_name.geojson"},
            preview="bad name.geojson -> bad_name.geojson",
        )

        result = ScanResult(
            root=tmp_path,
            ready=[
                ScannedFile(
                    path=tmp_path / "data.parquet",
                    relative_path="data.parquet",
                    extension=".parquet",
                    format_type=FormatType.VECTOR,
                    size_bytes=1024,
                )
            ],
            issues=[],  # Issue resolved
            skipped=[],
            directories_scanned=1,
            applied_fixes=[applied],
        )

        d = result.to_dict()

        # Should include applied_fixes
        assert "applied_fixes" in d
        assert len(d["applied_fixes"]) == 1
        assert d["applied_fixes"][0]["category"] == "safe"
        assert d["applied_fixes"][0]["preview"] == "bad name.geojson -> bad_name.geojson"

    def test_to_dict_excludes_empty_fixes(self, tmp_path: Path) -> None:
        """ScanResult.to_dict excludes proposed_fixes/applied_fixes when empty."""
        from portolan_cli.scan import ScanResult

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[],
            skipped=[],
            directories_scanned=1,
        )

        d = result.to_dict()

        # Should NOT include proposed_fixes or applied_fixes when empty
        assert "proposed_fixes" not in d
        assert "applied_fixes" not in d


@pytest.mark.unit
class TestScanResultIsRelativeTo:
    """Tests for ScanResult._is_relative_to helper method."""

    def test_is_relative_to_true(self, tmp_path: Path) -> None:
        """_is_relative_to returns True for child paths."""
        from portolan_cli.scan import ScanResult

        child = tmp_path / "subdir" / "file.txt"
        result = ScanResult._is_relative_to(child, tmp_path)
        assert result is True

    def test_is_relative_to_false(self, tmp_path: Path) -> None:
        """_is_relative_to returns False for unrelated paths."""
        from portolan_cli.scan import ScanResult

        other = Path("/completely/different/path")
        result = ScanResult._is_relative_to(other, tmp_path)
        assert result is False

    def test_is_relative_to_same_path(self, tmp_path: Path) -> None:
        """_is_relative_to returns True when path equals base."""
        from portolan_cli.scan import ScanResult

        result = ScanResult._is_relative_to(tmp_path, tmp_path)
        assert result is True


# =============================================================================
# Phase 3: User Story 1 - Basic Directory Scan
# =============================================================================


@pytest.mark.unit
class TestBasicScan:
    """Tests for basic directory scanning (US1)."""

    def test_scan_nonexistent_path_raises_file_not_found(self) -> None:
        """scan_directory raises FileNotFoundError for non-existent path."""
        from portolan_cli.scan import scan_directory

        with pytest.raises(FileNotFoundError):
            scan_directory(Path("/nonexistent/path/that/does/not/exist"))

    def test_scan_file_raises_not_a_directory(self, tmp_path: Path) -> None:
        """scan_directory raises NotADirectoryError when given a file."""
        from portolan_cli.scan import scan_directory

        test_file = tmp_path / "test.txt"
        test_file.write_text("content")

        with pytest.raises(NotADirectoryError):
            scan_directory(test_file)

    def test_scan_clean_flat_returns_three_files(self, fixtures_dir: Path) -> None:
        """scan_directory on clean_flat fixture returns 3 ready files.

        Note: The fixture contains 3 files in one directory, which correctly
        triggers a "multiple primaries" ERROR per ADR-0017. Multiple geo-assets
        per directory is a blocking error requiring manual reorganization.
        """
        from portolan_cli.scan import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "clean_flat")

        assert len(result.ready) == 3
        # Multiple primaries is now an ERROR (blocking), not a warning
        assert result.error_count == 1
        assert result.warning_count == 0
        assert any(i.issue_type == IssueType.MULTIPLE_PRIMARIES for i in result.issues)

    def test_scan_unsupported_returns_correct_counts(self, fixtures_dir: Path) -> None:
        """scan_directory on unsupported fixture returns correct ready/skipped."""
        from portolan_cli.scan import scan_directory

        result = scan_directory(fixtures_dir / "unsupported")

        # Should have 1 supported file (argentina.geojson)
        assert len(result.ready) == 1
        # Should have 2 skipped files (metadata.csv, project.mxd)
        assert len(result.skipped) == 2

    def test_scan_detects_vector_format(self, fixtures_dir: Path) -> None:
        """scan_directory correctly identifies vector formats."""
        from portolan_cli.scan import FormatType, scan_directory

        result = scan_directory(fixtures_dir / "clean_flat")

        # All files in clean_flat are vector (parquet, geojson)
        for f in result.ready:
            assert f.format_type == FormatType.VECTOR

    def test_scan_detects_raster_format(self, fixtures_dir: Path) -> None:
        """scan_directory correctly identifies raster formats."""
        from portolan_cli.scan import FormatType, scan_directory

        result = scan_directory(fixtures_dir / "mixed_formats")

        formats = {f.extension: f.format_type for f in result.ready}
        assert formats.get(".tif") == FormatType.RASTER or formats.get(".tiff") == FormatType.RASTER

    def test_scan_complete_shapefile_counts_as_one(self, fixtures_dir: Path) -> None:
        """scan_directory on complete_shapefile counts .shp as one file, sidecars skipped."""
        from portolan_cli.scan import scan_directory

        result = scan_directory(fixtures_dir / "complete_shapefile")

        # Should have 1 ready file (the .shp)
        assert len(result.ready) == 1
        assert result.ready[0].extension == ".shp"
        # Sidecars should be skipped (not in ready)
        assert len(result.skipped) >= 3  # .dbf, .shx, .prj


# =============================================================================
# Phase 4: User Story 2 - Issue Detection
# =============================================================================


@pytest.mark.unit
class TestIssueDetection:
    """Tests for issue detection (US2)."""

    def test_detect_incomplete_shapefile(self, fixtures_dir: Path) -> None:
        """scan_directory detects incomplete shapefile (missing .dbf)."""
        from portolan_cli.scan import IssueType, Severity, scan_directory

        result = scan_directory(fixtures_dir / "incomplete_shapefile")

        # Should have an error for incomplete shapefile
        incomplete_issues = [
            i for i in result.issues if i.issue_type == IssueType.INCOMPLETE_SHAPEFILE
        ]
        assert len(incomplete_issues) >= 1
        assert incomplete_issues[0].severity == Severity.ERROR

    def test_detect_zero_byte_file(self, tmp_path: Path) -> None:
        """scan_directory detects zero-byte files."""
        from portolan_cli.scan import IssueType, Severity, scan_directory

        # Create a zero-byte geospatial file
        zero_file = tmp_path / "empty.geojson"
        zero_file.touch()

        result = scan_directory(tmp_path)

        zero_issues = [i for i in result.issues if i.issue_type == IssueType.ZERO_BYTE_FILE]
        assert len(zero_issues) == 1
        assert zero_issues[0].severity == Severity.ERROR

    def test_detect_invalid_characters(self, fixtures_dir: Path) -> None:
        """scan_directory detects invalid characters in filenames."""
        from portolan_cli.scan import IssueType, Severity, scan_directory

        result = scan_directory(fixtures_dir / "invalid_chars")

        invalid_issues = [i for i in result.issues if i.issue_type == IssueType.INVALID_CHARACTERS]
        assert len(invalid_issues) >= 1
        assert invalid_issues[0].severity == Severity.WARNING

    def test_detect_multiple_primaries(self, fixtures_dir: Path) -> None:
        """scan_directory detects multiple primary assets in same directory."""
        from portolan_cli.scan import IssueType, Severity, scan_directory

        result = scan_directory(fixtures_dir / "multiple_primaries")

        multi_issues = [i for i in result.issues if i.issue_type == IssueType.MULTIPLE_PRIMARIES]
        assert len(multi_issues) >= 1
        # Multiple primaries is a blocking ERROR per ADR-0017
        assert multi_issues[0].severity == Severity.ERROR

    def test_detect_long_path(self, tmp_path: Path) -> None:
        """scan_directory detects very long paths (200+ chars)."""
        from portolan_cli.scan import IssueType, Severity, scan_directory

        # Create a deeply nested path to exceed 200 chars (use .geojson since .parquet needs metadata)
        long_name = "a" * 50
        nested = tmp_path / long_name / long_name / long_name / long_name / "data.geojson"
        nested.parent.mkdir(parents=True)
        nested.write_text('{"type": "FeatureCollection", "features": []}')

        result = scan_directory(tmp_path)

        long_issues = [i for i in result.issues if i.issue_type == IssueType.LONG_PATH]
        assert len(long_issues) >= 1
        assert long_issues[0].severity == Severity.WARNING

    def test_sibling_directories_same_filename_no_warning(self, fixtures_dir: Path) -> None:
        """Sibling directories with same filename should NOT produce duplicate warning.

        This is intentional organization (e.g., 2010/radios.parquet and 2022/radios.parquet).
        Only duplicates within the SAME directory should warn.
        """
        from portolan_cli.scan import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "duplicate_basenames")

        dup_issues = [i for i in result.issues if i.issue_type == IssueType.DUPLICATE_BASENAME]
        # Should be empty - files are in different directories (dir_a, dir_b)
        assert len(dup_issues) == 0

    def test_detect_duplicate_basenames_same_directory(self, tmp_path: Path) -> None:
        """scan_directory detects duplicate basenames within the same directory.

        This test creates two files with names that differ only in case (data.geojson
        and DATA.geojson). On case-sensitive filesystems (Linux), both files exist and
        the scan should detect them as duplicates. On case-insensitive filesystems
        (macOS, Windows), only one file can exist, so we skip the test there.
        """
        from portolan_cli.scan import IssueType, Severity, scan_directory

        # Create two files with same basename (case-insensitive) in SAME directory
        (tmp_path / "data.geojson").write_text('{"type": "FeatureCollection", "features": []}')
        (tmp_path / "DATA.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        # Skip on case-insensitive filesystems where we can't create both files
        if not (tmp_path / "DATA.geojson").exists() or not (tmp_path / "data.geojson").exists():
            pytest.skip("Case-insensitive filesystem: cannot create files differing only by case")

        # Verify we actually have two distinct files
        files_in_dir = list(tmp_path.iterdir())
        if len(files_in_dir) < 2:
            pytest.skip("Case-insensitive filesystem: files were merged")

        result = scan_directory(tmp_path)

        dup_issues = [i for i in result.issues if i.issue_type == IssueType.DUPLICATE_BASENAME]
        assert len(dup_issues) >= 1
        assert dup_issues[0].severity == Severity.INFO

    def test_detect_mixed_formats(self, fixtures_dir: Path) -> None:
        """scan_directory detects mixed raster/vector in same directory."""
        from portolan_cli.scan import IssueType, Severity, scan_directory

        result = scan_directory(fixtures_dir / "mixed_formats")

        mixed_issues = [i for i in result.issues if i.issue_type == IssueType.MIXED_FORMATS]
        assert len(mixed_issues) >= 1
        assert mixed_issues[0].severity == Severity.INFO


@pytest.mark.unit
class TestStructureValidation:
    """Tests for structure validation (US3) - structural issues in directories."""

    def test_orphan_sidecar_detected(self, tmp_path: Path) -> None:
        """Sidecar files without a primary (.shp) are flagged as orphan."""
        from portolan_cli.scan import IssueType, Severity, scan_directory

        # Create sidecar files without matching .shp
        (tmp_path / "orphan.dbf").write_bytes(b"\x00" * 100)
        (tmp_path / "orphan.shx").write_bytes(b"\x00" * 100)
        # No orphan.shp exists!

        result = scan_directory(tmp_path)

        orphan_issues = [i for i in result.issues if i.issue_type == IssueType.ORPHAN_SIDECAR]
        # Should detect orphan sidecars
        assert len(orphan_issues) >= 1
        assert orphan_issues[0].severity == Severity.WARNING

    def test_multiple_primaries_detected(self, tmp_path: Path) -> None:
        """Multiple primary assets in one directory are flagged."""
        from portolan_cli.scan import IssueType, Severity, scan_directory

        # Create multiple geospatial files in same directory
        (tmp_path / "dataset1.geojson").write_text('{"type": "FeatureCollection", "features": []}')
        (tmp_path / "dataset2.geojson").write_text('{"type": "FeatureCollection", "features": []}')
        (tmp_path / "dataset3.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        result = scan_directory(tmp_path)

        multi_issues = [i for i in result.issues if i.issue_type == IssueType.MULTIPLE_PRIMARIES]
        assert len(multi_issues) >= 1
        # Multiple primaries is a blocking ERROR per ADR-0017
        assert multi_issues[0].severity == Severity.ERROR
        # Message should indicate the count
        assert "3" in multi_issues[0].message

    def test_incomplete_shapefile_detected(self, tmp_path: Path) -> None:
        """Shapefile missing required sidecars (.dbf, .shx) is flagged as incomplete."""
        from portolan_cli.scan import IssueType, Severity, scan_directory

        # Create a .shp file without required sidecars
        (tmp_path / "incomplete.shp").write_bytes(b"\x00" * 100)
        # Only add optional sidecar, not required ones
        (tmp_path / "incomplete.prj").write_text("GEOGCS[...]")
        # Missing: .dbf and .shx (required)

        result = scan_directory(tmp_path)

        incomplete_issues = [
            i for i in result.issues if i.issue_type == IssueType.INCOMPLETE_SHAPEFILE
        ]
        assert len(incomplete_issues) >= 1
        assert incomplete_issues[0].severity == Severity.ERROR
        # Message should mention missing sidecars
        assert ".dbf" in incomplete_issues[0].message or ".shx" in incomplete_issues[0].message

    def test_mixed_formats_detected(self, tmp_path: Path) -> None:
        """Mixed raster/vector in same directory is flagged."""
        from portolan_cli.scan import IssueType, Severity, scan_directory

        # Create both vector and raster files in same directory
        (tmp_path / "vector.geojson").write_text('{"type": "FeatureCollection", "features": []}')
        (tmp_path / "raster.tif").write_bytes(b"II*\x00" + b"\x00" * 100)  # Minimal TIFF header

        result = scan_directory(tmp_path)

        mixed_issues = [i for i in result.issues if i.issue_type == IssueType.MIXED_FORMATS]
        assert len(mixed_issues) >= 1
        assert mixed_issues[0].severity == Severity.INFO

    def test_mixed_flat_multiitem_detected(self, tmp_path: Path) -> None:
        """Directory with files both at root and in subdirectories is flagged.

        This indicates an unclear catalog structure - is the root a single item
        with multiple files, or is each subdirectory a separate item?
        """
        from portolan_cli.scan import IssueType, Severity, scan_directory

        # Create files at root level
        (tmp_path / "root_data.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        # Create files in subdirectory
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (subdir / "nested_data.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        result = scan_directory(tmp_path)

        mixed_structure_issues = [
            i for i in result.issues if i.issue_type == IssueType.MIXED_FLAT_MULTIITEM
        ]
        assert len(mixed_structure_issues) >= 1
        assert mixed_structure_issues[0].severity == Severity.WARNING


# =============================================================================
# Phase 11: User Story 9 - Windows Reserved Names
# =============================================================================


@pytest.mark.unit
class TestWindowsReservedNames:
    """Tests for Windows reserved name detection (US9)."""

    def test_con_geojson_detected(self, tmp_path: Path) -> None:
        """Windows reserved name CON.geojson is flagged."""
        from portolan_cli.scan import IssueType, Severity, scan_directory

        (tmp_path / "CON.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        result = scan_directory(tmp_path)

        reserved_issues = [
            i for i in result.issues if i.issue_type == IssueType.WINDOWS_RESERVED_NAME
        ]
        assert len(reserved_issues) >= 1
        assert reserved_issues[0].severity == Severity.WARNING

    def test_prn_parquet_detected(self, tmp_path: Path) -> None:
        """Windows reserved name PRN.parquet is flagged."""
        from portolan_cli.scan import IssueType, scan_directory

        (tmp_path / "PRN.parquet").write_bytes(b"\x00" * 100)

        result = scan_directory(tmp_path)

        reserved_issues = [
            i for i in result.issues if i.issue_type == IssueType.WINDOWS_RESERVED_NAME
        ]
        assert len(reserved_issues) >= 1

    def test_nul_data_not_flagged(self, tmp_path: Path) -> None:
        """File containing reserved name (nul_data.parquet) is NOT flagged."""
        from portolan_cli.scan import IssueType, scan_directory

        # "nul_data" contains "nul" but is not exactly a reserved name
        (tmp_path / "nul_data.parquet").write_bytes(b"\x00" * 100)

        result = scan_directory(tmp_path)

        reserved_issues = [
            i for i in result.issues if i.issue_type == IssueType.WINDOWS_RESERVED_NAME
        ]
        assert len(reserved_issues) == 0

    def test_aux_directory_detected(self, tmp_path: Path) -> None:
        """Windows reserved name AUX as directory is flagged."""
        from portolan_cli.scan import IssueType, scan_directory

        aux_dir = tmp_path / "AUX"
        aux_dir.mkdir()
        (aux_dir / "data.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        result = scan_directory(tmp_path)

        reserved_issues = [
            i for i in result.issues if i.issue_type == IssueType.WINDOWS_RESERVED_NAME
        ]
        # Files inside a reserved-name directory should be flagged
        assert len(reserved_issues) >= 1


# =============================================================================
# Phase 5: User Story 3 - JSON Output
# =============================================================================


@pytest.mark.unit
class TestJsonOutput:
    """Tests for JSON output (US3)."""

    def test_to_dict_has_required_fields(self, fixtures_dir: Path) -> None:
        """ScanResult.to_dict contains all required fields."""
        from portolan_cli.scan import scan_directory

        result = scan_directory(fixtures_dir / "clean_flat")
        d = result.to_dict()

        assert "root" in d
        assert "summary" in d
        assert "ready" in d
        assert "issues" in d
        assert "skipped" in d

        assert "directories_scanned" in d["summary"]
        assert "ready_count" in d["summary"]
        assert "issue_count" in d["summary"]
        assert "skipped_count" in d["summary"]

    def test_to_dict_issues_have_required_fields(self, fixtures_dir: Path) -> None:
        """Each issue in to_dict has required fields."""
        from portolan_cli.scan import scan_directory

        result = scan_directory(fixtures_dir / "incomplete_shapefile")
        d = result.to_dict()

        for issue in d["issues"]:
            assert "path" in issue
            assert "relative_path" in issue
            assert "type" in issue
            assert "severity" in issue
            assert "message" in issue


# =============================================================================
# Phase 6: User Story 4 - Depth Control
# =============================================================================


@pytest.mark.unit
class TestDepthControl:
    """Tests for recursive depth control (US4)."""

    def test_no_recursive_scans_only_immediate(self, fixtures_dir: Path) -> None:
        """--no-recursive scans only immediate directory."""
        from portolan_cli.scan import ScanOptions, scan_directory

        opts = ScanOptions(recursive=False)
        result = scan_directory(fixtures_dir / "nested", opts)

        # nested/ has subdirectories, but non-recursive should only find root files
        # The nested fixture has files in nested/census/2020/, nested/census/2022/, nested/imagery/2024/
        # With non-recursive, we should find 0 files (all files are in subdirs)
        assert result.directories_scanned == 1

    def test_max_depth_zero_scans_only_target(self, fixtures_dir: Path) -> None:
        """--max-depth=0 scans only the target directory."""
        from portolan_cli.scan import ScanOptions, scan_directory

        opts = ScanOptions(max_depth=0)
        result = scan_directory(fixtures_dir / "nested", opts)

        # Same as non-recursive
        assert result.directories_scanned == 1

    def test_max_depth_limits_recursion(self, fixtures_dir: Path) -> None:
        """--max-depth=N limits recursion to N levels."""
        from portolan_cli.scan import ScanOptions, scan_directory

        # nested/ structure: nested/census/2020/boundaries.geojson (depth 3 from nested/)
        opts = ScanOptions(max_depth=1)
        result = scan_directory(fixtures_dir / "nested", opts)

        # At depth 1, we can see census/ and imagery/ but not their contents
        # Files are at depth 2+, so no files should be found
        assert len(result.ready) == 0

    def test_full_recursion_finds_all_files(self, fixtures_dir: Path) -> None:
        """Default recursion finds all files in nested structure."""
        from portolan_cli.scan import scan_directory

        result = scan_directory(fixtures_dir / "nested")

        # Should find all 3 files: census/2020/boundaries.geojson, census/2022/boundaries.geojson, imagery/2024/flood_depth.tif
        assert len(result.ready) == 3


# =============================================================================
# Phase 7: User Story 5 - Hidden and Symlink Handling
# =============================================================================


@pytest.mark.unit
class TestHiddenAndSymlinks:
    """Tests for hidden file and symlink handling (US5)."""

    def test_hidden_files_skipped_by_default(self, tmp_path: Path) -> None:
        """Hidden files are skipped by default."""
        from portolan_cli.scan import scan_directory

        # Create hidden and visible files (use .geojson since .parquet needs geo metadata)
        (tmp_path / ".hidden.geojson").write_text('{"type": "FeatureCollection", "features": []}')
        (tmp_path / "visible.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        result = scan_directory(tmp_path)

        # Only visible file should be found
        assert len(result.ready) == 1
        assert result.ready[0].basename == "visible.geojson"

    def test_include_hidden_includes_hidden_files(self, tmp_path: Path) -> None:
        """--include-hidden includes hidden files."""
        from portolan_cli.scan import ScanOptions, scan_directory

        # Create hidden and visible files (use .geojson since .parquet needs geo metadata)
        (tmp_path / ".hidden.geojson").write_text('{"type": "FeatureCollection", "features": []}')
        (tmp_path / "visible.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        opts = ScanOptions(include_hidden=True)
        result = scan_directory(tmp_path, opts)

        # Both files should be found
        assert len(result.ready) == 2

    def test_symlinks_skipped_by_default(self, tmp_path: Path) -> None:
        """Symlinks are skipped by default."""
        from portolan_cli.scan import scan_directory

        # Create a real file and a symlink (use .geojson since .parquet needs geo metadata)
        real_file = tmp_path / "real.geojson"
        real_file.write_text('{"type": "FeatureCollection", "features": []}')
        symlink = tmp_path / "link.geojson"
        symlink.symlink_to(real_file)

        result = scan_directory(tmp_path)

        # Only the real file should be found
        assert len(result.ready) == 1
        assert result.ready[0].basename == "real.geojson"

    def test_follow_symlinks_includes_symlinks(self, tmp_path: Path) -> None:
        """--follow-symlinks includes symlinked files."""
        from portolan_cli.scan import ScanOptions, scan_directory

        # Create a real file and a symlink (use .geojson since .parquet needs geo metadata)
        real_file = tmp_path / "real.geojson"
        real_file.write_text('{"type": "FeatureCollection", "features": []}')
        symlink = tmp_path / "link.geojson"
        symlink.symlink_to(real_file)

        opts = ScanOptions(follow_symlinks=True)
        result = scan_directory(tmp_path, opts)

        # Both should be found (or just the resolved one, depending on implementation)
        assert len(result.ready) >= 1

    def test_symlink_loop_detected(self, tmp_path: Path) -> None:
        """Symlink loops are detected and reported as errors."""
        from portolan_cli.scan import IssueType, ScanOptions, Severity, scan_directory

        # Create a symlink loop: dir_a -> dir_b -> dir_a
        dir_a = tmp_path / "dir_a"
        dir_b = tmp_path / "dir_b"
        dir_a.mkdir()
        dir_b.symlink_to(dir_a)
        (dir_a / "link_to_b").symlink_to(dir_b)

        opts = ScanOptions(follow_symlinks=True)
        result = scan_directory(tmp_path, opts)

        # Should detect the loop and report as error
        loop_issues = [i for i in result.issues if i.issue_type == IssueType.SYMLINK_LOOP]
        assert len(loop_issues) >= 1
        assert loop_issues[0].severity == Severity.ERROR


# =============================================================================
# GeoParquet vs Regular Parquet Detection
# =============================================================================


@pytest.mark.unit
class TestGeoParquetDetection:
    """Tests for distinguishing GeoParquet from regular Parquet files."""

    def test_geoparquet_file_is_primary_asset(self, tmp_path: Path) -> None:
        """GeoParquet files (with 'geo' metadata) are recognized as primary assets."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from portolan_cli.scan import FormatType, scan_directory

        # Create a minimal GeoParquet file with geo metadata
        table = pa.table({"name": ["test"], "value": [1]})
        geo_metadata = b'{"version": "1.0.0", "primary_column": "geometry", "columns": {}}'
        existing_meta = table.schema.metadata or {}
        new_meta = {**existing_meta, b"geo": geo_metadata}
        table = table.replace_schema_metadata(new_meta)

        parquet_path = tmp_path / "data.parquet"
        pq.write_table(table, parquet_path)

        result = scan_directory(tmp_path)

        # Should be recognized as a ready file
        assert len(result.ready) == 1
        assert result.ready[0].extension == ".parquet"
        assert result.ready[0].format_type == FormatType.VECTOR

    def test_regular_parquet_file_is_skipped(self, tmp_path: Path) -> None:
        """Regular Parquet files (without 'geo' metadata) are skipped."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from portolan_cli.scan import scan_directory

        # Create a regular Parquet file without geo metadata
        table = pa.table({"name": ["test"], "value": [1]})
        parquet_path = tmp_path / "census-data.parquet"
        pq.write_table(table, parquet_path)

        result = scan_directory(tmp_path)

        # Should NOT be in ready files
        assert len(result.ready) == 0
        # Should be in skipped files
        assert len(result.skipped) == 1
        assert result.skipped[0].path.name == "census-data.parquet"

    def test_mixed_parquet_types_only_geoparquet_ready(self, tmp_path: Path) -> None:
        """Directory with both GeoParquet and regular Parquet only has GeoParquet ready."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from portolan_cli.scan import scan_directory

        # Create GeoParquet file
        geo_table = pa.table({"name": ["test"]})
        geo_metadata = b'{"version": "1.0.0", "primary_column": "geometry", "columns": {}}'
        geo_table = geo_table.replace_schema_metadata({b"geo": geo_metadata})
        pq.write_table(geo_table, tmp_path / "radios.parquet")

        # Create regular Parquet files
        regular_table = pa.table({"census_id": [1], "population": [1000]})
        pq.write_table(regular_table, tmp_path / "census-data.parquet")
        pq.write_table(regular_table, tmp_path / "metadata.parquet")

        result = scan_directory(tmp_path)

        # Only the GeoParquet should be ready
        assert len(result.ready) == 1
        assert result.ready[0].basename == "radios.parquet"
        # The other two should be skipped
        assert len(result.skipped) == 2


# =============================================================================
# PMTiles as Overview Format
# =============================================================================


@pytest.mark.unit
class TestOverviewFormats:
    """Tests for overview/derivative format handling."""

    def test_pmtiles_files_are_skipped(self, tmp_path: Path) -> None:
        """PMTiles files are recognized as overviews and skipped (not primary assets)."""
        from portolan_cli.scan import scan_directory

        # Create a PMTiles file (just needs to exist for extension detection)
        pmtiles_path = tmp_path / "overview.pmtiles"
        pmtiles_path.write_bytes(b"fake pmtiles content")

        result = scan_directory(tmp_path)

        # Should NOT be in ready files
        assert len(result.ready) == 0
        # Should be in skipped files
        assert len(result.skipped) == 1
        assert result.skipped[0].path.name == "overview.pmtiles"

    def test_pmtiles_does_not_count_as_primary(self, tmp_path: Path) -> None:
        """PMTiles should not trigger 'multiple primaries' warning."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from portolan_cli.scan import IssueType, scan_directory

        # Create one GeoParquet and one PMTiles
        geo_table = pa.table({"name": ["test"]})
        geo_metadata = b'{"version": "1.0.0", "primary_column": "geometry", "columns": {}}'
        geo_table = geo_table.replace_schema_metadata({b"geo": geo_metadata})
        pq.write_table(geo_table, tmp_path / "radios.parquet")

        (tmp_path / "overview.pmtiles").write_bytes(b"fake pmtiles")

        result = scan_directory(tmp_path)

        # Only one primary asset (the GeoParquet)
        assert len(result.ready) == 1
        # No "multiple primaries" warning
        multi_issues = [i for i in result.issues if i.issue_type == IssueType.MULTIPLE_PRIMARIES]
        assert len(multi_issues) == 0


# =============================================================================
# FileGDB Directory Detection (US4)
# =============================================================================


@pytest.mark.unit
class TestFileGDBHandling:
    """Tests for FileGDB directory handling (US4).

    FileGDB (.gdb directories) are ESRI's file geodatabase format.
    They contain internal structure files that should not be scanned individually.
    The scan should:
    1. Detect .gdb directories as a single geospatial asset
    2. NOT walk into .gdb directories to enumerate internal files
    3. Report FILEGDB_DETECTED as an INFO issue for format awareness
    """

    def test_gdb_directory_not_walked_into(self, fixtures_dir: Path) -> None:
        """.gdb directories should be detected but not walked into.

        The internal .gdbtable, .gdbtablx, etc. files should NOT appear
        in the ready or skipped lists.
        """
        from portolan_cli.scan import scan_directory

        result = scan_directory(fixtures_dir / "filegdb")

        # Should NOT have any internal GDB files in ready or skipped
        all_paths = [f.path for f in result.ready] + [s.path for s in result.skipped]
        internal_files = [p for p in all_paths if ".gdbtable" in p.suffix or "gdbtablx" in p.suffix]

        assert len(internal_files) == 0, f"Found internal GDB files: {internal_files}"

    def test_gdb_directory_detected_as_filegdb(self, fixtures_dir: Path) -> None:
        """.gdb directories should emit FILEGDB_DETECTED info issue."""
        from portolan_cli.scan import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "filegdb")

        filegdb_issues = [i for i in result.issues if i.issue_type == IssueType.FILEGDB_DETECTED]
        # Should have 2 issues: one for sample.gdb directory, one for sample.gdb.zip
        assert len(filegdb_issues) == 2
        # Verify the directory is detected
        dir_issues = [i for i in filegdb_issues if str(i.path).endswith("sample.gdb")]
        assert len(dir_issues) == 1

    def test_gdb_zip_archive_recognized(self, fixtures_dir: Path) -> None:
        """.gdb.zip archives should also be detected as FileGDB.

        The scan should recognize compound extensions like `.gdb.zip` as
        archived FileGDB containers and handle them appropriately.
        """
        from portolan_cli.scan import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "filegdb")

        # The .gdb.zip file should emit a FILEGDB_DETECTED issue or be handled
        # Check if there's a FILEGDB_DETECTED for the zip file
        filegdb_zip_issues = [
            i
            for i in result.issues
            if i.issue_type == IssueType.FILEGDB_DETECTED and "sample.gdb.zip" in str(i.path)
        ]

        # Either it's detected as FileGDB OR it's skipped with a known category
        # (not UNKNOWN). For now, detecting it is sufficient.
        # If not detected, check that any skip is not categorized as "unknown"
        if not filegdb_zip_issues:
            unknown_skipped = [s for s in result.skipped if "sample.gdb.zip" in str(s.path)]
            for s in unknown_skipped:
                # If it's unknown, we need to add support for it
                assert s.category.value != "unknown", f"FileGDB zip should be recognized: {s}"


# =============================================================================
# Permission and Symlink Edge Cases (Issue #64)
# =============================================================================


@pytest.mark.unit
@pytest.mark.skipif(sys.platform == "win32", reason="Permission tests not supported on Windows")
class TestPermissionEdgeCases:
    """Tests for permission error handling during scan.

    These tests verify that scan emits warnings when encountering
    directories that cannot be scanned due to permission errors.

    Note: On Linux, os.scandir() can stat files with mode 000 as long as
    the parent directory has execute permission. Permission errors primarily
    occur when trying to list directory contents (no execute on directory).
    """

    def test_scan_no_execute_directory_emits_warning(self, tmp_path: Path) -> None:
        """Files in directories without execute permission emit warnings.

        Directory execute permission is required to stat files inside.
        On Linux, os.scandir may succeed (listing entries) but stat() fails.
        This tests that stat() failures are properly reported.
        """
        from portolan_cli.scan import IssueType, Severity, scan_directory

        # Create a subdirectory with a file, then remove execute permission
        subdir = tmp_path / "no_exec"
        subdir.mkdir()
        (subdir / "data.geojson").write_text('{"type": "FeatureCollection", "features": []}')
        os.chmod(subdir, 0o644)  # Read/write but no execute

        try:
            result = scan_directory(tmp_path)

            # Should have a permission denied issue
            # Note: On Linux, os.scandir succeeds but stat() fails on individual files
            perm_issues = [i for i in result.issues if i.issue_type == IssueType.PERMISSION_DENIED]
            assert len(perm_issues) >= 1
            assert perm_issues[0].severity == Severity.WARNING
            # Path contains either the directory or file path
            assert "no_exec" in str(perm_issues[0].path)
        finally:
            os.chmod(subdir, 0o755)

    def test_scan_stat_oserror_emits_warning(self, tmp_path: Path) -> None:
        """When entry.stat() raises OSError, emit a warning instead of silent skip.

        The stat() OSError behavior is tested via broken symlinks in
        TestBrokenSymlinkEdgeCases, where stat() fails because the symlink
        target doesn't exist. This test is kept as a marker for edge cases.
        """
        # stat() OSError handling is tested via broken symlinks:
        # - TestBrokenSymlinkEdgeCases.test_scan_broken_symlink_emits_warning
        # The current implementation detects broken symlinks specifically,
        # which covers the main stat() failure case.
        pytest.skip(
            "stat() OSError is tested via broken symlink tests in TestBrokenSymlinkEdgeCases"
        )


@pytest.mark.unit
@pytest.mark.skipif(sys.platform == "win32", reason="Symlink tests not supported on Windows")
class TestBrokenSymlinkEdgeCases:
    """Tests for broken symlink handling during scan.

    These tests verify that scan emits warnings when encountering
    broken/dangling symlinks (symlinks pointing to non-existent targets).

    Key insight: With follow_symlinks=True, a broken symlink has:
    - entry.is_symlink() = True
    - entry.is_file(follow_symlinks=True) = False (target doesn't exist)
    - entry.is_dir(follow_symlinks=True) = False
    - entry.stat(follow_symlinks=True) raises OSError

    The current code silently skips because is_file() returns False.
    We need to detect: is_symlink AND (not is_file AND not is_dir) = broken.
    """

    def test_scan_broken_symlink_emits_warning(self, tmp_path: Path) -> None:
        """A broken symlink emits a warning when follow_symlinks=True.

        Bug: Broken symlinks are silently skipped because entry.is_file()
        returns False for them, so they never enter the file processing branch.
        Expected: Should emit BROKEN_SYMLINK issue.
        """
        from portolan_cli.scan import IssueType, ScanOptions, Severity, scan_directory

        # Create a broken symlink (pointing to non-existent target)
        broken = tmp_path / "broken.geojson"
        broken.symlink_to(tmp_path / "nonexistent.geojson")

        # Also create a valid file so the directory isn't empty
        valid = tmp_path / "valid.geojson"
        valid.write_text('{"type": "FeatureCollection", "features": []}')

        opts = ScanOptions(follow_symlinks=True)
        result = scan_directory(tmp_path, opts)

        # Should have 1 ready file (the valid one)
        assert len(result.ready) == 1

        # Should have a broken symlink issue
        broken_issues = [i for i in result.issues if i.issue_type == IssueType.BROKEN_SYMLINK]
        assert len(broken_issues) == 1
        assert broken_issues[0].severity == Severity.WARNING
        assert "broken.geojson" in str(broken_issues[0].path)

    def test_scan_broken_symlink_continues_scanning(self, tmp_path: Path) -> None:
        """Scan continues processing after encountering broken symlink."""
        from portolan_cli.scan import IssueType, ScanOptions, scan_directory

        # Create broken symlink between valid files
        (tmp_path / "first.geojson").write_text('{"type": "FeatureCollection", "features": []}')
        broken = tmp_path / "middle.geojson"
        broken.symlink_to(tmp_path / "ghost.geojson")
        (tmp_path / "last.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        opts = ScanOptions(follow_symlinks=True)
        result = scan_directory(tmp_path, opts)

        # Should find both valid files
        assert len(result.ready) == 2
        basenames = {f.basename for f in result.ready}
        assert "first.geojson" in basenames
        assert "last.geojson" in basenames

        # Should also report the broken symlink
        broken_issues = [i for i in result.issues if i.issue_type == IssueType.BROKEN_SYMLINK]
        assert len(broken_issues) == 1

    def test_scan_valid_symlink_followed(self, tmp_path: Path) -> None:
        """Valid symlinks are processed when follow_symlinks=True.

        This is existing behavior - ensure it's preserved.
        """
        from portolan_cli.scan import ScanOptions, scan_directory

        # Create a real file and a valid symlink
        real = tmp_path / "real.geojson"
        real.write_text('{"type": "FeatureCollection", "features": []}')
        link = tmp_path / "link.geojson"
        link.symlink_to(real)

        opts = ScanOptions(follow_symlinks=True)
        result = scan_directory(tmp_path, opts)

        # Both should be found
        assert len(result.ready) == 2

    def test_scan_symlink_not_followed_by_default(self, tmp_path: Path) -> None:
        """Symlinks are not followed by default (existing behavior).

        When follow_symlinks=False, symlinks are simply skipped.
        No warning is needed because this is intentional.
        """
        from portolan_cli.scan import scan_directory

        real = tmp_path / "real.geojson"
        real.write_text('{"type": "FeatureCollection", "features": []}')
        link = tmp_path / "link.geojson"
        link.symlink_to(real)

        result = scan_directory(tmp_path)

        # Only the real file should be found
        assert len(result.ready) == 1
        assert result.ready[0].basename == "real.geojson"

    def test_scan_broken_symlink_ignored_when_not_following(self, tmp_path: Path) -> None:
        """Broken symlinks are silently skipped when follow_symlinks=False.

        This is intentional - if user doesn't want to follow symlinks,
        we don't need to warn about broken ones.
        """
        from portolan_cli.scan import IssueType, scan_directory

        # Create a broken symlink
        broken = tmp_path / "broken.geojson"
        broken.symlink_to(tmp_path / "nonexistent.geojson")

        # Create a valid file
        valid = tmp_path / "valid.geojson"
        valid.write_text('{"type": "FeatureCollection", "features": []}')

        result = scan_directory(tmp_path)  # Default: follow_symlinks=False

        # Should have 1 ready file (the valid one)
        assert len(result.ready) == 1

        # Should NOT have a broken symlink issue (we're not following symlinks)
        broken_issues = [i for i in result.issues if i.issue_type == IssueType.BROKEN_SYMLINK]
        assert len(broken_issues) == 0

    def test_scan_broken_symlink_shows_target_in_message(self, tmp_path: Path) -> None:
        """Broken symlink warning message includes the target path."""
        from portolan_cli.scan import IssueType, ScanOptions, scan_directory

        broken = tmp_path / "broken.geojson"
        target = tmp_path / "ghost_target.geojson"
        broken.symlink_to(target)

        opts = ScanOptions(follow_symlinks=True)
        result = scan_directory(tmp_path, opts)

        broken_issues = [i for i in result.issues if i.issue_type == IssueType.BROKEN_SYMLINK]
        assert len(broken_issues) == 1
        # Message should mention it's broken/dangling
        assert (
            "broken" in broken_issues[0].message.lower()
            or "dangling" in broken_issues[0].message.lower()
        )

    def test_scan_symlink_chain_resolved(self, tmp_path: Path) -> None:
        """Deep symlink chains are followed correctly."""
        from portolan_cli.scan import ScanOptions, scan_directory

        # Create a chain: link_a -> link_b -> link_c -> actual.geojson
        actual = tmp_path / "actual.geojson"
        actual.write_text('{"type": "FeatureCollection", "features": []}')

        link_c = tmp_path / "link_c.geojson"
        link_c.symlink_to(actual)

        link_b = tmp_path / "link_b.geojson"
        link_b.symlink_to(link_c)

        link_a = tmp_path / "link_a.geojson"
        link_a.symlink_to(link_b)

        opts = ScanOptions(follow_symlinks=True)
        result = scan_directory(tmp_path, opts)

        # All four should be found (including the chain)
        assert len(result.ready) == 4

    def test_scan_broken_symlink_in_report(self, tmp_path: Path) -> None:
        """Broken symlink issues appear in the final scan report."""
        from portolan_cli.scan import ScanOptions, scan_directory

        broken = tmp_path / "broken.geojson"
        broken.symlink_to(tmp_path / "nonexistent.geojson")

        opts = ScanOptions(follow_symlinks=True)
        result = scan_directory(tmp_path, opts)
        report = result.to_dict()

        # Should have issues in the report
        assert report["summary"]["issue_count"] >= 1
        broken_issues = [i for i in report["issues"] if i["type"] == "broken_symlink"]
        assert len(broken_issues) == 1
