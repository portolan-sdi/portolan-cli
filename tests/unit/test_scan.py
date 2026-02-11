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

    def test_issue_type_has_all_eight_types(self) -> None:
        """IssueType enum has all 8 issue types from spec."""
        from portolan_cli.scan import IssueType

        assert IssueType.INCOMPLETE_SHAPEFILE.value == "incomplete_shapefile"
        assert IssueType.ZERO_BYTE_FILE.value == "zero_byte_file"
        assert IssueType.SYMLINK_LOOP.value == "symlink_loop"
        assert IssueType.INVALID_CHARACTERS.value == "invalid_characters"
        assert IssueType.MULTIPLE_PRIMARIES.value == "multiple_primaries"
        assert IssueType.LONG_PATH.value == "long_path"
        assert IssueType.DUPLICATE_BASENAME.value == "duplicate_basename"
        assert IssueType.MIXED_FORMATS.value == "mixed_formats"

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
        assert opts.recursive is True
        assert opts.max_depth is None
        assert opts.include_hidden is False
        assert opts.follow_symlinks is False

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
        triggers a "multiple primaries" warning per FR-014. This is expected
        behavior, not an error in the scan.
        """
        from portolan_cli.scan import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "clean_flat")

        assert len(result.ready) == 3
        assert result.error_count == 0
        # Multiple primaries warning is expected: 3 files in same directory
        assert result.warning_count == 1
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
        assert multi_issues[0].severity == Severity.WARNING

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
        assert result.skipped[0].name == "census-data.parquet"

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
        assert result.skipped[0].name == "overview.pmtiles"

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
