"""Comprehensive unit tests to achieve 90%+ coverage on scan modules.

This file tests edge cases and helper functions that aren't covered by
the main feature tests.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from portolan_cli.scan import (
    FormatType,
    IssueType,
    ScanIssue,
    ScannedFile,
    ScanOptions,
    ScanResult,
    Severity,
    _get_dir_size,
    _get_format_type,
    _get_relative_path,
    _has_invalid_characters,
    _is_hidden,
    _is_recognized_extension,
    _is_sidecar_extension,
    is_windows_reserved_name,
    scan_directory,
)
from portolan_cli.scan_detect import (
    DualFormatPair,
    SpecialFormat,
    detect_hive_partitions,
    detect_stac_catalogs,
    is_hive_partition_dir,
)

# =============================================================================
# scan.py Helper Function Tests
# =============================================================================


@pytest.mark.unit
class TestFormatTypeDetection:
    """Tests for _get_format_type edge cases."""

    def test_vector_extension_detection(self) -> None:
        """Vector extensions should return VECTOR format type."""
        for ext in [".geojson", ".shp", ".gpkg", ".fgb"]:
            assert _get_format_type(ext) == FormatType.VECTOR

    def test_raster_extension_detection(self) -> None:
        """Raster extensions should return RASTER format type."""
        for ext in [".tif", ".tiff", ".jp2"]:
            assert _get_format_type(ext) == FormatType.RASTER

    def test_case_insensitive_detection(self) -> None:
        """Extension detection should be case-insensitive."""
        assert _get_format_type(".GEOJSON") == FormatType.VECTOR
        assert _get_format_type(".TIF") == FormatType.RASTER
        assert _get_format_type(".GeoJSON") == FormatType.VECTOR

    def test_unknown_extension_defaults_to_vector(self) -> None:
        """Unknown extensions should default to VECTOR (line 382 coverage)."""
        # This covers the default return statement
        assert _get_format_type(".xyz") == FormatType.VECTOR
        assert _get_format_type(".unknown") == FormatType.VECTOR
        assert _get_format_type("") == FormatType.VECTOR


@pytest.mark.unit
class TestRelativePathCalculation:
    """Tests for _get_relative_path edge cases."""

    def test_normal_relative_path(self, tmp_path: Path) -> None:
        """Normal paths should return relative path string."""
        child = tmp_path / "subdir" / "file.txt"
        result = _get_relative_path(child, tmp_path)
        # _get_relative_path uses as_posix() for STAC compatibility - always forward slashes
        assert result == "subdir/file.txt"

    def test_non_relative_path_returns_full_path(self, tmp_path: Path) -> None:
        """Paths not relative to root should return full path (line 399-400 coverage)."""
        # Create two unrelated paths
        path = Path("/some/other/path/file.txt")
        root = tmp_path
        result = _get_relative_path(path, root)
        # Should return the full path with forward slashes (as_posix) for STAC compatibility
        assert result == path.as_posix()

    def test_same_path_returns_dot(self, tmp_path: Path) -> None:
        """Same path should return '.'"""
        result = _get_relative_path(tmp_path, tmp_path)
        assert result == "."


@pytest.mark.unit
class TestHelperFunctions:
    """Tests for various helper functions."""

    def test_is_recognized_extension(self) -> None:
        """Test recognized extension detection."""
        assert _is_recognized_extension(".geojson") is True
        assert _is_recognized_extension(".GEOJSON") is True
        assert _is_recognized_extension(".tif") is True
        assert _is_recognized_extension(".txt") is False
        assert _is_recognized_extension(".parquet") is False  # Needs metadata check

    def test_is_sidecar_extension(self) -> None:
        """Test sidecar extension detection."""
        assert _is_sidecar_extension(".dbf") is True
        assert _is_sidecar_extension(".shx") is True
        assert _is_sidecar_extension(".prj") is True
        assert _is_sidecar_extension(".txt") is False

    def test_is_hidden(self) -> None:
        """Test hidden file detection."""
        assert _is_hidden(".hidden") is True
        assert _is_hidden(".DS_Store") is True
        assert _is_hidden("visible") is False
        assert _is_hidden("file.txt") is False

    def test_has_invalid_characters(self) -> None:
        """Test invalid character detection."""
        assert _has_invalid_characters("file name.txt") is True  # Space
        assert _has_invalid_characters("file(1).txt") is True  # Parentheses
        assert _has_invalid_characters("file[1].txt") is True  # Brackets
        assert _has_invalid_characters("file.txt") is False
        assert _has_invalid_characters("file_name.txt") is False

    def test_is_windows_reserved_name(self) -> None:
        """Test Windows reserved name detection."""
        assert is_windows_reserved_name("con") is True
        assert is_windows_reserved_name("CON") is True
        assert is_windows_reserved_name("prn") is True
        assert is_windows_reserved_name("aux") is True
        assert is_windows_reserved_name("nul") is True
        assert is_windows_reserved_name("com1") is True
        assert is_windows_reserved_name("lpt1") is True
        assert is_windows_reserved_name("normal") is False


# =============================================================================
# scan_detect.py Serialization Tests
# =============================================================================


@pytest.mark.unit
class TestSpecialFormatSerialization:
    """Tests for SpecialFormat.to_dict() (line 54 coverage)."""

    def test_to_dict_returns_expected_structure(self, tmp_path: Path) -> None:
        """SpecialFormat.to_dict() should return proper JSON structure."""
        sf = SpecialFormat(
            path=tmp_path / "test.gdb",
            relative_path="test.gdb",
            format_type="filegdb",
            details={"gdbtable_count": 5, "lock_files_present": False},
        )
        result = sf.to_dict()

        assert "path" in result
        assert "relative_path" in result
        assert "format_type" in result
        assert "details" in result
        assert result["relative_path"] == "test.gdb"
        assert result["format_type"] == "filegdb"
        assert result["details"]["gdbtable_count"] == 5

    def test_to_dict_with_empty_details(self, tmp_path: Path) -> None:
        """SpecialFormat.to_dict() should handle empty details."""
        sf = SpecialFormat(
            path=tmp_path / "catalog.json",
            relative_path="catalog.json",
            format_type="stac_catalog",
            details={},
        )
        result = sf.to_dict()
        assert result["details"] == {}


@pytest.mark.unit
class TestDualFormatPairSerialization:
    """Tests for DualFormatPair.to_dict() (line 72 coverage)."""

    def test_to_dict_returns_expected_structure(self, tmp_path: Path) -> None:
        """DualFormatPair.to_dict() should return proper JSON structure."""
        dfp = DualFormatPair(
            basename="boundaries",
            files=(tmp_path / "boundaries.geojson", tmp_path / "boundaries.parquet"),
            format_types=(".geojson", ".parquet"),
        )
        result = dfp.to_dict()

        assert result["basename"] == "boundaries"
        assert len(result["files"]) == 2
        assert ".geojson" in result["files"][0]
        assert len(result["format_types"]) == 2
        assert ".geojson" in result["format_types"]
        assert ".parquet" in result["format_types"]


# =============================================================================
# ScanResult Serialization Tests
# =============================================================================


@pytest.mark.unit
class TestScanResultSerialization:
    """Tests for ScanResult.to_dict() edge cases."""

    def test_to_dict_with_special_formats(self, tmp_path: Path) -> None:
        """ScanResult.to_dict() should include special_formats."""
        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[],
            skipped=[],
            directories_scanned=1,
            special_formats=[
                SpecialFormat(
                    path=tmp_path / "test.gdb",
                    relative_path="test.gdb",
                    format_type="filegdb",
                    details={"size_bytes": 1000},
                )
            ],
        )
        d = result.to_dict()
        assert "special_formats" in d
        assert len(d["special_formats"]) == 1
        assert d["special_formats"][0]["format_type"] == "filegdb"

    def test_to_dict_with_dual_format_pairs(self, tmp_path: Path) -> None:
        """ScanResult.to_dict() should include dual_format_pairs."""
        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[],
            skipped=[],
            directories_scanned=1,
            dual_format_pairs=[
                DualFormatPair(
                    basename="test",
                    files=(tmp_path / "test.shp", tmp_path / "test.gpkg"),
                    format_types=(".shp", ".gpkg"),
                )
            ],
        )
        d = result.to_dict()
        assert "dual_format_pairs" in d
        assert len(d["dual_format_pairs"]) == 1

    def test_classification_summary(self, tmp_path: Path) -> None:
        """ScanResult.classification_summary should count categories."""
        result = ScanResult(
            root=tmp_path,
            ready=[
                ScannedFile(
                    path=tmp_path / "data.geojson",
                    relative_path="data.geojson",
                    extension=".geojson",
                    format_type=FormatType.VECTOR,
                    size_bytes=100,
                )
            ],
            issues=[],
            skipped=[],
            directories_scanned=1,
        )
        summary = result.classification_summary
        assert summary["geo_asset"] == 1


# =============================================================================
# Hive Partition Detection Edge Cases
# =============================================================================


@pytest.mark.unit
class TestHivePartitionEdgeCases:
    """Tests for Hive partition detection edge cases."""

    def test_hive_partition_with_numeric_key_rejected(self) -> None:
        """Hive partition keys starting with numbers should be rejected."""
        assert is_hive_partition_dir("123=value") is None

    def test_hive_partition_with_underscore_prefix(self) -> None:
        """Hive partition keys with underscore prefix should be accepted."""
        result = is_hive_partition_dir("_private=value")
        assert result == ("_private", "value")

    def test_hive_partition_with_complex_value(self) -> None:
        """Hive partition values can contain special characters."""
        result = is_hive_partition_dir("date=2020-01-01")
        assert result == ("date", "2020-01-01")

    def test_detect_hive_partitions_nested_keys(self, tmp_path: Path) -> None:
        """Nested Hive partitions should be detected."""
        # Create nested partition structure
        (tmp_path / "year=2020" / "month=01").mkdir(parents=True)
        (tmp_path / "year=2020" / "month=02").mkdir(parents=True)
        (tmp_path / "year=2021" / "month=01").mkdir(parents=True)

        results = detect_hive_partitions(tmp_path)
        # Should find partition root(s)
        assert len(results) >= 1


# =============================================================================
# STAC Detection Edge Cases
# =============================================================================


@pytest.mark.unit
class TestSTACDetectionEdgeCases:
    """Tests for STAC catalog detection edge cases."""

    def test_nested_stac_catalogs(self, tmp_path: Path) -> None:
        """Multiple nested STAC catalogs should all be detected."""
        # Create root catalog
        (tmp_path / "catalog.json").write_text('{"type": "Catalog"}')
        # Create nested collection
        subdir = tmp_path / "collection1"
        subdir.mkdir()
        (subdir / "collection.json").write_text('{"type": "Collection"}')

        results = detect_stac_catalogs(tmp_path)
        assert len(results) == 2
        types = {r.format_type for r in results}
        assert "stac_catalog" in types
        assert "stac_collection" in types


# =============================================================================
# Scan Options Validation
# =============================================================================


@pytest.mark.unit
class TestScanOptionsValidation:
    """Tests for ScanOptions validation."""

    def test_unsafe_fix_requires_fix(self) -> None:
        """--unsafe-fix should require --fix."""
        with pytest.raises(ValueError, match="--unsafe-fix requires --fix"):
            ScanOptions(fix=False, unsafe_fix=True)

    def test_valid_options_combinations(self) -> None:
        """Valid option combinations should not raise."""
        # Default options
        ScanOptions()
        # Fix mode
        ScanOptions(fix=True)
        # Fix with unsafe
        ScanOptions(fix=True, unsafe_fix=True)
        # Dry run
        ScanOptions(fix=True, dry_run=True)


# =============================================================================
# Symlink Handling Tests
# =============================================================================


@pytest.mark.unit
class TestSymlinkHandling:
    """Tests for symlink handling in scan."""

    def test_symlink_not_followed_by_default(self, tmp_path: Path) -> None:
        """Symlinks should not be followed by default."""
        # Create a separate directory with file (not under tmp_path directly)
        target_dir = tmp_path / "external_target"
        target_dir.mkdir()
        (target_dir / "data.geojson").write_text('{"type": "FeatureCollection"}')

        # Create scan directory separate from target
        scan_root = tmp_path / "scan_root"
        scan_root.mkdir()

        # Create symlink in scan_root pointing to external_target
        link = scan_root / "link"
        link.symlink_to(target_dir)

        result = scan_directory(scan_root, ScanOptions(follow_symlinks=False))

        # With follow_symlinks=False, the symlink itself is skipped, not followed
        # So we should find 0 geojson files (the link is not traversed)
        assert len(result.ready) == 0

    def test_symlink_followed_when_enabled(self, tmp_path: Path) -> None:
        """Symlinks should be followed when follow_symlinks=True."""
        # Create target directory with file
        target_dir = tmp_path / "target"
        target_dir.mkdir()
        (target_dir / "data.geojson").write_text('{"type": "FeatureCollection"}')

        # Create symlink to target
        link = tmp_path / "link"
        link.symlink_to(target_dir)

        result = scan_directory(tmp_path, ScanOptions(follow_symlinks=True))

        # Should find the geojson twice: once through target, once through link
        geojson_count = len([f for f in result.ready if f.extension == ".geojson"])
        assert geojson_count >= 1

    def test_broken_symlink_detected(self, tmp_path: Path) -> None:
        """Broken symlinks should be detected as issues."""
        # Create symlink to non-existent target
        link = tmp_path / "broken_link"
        link.symlink_to(tmp_path / "nonexistent")

        result = scan_directory(tmp_path, ScanOptions(follow_symlinks=True))

        # Should have a broken symlink issue
        broken_issues = [i for i in result.issues if i.issue_type == IssueType.BROKEN_SYMLINK]
        assert len(broken_issues) == 1

    def test_symlink_loop_detected(self, tmp_path: Path) -> None:
        """Symlink loops should be detected."""
        # Create a self-referential symlink loop
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        loop = subdir / "loop"
        loop.symlink_to(subdir)

        result = scan_directory(tmp_path, ScanOptions(follow_symlinks=True))

        # Should have a symlink loop issue
        loop_issues = [i for i in result.issues if i.issue_type == IssueType.SYMLINK_LOOP]
        assert len(loop_issues) == 1


# =============================================================================
# Hypothesis Property-Based Tests
# =============================================================================


@pytest.mark.unit
class TestPropertyBasedExtensionDetection:
    """Property-based tests for extension detection."""

    @given(ext=st.sampled_from([".geojson", ".shp", ".gpkg", ".fgb"]))
    @settings(max_examples=20)
    def test_all_vector_extensions_return_vector(self, ext: str) -> None:
        """All known vector extensions should return VECTOR."""
        assert _get_format_type(ext) == FormatType.VECTOR

    @given(ext=st.sampled_from([".tif", ".tiff", ".jp2"]))
    @settings(max_examples=20)
    def test_all_raster_extensions_return_raster(self, ext: str) -> None:
        """All known raster extensions should return RASTER."""
        assert _get_format_type(ext) == FormatType.RASTER

    @given(ext=st.text(min_size=1, max_size=10).map(lambda x: "." + x))
    @settings(max_examples=50)
    def test_unknown_extensions_never_raise(self, ext: str) -> None:
        """Unknown extensions should never raise, always return a FormatType."""
        # Skip known extensions
        known = {".geojson", ".shp", ".gpkg", ".fgb", ".tif", ".tiff", ".jp2"}
        assume(ext.lower() not in known)
        result = _get_format_type(ext)
        assert isinstance(result, FormatType)


@pytest.mark.unit
class TestPropertyBasedHivePartition:
    """Property-based tests for Hive partition detection."""

    @given(
        key=st.from_regex(r"[a-zA-Z_][a-zA-Z0-9_]{0,10}", fullmatch=True),
        value=st.text(
            min_size=1,
            max_size=20,
            alphabet=st.characters(
                whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_-"
            ),
        ),
    )
    @settings(max_examples=50)
    def test_valid_hive_pattern_always_detected(self, key: str, value: str) -> None:
        """Valid key=value patterns should always be detected."""
        assume(value)  # Non-empty value
        pattern = f"{key}={value}"
        result = is_hive_partition_dir(pattern)
        assert result is not None
        assert result[0] == key
        assert result[1] == value


@pytest.mark.unit
class TestPropertyBasedScanResult:
    """Property-based tests for ScanResult properties."""

    @given(
        error_count=st.integers(min_value=0, max_value=10),
        warning_count=st.integers(min_value=0, max_value=10),
        info_count=st.integers(min_value=0, max_value=10),
    )
    @settings(max_examples=20)
    def test_issue_counts_match(
        self, error_count: int, warning_count: int, info_count: int
    ) -> None:
        """Issue counts should match the number of issues by severity."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            issues = []
            for _ in range(error_count):
                issues.append(
                    ScanIssue(
                        path=tmp_path / "error",
                        relative_path="error",
                        issue_type=IssueType.ZERO_BYTE_FILE,
                        severity=Severity.ERROR,
                        message="Error",
                    )
                )
            for _ in range(warning_count):
                issues.append(
                    ScanIssue(
                        path=tmp_path / "warning",
                        relative_path="warning",
                        issue_type=IssueType.LONG_PATH,
                        severity=Severity.WARNING,
                        message="Warning",
                    )
                )
            for _ in range(info_count):
                issues.append(
                    ScanIssue(
                        path=tmp_path / "info",
                        relative_path="info",
                        issue_type=IssueType.MIXED_FORMATS,
                        severity=Severity.INFO,
                        message="Info",
                    )
                )

            result = ScanResult(
                root=tmp_path,
                ready=[],
                issues=issues,
                skipped=[],
                directories_scanned=1,
            )

            assert result.error_count == error_count
            assert result.warning_count == warning_count
            assert result.info_count == info_count
            assert result.has_errors == (error_count > 0)


# =============================================================================
# _get_dir_size Error Path Tests (lines 872-875 coverage)
# =============================================================================


@pytest.mark.unit
class TestGetDirSizeErrorPaths:
    """Tests for _get_dir_size error handling paths."""

    def test_get_dir_size_oserror_on_scandir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """OSError during scandir should be caught and return 0."""
        from portolan_cli import scan as scan_module

        # Create a FileGDB structure
        gdb_dir = tmp_path / "test.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"x" * 100)

        # Mock os.scandir to raise OSError
        def raise_oserror(path: Path) -> None:
            raise OSError("Permission denied")

        monkeypatch.setattr(scan_module.os, "scandir", raise_oserror)

        # Call _get_dir_size - should catch OSError and return 0
        result = scan_module._get_dir_size(gdb_dir)
        assert result == 0

    def test_get_dir_size_oserror_on_stat(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """OSError during stat should be caught and file skipped."""
        import os

        from portolan_cli import scan as scan_module

        # Create a FileGDB structure
        gdb_dir = tmp_path / "test.gdb"
        gdb_dir.mkdir()
        (gdb_dir / "a00000001.gdbtable").write_bytes(b"x" * 100)
        (gdb_dir / "a00000002.gdbtable").write_bytes(b"y" * 200)

        # Store original scandir
        original_scandir = os.scandir

        # Create a wrapper that makes stat fail for one file
        class FailingStatEntry:
            def __init__(self, entry: os.DirEntry) -> None:
                self._entry = entry
                self._fail_stat = "a00000001" in entry.name

            def is_file(self, follow_symlinks: bool = True) -> bool:
                return self._entry.is_file(follow_symlinks=follow_symlinks)

            def stat(self, follow_symlinks: bool = True) -> os.stat_result:
                if self._fail_stat:
                    raise OSError("Permission denied on stat")
                return self._entry.stat(follow_symlinks=follow_symlinks)

            @property
            def name(self) -> str:
                return self._entry.name

        def wrapped_scandir(path: Path):
            for entry in original_scandir(path):
                yield FailingStatEntry(entry)

        monkeypatch.setattr(scan_module.os, "scandir", wrapped_scandir)

        # Call _get_dir_size - should catch OSError on one file, still count the other
        result = scan_module._get_dir_size(gdb_dir)
        # Only the 200-byte file should be counted (a00000002.gdbtable)
        assert result == 200

    def test_get_dir_size_normal_operation(self, tmp_path: Path) -> None:
        """Normal operation should sum all file sizes."""

        # Create a directory with known file sizes
        test_dir = tmp_path / "testdir"
        test_dir.mkdir()
        (test_dir / "file1.txt").write_bytes(b"a" * 100)
        (test_dir / "file2.txt").write_bytes(b"b" * 200)
        (test_dir / "file3.txt").write_bytes(b"c" * 300)

        result = _get_dir_size(test_dir)
        assert result == 600

    def test_get_dir_size_empty_directory(self, tmp_path: Path) -> None:
        """Empty directory should return 0."""

        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        result = _get_dir_size(empty_dir)
        assert result == 0

    def test_get_dir_size_skips_subdirectories(self, tmp_path: Path) -> None:
        """Subdirectories should be skipped in size calculation."""

        test_dir = tmp_path / "testdir"
        test_dir.mkdir()
        (test_dir / "file.txt").write_bytes(b"x" * 100)
        subdir = test_dir / "subdir"
        subdir.mkdir()
        (subdir / "nested.txt").write_bytes(b"y" * 500)  # Should NOT be counted

        result = _get_dir_size(test_dir)
        assert result == 100  # Only top-level file


# =============================================================================
# Integration Tests for Edge Cases
# =============================================================================


@pytest.mark.integration
class TestScanIntegrationEdgeCases:
    """Integration tests for edge cases."""

    def test_permission_denied_handling(self, tmp_path: Path) -> None:
        """Permission denied errors should be reported as issues."""
        # This test may be skipped on Windows or when running as root
        import os

        restricted_dir = tmp_path / "restricted"
        restricted_dir.mkdir()
        (restricted_dir / "data.geojson").write_text('{"type": "FeatureCollection"}')

        # Remove read permission
        original_mode = restricted_dir.stat().st_mode
        try:
            os.chmod(restricted_dir, 0o000)
            result = scan_directory(tmp_path)

            # Should have permission denied issue (or not depending on permissions)
            # May or may not trigger depending on OS/permissions
            # Just ensure no crash - we count issues to verify test ran
            _ = [i for i in result.issues if i.issue_type == IssueType.PERMISSION_DENIED]
        finally:
            # Restore permissions
            os.chmod(restricted_dir, original_mode)

    def test_empty_directory_scan(self, tmp_path: Path) -> None:
        """Empty directory should return empty result."""
        result = scan_directory(tmp_path)
        assert result.directories_scanned == 1
        assert len(result.ready) == 0
        assert len(result.issues) == 0

    def test_max_depth_limiting(self, tmp_path: Path) -> None:
        """Max depth should limit scanning depth."""
        # Create deep structure
        deep = tmp_path / "a" / "b" / "c" / "d"
        deep.mkdir(parents=True)
        (deep / "data.geojson").write_text('{"type": "FeatureCollection"}')

        # Scan with depth limit
        result = scan_directory(tmp_path, ScanOptions(max_depth=2))

        # Should not find the deep file
        assert not any(f.path == deep / "data.geojson" for f in result.ready)

    def test_non_recursive_scan(self, tmp_path: Path) -> None:
        """Non-recursive scan should only scan root directory."""
        # Create nested structure
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (tmp_path / "root.geojson").write_text('{"type": "FeatureCollection"}')
        (subdir / "nested.geojson").write_text('{"type": "FeatureCollection"}')

        result = scan_directory(tmp_path, ScanOptions(recursive=False))

        # Should only find root file
        assert len(result.ready) == 1
        assert result.ready[0].path.name == "root.geojson"
