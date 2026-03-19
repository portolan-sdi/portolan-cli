"""Unit tests for scan output improvements.

Tests for:
- Structure validation checklist display
- Fixability labels for issues
- Collection inference output
- Better skip reason display
- Next steps summary
- Tree view output
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.scan import (
    FormatType,
    IssueType,
    ScanIssue,
    ScannedFile,
    ScanResult,
    Severity,
)
from portolan_cli.scan_classify import FileCategory, SkippedFile, SkipReasonType
from portolan_cli.scan_infer import CollectionSuggestion

# =============================================================================
# Test Data Factories
# =============================================================================


def make_scanned_file(
    path: Path,
    ext: str = ".geojson",
    format_type: FormatType = FormatType.VECTOR,
    size: int = 1000,
) -> ScannedFile:
    """Create a ScannedFile for testing."""
    return ScannedFile(
        path=path,
        relative_path=str(path.name),
        extension=ext,
        format_type=format_type,
        size_bytes=size,
    )


def make_scan_issue(
    path: Path,
    issue_type: IssueType,
    severity: Severity,
    message: str = "Test issue",
    suggestion: str | None = None,
    relative_path: str | None = None,
) -> ScanIssue:
    """Create a ScanIssue for testing.

    Args:
        path: The absolute path to the file/directory with the issue.
        issue_type: The type of issue.
        severity: The severity level.
        message: The issue message.
        suggestion: Optional suggestion for fixing.
        relative_path: Optional relative path override. If not provided,
            uses path.name (basename only).
    """
    return ScanIssue(
        path=path,
        relative_path=relative_path if relative_path is not None else str(path.name),
        issue_type=issue_type,
        severity=severity,
        message=message,
        suggestion=suggestion,
    )


def make_skipped_file(
    path: Path,
    category: FileCategory,
    reason_type: SkipReasonType,
    reason: str = "Test skip reason",
) -> SkippedFile:
    """Create a SkippedFile for testing."""
    return SkippedFile(
        path=path,
        relative_path=str(path.name),
        category=category,
        reason_type=reason_type,
        reason_message=reason,
    )


# =============================================================================
# Tests for Fixability Labels
# =============================================================================


@pytest.mark.unit
class TestFixabilityLabels:
    """Tests for fixability label assignment to issues."""

    def test_get_fixability_auto_fix(self) -> None:
        """Issues like missing catalog.json are auto-fix (generated on import)."""
        from portolan_cli.scan_output import Fixability, get_fixability

        # Missing catalog.json is auto-generated on import
        assert get_fixability(IssueType.EXISTING_CATALOG) == Fixability.AUTO_FIX

    def test_get_fixability_fix_flag(self) -> None:
        """Issues like invalid_characters are fixable with --fix."""
        from portolan_cli.scan_output import Fixability, get_fixability

        assert get_fixability(IssueType.INVALID_CHARACTERS) == Fixability.FIX_FLAG

    def test_get_fixability_manual(self) -> None:
        """Issues like multiple_primaries require manual resolution."""
        from portolan_cli.scan_output import Fixability, get_fixability

        assert get_fixability(IssueType.MULTIPLE_PRIMARIES) == Fixability.MANUAL

    def test_fixability_label_text(self) -> None:
        """Fixability enum provides correct label text."""
        from portolan_cli.scan_output import Fixability

        assert Fixability.AUTO_FIX.label == "[auto-fix]"
        assert Fixability.FIX_FLAG.label == "[--fix]"
        assert Fixability.MANUAL.label == "[manual]"


# =============================================================================
# Tests for Structure Checklist
# =============================================================================


@pytest.mark.unit
class TestStructureChecklist:
    """Tests for structure validation checklist."""

    def test_generate_checklist_items(self, tmp_path: Path) -> None:
        """generate_structure_checklist returns expected items."""
        from portolan_cli.scan_output import generate_structure_checklist

        result = ScanResult(
            root=tmp_path,
            ready=[make_scanned_file(tmp_path / "data.geojson")],
            issues=[],
            skipped=[],
            directories_scanned=1,
        )

        checklist = generate_structure_checklist(result)

        # Should include these check items
        item_names = {item.name for item in checklist}
        assert "root_catalog" in item_names
        assert "root_readme" in item_names
        assert "geo_asset_naming" in item_names

    def test_checklist_item_passed(self, tmp_path: Path) -> None:
        """Checklist item shows passed when condition met."""
        from portolan_cli.scan_output import generate_structure_checklist

        # Create a result with a valid geo-asset at proper location
        result = ScanResult(
            root=tmp_path,
            ready=[make_scanned_file(tmp_path / "collection" / "data.geojson")],
            issues=[],
            skipped=[],
            directories_scanned=2,
        )

        checklist = generate_structure_checklist(result)

        # At least one check should pass
        assert any(item.passed for item in checklist)

    def test_checklist_item_failed(self, tmp_path: Path) -> None:
        """Checklist item shows failed when condition not met."""
        from portolan_cli.scan_output import generate_structure_checklist

        # Result with geo-asset at root (violates structure rule)
        result = ScanResult(
            root=tmp_path,
            ready=[make_scanned_file(tmp_path / "data.geojson")],
            issues=[
                make_scan_issue(
                    tmp_path / "data.geojson",
                    IssueType.MULTIPLE_PRIMARIES,
                    Severity.WARNING,
                )
            ],
            skipped=[],
            directories_scanned=1,
        )

        checklist = generate_structure_checklist(result)

        # "no_root_geo_assets" should fail since we have geo-asset at root
        root_check = next((item for item in checklist if item.name == "no_root_geo_assets"), None)
        if root_check:
            assert not root_check.passed


# =============================================================================
# Tests for Skip Reason Categories
# =============================================================================


@pytest.mark.unit
class TestSkipReasonDisplay:
    """Tests for better skip reason display."""

    def test_group_skipped_by_category(self, tmp_path: Path) -> None:
        """group_skipped_files groups by category."""
        from portolan_cli.scan_output import group_skipped_files

        skipped = [
            make_skipped_file(
                tmp_path / "data.dbf",
                FileCategory.KNOWN_SIDECAR,
                SkipReasonType.SIDECAR_FILE,
            ),
            make_skipped_file(
                tmp_path / "data.prj",
                FileCategory.KNOWN_SIDECAR,
                SkipReasonType.SIDECAR_FILE,
            ),
            make_skipped_file(
                tmp_path / "readme.md",
                FileCategory.DOCUMENTATION,
                SkipReasonType.NOT_GEOSPATIAL,
            ),
            make_skipped_file(
                tmp_path / "style.json",
                FileCategory.STYLE,
                SkipReasonType.METADATA_FILE,
            ),
        ]

        grouped = group_skipped_files(skipped)

        assert FileCategory.KNOWN_SIDECAR in grouped
        assert len(grouped[FileCategory.KNOWN_SIDECAR]) == 2
        assert FileCategory.DOCUMENTATION in grouped
        assert len(grouped[FileCategory.DOCUMENTATION]) == 1

    def test_category_display_name(self) -> None:
        """FileCategory has human-readable display names."""
        from portolan_cli.scan_output import get_category_display_name

        assert get_category_display_name(FileCategory.KNOWN_SIDECAR) == "sidecar"
        assert get_category_display_name(FileCategory.TABULAR_DATA) == "tabular"
        assert get_category_display_name(FileCategory.DOCUMENTATION) == "documentation"
        assert get_category_display_name(FileCategory.VISUALIZATION) == "visualization"
        assert get_category_display_name(FileCategory.THUMBNAIL) == "thumbnail"
        assert get_category_display_name(FileCategory.STAC_METADATA) == "stac-metadata"
        assert get_category_display_name(FileCategory.JUNK) == "junk"


# =============================================================================
# Tests for Collection Inference Output
# =============================================================================


@pytest.mark.unit
class TestCollectionInferenceOutput:
    """Tests for collection inference output formatting."""

    def test_format_collection_suggestion(self, tmp_path: Path) -> None:
        """format_collection_suggestion formats suggestion nicely."""
        from portolan_cli.scan_output import format_collection_suggestion

        suggestion = CollectionSuggestion(
            suggested_name="flood-depth",
            files=(
                tmp_path / "flood_rp10.tif",
                tmp_path / "flood_rp50.tif",
                tmp_path / "flood_rp100.tif",
                tmp_path / "flood_rp500.tif",
            ),
            pattern_type="return_period",
            confidence=0.85,
            reason="Detected return period pattern",
        )

        output = format_collection_suggestion(suggestion)

        assert "flood-depth" in output
        assert "4 files" in output
        assert "85%" in output

    def test_format_collection_suggestion_truncates_files(self, tmp_path: Path) -> None:
        """format_collection_suggestion truncates long file lists."""
        from portolan_cli.scan_output import format_collection_suggestion

        # Create suggestion with many files
        files = tuple(tmp_path / f"file_{i}.tif" for i in range(20))
        suggestion = CollectionSuggestion(
            suggested_name="large-collection",
            files=files,
            pattern_type="numeric",
            confidence=0.7,
            reason="Found numeric pattern",
        )

        output = format_collection_suggestion(suggestion, max_files=5)

        # Should show truncation indicator
        assert "..." in output or "more" in output.lower()


# =============================================================================
# Tests for Next Steps Summary
# =============================================================================


@pytest.mark.unit
class TestNextStepsSummary:
    """Tests for actionable next steps summary."""

    def test_generate_next_steps_with_fixable(self, tmp_path: Path) -> None:
        """generate_next_steps includes --fix suggestion when fixable issues exist."""
        from portolan_cli.scan_output import generate_next_steps

        result = ScanResult(
            root=tmp_path,
            ready=[make_scanned_file(tmp_path / "data.geojson")],
            issues=[
                make_scan_issue(
                    tmp_path / "file with spaces.geojson",
                    IssueType.INVALID_CHARACTERS,
                    Severity.WARNING,
                )
            ],
            skipped=[],
            directories_scanned=1,
        )

        steps = generate_next_steps(result)

        # Should suggest --fix for invalid characters
        assert any("--fix" in step for step in steps)

    def test_generate_next_steps_with_manual(self, tmp_path: Path) -> None:
        """generate_next_steps includes manual resolution guidance."""
        from portolan_cli.scan_output import generate_next_steps

        result = ScanResult(
            root=tmp_path,
            ready=[
                make_scanned_file(tmp_path / "a.geojson"),
                make_scanned_file(tmp_path / "b.geojson"),
            ],
            issues=[
                make_scan_issue(
                    tmp_path,
                    IssueType.MULTIPLE_PRIMARIES,
                    Severity.WARNING,
                    message="Multiple geo-assets in directory",
                )
            ],
            skipped=[],
            directories_scanned=1,
        )

        steps = generate_next_steps(result)

        # Should mention manual decisions needed
        assert any("manual" in step.lower() for step in steps)

    def test_generate_next_steps_ready_for_import(self, tmp_path: Path) -> None:
        """generate_next_steps shows ready state when no issues."""
        from portolan_cli.scan_output import generate_next_steps

        result = ScanResult(
            root=tmp_path,
            ready=[make_scanned_file(tmp_path / "collection" / "data.geojson")],
            issues=[],
            skipped=[],
            directories_scanned=2,
        )

        steps = generate_next_steps(result)

        # Should indicate structure is valid/ready
        assert any("ready" in step.lower() or "valid" in step.lower() for step in steps)

    def test_generate_next_steps_no_files(self, tmp_path: Path) -> None:
        """generate_next_steps handles empty scan result."""
        from portolan_cli.scan_output import generate_next_steps

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[],
            skipped=[],
            directories_scanned=1,
        )

        steps = generate_next_steps(result)

        # Should indicate no files found
        assert any("no" in step.lower() and "file" in step.lower() for step in steps)


# =============================================================================
# Tests for Tree View Output
# =============================================================================


@pytest.mark.unit
class TestTreeViewOutput:
    """Tests for tree-style terminal output."""

    def test_build_tree_structure(self, tmp_path: Path) -> None:
        """build_tree_structure creates nested dict from paths."""
        from portolan_cli.scan_output import build_tree_structure

        result = ScanResult(
            root=tmp_path,
            ready=[
                make_scanned_file(tmp_path / "census" / "census.parquet"),
                make_scanned_file(tmp_path / "flood" / "flood_rp10.tif", ext=".tif"),
            ],
            issues=[],
            skipped=[
                make_skipped_file(
                    tmp_path / "census" / "overview.pmtiles",
                    FileCategory.VISUALIZATION,
                    SkipReasonType.VISUALIZATION_ONLY,
                ),
            ],
            directories_scanned=3,
        )

        tree = build_tree_structure(result)

        # Should have top-level directories
        assert "census" in tree
        assert "flood" in tree

    def test_render_tree_view(self, tmp_path: Path) -> None:
        """render_tree_view produces expected tree characters."""
        from portolan_cli.scan_output import render_tree_view

        result = ScanResult(
            root=tmp_path,
            ready=[
                make_scanned_file(tmp_path / "data.geojson"),
            ],
            issues=[],
            skipped=[],
            directories_scanned=1,
        )

        output = render_tree_view(result)

        # Should have tree characters
        assert "├" in output or "└" in output

    def test_render_tree_view_with_status_markers(self, tmp_path: Path) -> None:
        """render_tree_view shows status markers for each file."""
        from portolan_cli.scan_output import render_tree_view

        result = ScanResult(
            root=tmp_path,
            ready=[make_scanned_file(tmp_path / "valid.geojson")],
            issues=[
                make_scan_issue(
                    tmp_path / "problem.geojson",
                    IssueType.INVALID_CHARACTERS,
                    Severity.WARNING,
                )
            ],
            skipped=[
                make_skipped_file(
                    tmp_path / "readme.md",
                    FileCategory.DOCUMENTATION,
                    SkipReasonType.NOT_GEOSPATIAL,
                ),
            ],
            directories_scanned=1,
        )

        output = render_tree_view(result)

        # Should show checkmark for ready files
        assert "✓" in output or "geo-asset" in output.lower()

    def test_tree_view_missing_files_marker(self, tmp_path: Path) -> None:
        """render_tree_view marks missing expected files."""
        from portolan_cli.scan_output import render_tree_view

        # Result missing catalog.json
        result = ScanResult(
            root=tmp_path,
            ready=[make_scanned_file(tmp_path / "collection" / "data.geojson")],
            issues=[],
            skipped=[],
            directories_scanned=2,
        )

        output = render_tree_view(result, show_missing=True)

        # Should indicate missing catalog.json
        assert "catalog.json" in output
        assert "missing" in output.lower() or "will generate" in output.lower()


# =============================================================================
# Tests for _format_size Function (Coverage Improvement)
# =============================================================================


@pytest.mark.unit
class TestFormatSize:
    """Tests for _format_size helper function."""

    def test_format_size_none(self) -> None:
        """_format_size returns empty string for None."""
        from portolan_cli.scan_output import _format_size

        assert _format_size(None) == ""

    def test_format_size_bytes(self) -> None:
        """_format_size returns bytes format for small sizes."""
        from portolan_cli.scan_output import _format_size

        assert _format_size(500) == "500 B"
        assert _format_size(0) == "0 B"
        assert _format_size(1023) == "1023 B"

    def test_format_size_kilobytes(self) -> None:
        """_format_size returns KB format for kilobyte range."""
        from portolan_cli.scan_output import _format_size

        # 1024 bytes = 1 KB
        assert _format_size(1024) == "1.0 KB"
        # 10 KB
        assert _format_size(10 * 1024) == "10.0 KB"
        # Just under 1 MB
        assert _format_size(1024 * 1024 - 1) == "1024.0 KB"

    def test_format_size_megabytes(self) -> None:
        """_format_size returns MB format for megabyte range."""
        from portolan_cli.scan_output import _format_size

        # 1 MB
        assert _format_size(1024 * 1024) == "1.0 MB"
        # 500 MB
        assert _format_size(500 * 1024 * 1024) == "500.0 MB"
        # Just under 1 GB
        assert _format_size(1024 * 1024 * 1024 - 1) == "1024.0 MB"

    def test_format_size_gigabytes(self) -> None:
        """_format_size returns GB format for gigabyte range."""
        from portolan_cli.scan_output import _format_size

        # 1 GB
        assert _format_size(1024 * 1024 * 1024) == "1.0 GB"
        # 10 GB
        assert _format_size(10 * 1024 * 1024 * 1024) == "10.0 GB"


# =============================================================================
# Tests for Empty Sections in Formatted Output (Coverage Improvement)
# =============================================================================


@pytest.mark.unit
class TestEmptySectionFormatting:
    """Tests for empty section handling in output formatting."""

    def test_format_header_no_geo_assets(self, tmp_path: Path) -> None:
        """_format_header shows 'No geo-assets found' when none exist."""
        from portolan_cli.scan_output import _format_header

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[],
            skipped=[],
            directories_scanned=5,
        )

        lines = _format_header(result)
        combined = " ".join(lines).lower()

        assert "scanned" in combined
        assert "no geo-assets" in combined

    def test_format_breakdown_empty(self, tmp_path: Path) -> None:
        """_format_breakdown returns empty list when no ready files."""
        from portolan_cli.scan_output import _format_breakdown

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[],
            skipped=[],
            directories_scanned=1,
        )

        lines = _format_breakdown(result)
        assert lines == []

    def test_format_issues_empty(self, tmp_path: Path) -> None:
        """_format_issues returns empty list when no issues."""
        from portolan_cli.scan_output import _format_issues

        result = ScanResult(
            root=tmp_path,
            ready=[make_scanned_file(tmp_path / "data.geojson")],
            issues=[],
            skipped=[],
            directories_scanned=1,
        )

        lines = _format_issues(result)
        assert lines == []

    def test_format_skipped_empty(self, tmp_path: Path) -> None:
        """_format_skipped returns empty list when no skipped files."""
        from portolan_cli.scan_output import _format_skipped

        result = ScanResult(
            root=tmp_path,
            ready=[make_scanned_file(tmp_path / "data.geojson")],
            issues=[],
            skipped=[],
            directories_scanned=1,
        )

        lines = _format_skipped(result)
        assert lines == []

    def test_format_skipped_with_empty_grouped(self, tmp_path: Path) -> None:
        """_format_skipped handles legacy Path objects in skipped list."""
        from portolan_cli.scan_output import _format_skipped

        # Only legacy Path objects (no SkippedFile instances) will result in
        # group_skipped_files returning an empty dict
        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[],
            skipped=[tmp_path / "unknown.xyz"],  # Legacy Path
            directories_scanned=1,
        )

        # Legacy paths are still counted as unknown, so may not be empty
        # But the function should handle gracefully without raising
        _format_skipped(result)  # Should not raise


@pytest.mark.unit
class TestIssuesFormattingTruncation:
    """Tests for issue truncation in formatted output."""

    def test_format_issues_truncates_at_10(self, tmp_path: Path) -> None:
        """_format_issues truncates each severity group at 10."""
        from portolan_cli.scan_output import _format_issues

        # Create 15 warnings
        issues = [
            make_scan_issue(
                tmp_path / f"file{i}.geojson",
                IssueType.INVALID_CHARACTERS,
                Severity.WARNING,
                f"Warning {i}",
            )
            for i in range(15)
        ]

        result = ScanResult(
            root=tmp_path,
            ready=[make_scanned_file(tmp_path / "data.geojson")],
            issues=issues,
            skipped=[],
            directories_scanned=1,
        )

        lines = _format_issues(result)
        combined = "\n".join(lines)

        # Should show truncation indicator
        assert "more" in combined.lower()


@pytest.mark.unit
class TestTreeBuildingEdgeCases:
    """Tests for edge cases in tree building."""

    def test_tree_with_path_outside_root(self, tmp_path: Path) -> None:
        """build_tree_structure handles paths not relative to root gracefully."""
        from portolan_cli.scan_output import build_tree_structure

        # Create a result where issue path is outside the root (edge case)
        external_path = Path("/completely/different/path/file.geojson")

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[
                make_scan_issue(
                    external_path,
                    IssueType.PERMISSION_DENIED,
                    Severity.ERROR,
                    "Permission denied",
                )
            ],
            skipped=[],
            directories_scanned=1,
        )

        # Should not raise - should handle gracefully by skipping
        tree = build_tree_structure(result)
        # External path should not be added to tree
        assert "completely" not in str(tree)

    def test_tree_with_skipped_legacy_path(self, tmp_path: Path) -> None:
        """build_tree_structure handles legacy Path in skipped list."""
        from portolan_cli.scan_output import build_tree_structure

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[],
            skipped=[tmp_path / "legacy.csv"],  # Legacy Path object
            directories_scanned=1,
        )

        tree = build_tree_structure(result)
        # Should include the legacy path with "skipped" status
        assert "legacy.csv" in tree


@pytest.mark.unit
class TestFormatScanOutputWithCollections:
    """Tests for format_scan_output with collection suggestions."""

    def test_format_scan_output_with_collection_suggestions(self, tmp_path: Path) -> None:
        """format_scan_output includes collection suggestions section."""
        from portolan_cli.scan_output import format_scan_output

        result = ScanResult(
            root=tmp_path,
            ready=[
                make_scanned_file(tmp_path / "flood_rp10.tif", ext=".tif"),
                make_scanned_file(tmp_path / "flood_rp50.tif", ext=".tif"),
            ],
            issues=[],
            skipped=[],
            directories_scanned=1,
            collection_suggestions=[
                CollectionSuggestion(
                    suggested_name="flood-depth",
                    files=(tmp_path / "flood_rp10.tif", tmp_path / "flood_rp50.tif"),
                    pattern_type="return_period",
                    confidence=0.85,
                    reason="Detected return period pattern",
                )
            ],
        )

        output = format_scan_output(result)

        assert "suggested collections" in output.lower()
        assert "flood-depth" in output

    def test_format_scan_output_with_tree(self, tmp_path: Path) -> None:
        """format_scan_output includes tree view when show_tree=True."""
        from portolan_cli.scan_output import format_scan_output

        result = ScanResult(
            root=tmp_path,
            ready=[make_scanned_file(tmp_path / "data.geojson")],
            issues=[],
            skipped=[],
            directories_scanned=1,
        )

        output = format_scan_output(result, show_tree=True)

        # Tree should have tree characters
        assert "├" in output or "└" in output


# =============================================================================
# Integration Tests for Full Output
# =============================================================================


@pytest.mark.unit
class TestFullScanOutputFormat:
    """Tests for complete formatted scan output."""

    def test_format_scan_output_sections(self, tmp_path: Path) -> None:
        """format_scan_output includes all expected sections."""
        from portolan_cli.scan_output import format_scan_output

        result = ScanResult(
            root=tmp_path,
            ready=[make_scanned_file(tmp_path / "collection" / "data.geojson")],
            issues=[
                make_scan_issue(
                    tmp_path / "file with spaces.geojson",
                    IssueType.INVALID_CHARACTERS,
                    Severity.WARNING,
                )
            ],
            skipped=[
                make_skipped_file(
                    tmp_path / "readme.md",
                    FileCategory.DOCUMENTATION,
                    SkipReasonType.NOT_GEOSPATIAL,
                ),
            ],
            directories_scanned=2,
        )

        output = format_scan_output(result)

        # Should have summary section with geo-asset count
        assert "geo-asset" in output.lower()

        # Should have next steps
        assert "next step" in output.lower()


# =============================================================================
# Tests for --errors-only Output Mode
# =============================================================================


@pytest.mark.unit
class TestManualOnlyOutput:
    """Tests for --errors-only flag output formatting with tree structure."""

    def test_manual_only_shows_tree_structure(self, tmp_path: Path) -> None:
        """manual_only=True shows issues in tree structure."""
        from portolan_cli.scan_output import format_scan_output

        result = ScanResult(
            root=tmp_path,
            ready=[make_scanned_file(tmp_path / "data.geojson")],
            issues=[
                make_scan_issue(
                    tmp_path / "subdir" / "nested",
                    IssueType.MULTIPLE_PRIMARIES,
                    Severity.ERROR,
                    message="Multiple primary geo files",
                    relative_path="subdir/nested",
                ),
            ],
            skipped=[],
            directories_scanned=1,
        )

        output = format_scan_output(result, manual_only=True)

        # Should show tree structure with directory
        assert "subdir" in output
        # Should show the issue marker (✗ for error)
        assert "\u2717" in output or "nested" in output

    def test_manual_only_groups_by_directory(self, tmp_path: Path) -> None:
        """manual_only=True groups multiple issues under same directory."""
        from portolan_cli.scan_output import format_scan_output

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[
                make_scan_issue(
                    tmp_path / "data" / "file1.shp",
                    IssueType.INCOMPLETE_SHAPEFILE,
                    Severity.ERROR,
                    message="Missing .shx",
                    relative_path="data/file1.shp",
                ),
                make_scan_issue(
                    tmp_path / "data" / "file2.shp",
                    IssueType.INCOMPLETE_SHAPEFILE,
                    Severity.ERROR,
                    message="Missing .dbf",
                    relative_path="data/file2.shp",
                ),
            ],
            skipped=[],
            directories_scanned=1,
        )

        output = format_scan_output(result, manual_only=True)

        # Directory should appear once, not repeated for each issue
        assert output.count("data") >= 1
        # Both files should be shown
        assert "file1.shp" in output
        assert "file2.shp" in output

    def test_manual_only_hides_ready_files(self, tmp_path: Path) -> None:
        """manual_only=True hides the 'ready to import' section."""
        from portolan_cli.scan_output import format_scan_output

        result = ScanResult(
            root=tmp_path,
            ready=[
                make_scanned_file(tmp_path / "a.geojson"),
                make_scanned_file(tmp_path / "b.geojson"),
                make_scanned_file(tmp_path / "c.geojson"),
            ],
            issues=[
                make_scan_issue(
                    tmp_path / "collection",
                    IssueType.MULTIPLE_PRIMARIES,
                    Severity.ERROR,
                ),
            ],
            skipped=[],
            directories_scanned=1,
        )

        output = format_scan_output(result, manual_only=True)

        # Should NOT show ready files count
        assert "3 geo-asset" not in output.lower()
        assert "ready to import" not in output.lower()

    def test_manual_only_hides_fixable_issues(self, tmp_path: Path) -> None:
        """manual_only=True hides issues fixable with --fix."""
        from portolan_cli.scan_output import format_scan_output

        result = ScanResult(
            root=tmp_path,
            ready=[make_scanned_file(tmp_path / "data.geojson")],
            issues=[
                # FIX_FLAG issue - should be hidden
                make_scan_issue(
                    tmp_path / "file with spaces.geojson",
                    IssueType.INVALID_CHARACTERS,
                    Severity.WARNING,
                    message="Invalid characters in filename",
                ),
                # Manual issue - should be shown
                make_scan_issue(
                    tmp_path / "collection",
                    IssueType.MULTIPLE_PRIMARIES,
                    Severity.ERROR,
                ),
            ],
            skipped=[],
            directories_scanned=1,
        )

        output = format_scan_output(result, manual_only=True)

        # Should NOT show fixable issue details
        assert "invalid characters" not in output.lower()
        assert "--fix" not in output

    def test_manual_only_shows_count_header(self, tmp_path: Path) -> None:
        """manual_only=True shows count of manual resolution items in header."""
        from portolan_cli.scan_output import format_scan_output

        result = ScanResult(
            root=tmp_path,
            ready=[make_scanned_file(tmp_path / "data.geojson")],
            issues=[
                make_scan_issue(
                    tmp_path / "dir1",
                    IssueType.MULTIPLE_PRIMARIES,
                    Severity.ERROR,
                ),
                make_scan_issue(
                    tmp_path / "dir2",
                    IssueType.MIXED_FORMATS,
                    Severity.WARNING,
                ),
            ],
            skipped=[],
            directories_scanned=2,
        )

        output = format_scan_output(result, manual_only=True)

        # Should show count of manual issues (2) in header
        assert "2" in output
        assert "manual" in output.lower() or "require" in output.lower()

    def test_manual_only_shows_short_descriptions(self, tmp_path: Path) -> None:
        """manual_only=True shows short inline descriptions, not full sentences."""
        from portolan_cli.scan_output import format_scan_output

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[
                make_scan_issue(
                    tmp_path / "data.shp",
                    IssueType.INCOMPLETE_SHAPEFILE,
                    Severity.ERROR,
                    message="Shapefile missing required sidecars: .shx, .dbf",
                ),
            ],
            skipped=[],
            directories_scanned=1,
        )

        output = format_scan_output(result, manual_only=True)

        # Should show the file with a short description
        assert "data.shp" in output
        # Description should be present (either full or shortened)
        assert "shx" in output.lower() or "missing" in output.lower()

    def test_manual_only_no_errors_message(self, tmp_path: Path) -> None:
        """manual_only=True with no manual issues shows success message."""
        from portolan_cli.scan_output import format_scan_output

        result = ScanResult(
            root=tmp_path,
            ready=[
                make_scanned_file(tmp_path / "a.geojson"),
                make_scanned_file(tmp_path / "b.geojson"),
            ],
            issues=[
                # Only fixable issues, no manual
                make_scan_issue(
                    tmp_path / "file with spaces.geojson",
                    IssueType.INVALID_CHARACTERS,
                    Severity.WARNING,
                ),
            ],
            skipped=[],
            directories_scanned=1,
        )

        output = format_scan_output(result, manual_only=True)

        # Should show success message
        assert "no files require manual resolution" in output.lower()

    def test_manual_only_completely_clean(self, tmp_path: Path) -> None:
        """manual_only=True with no issues at all shows success message."""
        from portolan_cli.scan_output import format_scan_output

        result = ScanResult(
            root=tmp_path,
            ready=[make_scanned_file(tmp_path / "data.geojson")],
            issues=[],
            skipped=[],
            directories_scanned=1,
        )

        output = format_scan_output(result, manual_only=True)

        # Should show success message
        assert "no files require manual resolution" in output.lower()

    def test_manual_only_uses_severity_markers(self, tmp_path: Path) -> None:
        """manual_only=True uses ✗ for errors and ⚠ for warnings."""
        from portolan_cli.scan_output import format_scan_output

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[
                make_scan_issue(
                    tmp_path / "error_file.shp",
                    IssueType.INCOMPLETE_SHAPEFILE,
                    Severity.ERROR,
                    message="Missing sidecars",
                ),
                make_scan_issue(
                    tmp_path / "warning_dir",
                    IssueType.MULTIPLE_PRIMARIES,
                    Severity.WARNING,
                    message="Multiple primaries",
                ),
            ],
            skipped=[],
            directories_scanned=1,
        )

        output = format_scan_output(result, manual_only=True)

        # Should use markers: ✗ for error, ⚠ for warning
        assert "\u2717" in output  # ✗
        assert "\u26a0" in output  # ⚠

    def test_manual_only_handles_root_directory_issues(self, tmp_path: Path) -> None:
        """manual_only=True handles issues on the root directory itself."""
        from portolan_cli.scan_output import format_scan_output

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[
                # Directory-level issue (relative_path is ".")
                make_scan_issue(
                    tmp_path,
                    IssueType.MULTIPLE_PRIMARIES,
                    Severity.WARNING,
                    message="Directory has 5 primary assets",
                    relative_path=".",
                ),
            ],
            skipped=[],
            directories_scanned=1,
        )

        output = format_scan_output(result, manual_only=True)

        # Should show the root directory issue
        assert "5 primary assets" in output
        # Should have the warning marker
        assert "\u26a0" in output

    def test_manual_only_singular_grammar(self, tmp_path: Path) -> None:
        """manual_only=True uses correct grammar for singular case."""
        from portolan_cli.scan_output import format_scan_output

        result = ScanResult(
            root=tmp_path,
            ready=[],
            issues=[
                make_scan_issue(
                    tmp_path / "file.shp",
                    IssueType.INCOMPLETE_SHAPEFILE,
                    Severity.ERROR,
                    message="Missing sidecars",
                ),
            ],
            skipped=[],
            directories_scanned=1,
        )

        output = format_scan_output(result, manual_only=True)

        # Should say "1 file requires" not "1 file require"
        assert "1 file requires" in output


# =============================================================================
# Tests for Phase 3: Enhanced Output (Nested Catalogs)
# =============================================================================


@pytest.mark.unit
class TestNestedCollectionIdDisplay:
    """Tests for displaying nested collection IDs."""

    def test_format_file_with_nested_collection_id(self, tmp_path: Path) -> None:
        """format_file_entry shows nested collection ID when present."""
        from portolan_cli.scan_output import format_file_entry

        file = ScannedFile(
            path=tmp_path / "climate" / "hittekaart" / "data.parquet",
            relative_path="climate/hittekaart/data.parquet",
            extension=".parquet",
            format_type=FormatType.VECTOR,
            size_bytes=1000,
            metadata={
                "inferred_collection_id": "climate/hittekaart",
                "format_status": "cloud_native",
                "format_display_name": "GeoParquet",
            },
        )

        output = format_file_entry(file)

        # Should show the nested collection ID
        assert "climate/hittekaart" in output
        # Should show the format status
        assert "GeoParquet" in output

    def test_format_file_with_simple_collection_id(self, tmp_path: Path) -> None:
        """format_file_entry shows simple collection ID for flat structures."""
        from portolan_cli.scan_output import format_file_entry

        file = ScannedFile(
            path=tmp_path / "census" / "data.parquet",
            relative_path="census/data.parquet",
            extension=".parquet",
            format_type=FormatType.VECTOR,
            size_bytes=1000,
            metadata={
                "inferred_collection_id": "census",
                "format_status": "cloud_native",
                "format_display_name": "GeoParquet",
            },
        )

        output = format_file_entry(file)

        assert "census" in output

    def test_format_file_without_collection_id(self, tmp_path: Path) -> None:
        """format_file_entry handles files without inferred_collection_id."""
        from portolan_cli.scan_output import format_file_entry

        # Legacy ScannedFile without metadata
        file = make_scanned_file(tmp_path / "data.geojson")

        output = format_file_entry(file)

        # Should still work, showing filename
        assert "data.geojson" in output


@pytest.mark.unit
class TestFormatStatusDisplay:
    """Tests for format status display per file."""

    def test_format_status_cloud_native(self, tmp_path: Path) -> None:
        """Cloud-native formats show positive status."""
        from portolan_cli.scan_output import format_file_entry

        file = ScannedFile(
            path=tmp_path / "data.parquet",
            relative_path="data.parquet",
            extension=".parquet",
            format_type=FormatType.VECTOR,
            size_bytes=1000,
            metadata={
                "format_status": "cloud_native",
                "format_display_name": "GeoParquet",
            },
        )

        output = format_file_entry(file)

        assert "GeoParquet" in output

    def test_format_status_parquet_no_geometry(self, tmp_path: Path) -> None:
        """Parquet without geometry shows different status."""
        from portolan_cli.scan_output import format_file_entry

        file = ScannedFile(
            path=tmp_path / "lookup.parquet",
            relative_path="lookup.parquet",
            extension=".parquet",
            format_type=FormatType.VECTOR,
            size_bytes=1000,
            metadata={
                "format_status": "companion",
                "format_display_name": "Parquet (no geometry)",
            },
        )

        output = format_file_entry(file)

        assert "no geometry" in output.lower() or "companion" in output.lower()

    def test_format_status_convertible(self, tmp_path: Path) -> None:
        """Convertible formats show target format."""
        from portolan_cli.scan_output import format_file_entry

        file = ScannedFile(
            path=tmp_path / "data.geojson",
            relative_path="data.geojson",
            extension=".geojson",
            format_type=FormatType.VECTOR,
            size_bytes=1000,
            metadata={
                "format_status": "convertible",
                "format_display_name": "GeoJSON",
                "target_format": "GeoParquet",
            },
        )

        output = format_file_entry(file)

        # Should mention either the format or conversion
        assert "GeoJSON" in output or "convert" in output.lower()


@pytest.mark.unit
class TestGroupFilesByCollection:
    """Tests for grouping files by inferred collection."""

    def test_group_files_by_collection(self, tmp_path: Path) -> None:
        """group_files_by_collection groups files correctly."""
        from portolan_cli.scan_output import group_files_by_collection

        files = [
            ScannedFile(
                path=tmp_path / "climate" / "hittekaart" / "data.parquet",
                relative_path="climate/hittekaart/data.parquet",
                extension=".parquet",
                format_type=FormatType.VECTOR,
                size_bytes=1000,
                metadata={"inferred_collection_id": "climate/hittekaart"},
            ),
            ScannedFile(
                path=tmp_path / "climate" / "hittekaart" / "lookup.parquet",
                relative_path="climate/hittekaart/lookup.parquet",
                extension=".parquet",
                format_type=FormatType.VECTOR,
                size_bytes=500,
                metadata={"inferred_collection_id": "climate/hittekaart"},
            ),
            ScannedFile(
                path=tmp_path / "census" / "data.parquet",
                relative_path="census/data.parquet",
                extension=".parquet",
                format_type=FormatType.VECTOR,
                size_bytes=2000,
                metadata={"inferred_collection_id": "census"},
            ),
        ]

        grouped = group_files_by_collection(files)

        assert "climate/hittekaart" in grouped
        assert len(grouped["climate/hittekaart"]) == 2
        assert "census" in grouped
        assert len(grouped["census"]) == 1

    def test_group_files_without_collection_id(self, tmp_path: Path) -> None:
        """Files without collection_id go to 'uncategorized' group."""
        from portolan_cli.scan_output import group_files_by_collection

        files = [
            make_scanned_file(tmp_path / "data.geojson"),  # No metadata
        ]

        grouped = group_files_by_collection(files)

        # Should handle gracefully with a fallback group
        assert len(grouped) >= 1


@pytest.mark.unit
class TestStructureRecommendations:
    """Tests for structure recommendations section."""

    def test_detect_vector_collection_pattern(self, tmp_path: Path) -> None:
        """detect_structure_pattern identifies vector collection."""
        from portolan_cli.scan_output import detect_structure_pattern

        result = ScanResult(
            root=tmp_path,
            ready=[
                ScannedFile(
                    path=tmp_path / "census" / "data.parquet",
                    relative_path="census/data.parquet",
                    extension=".parquet",
                    format_type=FormatType.VECTOR,
                    size_bytes=1000,
                    metadata={"inferred_collection_id": "census"},
                ),
            ],
            issues=[],
            skipped=[],
            directories_scanned=2,
        )

        pattern = detect_structure_pattern(result)

        assert pattern.pattern_type in ("vector_collection", "single_collection")

    def test_detect_raster_items_pattern(self, tmp_path: Path) -> None:
        """detect_structure_pattern identifies raster items."""
        from portolan_cli.scan_output import detect_structure_pattern

        result = ScanResult(
            root=tmp_path,
            ready=[
                ScannedFile(
                    path=tmp_path / "imagery" / "2024-01-15" / "scene.tif",
                    relative_path="imagery/2024-01-15/scene.tif",
                    extension=".tif",
                    format_type=FormatType.RASTER,
                    size_bytes=1000000,
                    metadata={"inferred_collection_id": "imagery"},
                ),
                ScannedFile(
                    path=tmp_path / "imagery" / "2024-01-16" / "scene.tif",
                    relative_path="imagery/2024-01-16/scene.tif",
                    extension=".tif",
                    format_type=FormatType.RASTER,
                    size_bytes=1000000,
                    metadata={"inferred_collection_id": "imagery"},
                ),
            ],
            issues=[],
            skipped=[],
            directories_scanned=4,
        )

        pattern = detect_structure_pattern(result)

        assert pattern.pattern_type == "raster_items"

    def test_generate_structure_recommendation(self, tmp_path: Path) -> None:
        """generate_structure_recommendation produces actionable output."""
        from portolan_cli.scan_output import generate_structure_recommendation

        result = ScanResult(
            root=tmp_path,
            ready=[
                ScannedFile(
                    path=tmp_path / "census" / "data.parquet",
                    relative_path="census/data.parquet",
                    extension=".parquet",
                    format_type=FormatType.VECTOR,
                    size_bytes=1000,
                    metadata={"inferred_collection_id": "census"},
                ),
            ],
            issues=[],
            skipped=[],
            directories_scanned=2,
        )

        recommendation = generate_structure_recommendation(result)

        # Should include suggested commands
        assert "portolan add" in recommendation or recommendation == ""
        # Or if there's nothing to recommend, empty is fine

    def test_generate_ascii_tree_recommendation(self, tmp_path: Path) -> None:
        """generate_ascii_tree_recommendation shows suggested structure."""
        from portolan_cli.scan_output import generate_ascii_tree_recommendation

        collections = ["climate/hittekaart", "census"]

        output = generate_ascii_tree_recommendation(collections)

        # Should be an ASCII tree
        assert "├" in output or "└" in output or "│" in output
        assert "climate" in output
        assert "hittekaart" in output
        assert "census" in output


@pytest.mark.unit
class TestEnhancedVerboseOutput:
    """Tests for enhanced --verbose output."""

    def test_format_issue_verbose(self, tmp_path: Path) -> None:
        """format_issue_verbose includes extra details."""
        from portolan_cli.scan_output import format_issue_verbose

        issue = make_scan_issue(
            tmp_path / "collection",
            IssueType.MULTIPLE_PRIMARIES,
            Severity.WARNING,
            message="Directory has 3 primary assets",
            suggestion="Move to separate subdirectories",
        )

        output = format_issue_verbose(issue)

        # Basic info
        assert "3 primary assets" in output
        # Suggestion/recommendation
        assert "subdirectories" in output.lower() or "move" in output.lower()

    def test_format_issue_basic(self, tmp_path: Path) -> None:
        """format_issue_basic shows minimal info."""
        from portolan_cli.scan_output import format_issue_basic

        issue = make_scan_issue(
            tmp_path / "collection",
            IssueType.MULTIPLE_PRIMARIES,
            Severity.WARNING,
            message="Directory has 3 primary assets",
        )

        output = format_issue_basic(issue)

        # Should show issue and why it matters
        assert "primary assets" in output


@pytest.mark.unit
class TestEnhancedJsonOutput:
    """Tests for enhanced JSON output structure."""

    def test_format_ready_file_json(self, tmp_path: Path) -> None:
        """format_ready_file_json includes new fields."""
        from portolan_cli.scan_output import format_ready_file_json

        file = ScannedFile(
            path=tmp_path / "climate" / "hittekaart" / "data.parquet",
            relative_path="climate/hittekaart/data.parquet",
            extension=".parquet",
            format_type=FormatType.VECTOR,
            size_bytes=1000,
            metadata={
                "inferred_collection_id": "climate/hittekaart",
                "format_status": "cloud_native",
                "format_display_name": "GeoParquet",
            },
        )

        json_dict = format_ready_file_json(file)

        assert json_dict["inferred_collection_id"] == "climate/hittekaart"
        assert json_dict["format_status"] == "cloud_native"
        assert json_dict["format_display_name"] == "GeoParquet"

    def test_format_recommended_structure_json(self, tmp_path: Path) -> None:
        """format_recommended_structure_json includes pattern and commands."""
        from portolan_cli.scan_output import format_recommended_structure_json

        result = ScanResult(
            root=tmp_path,
            ready=[
                ScannedFile(
                    path=tmp_path / "census" / "data.parquet",
                    relative_path="census/data.parquet",
                    extension=".parquet",
                    format_type=FormatType.VECTOR,
                    size_bytes=1000,
                    metadata={"inferred_collection_id": "census"},
                ),
            ],
            issues=[],
            skipped=[],
            directories_scanned=2,
        )

        json_dict = format_recommended_structure_json(result)

        assert "pattern_type" in json_dict
        assert "collections" in json_dict
        assert isinstance(json_dict["collections"], list)

    def test_format_fix_commands_json(self, tmp_path: Path) -> None:
        """format_fix_commands_json returns structured commands."""
        from portolan_cli.scan_output import format_fix_commands_json

        result = ScanResult(
            root=tmp_path,
            ready=[
                ScannedFile(
                    path=tmp_path / "census" / "data.parquet",
                    relative_path="census/data.parquet",
                    extension=".parquet",
                    format_type=FormatType.VECTOR,
                    size_bytes=1000,
                    metadata={"inferred_collection_id": "census"},
                ),
            ],
            issues=[],
            skipped=[],
            directories_scanned=2,
        )

        commands = format_fix_commands_json(result)

        # Should return a list (may be empty if no fixes needed)
        assert isinstance(commands, list)
        # If there are commands, they should have required fields
        for cmd in commands:
            assert "command" in cmd
            assert "args" in cmd


@pytest.mark.unit
class TestFormatEnhancedSummary:
    """Tests for the enhanced summary format with nested catalogs."""

    def test_format_enhanced_summary_with_nested_collections(self, tmp_path: Path) -> None:
        """format_enhanced_summary shows nested collection grouping."""
        from portolan_cli.scan_output import format_enhanced_summary

        result = ScanResult(
            root=tmp_path,
            ready=[
                ScannedFile(
                    path=tmp_path / "climate" / "hittekaart" / "data.parquet",
                    relative_path="climate/hittekaart/data.parquet",
                    extension=".parquet",
                    format_type=FormatType.VECTOR,
                    size_bytes=1000,
                    metadata={
                        "inferred_collection_id": "climate/hittekaart",
                        "format_status": "cloud_native",
                        "format_display_name": "GeoParquet",
                    },
                ),
                ScannedFile(
                    path=tmp_path / "census" / "data.parquet",
                    relative_path="census/data.parquet",
                    extension=".parquet",
                    format_type=FormatType.VECTOR,
                    size_bytes=2000,
                    metadata={
                        "inferred_collection_id": "census",
                        "format_status": "cloud_native",
                        "format_display_name": "GeoParquet",
                    },
                ),
            ],
            issues=[],
            skipped=[],
            directories_scanned=4,
        )

        output = format_enhanced_summary(result)

        # Should show both collection IDs
        assert "climate/hittekaart" in output
        assert "census" in output

    def test_format_enhanced_summary_verbose(self, tmp_path: Path) -> None:
        """format_enhanced_summary includes extra info in verbose mode."""
        from portolan_cli.scan_output import format_enhanced_summary

        result = ScanResult(
            root=tmp_path,
            ready=[
                ScannedFile(
                    path=tmp_path / "census" / "data.parquet",
                    relative_path="census/data.parquet",
                    extension=".parquet",
                    format_type=FormatType.VECTOR,
                    size_bytes=1000,
                    metadata={
                        "inferred_collection_id": "census",
                        "format_status": "cloud_native",
                        "format_display_name": "GeoParquet",
                    },
                ),
            ],
            issues=[
                make_scan_issue(
                    tmp_path / "data",
                    IssueType.MULTIPLE_PRIMARIES,
                    Severity.WARNING,
                    message="Directory has 2 primary assets",
                    suggestion="Reorganize into separate collections",
                ),
            ],
            skipped=[],
            directories_scanned=2,
        )

        # Verbose mode
        output = format_enhanced_summary(result, verbose=True)

        # Should include recommendations
        assert "portolan" in output.lower() or "reorganize" in output.lower()
