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
) -> ScanIssue:
    """Create a ScanIssue for testing."""
    return ScanIssue(
        path=path,
        relative_path=str(path.name),
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
