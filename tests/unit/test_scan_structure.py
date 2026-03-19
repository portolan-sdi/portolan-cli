"""Unit tests for scan structure validation.

These tests follow TDD: written FIRST, verified to FAIL, then implementation added.

Tests cover structural validation of directory hierarchies:
- Valid nested structures (no issues)
- Mixed flat/multi-item structures (WARNING)
- GeoParquet with plain Parquet companions (valid)
- Multiple GeoParquet in same directory (WARNING)
- Deep nesting without data at intermediate levels (valid)

Test fixtures used:
- geoparquet_with_companions/: 1 GeoParquet + 1 plain Parquet (valid)
- multiple_geoparquet/: 2 GeoParquet files (should warn)
- deep_nested/: Deep nesting with data only at leaves (valid)
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Fixture path helper
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "scan"


@pytest.fixture
def fixtures_dir() -> Path:
    """Return the path to scan test fixtures."""
    return FIXTURES_DIR


# =============================================================================
# Test Case 1: Valid Nested Structure (No Issues)
# =============================================================================


@pytest.mark.unit
class TestValidNestedStructure:
    """Tests for valid nested directory structures with no structural issues."""

    def test_nested_structure_with_leaf_data_only_is_valid(self, fixtures_dir: Path) -> None:
        """Nested directories with data only at leaf levels produce no structure issues.

        Uses nested/ fixture: census/2020/boundaries.geojson, census/2022/boundaries.geojson
        This represents intentional year-based organization.
        """
        from portolan_cli.scan import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "nested")

        # Check for structure-related issues
        structure_issues = [
            i for i in result.issues if i.issue_type in (IssueType.MIXED_FLAT_MULTIITEM,)
        ]
        # Should have no structural issues - data is only at leaf directories
        assert len(structure_issues) == 0

    def test_three_level_nested_no_structure_issues(self, fixtures_dir: Path) -> None:
        """Three-level nesting (e.g., GAUL_L2/by_country/AFG/) is valid.

        Uses three_level_nested/ fixture: organized by country code at leaf level.
        """
        from portolan_cli.scan import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "three_level_nested")

        # No mixed structure issues expected
        mixed_issues = [i for i in result.issues if i.issue_type == IssueType.MIXED_FLAT_MULTIITEM]
        assert len(mixed_issues) == 0


# =============================================================================
# Test Case 2: Data at Intermediate Level = WARNING (MIXED_FLAT_MULTIITEM)
# =============================================================================


@pytest.mark.unit
class TestMixedFlatMultiitem:
    """Tests for detecting mixed flat/multi-item structures.

    When a directory has BOTH files at root AND files in subdirectories,
    this creates structural ambiguity that should produce a WARNING.
    """

    def test_files_at_root_and_in_subdirs_warns(self, tmp_path: Path) -> None:
        """Directory with files at root AND in subdirectories triggers warning.

        This is the classic MIXED_FLAT_MULTIITEM case: unclear whether the
        directory represents a single collection (flat) or multiple items (nested).
        """
        from portolan_cli.scan import IssueType, Severity, scan_directory

        # Create file at root level
        root_file = tmp_path / "root_data.geojson"
        root_file.write_text('{"type": "FeatureCollection", "features": []}')

        # Create file in subdirectory
        subdir = tmp_path / "subitem"
        subdir.mkdir()
        nested_file = subdir / "nested_data.geojson"
        nested_file.write_text('{"type": "FeatureCollection", "features": []}')

        result = scan_directory(tmp_path)

        mixed_issues = [i for i in result.issues if i.issue_type == IssueType.MIXED_FLAT_MULTIITEM]
        assert len(mixed_issues) >= 1
        assert mixed_issues[0].severity == Severity.WARNING

    def test_mixed_structure_suggests_reorganization(self, tmp_path: Path) -> None:
        """MIXED_FLAT_MULTIITEM issues include a suggestion to reorganize."""
        from portolan_cli.scan import IssueType, scan_directory

        # Create mixed structure
        (tmp_path / "root.geojson").write_text('{"type": "FeatureCollection", "features": []}')
        subdir = tmp_path / "nested"
        subdir.mkdir()
        (subdir / "nested.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        result = scan_directory(tmp_path)

        mixed_issues = [i for i in result.issues if i.issue_type == IssueType.MIXED_FLAT_MULTIITEM]
        assert len(mixed_issues) >= 1
        assert mixed_issues[0].suggestion is not None
        # Should suggest organizing as either flat or hierarchical
        assert (
            "flat" in mixed_issues[0].suggestion.lower()
            or "hierarch" in mixed_issues[0].suggestion.lower()
        )


# =============================================================================
# Test Case 3: One GeoParquet + Multiple Plain Parquet = VALID
# =============================================================================


@pytest.mark.unit
class TestGeoparquetWithCompanions:
    """Tests for GeoParquet files with plain Parquet companions.

    A directory with ONE GeoParquet (primary geo-asset) plus multiple plain
    Parquet files (lookup tables, metadata) is a valid pattern. Only the
    GeoParquet is the primary; plain files are companions.
    """

    def test_one_geoparquet_with_plain_parquet_companion_is_valid(self, fixtures_dir: Path) -> None:
        """Directory with 1 GeoParquet + 1 plain Parquet produces no multiple-primary warning.

        Uses geoparquet_with_companions/ fixture:
        - data.parquet: GeoParquet (has geo metadata, geometry column)
        - lookup.parquet: Plain Parquet (no geo metadata)
        """
        from portolan_cli.scan import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "geoparquet_with_companions")

        # Should NOT have MULTIPLE_PRIMARIES warning - only one is a geo primary
        multiple_primary_issues = [
            i for i in result.issues if i.issue_type == IssueType.MULTIPLE_PRIMARIES
        ]
        assert len(multiple_primary_issues) == 0

    def test_geoparquet_companions_no_geo_primary_warning(self, fixtures_dir: Path) -> None:
        """GeoParquet with companions should not trigger MULTIPLE_GEO_PRIMARIES.

        The MULTIPLE_GEO_PRIMARIES issue type (distinct from MULTIPLE_PRIMARIES)
        is specifically for multiple GeoParquet files in the same directory.
        One geo + companions should not trigger this.
        """
        from portolan_cli.scan import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "geoparquet_with_companions")

        # Should NOT have MULTIPLE_GEO_PRIMARIES - only one GeoParquet
        geo_primary_issues = [
            i for i in result.issues if i.issue_type == IssueType.MULTIPLE_GEO_PRIMARIES
        ]
        assert len(geo_primary_issues) == 0

    def test_geoparquet_identified_as_ready(self, fixtures_dir: Path) -> None:
        """The GeoParquet file in the companion fixture is identified as ready."""
        from portolan_cli.scan import scan_directory

        result = scan_directory(fixtures_dir / "geoparquet_with_companions")

        ready_names = {f.path.name for f in result.ready}
        assert "data.parquet" in ready_names


# =============================================================================
# Test Case 4: Multiple GeoParquet in Same Directory = WARNING
# =============================================================================


@pytest.mark.unit
class TestMultipleGeoparquet:
    """Tests for detecting multiple GeoParquet files in the same directory.

    Multiple GeoParquet files in the same directory should produce a
    MULTIPLE_GEO_PRIMARIES warning, which is distinct from the generic
    MULTIPLE_PRIMARIES warning used for other formats.

    NOTE: These tests are skipped until MULTIPLE_GEO_PRIMARIES is implemented.
    Remove the skip markers when implementing the issue type.
    """

    def test_multiple_geoparquet_triggers_warning(self, fixtures_dir: Path) -> None:
        """Directory with 2+ GeoParquet files triggers MULTIPLE_GEO_PRIMARIES warning.

        Uses multiple_geoparquet/ fixture:
        - peilbuizen.parquet: GeoParquet (has geo metadata)
        - milieuzone.parquet: GeoParquet (has geo metadata)

        Both are primary geo-assets, so this is ambiguous for catalog structure.
        """
        from portolan_cli.scan import IssueType, Severity, scan_directory

        result = scan_directory(fixtures_dir / "multiple_geoparquet")

        # Should have MULTIPLE_GEO_PRIMARIES warning
        geo_issues = [i for i in result.issues if i.issue_type == IssueType.MULTIPLE_GEO_PRIMARIES]
        assert len(geo_issues) >= 1, (
            f"Expected MULTIPLE_GEO_PRIMARIES warning for 2 GeoParquet files. "
            f"Issues found: {[i.issue_type.value for i in result.issues]}"
        )
        assert geo_issues[0].severity == Severity.WARNING

    def test_multiple_geoparquet_distinct_from_multiple_primaries(self, fixtures_dir: Path) -> None:
        """MULTIPLE_GEO_PRIMARIES is a distinct issue type from MULTIPLE_PRIMARIES.

        This distinction matters because:
        - MULTIPLE_PRIMARIES: Generic warning for any multiple primaries
        - MULTIPLE_GEO_PRIMARIES: Specific to GeoParquet, enables targeted guidance
        """
        from portolan_cli.scan import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "multiple_geoparquet")

        # Should NOT use generic MULTIPLE_PRIMARIES for GeoParquet case
        # (though implementation may emit both; at minimum, geo-specific one should exist)
        issue_types = {i.issue_type for i in result.issues}
        assert IssueType.MULTIPLE_GEO_PRIMARIES in issue_types

    def test_multiple_geoparquet_message_references_geoparquet(self, fixtures_dir: Path) -> None:
        """MULTIPLE_GEO_PRIMARIES message should reference GeoParquet specifically."""
        from portolan_cli.scan import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "multiple_geoparquet")

        geo_issues = [i for i in result.issues if i.issue_type == IssueType.MULTIPLE_GEO_PRIMARIES]
        assert len(geo_issues) >= 1
        # Message should mention GeoParquet to distinguish from generic primaries
        assert (
            "geoparquet" in geo_issues[0].message.lower() or "geo" in geo_issues[0].message.lower()
        )


# =============================================================================
# Test Case 5: Deep Nesting Without Data at Intermediate = Valid
# =============================================================================


@pytest.mark.unit
class TestDeepNesting:
    """Tests for deeply nested directory structures.

    Deep nesting is valid when intermediate directories contain no data
    files, only serving as organizational hierarchy.
    """

    def test_deep_nesting_no_intermediate_data_is_valid(self, fixtures_dir: Path) -> None:
        """Deeply nested directories with data only at leaves produce no issues.

        Uses deep_nested/ fixture:
        - level1/level2/shallow_collection/data.parquet
        - level1/level2/level3/level4/level5/data.parquet

        Intermediate directories (level1, level2, level3, level4) have no data.
        """
        from portolan_cli.scan import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "deep_nested")

        # Should have no MIXED_FLAT_MULTIITEM issues
        mixed_issues = [i for i in result.issues if i.issue_type == IssueType.MIXED_FLAT_MULTIITEM]
        assert len(mixed_issues) == 0

    def test_deep_nesting_finds_all_leaf_files(self, fixtures_dir: Path) -> None:
        """Deeply nested scan finds all files at different nesting depths."""
        from portolan_cli.scan import scan_directory

        result = scan_directory(fixtures_dir / "deep_nested")

        # Should find both parquet files at different depths
        # Use as_posix() for cross-platform path comparison (Windows uses backslashes)
        ready_paths = {
            f.path.relative_to(fixtures_dir / "deep_nested").as_posix() for f in result.ready
        }
        assert "level1/level2/shallow_collection/data.parquet" in ready_paths
        assert "level1/level2/level3/level4/level5/data.parquet" in ready_paths

    def test_deep_nesting_mixed_depths_valid(self, fixtures_dir: Path) -> None:
        """Mixed nesting depths (shallow + deep) in same tree is valid.

        Uses mixed_depths/ fixture:
        - shallow_collection/data.parquet (depth 1)
        - theme/nested_collection/data.parquet (depth 2)

        Different depths are fine as long as no intermediate has data.
        """
        from portolan_cli.scan import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "mixed_depths")

        mixed_issues = [i for i in result.issues if i.issue_type == IssueType.MIXED_FLAT_MULTIITEM]
        # Different depths are valid - no files at intermediate levels
        assert len(mixed_issues) == 0


# =============================================================================
# Edge Cases and Regression Tests
# =============================================================================


@pytest.mark.unit
class TestStructureValidationEdgeCases:
    """Edge cases for structure validation."""

    def test_flat_collection_no_subdirs_is_valid(self, fixtures_dir: Path) -> None:
        """Flat collection (all files in root, no subdirs) is valid structure.

        Uses flat_collection/ fixture: 3 parquet files, no subdirectories.
        This is a valid "collection-level assets" pattern per ADR-0031.
        """
        from portolan_cli.scan import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "flat_collection")

        mixed_issues = [i for i in result.issues if i.issue_type == IssueType.MIXED_FLAT_MULTIITEM]
        # Flat structure is valid - no ambiguity
        assert len(mixed_issues) == 0

    def test_empty_intermediate_directories_not_flagged(self, tmp_path: Path) -> None:
        """Empty intermediate directories don't trigger structure warnings."""
        from portolan_cli.scan import IssueType, scan_directory

        # Create nested structure with empty intermediate
        leaf = tmp_path / "empty_parent" / "empty_middle" / "leaf"
        leaf.mkdir(parents=True)
        (leaf / "data.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        result = scan_directory(tmp_path)

        mixed_issues = [i for i in result.issues if i.issue_type == IssueType.MIXED_FLAT_MULTIITEM]
        assert len(mixed_issues) == 0


class TestCorruptedParquetScan:
    """Tests for scan behavior with corrupted Parquet files."""

    @pytest.mark.unit
    def test_corrupted_parquet_skipped_with_error(self, tmp_path: Path) -> None:
        """Corrupted .parquet file is skipped with INVALID_FORMAT reason."""
        from portolan_cli.scan import ScanOptions, scan_directory
        from portolan_cli.scan_classify import FileCategory, SkipReasonType

        # Create a fake parquet file
        data_dir = tmp_path / "collection"
        data_dir.mkdir()
        bad_parquet = data_dir / "corrupted.parquet"
        bad_parquet.write_text("this is not a valid parquet file")

        # Scan
        result = scan_directory(tmp_path, ScanOptions())

        # Should be in skipped, not ready
        assert len(result.ready) == 0
        assert len(result.skipped) == 1

        skipped = result.skipped[0]
        assert skipped.category == FileCategory.UNKNOWN
        assert skipped.reason_type == SkipReasonType.INVALID_FORMAT
        assert "not a valid Parquet file" in skipped.reason_message

    @pytest.mark.unit
    def test_valid_non_geo_parquet_is_tabular(self, tmp_path: Path, fixtures_dir: Path) -> None:
        """Valid Parquet without geo metadata is skipped as TABULAR_DATA."""
        from portolan_cli.scan import ScanOptions, scan_directory
        from portolan_cli.scan_classify import FileCategory, SkipReasonType

        # Use one of our companion fixtures (plain Parquet)
        src = fixtures_dir / "scan" / "geoparquet_with_companions" / "lookup.parquet"
        if not src.exists():
            pytest.skip("Test fixture not found")

        data_dir = tmp_path / "collection"
        data_dir.mkdir()
        import shutil

        shutil.copy(src, data_dir / "lookup.parquet")

        # Scan
        result = scan_directory(tmp_path, ScanOptions())

        # Should be skipped as tabular data
        assert len(result.ready) == 0
        assert len(result.skipped) == 1

        skipped = result.skipped[0]
        assert skipped.category == FileCategory.TABULAR_DATA
        assert skipped.reason_type == SkipReasonType.NOT_GEOSPATIAL
