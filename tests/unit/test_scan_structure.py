"""Unit tests for scan structure validation."""

from __future__ import annotations

from pathlib import Path

import pytest

# Fixture path helper
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "scan"


@pytest.fixture
def fixtures_dir() -> Path:
    """Return the path to scan test fixtures."""
    return FIXTURES_DIR


# Valid Nested Structure (No Issues)


@pytest.mark.unit
class TestValidNestedStructure:
    """Tests for valid nested directory structures with no structural issues."""

    def test_nested_structure_with_leaf_data_only_is_valid(self, fixtures_dir: Path) -> None:
        from portolan_cli.scan.core import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "nested")

        # Check for structure-related issues
        structure_issues = [
            i for i in result.issues if i.issue_type == IssueType.MIXED_FLAT_MULTIITEM
        ]
        # Should have no structural issues - data is only at leaf directories
        assert len(structure_issues) == 0

    def test_three_level_nested_no_structure_issues(self, fixtures_dir: Path) -> None:
        """Three-level nesting (e.g., GAUL_L2/by_country/AFG/) is valid.

        Uses three_level_nested/ fixture: organized by country code at leaf level.
        """
        from portolan_cli.scan.core import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "three_level_nested")

        # No mixed structure issues expected
        mixed_issues = [i for i in result.issues if i.issue_type == IssueType.MIXED_FLAT_MULTIITEM]
        assert len(mixed_issues) == 0


# Data at Intermediate Level = WARNING (MIXED_FLAT_MULTIITEM)


@pytest.mark.unit
class TestMixedFlatMultiitem:
    """Tests for detecting mixed flat/multi-item structures."""

    def test_files_at_root_and_in_subdirs_warns(self, tmp_path: Path) -> None:
        from portolan_cli.scan.core import IssueType, Severity, scan_directory

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
        from portolan_cli.scan.core import IssueType, scan_directory

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

    def test_mixed_structure_deep_nesting_flags_correct_directory(self, tmp_path: Path) -> None:
        from portolan_cli.scan.core import IssueType, scan_directory

        # Create file at root
        (tmp_path / "data.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        # Create deeply nested file
        deep = tmp_path / "level1" / "level2" / "level3"
        deep.mkdir(parents=True)
        (deep / "deep.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        result = scan_directory(tmp_path)

        mixed_issues = [i for i in result.issues if i.issue_type == IssueType.MIXED_FLAT_MULTIITEM]
        assert len(mixed_issues) == 1, f"Expected 1 issue, got {len(mixed_issues)}"

        # The flagged directory should be the root (which has files AND a descendant with files)
        assert mixed_issues[0].path == tmp_path, (
            f"Expected root to be flagged, got {mixed_issues[0].path}"
        )

    def test_mixed_structure_multiple_levels_with_files(self, tmp_path: Path) -> None:
        from portolan_cli.scan.core import IssueType, scan_directory

        # Level 0: root
        (tmp_path / "data.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        # Level 1: mid
        mid = tmp_path / "mid"
        mid.mkdir()
        (mid / "mid_data.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        # Level 2: deep
        deep = mid / "deep"
        deep.mkdir()
        (deep / "deep.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        result = scan_directory(tmp_path)

        mixed_issues = [i for i in result.issues if i.issue_type == IssueType.MIXED_FLAT_MULTIITEM]
        flagged_paths = {i.path for i in mixed_issues}

        # Root should be flagged (has files AND mid/deep have files)
        assert tmp_path in flagged_paths, "Root should be flagged"

        # Mid should be flagged (has files AND deep has files)
        assert mid in flagged_paths, "Mid should be flagged"

        # Deep should NOT be flagged (no children with files)
        assert deep not in flagged_paths, "Deep should NOT be flagged (leaf)"

        assert len(mixed_issues) == 2, f"Expected 2 issues, got {len(mixed_issues)}"


# One GeoParquet + Multiple Plain Parquet = VALID


@pytest.mark.unit
class TestGeoparquetWithCompanions:
    """Tests for GeoParquet files with plain Parquet companions."""

    def test_one_geoparquet_with_plain_parquet_companion_is_valid(self, fixtures_dir: Path) -> None:
        from portolan_cli.scan.core import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "geoparquet_with_companions")

        # Should NOT have MULTIPLE_PRIMARIES warning - only one is a geo primary
        multiple_primary_issues = [
            i for i in result.issues if i.issue_type == IssueType.MULTIPLE_PRIMARIES
        ]
        assert len(multiple_primary_issues) == 0

    def test_geoparquet_companions_no_geo_primary_warning(self, fixtures_dir: Path) -> None:
        from portolan_cli.scan.core import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "geoparquet_with_companions")

        # Should NOT have MULTIPLE_GEO_PRIMARIES - only one GeoParquet
        geo_primary_issues = [
            i for i in result.issues if i.issue_type == IssueType.MULTIPLE_GEO_PRIMARIES
        ]
        assert len(geo_primary_issues) == 0

    def test_geoparquet_identified_as_ready(self, fixtures_dir: Path) -> None:
        """The GeoParquet file in the companion fixture is identified as ready."""
        from portolan_cli.scan.core import scan_directory

        result = scan_directory(fixtures_dir / "geoparquet_with_companions")

        ready_names = {f.path.name for f in result.ready}
        assert "data.parquet" in ready_names


# Multiple GeoParquet in Same Directory = WARNING


@pytest.mark.unit
class TestMultipleGeoparquet:
    """Tests for detecting multiple GeoParquet files in the same directory."""

    def test_multiple_geoparquet_triggers_warning(self, fixtures_dir: Path) -> None:
        from portolan_cli.scan.core import IssueType, Severity, scan_directory

        result = scan_directory(fixtures_dir / "multiple_geoparquet")

        # Should have MULTIPLE_GEO_PRIMARIES warning
        geo_issues = [i for i in result.issues if i.issue_type == IssueType.MULTIPLE_GEO_PRIMARIES]
        assert len(geo_issues) >= 1, (
            f"Expected MULTIPLE_GEO_PRIMARIES warning for 2 GeoParquet files. "
            f"Issues found: {[i.issue_type.value for i in result.issues]}"
        )
        assert geo_issues[0].severity == Severity.WARNING

    def test_multiple_geoparquet_distinct_from_multiple_primaries(self, fixtures_dir: Path) -> None:
        from portolan_cli.scan.core import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "multiple_geoparquet")

        # Should NOT use generic MULTIPLE_PRIMARIES for GeoParquet case
        # (though implementation may emit both; at minimum, geo-specific one should exist)
        issue_types = {i.issue_type for i in result.issues}
        assert IssueType.MULTIPLE_GEO_PRIMARIES in issue_types

    def test_multiple_geoparquet_message_references_geoparquet(self, fixtures_dir: Path) -> None:
        """MULTIPLE_GEO_PRIMARIES message should reference GeoParquet specifically."""
        from portolan_cli.scan.core import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "multiple_geoparquet")

        geo_issues = [i for i in result.issues if i.issue_type == IssueType.MULTIPLE_GEO_PRIMARIES]
        assert len(geo_issues) >= 1
        # Message should mention GeoParquet to distinguish from generic primaries
        assert (
            "geoparquet" in geo_issues[0].message.lower() or "geo" in geo_issues[0].message.lower()
        )


# Deep Nesting Without Data at Intermediate = Valid


@pytest.mark.unit
class TestDeepNesting:
    """Tests for deeply nested directory structures."""

    def test_deep_nesting_no_intermediate_data_is_valid(self, fixtures_dir: Path) -> None:
        from portolan_cli.scan.core import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "deep_nested")

        # Should have no MIXED_FLAT_MULTIITEM issues
        mixed_issues = [i for i in result.issues if i.issue_type == IssueType.MIXED_FLAT_MULTIITEM]
        assert len(mixed_issues) == 0

    def test_deep_nesting_finds_all_leaf_files(self, fixtures_dir: Path) -> None:
        """Deeply nested scan finds all files at different nesting depths."""
        from portolan_cli.scan.core import scan_directory

        result = scan_directory(fixtures_dir / "deep_nested")

        # Should find both parquet files at different depths
        # Use as_posix() for cross-platform path comparison (Windows uses backslashes)
        ready_paths = {
            f.path.relative_to(fixtures_dir / "deep_nested").as_posix() for f in result.ready
        }
        assert "level1/level2/shallow_collection/data.parquet" in ready_paths
        assert "level1/level2/level3/level4/level5/data.parquet" in ready_paths

    def test_deep_nesting_mixed_depths_valid(self, fixtures_dir: Path) -> None:
        from portolan_cli.scan.core import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "mixed_depths")

        mixed_issues = [i for i in result.issues if i.issue_type == IssueType.MIXED_FLAT_MULTIITEM]
        # Different depths are valid - no files at intermediate levels
        assert len(mixed_issues) == 0


# Edge Cases and Regression Tests


@pytest.mark.unit
class TestStructureValidationEdgeCases:
    """Edge cases for structure validation."""

    def test_flat_collection_no_subdirs_is_valid(self, fixtures_dir: Path) -> None:
        from portolan_cli.scan.core import IssueType, scan_directory

        result = scan_directory(fixtures_dir / "flat_collection")

        mixed_issues = [i for i in result.issues if i.issue_type == IssueType.MIXED_FLAT_MULTIITEM]
        # Flat structure is valid - no ambiguity
        assert len(mixed_issues) == 0

    def test_empty_intermediate_directories_not_flagged(self, tmp_path: Path) -> None:
        """Empty intermediate directories don't trigger structure warnings."""
        from portolan_cli.scan.core import IssueType, scan_directory

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
        from portolan_cli.scan.classify import FileCategory, SkippedFile, SkipReasonType
        from portolan_cli.scan.core import ScanOptions, scan_directory

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
        assert isinstance(skipped, SkippedFile)
        assert skipped.category == FileCategory.UNKNOWN
        assert skipped.reason_type == SkipReasonType.INVALID_FORMAT
        assert "not a valid Parquet file" in skipped.reason_message

    @pytest.mark.unit
    def test_valid_non_geo_parquet_is_tabular(self, tmp_path: Path, fixtures_dir: Path) -> None:
        """Valid Parquet without geo metadata is skipped as TABULAR_DATA."""
        from portolan_cli.scan.classify import FileCategory, SkippedFile, SkipReasonType
        from portolan_cli.scan.core import ScanOptions, scan_directory

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
        assert isinstance(skipped, SkippedFile)
        assert skipped.category == FileCategory.TABULAR_DATA
        assert skipped.reason_type == SkipReasonType.NOT_GEOSPATIAL
