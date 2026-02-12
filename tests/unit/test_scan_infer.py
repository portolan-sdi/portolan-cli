"""Unit tests for portolan_cli/scan_infer.py.

Tests collection inference from filename patterns.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.scan_infer import (
    CollectionSuggestion,
    detect_pattern_marker,
    extract_numeric_groups,
    find_common_prefix,
    infer_collections,
)


@pytest.mark.unit
class TestCollectionSuggestion:
    """Tests for CollectionSuggestion dataclass."""

    def test_valid_suggestion(self, tmp_path: Path) -> None:
        """CollectionSuggestion can be created with valid values."""
        suggestion = CollectionSuggestion(
            suggested_name="flood_rp",
            files=(tmp_path / "flood_rp10.tif", tmp_path / "flood_rp100.tif"),
            pattern_type="marker",
            confidence=0.85,
            reason="Detected return period pattern",
        )
        assert suggestion.suggested_name == "flood_rp"
        assert len(suggestion.files) == 2
        assert suggestion.confidence == 0.85

    def test_invalid_confidence_too_high(self, tmp_path: Path) -> None:
        """CollectionSuggestion rejects confidence > 1.0."""
        with pytest.raises(ValueError, match="confidence"):
            CollectionSuggestion(
                suggested_name="test",
                files=(tmp_path / "a.tif", tmp_path / "b.tif"),
                pattern_type="numeric",
                confidence=1.5,
                reason="test",
            )

    def test_invalid_confidence_negative(self, tmp_path: Path) -> None:
        """CollectionSuggestion rejects negative confidence."""
        with pytest.raises(ValueError, match="confidence"):
            CollectionSuggestion(
                suggested_name="test",
                files=(tmp_path / "a.tif", tmp_path / "b.tif"),
                pattern_type="numeric",
                confidence=-0.1,
                reason="test",
            )

    def test_invalid_files_too_few(self, tmp_path: Path) -> None:
        """CollectionSuggestion requires at least 2 files."""
        with pytest.raises(ValueError, match="files"):
            CollectionSuggestion(
                suggested_name="test",
                files=(tmp_path / "a.tif",),  # Only 1 file
                pattern_type="numeric",
                confidence=0.8,
                reason="test",
            )

    def test_to_dict(self, tmp_path: Path) -> None:
        """CollectionSuggestion.to_dict returns expected structure."""
        suggestion = CollectionSuggestion(
            suggested_name="flood_rp",
            files=(tmp_path / "flood_rp10.tif", tmp_path / "flood_rp100.tif"),
            pattern_type="marker",
            confidence=0.85,
            reason="Detected return period pattern",
        )
        d = suggestion.to_dict()
        assert d["suggested_name"] == "flood_rp"
        assert len(d["files"]) == 2
        assert d["pattern_type"] == "marker"
        assert d["confidence"] == 0.85


@pytest.mark.unit
class TestFindCommonPrefix:
    """Tests for find_common_prefix function."""

    def test_common_prefix_found(self) -> None:
        """find_common_prefix finds shared prefix."""
        names = ["flood_rp10.tif", "flood_rp50.tif", "flood_rp100.tif"]
        result = find_common_prefix(names)
        assert result == "flood_rp"

    def test_common_prefix_short_rejected(self) -> None:
        """find_common_prefix rejects prefix < 3 chars."""
        names = ["a1.tif", "a2.tif", "a3.tif"]
        result = find_common_prefix(names)
        # "a" is too short, return None
        assert result is None

    def test_no_common_prefix(self) -> None:
        """find_common_prefix returns None for unrelated names."""
        names = ["flood.tif", "census.geojson", "imagery.tif"]
        result = find_common_prefix(names)
        assert result is None

    def test_single_file_returns_none(self) -> None:
        """find_common_prefix returns None for single file."""
        names = ["flood_rp10.tif"]
        result = find_common_prefix(names)
        assert result is None

    def test_empty_list_returns_none(self) -> None:
        """find_common_prefix returns None for empty list."""
        names: list[str] = []
        result = find_common_prefix(names)
        assert result is None


@pytest.mark.unit
class TestExtractNumericGroups:
    """Tests for extract_numeric_groups function."""

    def test_numeric_suffix_groups(self) -> None:
        """extract_numeric_groups groups by numeric suffix."""
        names = ["flood_rp10.tif", "flood_rp50.tif", "flood_rp100.tif", "census_2020.parquet"]
        result = extract_numeric_groups(names)

        # Should have group for flood_rp
        assert "flood_rp" in result
        assert len(result["flood_rp"]) == 3

    def test_single_file_not_grouped(self) -> None:
        """extract_numeric_groups ignores single files."""
        names = ["flood_rp10.tif", "census_2020.parquet"]
        result = extract_numeric_groups(names)

        # Single items are not returned as groups
        assert all(len(v) >= 2 for v in result.values())

    def test_year_pattern_grouped(self) -> None:
        """extract_numeric_groups handles year patterns."""
        names = ["census_2010.parquet", "census_2015.parquet", "census_2020.parquet"]
        result = extract_numeric_groups(names)

        assert "census_" in result or "census" in result

    def test_level_pattern_grouped(self) -> None:
        """extract_numeric_groups handles level patterns (L1, L2)."""
        names = ["admin_L1.geojson", "admin_L2.geojson", "admin_L3.geojson"]
        result = extract_numeric_groups(names)

        # Should group by admin_L
        assert any("admin" in k for k in result)


@pytest.mark.unit
class TestDetectPatternMarker:
    """Tests for detect_pattern_marker function."""

    def test_return_period_pattern(self) -> None:
        """detect_pattern_marker finds rp pattern."""
        names = ["flood_rp10.tif", "flood_rp50.tif", "flood_rp100.tif"]
        result = detect_pattern_marker(names)

        assert result is not None
        assert result[0] == "flood"  # base name
        assert result[1] == "return_period"  # pattern type

    def test_level_pattern(self) -> None:
        """detect_pattern_marker finds L pattern."""
        names = ["admin_L1.geojson", "admin_L2.geojson"]
        result = detect_pattern_marker(names)

        assert result is not None
        assert "admin" in result[0]
        assert result[1] == "level"

    def test_version_pattern(self) -> None:
        """detect_pattern_marker finds v pattern."""
        names = ["data_v1.parquet", "data_v2.parquet"]
        result = detect_pattern_marker(names)

        assert result is not None
        assert "data" in result[0]
        assert result[1] == "version"

    def test_no_pattern_found(self) -> None:
        """detect_pattern_marker returns None for no patterns."""
        names = ["file1.tif", "file2.tif", "file3.tif"]
        result = detect_pattern_marker(names)

        assert result is None


@pytest.mark.unit
class TestInferCollections:
    """Tests for infer_collections function."""

    def test_infers_from_numeric_pattern(self, tmp_path: Path) -> None:
        """infer_collections suggests groups from numeric patterns."""
        from portolan_cli.scan import FormatType, ScannedFile

        files = [
            ScannedFile(
                path=tmp_path / "flood_rp10.tif",
                relative_path="flood_rp10.tif",
                extension=".tif",
                format_type=FormatType.RASTER,
                size_bytes=100,
            ),
            ScannedFile(
                path=tmp_path / "flood_rp50.tif",
                relative_path="flood_rp50.tif",
                extension=".tif",
                format_type=FormatType.RASTER,
                size_bytes=100,
            ),
            ScannedFile(
                path=tmp_path / "flood_rp100.tif",
                relative_path="flood_rp100.tif",
                extension=".tif",
                format_type=FormatType.RASTER,
                size_bytes=100,
            ),
        ]

        suggestions = infer_collections(files)

        assert len(suggestions) >= 1
        assert any("flood" in s.suggested_name.lower() for s in suggestions)

    def test_no_suggestions_for_unrelated(self, tmp_path: Path) -> None:
        """infer_collections returns empty for unrelated files."""
        from portolan_cli.scan import FormatType, ScannedFile

        files = [
            ScannedFile(
                path=tmp_path / "census.parquet",
                relative_path="census.parquet",
                extension=".parquet",
                format_type=FormatType.VECTOR,
                size_bytes=100,
            ),
            ScannedFile(
                path=tmp_path / "imagery.tif",
                relative_path="imagery.tif",
                extension=".tif",
                format_type=FormatType.RASTER,
                size_bytes=100,
            ),
        ]

        suggestions = infer_collections(files)

        assert suggestions == []

    def test_confidence_filtering(self, tmp_path: Path) -> None:
        """infer_collections respects min_confidence threshold."""
        from portolan_cli.scan import FormatType, ScannedFile

        files = [
            ScannedFile(
                path=tmp_path / "data_v1.parquet",
                relative_path="data_v1.parquet",
                extension=".parquet",
                format_type=FormatType.VECTOR,
                size_bytes=100,
            ),
            ScannedFile(
                path=tmp_path / "data_v2.parquet",
                relative_path="data_v2.parquet",
                extension=".parquet",
                format_type=FormatType.VECTOR,
                size_bytes=100,
            ),
        ]

        # With high threshold, might filter out weaker suggestions
        suggestions_high = infer_collections(files, min_confidence=0.95)
        suggestions_low = infer_collections(files, min_confidence=0.1)

        # Lower threshold should have same or more suggestions
        assert len(suggestions_low) >= len(suggestions_high)

    def test_sorted_by_confidence(self, tmp_path: Path) -> None:
        """infer_collections returns results sorted by confidence descending."""
        from portolan_cli.scan import FormatType, ScannedFile

        files = [
            ScannedFile(
                path=tmp_path / "flood_rp10.tif",
                relative_path="flood_rp10.tif",
                extension=".tif",
                format_type=FormatType.RASTER,
                size_bytes=100,
            ),
            ScannedFile(
                path=tmp_path / "flood_rp50.tif",
                relative_path="flood_rp50.tif",
                extension=".tif",
                format_type=FormatType.RASTER,
                size_bytes=100,
            ),
            ScannedFile(
                path=tmp_path / "flood_rp100.tif",
                relative_path="flood_rp100.tif",
                extension=".tif",
                format_type=FormatType.RASTER,
                size_bytes=100,
            ),
        ]

        suggestions = infer_collections(files)

        if len(suggestions) >= 2:
            # Should be sorted descending by confidence
            for i in range(len(suggestions) - 1):
                assert suggestions[i].confidence >= suggestions[i + 1].confidence

    def test_duplicate_filenames_across_directories(self, tmp_path: Path) -> None:
        """infer_collections handles duplicate filenames across directories.

        Bug fix: path_by_name was a dict that lost files when the same
        filename appeared in different directories (e.g., 2020/rivers.geojson
        and 2021/rivers.geojson). Now uses multimap to track all paths.
        """
        from portolan_cli.scan import FormatType, ScannedFile

        # Create directory structure with duplicate filenames
        dir_2020 = tmp_path / "2020"
        dir_2021 = tmp_path / "2021"
        dir_2020.mkdir()
        dir_2021.mkdir()

        # Files with SAME name in different directories
        files = [
            ScannedFile(
                path=dir_2020 / "data_v1.parquet",
                relative_path="2020/data_v1.parquet",
                extension=".parquet",
                format_type=FormatType.VECTOR,
                size_bytes=100,
            ),
            ScannedFile(
                path=dir_2020 / "data_v2.parquet",
                relative_path="2020/data_v2.parquet",
                extension=".parquet",
                format_type=FormatType.VECTOR,
                size_bytes=100,
            ),
            ScannedFile(
                path=dir_2021 / "data_v1.parquet",  # DUPLICATE NAME
                relative_path="2021/data_v1.parquet",
                extension=".parquet",
                format_type=FormatType.VECTOR,
                size_bytes=100,
            ),
            ScannedFile(
                path=dir_2021 / "data_v2.parquet",  # DUPLICATE NAME
                relative_path="2021/data_v2.parquet",
                extension=".parquet",
                format_type=FormatType.VECTOR,
                size_bytes=100,
            ),
        ]

        suggestions = infer_collections(files)

        # Should find a suggestion that includes ALL 4 files, not just 2
        # (before the fix, only 2 files would be included due to dict overwrite)
        if suggestions:
            # Find suggestion with most files
            max_files = max(len(s.files) for s in suggestions)
            # Should include files from BOTH directories
            assert max_files >= 3, (
                f"Expected at least 3 files in suggestion, got {max_files}. "
                "This suggests path_by_name dict collision is still occurring."
            )
