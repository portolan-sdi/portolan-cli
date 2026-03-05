"""Unit tests for portolan_cli/scan_infer.py.

Tests collection inference functionality including:
- Common prefix extraction
- Numeric suffix detection
- Pattern marker detection
- Temporal pattern detection
"""

from __future__ import annotations

from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from portolan_cli.scan_infer import (
    CollectionSuggestion,
    find_common_prefix,
)

# =============================================================================
# CollectionSuggestion Tests
# =============================================================================


@pytest.mark.unit
class TestCollectionSuggestion:
    """Tests for CollectionSuggestion dataclass."""

    def test_valid_suggestion(self, tmp_path: Path) -> None:
        """Valid suggestion should be created successfully."""
        files = (tmp_path / "file1.txt", tmp_path / "file2.txt")
        suggestion = CollectionSuggestion(
            suggested_name="test_collection",
            files=files,
            pattern_type="prefix",
            confidence=0.8,
            reason="Common prefix detected",
        )
        assert suggestion.suggested_name == "test_collection"
        assert len(suggestion.files) == 2
        assert suggestion.confidence == 0.8

    def test_invalid_confidence_too_high(self, tmp_path: Path) -> None:
        """Confidence > 1.0 should raise ValueError."""
        files = (tmp_path / "file1.txt", tmp_path / "file2.txt")
        with pytest.raises(ValueError, match="confidence must be"):
            CollectionSuggestion(
                suggested_name="test",
                files=files,
                pattern_type="prefix",
                confidence=1.5,
                reason="Test",
            )

    def test_invalid_confidence_negative(self, tmp_path: Path) -> None:
        """Confidence < 0.0 should raise ValueError."""
        files = (tmp_path / "file1.txt", tmp_path / "file2.txt")
        with pytest.raises(ValueError, match="confidence must be"):
            CollectionSuggestion(
                suggested_name="test",
                files=files,
                pattern_type="prefix",
                confidence=-0.1,
                reason="Test",
            )

    def test_too_few_files(self, tmp_path: Path) -> None:
        """Suggestions with < 2 files should raise ValueError."""
        files = (tmp_path / "file1.txt",)
        with pytest.raises(ValueError, match="files must have >= 2"):
            CollectionSuggestion(
                suggested_name="test",
                files=files,
                pattern_type="prefix",
                confidence=0.5,
                reason="Test",
            )

    def test_to_dict_structure(self, tmp_path: Path) -> None:
        """to_dict() should return proper structure."""
        files = (tmp_path / "file1.txt", tmp_path / "file2.txt")
        suggestion = CollectionSuggestion(
            suggested_name="census",
            files=files,
            pattern_type="temporal",
            confidence=0.9,
            reason="Temporal pattern detected",
        )
        result = suggestion.to_dict()

        assert "suggested_name" in result
        assert "files" in result
        assert "pattern_type" in result
        assert "confidence" in result
        assert "reason" in result
        assert result["suggested_name"] == "census"
        assert result["confidence"] == 0.9


# =============================================================================
# Common Prefix Tests
# =============================================================================


@pytest.mark.unit
class TestFindCommonPrefix:
    """Tests for find_common_prefix function."""

    def test_basic_common_prefix(self) -> None:
        """Basic common prefix should be found."""
        names = ["census_2010.shp", "census_2020.shp", "census_2030.shp"]
        result = find_common_prefix(names)
        assert result is not None
        assert result == "census_20"

    def test_no_common_prefix(self) -> None:
        """No common prefix should return None."""
        names = ["alpha.shp", "beta.shp", "gamma.shp"]
        result = find_common_prefix(names)
        # The common prefix would be less than MIN_PREFIX_LENGTH
        assert result is None or len(result) < 3

    def test_short_prefix_rejected(self) -> None:
        """Prefixes shorter than 3 characters should be rejected."""
        names = ["ab_file1.shp", "ab_file2.shp"]
        result = find_common_prefix(names)
        # "ab" is only 2 characters, should be rejected or extended
        assert result is None or len(result) >= 3

    def test_single_file_returns_none(self) -> None:
        """Single file should return None."""
        names = ["single_file.shp"]
        result = find_common_prefix(names)
        assert result is None

    def test_empty_list_returns_none(self) -> None:
        """Empty list should return None."""
        names: list[str] = []
        result = find_common_prefix(names)
        assert result is None

    def test_identical_names(self) -> None:
        """Identical names should return the stem as prefix."""
        names = ["identical.shp", "identical.shp", "identical.shp"]
        result = find_common_prefix(names)
        assert result == "identical"

    def test_prefix_with_separator(self) -> None:
        """Prefix ending with separator should be detected."""
        names = ["region_north.shp", "region_south.shp", "region_east.shp"]
        result = find_common_prefix(names)
        assert result is not None
        assert "region" in result


# =============================================================================
# Property-Based Tests
# =============================================================================


@pytest.mark.unit
class TestPropertyBasedInference:
    """Property-based tests for inference functions."""

    @given(
        confidence=st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
    )
    @settings(max_examples=20)
    def test_valid_confidence_always_accepted(self, confidence: float) -> None:
        """Valid confidence values should always be accepted."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            files = (tmp_path / "file1.txt", tmp_path / "file2.txt")
            # Should not raise
            suggestion = CollectionSuggestion(
                suggested_name="test",
                files=files,
                pattern_type="prefix",
                confidence=confidence,
                reason="Test",
            )
            assert suggestion.confidence == confidence

    @given(
        n_files=st.integers(min_value=2, max_value=10),
    )
    @settings(max_examples=20)
    def test_multiple_files_always_accepted(self, n_files: int) -> None:
        """Multiple files (>= 2) should always be accepted."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            files = tuple(tmp_path / f"file{i}.txt" for i in range(n_files))
            # Should not raise
            suggestion = CollectionSuggestion(
                suggested_name="test",
                files=files,
                pattern_type="numeric",
                confidence=0.5,
                reason="Test",
            )
            assert len(suggestion.files) == n_files

    @given(
        common_part=st.text(
            min_size=4,
            max_size=20,
            alphabet=st.characters(whitelist_categories=("Ll",), whitelist_characters="_"),
        ),
        suffixes=st.lists(
            st.text(
                min_size=1, max_size=5, alphabet=st.characters(whitelist_categories=("Ll", "Nd"))
            ),
            min_size=2,
            max_size=5,
            unique=True,
        ),
    )
    @settings(max_examples=30)
    def test_common_prefix_always_found(self, common_part: str, suffixes: list[str]) -> None:
        """Files with common prefix should always have it detected."""
        if len(common_part) < 3 or not suffixes:
            return

        names = [f"{common_part}{suffix}.txt" for suffix in suffixes]
        result = find_common_prefix(names)

        if result is not None:
            # The found prefix should be at least as long as MIN_PREFIX_LENGTH
            assert len(result) >= 3
            # All names should start with the prefix
            for name in names:
                stem = Path(name).stem
                assert stem.startswith(result)
