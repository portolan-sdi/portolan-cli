"""Tests for collection ID path syntax (nested catalogs per ADR-0032)."""

import pytest

from portolan_cli.collection_id import (
    CollectionIdError,
    normalize_collection_id,
    validate_collection_id,
)


class TestCollectionIdPathSyntax:
    """Test path-style collection IDs for nested catalogs."""

    def test_validate_simple_path(self):
        """Test that simple nested path is valid."""
        is_valid, error = validate_collection_id("environment/air-quality")
        assert is_valid
        assert error is None

    def test_validate_deep_path(self):
        """Test that deeply nested path is valid."""
        is_valid, error = validate_collection_id("region/country/state/county")
        assert is_valid
        assert error is None

    def test_validate_path_with_numbers(self):
        """Test that paths with numbers are valid."""
        is_valid, error = validate_collection_id("landsat/collection-2/level-2")
        assert is_valid
        assert error is None

    def test_validate_path_with_underscores(self):
        """Test that paths with underscores are valid."""
        is_valid, error = validate_collection_id("env_data/air_quality")
        assert is_valid
        assert error is None

    def test_validate_path_rejects_uppercase(self):
        """Test that paths with uppercase letters are rejected."""
        is_valid, error = validate_collection_id("Environment/Air-Quality")
        assert not is_valid
        assert "uppercase" in error.lower()

    def test_validate_path_rejects_spaces(self):
        """Test that paths with spaces are rejected."""
        is_valid, error = validate_collection_id("environment/air quality")
        assert not is_valid
        assert "spaces" in error.lower()

    def test_validate_path_rejects_invalid_chars(self):
        """Test that paths with invalid characters are rejected."""
        is_valid, error = validate_collection_id("environment/air@quality")
        assert not is_valid
        assert "invalid character" in error.lower()

    def test_validate_path_rejects_leading_slash(self):
        """Test that paths with leading slash are rejected."""
        is_valid, error = validate_collection_id("/environment/air-quality")
        assert not is_valid
        # Leading slash fails the "must start with letter" check

    def test_validate_path_rejects_trailing_slash(self):
        """Test that paths with trailing slash are rejected."""
        is_valid, error = validate_collection_id("environment/air-quality/")
        assert not is_valid
        # Trailing slash creates invalid character

    def test_validate_path_rejects_double_slash(self):
        """Test that paths with double slashes are rejected."""
        is_valid, error = validate_collection_id("environment//air-quality")
        assert not is_valid
        # Double slash creates empty segment

    def test_validate_path_rejects_empty_segment(self):
        """Test that paths with empty segments are rejected."""
        is_valid, error = validate_collection_id("environment//air-quality")
        assert not is_valid

    def test_normalize_path_with_uppercase(self):
        """Test normalizing path with uppercase letters."""
        result = normalize_collection_id("Environment/Air-Quality")
        assert result == "environment/air-quality"

    def test_normalize_path_with_spaces(self):
        """Test normalizing path with spaces."""
        result = normalize_collection_id("environment/air quality")
        assert result == "environment/air-quality"

    def test_normalize_path_with_non_ascii(self):
        """Test normalizing path with non-ASCII characters."""
        result = normalize_collection_id("données/qualité")
        assert result == "donnees/qualite"

    def test_normalize_path_strips_slashes(self):
        """Test that leading/trailing slashes are stripped."""
        result = normalize_collection_id("/environment/air-quality/")
        assert result == "environment/air-quality"

    def test_normalize_path_collapses_double_slashes(self):
        """Test that double slashes are collapsed."""
        result = normalize_collection_id("environment//air-quality")
        assert result == "environment/air-quality"

    def test_normalize_empty_path_raises_error(self):
        """Test that empty path raises CollectionIdError."""
        with pytest.raises(CollectionIdError, match="cannot be empty"):
            normalize_collection_id("")

    def test_normalize_only_slashes_raises_error(self):
        """Test that path with only slashes raises error."""
        with pytest.raises(CollectionIdError, match="no valid characters remain"):
            normalize_collection_id("///")


class TestCollectionIdSingleNames:
    """Test that single-segment collection IDs still work (flat collections)."""

    def test_validate_simple_id(self):
        """Test that simple collection ID is valid."""
        is_valid, error = validate_collection_id("demographics")
        assert is_valid
        assert error is None

    def test_validate_id_with_hyphens(self):
        """Test that collection ID with hyphens is valid."""
        is_valid, error = validate_collection_id("census-2020")
        assert is_valid
        assert error is None

    def test_validate_id_with_underscores(self):
        """Test that collection ID with underscores is valid."""
        is_valid, error = validate_collection_id("air_quality")
        assert is_valid
        assert error is None
