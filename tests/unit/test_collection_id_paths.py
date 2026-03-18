"""Tests for collection ID path syntax (nested catalogs per ADR-0032)."""

import pytest

from portolan_cli.collection_id import (
    CollectionIdError,
    normalize_collection_id,
    validate_collection_id,
)

pytestmark = pytest.mark.unit


@pytest.mark.unit
class TestCollectionIdPathSyntax:
    """Test path-style collection IDs for nested catalogs."""

    def test_validate_simple_path(self) -> None:
        """Test that simple nested path is valid."""
        is_valid, error = validate_collection_id("environment/air-quality")
        assert is_valid
        assert error is None

    def test_validate_deep_path(self) -> None:
        """Test that deeply nested path is valid."""
        is_valid, error = validate_collection_id("region/country/state/county")
        assert is_valid
        assert error is None

    def test_validate_path_with_numbers(self) -> None:
        """Test that paths with numbers are valid."""
        is_valid, error = validate_collection_id("landsat/collection-2/level-2")
        assert is_valid
        assert error is None

    def test_validate_path_with_underscores(self) -> None:
        """Test that paths with underscores are valid."""
        is_valid, error = validate_collection_id("env_data/air_quality")
        assert is_valid
        assert error is None

    def test_validate_path_rejects_uppercase(self) -> None:
        """Test that paths with uppercase letters are rejected."""
        is_valid, error = validate_collection_id("Environment/Air-Quality")
        assert not is_valid
        assert "uppercase" in error.lower()

    def test_validate_path_rejects_spaces(self) -> None:
        """Test that paths with spaces are rejected."""
        is_valid, error = validate_collection_id("environment/air quality")
        assert not is_valid
        assert "spaces" in error.lower()

    def test_validate_path_rejects_invalid_chars(self) -> None:
        """Test that paths with invalid characters are rejected."""
        is_valid, error = validate_collection_id("environment/air@quality")
        assert not is_valid
        assert "invalid character" in error.lower()

    def test_validate_path_rejects_leading_slash(self) -> None:
        """Test that paths with leading slash are rejected."""
        is_valid, error = validate_collection_id("/environment/air-quality")
        assert not is_valid
        # Leading slash fails the "must start with letter" check

    def test_validate_path_rejects_trailing_slash(self) -> None:
        """Test that paths with trailing slash are rejected."""
        is_valid, error = validate_collection_id("environment/air-quality/")
        assert not is_valid
        # Trailing slash creates invalid character

    def test_validate_path_rejects_double_slash(self) -> None:
        """Test that paths with double slashes are rejected."""
        is_valid, error = validate_collection_id("environment//air-quality")
        assert not is_valid
        # Double slash creates empty segment

    def test_validate_path_rejects_empty_segment(self) -> None:
        """Test that paths with empty segments are rejected."""
        is_valid, error = validate_collection_id("environment//air-quality")
        assert not is_valid

    def test_validate_path_rejects_segment_starting_with_number(self) -> None:
        """Test that path segments starting with numbers are rejected."""
        is_valid, error = validate_collection_id("environment/2024")
        assert not is_valid
        is_valid, error = validate_collection_id("2024/january")
        assert not is_valid

    def test_validate_path_rejects_segment_starting_with_hyphen(self) -> None:
        """Test that path segments starting with hyphens are rejected."""
        is_valid, error = validate_collection_id("environment/-air")
        assert not is_valid
        is_valid, error = validate_collection_id("-environment/air")
        assert not is_valid

    def test_validate_path_rejects_segment_starting_with_underscore(self) -> None:
        """Test that path segments starting with underscores are rejected."""
        is_valid, error = validate_collection_id("environment/_quality")
        assert not is_valid
        is_valid, error = validate_collection_id("_environment/quality")
        assert not is_valid

    def test_normalize_path_with_uppercase(self) -> None:
        """Test normalizing path with uppercase letters."""
        result = normalize_collection_id("Environment/Air-Quality")
        assert result == "environment/air-quality"

    def test_normalize_path_with_spaces(self) -> None:
        """Test normalizing path with spaces."""
        result = normalize_collection_id("environment/air quality")
        assert result == "environment/air-quality"

    def test_normalize_path_with_non_ascii(self) -> None:
        """Test normalizing path with non-ASCII characters."""
        result = normalize_collection_id("données/qualité")
        assert result == "donnees/qualite"

    def test_normalize_path_strips_slashes(self) -> None:
        """Test that leading/trailing slashes are stripped."""
        result = normalize_collection_id("/environment/air-quality/")
        assert result == "environment/air-quality"

    def test_normalize_path_collapses_double_slashes(self) -> None:
        """Test that double slashes are collapsed."""
        result = normalize_collection_id("environment//air-quality")
        assert result == "environment/air-quality"

    def test_normalize_empty_path_raises_error(self) -> None:
        """Test that empty path raises CollectionIdError."""
        with pytest.raises(CollectionIdError, match="cannot be empty"):
            normalize_collection_id("")

    def test_normalize_only_slashes_raises_error(self) -> None:
        """Test that path with only slashes raises error."""
        with pytest.raises(CollectionIdError, match="no valid characters remain"):
            normalize_collection_id("///")

    def test_normalize_path_with_numeric_segment(self) -> None:
        """Test normalizing path with segment starting with number."""
        result = normalize_collection_id("environment/2024")
        assert result == "environment/n2024"

    def test_normalize_path_with_hyphen_segment(self) -> None:
        """Test normalizing path with segment starting with hyphen."""
        result = normalize_collection_id("environment/-air")
        assert result == "environment/air"  # Leading hyphen stripped from segment

    def test_normalize_path_with_underscore_segment(self) -> None:
        """Test normalizing path with segment starting with underscore."""
        result = normalize_collection_id("environment/_quality")
        # Underscore is preserved, segment prefixed with 'n' since doesn't start with letter
        assert result == "environment/n_quality"

    def test_normalize_numeric_first_segment(self) -> None:
        """Test normalizing path where first segment starts with number."""
        result = normalize_collection_id("2024/january")
        assert result == "n2024/january"


@pytest.mark.unit
class TestCollectionIdSingleNames:
    """Test that single-segment collection IDs still work (flat collections)."""

    def test_validate_simple_id(self) -> None:
        """Test that simple collection ID is valid."""
        is_valid, error = validate_collection_id("demographics")
        assert is_valid
        assert error is None

    def test_validate_id_with_hyphens(self) -> None:
        """Test that collection ID with hyphens is valid."""
        is_valid, error = validate_collection_id("census-2020")
        assert is_valid
        assert error is None

    def test_validate_id_with_underscores(self) -> None:
        """Test that collection ID with underscores is valid."""
        is_valid, error = validate_collection_id("air_quality")
        assert is_valid
        assert error is None
