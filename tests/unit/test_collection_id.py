"""Tests for collection ID validation and normalization.

Per portolan-spec/structure.md, collection IDs SHOULD:
- Contain only lowercase letters, numbers, hyphens, and underscores
- Start with a letter
- Be unique within the catalog (uniqueness tested elsewhere)
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.collection_id import (
    CollectionIdError,
    normalize_collection_id,
    validate_collection_id,
)

pytestmark = pytest.mark.unit


class TestValidateCollectionId:
    """Tests for validate_collection_id()."""

    # Valid IDs
    @pytest.mark.parametrize(
        "collection_id",
        [
            "census",
            "census-2020",
            "census_2020",
            "my-data-set",
            "a",
            "a1",
            "demographics",
        ],
    )
    def test_valid_ids(self, collection_id: str) -> None:
        """Valid collection IDs should pass validation."""
        is_valid, error = validate_collection_id(collection_id)
        assert is_valid is True
        assert error is None

    # Invalid: starts with number
    @pytest.mark.parametrize(
        "collection_id",
        [
            "2020-census",
            "123data",
            "1",
        ],
    )
    def test_invalid_starts_with_number(self, collection_id: str) -> None:
        """Collection IDs starting with numbers should be rejected."""
        is_valid, error = validate_collection_id(collection_id)
        assert is_valid is False
        assert error is not None
        assert "start with a letter" in error.lower()

    # Invalid: contains spaces
    @pytest.mark.parametrize(
        "collection_id",
        [
            "my data",
            "census 2020",
            " leading",
            "trailing ",
        ],
    )
    def test_invalid_contains_spaces(self, collection_id: str) -> None:
        """Collection IDs with spaces should be rejected."""
        is_valid, error = validate_collection_id(collection_id)
        assert is_valid is False
        assert error is not None
        assert "invalid character" in error.lower() or "space" in error.lower()

    # Invalid: contains special characters
    @pytest.mark.parametrize(
        "collection_id",
        [
            "my@data",
            "census!2020",
            "data.set",
            "path/traversal",
            "back\\slash",
        ],
    )
    def test_invalid_special_characters(self, collection_id: str) -> None:
        """Collection IDs with special characters should be rejected."""
        is_valid, error = validate_collection_id(collection_id)
        assert is_valid is False
        assert error is not None

    # Invalid: uppercase (can be normalized)
    @pytest.mark.parametrize(
        "collection_id",
        [
            "MyData",
            "CENSUS",
            "Census-2020",
        ],
    )
    def test_invalid_uppercase(self, collection_id: str) -> None:
        """Collection IDs with uppercase should be rejected (but normalizable)."""
        is_valid, error = validate_collection_id(collection_id)
        assert is_valid is False
        assert error is not None
        assert "lowercase" in error.lower() or "uppercase" in error.lower()

    # Invalid: empty or whitespace-only
    @pytest.mark.parametrize(
        "collection_id",
        [
            "",
            "   ",
        ],
    )
    def test_invalid_empty(self, collection_id: str) -> None:
        """Empty collection IDs should be rejected."""
        is_valid, error = validate_collection_id(collection_id)
        assert is_valid is False
        assert error is not None

    # Invalid: starts with hyphen or underscore
    @pytest.mark.parametrize(
        "collection_id",
        [
            "-data",
            "_data",
            "--double",
        ],
    )
    def test_invalid_starts_with_punctuation(self, collection_id: str) -> None:
        """Collection IDs starting with hyphen/underscore should be rejected."""
        is_valid, error = validate_collection_id(collection_id)
        assert is_valid is False
        assert error is not None
        assert "start with a letter" in error.lower()


class TestNormalizeCollectionId:
    """Tests for normalize_collection_id()."""

    # Basic normalization
    @pytest.mark.parametrize(
        ("input_id", "expected"),
        [
            ("MyData", "mydata"),
            ("CENSUS", "census"),
            ("Census-2020", "census-2020"),
            ("My Data", "my-data"),
            ("census  2020", "census-2020"),  # multiple spaces -> single hyphen
        ],
    )
    def test_basic_normalization(self, input_id: str, expected: str) -> None:
        """Basic normalization should lowercase and replace spaces."""
        assert normalize_collection_id(input_id) == expected

    # Special character replacement
    @pytest.mark.parametrize(
        ("input_id", "expected"),
        [
            ("my@data", "my-data"),
            ("census!2020", "census-2020"),
            ("data.set", "data-set"),
            ("path/traversal", "path-traversal"),
        ],
    )
    def test_special_char_replacement(self, input_id: str, expected: str) -> None:
        """Special characters should be replaced with hyphens."""
        assert normalize_collection_id(input_id) == expected

    # Leading number handling
    @pytest.mark.parametrize(
        ("input_id", "expected"),
        [
            ("2020-census", "n2020-census"),  # prefix with 'n'
            ("123data", "n123data"),
        ],
    )
    def test_leading_number_prefix(self, input_id: str, expected: str) -> None:
        """IDs starting with numbers should be prefixed with 'n'."""
        assert normalize_collection_id(input_id) == expected

    @pytest.mark.parametrize(
        ("input_id", "expected"),
        [
            ("_data", "n_data"),  # leading underscore gets 'n' prefix
            ("_my-collection", "n_my-collection"),
            ("@_test", "n_test"),  # @ removed, then underscore prefixed
        ],
    )
    def test_leading_underscore_prefix(self, input_id: str, expected: str) -> None:
        """IDs starting with underscores should be prefixed with 'n'."""
        assert normalize_collection_id(input_id) == expected

    # Edge cases
    def test_already_valid(self) -> None:
        """Already valid IDs should be unchanged."""
        assert normalize_collection_id("census-2020") == "census-2020"

    def test_collapse_multiple_hyphens(self) -> None:
        """Multiple consecutive hyphens should collapse to one."""
        assert normalize_collection_id("my---data") == "my-data"
        assert normalize_collection_id("a - b - c") == "a-b-c"

    def test_strip_leading_trailing_hyphens(self) -> None:
        """Leading/trailing hyphens should be stripped."""
        assert normalize_collection_id("-data-") == "data"
        assert normalize_collection_id("--data--") == "data"

    def test_non_ascii_transliteration(self) -> None:
        """Non-ASCII characters should be transliterated or removed."""
        # Test actual non-ASCII input - accented characters get transliterated
        assert normalize_collection_id("données") == "donnees"
        assert normalize_collection_id("naïve") == "naive"
        assert normalize_collection_id("café") == "cafe"
        # Mixed case with accents
        result = normalize_collection_id("Données")
        assert result == "donnees"

    def test_empty_raises(self) -> None:
        """Empty input should raise CollectionIdError."""
        with pytest.raises(CollectionIdError):
            normalize_collection_id("")

    def test_whitespace_only_raises(self) -> None:
        """Whitespace-only input should raise CollectionIdError."""
        with pytest.raises(CollectionIdError):
            normalize_collection_id("   ")

    def test_all_special_chars_raises(self) -> None:
        """Input that normalizes to empty should raise CollectionIdError."""
        with pytest.raises(CollectionIdError):
            normalize_collection_id("@#$%")


class TestNormalizedIdIsValid:
    """Ensure normalized IDs always pass validation."""

    @pytest.mark.parametrize(
        "input_id",
        [
            "MyData",
            "CENSUS",
            "my data",
            "2020-census",
            "my@data!set",
            "Census_2020",
            "_data",  # underscore-prefixed should also normalize to valid
            "_my-collection",
        ],
    )
    def test_normalized_ids_are_valid(self, input_id: str) -> None:
        """Any normalized ID should pass validation."""
        normalized = normalize_collection_id(input_id)
        is_valid, error = validate_collection_id(normalized)
        assert is_valid is True, f"Normalized '{input_id}' -> '{normalized}' failed: {error}"


class TestScanFixCollectionId:
    """Tests for collection ID fix in scan_fix module."""

    def test_compute_collection_id_fix_uppercase(self) -> None:
        """Should compute fix for uppercase collection ID."""
        from pathlib import Path

        from portolan_cli.scan_fix import _compute_collection_id_fix

        # Mock directory path
        dir_path = Path("/data/MyCollection")

        result = _compute_collection_id_fix(dir_path)

        assert result is not None
        new_path, preview = result
        assert new_path == Path("/data/mycollection")
        assert "MyCollection" in preview
        assert "mycollection" in preview

    def test_compute_collection_id_fix_spaces(self) -> None:
        """Should compute fix for collection ID with spaces."""
        from pathlib import Path

        from portolan_cli.scan_fix import _compute_collection_id_fix

        dir_path = Path("/data/My Data")

        result = _compute_collection_id_fix(dir_path)

        assert result is not None
        new_path, preview = result
        assert new_path == Path("/data/my-data")

    def test_compute_collection_id_fix_starts_with_number(self) -> None:
        """Should compute fix for collection ID starting with number."""
        from pathlib import Path

        from portolan_cli.scan_fix import _compute_collection_id_fix

        dir_path = Path("/data/2020-census")

        result = _compute_collection_id_fix(dir_path)

        assert result is not None
        new_path, preview = result
        assert new_path == Path("/data/n2020-census")

    def test_compute_collection_id_fix_already_valid(self) -> None:
        """Should return None for already valid collection ID."""
        from pathlib import Path

        from portolan_cli.scan_fix import _compute_collection_id_fix

        dir_path = Path("/data/census-2020")

        result = _compute_collection_id_fix(dir_path)

        # Already valid - no fix needed
        assert result is None

    def test_is_fix_flag_issue_includes_collection_id(self) -> None:
        """INVALID_COLLECTION_ID should be in FIX_FLAG_ISSUE_TYPES."""
        from portolan_cli.scan_fix import FIX_FLAG_ISSUE_TYPES

        assert "invalid_collection_id" in FIX_FLAG_ISSUE_TYPES


class TestAddDatasetValidation:
    """Tests for collection ID validation in add_dataset()."""

    def test_add_dataset_rejects_invalid_collection_id(self, tmp_path: Path) -> None:
        """add_dataset should reject invalid collection IDs with helpful error."""
        from portolan_cli.dataset import add_dataset

        # Create a test file
        test_file = tmp_path / "data.geojson"
        test_file.write_text('{"type": "FeatureCollection", "features": []}')

        # Try to add with invalid collection ID
        with pytest.raises(ValueError) as exc_info:
            add_dataset(
                path=test_file,
                catalog_root=tmp_path,
                collection_id="My Data",  # Invalid: contains space
            )

        error_msg = str(exc_info.value)
        assert "Invalid collection ID" in error_msg
        assert "my-data" in error_msg.lower()  # Should suggest normalized name

    def test_add_dataset_rejects_uppercase_collection_id(self, tmp_path: Path) -> None:
        """add_dataset should reject uppercase collection IDs."""
        from portolan_cli.dataset import add_dataset

        test_file = tmp_path / "data.geojson"
        test_file.write_text('{"type": "FeatureCollection", "features": []}')

        with pytest.raises(ValueError) as exc_info:
            add_dataset(
                path=test_file,
                catalog_root=tmp_path,
                collection_id="MyCollection",
            )

        error_msg = str(exc_info.value)
        assert "Invalid collection ID" in error_msg
        assert "mycollection" in error_msg.lower()
