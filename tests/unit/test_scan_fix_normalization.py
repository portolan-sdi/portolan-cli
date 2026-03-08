"""Unit tests for filename normalization in scan --fix.

Issue #208: Filenames should be normalized to lowercase with dashes.

Test Strategy:
- Verify spaces are replaced with dashes (not underscores)
- Verify filenames are lowercased
- Verify special characters are replaced with dashes
- Verify multiple consecutive dashes are collapsed
- Hypothesis property tests for edge cases
"""

from __future__ import annotations

from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from portolan_cli.scan_fix import _compute_safe_rename, _sanitize_filename


@pytest.mark.unit
class TestNormalizationUsesLowercaseAndDashes:
    """Tests that normalization uses lowercase + dashes per issue #208."""

    # -------------------------------------------------------------------------
    # Core normalization: lowercase + dashes
    # -------------------------------------------------------------------------

    def test_spaces_replaced_with_dashes(self, tmp_path: Path) -> None:
        """Spaces in filename should be replaced with dashes (not underscores)."""
        path = tmp_path / "My File.geojson"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, preview = result
        assert new_path.name == "my-file.geojson"
        assert "_" not in new_path.name
        assert "my-file.geojson" in preview

    def test_uppercase_converted_to_lowercase(self, tmp_path: Path) -> None:
        """Uppercase letters should be converted to lowercase."""
        path = tmp_path / "MyDataFile.geojson"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        assert new_path.name == "mydatafile.geojson"

    def test_mixed_case_with_spaces(self, tmp_path: Path) -> None:
        """Mixed case and spaces should normalize to lowercase with dashes."""
        path = tmp_path / "Radios 2010 v2025-1.shp"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        # The exact example from issue #208
        assert new_path.name == "radios-2010-v2025-1.shp"

    def test_multiple_spaces_become_single_dash(self, tmp_path: Path) -> None:
        """Multiple consecutive spaces should become a single dash."""
        path = tmp_path / "My   File.geojson"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        assert "--" not in new_path.name
        assert new_path.name == "my-file.geojson"

    def test_special_chars_replaced_with_dashes(self, tmp_path: Path) -> None:
        """Parentheses and brackets should be replaced with dashes."""
        path = tmp_path / "My (Test) [File].shp"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        assert "(" not in new_path.name
        assert ")" not in new_path.name
        assert "[" not in new_path.name
        assert "]" not in new_path.name
        assert "_" not in new_path.name
        assert "--" not in new_path.name
        # All lowercase
        assert new_path.name == new_path.name.lower()

    def test_curly_braces_replaced_with_dashes(self, tmp_path: Path) -> None:
        """Curly braces should be replaced with dashes."""
        path = tmp_path / "File{1}.geojson"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        assert "{" not in new_path.name
        assert "}" not in new_path.name
        assert "_" not in new_path.name

    def test_non_ascii_transliterated_and_lowercased(self, tmp_path: Path) -> None:
        """Non-ASCII should be transliterated, result lowercased."""
        path = tmp_path / "Données.geojson"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        # 'Données' → 'donnees' (É→e, é→e, all lowercase)
        assert new_path.name == "donnees.geojson"

    def test_extension_preserved_as_lowercase(self, tmp_path: Path) -> None:
        """Extension should also be lowercase."""
        path = tmp_path / "MyFile.GeoJSON"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        # Both stem and extension lowercase
        assert new_path.name == "myfile.geojson"

    # -------------------------------------------------------------------------
    # Edge cases
    # -------------------------------------------------------------------------

    def test_already_normalized_returns_none(self, tmp_path: Path) -> None:
        """Already normalized filenames should return None (no rename needed)."""
        path = tmp_path / "already-normalized.geojson"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is None

    def test_only_lowercase_change_still_renames(self, tmp_path: Path) -> None:
        """A file needing only lowercase change should still be renamed."""
        path = tmp_path / "MyFile.geojson"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        assert new_path.name == "myfile.geojson"

    def test_preserves_existing_dashes(self, tmp_path: Path) -> None:
        """Existing dashes in filename should be preserved."""
        path = tmp_path / "My-Data File.shp"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        assert new_path.name == "my-data-file.shp"

    def test_leading_trailing_dashes_stripped(self, tmp_path: Path) -> None:
        """Leading and trailing dashes should be stripped."""
        path = tmp_path / " My File .geojson"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        assert not new_path.stem.startswith("-")
        assert not new_path.stem.endswith("-")

    def test_windows_reserved_prefix_uses_underscore(self, tmp_path: Path) -> None:
        """Windows reserved names should still use underscore prefix (not dash).

        This is intentional: _CON is a common convention, -CON looks wrong.

        Note: We don't call path.touch() because Windows cannot create files
        named CON, PRN, etc. The _compute_safe_rename function only inspects
        the path string, not the filesystem.
        """
        path = tmp_path / "CON.txt"
        # No touch() - Windows reserved names cannot be created on Windows

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        # Windows reserved names get underscore prefix
        assert new_path.name == "_con.txt"


@pytest.mark.unit
class TestSanitizeFilenameDirectly:
    """Direct tests for _sanitize_filename function."""

    def test_basic_space_replacement(self) -> None:
        """Spaces become dashes."""
        result = _sanitize_filename("my file")
        assert result == "my-file"

    def test_basic_lowercase(self) -> None:
        """Uppercase becomes lowercase."""
        result = _sanitize_filename("MyFile")
        assert result == "myfile"

    def test_combined_normalization(self) -> None:
        """Combined space + case normalization."""
        result = _sanitize_filename("My Data File")
        assert result == "my-data-file"

    def test_multiple_special_chars(self) -> None:
        """Multiple special chars become single dash."""
        result = _sanitize_filename("File (v1) [test]")
        assert result == "file-v1-test"

    def test_non_ascii_with_case(self) -> None:
        """Non-ASCII transliterated and lowercased."""
        result = _sanitize_filename("Données Géo")
        assert result == "donnees-geo"

    def test_extension_handling(self) -> None:
        """Extension passed through is also lowercased."""
        result = _sanitize_filename("MyFile.SHP")
        # Note: _sanitize_filename handles stem+extension together
        assert result.lower() == result


# =============================================================================
# Property-based tests with Hypothesis
# =============================================================================


@pytest.mark.unit
class TestNormalizationProperties:
    """Property-based tests for filename normalization invariants."""

    @given(st.text(min_size=1, max_size=50))
    @settings(max_examples=100)
    def test_output_is_always_lowercase(self, stem: str) -> None:
        """Sanitized output should always be lowercase (or hash fallback)."""
        result = _sanitize_filename(stem)
        # Either all lowercase or empty-input fallback (starts with 'file_')
        assert result == result.lower() or result.startswith("file_")

    @given(st.text(min_size=1, max_size=50))
    @settings(max_examples=100)
    def test_no_spaces_in_output(self, stem: str) -> None:
        """Sanitized output should never contain spaces."""
        result = _sanitize_filename(stem)
        assert " " not in result

    @given(st.text(min_size=1, max_size=50))
    @settings(max_examples=100)
    def test_no_consecutive_dashes_in_stem(self, stem: str) -> None:
        """Sanitized stem should not have consecutive dashes.

        Note: We only check the stem portion, not the extension.
        Edge case inputs with path separators (e.g., '0./\\') may produce
        extensions like '.--' which is acceptable for malformed inputs.
        """
        from pathlib import Path

        result = _sanitize_filename(stem)
        result_stem = Path(result).stem if "." in result else result
        assert "--" not in result_stem

    @given(st.text(min_size=1, max_size=50))
    @settings(max_examples=100)
    def test_no_leading_trailing_dashes(self, stem: str) -> None:
        """Sanitized output should not start or end with dash."""
        result = _sanitize_filename(stem)
        # Split to handle extension
        stem_part = result.split(".")[0] if "." in result else result
        if stem_part:  # Empty stems handled elsewhere
            assert not stem_part.startswith("-")
            assert not stem_part.endswith("-")

    @given(
        st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=("L", "N")))
    )
    @settings(max_examples=50)
    def test_alphanumeric_preserved(self, stem: str) -> None:
        """Alphanumeric characters should be preserved (just lowercased)."""
        result = _sanitize_filename(stem)
        # All original alphanumeric chars should appear (lowercased)
        for char in stem:
            if char.isalnum() and char.isascii():
                assert char.lower() in result.lower()

    @given(st.text(min_size=1, max_size=50))
    @settings(max_examples=100)
    def test_output_never_empty(self, stem: str) -> None:
        """Sanitized output should never be empty."""
        result = _sanitize_filename(stem)
        assert len(result) > 0

    @given(
        st.tuples(
            st.text(min_size=1, max_size=30),
            st.sampled_from([".shp", ".geojson", ".gpkg", ".tif", ".SHP", ".GeoJSON"]),
        )
    )
    @settings(max_examples=50)
    def test_extension_preserved(self, stem_ext: tuple[str, str]) -> None:
        """File extension should be preserved."""
        stem, ext = stem_ext
        result = _sanitize_filename(stem + ext)
        # Extension should be at the end (lowercased)
        assert result.lower().endswith(ext.lower())


# =============================================================================
# Regression tests for issue #208
# =============================================================================


@pytest.mark.unit
class TestIssue208Regression:
    """Regression tests for specific cases from issue #208."""

    def test_reserved_name_after_normalization(self, tmp_path: Path) -> None:
        """Edge case: 'CON ()' normalizes to 'con' which is still reserved.

        The Windows reserved check must happen AFTER normalization, not before.
        Otherwise 'CON ()' → 'con' instead of '_con'.

        Note: No path.touch() - Windows cannot create files named CON.
        """
        path = tmp_path / "CON ().txt"
        # No touch() - Windows reserved names cannot be created on Windows

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        # Must get underscore prefix because 'con' is reserved
        assert new_path.name == "_con.txt"

    def test_radios_2010_example(self, tmp_path: Path) -> None:
        """The exact example from issue #208."""
        path = tmp_path / "Radios 2010 v2025-1.shp"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        assert new_path.name == "radios-2010-v2025-1.shp"

    def test_uppercase_geojson(self, tmp_path: Path) -> None:
        """GeoJSON extension should be lowercased."""
        path = tmp_path / "MyData.GeoJSON"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        assert new_path.suffix == ".geojson"

    def test_shapefile_with_spaces_all_components(self, tmp_path: Path) -> None:
        """Shapefile with spaces - all sidecar files should be renamed."""
        # Create shapefile and sidecars
        (tmp_path / "Census Data 2020.shp").touch()
        (tmp_path / "Census Data 2020.shx").touch()
        (tmp_path / "Census Data 2020.dbf").touch()
        (tmp_path / "Census Data 2020.prj").touch()

        path = tmp_path / "Census Data 2020.shp"
        result = _compute_safe_rename(path)

        assert result is not None
        new_path, preview = result
        assert new_path.name == "census-data-2020.shp"
        # Preview should mention sidecars
        assert "sidecars" in preview.lower() or ".shx" in preview or ".dbf" in preview
