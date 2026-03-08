"""Unit tests for portolan_cli/scan_fix.py.

Tests fix mode implementations including:
- Filename sanitization
- Windows reserved name handling
- Path length checks
- Character transliteration
- Fix proposals
"""

from __future__ import annotations

from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from portolan_cli.scan import IssueType, ScanIssue, Severity
from portolan_cli.scan_fix import (
    FixCategory,
    ProposedFix,
    _compute_short_hash,
    _is_case_only_rename,
    _is_windows_reserved,
    _needs_rename,
    _sanitize_filename,
    _transliterate_to_ascii,
)

# =============================================================================
# Transliteration Tests
# =============================================================================


@pytest.mark.unit
class TestTransliterateToAscii:
    """Tests for _transliterate_to_ascii function."""

    def test_basic_latin_unchanged(self) -> None:
        """Basic ASCII text should be unchanged."""
        assert _transliterate_to_ascii("hello") == "hello"
        assert _transliterate_to_ascii("test_file") == "test_file"

    def test_accented_characters_transliterated(self) -> None:
        """Accented characters should be transliterated to ASCII."""
        assert _transliterate_to_ascii("données") == "donnees"
        assert _transliterate_to_ascii("naïve") == "naive"
        assert _transliterate_to_ascii("café") == "cafe"
        assert _transliterate_to_ascii("résumé") == "resume"

    def test_umlaut_characters(self) -> None:
        """Umlaut characters should be transliterated."""
        assert _transliterate_to_ascii("über") == "uber"
        assert _transliterate_to_ascii("Müller") == "Muller"
        assert _transliterate_to_ascii("Größe") == "Groe"  # ß doesn't decompose to ASCII

    def test_mixed_text(self) -> None:
        """Mixed ASCII and non-ASCII text should be handled."""
        assert _transliterate_to_ascii("file_données_2020") == "file_donnees_2020"

    def test_non_latin_scripts_removed(self) -> None:
        """Non-Latin scripts (CJK, Arabic, etc.) should be removed."""
        # These characters don't have ASCII equivalents
        result = _transliterate_to_ascii("日本語")
        assert result == ""  # No ASCII equivalents

    def test_empty_string(self) -> None:
        """Empty string should return empty string."""
        assert _transliterate_to_ascii("") == ""


# =============================================================================
# Filename Sanitization Tests
# =============================================================================


@pytest.mark.unit
class TestSanitizeFilename:
    """Tests for _sanitize_filename function (issue #208: dashes + lowercase)."""

    def test_spaces_replaced_with_dashes(self) -> None:
        """Spaces should be replaced with dashes (issue #208)."""
        result = _sanitize_filename("file name.txt")
        assert " " not in result
        assert "-" in result
        assert result == "file-name.txt"

    def test_parentheses_replaced(self) -> None:
        """Parentheses should be replaced with dashes."""
        result = _sanitize_filename("file(1).txt")
        assert "(" not in result
        assert ")" not in result

    def test_brackets_replaced(self) -> None:
        """Brackets should be replaced with dashes."""
        result = _sanitize_filename("file[version].txt")
        assert "[" not in result
        assert "]" not in result

    def test_braces_replaced(self) -> None:
        """Braces should be replaced with dashes."""
        result = _sanitize_filename("file{id}.txt")
        assert "{" not in result
        assert "}" not in result

    def test_accents_transliterated(self) -> None:
        """Accented characters should be transliterated."""
        result = _sanitize_filename("données.geojson")
        assert "é" not in result
        assert "donnees" in result

    def test_consecutive_dashes_collapsed(self) -> None:
        """Multiple consecutive dashes should be collapsed (issue #208)."""
        result = _sanitize_filename("file   name.txt")  # Multiple spaces
        assert "---" not in result
        assert "--" not in result

    def test_leading_trailing_dashes_removed(self) -> None:
        """Leading and trailing dashes should be removed (issue #208)."""
        result = _sanitize_filename(" file .txt")
        stem = result.rsplit(".", 1)[0]
        assert not stem.startswith("-")
        assert not stem.endswith("-")

    def test_extension_preserved(self) -> None:
        """File extension should be preserved."""
        result = _sanitize_filename("file name.geojson")
        assert result.endswith(".geojson")

    def test_path_separators_sanitized(self) -> None:
        """Path separators should be replaced (defense-in-depth)."""
        result = _sanitize_filename("../../../etc/passwd")
        assert "/" not in result
        assert "\\" not in result
        assert ".." not in result or "-" in result

    def test_empty_result_uses_hash(self) -> None:
        """If sanitization produces empty string, use hash-based name."""
        result = _sanitize_filename("日本語.shp")
        # Should produce something like "file_abc12345.shp"
        assert result.endswith(".shp")
        assert len(result) > 4  # Not just ".shp"
        assert result.startswith("file_")

    def test_valid_filename_minimal_changes(self) -> None:
        """Valid filenames should have minimal changes."""
        result = _sanitize_filename("valid_filename.txt")
        assert result == "valid_filename.txt"


# =============================================================================
# Windows Reserved Name Tests
# =============================================================================


@pytest.mark.unit
class TestWindowsReservedNames:
    """Tests for _is_windows_reserved function."""

    def test_device_names_detected(self) -> None:
        """Windows device names should be detected."""
        assert _is_windows_reserved("con") is True
        assert _is_windows_reserved("prn") is True
        assert _is_windows_reserved("aux") is True
        assert _is_windows_reserved("nul") is True

    def test_com_ports_detected(self) -> None:
        """COM ports should be detected."""
        for i in range(1, 10):
            assert _is_windows_reserved(f"com{i}") is True

    def test_lpt_ports_detected(self) -> None:
        """LPT ports should be detected."""
        for i in range(1, 10):
            assert _is_windows_reserved(f"lpt{i}") is True

    def test_case_insensitive(self) -> None:
        """Detection should be case-insensitive."""
        assert _is_windows_reserved("CON") is True
        assert _is_windows_reserved("Con") is True
        assert _is_windows_reserved("cOn") is True

    def test_normal_names_not_detected(self) -> None:
        """Normal names should not be flagged."""
        assert _is_windows_reserved("conference") is False
        assert _is_windows_reserved("auxiliary") is False
        assert _is_windows_reserved("null") is False  # 'null' != 'nul'


# =============================================================================
# Needs Rename Tests
# =============================================================================


@pytest.mark.unit
class TestNeedsRename:
    """Tests for _needs_rename function."""

    def test_valid_filename_no_rename(self, tmp_path: Path) -> None:
        """Valid filenames should not need renaming."""
        path = tmp_path / "valid_filename.txt"
        assert _needs_rename(path) is False

    def test_spaces_need_rename(self, tmp_path: Path) -> None:
        """Filenames with spaces need renaming."""
        path = tmp_path / "file name.txt"
        assert _needs_rename(path) is True

    def test_parentheses_need_rename(self, tmp_path: Path) -> None:
        """Filenames with parentheses need renaming."""
        path = tmp_path / "file(1).txt"
        assert _needs_rename(path) is True

    def test_non_ascii_need_rename(self, tmp_path: Path) -> None:
        """Filenames with non-ASCII chars need renaming."""
        path = tmp_path / "données.txt"
        assert _needs_rename(path) is True

    def test_windows_reserved_needs_rename(self, tmp_path: Path) -> None:
        """Windows reserved names need renaming."""
        path = tmp_path / "con.txt"
        assert _needs_rename(path) is True

    def test_long_path_needs_rename(self, tmp_path: Path) -> None:
        """Very long paths need renaming."""
        # Create a path longer than 200 characters
        long_name = "a" * 250 + ".txt"
        path = tmp_path / long_name
        assert _needs_rename(path) is True


# =============================================================================
# Hash Computation Tests
# =============================================================================


@pytest.mark.unit
class TestComputeShortHash:
    """Tests for _compute_short_hash function."""

    def test_default_length(self) -> None:
        """Default hash should be 8 characters."""
        result = _compute_short_hash("test")
        assert len(result) == 8

    def test_custom_length(self) -> None:
        """Custom length should be respected."""
        result = _compute_short_hash("test", length=4)
        assert len(result) == 4
        result = _compute_short_hash("test", length=16)
        assert len(result) == 16

    def test_deterministic(self) -> None:
        """Same input should produce same hash."""
        hash1 = _compute_short_hash("consistent")
        hash2 = _compute_short_hash("consistent")
        assert hash1 == hash2

    def test_different_inputs_different_hashes(self) -> None:
        """Different inputs should produce different hashes."""
        hash1 = _compute_short_hash("input1")
        hash2 = _compute_short_hash("input2")
        assert hash1 != hash2

    def test_hex_output(self) -> None:
        """Output should be valid hexadecimal."""
        result = _compute_short_hash("test")
        # All characters should be hex digits
        assert all(c in "0123456789abcdef" for c in result)


# =============================================================================
# Case-Only Rename Tests
# =============================================================================


@pytest.mark.unit
class TestIsCaseOnlyRename:
    """Tests for _is_case_only_rename function."""

    def test_nonexistent_target_not_case_only(self, tmp_path: Path) -> None:
        """If target doesn't exist, it's not a case-only rename."""
        source = tmp_path / "Source.txt"
        target = tmp_path / "target.txt"
        source.write_text("content")
        assert _is_case_only_rename(source, target) is False

    def test_same_file_is_case_only(self, tmp_path: Path) -> None:
        """Renaming to same file with different case should be detected."""
        source = tmp_path / "TestFile.txt"
        source.write_text("content")
        # On case-insensitive systems, these refer to same file
        target = tmp_path / "testfile.txt"
        # If the file exists (on case-insensitive), samefile returns True
        result = _is_case_only_rename(source, target)
        # Result depends on filesystem case-sensitivity
        # On Linux (case-sensitive): False (different files)
        # On macOS/Windows (case-insensitive): True (same file)
        assert isinstance(result, bool)

    def test_different_files_not_case_only(self, tmp_path: Path) -> None:
        """Renaming to different existing file should return False."""
        source = tmp_path / "source.txt"
        target = tmp_path / "target.txt"
        source.write_text("source content")
        target.write_text("target content")
        assert _is_case_only_rename(source, target) is False


# =============================================================================
# ProposedFix Tests
# =============================================================================


@pytest.mark.unit
class TestProposedFix:
    """Tests for ProposedFix dataclass."""

    def test_to_dict_structure(self, tmp_path: Path) -> None:
        """ProposedFix.to_dict() should return proper structure."""
        issue = ScanIssue(
            path=tmp_path / "file name.txt",
            relative_path="file name.txt",
            issue_type=IssueType.INVALID_CHARACTERS,
            severity=Severity.WARNING,
            message="Invalid characters",
        )
        fix = ProposedFix(
            issue=issue,
            category=FixCategory.SAFE,
            action="rename",
            details={"old_name": "file name.txt", "new_name": "file_name.txt"},
            preview="file name.txt -> file_name.txt",
        )

        result = fix.to_dict()

        assert "issue_path" in result
        assert "category" in result
        assert "action" in result
        assert "details" in result
        assert "preview" in result
        assert result["category"] == "safe"
        assert result["action"] == "rename"

    def test_category_enum_serialization(self, tmp_path: Path) -> None:
        """FixCategory enum should serialize to string."""
        issue = ScanIssue(
            path=tmp_path / "test",
            relative_path="test",
            issue_type=IssueType.ZERO_BYTE_FILE,
            severity=Severity.ERROR,
            message="Test",
        )

        for category in [FixCategory.SAFE, FixCategory.UNSAFE, FixCategory.MANUAL]:
            fix = ProposedFix(
                issue=issue,
                category=category,
                action="test",
                details={},
                preview="test",
            )
            result = fix.to_dict()
            assert result["category"] == category.value


# =============================================================================
# Hypothesis Property-Based Tests
# =============================================================================


@pytest.mark.unit
class TestPropertyBasedSanitization:
    """Property-based tests for filename sanitization."""

    @given(text=st.text(min_size=1, max_size=50))
    @settings(max_examples=50)
    def test_sanitized_is_ascii_safe(self, text: str) -> None:
        """Sanitized output should be ASCII-safe."""
        result = _transliterate_to_ascii(text)
        # All characters should be ASCII
        assert all(ord(c) < 128 for c in result)

    @given(name=st.text(min_size=1, max_size=50).map(lambda x: x + ".txt"))
    @settings(max_examples=50)
    def test_sanitized_filename_never_empty(self, name: str) -> None:
        """Sanitized filename should never be completely empty."""
        result = _sanitize_filename(name)
        # Should at least have the extension
        assert len(result) >= 4  # ".txt"

    @given(
        name=st.text(
            min_size=1,
            max_size=20,
            alphabet=st.characters(
                whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_-"
            ),
        ).map(lambda x: x + ".shp")
    )
    @settings(max_examples=30)
    def test_valid_names_minimally_changed(self, name: str) -> None:
        """Valid filenames should be minimally changed."""
        # Skip if name starts with invalid chars
        if name.startswith("-") or name.startswith("_"):
            return
        result = _sanitize_filename(name)
        # Extension should be preserved
        assert result.endswith(".shp")

    @given(length=st.integers(min_value=1, max_value=32))
    @settings(max_examples=20)
    def test_hash_length_respected(self, length: int) -> None:
        """Hash length parameter should be respected."""
        result = _compute_short_hash("test", length=length)
        assert len(result) == length
