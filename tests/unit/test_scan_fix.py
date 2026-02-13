"""Unit tests for portolan_cli/scan_fix.py.

Tests fix mode implementations: safe fixes, unsafe fixes, dry-run.

Test Strategy:
- Test _compute_safe_rename for each issue type (INVALID_CHARACTERS, WINDOWS_RESERVED_NAME, LONG_PATH)
- Test sidecar handling (shapefile sidecars must be renamed together)
- Test collision detection (fail if target exists)
- Test apply_safe_fixes with dry_run=True and dry_run=False
- Test preview_fix for generating fix proposals
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.scan import IssueType, ScanIssue, Severity
from portolan_cli.scan_fix import (
    FixCategory,
    _compute_safe_rename,
    apply_safe_fixes,
    preview_fix,
)


@pytest.mark.unit
class TestFixCategory:
    """Tests for FixCategory enum."""

    def test_has_three_categories(self) -> None:
        """FixCategory should have exactly 3 categories."""
        assert len(FixCategory) == 3

    def test_category_values(self) -> None:
        """FixCategory should have expected values."""
        assert FixCategory.SAFE.value == "safe"
        assert FixCategory.UNSAFE.value == "unsafe"
        assert FixCategory.MANUAL.value == "manual"


# =============================================================================
# Tests for _compute_safe_rename
# =============================================================================


@pytest.mark.unit
class TestComputeSafeRename:
    """Tests for _compute_safe_rename function."""

    # -------------------------------------------------------------------------
    # INVALID_CHARACTERS: spaces → underscores, non-ASCII → transliterated
    # -------------------------------------------------------------------------

    def test_spaces_replaced_with_underscores(self, tmp_path: Path) -> None:
        """Spaces in filename should be replaced with underscores."""
        path = tmp_path / "my file.geojson"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, preview = result
        assert new_path.name == "my_file.geojson"
        assert new_path.parent == path.parent
        assert "my_file.geojson" in preview

    def test_multiple_spaces_and_special_chars(self, tmp_path: Path) -> None:
        """Multiple spaces and special chars should all be replaced."""
        path = tmp_path / "my (test) [file].shp"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        # Parentheses and brackets should become underscores
        assert "(" not in new_path.name
        assert ")" not in new_path.name
        assert "[" not in new_path.name
        assert "]" not in new_path.name
        assert " " not in new_path.name

    def test_non_ascii_transliterated(self, tmp_path: Path) -> None:
        """Non-ASCII characters should be transliterated to ASCII."""
        path = tmp_path / "données.geojson"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        # 'données' should become 'donnees' (é → e)
        assert new_path.name == "donnees.geojson"

    def test_mixed_issues(self, tmp_path: Path) -> None:
        """Files with spaces AND non-ASCII should fix both."""
        path = tmp_path / "mes données (2024).geojson"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        assert " " not in new_path.name
        assert "é" not in new_path.name
        assert "(" not in new_path.name

    def test_already_valid_returns_none(self, tmp_path: Path) -> None:
        """Files with valid names should return None (no rename needed)."""
        path = tmp_path / "valid_filename.geojson"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is None

    def test_only_non_ascii_chars_uses_hash_fallback(self, tmp_path: Path) -> None:
        """Filenames with ONLY non-ASCII chars should get hash-based name."""
        # This would produce empty string after transliteration
        path = tmp_path / "日本語.shp"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        # Should NOT be empty or hidden file
        assert new_path.stem != ""
        assert not new_path.stem.startswith(".")
        # Should have "file_" prefix with hash
        assert new_path.stem.startswith("file_")
        assert len(new_path.stem) > 5  # "file_" + hash

    def test_curly_braces_sanitized(self, tmp_path: Path) -> None:
        """Curly braces should be replaced with underscores."""
        path = tmp_path / "test{data}.geojson"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        assert "{" not in new_path.name
        assert "}" not in new_path.name

    def test_path_traversal_sanitized(self, tmp_path: Path) -> None:
        """Path separators should be sanitized for defense-in-depth."""
        from portolan_cli.scan_fix import _sanitize_filename

        # Test sanitization directly - path separators should become underscores
        result = _sanitize_filename("test_data")
        assert result == "test_data"

        # Backslashes should be sanitized
        result = _sanitize_filename(r"test\data")
        assert "\\" not in result

        # Forward slashes should be sanitized
        result = _sanitize_filename("test/data")
        assert "/" not in result

    # -------------------------------------------------------------------------
    # WINDOWS_RESERVED_NAME: CON, PRN, etc. → _CON, _PRN
    # -------------------------------------------------------------------------

    def test_windows_reserved_con(self, tmp_path: Path) -> None:
        """CON.shp should become _CON.shp."""
        path = tmp_path / "CON.shp"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, preview = result
        assert new_path.name == "_CON.shp"
        assert "_CON.shp" in preview

    def test_windows_reserved_case_insensitive(self, tmp_path: Path) -> None:
        """Windows reserved names should be case-insensitive."""
        for name in ["con.shp", "Con.shp", "CON.shp", "prn.tif", "AUX.geojson"]:
            path = tmp_path / name
            path.touch()

            result = _compute_safe_rename(path)

            assert result is not None, f"Expected rename for {name}"
            new_path, _ = result
            assert new_path.name.startswith("_"), f"Expected _ prefix for {name}"

    def test_windows_reserved_nul(self, tmp_path: Path) -> None:
        """NUL should get underscore prefix."""
        path = tmp_path / "nul.gpkg"
        path.touch()

        result = _compute_safe_rename(path)

        assert result is not None
        new_path, _ = result
        assert new_path.name == "_nul.gpkg"

    # -------------------------------------------------------------------------
    # LONG_PATH: truncate to ~200 chars with hash
    # -------------------------------------------------------------------------

    def test_long_filename_truncated(self, tmp_path: Path) -> None:
        """Long filenames should be truncated with hash suffix."""
        # Calculate stem length to guarantee total path > 200 chars
        # Formula: len(tmp_path) + 1 (/) + stem_len + len(".geojson") > 200
        extension = ".geojson"
        stem_len = 201 - len(str(tmp_path)) - len(extension)
        # Ensure we have a reasonable stem length
        if stem_len < 50:
            stem_len = 50  # Force long enough to trigger truncation
        long_stem = "a" * stem_len
        path = tmp_path / f"{long_stem}{extension}"
        path.touch()

        # Verify test setup - path must exceed threshold
        assert len(str(path)) > 200, f"Test setup error: path is only {len(str(path))} chars"

        result = _compute_safe_rename(path)

        # Must return a rename since path exceeds threshold
        assert result is not None, "Expected rename for long path"
        new_path, _ = result
        # Total path should be <= 200 chars
        assert len(str(new_path)) <= 200, f"New path still too long: {len(str(new_path))} chars"
        # Should preserve extension
        assert new_path.suffix == extension
        # Should have hash appended (8 chars after underscore)
        assert "_" in new_path.stem, "Expected hash suffix in truncated filename"

    def test_long_path_preserves_extension(self, tmp_path: Path) -> None:
        """Truncation should preserve the file extension."""
        long_stem = "b" * 200
        path = tmp_path / f"{long_stem}.parquet"
        path.touch()

        result = _compute_safe_rename(path)

        if result is not None:
            new_path, _ = result
            assert new_path.suffix == ".parquet"

    def test_short_path_no_truncation(self, tmp_path: Path) -> None:
        """Paths under threshold should not be truncated."""
        path = tmp_path / "short_name.geojson"
        path.touch()

        result = _compute_safe_rename(path)

        # No rename needed for valid short paths
        assert result is None

    def test_long_directory_path_returns_none(self, tmp_path: Path) -> None:
        """When directory path alone exceeds threshold, should return None."""
        # Create a directory structure where the DIR path is > 200 chars
        # This cannot be auto-fixed by truncating filename
        long_dir_name = "d" * 100
        deep_path = tmp_path
        # Create nested dirs to exceed threshold
        for _ in range(3):  # 3 x 100 = 300 chars just in dir names
            deep_path = deep_path / long_dir_name
        deep_path.mkdir(parents=True, exist_ok=True)

        # Even a short filename can't help
        path = deep_path / "x.shp"
        path.touch()

        # Verify the path is actually long
        assert len(str(path)) > 200, f"Test setup failed: path is only {len(str(path))} chars"

        result = _compute_safe_rename(path)

        # Should return None because dir path is too long to fix
        assert result is None, "Should return None when directory path exceeds threshold"


# =============================================================================
# Tests for shapefile sidecar handling
# =============================================================================


@pytest.mark.unit
class TestSidecarHandling:
    """Tests for shapefile sidecar detection and renaming."""

    def test_find_sidecars_for_shapefile(self, tmp_path: Path) -> None:
        """Should detect all sidecars with matching stem."""
        # Create shapefile with sidecars
        (tmp_path / "data with spaces.shp").touch()
        (tmp_path / "data with spaces.dbf").touch()
        (tmp_path / "data with spaces.shx").touch()
        (tmp_path / "data with spaces.prj").touch()

        # Import function being tested
        from portolan_cli.scan_fix import _find_sidecars

        sidecars = _find_sidecars(tmp_path / "data with spaces.shp")

        assert len(sidecars) == 3  # dbf, shx, prj
        extensions = {s.suffix for s in sidecars}
        assert extensions == {".dbf", ".shx", ".prj"}

    def test_compute_safe_rename_includes_sidecars(self, tmp_path: Path) -> None:
        """_compute_safe_rename should report sidecars in preview."""
        (tmp_path / "données.shp").touch()
        (tmp_path / "données.dbf").touch()
        (tmp_path / "données.shx").touch()

        result = _compute_safe_rename(tmp_path / "données.shp")

        assert result is not None
        _, preview = result
        # Preview should mention sidecars being renamed
        assert "dbf" in preview.lower() or "sidecar" in preview.lower()

    def test_find_sidecars_includes_qix(self, tmp_path: Path) -> None:
        """Should detect .qix spatial index sidecar."""
        (tmp_path / "data.shp").touch()
        (tmp_path / "data.qix").touch()

        from portolan_cli.scan_fix import _find_sidecars

        sidecars = _find_sidecars(tmp_path / "data.shp")

        extensions = {s.suffix for s in sidecars}
        assert ".qix" in extensions

    def test_find_sidecars_includes_shp_xml(self, tmp_path: Path) -> None:
        """Should detect .shp.xml metadata sidecar."""
        (tmp_path / "data.shp").touch()
        (tmp_path / "data.shp.xml").touch()

        from portolan_cli.scan_fix import _find_sidecars

        sidecars = _find_sidecars(tmp_path / "data.shp")

        # Should find the .shp.xml file
        sidecar_names = [s.name for s in sidecars]
        assert "data.shp.xml" in sidecar_names

    def test_find_sidecars_includes_qmd(self, tmp_path: Path) -> None:
        """Should detect .qmd QGIS metadata sidecar."""
        (tmp_path / "data.shp").touch()
        (tmp_path / "data.qmd").touch()

        from portolan_cli.scan_fix import _find_sidecars

        sidecars = _find_sidecars(tmp_path / "data.shp")

        extensions = {s.suffix for s in sidecars}
        assert ".qmd" in extensions


# =============================================================================
# Tests for apply_safe_fixes
# =============================================================================


@pytest.mark.unit
class TestApplySafeFixes:
    """Tests for apply_safe_fixes function."""

    def _make_issue(
        self,
        path: Path,
        issue_type: IssueType = IssueType.INVALID_CHARACTERS,
    ) -> ScanIssue:
        """Helper to create a ScanIssue."""
        return ScanIssue(
            path=path,
            relative_path=path.name,
            issue_type=issue_type,
            severity=Severity.WARNING,
            message=f"Test issue for {path.name}",
        )

    def test_dry_run_does_not_modify_files(self, tmp_path: Path) -> None:
        """With dry_run=True, files should not be renamed."""
        path = tmp_path / "my file.geojson"
        path.touch()
        issues = [self._make_issue(path)]

        proposed, applied = apply_safe_fixes(issues, dry_run=True)

        # File should still exist at original location
        assert path.exists()
        assert not (tmp_path / "my_file.geojson").exists()
        # Should return proposed fixes but no applied fixes
        assert len(proposed) == 1
        assert len(applied) == 0

    def test_apply_renames_files(self, tmp_path: Path) -> None:
        """With dry_run=False, files should be renamed."""
        path = tmp_path / "my file.geojson"
        path.write_text('{"type": "FeatureCollection"}')
        issues = [self._make_issue(path)]

        proposed, applied = apply_safe_fixes(issues, dry_run=False)

        # Original should be gone, new should exist
        assert not path.exists()
        new_path = tmp_path / "my_file.geojson"
        assert new_path.exists()
        # Content should be preserved
        assert new_path.read_text() == '{"type": "FeatureCollection"}'
        # Both lists should have one entry
        assert len(proposed) == 1
        assert len(applied) == 1

    def test_apply_renames_sidecars_together(self, tmp_path: Path) -> None:
        """Shapefile sidecars should be renamed along with primary."""
        shp = tmp_path / "données.shp"
        dbf = tmp_path / "données.dbf"
        shx = tmp_path / "données.shx"
        shp.touch()
        dbf.touch()
        shx.touch()

        issues = [self._make_issue(shp)]

        _, applied = apply_safe_fixes(issues, dry_run=False)

        # Original files should be gone
        assert not shp.exists()
        assert not dbf.exists()
        assert not shx.exists()
        # New files should exist
        assert (tmp_path / "donnees.shp").exists()
        assert (tmp_path / "donnees.dbf").exists()
        assert (tmp_path / "donnees.shx").exists()

    def test_collision_detection_fails_gracefully(self, tmp_path: Path) -> None:
        """If target filename already exists, should not overwrite."""
        source = tmp_path / "my file.geojson"
        target = tmp_path / "my_file.geojson"
        source.write_text("source content")
        target.write_text("target content")

        issues = [self._make_issue(source)]

        proposed, applied = apply_safe_fixes(issues, dry_run=False)

        # Source should still exist (not renamed due to collision)
        assert source.exists()
        # Target should be unchanged
        assert target.read_text() == "target content"
        # Should have proposed but not applied
        assert len(proposed) == 1
        assert len(applied) == 0
        # The proposed fix should indicate the collision
        assert "exists" in proposed[0].preview.lower() or "collision" in proposed[0].preview.lower()

    def test_filters_non_fixable_issues(self, tmp_path: Path) -> None:
        """Issues that aren't FIX_FLAG should be ignored."""
        path = tmp_path / "valid.geojson"
        path.touch()

        # Create a MANUAL issue (not fixable)
        issue = ScanIssue(
            path=path,
            relative_path=path.name,
            issue_type=IssueType.MULTIPLE_PRIMARIES,  # MANUAL fixability
            severity=Severity.WARNING,
            message="Multiple primaries",
        )

        proposed, applied = apply_safe_fixes([issue], dry_run=False)

        # Should not attempt to fix manual issues
        assert len(proposed) == 0
        assert len(applied) == 0

    def test_handles_missing_file(self, tmp_path: Path) -> None:
        """Should handle files that no longer exist gracefully."""
        path = tmp_path / "deleted file.geojson"
        # Don't create the file

        issues = [self._make_issue(path)]

        proposed, applied = apply_safe_fixes(issues, dry_run=False)

        # Should handle gracefully without crashing
        assert len(applied) == 0

    def test_collision_between_two_source_files(self, tmp_path: Path) -> None:
        """Two files that sanitize to same name should both fail gracefully."""
        # Both of these would become "donnees.geojson"
        file1 = tmp_path / "données.geojson"
        file2 = tmp_path / "donnees.geojson"  # Already the target name
        file1.write_text("file1")
        file2.write_text("file2")

        issues = [self._make_issue(file1)]

        proposed, applied = apply_safe_fixes(issues, dry_run=False)

        # Should NOT overwrite existing file
        assert file1.exists()
        assert file2.read_text() == "file2"  # Unchanged
        assert len(applied) == 0

    def test_sidecar_collision_prevents_all_renames(self, tmp_path: Path) -> None:
        """If any sidecar target exists, entire shapefile rename should fail."""
        # Source shapefile
        (tmp_path / "données.shp").touch()
        (tmp_path / "données.dbf").touch()
        (tmp_path / "données.shx").touch()

        # Target sidecar already exists
        (tmp_path / "donnees.dbf").write_text("existing")

        issues = [self._make_issue(tmp_path / "données.shp")]

        proposed, applied = apply_safe_fixes(issues, dry_run=False)

        # All original files should still exist
        assert (tmp_path / "données.shp").exists()
        assert (tmp_path / "données.dbf").exists()
        assert (tmp_path / "données.shx").exists()
        # Target .dbf should be unchanged
        assert (tmp_path / "donnees.dbf").read_text() == "existing"
        # No fixes should have been applied
        assert len(applied) == 0


# =============================================================================
# Tests for preview_fix
# =============================================================================


@pytest.mark.unit
class TestPreviewFix:
    """Tests for preview_fix function."""

    def test_preview_invalid_characters(self, tmp_path: Path) -> None:
        """preview_fix should generate ProposedFix for invalid chars."""
        path = tmp_path / "my file.geojson"
        path.touch()

        issue = ScanIssue(
            path=path,
            relative_path=path.name,
            issue_type=IssueType.INVALID_CHARACTERS,
            severity=Severity.WARNING,
            message="Filename has spaces",
        )

        fix = preview_fix(issue)

        assert fix is not None
        assert fix.category == FixCategory.SAFE
        assert fix.action == "rename"
        assert "my_file.geojson" in fix.preview

    def test_preview_windows_reserved(self, tmp_path: Path) -> None:
        """preview_fix should generate ProposedFix for Windows reserved names."""
        path = tmp_path / "CON.shp"
        path.touch()

        issue = ScanIssue(
            path=path,
            relative_path=path.name,
            issue_type=IssueType.WINDOWS_RESERVED_NAME,
            severity=Severity.WARNING,
            message="Windows reserved name",
        )

        fix = preview_fix(issue)

        assert fix is not None
        assert fix.category == FixCategory.SAFE
        assert fix.action == "rename"
        assert "_CON.shp" in fix.preview

    def test_preview_manual_issue_returns_none(self, tmp_path: Path) -> None:
        """preview_fix should return None for manual-only issues."""
        path = tmp_path / "valid.geojson"
        path.touch()

        issue = ScanIssue(
            path=path,
            relative_path=path.name,
            issue_type=IssueType.MULTIPLE_PRIMARIES,
            severity=Severity.WARNING,
            message="Multiple primaries in directory",
        )

        fix = preview_fix(issue)

        assert fix is None

    def test_preview_missing_file_returns_none(self, tmp_path: Path) -> None:
        """preview_fix should return None if file doesn't exist."""
        path = tmp_path / "deleted file.geojson"
        # Don't create the file

        issue = ScanIssue(
            path=path,
            relative_path=path.name,
            issue_type=IssueType.INVALID_CHARACTERS,
            severity=Severity.WARNING,
            message="File has spaces",
        )

        fix = preview_fix(issue)

        assert fix is None

    def test_preview_long_dir_returns_none(self, tmp_path: Path) -> None:
        """preview_fix should return None when dir path exceeds threshold."""
        # Create a directory structure where the DIR path is > 200 chars
        long_dir_name = "d" * 100
        deep_path = tmp_path
        for _ in range(3):
            deep_path = deep_path / long_dir_name
        deep_path.mkdir(parents=True, exist_ok=True)

        path = deep_path / "x.shp"
        path.touch()

        # Verify path exceeds threshold
        assert len(str(path)) > 200

        issue = ScanIssue(
            path=path,
            relative_path=path.name,
            issue_type=IssueType.LONG_PATH,
            severity=Severity.WARNING,
            message="Path too long",
        )

        fix = preview_fix(issue)

        # Should return None because dir path is too long to auto-fix
        assert fix is None

    def test_preview_details_contain_paths(self, tmp_path: Path) -> None:
        """ProposedFix.details should contain old and new paths."""
        path = tmp_path / "test file.geojson"
        path.touch()

        issue = ScanIssue(
            path=path,
            relative_path=path.name,
            issue_type=IssueType.INVALID_CHARACTERS,
            severity=Severity.WARNING,
            message="Spaces in filename",
        )

        fix = preview_fix(issue)

        assert fix is not None
        assert "old_path" in fix.details or "source" in fix.details
        assert "new_path" in fix.details or "target" in fix.details


# =============================================================================
# Tests for UnsafeFixes (placeholder - not in scope for issue #62)
# =============================================================================


@pytest.mark.unit
class TestUnsafeFixes:
    """Tests for unsafe fix operations.

    NOTE: Unsafe fixes are NOT in scope for issue #62.
    These tests are placeholders for future implementation.
    """

    def test_placeholder(self) -> None:
        """Placeholder test - unsafe fix tests to be implemented later."""
        pytest.skip("Unsafe fix tests not in scope for issue #62")
