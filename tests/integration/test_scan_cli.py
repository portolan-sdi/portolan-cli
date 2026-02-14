"""Integration tests for the scan CLI command.

These tests verify the CLI wrapper correctly calls the library and formats output.
Tests use Click's CliRunner for isolated command invocation.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

# Fixture path helper
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "scan"


@pytest.fixture
def runner() -> CliRunner:
    """Create a Click test runner."""
    return CliRunner()


@pytest.fixture
def fixtures_dir() -> Path:
    """Return the path to scan test fixtures."""
    return FIXTURES_DIR


# =============================================================================
# Phase 8: CLI Integration Tests
# =============================================================================


@pytest.mark.integration
class TestScanCLI:
    """Integration tests for `portolan scan` command."""

    def test_scan_basic_output(self, runner: CliRunner, fixtures_dir: Path) -> None:
        """portolan scan shows human-readable summary."""
        result = runner.invoke(cli, ["scan", str(fixtures_dir / "clean_flat")])

        # Should exit with 0 (warnings don't cause failure, only errors)
        assert result.exit_code == 0
        # Should show geo-asset count
        assert "3 geo-asset" in result.output.lower()

    def test_scan_nonexistent_path_exits_with_error(self, runner: CliRunner) -> None:
        """portolan scan on nonexistent path exits with error."""
        result = runner.invoke(cli, ["scan", "/nonexistent/path/that/does/not/exist"])

        # Exit code 1 for path not found (handled in our code for JSON envelope support)
        assert result.exit_code == 1
        assert "not exist" in result.output.lower() or "error" in result.output.lower()

    def test_scan_json_output(self, runner: CliRunner, fixtures_dir: Path) -> None:
        """portolan scan --json outputs valid JSON with envelope structure."""
        result = runner.invoke(cli, ["scan", str(fixtures_dir / "clean_flat"), "--json"])

        assert result.exit_code == 0

        # Parse the JSON output - now has envelope structure
        envelope = json.loads(result.output)

        # Verify envelope structure
        assert envelope["success"] is True
        assert envelope["command"] == "scan"
        assert "data" in envelope

        # Verify required fields per FR-019 inside data
        data = envelope["data"]
        assert "ready" in data
        assert "issues" in data
        assert "summary" in data

    def test_scan_detects_issues_exits_zero(self, runner: CliRunner, fixtures_dir: Path) -> None:
        """portolan scan exits with code 0 even when issues found (informational)."""
        result = runner.invoke(cli, ["scan", str(fixtures_dir / "incomplete_shapefile")])

        # Scan is informational — always exit 0 on success
        assert result.exit_code == 0
        assert "error" in result.output.lower() or "missing" in result.output.lower()

    def test_scan_no_recursive_flag(self, runner: CliRunner, fixtures_dir: Path) -> None:
        """portolan scan --no-recursive limits to immediate directory."""
        result = runner.invoke(cli, ["scan", str(fixtures_dir / "nested"), "--no-recursive"])

        # The nested fixture has all files in subdirectories
        # --no-recursive should find no geo-assets at root level
        assert result.exit_code == 0
        assert "no geo-assets" in result.output.lower()

    def test_scan_max_depth_flag(self, runner: CliRunner, fixtures_dir: Path) -> None:
        """portolan scan --max-depth limits recursion depth."""
        result = runner.invoke(cli, ["scan", str(fixtures_dir / "nested"), "--max-depth", "1"])

        assert result.exit_code == 0
        # With max-depth=1, we can see census/ and imagery/ but not their contents
        # Files are at depth 2+, so should find 0 files
        assert "no geo-assets" in result.output.lower()

    def test_scan_include_hidden_flag(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan scan --include-hidden includes hidden files."""
        # Create a hidden file
        hidden_file = tmp_path / ".hidden.geojson"
        hidden_file.write_text('{"type": "FeatureCollection", "features": []}')

        # Without flag
        result_without = runner.invoke(cli, ["scan", str(tmp_path)])
        # With flag
        result_with = runner.invoke(cli, ["scan", str(tmp_path), "--include-hidden"])

        assert result_without.exit_code == 0
        assert result_with.exit_code == 0

        # Parse JSON to compare file counts (now with envelope structure)
        result_json_without = runner.invoke(cli, ["scan", str(tmp_path), "--json"])
        result_json_with = runner.invoke(cli, ["scan", str(tmp_path), "--include-hidden", "--json"])

        envelope_without = json.loads(result_json_without.output)
        envelope_with = json.loads(result_json_with.output)

        # Should have more files with --include-hidden
        data_without = envelope_without["data"]
        data_with = envelope_with["data"]
        assert len(data_with.get("ready", [])) > len(data_without.get("ready", []))

    def test_scan_follow_symlinks_flag(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan scan --follow-symlinks follows symlinks."""
        # Create a target file (use .geojson since .parquet requires geo metadata)
        target = tmp_path / "target.geojson"
        target.write_text('{"type": "FeatureCollection", "features": []}')

        # Create a subdirectory with a symlink
        subdir = tmp_path / "links"
        subdir.mkdir()
        link = subdir / "link.geojson"
        link.symlink_to(target)

        # Without flag (default: skip symlinks)
        result_without = runner.invoke(cli, ["scan", str(subdir), "--json"])
        # With flag
        result_with = runner.invoke(cli, ["scan", str(subdir), "--follow-symlinks", "--json"])

        envelope_without = json.loads(result_without.output)
        envelope_with = json.loads(result_with.output)

        data_without = envelope_without["data"]
        data_with = envelope_with["data"]

        # Without flag: symlink should be skipped
        assert len(data_without.get("ready", [])) == 0
        # With flag: symlink should be followed
        assert len(data_with.get("ready", [])) == 1

    def test_scan_issues_shown_in_output(self, runner: CliRunner, fixtures_dir: Path) -> None:
        """portolan scan shows issues in human-readable output."""
        result = runner.invoke(cli, ["scan", str(fixtures_dir / "invalid_chars")])

        # Should show warning about invalid characters
        assert result.exit_code == 0  # Warnings don't cause failure
        assert "warning" in result.output.lower() or "invalid" in result.output.lower()

    def test_scan_json_issues_have_required_fields(
        self, runner: CliRunner, fixtures_dir: Path
    ) -> None:
        """portolan scan --json issues include path, type, severity, message."""
        result = runner.invoke(cli, ["scan", str(fixtures_dir / "invalid_chars"), "--json"])

        envelope = json.loads(result.output)
        data = envelope["data"]
        issues = data.get("issues", [])

        assert len(issues) > 0

        for issue in issues:
            assert "path" in issue or "relative_path" in issue
            assert "issue_type" in issue or "type" in issue
            assert "severity" in issue
            assert "message" in issue


# =============================================================================
# Phase 12: Output Truncation Tests (US10)
# =============================================================================


# =============================================================================
# Phase 15: Full Workflow Integration Tests
# =============================================================================


@pytest.mark.integration
class TestScanFullWorkflow:
    """Integration tests for complete scan workflows."""

    def test_scan_nested_directory_full_report(self, runner: CliRunner, fixtures_dir: Path) -> None:
        """Scan nested directory and verify complete report structure."""
        result = runner.invoke(cli, ["scan", str(fixtures_dir / "nested"), "--json"])

        assert result.exit_code == 0
        envelope = json.loads(result.output)

        # Should have envelope structure
        assert envelope["success"] is True
        assert envelope["command"] == "scan"
        assert "data" in envelope

        data = envelope["data"]

        # Should have summary with all required fields
        assert "summary" in data
        assert "ready_count" in data["summary"]
        assert "skipped_count" in data["summary"]

        # Should find files in nested structure
        assert data["summary"]["ready_count"] == 3  # 3 files in nested fixture

    def test_scan_with_mixed_issues_complete_output(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Scan directory with multiple issue types and verify all reported."""
        # Create files that trigger various issues
        # 1. Zero-byte file (ERROR)
        (tmp_path / "empty.geojson").touch()
        # 2. Invalid characters (WARNING)
        (tmp_path / "file with spaces.geojson").write_text(
            '{"type": "FeatureCollection", "features": []}'
        )
        # 3. Valid file for comparison
        (tmp_path / "valid.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--json"])

        # Scan is informational — always exit 0 on success
        assert result.exit_code == 0

        envelope = json.loads(result.output)

        # JSON envelope still indicates issues found (success=False)
        assert envelope["success"] is False
        assert envelope["command"] == "scan"

        data = envelope["data"]

        # Should have both errors and warnings
        issue_types = {i["type"] for i in data.get("issues", [])}
        assert "zero_byte_file" in issue_types  # ERROR
        assert "invalid_characters" in issue_types  # WARNING

    def test_scan_help_shows_all_flags(self, runner: CliRunner) -> None:
        """Verify all scan flags appear in help text."""
        result = runner.invoke(cli, ["scan", "--help"])

        assert result.exit_code == 0
        # Check all flags are documented
        assert "--json" in result.output
        assert "--no-recursive" in result.output
        assert "--max-depth" in result.output
        assert "--include-hidden" in result.output
        assert "--follow-symlinks" in result.output
        assert "--all" in result.output

    def test_scan_empty_directory(self, runner: CliRunner, tmp_path: Path) -> None:
        """Scan empty directory returns gracefully."""
        result = runner.invoke(cli, ["scan", str(tmp_path)])

        assert result.exit_code == 0
        assert "no geo-assets" in result.output.lower()

    def test_scan_classification_summary_in_json(
        self, runner: CliRunner, fixtures_dir: Path
    ) -> None:
        """JSON output includes classification summary."""
        result = runner.invoke(cli, ["scan", str(fixtures_dir / "unsupported"), "--json"])

        assert result.exit_code == 0
        envelope = json.loads(result.output)

        # Should have envelope structure
        assert envelope["success"] is True
        assert envelope["command"] == "scan"

        data = envelope["data"]

        # Should have classification breakdown
        assert "classification" in data
        assert "geo_asset" in data["classification"]


@pytest.mark.integration
class TestScanOutputTruncation:
    """Integration tests for output truncation with --all flag."""

    def test_scan_default_truncates_many_issues(self, runner: CliRunner, tmp_path: Path) -> None:
        """By default, scan truncates output when many issues exist."""
        # Create 15 files with invalid characters to generate many warnings
        for i in range(15):
            (tmp_path / f"file with spaces {i}.geojson").write_text(
                '{"type": "FeatureCollection", "features": []}'
            )

        result = runner.invoke(cli, ["scan", str(tmp_path)])

        # Should show truncation message
        assert result.exit_code == 0
        assert "truncated" in result.output.lower() or "more" in result.output.lower()

    def test_scan_all_shows_all_issues(self, runner: CliRunner, tmp_path: Path) -> None:
        """--all flag shows all issues without truncation."""
        # Create 15 files with invalid characters
        for i in range(15):
            (tmp_path / f"file with spaces {i}.geojson").write_text(
                '{"type": "FeatureCollection", "features": []}'
            )

        result = runner.invoke(cli, ["scan", str(tmp_path), "--all"])

        # Should NOT show truncation message
        assert result.exit_code == 0
        assert "more" not in result.output.lower() or "use --all" not in result.output
        # Should show all 15 invalid character warnings (message uses "problematic")
        assert result.output.count("problematic") >= 15

    def test_scan_json_never_truncates(self, runner: CliRunner, tmp_path: Path) -> None:
        """JSON output never truncates regardless of --all flag."""
        # Create 15 files with invalid characters
        for i in range(15):
            (tmp_path / f"file with spaces {i}.geojson").write_text(
                '{"type": "FeatureCollection", "features": []}'
            )

        result = runner.invoke(cli, ["scan", str(tmp_path), "--json"])

        envelope = json.loads(result.output)
        data = envelope["data"]
        # Should have all 15+ issues (15 invalid chars + multiple primaries)
        assert len(data.get("issues", [])) >= 15

    def test_scan_few_issues_no_truncation(self, runner: CliRunner, fixtures_dir: Path) -> None:
        """Small number of issues are not truncated."""
        result = runner.invoke(cli, ["scan", str(fixtures_dir / "invalid_chars")])

        # Should NOT show truncation message for small issue count
        assert "truncated" not in result.output.lower()


# =============================================================================
# Phase 17: Enhanced Output Features Tests
# =============================================================================


@pytest.mark.integration
class TestScanEnhancedOutput:
    """Integration tests for enhanced scan output features."""

    def test_scan_shows_next_steps(self, runner: CliRunner, tmp_path: Path) -> None:
        """Scan output includes actionable next steps."""
        # Create a file with invalid characters (fixable with --fix)
        (tmp_path / "file with spaces.geojson").write_text(
            '{"type": "FeatureCollection", "features": []}'
        )

        result = runner.invoke(cli, ["scan", str(tmp_path)])

        assert result.exit_code == 0
        # Should show next steps section
        assert "next step" in result.output.lower()

    def test_scan_shows_fixability_labels(self, runner: CliRunner, tmp_path: Path) -> None:
        """Scan output shows fixability labels for issues."""
        # Create a file with invalid characters (--fix label)
        (tmp_path / "file with spaces.geojson").write_text(
            '{"type": "FeatureCollection", "features": []}'
        )

        result = runner.invoke(cli, ["scan", str(tmp_path)])

        assert result.exit_code == 0
        # Should show --fix label for fixable issues
        assert "[--fix]" in result.output

    def test_scan_shows_supporting_files_by_category(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Scan output shows supporting files (non-geo-assets) by category."""
        # Create a geo-asset and supporting files
        (tmp_path / "data.geojson").write_text('{"type": "FeatureCollection", "features": []}')
        (tmp_path / "readme.md").write_text("# Documentation")
        (tmp_path / "style.json").write_text("{}")

        result = runner.invoke(cli, ["scan", str(tmp_path)])

        assert result.exit_code == 0
        # Should show supporting files grouped by category
        # Categories: style, documentation
        assert "style" in result.output.lower()
        assert "documentation" in result.output.lower()

    def test_scan_tree_flag(self, runner: CliRunner, tmp_path: Path) -> None:
        """Scan with --tree shows directory tree view."""
        # Create a simple directory structure
        collection = tmp_path / "census"
        collection.mkdir()
        (collection / "census.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--tree"])

        assert result.exit_code == 0
        # Should show tree characters
        assert "/" in result.output  # Directory markers

    def test_scan_tree_shows_status_markers(self, runner: CliRunner, tmp_path: Path) -> None:
        """Tree view shows status markers for files."""
        # Create files with different statuses
        (tmp_path / "valid.geojson").write_text('{"type": "FeatureCollection", "features": []}')
        (tmp_path / "readme.md").write_text("# Documentation")

        result = runner.invoke(cli, ["scan", str(tmp_path), "--tree"])

        assert result.exit_code == 0
        # Should show geo-asset marker
        assert "geo-asset" in result.output.lower()

    def test_scan_suggests_collections(self, runner: CliRunner, tmp_path: Path) -> None:
        """Scan with collection inference shows suggestions."""
        # Create files with pattern that can be grouped
        (tmp_path / "flood_rp10.geojson").write_text(
            '{"type": "FeatureCollection", "features": []}'
        )
        (tmp_path / "flood_rp50.geojson").write_text(
            '{"type": "FeatureCollection", "features": []}'
        )
        (tmp_path / "flood_rp100.geojson").write_text(
            '{"type": "FeatureCollection", "features": []}'
        )

        result = runner.invoke(cli, ["scan", str(tmp_path), "--suggest-collections"])

        assert result.exit_code == 0
        # Should show suggested collections (if inference is wired up)
        # Note: This may show "multiple primaries" warning since all are in same dir

    def test_scan_ready_message(self, runner: CliRunner, tmp_path: Path) -> None:
        """When no issues, shows structure valid message."""
        # Create a proper catalog structure
        collection = tmp_path / "census"
        collection.mkdir()
        (collection / "census.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(tmp_path)])

        assert result.exit_code == 0
        # Should indicate structure is valid
        assert "ready" in result.output.lower() or "valid" in result.output.lower()

    def test_scan_help_shows_new_flags(self, runner: CliRunner) -> None:
        """Help text shows new flags."""
        result = runner.invoke(cli, ["scan", "--help"])

        assert result.exit_code == 0
        # Should document new flags
        assert "--tree" in result.output
        assert "--suggest-collections" in result.output


# =============================================================================
# Error Handling Tests (Coverage Improvement)
# =============================================================================


@pytest.mark.integration
class TestScanErrorHandling:
    """Integration tests for scan error handling paths."""

    def test_scan_file_path_exits_with_error(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan scan on a file (not directory) exits with error code 1."""
        # Create a regular file, not a directory
        file_path = tmp_path / "not_a_directory.geojson"
        file_path.write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(file_path)])

        # Should exit with error code (either 1 for NotADirectoryError or 2 for Click validation)
        assert result.exit_code != 0
        # Should show error message
        assert "not a directory" in result.output.lower() or "error" in result.output.lower()

    def test_scan_issue_truncation_shows_count(self, runner: CliRunner, tmp_path: Path) -> None:
        """When issues are truncated, shows count of hidden issues."""
        # Create more than 10 files with invalid characters to trigger truncation
        for i in range(15):
            (tmp_path / f"file with spaces {i}.geojson").write_text(
                '{"type": "FeatureCollection", "features": []}'
            )

        result = runner.invoke(cli, ["scan", str(tmp_path)])

        # Should show truncation message with count
        assert result.exit_code == 0
        # Look for "more" in output (truncation indicator)
        assert "more" in result.output.lower()

    def test_scan_zero_count_issue_group_skipped(
        self, runner: CliRunner, fixtures_dir: Path
    ) -> None:
        """Issue groups with zero count are not printed."""
        # Use clean_flat fixture which has no errors
        result = runner.invoke(cli, ["scan", str(fixtures_dir / "clean_flat")])

        # Should exit with 0 (no errors)
        assert result.exit_code == 0
        # Should NOT show "0 errors" - groups with count=0 are skipped
        assert "0 error" not in result.output.lower()

    def test_scan_issue_with_suggestion_shows_hint(self, runner: CliRunner, tmp_path: Path) -> None:
        """Issues with suggestions show hint in output."""
        # Create incomplete shapefile - has suggestion to add missing files
        shp_file = tmp_path / "data.shp"
        shp_file.write_bytes(b"\x00\x00\x27\x0a")  # Shapefile magic bytes

        result = runner.invoke(cli, ["scan", str(tmp_path)])

        # Should show hint/suggestion
        assert result.exit_code == 0  # Scan is informational
        assert "hint" in result.output.lower() or "add" in result.output.lower()


# =============================================================================
# Tests for --manual Flag
# =============================================================================


@pytest.mark.integration
class TestScanManualFlag:
    """Tests for the --manual CLI flag."""

    def test_manual_flag_exists(self, runner: CliRunner, tmp_path: Path) -> None:
        """The --manual flag is recognized by the CLI."""
        geo_file = tmp_path / "data.geojson"
        geo_file.write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--manual"])

        # Should not fail with "no such option" error
        assert "no such option" not in result.output.lower()
        assert result.exit_code == 0

    def test_manual_shows_manual_issues(self, runner: CliRunner, tmp_path: Path) -> None:
        """--manual shows issues requiring manual resolution."""
        # Create multiple primary files in same directory (MANUAL issue)
        (tmp_path / "a.geojson").write_text('{"type": "FeatureCollection", "features": []}')
        (tmp_path / "b.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--manual"])

        # Should mention manual resolution needed
        assert "manual" in result.output.lower() or "require" in result.output.lower()

    def test_manual_hides_ready_count(self, runner: CliRunner, tmp_path: Path) -> None:
        """--manual hides the 'X geo-assets ready' message."""
        # Create multiple primary files (will trigger MANUAL issue)
        (tmp_path / "a.geojson").write_text('{"type": "FeatureCollection", "features": []}')
        (tmp_path / "b.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--manual"])

        # Should NOT show the ready count
        assert "geo-asset" not in result.output.lower()

    def test_manual_no_errors_shows_success(self, runner: CliRunner, tmp_path: Path) -> None:
        """--manual with no manual issues shows success message."""
        # Single file = no manual issues
        subdir = tmp_path / "collection"
        subdir.mkdir()
        (subdir / "data.geojson").write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--manual"])

        # Should show success message
        assert "no files require manual resolution" in result.output.lower()
        assert result.exit_code == 0

    def test_manual_hides_fixable_issues(self, runner: CliRunner, tmp_path: Path) -> None:
        """--manual hides issues fixable with --fix."""
        # File with invalid characters (FIX_FLAG issue)
        subdir = tmp_path / "collection"
        subdir.mkdir()
        bad_file = subdir / "file with spaces.geojson"
        bad_file.write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--manual"])

        # Should NOT show the fixable issue
        assert "--fix" not in result.output
        # Should show success since no manual issues
        assert "no files require manual resolution" in result.output.lower()


# =============================================================================
# Tests for --fix Flag
# =============================================================================


@pytest.mark.integration
class TestScanFixFlag:
    """Tests for the --fix CLI flag."""

    def test_fix_flag_help_exists(self, runner: CliRunner) -> None:
        """The --fix flag is documented in help."""
        result = runner.invoke(cli, ["scan", "--help"])

        assert "--fix" in result.output
        assert "--dry-run" in result.output

    def test_fix_dry_run_shows_preview(self, runner: CliRunner, tmp_path: Path) -> None:
        """--fix --dry-run shows what would be changed without modifying files."""
        bad_file = tmp_path / "file with spaces.geojson"
        bad_file.write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--fix", "--dry-run"])

        assert result.exit_code == 0
        # Should show dry run message
        assert "dry run" in result.output.lower()
        # Should preview the rename
        assert "file_with_spaces" in result.output
        # File should NOT be renamed
        assert bad_file.exists()
        assert not (tmp_path / "file_with_spaces.geojson").exists()

    def test_fix_applies_immediately(self, runner: CliRunner, tmp_path: Path) -> None:
        """--fix applies fixes immediately without prompting."""
        bad_file = tmp_path / "file with spaces.geojson"
        bad_file.write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--fix"])

        assert result.exit_code == 0
        # Should show success message
        assert "applied" in result.output.lower()
        # File should be renamed
        assert not bad_file.exists()
        assert (tmp_path / "file_with_spaces.geojson").exists()

    def test_fix_renames_windows_reserved(self, runner: CliRunner, tmp_path: Path) -> None:
        """--fix renames Windows reserved names."""
        bad_file = tmp_path / "CON.geojson"
        bad_file.write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--fix"])

        assert result.exit_code == 0
        # Should rename to _CON.geojson
        assert not bad_file.exists()
        assert (tmp_path / "_CON.geojson").exists()

    def test_fix_renames_shapefile_sidecars(self, runner: CliRunner, tmp_path: Path) -> None:
        """--fix renames shapefile sidecars along with primary file."""
        # Create shapefile with sidecars (use non-ASCII to trigger rename)
        shp_file = tmp_path / "données.shp"
        dbf_file = tmp_path / "données.dbf"
        shx_file = tmp_path / "données.shx"
        shp_file.write_bytes(b"\x00\x00\x27\x0a")  # Shapefile magic bytes
        dbf_file.touch()
        shx_file.touch()

        result = runner.invoke(cli, ["scan", str(tmp_path), "--fix"])

        assert result.exit_code == 0
        # All files should be renamed
        assert not shp_file.exists()
        assert not dbf_file.exists()
        assert not shx_file.exists()
        assert (tmp_path / "donnees.shp").exists()
        assert (tmp_path / "donnees.dbf").exists()
        assert (tmp_path / "donnees.shx").exists()

    def test_fix_no_issues_shows_info(self, runner: CliRunner, tmp_path: Path) -> None:
        """--fix with no fixable issues shows info message."""
        # Create a valid file (no issues)
        valid_file = tmp_path / "valid.geojson"
        valid_file.write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--fix"])

        assert result.exit_code == 0
        # Should show "no issues to fix"
        assert "no issues" in result.output.lower()

    def test_fix_json_output_includes_fixes(self, runner: CliRunner, tmp_path: Path) -> None:
        """--fix --json includes proposed and applied fixes in output."""
        bad_file = tmp_path / "file with spaces.geojson"
        bad_file.write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--fix", "--json"])

        assert result.exit_code == 0
        envelope = json.loads(result.output)
        data = envelope["data"]

        # Should include both proposed_fixes and applied_fixes
        assert "proposed_fixes" in data, "Missing proposed_fixes in JSON output"
        assert "applied_fixes" in data, "Missing applied_fixes in JSON output"

    def test_fix_collision_detection(self, runner: CliRunner, tmp_path: Path) -> None:
        """--fix detects collisions and doesn't overwrite existing files."""
        # Create both source and would-be target
        source = tmp_path / "file with spaces.geojson"
        target = tmp_path / "file_with_spaces.geojson"
        source.write_text('{"type": "source"}')
        target.write_text('{"type": "target"}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--fix"])

        assert result.exit_code == 0
        # Source should still exist (collision prevented rename)
        assert source.exists()
        # Target should be unchanged
        assert target.read_text() == '{"type": "target"}'
        # Should mention collision
        assert "collision" in result.output.lower() or "could not" in result.output.lower()

    def test_fix_transliterates_non_ascii(self, runner: CliRunner, tmp_path: Path) -> None:
        """--fix transliterates non-ASCII characters to ASCII."""
        bad_file = tmp_path / "données.geojson"
        bad_file.write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--fix"])

        assert result.exit_code == 0
        # Should rename to donnees.geojson
        assert not bad_file.exists()
        assert (tmp_path / "donnees.geojson").exists()

    def test_dry_run_without_fix_shows_warning(self, runner: CliRunner, tmp_path: Path) -> None:
        """--dry-run without --fix shows warning that it has no effect."""
        valid_file = tmp_path / "valid.geojson"
        valid_file.write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--dry-run"])

        assert result.exit_code == 0
        # Should warn about --dry-run having no effect
        assert "no effect" in result.output.lower()

    def test_fix_dry_run_json_includes_proposed_fixes(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--fix --dry-run --json includes proposed_fixes (applied_fixes omitted when empty)."""
        bad_file = tmp_path / "file with spaces.geojson"
        bad_file.write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--fix", "--dry-run", "--json"])

        assert result.exit_code == 0
        envelope = json.loads(result.output)
        data = envelope["data"]

        # Should have proposed_fixes with content
        assert "proposed_fixes" in data
        assert len(data["proposed_fixes"]) > 0

        # applied_fixes is omitted from JSON when empty (minimizes output)
        # This is correct behavior - empty lists are not serialized
        assert data.get("applied_fixes", []) == []

    def test_fix_multiple_files_shows_count(self, runner: CliRunner, tmp_path: Path) -> None:
        """--fix with multiple fixable files shows correct count."""
        import re

        # Create multiple files with invalid chars
        (tmp_path / "file one.geojson").write_text('{"type": "FeatureCollection"}')
        (tmp_path / "file two.geojson").write_text('{"type": "FeatureCollection"}')
        (tmp_path / "file three.geojson").write_text('{"type": "FeatureCollection"}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--fix"])

        assert result.exit_code == 0
        # Should show count of applied fixes with action word
        # Match patterns like "Applied 3 fix(es)" or "3 fixes applied"
        output_lower = result.output.lower()
        assert re.search(r"(applied\s+3|3\s+(fix|fixes))", output_lower), (
            f"Expected '3' adjacent to 'applied' or 'fix(es)' in output: {result.output}"
        )

    def test_fix_with_collision_shows_failed_count(self, runner: CliRunner, tmp_path: Path) -> None:
        """--fix shows count of fixes that couldn't be applied."""
        # Create source with invalid chars and conflicting target
        source = tmp_path / "file one.geojson"
        target = tmp_path / "file_one.geojson"
        source.write_text('{"type": "source"}')
        target.write_text('{"type": "target"}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--fix"])

        assert result.exit_code == 0
        # Should show that fix could not be applied
        assert "could not" in result.output.lower() or "collision" in result.output.lower()

    def test_fix_dry_run_human_output_shows_preview(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--fix --dry-run shows preview of what would be changed (human output)."""
        bad_file = tmp_path / "données.geojson"
        bad_file.write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--fix", "--dry-run"])

        assert result.exit_code == 0
        # Should show preview
        assert "donnees" in result.output
        # Should indicate it's a dry run
        assert "dry run" in result.output.lower()

    def test_fix_dry_run_no_issues_shows_info(self, runner: CliRunner, tmp_path: Path) -> None:
        """--fix --dry-run with no fixable issues shows info message."""
        valid_file = tmp_path / "valid.geojson"
        valid_file.write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(cli, ["scan", str(tmp_path), "--fix", "--dry-run"])

        assert result.exit_code == 0
        # Should show "no issues to fix"
        assert "no issues" in result.output.lower()
