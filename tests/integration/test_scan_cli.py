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
        # Should show file count
        assert "3 files ready" in result.output or "3 file" in result.output.lower()

    def test_scan_nonexistent_path_exits_with_error(self, runner: CliRunner) -> None:
        """portolan scan on nonexistent path exits with error."""
        result = runner.invoke(cli, ["scan", "/nonexistent/path/that/does/not/exist"])

        # Click returns exit code 2 for usage errors (path validation failure)
        assert result.exit_code == 2
        assert "not exist" in result.output.lower() or "error" in result.output.lower()

    def test_scan_json_output(self, runner: CliRunner, fixtures_dir: Path) -> None:
        """portolan scan --json outputs valid JSON."""
        result = runner.invoke(cli, ["scan", str(fixtures_dir / "clean_flat"), "--json"])

        assert result.exit_code == 0

        # Parse the JSON output
        data = json.loads(result.output)

        # Verify required fields per FR-019
        assert "scanned" in data or "ready" in data
        assert "issues" in data
        assert "summary" in data or "directories_scanned" in data

    def test_scan_detects_errors_exits_nonzero(self, runner: CliRunner, fixtures_dir: Path) -> None:
        """portolan scan exits with code 1 when errors found."""
        result = runner.invoke(cli, ["scan", str(fixtures_dir / "incomplete_shapefile")])

        # Should exit with 1 due to incomplete shapefile error
        assert result.exit_code == 1
        assert "error" in result.output.lower() or "missing" in result.output.lower()

    def test_scan_no_recursive_flag(self, runner: CliRunner, fixtures_dir: Path) -> None:
        """portolan scan --no-recursive limits to immediate directory."""
        result = runner.invoke(cli, ["scan", str(fixtures_dir / "nested"), "--no-recursive"])

        # The nested fixture has all files in subdirectories
        # --no-recursive should find 0 files
        assert result.exit_code == 0
        assert "0 files" in result.output.lower() or "no files" in result.output.lower()

    def test_scan_max_depth_flag(self, runner: CliRunner, fixtures_dir: Path) -> None:
        """portolan scan --max-depth limits recursion depth."""
        result = runner.invoke(cli, ["scan", str(fixtures_dir / "nested"), "--max-depth", "1"])

        assert result.exit_code == 0
        # With max-depth=1, we can see census/ and imagery/ but not their contents
        # Files are at depth 2+, so should find 0 files
        assert "0 files" in result.output.lower()

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

        # Parse JSON to compare file counts
        result_json_without = runner.invoke(cli, ["scan", str(tmp_path), "--json"])
        result_json_with = runner.invoke(cli, ["scan", str(tmp_path), "--include-hidden", "--json"])

        data_without = json.loads(result_json_without.output)
        data_with = json.loads(result_json_with.output)

        # Should have more files with --include-hidden
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

        data_without = json.loads(result_without.output)
        data_with = json.loads(result_with.output)

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

        data = json.loads(result.output)
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
        data = json.loads(result.output)

        # Should have summary with all required fields
        assert "summary" in data
        assert "directories_scanned" in data["summary"]
        assert "ready_count" in data["summary"]
        assert "issue_count" in data["summary"]
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

        # Should exit with 1 due to ERROR (zero-byte file)
        assert result.exit_code == 1

        data = json.loads(result.output)

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
        assert "0 files" in result.output.lower()

    def test_scan_classification_summary_in_json(
        self, runner: CliRunner, fixtures_dir: Path
    ) -> None:
        """JSON output includes classification summary."""
        result = runner.invoke(cli, ["scan", str(fixtures_dir / "unsupported"), "--json"])

        assert result.exit_code == 0
        data = json.loads(result.output)

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

        data = json.loads(result.output)
        # Should have all 15+ issues (15 invalid chars + multiple primaries)
        assert len(data.get("issues", [])) >= 15

    def test_scan_few_issues_no_truncation(self, runner: CliRunner, fixtures_dir: Path) -> None:
        """Small number of issues are not truncated."""
        result = runner.invoke(cli, ["scan", str(fixtures_dir / "invalid_chars")])

        # Should NOT show truncation message for small issue count
        assert "truncated" not in result.output.lower()
