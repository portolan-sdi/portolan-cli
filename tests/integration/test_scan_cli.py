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

        assert result.exit_code == 1
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
