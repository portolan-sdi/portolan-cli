"""Tests for 'portolan check' CLI command.

The `check` command is the primary CLI command for scanning directories
for geospatial files and reporting issues. It was renamed from `scan`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

# =============================================================================
# Check command tests (renamed from scan)
# =============================================================================


@pytest.mark.unit
class TestCheckCommand:
    """Tests for 'portolan check' CLI command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI test runner."""
        return CliRunner()

    @pytest.fixture
    def geo_dir(self, tmp_path: Path) -> Path:
        """Create a directory with geospatial files."""
        (tmp_path / "data.geojson").write_text('{"type": "FeatureCollection", "features": []}')
        return tmp_path

    @pytest.mark.unit
    def test_check_command_exists(self, runner: CliRunner) -> None:
        """'portolan check' command should exist."""
        result = runner.invoke(cli, ["check", "--help"])
        assert result.exit_code == 0
        assert "check" in result.output.lower()

    @pytest.mark.unit
    def test_check_scans_directory(self, runner: CliRunner, geo_dir: Path) -> None:
        """'portolan check' scans directory for geospatial files."""
        result = runner.invoke(cli, ["check", str(geo_dir)])
        assert result.exit_code == 0
        # Minimal success message
        assert "passed" in result.output.lower() or "✓" in result.output

    @pytest.mark.unit
    def test_check_verbose_shows_assets(self, runner: CliRunner, geo_dir: Path) -> None:
        """'portolan check --verbose' shows geo-asset count."""
        result = runner.invoke(cli, ["check", str(geo_dir), "--verbose"])
        assert result.exit_code == 0
        assert "geo-asset" in result.output.lower()

    @pytest.mark.unit
    def test_check_empty_directory(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan check' handles empty directory (no geo-assets is still a pass)."""
        result = runner.invoke(cli, ["check", str(tmp_path)])
        assert result.exit_code == 0
        # Empty directory with no issues = pass
        assert "passed" in result.output.lower() or "✓" in result.output

    @pytest.mark.unit
    def test_check_json_output(self, runner: CliRunner, geo_dir: Path) -> None:
        """'portolan check --json' outputs JSON format with envelope."""
        result = runner.invoke(cli, ["check", str(geo_dir), "--json"])
        assert result.exit_code == 0

        output = json.loads(result.output)
        assert output["success"] is True
        assert output["command"] == "check"
        assert "data" in output
        assert "ready" in output["data"]

    @pytest.mark.unit
    def test_check_nonexistent_path_returns_error(self, runner: CliRunner, tmp_path: Path) -> None:
        """Check on non-existent path returns error."""
        nonexistent = tmp_path / "does_not_exist"

        result = runner.invoke(cli, ["check", str(nonexistent)])

        assert result.exit_code == 1
        assert "does not exist" in result.output.lower() or "not found" in result.output.lower()

    @pytest.mark.unit
    def test_check_file_path_returns_error(self, runner: CliRunner, tmp_path: Path) -> None:
        """Check on file path returns error."""
        file_path = tmp_path / "file.txt"
        file_path.write_text("hello")

        result = runner.invoke(cli, ["check", str(file_path)])

        assert result.exit_code == 1
        assert "not a directory" in result.output.lower() or "directory" in result.output.lower()


@pytest.mark.unit
class TestCheckCommandOptions:
    """Tests for check command options."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI test runner."""
        return CliRunner()

    @pytest.fixture
    def geo_dir(self, tmp_path: Path) -> Path:
        """Create a directory with geospatial files."""
        (tmp_path / "data.geojson").write_text('{"type": "FeatureCollection", "features": []}')
        return tmp_path

    def test_check_accepts_recursive_option(self, runner: CliRunner, geo_dir: Path) -> None:
        """Check command accepts --no-recursive option."""
        result = runner.invoke(cli, ["check", str(geo_dir), "--no-recursive"])
        assert result.exit_code == 0

    def test_check_accepts_max_depth_option(self, runner: CliRunner, geo_dir: Path) -> None:
        """Check command accepts --max-depth option."""
        result = runner.invoke(cli, ["check", str(geo_dir), "--max-depth", "2"])
        assert result.exit_code == 0

    def test_check_accepts_follow_symlinks_option(self, runner: CliRunner, geo_dir: Path) -> None:
        """Check command accepts --follow-symlinks option."""
        result = runner.invoke(cli, ["check", str(geo_dir), "--follow-symlinks"])
        assert result.exit_code == 0

    def test_check_accepts_all_flag(self, runner: CliRunner, geo_dir: Path) -> None:
        """Check command accepts --all option."""
        result = runner.invoke(cli, ["check", str(geo_dir), "--all"])
        assert result.exit_code == 0

    def test_check_accepts_tree_flag(self, runner: CliRunner, geo_dir: Path) -> None:
        """Check command accepts --tree option."""
        result = runner.invoke(cli, ["check", str(geo_dir), "--tree"])
        assert result.exit_code == 0

    def test_check_accepts_fix_flag(self, runner: CliRunner, geo_dir: Path) -> None:
        """Check command accepts --fix option."""
        result = runner.invoke(cli, ["check", str(geo_dir), "--fix"])
        assert result.exit_code == 0
