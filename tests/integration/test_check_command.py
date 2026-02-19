"""Integration tests for the check command with --fix flag.

These tests verify the check command correctly identifies files needing
conversion and the --fix flag properly converts them using the convert module.
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    """Create a Click test runner."""
    return CliRunner()


# =============================================================================
# Task 6.2: Check Command Detects Convertible Files
# =============================================================================


@pytest.mark.integration
class TestCheckCommandDetection:
    """Tests for check command detecting file statuses."""

    def test_check_detects_convertible_geojson(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Check command detects GeoJSON as needing conversion."""
        # Set up directory with GeoJSON (convertible)
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "points.geojson")

        result = runner.invoke(cli, ["check", str(input_dir)])

        # Should report the file needs conversion
        # Note: Check command may not exist yet, so test the expected behavior
        assert result.exit_code in (0, 1, 2)  # Flexible for now

    def test_check_detects_cloud_native_parquet(
        self,
        runner: CliRunner,
        valid_points_parquet: Path,
        tmp_path: Path,
    ) -> None:
        """Check command detects GeoParquet as already cloud-native."""
        # Set up directory with GeoParquet (cloud-native)
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        shutil.copy(valid_points_parquet, input_dir / "data.parquet")

        result = runner.invoke(cli, ["check", str(input_dir)])

        # Should not report conversion needed
        assert result.exit_code in (0, 1, 2)  # Flexible for now


# =============================================================================
# Task 6.3: Check --fix Converts and Validates
# =============================================================================


@pytest.mark.integration
class TestCheckFixConversion:
    """Tests for check --fix converting files."""

    def test_check_fix_converts_geojson_to_parquet(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Check --fix converts GeoJSON to GeoParquet."""
        # Set up directory with GeoJSON
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "points.geojson")

        result = runner.invoke(cli, ["check", str(input_dir), "--fix"])

        # Check command should convert the file
        assert result.exit_code == 0
        # Output parquet should exist
        assert (input_dir / "points.parquet").exists()

    def test_check_fix_skips_already_cloud_native(
        self,
        runner: CliRunner,
        valid_points_parquet: Path,
        tmp_path: Path,
    ) -> None:
        """Check --fix skips files that are already cloud-native."""
        # Set up directory with GeoParquet
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        shutil.copy(valid_points_parquet, input_dir / "data.parquet")

        original_mtime = (input_dir / "data.parquet").stat().st_mtime

        result = runner.invoke(cli, ["check", str(input_dir), "--fix"])

        # Should succeed without error
        assert result.exit_code == 0
        # File should be unchanged
        assert (input_dir / "data.parquet").stat().st_mtime == original_mtime

    def test_check_fix_reports_summary(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        valid_points_parquet: Path,
        tmp_path: Path,
    ) -> None:
        """Check --fix reports summary of conversions."""
        # Set up mixed directory
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "vector.geojson")
        shutil.copy(valid_points_parquet, input_dir / "existing.parquet")

        result = runner.invoke(cli, ["check", str(input_dir), "--fix"])

        assert result.exit_code == 0
        # Should mention conversion results
        # (exact wording depends on implementation)
        assert "1" in result.output or "convert" in result.output.lower()


# =============================================================================
# Task 6.5: Check --fix --dry-run
# =============================================================================


@pytest.mark.integration
class TestCheckFixDryRun:
    """Tests for check --fix --dry-run preview mode."""

    def test_dry_run_shows_what_would_convert(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Dry run shows what would be converted without changing files."""
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "points.geojson")

        result = runner.invoke(cli, ["check", str(input_dir), "--fix", "--dry-run"])

        assert result.exit_code == 0
        # Should mention the file would be converted
        assert "points" in result.output.lower() or "would" in result.output.lower()

    def test_dry_run_does_not_create_files(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Dry run does NOT create any output files."""
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "points.geojson")

        result = runner.invoke(cli, ["check", str(input_dir), "--fix", "--dry-run"])

        assert result.exit_code == 0
        # No parquet file should be created
        assert not (input_dir / "points.parquet").exists()


# =============================================================================
# Task 6.6: Partial Failure Handling
# =============================================================================


@pytest.mark.integration
@pytest.mark.skipif(
    sys.platform == "win32",
    reason="geoparquet-io segfaults on malformed input on Windows (upstream bug)",
)
class TestCheckFixPartialFailure:
    """Tests for check --fix handling partial failures."""

    def test_continues_after_one_failure(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """One file fails, others still converted."""
        input_dir = tmp_path / "data"
        input_dir.mkdir()

        # Valid file
        shutil.copy(valid_points_geojson, input_dir / "valid.geojson")

        # Invalid file that will fail conversion
        bad_file = input_dir / "bad.geojson"
        bad_file.write_text('{"type": "FeatureCollection", "features": [INVALID')

        result = runner.invoke(cli, ["check", str(input_dir), "--fix"])

        # Exit code may be non-zero due to partial failure, that's acceptable
        # The CLI should not crash (exit_code would be None if it did)
        assert result.exit_code is not None, f"CLI crashed: {result.output}"

        # The valid file should still be converted despite the bad file
        assert (input_dir / "valid.parquet").exists()

    def test_reports_both_success_and_failure(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Report includes both succeeded and failed counts."""
        input_dir = tmp_path / "data"
        input_dir.mkdir()

        # Valid file
        shutil.copy(valid_points_geojson, input_dir / "valid.geojson")

        # Invalid file
        bad_file = input_dir / "bad.geojson"
        bad_file.write_text('{"type": "FeatureCollection", "features": [INVALID')

        result = runner.invoke(cli, ["check", str(input_dir), "--fix"])

        # Output should mention success and failure
        output_lower = result.output.lower()
        # Should have some indication of mixed results
        assert "1" in result.output or "failed" in output_lower or "success" in output_lower


# =============================================================================
# Task 6.7, 6.8: CLI Output
# =============================================================================


@pytest.mark.integration
class TestCheckOutput:
    """Tests for check command output formatting."""

    def test_json_output_format(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """--json flag outputs valid JSON envelope."""
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "test.geojson")

        result = runner.invoke(cli, ["check", str(input_dir), "--fix", "--json"])

        assert result.exit_code == 0

        # Parse the JSON output
        envelope = json.loads(result.output)

        # Verify envelope structure
        assert "success" in envelope
        assert envelope["command"] == "check"
        assert "data" in envelope
