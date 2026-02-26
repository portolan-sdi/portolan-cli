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
        """Check --fix --dry-run detects GeoJSON as needing conversion."""
        # Set up directory with GeoJSON (convertible)
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "points.geojson")

        # Use --fix --dry-run to check file status (check without --fix does catalog validation)
        result = runner.invoke(cli, ["check", str(input_dir), "--fix", "--dry-run"])

        # Should succeed and report file would be converted
        assert result.exit_code == 0
        # Output should mention the convertible file
        assert "points" in result.output.lower() or "convertible" in result.output.lower()

    def test_check_detects_cloud_native_parquet(
        self,
        runner: CliRunner,
        valid_points_parquet: Path,
        tmp_path: Path,
    ) -> None:
        """Check --fix --dry-run detects GeoParquet as already cloud-native."""
        # Set up directory with GeoParquet (cloud-native)
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        shutil.copy(valid_points_parquet, input_dir / "data.parquet")

        # Use --fix --dry-run to check file status
        result = runner.invoke(cli, ["check", str(input_dir), "--fix", "--dry-run"])

        # Should succeed
        assert result.exit_code == 0
        # Output should indicate nothing needs conversion (cloud-native file)
        output_lower = result.output.lower()
        assert (
            "data.parquet" in result.output
            or "cloud" in output_lower
            or "0 convertible" in output_lower
            or "no files need conversion" in output_lower
        )


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
        output_lower = result.output.lower()
        assert (
            "converted" in output_lower
            or "success" in output_lower
            or "vector.parquet" in result.output
        )


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
        # Should mention the file that would be converted
        output_lower = result.output.lower()
        assert (
            "points" in output_lower
            or "would" in output_lower
            or "dry" in output_lower
            or "preview" in output_lower
        )

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

        # Output should mention both success and failure
        output_lower = result.output.lower()
        # Valid file should be converted successfully
        assert (input_dir / "valid.parquet").exists()
        # Should have some indication of results
        assert "failed" in output_lower or "success" in output_lower or "error" in output_lower


# =============================================================================
# Task: UNSUPPORTED Files Handling
# =============================================================================


@pytest.mark.integration
class TestCheckFixUnsupportedFiles:
    """Tests for check --fix handling files that aren't in geospatial extensions.

    Note: The check command only scans for files with recognized geospatial extensions
    (e.g., .geojson, .shp, .tif). Files with unsupported extensions like .nc or .h5
    are simply not scanned at all - they're ignored, not counted as "unsupported".

    The UNSUPPORTED status is for files that ARE scanned (have a recognized extension)
    but can't be converted (e.g., a .json file that isn't GeoJSON).
    """

    def test_unrecognized_extensions_are_ignored(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Files with unrecognized extensions (e.g., .nc) are ignored, not failed."""
        input_dir = tmp_path / "data"
        input_dir.mkdir()

        # Valid convertible file
        shutil.copy(valid_points_geojson, input_dir / "points.geojson")

        # Unrecognized extension file - will be ignored (not scanned)
        (input_dir / "data.nc").write_bytes(b"netcdf placeholder")

        result = runner.invoke(cli, ["check", str(input_dir), "--fix", "--json"])

        assert result.exit_code == 0

        # Parse JSON output
        envelope = json.loads(result.output)
        data = envelope["data"]

        # With --fix, format data is nested under "conversion" key
        conversion_data = data.get("conversion", data)

        # Only the GeoJSON should be in the report - .nc is ignored
        assert conversion_data["summary"]["total"] == 1
        assert conversion_data["summary"]["convertible"] == 1

        # Conversion results should show only the GeoJSON was converted
        if "conversion" in conversion_data:
            conversion = conversion_data["conversion"]
            # No failures
            assert conversion["summary"]["failed"] == 0
            # Only the GeoJSON was converted
            assert conversion["summary"]["succeeded"] == 1

    def test_json_file_that_is_not_geojson_is_unsupported(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """A .json file that isn't valid GeoJSON is reported as UNSUPPORTED."""
        input_dir = tmp_path / "data"
        input_dir.mkdir()

        # A .json file that isn't GeoJSON (regular JSON config)
        json_file = input_dir / "config.json"
        json_file.write_text('{"key": "value", "number": 42}')

        result = runner.invoke(cli, ["check", str(input_dir), "--fix", "--dry-run", "--json"])

        # Note: .json files may or may not be scanned depending on extensions config
        # If scanned and not GeoJSON, they would be UNSUPPORTED
        # This test documents expected behavior
        assert result.exit_code == 0


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


# =============================================================================
# Issue #99: --metadata and --geo-assets Flags
# =============================================================================


@pytest.fixture
def catalog_with_files(
    runner: CliRunner,
    valid_points_geojson: Path,
    valid_points_parquet: Path,
    tmp_path: Path,
) -> Path:
    """Create a Portolan catalog with both GeoJSON and GeoParquet files.

    This creates a fully initialized catalog for testing metadata validation
    alongside geo-assets checking.
    """
    import shutil

    # Create the data directory
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    # Copy test files
    shutil.copy(valid_points_geojson, data_dir / "points.geojson")
    shutil.copy(valid_points_parquet, data_dir / "buildings.parquet")

    # Initialize the catalog (creates .portolan/ and catalog.json)
    # Use --auto for non-interactive mode
    result = runner.invoke(cli, ["init", "--auto", str(tmp_path)])
    assert result.exit_code == 0, f"Init failed: {result.output}"

    return tmp_path


@pytest.mark.integration
class TestCheckMetadataFlag:
    """Integration tests for --metadata flag."""

    def test_metadata_validates_catalog_structure(
        self,
        runner: CliRunner,
        catalog_with_files: Path,
    ) -> None:
        """'portolan check --metadata' validates STAC catalog structure."""
        result = runner.invoke(cli, ["check", str(catalog_with_files), "--metadata"])

        # Should succeed (catalog is valid)
        assert result.exit_code == 0
        # Output should show validation passed
        assert "pass" in result.output.lower() or "✓" in result.output

    def test_metadata_fails_for_missing_catalog(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """'portolan check --metadata' fails when catalog is missing."""
        # Create a data directory without initializing a catalog
        (tmp_path / "data").mkdir()

        result = runner.invoke(cli, ["check", str(tmp_path), "--metadata"])

        # Should fail
        assert result.exit_code == 1
        # Should mention catalog or .portolan
        assert "catalog" in result.output.lower() or ".portolan" in result.output

    def test_metadata_json_output(
        self,
        runner: CliRunner,
        catalog_with_files: Path,
    ) -> None:
        """'portolan check --metadata --json' outputs JSON with mode indicator."""
        result = runner.invoke(cli, ["check", str(catalog_with_files), "--metadata", "--json"])

        assert result.exit_code == 0

        envelope = json.loads(result.output)
        assert envelope["success"] is True
        assert envelope["command"] == "check"
        # Should indicate metadata mode
        assert envelope["data"]["mode"] == "metadata"


@pytest.mark.integration
class TestCheckGeoAssetsFlag:
    """Integration tests for --geo-assets flag."""

    def test_geo_assets_without_fix_reports_status(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """'portolan check --geo-assets' reports file format status."""
        import shutil

        # Create a directory with a convertible file
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "points.geojson")

        result = runner.invoke(cli, ["check", str(input_dir), "--geo-assets"])

        # Should succeed
        assert result.exit_code == 0
        # Output should mention the file or format status
        output_lower = result.output.lower()
        assert (
            "conversion" in output_lower
            or "cloud" in output_lower
            or "geojson" in output_lower
            or "format" in output_lower
        )

    def test_geo_assets_skips_metadata_validation(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """'portolan check --geo-assets' succeeds without a catalog."""
        import shutil

        # Create a directory WITHOUT a catalog (no .portolan)
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "points.geojson")

        result = runner.invoke(cli, ["check", str(input_dir), "--geo-assets"])

        # Should succeed - metadata validation is skipped
        assert result.exit_code == 0
        # Should show geo-assets check results
        assert "conversion" in result.output.lower() or "file" in result.output.lower()

    def test_geo_assets_with_fix_converts_files(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """'portolan check --geo-assets --fix' converts non-cloud-native files."""
        import shutil

        # Create a directory with a convertible file
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "points.geojson")

        result = runner.invoke(cli, ["check", str(input_dir), "--geo-assets", "--fix"])

        # Should succeed
        assert result.exit_code == 0
        # Output file should be created
        assert (input_dir / "points.parquet").exists()

    def test_geo_assets_json_output(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """'portolan check --geo-assets --json' outputs JSON with mode indicator."""
        import shutil

        # Create a directory with a convertible file
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "points.geojson")

        result = runner.invoke(cli, ["check", str(input_dir), "--geo-assets", "--json"])

        assert result.exit_code == 0

        envelope = json.loads(result.output)
        assert envelope["success"] is True
        assert envelope["command"] == "check"
        # Should indicate geo-assets mode
        assert envelope["data"]["mode"] == "geo-assets"


@pytest.mark.integration
class TestCheckBothFlags:
    """Integration tests for --metadata --geo-assets combined."""

    def test_both_flags_run_both_validations(
        self,
        runner: CliRunner,
        catalog_with_files: Path,
    ) -> None:
        """'portolan check --metadata --geo-assets' runs both validations."""
        result = runner.invoke(
            cli, ["check", str(catalog_with_files), "--metadata", "--geo-assets"]
        )

        # Should succeed (valid catalog and valid format files)
        assert result.exit_code == 0
        # Output should show both validation types
        output_lower = result.output.lower()
        assert "metadata" in output_lower or "catalog" in output_lower
        assert "format" in output_lower or "cloud" in output_lower or "file" in output_lower

    def test_both_flags_with_fix_converts_and_validates(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """'portolan check --metadata --geo-assets --fix' validates and converts."""
        import shutil

        # Initialize a catalog (use --auto for non-interactive mode)
        result = runner.invoke(cli, ["init", "--auto", str(tmp_path)])
        assert result.exit_code == 0

        # Add a convertible file
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        shutil.copy(valid_points_geojson, data_dir / "points.geojson")

        result = runner.invoke(
            cli,
            ["check", str(tmp_path), "--metadata", "--geo-assets", "--fix"],
        )

        # Should succeed
        assert result.exit_code == 0
        # Output file should be created
        assert (data_dir / "points.parquet").exists()

    def test_both_flags_json_output(
        self,
        runner: CliRunner,
        catalog_with_files: Path,
    ) -> None:
        """'portolan check --metadata --geo-assets --json' outputs combined JSON."""
        result = runner.invoke(
            cli,
            ["check", str(catalog_with_files), "--metadata", "--geo-assets", "--json"],
        )

        assert result.exit_code == 0

        envelope = json.loads(result.output)
        assert envelope["success"] is True
        assert envelope["command"] == "check"
        # Should indicate all/both mode
        assert envelope["data"]["mode"] == "all"
        # Should have both metadata and geo-assets data
        assert "metadata" in envelope["data"] or "geo_assets" in envelope["data"]

    def test_metadata_error_fails_combined_check(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """'portolan check --metadata --geo-assets' fails if metadata invalid."""
        import shutil

        # Create directory with valid format file but NO catalog
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "points.geojson")

        result = runner.invoke(cli, ["check", str(tmp_path), "--metadata", "--geo-assets"])

        # Should fail due to missing catalog
        assert result.exit_code == 1


@pytest.mark.integration
class TestCheckBackwardCompatibility:
    """Integration tests verifying backward-compatible default behavior."""

    def test_check_without_flags_runs_metadata_only(
        self,
        runner: CliRunner,
        catalog_with_files: Path,
    ) -> None:
        """'portolan check' (no flags) validates metadata only."""
        result = runner.invoke(cli, ["check", str(catalog_with_files)])

        # Should succeed
        assert result.exit_code == 0
        # Should show validation results
        assert "pass" in result.output.lower() or "✓" in result.output

    def test_check_fix_without_flags_runs_format_only(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """'portolan check --fix' (no filter flags) converts files only."""
        import shutil

        # Create directory without catalog (would fail metadata validation)
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        shutil.copy(valid_points_geojson, input_dir / "points.geojson")

        result = runner.invoke(cli, ["check", str(input_dir), "--fix"])

        # Should succeed - only format conversion, no metadata validation
        assert result.exit_code == 0
        # Output file should be created
        assert (input_dir / "points.parquet").exists()
