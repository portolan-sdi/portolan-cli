"""Test that README.md examples actually work.

These tests verify that the code examples in README.md execute successfully.
If a test fails, either the README is out of date or there's a regression.

Markers:
- @pytest.mark.integration: These are integration tests (touch filesystem)

Non-testable examples (marked with # notest in README):
- Installation commands (pipx, pip, uv sync)
- Commands requiring real S3 credentials

Test organization mirrors README structure:
1. TestReadmeQuickStart - The quick start workflow
2. TestReadmeCommonCommands - "Other common commands" section
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

from .conftest import MINIMAL_GEOJSON


class TestReadmeQuickStart:
    """Test the README Quick Start workflow executes without error.

    The quick start demonstrates the basic flow:
    1. portolan init
    2. portolan scan demographics/
    3. portolan add demographics/
    4. portolan push ... (skipped - requires S3)
    """

    @pytest.mark.integration
    def test_init_creates_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan init' creates a catalog structure."""
        result = runner.invoke(cli, ["init", str(tmp_path), "--auto"])
        assert result.exit_code == 0, f"Init failed: {result.output}"
        # Verify catalog.json created
        assert (tmp_path / "catalog.json").exists()

    @pytest.mark.integration
    def test_scan_reports_issues(self, runner: CliRunner, catalog_with_minimal_data: Path) -> None:
        """'portolan scan demographics/' reports file status."""
        demographics = catalog_with_minimal_data / "demographics"
        result = runner.invoke(cli, ["scan", str(demographics)])
        # Scan should succeed even if it finds issues to report
        assert result.exit_code == 0, f"Scan failed: {result.output}"

    @pytest.mark.integration
    def test_add_creates_collection(
        self, runner: CliRunner, catalog_with_minimal_data: Path
    ) -> None:
        """'portolan add demographics/' creates a collection."""
        demographics = catalog_with_minimal_data / "demographics"
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(catalog_with_minimal_data),
                str(demographics),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"
        # Verify STAC structure
        assert (demographics / "collection.json").exists()

    @pytest.mark.integration
    def test_quickstart_full_workflow(self, runner: CliRunner, tmp_path: Path) -> None:
        """Full quick start workflow: init -> scan -> add.

        This is the integration test that mirrors exactly what the
        README quick start shows (minus the push which requires S3).
        """
        # Step 1: Initialize catalog
        result = runner.invoke(cli, ["init", str(tmp_path), "--auto"])
        assert result.exit_code == 0, f"init failed: {result.output}"

        # Set up: Create demographics directory with data
        demographics = tmp_path / "demographics"
        demographics.mkdir()
        (demographics / "sample.geojson").write_text(MINIMAL_GEOJSON)

        # Step 2: Scan (optional but recommended per README)
        result = runner.invoke(cli, ["scan", str(demographics)])
        assert result.exit_code == 0, f"scan failed: {result.output}"

        # Step 3: Add (creates collection)
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(tmp_path), str(demographics)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, f"add failed: {result.output}"

        # Verify final state
        assert (demographics / "collection.json").exists()
        assert (demographics / "versions.json").exists()


class TestReadmeCommonCommands:
    """Test the 'Other common commands' section from README.

    Tests commands that can run locally without S3:
    - portolan check
    - portolan check --fix
    - portolan rm --keep ...
    - portolan config set/list

    Skipped (require S3 or network):
    - portolan push (tested separately with mocks)
    - portolan pull (tested separately with mocks)
    - portolan sync (tested separately with mocks)
    """

    @pytest.mark.integration
    def test_check_validates_catalog(
        self, runner: CliRunner, catalog_with_minimal_data: Path
    ) -> None:
        """'portolan check' validates the catalog structure.

        Note: check command may exit with code 1 if it finds issues to report.
        The test verifies the command runs and produces output, not that
        the catalog passes all checks. Exit code 0 = pass, 1 = issues found.
        """
        demographics = catalog_with_minimal_data / "demographics"

        # First add data to have something to check
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_with_minimal_data), str(demographics)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, f"add failed: {result.output}"

        # Now check - pass the catalog path as argument
        # Use --geo-assets to check only geo-asset status (not metadata freshness)
        result = runner.invoke(cli, ["check", str(catalog_with_minimal_data), "--geo-assets"])
        # Check should pass when only validating geo-asset status
        assert result.exit_code == 0, f"check failed: {result.output}"

    @pytest.mark.integration
    def test_check_fix_converts_formats(
        self, runner: CliRunner, catalog_with_minimal_data: Path
    ) -> None:
        """'portolan check --fix' attempts format conversion."""
        demographics = catalog_with_minimal_data / "demographics"

        # Add the data first
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_with_minimal_data), str(demographics)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, f"add failed: {result.output}"

        # Run check with --fix
        result = runner.invoke(cli, ["check", str(catalog_with_minimal_data), "--fix"])
        # Should succeed (may convert or report already cloud-native)
        assert result.exit_code == 0, f"check --fix failed: {result.output}"

    @pytest.mark.integration
    def test_rm_keep_untracks_file(
        self, runner: CliRunner, catalog_with_minimal_data: Path
    ) -> None:
        """'portolan rm --keep file' untracks without deleting."""
        demographics = catalog_with_minimal_data / "demographics"
        sample_file = demographics / "sample.geojson"

        # Add first
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_with_minimal_data), str(sample_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, f"add failed: {result.output}"

        # Now rm --keep
        result = runner.invoke(
            cli,
            [
                "rm",
                "--portolan-dir",
                str(catalog_with_minimal_data),
                "--keep",
                str(sample_file),
            ],
        )
        assert result.exit_code == 0, f"rm --keep failed: {result.output}"

        # File should still exist (--keep)
        assert sample_file.exists(), "File was deleted despite --keep flag"

    @pytest.mark.integration
    def test_config_set_and_list(self, runner: CliRunner, catalog_with_minimal_data: Path) -> None:
        """'portolan config set' and 'config list' work together.

        Note: config commands operate on the cwd, so we change to the catalog dir.
        """
        # Save original cwd
        original_cwd = os.getcwd()
        try:
            # Change to catalog directory for config commands
            os.chdir(catalog_with_minimal_data)

            # Set a config value
            result = runner.invoke(cli, ["config", "set", "remote", "s3://test-bucket/catalog"])
            assert result.exit_code == 0, f"config set failed: {result.output}"

            # List config
            result = runner.invoke(cli, ["config", "list"])
            assert result.exit_code == 0, f"config list failed: {result.output}"
            assert "s3://test-bucket/catalog" in result.output
        finally:
            # Restore original cwd
            os.chdir(original_cwd)


class TestReadmeWorkflowEdgeCases:
    """Test edge cases that README examples might encounter.

    These ensure the documented workflow handles:
    - Empty directories
    - Already-initialized catalogs
    - Non-existent paths
    """

    @pytest.mark.integration
    def test_init_already_initialized(
        self, runner: CliRunner, catalog_with_minimal_data: Path
    ) -> None:
        """Re-running 'portolan init' on existing catalog fails gracefully."""
        result = runner.invoke(cli, ["init", str(catalog_with_minimal_data), "--auto"])
        # Should fail or warn - catalog already exists
        # Exit code 1 indicates the expected failure
        assert result.exit_code != 0 or "already" in result.output.lower()

    @pytest.mark.integration
    def test_scan_empty_directory(self, runner: CliRunner, catalog_with_minimal_data: Path) -> None:
        """Scanning an empty directory reports no files."""
        empty_dir = catalog_with_minimal_data / "empty"
        empty_dir.mkdir()

        result = runner.invoke(cli, ["scan", str(empty_dir)])
        # Should succeed but report nothing to scan
        assert result.exit_code == 0

    @pytest.mark.integration
    def test_add_nonexistent_path_fails(
        self, runner: CliRunner, catalog_with_minimal_data: Path
    ) -> None:
        """Adding a non-existent path fails with clear error."""
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(catalog_with_minimal_data),
                str(catalog_with_minimal_data / "does_not_exist"),
            ],
        )
        assert result.exit_code != 0, "Should fail for non-existent path"
