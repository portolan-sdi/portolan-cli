"""Check that the README.md examples run. Examples marked ``# notest`` are skipped."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from portolan_cli.cli import cli

from .conftest import MINIMAL_GEOJSON

if TYPE_CHECKING:
    from click.testing import CliRunner


class TestReadmeQuickStart:
    """The README quick start runs without error (push needs S3, so it is skipped)."""

    @pytest.mark.integration
    def test_init_creates_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan init' creates a catalog structure."""
        result = runner.invoke(cli, ["init", str(tmp_path), "--auto", "--license", "CC-BY-4.0"])
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
        # Step 1: Initialize catalog
        result = runner.invoke(cli, ["init", str(tmp_path), "--auto", "--license", "CC-BY-4.0"])
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
    """The README "Other common commands" that run without S3."""

    @pytest.mark.integration
    def test_check_validates_catalog(
        self, runner: CliRunner, catalog_with_minimal_data: Path
    ) -> None:
        """check exits 1 when it reports issues, so the test checks that it runs and prints."""
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
        result = runner.invoke(cli, ["check", str(catalog_with_minimal_data), "--fix", "--json"])

        # A generated catalog still has known gaps, so the run exits 1; the test owns the fix half.
        # A failed conversion lands in the nested report's ``failed``, not in ``unsupported``.
        payload = json.loads(result.output)["data"]
        scan = payload["fix"]["conversion"]
        assert scan["summary"]["unsupported"] == 0, f"check --fix failed: {scan}"

        conversion = scan["conversion"]["summary"]
        assert conversion["failed"] == 0, f"check --fix failed: {scan}"
        assert conversion["invalid"] == 0, f"check --fix failed: {scan}"
        # The quick start's GeoJSON is the file being converted.
        assert conversion["succeeded"] == 1, f"check --fix converted nothing: {scan}"
        assert (demographics / "sample.parquet").exists()

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
        """Sensitive settings need env vars, so the test sets ``backend``."""
        # Save original cwd
        original_cwd = Path.cwd()
        try:
            # Change to catalog directory for config commands
            os.chdir(catalog_with_minimal_data)

            # Set a config value (use non-sensitive setting)
            result = runner.invoke(cli, ["config", "set", "backend", "iceberg"])
            assert result.exit_code == 0, f"config set failed: {result.output}"

            # List config
            result = runner.invoke(cli, ["config", "list"])
            assert result.exit_code == 0, f"config list failed: {result.output}"
            assert "iceberg" in result.output
        finally:
            # Restore original cwd
            os.chdir(original_cwd)


class TestReadmeWorkflowEdgeCases:
    """Edge cases that the README examples can meet."""

    @pytest.mark.integration
    def test_init_already_initialized(
        self, runner: CliRunner, catalog_with_minimal_data: Path
    ) -> None:
        """Re-running 'portolan init' on existing catalog fails gracefully."""
        result = runner.invoke(
            cli, ["init", str(catalog_with_minimal_data), "--auto", "--license", "CC-BY-4.0"]
        )
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
