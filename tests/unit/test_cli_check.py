"""Tests for 'portolan check' CLI command."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


class TestCheckCommand:
    """Tests for 'portolan check' CLI command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI test runner."""
        return CliRunner()

    @pytest.fixture
    def valid_catalog(self, tmp_path: Path) -> Path:
        """Create a valid Portolan catalog."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        catalog_file = portolan_dir / "catalog.json"
        catalog_file.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "stac_version": "1.0.0",
                    "id": "test-catalog",
                    "description": "A test catalog",
                    "links": [],
                }
            )
        )
        return tmp_path

    @pytest.mark.unit
    def test_check_command_exists(self, runner: CliRunner) -> None:
        """'portolan check' command should exist."""
        result = runner.invoke(cli, ["check", "--help"])
        assert result.exit_code == 0
        assert "Validate" in result.output or "validate" in result.output

    @pytest.mark.unit
    def test_check_passes_for_valid_catalog(self, runner: CliRunner, valid_catalog: Path) -> None:
        """'portolan check' returns exit code 0 for valid catalog."""
        result = runner.invoke(cli, ["check", str(valid_catalog)])
        assert result.exit_code == 0

    @pytest.mark.unit
    def test_check_fails_for_missing_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan check' returns exit code 1 for missing catalog."""
        result = runner.invoke(cli, ["check", str(tmp_path)])
        assert result.exit_code == 1

    @pytest.mark.unit
    def test_check_uses_current_dir_by_default(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        """'portolan check' without path uses current directory."""
        import os

        original_cwd = os.getcwd()
        try:
            os.chdir(valid_catalog)
            result = runner.invoke(cli, ["check"])
            assert result.exit_code == 0
        finally:
            os.chdir(original_cwd)

    @pytest.mark.unit
    def test_check_shows_success_message(self, runner: CliRunner, valid_catalog: Path) -> None:
        """'portolan check' shows success message when validation passes."""
        result = runner.invoke(cli, ["check", str(valid_catalog)])
        # Should contain checkmark or "pass" or "valid"
        assert (
            "✓" in result.output
            or "pass" in result.output.lower()
            or "valid" in result.output.lower()
        )

    @pytest.mark.unit
    def test_check_shows_error_details(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan check' shows error details when validation fails."""
        result = runner.invoke(cli, ["check", str(tmp_path)])
        # Should mention .portolan or catalog
        assert ".portolan" in result.output or "catalog" in result.output.lower()

    @pytest.mark.unit
    def test_check_json_output(self, runner: CliRunner, valid_catalog: Path) -> None:
        """'portolan check --json' outputs JSON format."""
        result = runner.invoke(cli, ["check", str(valid_catalog), "--json"])
        assert result.exit_code == 0

        # Output should be valid JSON
        output = json.loads(result.output)
        assert "passed" in output
        assert output["passed"] is True

    @pytest.mark.unit
    def test_check_json_output_on_failure(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan check --json' outputs JSON even on failure."""
        result = runner.invoke(cli, ["check", str(tmp_path), "--json"])
        assert result.exit_code == 1

        output = json.loads(result.output)
        assert output["passed"] is False

    @pytest.mark.unit
    def test_check_verbose_shows_all_rules(self, runner: CliRunner, valid_catalog: Path) -> None:
        """'portolan check --verbose' shows all rules, not just failures."""
        result = runner.invoke(cli, ["check", str(valid_catalog), "--verbose"])
        assert result.exit_code == 0

        # Should show rule names or descriptions
        assert "catalog_exists" in result.output.lower() or "exists" in result.output.lower()
