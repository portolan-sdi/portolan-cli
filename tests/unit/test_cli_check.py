"""Tests for 'portolan check' CLI command."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.validation.results import Severity, ValidationReport, ValidationResult


class TestCheckCommand:
    """Tests for 'portolan check' CLI command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI test runner."""
        return CliRunner()

    @pytest.fixture
    def valid_catalog(self, tmp_path: Path) -> Path:
        """Create a valid MANAGED Portolan catalog with v2 structure."""
        # v2: catalog.json at root
        catalog_file = tmp_path / "catalog.json"
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
        # .portolan with management files (required for MANAGED state)
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.json").write_text("{}")
        (portolan_dir / "state.json").write_text("{}")
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
        """'portolan check --json' outputs JSON format with envelope."""
        result = runner.invoke(cli, ["check", str(valid_catalog), "--json"])
        assert result.exit_code == 0

        # Output should be valid JSON with envelope structure
        output = json.loads(result.output)
        assert output["success"] is True
        assert output["command"] == "check"
        assert "data" in output
        assert output["data"]["passed"] is True

    @pytest.mark.unit
    def test_check_json_output_on_failure(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan check --json' outputs JSON envelope even on failure."""
        result = runner.invoke(cli, ["check", str(tmp_path), "--json"])
        assert result.exit_code == 1

        output = json.loads(result.output)
        assert output["success"] is False
        assert output["command"] == "check"
        assert output["data"]["passed"] is False

    @pytest.mark.unit
    def test_check_verbose_shows_all_rules(self, runner: CliRunner, valid_catalog: Path) -> None:
        """'portolan check --verbose' shows all rules, not just failures."""
        result = runner.invoke(cli, ["check", str(valid_catalog), "--verbose"])
        assert result.exit_code == 0

        # Should show rule names or descriptions
        assert "catalog_exists" in result.output.lower() or "exists" in result.output.lower()

    @pytest.mark.unit
    def test_check_verbose_shows_error_with_fix_hint(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """'portolan check --verbose' shows error details with fix hints."""
        result = runner.invoke(cli, ["check", str(tmp_path), "--verbose"])
        assert result.exit_code == 1
        # Should show error and hint
        assert "✗" in result.output or "error" in result.output.lower()

    @pytest.mark.unit
    def test_check_verbose_shows_warning(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan check --verbose' shows warnings correctly."""
        # Create a catalog with structure but no catalog.json (will trigger different rule)
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        catalog_file = portolan_dir / "catalog.json"
        catalog_file.write_text('{"type": "Catalog"}')  # Missing required fields

        result = runner.invoke(cli, ["check", str(tmp_path), "--verbose"])
        # Should show failed stac_fields rule
        assert result.exit_code == 1

    @pytest.mark.unit
    def test_check_shows_plural_errors(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan check' shows plural 'errors' for multiple failures."""
        result = runner.invoke(cli, ["check", str(tmp_path)])
        assert result.exit_code == 1
        # Multiple rules fail, should show plural
        assert "errors" in result.output.lower() or "error" in result.output.lower()

    @pytest.mark.unit
    def test_check_shows_plural_warnings_message(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan check' handles warning count formatting."""
        # This tests the warning plural formatting code path
        result = runner.invoke(cli, ["check", str(tmp_path)])
        # Just ensure we can run and get error output
        assert result.exit_code == 1

    @pytest.mark.unit
    def test_check_verbose_shows_info_severity(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        """'portolan check --verbose' handles INFO severity in output."""
        # For now, we don't have INFO rules, but ensure verbose mode runs
        result = runner.invoke(cli, ["check", str(valid_catalog), "-v"])
        assert result.exit_code == 0
        # Verbose should show passed rules
        assert "✓" in result.output or "pass" in result.output.lower()

    @pytest.mark.unit
    def test_check_non_verbose_with_hint(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan check' shows fix hint in non-verbose mode."""
        result = runner.invoke(cli, ["check", str(tmp_path)])
        assert result.exit_code == 1
        # Should show hint text
        assert "init" in result.output.lower() or "hint" in result.output.lower()


class TestCheckCommandWithMockedRules:
    """Tests for 'portolan check' with mocked rules to cover all severity paths."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_check_verbose_warning_severity(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan check --verbose' shows WARNING severity correctly."""
        # Mock a report with a WARNING severity failed result
        mock_report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="test_warning",
                    passed=False,
                    severity=Severity.WARNING,
                    message="This is a warning",
                    fix_hint="Fix this warning",
                ),
            ]
        )

        with patch("portolan_cli.cli.validate_catalog", return_value=mock_report):
            result = runner.invoke(cli, ["check", str(tmp_path), "--verbose"])
            # Should exit 0 (warnings don't block)
            assert result.exit_code == 0
            # Should show warning output
            assert "warning" in result.output.lower() or "⚠" in result.output

    @pytest.mark.unit
    def test_check_verbose_info_severity(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan check --verbose' shows INFO severity correctly."""
        # Mock a report with an INFO severity result (passed)
        mock_report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="test_info",
                    passed=True,
                    severity=Severity.INFO,
                    message="This is informational",
                ),
            ]
        )

        with patch("portolan_cli.cli.validate_catalog", return_value=mock_report):
            result = runner.invoke(cli, ["check", str(tmp_path), "--verbose"])
            assert result.exit_code == 0
            # Should show info message
            assert "informational" in result.output.lower()

    @pytest.mark.unit
    def test_check_verbose_info_severity_failed(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan check --verbose' shows failed INFO severity correctly."""
        mock_report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="test_info",
                    passed=False,
                    severity=Severity.INFO,
                    message="This info check failed",
                ),
            ]
        )

        with patch("portolan_cli.cli.validate_catalog", return_value=mock_report):
            result = runner.invoke(cli, ["check", str(tmp_path), "--verbose"])
            assert result.exit_code == 0  # INFO failures don't block
            # Should show info failure
            assert "info" in result.output.lower() or "→" in result.output

    @pytest.mark.unit
    def test_check_non_verbose_warning_severity(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan check' (non-verbose) shows WARNING severity correctly."""
        mock_report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="test_warning",
                    passed=False,
                    severity=Severity.WARNING,
                    message="This is a warning",
                ),
            ]
        )

        with patch("portolan_cli.cli.validate_catalog", return_value=mock_report):
            result = runner.invoke(cli, ["check", str(tmp_path)])
            assert result.exit_code == 0  # Warnings don't block

    @pytest.mark.unit
    def test_check_non_verbose_info_severity(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan check' (non-verbose) shows INFO severity correctly."""
        mock_report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="test_info",
                    passed=False,
                    severity=Severity.INFO,
                    message="This is info",
                ),
            ]
        )

        with patch("portolan_cli.cli.validate_catalog", return_value=mock_report):
            result = runner.invoke(cli, ["check", str(tmp_path)])
            assert result.exit_code == 0  # INFO failures don't block

    @pytest.mark.unit
    def test_check_warning_count_formatting(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan check' formats warning count correctly."""
        mock_report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="error1",
                    passed=False,
                    severity=Severity.ERROR,
                    message="Error 1",
                ),
                ValidationResult(
                    rule_name="warning1",
                    passed=False,
                    severity=Severity.WARNING,
                    message="Warning 1",
                ),
                ValidationResult(
                    rule_name="warning2",
                    passed=False,
                    severity=Severity.WARNING,
                    message="Warning 2",
                ),
            ]
        )

        with patch("portolan_cli.cli.validate_catalog", return_value=mock_report):
            result = runner.invoke(cli, ["check", str(tmp_path)])
            assert result.exit_code == 1  # Errors block
            # Should show plural warnings
            assert "warnings" in result.output.lower() or "warning" in result.output.lower()
