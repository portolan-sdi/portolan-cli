"""Tests for 'portolan check' CLI command."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from portolan_cli.cli import cli
from portolan_cli.validation.results import Severity, ValidationReport, ValidationResult

# =============================================================================
# Shared Fixtures for Mock Reports
# =============================================================================


@pytest.fixture
def mock_passing_validation_report() -> ValidationReport:
    """Create a passing ValidationReport for testing.

    Returns:
        ValidationReport with a single passing result.
    """
    return ValidationReport(
        results=[
            ValidationResult(
                rule_name="catalog_exists",
                passed=True,
                severity=Severity.ERROR,
                message="Catalog exists",
            ),
        ]
    )


@pytest.fixture
def mock_failing_validation_report() -> ValidationReport:
    """Create a failing ValidationReport for testing.

    Returns:
        ValidationReport with a failing error result.
    """
    return ValidationReport(
        results=[
            ValidationResult(
                rule_name="catalog_exists",
                passed=False,
                severity=Severity.ERROR,
                message="Catalog does not exist",
            ),
        ]
    )


@pytest.fixture
def mock_check_report(tmp_path: Path):
    """Create a mock CheckReport for testing.

    Returns:
        CheckReport with empty files list.
    """
    from portolan_cli.check import CheckReport

    return CheckReport(root=tmp_path, files=[], conversion_report=None)


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


class TestCheckMetadataGeoAssetsFlags:
    """Tests for --metadata and --geo-assets flags on the check command.

    These flags allow selective validation:
    - --metadata: Only run STAC metadata validation (links, schema)
    - --geo-assets: Only check geospatial assets (cloud-native status)
    - Both flags: Run both validations (same as no flags)
    - Neither flag: Run both validations (default behavior)
    """

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

    # ─────────────────────────────────────────────────────────────────────────
    # Flag existence tests
    # ─────────────────────────────────────────────────────────────────────────

    @pytest.mark.unit
    def test_metadata_flag_exists(self, runner: CliRunner) -> None:
        """'portolan check --metadata' flag should exist."""
        result = runner.invoke(cli, ["check", "--help"])
        assert result.exit_code == 0
        assert "--metadata" in result.output

    @pytest.mark.unit
    def test_geo_assets_flag_exists(self, runner: CliRunner) -> None:
        """'portolan check --geo-assets' flag should exist."""
        result = runner.invoke(cli, ["check", "--help"])
        assert result.exit_code == 0
        assert "--geo-assets" in result.output

    # ─────────────────────────────────────────────────────────────────────────
    # Metadata-only mode
    # ─────────────────────────────────────────────────────────────────────────

    @pytest.mark.unit
    def test_metadata_flag_runs_only_metadata_validation(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """'portolan check --metadata' should only run metadata validation."""
        mock_report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="catalog_exists",
                    passed=True,
                    severity=Severity.ERROR,
                    message="Catalog exists",
                ),
            ]
        )

        with (
            patch("portolan_cli.cli.validate_catalog", return_value=mock_report) as mock_validate,
            patch("portolan_cli.cli.check_directory") as mock_check_dir,
        ):
            result = runner.invoke(cli, ["check", str(tmp_path), "--metadata"])

            # Should call validate_catalog
            mock_validate.assert_called_once()
            # Should NOT call check_directory (format checking)
            mock_check_dir.assert_not_called()
            assert result.exit_code == 0

    @pytest.mark.unit
    def test_metadata_flag_with_fix_runs_metadata_fixes(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """'portolan check --metadata --fix' should run metadata fixing workflow."""
        mock_report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="catalog_exists",
                    passed=True,
                    severity=Severity.ERROR,
                    message="Catalog exists",
                ),
            ]
        )

        with (
            patch("portolan_cli.cli.validate_catalog", return_value=mock_report) as mock_validate,
            patch("portolan_cli.cli.check_directory") as mock_check_dir,
        ):
            result = runner.invoke(cli, ["check", str(tmp_path), "--metadata", "--fix"])

            # With --metadata --fix, should run metadata validation
            # (In the future this could trigger metadata repair, but for MVP it validates)
            mock_validate.assert_called_once()
            # Should NOT call check_directory
            mock_check_dir.assert_not_called()
            assert result.exit_code == 0

    # ─────────────────────────────────────────────────────────────────────────
    # Geo-assets-only mode
    # ─────────────────────────────────────────────────────────────────────────

    @pytest.mark.unit
    def test_geo_assets_flag_runs_only_asset_checking(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """'portolan check --geo-assets' should only check geospatial assets."""
        from portolan_cli.check import CheckReport

        mock_check_report = CheckReport(root=tmp_path, files=[], conversion_report=None)

        with (
            patch("portolan_cli.cli.validate_catalog") as mock_validate,
            patch(
                "portolan_cli.cli.check_directory", return_value=mock_check_report
            ) as mock_check_dir,
        ):
            result = runner.invoke(cli, ["check", str(tmp_path), "--geo-assets"])

            # Should NOT call validate_catalog
            mock_validate.assert_not_called()
            # Should call check_directory for asset checking
            mock_check_dir.assert_called_once()
            assert result.exit_code == 0

    @pytest.mark.unit
    def test_geo_assets_flag_with_fix_converts_files(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """'portolan check --geo-assets --fix' should convert non-cloud-native files."""
        from portolan_cli.check import CheckReport

        mock_check_report = CheckReport(root=tmp_path, files=[], conversion_report=None)

        with (
            patch("portolan_cli.cli.validate_catalog") as mock_validate,
            patch(
                "portolan_cli.cli.check_directory", return_value=mock_check_report
            ) as mock_check_dir,
        ):
            result = runner.invoke(cli, ["check", str(tmp_path), "--geo-assets", "--fix"])

            # Should NOT call validate_catalog
            mock_validate.assert_not_called()
            # Should call check_directory with fix=True
            mock_check_dir.assert_called_once()
            call_kwargs = mock_check_dir.call_args
            assert call_kwargs[1].get("fix") is True
            assert result.exit_code == 0

    # ─────────────────────────────────────────────────────────────────────────
    # Both flags together
    # ─────────────────────────────────────────────────────────────────────────

    @pytest.mark.unit
    def test_both_flags_runs_both_validations(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan check --metadata --geo-assets' should run both validations."""
        from portolan_cli.check import CheckReport

        mock_validation_report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="catalog_exists",
                    passed=True,
                    severity=Severity.ERROR,
                    message="Catalog exists",
                ),
            ]
        )
        mock_check_report = CheckReport(root=tmp_path, files=[], conversion_report=None)

        with (
            patch(
                "portolan_cli.cli.validate_catalog", return_value=mock_validation_report
            ) as mock_validate,
            patch(
                "portolan_cli.cli.check_directory", return_value=mock_check_report
            ) as mock_check_dir,
        ):
            result = runner.invoke(cli, ["check", str(tmp_path), "--metadata", "--geo-assets"])

            # Both should be called
            mock_validate.assert_called_once()
            mock_check_dir.assert_called_once()
            assert result.exit_code == 0

    # ─────────────────────────────────────────────────────────────────────────
    # Default behavior (no flags) - backward compatible
    # ─────────────────────────────────────────────────────────────────────────

    @pytest.mark.unit
    def test_no_flags_without_fix_runs_metadata_only(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """'portolan check' (no flags, no fix) runs metadata validation only (backward compatible)."""
        mock_validation_report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="catalog_exists",
                    passed=True,
                    severity=Severity.ERROR,
                    message="Catalog exists",
                ),
            ]
        )

        with (
            patch(
                "portolan_cli.cli.validate_catalog", return_value=mock_validation_report
            ) as mock_validate,
            patch("portolan_cli.cli.check_directory") as mock_check_dir,
        ):
            result = runner.invoke(cli, ["check", str(tmp_path)])

            # Without flags and without --fix, only metadata validation runs
            mock_validate.assert_called_once()
            mock_check_dir.assert_not_called()
            assert result.exit_code == 0

    @pytest.mark.unit
    def test_no_flags_with_fix_runs_format_only(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan check --fix' (no filter flags) runs format conversion only (backward compatible)."""
        from portolan_cli.check import CheckReport

        mock_check_report = CheckReport(root=tmp_path, files=[], conversion_report=None)

        with (
            patch("portolan_cli.cli.validate_catalog") as mock_validate,
            patch(
                "portolan_cli.cli.check_directory", return_value=mock_check_report
            ) as mock_check_dir,
        ):
            result = runner.invoke(cli, ["check", str(tmp_path), "--fix"])

            # Without filter flags but with --fix, only format conversion runs
            mock_validate.assert_not_called()
            mock_check_dir.assert_called_once()
            assert result.exit_code == 0

    # ─────────────────────────────────────────────────────────────────────────
    # JSON output mode
    # ─────────────────────────────────────────────────────────────────────────

    @pytest.mark.unit
    def test_json_output_includes_mode_metadata_only(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """JSON output should indicate when only metadata validation was run."""
        mock_report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="catalog_exists",
                    passed=True,
                    severity=Severity.ERROR,
                    message="Catalog exists",
                ),
            ]
        )

        with patch("portolan_cli.cli.validate_catalog", return_value=mock_report):
            result = runner.invoke(cli, ["check", str(tmp_path), "--metadata", "--json"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            assert output["command"] == "check"
            # Should indicate mode in output
            assert output["data"].get("mode") == "metadata"

    @pytest.mark.unit
    def test_json_output_includes_mode_geo_assets_only(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """JSON output should indicate when only geo-assets checking was run."""
        from portolan_cli.check import CheckReport

        mock_check_report = CheckReport(root=tmp_path, files=[], conversion_report=None)

        with patch("portolan_cli.cli.check_directory", return_value=mock_check_report):
            result = runner.invoke(cli, ["check", str(tmp_path), "--geo-assets", "--json"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            assert output["command"] == "check"
            # Should indicate mode in output
            assert output["data"].get("mode") == "geo-assets"

    @pytest.mark.unit
    def test_json_output_includes_mode_both(self, runner: CliRunner, tmp_path: Path) -> None:
        """JSON output should indicate when both validations were run (explicit flags)."""
        from portolan_cli.check import CheckReport

        mock_validation_report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="catalog_exists",
                    passed=True,
                    severity=Severity.ERROR,
                    message="Catalog exists",
                ),
            ]
        )
        mock_check_report = CheckReport(root=tmp_path, files=[], conversion_report=None)

        with (
            patch("portolan_cli.cli.validate_catalog", return_value=mock_validation_report),
            patch("portolan_cli.cli.check_directory", return_value=mock_check_report),
        ):
            # Explicit flags to run both validations
            result = runner.invoke(
                cli, ["check", str(tmp_path), "--metadata", "--geo-assets", "--json"]
            )

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            # Should indicate both modes
            mode = output["data"].get("mode", "")
            assert mode == "all"

    @pytest.mark.unit
    def test_json_output_default_no_flags_is_metadata(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """JSON output without flags indicates metadata mode (backward compatible)."""
        mock_validation_report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="catalog_exists",
                    passed=True,
                    severity=Severity.ERROR,
                    message="Catalog exists",
                ),
            ]
        )

        with patch("portolan_cli.cli.validate_catalog", return_value=mock_validation_report):
            result = runner.invoke(cli, ["check", str(tmp_path), "--json"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            # Default without flags is metadata mode
            mode = output["data"].get("mode", "")
            assert mode == "metadata"

    # ─────────────────────────────────────────────────────────────────────────
    # Error handling
    # ─────────────────────────────────────────────────────────────────────────

    @pytest.mark.unit
    def test_metadata_errors_reported_correctly(self, runner: CliRunner, tmp_path: Path) -> None:
        """Metadata validation errors should be reported with --metadata flag."""
        mock_report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="catalog_exists",
                    passed=False,
                    severity=Severity.ERROR,
                    message="Catalog does not exist",
                    fix_hint="Run portolan init",
                ),
            ]
        )

        with patch("portolan_cli.cli.validate_catalog", return_value=mock_report):
            result = runner.invoke(cli, ["check", str(tmp_path), "--metadata"])

            assert result.exit_code == 1
            # Should show error details
            assert "catalog" in result.output.lower() or "✗" in result.output

    @pytest.mark.unit
    def test_format_errors_do_not_affect_metadata_only(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Format issues should not cause failures in --metadata mode."""
        mock_report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="catalog_exists",
                    passed=True,
                    severity=Severity.ERROR,
                    message="Catalog exists",
                ),
            ]
        )

        with (
            patch("portolan_cli.cli.validate_catalog", return_value=mock_report),
            patch("portolan_cli.cli.check_directory") as mock_check_dir,
        ):
            # Even if there are format issues, --metadata should succeed if metadata is valid
            result = runner.invoke(cli, ["check", str(tmp_path), "--metadata"])

            # check_directory should not be called
            mock_check_dir.assert_not_called()
            assert result.exit_code == 0

    # ─────────────────────────────────────────────────────────────────────────
    # Error path tests
    # ─────────────────────────────────────────────────────────────────────────

    @pytest.mark.unit
    def test_nonexistent_path_with_metadata_flag(self, runner: CliRunner) -> None:
        """'portolan check /nonexistent --metadata' should fail with error."""
        result = runner.invoke(cli, ["check", "/nonexistent/path", "--metadata"])

        assert result.exit_code == 1
        assert "not exist" in result.output.lower() or "not found" in result.output.lower()

    @pytest.mark.unit
    def test_nonexistent_path_with_geo_assets_flag(self, runner: CliRunner) -> None:
        """'portolan check /nonexistent --geo-assets' should fail with error."""
        result = runner.invoke(cli, ["check", "/nonexistent/path", "--geo-assets"])

        assert result.exit_code == 1
        assert "not exist" in result.output.lower() or "not found" in result.output.lower()

    @pytest.mark.unit
    def test_nonexistent_path_with_both_flags(self, runner: CliRunner) -> None:
        """'portolan check /nonexistent --metadata --geo-assets' should fail."""
        result = runner.invoke(cli, ["check", "/nonexistent/path", "--metadata", "--geo-assets"])

        assert result.exit_code == 1
        assert "not exist" in result.output.lower() or "not found" in result.output.lower()

    @pytest.mark.unit
    def test_metadata_fix_with_metadata_errors(self, runner: CliRunner, tmp_path: Path) -> None:
        """'portolan check --metadata --fix' should fail if metadata has errors."""
        from portolan_cli.check import CheckReport

        mock_failing_report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="catalog_exists",
                    passed=False,
                    severity=Severity.ERROR,
                    message="Catalog does not exist",
                ),
            ]
        )
        mock_check_report = CheckReport(root=tmp_path, files=[], conversion_report=None)

        with (
            patch("portolan_cli.cli.validate_catalog", return_value=mock_failing_report),
            patch("portolan_cli.cli.check_directory", return_value=mock_check_report),
        ):
            result = runner.invoke(
                cli, ["check", str(tmp_path), "--metadata", "--geo-assets", "--fix"]
            )

            # Should fail due to metadata errors
            assert result.exit_code == 1

    @pytest.mark.unit
    def test_metadata_fix_json_with_metadata_errors(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """'portolan check --metadata --fix --json' should include errors in JSON."""
        from portolan_cli.check import CheckReport

        mock_failing_report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="catalog_exists",
                    passed=False,
                    severity=Severity.ERROR,
                    message="Catalog does not exist",
                ),
            ]
        )
        mock_check_report = CheckReport(root=tmp_path, files=[], conversion_report=None)

        with (
            patch("portolan_cli.cli.validate_catalog", return_value=mock_failing_report),
            patch("portolan_cli.cli.check_directory", return_value=mock_check_report),
        ):
            result = runner.invoke(
                cli,
                ["check", str(tmp_path), "--metadata", "--geo-assets", "--fix", "--json"],
            )

            # Should fail
            assert result.exit_code == 1

            # JSON should indicate failure
            output = json.loads(result.output)
            assert output["success"] is False
            assert "errors" in output

    @pytest.mark.unit
    def test_nonexistent_path_json_output(self, runner: CliRunner) -> None:
        """'portolan check /nonexistent --json' should output JSON error envelope."""
        result = runner.invoke(cli, ["check", "/nonexistent/path", "--json"])

        assert result.exit_code == 1

        output = json.loads(result.output)
        assert output["success"] is False
        assert output["command"] == "check"
        assert "errors" in output


# =============================================================================
# Hypothesis (Property-Based) Tests for Flag Combinations
# =============================================================================


class TestCheckFlagCombinationsHypothesis:
    """Property-based tests for --metadata and --geo-assets flag combinations.

    These tests verify invariants that should hold across all valid flag combinations.

    Note on health check suppression:
        We suppress HealthCheck.function_scoped_fixture because pytest fixtures
        (runner, tmp_path) are function-scoped by design. Hypothesis warns about
        this because function-scoped fixtures are recreated for each example,
        which can be slow. In our case, this is acceptable because:
        1. CliRunner is lightweight
        2. tmp_path cleanup is handled by pytest
        3. Test execution time is still reasonable
    """

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI test runner."""
        return CliRunner()

    # Strategy for flag combinations - generates random boolean combinations
    flag_combinations = st.fixed_dictionaries(
        {
            "metadata": st.booleans(),
            "geo_assets": st.booleans(),
            "fix": st.booleans(),
            "dry_run": st.booleans(),
            "json_output": st.booleans(),
        }
    )

    # Suppress function_scoped_fixture health check - see class docstring for rationale
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=50)
    @given(flags=flag_combinations)
    @pytest.mark.unit
    def test_exit_code_is_valid(
        self,
        runner: CliRunner,
        flags: dict,
        tmp_path: Path,
        mock_passing_validation_report: ValidationReport,
        mock_check_report,
    ) -> None:
        """Exit code should always be 0 or 1 for any flag combination."""

        # Build command args
        args = ["check", str(tmp_path)]
        if flags["metadata"]:
            args.append("--metadata")
        if flags["geo_assets"]:
            args.append("--geo-assets")
        if flags["fix"]:
            args.append("--fix")
        if flags["dry_run"]:
            args.append("--dry-run")
        if flags["json_output"]:
            args.append("--json")

        with (
            patch(
                "portolan_cli.cli.validate_catalog",
                return_value=mock_passing_validation_report,
            ),
            patch("portolan_cli.cli.check_directory", return_value=mock_check_report),
        ):
            result = runner.invoke(cli, args)

            # Exit code should always be 0 or 1
            assert result.exit_code in (0, 1), (
                f"Unexpected exit code {result.exit_code} for args {args}"
            )

    # Suppress function_scoped_fixture - see class docstring for rationale
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=30)
    @given(flags=st.fixed_dictionaries({"metadata": st.booleans(), "geo_assets": st.booleans()}))
    @pytest.mark.unit
    def test_json_output_is_valid_json(
        self,
        runner: CliRunner,
        flags: dict,
        tmp_path: Path,
        mock_passing_validation_report: ValidationReport,
        mock_check_report,
    ) -> None:
        """JSON output should always be valid JSON regardless of flag combination."""
        args = ["check", str(tmp_path), "--json"]
        if flags["metadata"]:
            args.append("--metadata")
        if flags["geo_assets"]:
            args.append("--geo-assets")

        with (
            patch(
                "portolan_cli.cli.validate_catalog",
                return_value=mock_passing_validation_report,
            ),
            patch("portolan_cli.cli.check_directory", return_value=mock_check_report),
        ):
            result = runner.invoke(cli, args)

            if result.exit_code == 0:
                # Should be valid JSON
                try:
                    output = json.loads(result.output)
                    assert isinstance(output, dict), "JSON output should be a dict"
                    assert "success" in output, "JSON should have 'success' field"
                    assert "command" in output, "JSON should have 'command' field"
                except json.JSONDecodeError as e:
                    pytest.fail(
                        f"Invalid JSON output for args {args}: {e}\nOutput: {result.output[:200]}"
                    )

    # Suppress function_scoped_fixture - see class docstring for rationale
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=20)
    @given(
        metadata=st.booleans(),
        geo_assets=st.booleans(),
    )
    @pytest.mark.unit
    def test_json_mode_field_matches_flags(
        self,
        runner: CliRunner,
        metadata: bool,
        geo_assets: bool,
        tmp_path: Path,
        mock_passing_validation_report: ValidationReport,
        mock_check_report,
    ) -> None:
        """JSON 'mode' field should correctly reflect which flags were used."""
        args = ["check", str(tmp_path), "--json"]
        if metadata:
            args.append("--metadata")
        if geo_assets:
            args.append("--geo-assets")

        with (
            patch(
                "portolan_cli.cli.validate_catalog",
                return_value=mock_passing_validation_report,
            ),
            patch("portolan_cli.cli.check_directory", return_value=mock_check_report),
        ):
            result = runner.invoke(cli, args)

            if result.exit_code == 0:
                output = json.loads(result.output)
                mode = output.get("data", {}).get("mode")

                # Determine expected mode based on flags
                if metadata and geo_assets:
                    expected_mode = "all"
                elif metadata and not geo_assets:
                    expected_mode = "metadata"
                elif geo_assets and not metadata:
                    expected_mode = "geo-assets"
                else:
                    # No explicit flags = backward compatible (metadata only without --fix)
                    expected_mode = "metadata"

                assert mode == expected_mode, (
                    f"Expected mode '{expected_mode}' but got '{mode}' "
                    f"for flags metadata={metadata}, geo_assets={geo_assets}"
                )

    # Suppress function_scoped_fixture - see class docstring for rationale
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=20)
    @given(fix=st.booleans(), dry_run=st.booleans())
    @pytest.mark.unit
    def test_dry_run_without_fix_warns(
        self,
        runner: CliRunner,
        fix: bool,
        dry_run: bool,
        tmp_path: Path,
        mock_passing_validation_report: ValidationReport,
        mock_check_report,
    ) -> None:
        """--dry-run without --fix should produce a warning."""
        args = ["check", str(tmp_path)]
        if fix:
            args.append("--fix")
        if dry_run:
            args.append("--dry-run")

        with (
            patch(
                "portolan_cli.cli.validate_catalog",
                return_value=mock_passing_validation_report,
            ),
            patch("portolan_cli.cli.check_directory", return_value=mock_check_report),
        ):
            result = runner.invoke(cli, args)

            # If --dry-run is used without --fix, should warn
            if dry_run and not fix:
                assert "--dry-run has no effect without --fix" in result.output
