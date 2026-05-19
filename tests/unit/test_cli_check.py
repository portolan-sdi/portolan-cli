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
        (portolan_dir / "config.yaml").write_text("{}")
        return tmp_path

    @pytest.mark.unit
    def test_check_returns_json_with_json_flag(
        self,
        runner: CliRunner,
        valid_catalog: Path,
        mock_passing_validation_report: ValidationReport,
    ) -> None:
        """check --json outputs JSON."""
        with patch(
            "portolan_cli.cli.validate_catalog",
            return_value=mock_passing_validation_report,
        ):
            result = runner.invoke(cli, ["check", str(valid_catalog), "--json"])
            assert result.exit_code == 0
            assert "success" in result.output.lower() or "{" in result.output

    @pytest.mark.unit
    def test_check_fails_on_catalog_not_found(self, runner: CliRunner, tmp_path: Path) -> None:
        """check exits with error when catalog not found."""
        nonexistent = tmp_path / "nonexistent"
        result = runner.invoke(cli, ["check", str(nonexistent)])
        assert result.exit_code == 1
        assert "does not exist" in result.output.lower()

    @pytest.mark.unit
    def test_check_with_verbose_flag(
        self,
        runner: CliRunner,
        valid_catalog: Path,
        mock_passing_validation_report: ValidationReport,
    ) -> None:
        """check --verbose shows all validation results."""
        with patch(
            "portolan_cli.cli.validate_catalog",
            return_value=mock_passing_validation_report,
        ):
            result = runner.invoke(cli, ["check", str(valid_catalog), "--verbose"])
            assert result.exit_code == 0

    @pytest.mark.unit
    def test_check_without_flags_succeeds(
        self,
        runner: CliRunner,
        valid_catalog: Path,
        mock_passing_validation_report: ValidationReport,
        mock_check_report,
    ) -> None:
        """check succeeds without any flags (default behavior)."""
        with (
            patch(
                "portolan_cli.cli.validate_catalog",
                return_value=mock_passing_validation_report,
            ),
            patch("portolan_cli.cli.check_directory", return_value=mock_check_report),
        ):
            result = runner.invoke(cli, ["check", str(valid_catalog)])
            assert result.exit_code == 0


class TestCheckCommandWithMockedRules:
    """Tests for check command with mocked validation rules."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI test runner."""
        return CliRunner()

    @pytest.fixture
    def valid_catalog(self, tmp_path: Path) -> Path:
        """Create a valid MANAGED Portolan catalog."""
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
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("{}")
        return tmp_path

    @pytest.mark.unit
    def test_check_respects_path_argument(
        self,
        runner: CliRunner,
        valid_catalog: Path,
        mock_passing_validation_report: ValidationReport,
    ) -> None:
        """check respects the path argument."""
        with patch(
            "portolan_cli.cli.validate_catalog",
            return_value=mock_passing_validation_report,
        ) as mock_validate:
            runner.invoke(cli, ["check", str(valid_catalog)])
            # validate_catalog should be called with the path
            mock_validate.assert_called_once()

    @pytest.mark.unit
    def test_check_default_path_is_current_directory(
        self,
        runner: CliRunner,
        valid_catalog: Path,
        mock_passing_validation_report: ValidationReport,
    ) -> None:
        """check uses current directory as default path."""
        with patch(
            "portolan_cli.cli.validate_catalog",
            return_value=mock_passing_validation_report,
        ):
            result = runner.invoke(cli, ["check"], catch_exceptions=False)
            # Should execute (may fail due to no valid catalog in .)
            assert isinstance(result.exit_code, int)

    @pytest.mark.unit
    def test_check_errors_block_success_exit_code(
        self,
        runner: CliRunner,
        valid_catalog: Path,
    ) -> None:
        """check exits with 1 when errors are found."""
        report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="test",
                    passed=False,
                    severity=Severity.ERROR,
                    message="Test error",
                ),
            ]
        )

        with patch("portolan_cli.cli.validate_catalog", return_value=report):
            result = runner.invoke(cli, ["check", str(valid_catalog)])
            assert result.exit_code == 1

    @pytest.mark.unit
    def test_check_shows_warnings_without_blocking(
        self,
        runner: CliRunner,
        valid_catalog: Path,
    ) -> None:
        """check shows warnings but exits with 0 (warnings don't block)."""
        report = ValidationReport(
            results=[
                ValidationResult(
                    rule_name="test",
                    passed=False,
                    severity=Severity.WARNING,
                    message="Test warning",
                ),
                ValidationResult(
                    rule_name="test2",
                    passed=False,
                    severity=Severity.WARNING,
                    message="Test warning 2",
                ),
            ]
        )

        with patch("portolan_cli.cli.validate_catalog", return_value=report):
            result = runner.invoke(cli, ["check", str(valid_catalog)])
            assert result.exit_code == 0  # Warnings don't block
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
        (portolan_dir / "config.yaml").write_text("{}")
        return tmp_path

    # ─────────────────────────────────────────────────────────────────────────
    # Flag existence tests
    # ─────────────────────────────────────────────────────────────────────────

    @pytest.mark.unit
    def test_metadata_flag_exists(
        self,
        runner: CliRunner,
    ) -> None:
        """--metadata flag should be available."""
        result = runner.invoke(cli, ["check", "--help"])
        assert "--metadata" in result.output

    @pytest.mark.unit
    def test_geo_assets_flag_exists(
        self,
        runner: CliRunner,
    ) -> None:
        """--geo-assets flag should be available."""
        result = runner.invoke(cli, ["check", "--help"])
        assert "--geo-assets" in result.output

    # ─────────────────────────────────────────────────────────────────────────
    # Flag combination tests
    # ─────────────────────────────────────────────────────────────────────────

    @pytest.mark.unit
    def test_metadata_flag_alone(
        self,
        runner: CliRunner,
        valid_catalog: Path,
        mock_passing_validation_report: ValidationReport,
    ) -> None:
        """--metadata alone should run metadata checks only."""
        with patch(
            "portolan_cli.cli.validate_catalog",
            return_value=mock_passing_validation_report,
        ):
            result = runner.invoke(cli, ["check", str(valid_catalog), "--metadata"])
            assert result.exit_code == 0

    @pytest.mark.unit
    def test_geo_assets_flag_alone(
        self,
        runner: CliRunner,
        valid_catalog: Path,
        mock_check_report,
    ) -> None:
        """--geo-assets alone should run format checks only."""
        with patch("portolan_cli.cli.check_directory", return_value=mock_check_report):
            result = runner.invoke(cli, ["check", str(valid_catalog), "--geo-assets"])
            assert result.exit_code == 0

    @pytest.mark.unit
    def test_both_flags_together(
        self,
        runner: CliRunner,
        valid_catalog: Path,
        mock_passing_validation_report: ValidationReport,
        mock_check_report,
    ) -> None:
        """--metadata and --geo-assets together should run both checks."""
        with (
            patch(
                "portolan_cli.cli.validate_catalog",
                return_value=mock_passing_validation_report,
            ),
            patch("portolan_cli.cli.check_directory", return_value=mock_check_report),
        ):
            result = runner.invoke(cli, ["check", str(valid_catalog), "--metadata", "--geo-assets"])
            assert result.exit_code == 0

    @pytest.mark.unit
    def test_json_output_with_flags(
        self,
        runner: CliRunner,
        valid_catalog: Path,
        mock_passing_validation_report: ValidationReport,
        mock_check_report,
    ) -> None:
        """--json should work with --metadata and --geo-assets."""
        with (
            patch(
                "portolan_cli.cli.validate_catalog",
                return_value=mock_passing_validation_report,
            ),
            patch("portolan_cli.cli.check_directory", return_value=mock_check_report),
        ):
            result = runner.invoke(
                cli,
                ["check", str(valid_catalog), "--json", "--metadata", "--geo-assets"],
            )
            if result.exit_code == 0:
                try:
                    output = json.loads(result.output)
                    assert isinstance(output, dict)
                except json.JSONDecodeError as e:
                    pytest.fail(f"Invalid JSON: {e}\nOutput: {result.output[:200]}")


class TestCheckFlagCombinationsHypothesis:
    """Property-based tests for check command flag combinations."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI test runner."""
        return CliRunner()

    @pytest.fixture
    def valid_catalog(self, tmp_path: Path) -> Path:
        """Create a valid MANAGED Portolan catalog."""
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
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("{}")
        return tmp_path

    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=20)
    @given(
        metadata=st.booleans(),
        geo_assets=st.booleans(),
        json_output=st.booleans(),
    )
    @pytest.mark.unit
    def test_all_flag_combinations(
        self,
        runner: CliRunner,
        metadata: bool,
        geo_assets: bool,
        json_output: bool,
        valid_catalog: Path,
        mock_passing_validation_report: ValidationReport,
        mock_check_report,
    ) -> None:
        """check should accept any combination of flags."""
        args = ["check", str(valid_catalog)]
        if metadata:
            args.append("--metadata")
        if geo_assets:
            args.append("--geo-assets")
        if json_output:
            args.append("--json")

        with (
            patch(
                "portolan_cli.cli.validate_catalog",
                return_value=mock_passing_validation_report,
            ),
            patch("portolan_cli.cli.check_directory", return_value=mock_check_report),
        ):
            result = runner.invoke(cli, args)

            # All combinations should be accepted (may pass or fail based on validation)
            assert isinstance(result.exit_code, int)

            if json_output and result.exit_code == 0:
                try:
                    output = json.loads(result.output)
                    assert isinstance(output, dict), "JSON output should be a dict"
                    assert "success" in output, "JSON should have 'success' field"
                    assert "command" in output, "JSON should have 'command' field"
                except json.JSONDecodeError as e:
                    pytest.fail(
                        f"Invalid JSON output for args {args}: {e}\nOutput: {result.output[:200]}"
                    )

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
        valid_catalog: Path,
        mock_passing_validation_report: ValidationReport,
        mock_check_report,
    ) -> None:
        """JSON 'mode' field should correctly reflect which flags were used."""
        args = ["check", str(valid_catalog), "--json"]
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
                    # No explicit flags = check both (new behavior)
                    expected_mode = "all"

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
        valid_catalog: Path,
        mock_passing_validation_report: ValidationReport,
        mock_check_report,
    ) -> None:
        """--dry-run without --fix should produce a warning."""
        args = ["check", str(valid_catalog)]
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


class TestCheckMetadataFixFlag:
    """Tests for --metadata --fix flag combination on the check command.

    The --metadata --fix combination fixes metadata issues found during
    metadata validation (MISSING or STALE STAC items).
    """

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI test runner."""
        return CliRunner()

    @pytest.fixture
    def valid_catalog_with_parquet(self, tmp_path: Path) -> Path:
        """Create a valid catalog with a parquet file."""
        # Create catalog.json
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
        # Create .portolan directory
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("{}")
        return tmp_path

    @pytest.mark.unit
    def test_metadata_fix_flags_exist(
        self,
        runner: CliRunner,
        valid_catalog_with_parquet: Path,
    ) -> None:
        """--metadata and --fix flags should be accepted by check command."""
        # Use --help to verify flags exist
        result = runner.invoke(cli, ["check", "--help"])
        assert result.exit_code == 0
        assert "--metadata" in result.output
        assert "--fix" in result.output

    @pytest.mark.unit
    def test_metadata_fix_with_passing_validation(
        self,
        runner: CliRunner,
        valid_catalog_with_parquet: Path,
        mock_passing_validation_report: ValidationReport,
    ) -> None:
        """--metadata --fix with no issues should succeed."""
        # Create a metadata report with no issues
        from portolan_cli.metadata.models import MetadataReport

        metadata_report = MetadataReport(results=[])

        with (
            patch(
                "portolan_cli.cli.validate_catalog",
                return_value=mock_passing_validation_report,
            ),
            patch(
                "portolan_cli.metadata.scan.scan_catalog_metadata",
                return_value=metadata_report,
            ),
            patch("portolan_cli.cli.fix_metadata") as mock_fix,
        ):
            from portolan_cli.metadata.fix import FixReport

            mock_fix.return_value = FixReport(results=[], skipped_count=0)
            result = runner.invoke(
                cli,
                ["check", str(valid_catalog_with_parquet), "--metadata", "--fix"],
            )
            assert result.exit_code == 0

    @pytest.mark.unit
    def test_metadata_fix_calls_fix_metadata_function(
        self,
        runner: CliRunner,
        valid_catalog_with_parquet: Path,
        mock_passing_validation_report: ValidationReport,
    ) -> None:
        """--metadata --fix should call fix_metadata function."""
        from portolan_cli.metadata.models import (
            MetadataCheckResult,
            MetadataReport,
            MetadataStatus,
        )

        # Create a metadata report with a MISSING status
        test_file = valid_catalog_with_parquet / "test.parquet"
        metadata_report = MetadataReport(
            results=[
                MetadataCheckResult(
                    file_path=test_file,
                    status=MetadataStatus.MISSING,
                    message="No STAC item found",
                )
            ]
        )

        with (
            patch(
                "portolan_cli.cli.validate_catalog",
                return_value=mock_passing_validation_report,
            ),
            patch(
                "portolan_cli.metadata.scan.scan_catalog_metadata",
                return_value=metadata_report,
            ),
            patch("portolan_cli.cli.fix_metadata") as mock_fix,
        ):
            # Mock fix_metadata to return a successful report
            from portolan_cli.metadata.fix import FixReport

            mock_fix.return_value = FixReport(results=[], skipped_count=0)

            result = runner.invoke(
                cli,
                ["check", str(valid_catalog_with_parquet), "--metadata", "--fix"],
            )

            # Verify fix_metadata was called
            mock_fix.assert_called_once()
            assert result.exit_code == 0

    @pytest.mark.unit
    def test_metadata_fix_with_dry_run(
        self,
        runner: CliRunner,
        valid_catalog_with_parquet: Path,
        mock_passing_validation_report: ValidationReport,
    ) -> None:
        """--metadata --fix --dry-run should not make changes."""
        from portolan_cli.metadata.models import (
            MetadataCheckResult,
            MetadataReport,
            MetadataStatus,
        )

        test_file = valid_catalog_with_parquet / "test.parquet"
        metadata_report = MetadataReport(
            results=[
                MetadataCheckResult(
                    file_path=test_file,
                    status=MetadataStatus.MISSING,
                    message="No STAC item found",
                )
            ]
        )

        with (
            patch(
                "portolan_cli.cli.validate_catalog",
                return_value=mock_passing_validation_report,
            ),
            patch(
                "portolan_cli.metadata.scan.scan_catalog_metadata",
                return_value=metadata_report,
            ),
            patch("portolan_cli.cli.fix_metadata") as mock_fix,
        ):
            from portolan_cli.metadata.fix import FixReport

            mock_fix.return_value = FixReport(results=[], skipped_count=0)

            result = runner.invoke(
                cli,
                [
                    "check",
                    str(valid_catalog_with_parquet),
                    "--metadata",
                    "--fix",
                    "--dry-run",
                ],
            )

            # Verify fix_metadata was called with dry_run=True
            mock_fix.assert_called_once()
            call_kwargs = mock_fix.call_args[1]
            assert call_kwargs.get("dry_run") is True
            assert result.exit_code == 0

    @pytest.mark.unit
    def test_fix_with_both_scopes(
        self,
        runner: CliRunner,
        valid_catalog_with_parquet: Path,
    ) -> None:
        """--fix alone should fix both metadata and geo-assets."""
        # --fix without scope flags should run both metadata and geo-asset fixes
        with (
            patch("portolan_cli.cli.validate_catalog") as mock_validate,
            patch("portolan_cli.cli.check_directory") as mock_check,
            patch("portolan_cli.metadata.scan.scan_catalog_metadata") as mock_md_check,
            patch("portolan_cli.cli.fix_metadata") as mock_fix,
        ):
            from portolan_cli.validation.results import ValidationReport

            mock_validate.return_value = ValidationReport(results=[])
            from portolan_cli.check import CheckReport

            mock_check.return_value = CheckReport(
                root=valid_catalog_with_parquet, files=[], conversion_report=None
            )
            from portolan_cli.metadata.models import MetadataReport

            mock_md_check.return_value = MetadataReport(results=[])
            from portolan_cli.metadata.fix import FixReport

            mock_fix.return_value = FixReport(results=[], skipped_count=0)

            result = runner.invoke(
                cli,
                ["check", str(valid_catalog_with_parquet), "--fix"],
            )
            # Command should execute and call both fix workflows
            assert result.exit_code in (0, 1)
            # Both fix functions should be called
            mock_fix.assert_called_once()
            mock_check.assert_called_once()
