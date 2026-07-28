"""Tests for 'portolan check' CLI command.

Validation runs on rashid (ADR-0057), so these tests build catalogs through the
real generator rather than hand-writing catalog.json: a hand-built catalog trips
half the PTL-* rule set before the test's own subject is reached. Findings a
test asserts on are ones it introduces itself.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from portolan_cli.cli import cli

# =============================================================================
# Shared fixtures
# =============================================================================


@pytest.fixture
def runner() -> CliRunner:
    """A CLI test runner."""
    return CliRunner()


@pytest.fixture
def valid_catalog(conformant_catalog: Path) -> Path:
    """A generated catalog that passes validation with no findings."""
    return conformant_catalog


@pytest.fixture
def mock_check_report(tmp_path: Path) -> Any:
    """A CheckReport covering no source files."""
    from portolan_cli.scan.check import CheckReport

    return CheckReport(root=tmp_path, files=[], conversion_report=None)


def _break_catalog(root: Path) -> str:
    """Introduce one PTL-LNK-005 error and return the rule id it raises."""
    catalog_file = root / "catalog.json"
    doc = json.loads(catalog_file.read_text())
    doc["links"].append({"rel": "self", "href": "./catalog.json", "type": "application/json"})
    catalog_file.write_text(json.dumps(doc))
    return "PTL-LNK-005"


def _warn_catalog(root: Path) -> str:
    """Introduce one WARNING-severity finding and return its rule id.

    PTL-TTL-002 fires on a title that is a raw slug rather than prose.
    """
    catalog_file = root / "catalog.json"
    doc = json.loads(catalog_file.read_text())
    doc["title"] = "road_centerlines_2024"
    catalog_file.write_text(json.dumps(doc))
    return "PTL-TTL-002"


class TestCheckCommand:
    """The check command's basic contract."""

    @pytest.mark.unit
    def test_conformant_catalog_passes(self, runner: CliRunner, valid_catalog: Path) -> None:
        result = runner.invoke(cli, ["check", str(valid_catalog)])
        assert result.exit_code == 0
        assert "Catalog conforms" in result.output

    @pytest.mark.unit
    def test_check_returns_json_with_json_flag(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        result = runner.invoke(cli, ["check", str(valid_catalog), "--json"])
        assert result.exit_code == 0
        envelope = json.loads(result.output)
        assert envelope["success"] is True
        assert envelope["data"]["passed"] is True

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "extra_flags",
        [
            pytest.param(["--metadata"], id="metadata-only"),
            pytest.param(["--geo-assets"], id="geo-assets-only"),
            pytest.param(["--metadata", "--geo-assets"], id="combined"),
        ],
    )
    def test_check_json_exposes_spec_version(
        self, runner: CliRunner, valid_catalog: Path, extra_flags: list[str]
    ) -> None:
        """check --json reports the Portolan spec version across every scope (#566)."""
        from rashid.schema import bundled_schema_versions

        expected = max(bundled_schema_versions()).removeprefix("v")

        result = runner.invoke(cli, ["check", str(valid_catalog), *extra_flags, "--json"])
        assert result.exit_code == 0
        envelope = json.loads(result.output)
        assert envelope["data"]["spec_version"] == expected

    @pytest.mark.unit
    def test_check_json_names_the_validator(self, runner: CliRunner, valid_catalog: Path) -> None:
        """The payload identifies rashid and its version, so a report is reproducible."""
        result = runner.invoke(cli, ["check", str(valid_catalog), "--json"])
        assert json.loads(result.output)["data"]["validator"]["name"] == "rashid"

    @pytest.mark.unit
    def test_check_fails_on_catalog_not_found(self, runner: CliRunner, tmp_path: Path) -> None:
        nonexistent = tmp_path / "nonexistent"
        result = runner.invoke(cli, ["check", str(nonexistent)])
        assert result.exit_code == 1
        assert "does not exist" in result.output.lower()

    @pytest.mark.unit
    def test_missing_catalog_json_reports_ptl_gen_000(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """A directory with no catalog.json is not a catalog, and check says which rule."""
        result = runner.invoke(cli, ["check", str(tmp_path), "--metadata"])
        assert result.exit_code == 1
        assert "PTL-GEN-000" in result.output

    @pytest.mark.unit
    def test_check_with_verbose_flag(self, runner: CliRunner, valid_catalog: Path) -> None:
        result = runner.invoke(cli, ["check", str(valid_catalog), "--verbose"])
        assert result.exit_code == 0


class TestCheckExitCodes:
    """Errors fail; warnings only fail under --strict."""

    @pytest.mark.unit
    def test_errors_exit_nonzero_and_name_the_rule(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        rule_id = _break_catalog(valid_catalog)
        result = runner.invoke(cli, ["check", str(valid_catalog)])
        assert result.exit_code == 1
        assert rule_id in result.output
        assert "Catalog does not conform" in result.output

    @pytest.mark.unit
    def test_warnings_do_not_block(self, runner: CliRunner, valid_catalog: Path) -> None:
        rule_id = _warn_catalog(valid_catalog)
        result = runner.invoke(cli, ["check", str(valid_catalog)])
        assert result.exit_code == 0
        assert rule_id in result.output

    @pytest.mark.unit
    def test_strict_turns_warnings_into_failure(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        """--strict fails the run on warnings (behavior change in the rashid swap)."""
        _warn_catalog(valid_catalog)
        result = runner.invoke(cli, ["check", str(valid_catalog), "--strict"])
        assert result.exit_code == 1

    @pytest.mark.unit
    def test_strict_still_passes_a_clean_catalog(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        result = runner.invoke(cli, ["check", str(valid_catalog), "--strict"])
        assert result.exit_code == 0


class TestCheckFindingPayload:
    """The JSON findings carry rashid's fields plus the remediation enrichment."""

    @pytest.mark.unit
    def test_finding_carries_remediation_and_requirement(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        rule_id = _break_catalog(valid_catalog)
        result = runner.invoke(cli, ["check", str(valid_catalog), "--json"])
        assert result.exit_code == 1
        envelope = json.loads(result.output)
        finding = next(f for f in envelope["data"]["findings"] if f["rule_id"] == rule_id)
        assert finding["remediation"] == "auto"
        assert finding["auto_fixable"] is True
        assert finding["requirement"] == "Remove the self link; a SELF_CONTAINED catalog omits it."

    @pytest.mark.unit
    def test_error_envelope_names_the_failing_rule(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        rule_id = _break_catalog(valid_catalog)
        result = runner.invoke(cli, ["check", str(valid_catalog), "--json"])
        envelope = json.loads(result.output)
        assert envelope["success"] is False
        assert [e["type"] for e in envelope["errors"]] == [rule_id]

    @pytest.mark.unit
    def test_counts_by_remediation_is_present(self, runner: CliRunner, valid_catalog: Path) -> None:
        _break_catalog(valid_catalog)
        result = runner.invoke(cli, ["check", str(valid_catalog), "--json"])
        counts = json.loads(result.output)["data"]["counts_by_remediation"]
        assert counts["auto"] == 1


class TestCheckPassFlags:
    """--no-data, --no-structural, --schema, --live route to rashid's passes."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "flag", ["--live", "--no-data", "--no-structural", "--schema", "--url"]
    )
    def test_flags_are_advertised(self, runner: CliRunner, flag: str) -> None:
        result = runner.invoke(cli, ["check", "--help"])
        assert flag in result.output

    @pytest.mark.unit
    def test_no_data_disables_the_data_pass(self, runner: CliRunner, valid_catalog: Path) -> None:
        with patch("portolan_cli.cli.run_check") as mock_run:
            mock_run.return_value = _empty_outcome()
            runner.invoke(cli, ["check", str(valid_catalog), "--no-data"])
        assert mock_run.call_args.kwargs["data"] is False

    @pytest.mark.unit
    def test_data_pass_is_on_by_default(self, runner: CliRunner, valid_catalog: Path) -> None:
        with patch("portolan_cli.cli.run_check") as mock_run:
            mock_run.return_value = _empty_outcome()
            runner.invoke(cli, ["check", str(valid_catalog)])
        assert mock_run.call_args.kwargs["data"] is True

    @pytest.mark.unit
    def test_structural_is_on_by_default_and_disablable(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        with patch("portolan_cli.cli.run_check") as mock_run:
            mock_run.return_value = _empty_outcome()
            runner.invoke(cli, ["check", str(valid_catalog)])
            assert mock_run.call_args.kwargs["structural"] is True
            runner.invoke(cli, ["check", str(valid_catalog), "--no-structural"])
            assert mock_run.call_args.kwargs["structural"] is False

    @pytest.mark.unit
    def test_schema_is_off_by_default_and_enablable(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        """The profile schema pass restates hand-rule findings, so it is opt-in."""
        with patch("portolan_cli.cli.run_check") as mock_run:
            mock_run.return_value = _empty_outcome()
            runner.invoke(cli, ["check", str(valid_catalog)])
            assert mock_run.call_args.kwargs["schema"] is False
            runner.invoke(cli, ["check", str(valid_catalog), "--schema"])
            assert mock_run.call_args.kwargs["schema"] is True

    @pytest.mark.unit
    def test_live_is_off_by_default_and_enablable(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        with patch("portolan_cli.cli.run_check") as mock_run:
            mock_run.return_value = _empty_outcome()
            runner.invoke(cli, ["check", str(valid_catalog)])
            assert mock_run.call_args.kwargs["live"] is False
            runner.invoke(cli, ["check", str(valid_catalog), "--live"])
            assert mock_run.call_args.kwargs["live"] is True

    @pytest.mark.unit
    def test_url_is_threaded_through(self, runner: CliRunner, valid_catalog: Path) -> None:
        with patch("portolan_cli.cli.run_check") as mock_run:
            mock_run.return_value = _empty_outcome()
            runner.invoke(
                cli, ["check", str(valid_catalog), "--url", "https://data.example.org/c/"]
            )
        assert mock_run.call_args.kwargs["public_url"] == "https://data.example.org/c/"


def _empty_outcome() -> Any:
    """A CheckOutcome with a clean report and nothing else set."""
    from rashid.model import Report

    from portolan_cli.validation.runner import CheckOutcome

    return CheckOutcome(
        report=Report(findings=[], files_checked=1),
        format_report=None,
        legacy_note=None,
        live_hint=None,
    )


class TestLiveHintOutput:
    @pytest.mark.unit
    def test_published_catalog_suggests_live(self, runner: CliRunner, valid_catalog: Path) -> None:
        config = valid_catalog / ".portolan" / "config.yaml"
        config.write_text(
            config.read_text() + "publish:\n  public_url: https://data.example.org/c/\n",
            encoding="utf-8",
        )
        result = runner.invoke(cli, ["check", str(valid_catalog)])
        assert "portolan check --live" in result.output

    @pytest.mark.unit
    def test_unpublished_catalog_does_not(self, runner: CliRunner, valid_catalog: Path) -> None:
        result = runner.invoke(cli, ["check", str(valid_catalog)])
        assert "--live" not in result.output


class TestLegacyCatalogNote:
    @pytest.mark.unit
    def test_pre_schema_catalog_is_called_out(self, runner: CliRunner, tmp_path: Path) -> None:
        (tmp_path / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "stac_version": "1.1.0",
                    "id": "old",
                    "description": "An old catalog.",
                    "links": [],
                    "portolan:version": "0.0.9",
                }
            ),
            encoding="utf-8",
        )
        result = runner.invoke(cli, ["check", str(tmp_path), "--metadata"])
        assert "predates the Portolan profile schema" in result.output


class TestCheckMetadataGeoAssetsFlags:
    """--metadata and --geo-assets select what is checked."""

    @pytest.mark.unit
    @pytest.mark.parametrize("flag", ["--metadata", "--geo-assets"])
    def test_scope_flags_are_advertised(self, runner: CliRunner, flag: str) -> None:
        result = runner.invoke(cli, ["check", "--help"])
        assert flag in result.output

    @pytest.mark.unit
    def test_metadata_flag_alone_skips_the_source_scan(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        result = runner.invoke(cli, ["check", str(valid_catalog), "--metadata"])
        assert result.exit_code == 0
        assert "Source files:" not in result.output

    @pytest.mark.unit
    def test_geo_assets_flag_alone_skips_validation(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        """--geo-assets does not validate the catalog, so a broken one still exits 0."""
        _break_catalog(valid_catalog)
        result = runner.invoke(cli, ["check", str(valid_catalog), "--geo-assets"])
        assert result.exit_code == 0
        assert "Source files:" in result.output

    @pytest.mark.unit
    def test_both_flags_run_both(self, runner: CliRunner, valid_catalog: Path) -> None:
        result = runner.invoke(cli, ["check", str(valid_catalog), "--metadata", "--geo-assets"])
        assert result.exit_code == 0
        assert "Catalog conforms" in result.output
        assert "Source files:" in result.output


class TestCheckFlagCombinationsHypothesis:
    """Property-based tests for check command flag combinations."""

    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=20)
    @given(metadata=st.booleans(), geo_assets=st.booleans(), json_output=st.booleans())
    @pytest.mark.unit
    def test_all_flag_combinations(
        self,
        runner: CliRunner,
        metadata: bool,
        geo_assets: bool,
        json_output: bool,
        valid_catalog: Path,
    ) -> None:
        """Every scope combination succeeds on a conformant catalog."""
        args = ["check", str(valid_catalog)]
        if metadata:
            args.append("--metadata")
        if geo_assets:
            args.append("--geo-assets")
        if json_output:
            args.append("--json")

        result = runner.invoke(cli, args)
        assert result.exit_code == 0

        if json_output:
            output = json.loads(result.output)
            assert output["success"] is True
            assert output["command"] == "check"

    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=20)
    @given(metadata=st.booleans(), geo_assets=st.booleans())
    @pytest.mark.unit
    def test_json_mode_field_matches_flags(
        self, runner: CliRunner, metadata: bool, geo_assets: bool, valid_catalog: Path
    ) -> None:
        """JSON 'mode' reflects which scope flags were used."""
        args = ["check", str(valid_catalog), "--json"]
        if metadata:
            args.append("--metadata")
        if geo_assets:
            args.append("--geo-assets")

        result = runner.invoke(cli, args)
        assert result.exit_code == 0

        if metadata and not geo_assets:
            expected_mode = "metadata"
        elif geo_assets and not metadata:
            expected_mode = "geo-assets"
        else:
            expected_mode = "all"

        assert json.loads(result.output)["data"]["mode"] == expected_mode

    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=20)
    @given(fix=st.booleans(), dry_run=st.booleans())
    @pytest.mark.unit
    def test_dry_run_without_fix_warns(
        self, runner: CliRunner, fix: bool, dry_run: bool, valid_catalog: Path
    ) -> None:
        """--dry-run without --fix should produce a warning."""
        args = ["check", str(valid_catalog)]
        if fix:
            args.append("--fix")
        if dry_run:
            args.append("--dry-run")

        result = runner.invoke(cli, args)

        if dry_run and not fix:
            assert "--dry-run has no effect without --fix" in result.output
        else:
            assert "--dry-run has no effect without --fix" not in result.output


class TestCheckMetadataFixFlag:
    """--fix drives the existing repair machinery, then re-validates."""

    @pytest.fixture
    def valid_catalog_with_parquet(self, conformant_catalog: Path) -> Path:
        return conformant_catalog

    @pytest.mark.unit
    def test_metadata_fix_flags_exist(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["check", "--help"])
        assert result.exit_code == 0
        assert "--metadata" in result.output
        assert "--fix" in result.output

    @pytest.mark.unit
    def test_metadata_fix_with_passing_validation(
        self, runner: CliRunner, valid_catalog_with_parquet: Path
    ) -> None:
        """--metadata --fix with no issues should succeed."""
        from portolan_cli.metadata.models import MetadataReport

        with (
            patch(
                "portolan_cli.metadata.scan.scan_catalog_metadata",
                return_value=MetadataReport(results=[]),
            ),
            patch("portolan_cli.metadata.fix_metadata") as mock_fix,
        ):
            from portolan_cli.metadata.fix import FixReport

            mock_fix.return_value = FixReport(results=[], fresh_skipped=0)
            result = runner.invoke(
                cli, ["check", str(valid_catalog_with_parquet), "--metadata", "--fix"]
            )
            assert result.exit_code == 0

    @pytest.mark.unit
    def test_fix_json_reports_post_fix_state_in_one_envelope(
        self, runner: CliRunner, valid_catalog_with_parquet: Path
    ) -> None:
        """--fix --json emits a single envelope carrying both the fixes and the re-check."""
        from portolan_cli.constants import PORTOLAN_SPEC_VERSION
        from portolan_cli.metadata.models import MetadataReport

        with (
            patch(
                "portolan_cli.metadata.scan.scan_catalog_metadata",
                return_value=MetadataReport(results=[]),
            ),
            patch("portolan_cli.metadata.fix_metadata") as mock_fix,
        ):
            from portolan_cli.metadata.fix import FixReport

            mock_fix.return_value = FixReport(results=[], fresh_skipped=0)
            result = runner.invoke(
                cli, ["check", str(valid_catalog_with_parquet), "--metadata", "--fix", "--json"]
            )
            assert result.exit_code == 0
            envelope = json.loads(result.output)
            assert envelope["data"]["spec_version"] == PORTOLAN_SPEC_VERSION
            assert "metadata_fix" in envelope["data"]["fix"]
            assert envelope["data"]["passed"] is True

    @pytest.mark.unit
    def test_fix_repairs_a_real_defect_and_rechecks_clean(
        self, runner: CliRunner, valid_catalog_with_parquet: Path
    ) -> None:
        """The summary reflects the post-fix state, so an agent's loop terminates."""
        catalog_file = valid_catalog_with_parquet / "catalog.json"
        doc = json.loads(catalog_file.read_text())
        doc["stac_extensions"] = []
        catalog_file.write_text(json.dumps(doc))

        failing = runner.invoke(cli, ["check", str(valid_catalog_with_parquet), "--metadata"])
        assert failing.exit_code == 1
        assert "PTL-CNF-001" in failing.output

        fixed = runner.invoke(
            cli, ["check", str(valid_catalog_with_parquet), "--metadata", "--fix"]
        )
        assert fixed.exit_code == 0
        assert "PTL-CNF-001" not in fixed.output

    @pytest.mark.unit
    def test_metadata_fix_calls_fix_metadata_function(
        self, runner: CliRunner, valid_catalog_with_parquet: Path
    ) -> None:
        from portolan_cli.metadata.models import (
            MetadataCheckResult,
            MetadataReport,
            MetadataStatus,
        )

        metadata_report = MetadataReport(
            results=[
                MetadataCheckResult(
                    file_path=valid_catalog_with_parquet / "test.parquet",
                    status=MetadataStatus.MISSING,
                    message="No STAC item found",
                )
            ]
        )

        with (
            patch(
                "portolan_cli.metadata.scan.scan_catalog_metadata",
                return_value=metadata_report,
            ),
            patch("portolan_cli.metadata.fix_metadata") as mock_fix,
        ):
            from portolan_cli.metadata.fix import FixReport

            mock_fix.return_value = FixReport(results=[], fresh_skipped=0)
            result = runner.invoke(
                cli, ["check", str(valid_catalog_with_parquet), "--metadata", "--fix"]
            )

            mock_fix.assert_called_once()
            assert result.exit_code == 0

    @pytest.mark.unit
    def test_metadata_fix_with_dry_run(
        self, runner: CliRunner, valid_catalog_with_parquet: Path
    ) -> None:
        from portolan_cli.metadata.models import (
            MetadataCheckResult,
            MetadataReport,
            MetadataStatus,
        )

        metadata_report = MetadataReport(
            results=[
                MetadataCheckResult(
                    file_path=valid_catalog_with_parquet / "test.parquet",
                    status=MetadataStatus.MISSING,
                    message="No STAC item found",
                )
            ]
        )

        with (
            patch(
                "portolan_cli.metadata.scan.scan_catalog_metadata",
                return_value=metadata_report,
            ),
            patch("portolan_cli.metadata.fix_metadata") as mock_fix,
        ):
            from portolan_cli.metadata.fix import FixReport

            mock_fix.return_value = FixReport(results=[], fresh_skipped=0)
            result = runner.invoke(
                cli,
                ["check", str(valid_catalog_with_parquet), "--metadata", "--fix", "--dry-run"],
            )

            mock_fix.assert_called_once()
            assert mock_fix.call_args[1].get("dry_run") is True
            assert result.exit_code == 0

    @pytest.mark.unit
    def test_fix_with_both_scopes(
        self, runner: CliRunner, valid_catalog_with_parquet: Path
    ) -> None:
        """--fix alone runs both the metadata and geo-asset repairs."""
        from portolan_cli.metadata.models import MetadataReport
        from portolan_cli.scan.check import CheckReport

        with (
            patch("portolan_cli.scan.check.check_directory") as mock_check,
            patch(
                "portolan_cli.metadata.scan.scan_catalog_metadata",
                return_value=MetadataReport(results=[]),
            ),
            patch("portolan_cli.metadata.fix_metadata") as mock_fix,
        ):
            from portolan_cli.metadata.fix import FixReport

            mock_check.return_value = CheckReport(
                root=valid_catalog_with_parquet, files=[], conversion_report=None
            )
            mock_fix.return_value = FixReport(results=[], fresh_skipped=0)

            result = runner.invoke(cli, ["check", str(valid_catalog_with_parquet), "--fix"])

            assert result.exit_code == 0
            mock_fix.assert_called_once()
            mock_check.assert_called_once()


class TestCheckForceWorkers:
    """The --force and --workers flags (issue #530)."""

    @pytest.fixture
    def catalog(self, conformant_catalog: Path) -> Path:
        return conformant_catalog

    @pytest.mark.unit
    @pytest.mark.parametrize("flag", ["--force", "--workers"])
    def test_flag_is_advertised(self, runner: CliRunner, flag: str) -> None:
        result = runner.invoke(cli, ["check", "--help"])
        assert flag in result.output

    @pytest.mark.unit
    def test_force_and_workers_threaded_to_check_directory(
        self, runner: CliRunner, catalog: Path
    ) -> None:
        """--fix --force --workers N reaches check_directory with those values."""
        from portolan_cli.scan.check import CheckReport

        with patch("portolan_cli.scan.check.check_directory") as mock_check:
            mock_check.return_value = CheckReport(root=catalog, files=[], conversion_report=None)
            result = runner.invoke(
                cli,
                ["check", str(catalog), "--geo-assets", "--fix", "--force", "--workers", "3"],
            )

        assert result.exit_code == 0
        mock_check.assert_called_once()
        kwargs = mock_check.call_args.kwargs
        assert kwargs.get("force") is True
        assert kwargs.get("workers") == 3

    @pytest.mark.unit
    def test_workers_defaults_to_cpu_count_when_unset(
        self, runner: CliRunner, catalog: Path
    ) -> None:
        """Without --workers, the CLI resolves a concrete worker count (parallel by default)."""
        import os

        from portolan_cli.scan.check import CheckReport

        with patch("portolan_cli.scan.check.check_directory") as mock_check:
            mock_check.return_value = CheckReport(root=catalog, files=[], conversion_report=None)
            result = runner.invoke(cli, ["check", str(catalog), "--geo-assets", "--fix", "--force"])

        assert result.exit_code == 0
        assert mock_check.call_args.kwargs.get("workers") == (os.cpu_count() or 1)

    @pytest.mark.unit
    def test_force_without_fix_warns(self, runner: CliRunner, catalog: Path) -> None:
        """--force without --fix warns and does not enter the fix path."""
        result = runner.invoke(cli, ["check", str(catalog), "--force"])
        assert "--force requires --fix" in result.output

    @pytest.mark.unit
    def test_remove_legacy_without_fix_warns(self, runner: CliRunner, catalog: Path) -> None:
        result = runner.invoke(cli, ["check", str(catalog), "--remove-legacy"])
        assert "--remove-legacy requires --fix" in result.output

    @pytest.mark.unit
    def test_workers_rejects_zero(self, runner: CliRunner, catalog: Path) -> None:
        """--workers 0 is rejected by click.IntRange(min=1)."""
        result = runner.invoke(
            cli, ["check", str(catalog), "--geo-assets", "--fix", "--workers", "0"]
        )
        assert result.exit_code == 2


class TestCheckConfig:
    """The `check:` config block reaches rashid."""

    @pytest.mark.unit
    def test_disabled_rule_is_silenced(
        self, runner: CliRunner, build_conformant_catalog: Callable[..., Path], tmp_path: Path
    ) -> None:
        root = build_conformant_catalog(tmp_path / "cat")
        rule_id = _break_catalog(root)

        failing = runner.invoke(cli, ["check", str(root), "--metadata"])
        assert failing.exit_code == 1

        config = root / ".portolan" / "config.yaml"
        config.write_text(
            config.read_text() + f"check:\n  disabled:\n    - {rule_id}\n", encoding="utf-8"
        )
        result = runner.invoke(cli, ["check", str(root), "--metadata"])
        assert result.exit_code == 0
        assert rule_id not in result.output
