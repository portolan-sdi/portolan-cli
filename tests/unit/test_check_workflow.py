"""Tests for the check/fix workflow orchestration in check.py (issue #620).

These exercise the Click-free workflow layer extracted from cli.py:
- resolve_catalog_root_for_check: walk up to catalog.json
- build_check_rules: rule construction honoring config + strict
- run_fix_workflow: metadata/geo-asset fix orchestration returning reports
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest


def _init_catalog(path: Path) -> None:
    """Create a minimal managed catalog (sentinel + catalog.json)."""
    portolan_dir = path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("{}")
    (path / "catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "c",
                "description": "d",
                "links": [],
            }
        )
    )


class TestResolveCatalogRootForCheck:
    """Tests for resolve_catalog_root_for_check."""

    @pytest.mark.unit
    def test_finds_catalog_json_in_parent(self, tmp_path: Path) -> None:
        from portolan_cli.check import resolve_catalog_root_for_check

        _init_catalog(tmp_path)
        subdir = tmp_path / "collection" / "nested"
        subdir.mkdir(parents=True)

        assert resolve_catalog_root_for_check(subdir) == tmp_path.resolve()

    @pytest.mark.unit
    def test_returns_none_without_catalog_json(self, tmp_path: Path) -> None:
        from portolan_cli.check import resolve_catalog_root_for_check

        # No catalog.json anywhere under a private temp dir.
        loose = tmp_path / "loose"
        loose.mkdir()

        assert resolve_catalog_root_for_check(loose) is None


class TestBuildCheckRules:
    """Tests for build_check_rules (extraction parity, #620)."""

    @pytest.mark.unit
    def test_matches_underlying_build_rules(self, tmp_path: Path) -> None:
        """Without config, build_check_rules matches validation.runner._build_rules."""
        from portolan_cli.check import build_check_rules
        from portolan_cli.validation.runner import _build_rules

        _init_catalog(tmp_path)
        got = build_check_rules(tmp_path, strict=False)
        expected = _build_rules(strict=False, config=None)

        assert [type(r).__name__ for r in got] == [type(r).__name__ for r in expected]

    @pytest.mark.unit
    def test_returns_rules_without_portolan_dir(self, tmp_path: Path) -> None:
        """A path with no .portolan/config.yaml still yields the default rules."""
        from portolan_cli.check import build_check_rules
        from portolan_cli.validation.runner import _build_rules

        got = build_check_rules(tmp_path, strict=True)
        expected = _build_rules(strict=True, config=None)

        assert [type(r).__name__ for r in got] == [type(r).__name__ for r in expected]


class TestRunFixWorkflow:
    """Tests for run_fix_workflow returning a FixWorkflowOutcome (#620)."""

    @pytest.mark.unit
    def test_metadata_only_without_catalog_sets_fatal_error(self, tmp_path: Path) -> None:
        """Metadata-only fix with no catalog.json returns a fatal_error message."""
        from portolan_cli.check import run_fix_workflow

        loose = tmp_path / "loose"
        loose.mkdir()

        outcome = run_fix_workflow(
            path=loose,
            run_metadata=True,
            run_geo_assets=False,
            dry_run=False,
            remove_legacy=False,
        )

        assert outcome.fatal_error is not None
        assert "not a portolan catalog" in outcome.fatal_error
        assert outcome.metadata_fix_report is None

    @pytest.mark.unit
    def test_mixed_mode_without_catalog_skips_metadata(self, tmp_path: Path) -> None:
        """Mixed mode with no catalog skips metadata (no fatal) and still runs geo fix."""
        from portolan_cli.check import CheckReport, run_fix_workflow

        loose = tmp_path / "loose"
        loose.mkdir()

        sentinel = CheckReport(root=loose, files=[], conversion_report=None)
        with patch("portolan_cli.check.check_directory", return_value=sentinel) as mock_check:
            outcome = run_fix_workflow(
                path=loose,
                run_metadata=True,
                run_geo_assets=True,
                dry_run=False,
                remove_legacy=False,
            )

        assert outcome.fatal_error is None
        assert outcome.metadata_fix_report is None
        assert outcome.format_fix_report is sentinel
        mock_check.assert_called_once()

    @pytest.mark.unit
    def test_geo_only_threads_force_and_workers(self, tmp_path: Path) -> None:
        """Geo-only fix forwards force/workers to check_directory."""
        from portolan_cli.check import CheckReport, run_fix_workflow

        _init_catalog(tmp_path)
        sentinel = CheckReport(root=tmp_path, files=[], conversion_report=None)
        with patch("portolan_cli.check.check_directory", return_value=sentinel) as mock_check:
            outcome = run_fix_workflow(
                path=tmp_path,
                run_metadata=False,
                run_geo_assets=True,
                dry_run=False,
                remove_legacy=False,
                force=True,
                workers=3,
            )

        assert outcome.format_fix_report is sentinel
        kwargs = mock_check.call_args.kwargs
        assert kwargs.get("force") is True
        assert kwargs.get("workers") == 3

    @pytest.mark.unit
    def test_metadata_fix_reports_failures(self, tmp_path: Path) -> None:
        """A metadata fix with failures sets has_failures on the outcome."""
        from portolan_cli.check import run_fix_workflow
        from portolan_cli.metadata.fix import FixAction, FixReport, FixResult
        from portolan_cli.metadata.models import (
            MetadataCheckResult,
            MetadataReport,
            MetadataStatus,
        )

        _init_catalog(tmp_path)

        check_report = MetadataReport(
            results=[
                MetadataCheckResult(
                    file_path=tmp_path / "x.parquet",
                    status=MetadataStatus.MISSING,
                    message="missing",
                )
            ]
        )
        fix_report = FixReport(
            results=[
                FixResult(
                    file_path=tmp_path / "x.parquet",
                    action=FixAction.CREATED,
                    success=False,
                    message="failed",
                )
            ]
        )

        with (
            patch(
                "portolan_cli.metadata.scan.scan_catalog_metadata",
                return_value=check_report,
            ),
            patch("portolan_cli.metadata.fix_metadata", return_value=fix_report),
        ):
            outcome = run_fix_workflow(
                path=tmp_path,
                run_metadata=True,
                run_geo_assets=False,
                dry_run=False,
                remove_legacy=False,
            )

        assert outcome.has_failures is True
        assert outcome.metadata_fix_report is fix_report
