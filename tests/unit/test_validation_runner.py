"""Tests for validation runner."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.validation import ValidationReport, check


class TestCheck:
    """Tests for check() function."""

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
    def test_check_returns_validation_report(self, valid_catalog: Path) -> None:
        """check() returns a ValidationReport."""
        report = check(valid_catalog)
        assert isinstance(report, ValidationReport)

    @pytest.mark.unit
    def test_check_passes_for_valid_catalog(self, valid_catalog: Path) -> None:
        """check() passes for a valid catalog."""
        report = check(valid_catalog)
        assert report.passed is True

    @pytest.mark.unit
    def test_check_fails_when_no_portolan_dir(self, tmp_path: Path) -> None:
        """check() fails when .portolan doesn't exist."""
        report = check(tmp_path)
        assert report.passed is False
        assert len(report.errors) > 0

    @pytest.mark.unit
    def test_check_fails_when_catalog_json_missing(self, tmp_path: Path) -> None:
        """check() fails when catalog.json is missing."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()

        report = check(tmp_path)
        assert report.passed is False

    @pytest.mark.unit
    def test_check_fails_when_stac_fields_missing(self, tmp_path: Path) -> None:
        """check() fails when required STAC fields are missing."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        catalog_file = portolan_dir / "catalog.json"
        catalog_file.write_text('{"type": "Catalog"}')

        report = check(tmp_path)
        assert report.passed is False

    @pytest.mark.unit
    def test_check_runs_all_rules(self, valid_catalog: Path) -> None:
        """check() runs all registered rules."""
        report = check(valid_catalog)
        # Should have results from at least 3 rules
        assert len(report.results) >= 3

    @pytest.mark.unit
    def test_check_continues_after_early_failure(self, tmp_path: Path) -> None:
        """check() continues running rules even after early failures."""
        # No .portolan dir = first rule fails
        report = check(tmp_path)

        # Should have run all 3 default rules even though the first one failed
        # This verifies the runner doesn't short-circuit on failure
        assert len(report.results) == 3
