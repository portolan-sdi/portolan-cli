"""Tests for validation result data structures."""

from __future__ import annotations

import pytest

from portolan_cli.validation.results import (
    Severity,
    ValidationReport,
    ValidationResult,
)


class TestSeverity:
    """Tests for Severity enum."""

    @pytest.mark.unit
    def test_severity_has_error_level(self) -> None:
        """Severity must have ERROR level for blocking issues."""
        assert Severity.ERROR.value == "error"

    @pytest.mark.unit
    def test_severity_has_warning_level(self) -> None:
        """Severity must have WARNING level for non-blocking issues."""
        assert Severity.WARNING.value == "warning"

    @pytest.mark.unit
    def test_severity_has_info_level(self) -> None:
        """Severity must have INFO level for suggestions."""
        assert Severity.INFO.value == "info"


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    @pytest.mark.unit
    def test_result_stores_rule_name(self) -> None:
        """ValidationResult must store which rule produced it."""
        result = ValidationResult(
            rule_name="test_rule",
            passed=True,
            severity=Severity.ERROR,
            message="All good",
        )
        assert result.rule_name == "test_rule"

    @pytest.mark.unit
    def test_result_stores_pass_status(self) -> None:
        """ValidationResult must indicate pass/fail."""
        result = ValidationResult(
            rule_name="test_rule",
            passed=False,
            severity=Severity.ERROR,
            message="Failed",
        )
        assert result.passed is False

    @pytest.mark.unit
    def test_result_stores_severity(self) -> None:
        """ValidationResult must store severity level."""
        result = ValidationResult(
            rule_name="test_rule",
            passed=False,
            severity=Severity.WARNING,
            message="Warning",
        )
        assert result.severity == Severity.WARNING

    @pytest.mark.unit
    def test_result_stores_message(self) -> None:
        """ValidationResult must store human-readable message."""
        result = ValidationResult(
            rule_name="test_rule",
            passed=True,
            severity=Severity.INFO,
            message="Looks good",
        )
        assert result.message == "Looks good"

    @pytest.mark.unit
    def test_result_stores_optional_fix_hint(self) -> None:
        """ValidationResult can store optional fix hint."""
        result = ValidationResult(
            rule_name="test_rule",
            passed=False,
            severity=Severity.ERROR,
            message="Missing field",
            fix_hint="Run 'portolan check --fix' to add defaults",
        )
        assert result.fix_hint == "Run 'portolan check --fix' to add defaults"

    @pytest.mark.unit
    def test_result_fix_hint_defaults_to_none(self) -> None:
        """ValidationResult.fix_hint defaults to None."""
        result = ValidationResult(
            rule_name="test_rule",
            passed=True,
            severity=Severity.INFO,
            message="OK",
        )
        assert result.fix_hint is None


class TestValidationReport:
    """Tests for ValidationReport aggregate."""

    @pytest.mark.unit
    def test_report_stores_results(self) -> None:
        """ValidationReport must store list of results."""
        results = [
            ValidationResult("rule1", True, Severity.INFO, "OK"),
            ValidationResult("rule2", False, Severity.ERROR, "Failed"),
        ]
        report = ValidationReport(results=results)
        assert len(report.results) == 2

    @pytest.mark.unit
    def test_report_passed_true_when_all_pass(self) -> None:
        """ValidationReport.passed is True when all results pass."""
        results = [
            ValidationResult("rule1", True, Severity.INFO, "OK"),
            ValidationResult("rule2", True, Severity.WARNING, "OK"),
        ]
        report = ValidationReport(results=results)
        assert report.passed is True

    @pytest.mark.unit
    def test_report_passed_false_when_any_error_fails(self) -> None:
        """ValidationReport.passed is False when any ERROR severity fails."""
        results = [
            ValidationResult("rule1", True, Severity.INFO, "OK"),
            ValidationResult("rule2", False, Severity.ERROR, "Failed"),
        ]
        report = ValidationReport(results=results)
        assert report.passed is False

    @pytest.mark.unit
    def test_report_passed_true_when_only_warnings_fail(self) -> None:
        """ValidationReport.passed is True when only WARNINGs fail (non-blocking)."""
        results = [
            ValidationResult("rule1", True, Severity.INFO, "OK"),
            ValidationResult("rule2", False, Severity.WARNING, "Warn"),
        ]
        report = ValidationReport(results=results)
        assert report.passed is True

    @pytest.mark.unit
    def test_report_errors_property(self) -> None:
        """ValidationReport.errors returns only failed ERROR results."""
        results = [
            ValidationResult("rule1", True, Severity.ERROR, "OK"),
            ValidationResult("rule2", False, Severity.ERROR, "Failed"),
            ValidationResult("rule3", False, Severity.WARNING, "Warn"),
        ]
        report = ValidationReport(results=results)
        errors = report.errors
        assert len(errors) == 1
        assert errors[0].rule_name == "rule2"

    @pytest.mark.unit
    def test_report_warnings_property(self) -> None:
        """ValidationReport.warnings returns only failed WARNING results."""
        results = [
            ValidationResult("rule1", False, Severity.ERROR, "Error"),
            ValidationResult("rule2", False, Severity.WARNING, "Warn1"),
            ValidationResult("rule3", False, Severity.WARNING, "Warn2"),
        ]
        report = ValidationReport(results=results)
        warnings = report.warnings
        assert len(warnings) == 2

    @pytest.mark.unit
    def test_report_to_dict_for_json_output(self) -> None:
        """ValidationReport.to_dict() returns JSON-serializable dict."""
        results = [
            ValidationResult("rule1", True, Severity.INFO, "OK"),
        ]
        report = ValidationReport(results=results)
        d = report.to_dict()
        assert d["passed"] is True
        assert len(d["results"]) == 1
        assert d["results"][0]["rule_name"] == "rule1"

    @pytest.mark.unit
    def test_empty_report_passes(self) -> None:
        """Empty ValidationReport passes (no rules = no failures)."""
        report = ValidationReport(results=[])
        assert report.passed is True
