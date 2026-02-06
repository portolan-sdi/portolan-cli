"""Tests for validation rule base class and registry."""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.validation.results import Severity, ValidationResult
from portolan_cli.validation.rules import ValidationRule


class TestValidationRule:
    """Tests for ValidationRule base class."""

    @pytest.mark.unit
    def test_rule_has_name_attribute(self) -> None:
        """ValidationRule must have a name for identification."""

        class TestRule(ValidationRule):
            name = "test_rule"
            severity = Severity.ERROR
            description = "A test rule"

            def check(self, _catalog_path: Path) -> ValidationResult:
                return self._pass("OK")

        rule = TestRule()
        assert rule.name == "test_rule"

    @pytest.mark.unit
    def test_rule_has_severity_attribute(self) -> None:
        """ValidationRule must have a severity level."""

        class TestRule(ValidationRule):
            name = "test_rule"
            severity = Severity.WARNING
            description = "A test rule"

            def check(self, _catalog_path: Path) -> ValidationResult:
                return self._pass("OK")

        rule = TestRule()
        assert rule.severity == Severity.WARNING

    @pytest.mark.unit
    def test_rule_has_description(self) -> None:
        """ValidationRule must have a description for --verbose output."""

        class TestRule(ValidationRule):
            name = "test_rule"
            severity = Severity.INFO
            description = "Checks something important"

            def check(self, _catalog_path: Path) -> ValidationResult:
                return self._pass("OK")

        rule = TestRule()
        assert rule.description == "Checks something important"

    @pytest.mark.unit
    def test_rule_check_returns_validation_result(self, tmp_path: Path) -> None:
        """check() must return a ValidationResult."""

        class TestRule(ValidationRule):
            name = "test_rule"
            severity = Severity.ERROR
            description = "Test"

            def check(self, _catalog_path: Path) -> ValidationResult:
                return self._pass("Passed")

        rule = TestRule()
        result = rule.check(tmp_path)
        assert isinstance(result, ValidationResult)
        assert result.passed is True

    @pytest.mark.unit
    def test_rule_pass_helper_creates_passing_result(self, tmp_path: Path) -> None:
        """_pass() helper creates a passing ValidationResult."""

        class TestRule(ValidationRule):
            name = "my_rule"
            severity = Severity.ERROR
            description = "Test"

            def check(self, _catalog_path: Path) -> ValidationResult:
                return self._pass("Everything OK")

        rule = TestRule()
        result = rule.check(tmp_path)
        assert result.passed is True
        assert result.rule_name == "my_rule"
        assert result.severity == Severity.ERROR
        assert result.message == "Everything OK"

    @pytest.mark.unit
    def test_rule_fail_helper_creates_failing_result(self, tmp_path: Path) -> None:
        """_fail() helper creates a failing ValidationResult."""

        class TestRule(ValidationRule):
            name = "my_rule"
            severity = Severity.ERROR
            description = "Test"

            def check(self, _catalog_path: Path) -> ValidationResult:
                return self._fail("Something wrong")

        rule = TestRule()
        result = rule.check(tmp_path)
        assert result.passed is False
        assert result.rule_name == "my_rule"
        assert result.message == "Something wrong"

    @pytest.mark.unit
    def test_rule_fail_helper_accepts_fix_hint(self, tmp_path: Path) -> None:
        """_fail() helper can include a fix hint."""

        class TestRule(ValidationRule):
            name = "my_rule"
            severity = Severity.ERROR
            description = "Test"

            def check(self, _catalog_path: Path) -> ValidationResult:
                return self._fail("Missing X", fix_hint="Add X to catalog.json")

        rule = TestRule()
        result = rule.check(tmp_path)
        assert result.fix_hint == "Add X to catalog.json"

    @pytest.mark.unit
    def test_rule_is_abstract(self) -> None:
        """ValidationRule.check() must be implemented by subclasses."""
        with pytest.raises(TypeError, match="abstract"):
            ValidationRule()  # type: ignore[abstract]
