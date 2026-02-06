"""Tests for validation rule base class and registry."""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.validation.results import Severity, ValidationResult
from portolan_cli.validation.rules import CatalogExistsRule, ValidationRule


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


class TestCatalogExistsRule:
    """Tests for CatalogExistsRule."""

    @pytest.mark.unit
    def test_passes_when_portolan_dir_exists(self, tmp_path: Path) -> None:
        """Rule passes when .portolan directory exists."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()

        rule = CatalogExistsRule()
        result = rule.check(tmp_path)

        assert result.passed is True
        assert "exists" in result.message.lower()

    @pytest.mark.unit
    def test_fails_when_portolan_dir_missing(self, tmp_path: Path) -> None:
        """Rule fails when .portolan directory is missing."""
        rule = CatalogExistsRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert ".portolan" in result.message

    @pytest.mark.unit
    def test_has_error_severity(self) -> None:
        """Missing catalog is an ERROR (blocking)."""
        rule = CatalogExistsRule()
        assert rule.severity == Severity.ERROR

    @pytest.mark.unit
    def test_provides_fix_hint_on_failure(self, tmp_path: Path) -> None:
        """Failure includes hint to run 'portolan init'."""
        rule = CatalogExistsRule()
        result = rule.check(tmp_path)

        assert result.fix_hint is not None
        assert "init" in result.fix_hint.lower()

    @pytest.mark.unit
    def test_fails_when_portolan_is_file_not_dir(self, tmp_path: Path) -> None:
        """Rule fails when .portolan exists but is a file, not directory."""
        portolan_file = tmp_path / ".portolan"
        portolan_file.write_text("not a directory")

        rule = CatalogExistsRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert "directory" in result.message.lower()
