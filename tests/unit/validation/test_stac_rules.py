"""Tests for STAC schema and lint validation rules.

TDD: These tests are written before the implementation.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.validation.results import Severity

FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "validation" / "stac"


@pytest.fixture
def valid_catalog() -> Path:
    """Path to valid STAC catalog fixture."""
    return FIXTURES_DIR / "valid"


@pytest.fixture
def catalog_missing_id() -> Path:
    """Path to catalog missing required 'id' field."""
    return FIXTURES_DIR / "missing-id"


@pytest.fixture
def catalog_bad_id() -> Path:
    """Path to catalog with special characters in ID (fails percent_encoded check)."""
    return FIXTURES_DIR / "bad-id"


@pytest.fixture
def catalog_invalid_json() -> Path:
    """Path to catalog with invalid JSON syntax."""
    return FIXTURES_DIR / "invalid-json"


@pytest.fixture
def empty_dir(tmp_path: Path) -> Path:
    """Empty directory with no catalog.json."""
    return tmp_path


@pytest.fixture
def minimal_catalog(tmp_path: Path) -> Path:
    """Create minimal valid catalog in tmp_path."""
    catalog = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "tmp-catalog",
        "description": "Temporary test catalog",
        "links": [],
    }
    (tmp_path / "catalog.json").write_text(json.dumps(catalog))
    return tmp_path


@pytest.mark.unit
class TestStacSchemaRule:
    """Tests for StacSchemaRule."""

    def test_valid_catalog_passes(self, valid_catalog: Path) -> None:
        """Valid STAC catalog passes schema validation."""
        from portolan_cli.validation.stac_rules import StacSchemaRule

        rule = StacSchemaRule()
        result = rule.check(valid_catalog)
        assert result.passed
        assert result.severity == Severity.ERROR
        assert result.rule_name == "stac_schema"

    def test_missing_required_field_fails(self, catalog_missing_id: Path) -> None:
        """Catalog missing required 'id' field fails schema validation."""
        from portolan_cli.validation.stac_rules import StacSchemaRule

        rule = StacSchemaRule()
        result = rule.check(catalog_missing_id)
        assert not result.passed
        assert result.severity == Severity.ERROR

    def test_invalid_json_fails(self, catalog_invalid_json: Path) -> None:
        """Malformed JSON fails with helpful error."""
        from portolan_cli.validation.stac_rules import StacSchemaRule

        rule = StacSchemaRule()
        result = rule.check(catalog_invalid_json)
        assert not result.passed

    def test_no_catalog_passes(self, empty_dir: Path) -> None:
        """Missing catalog.json is a pass (nothing to validate)."""
        from portolan_cli.validation.stac_rules import StacSchemaRule

        rule = StacSchemaRule()
        result = rule.check(empty_dir)
        assert result.passed
        assert "No catalog.json" in result.message

    def test_strict_mode_available(self, minimal_catalog: Path) -> None:
        """Strict mode can be enabled via constructor."""
        from portolan_cli.validation.stac_rules import StacSchemaRule

        rule = StacSchemaRule(strict=True)
        assert rule.strict is True
        result = rule.check(minimal_catalog)
        assert result.passed

    def test_default_is_non_strict(self) -> None:
        """Default mode is non-strict (fast validation)."""
        from portolan_cli.validation.stac_rules import StacSchemaRule

        rule = StacSchemaRule()
        assert rule.strict is False


@pytest.mark.unit
class TestStacLintRule:
    """Tests for StacLintRule."""

    def test_valid_catalog_no_errors(self, valid_catalog: Path) -> None:
        """Valid catalog has no ERROR-level lint violations."""
        from portolan_cli.validation.stac_rules import StacLintRule

        rule = StacLintRule()
        result = rule.check(valid_catalog)
        # May have warnings but should not have errors that fail validation
        if not result.passed:
            assert result.severity != Severity.ERROR

    def test_rule_name_and_severity(self) -> None:
        """Rule has correct name and default severity."""
        from portolan_cli.validation.stac_rules import StacLintRule

        rule = StacLintRule()
        assert rule.name == "stac_lint"
        assert rule.severity == Severity.WARNING

    def test_bad_id_detected(self, catalog_bad_id: Path) -> None:
        """Special characters in ID are flagged by percent_encoded check."""
        from portolan_cli.validation.stac_rules import StacLintRule

        rule = StacLintRule()
        result = rule.check(catalog_bad_id)
        # percent_encoded is ERROR by default, so this should fail
        assert not result.passed
        assert "percent_encoded" in result.message.lower() or ":" in result.message

    def test_no_catalog_passes(self, empty_dir: Path) -> None:
        """Missing catalog.json is a pass."""
        from portolan_cli.validation.stac_rules import StacLintRule

        rule = StacLintRule()
        result = rule.check(empty_dir)
        assert result.passed

    def test_severity_configurable_via_config(self) -> None:
        """Severity can be overridden via config dict."""
        from portolan_cli.validation.stac_rules import StacLintRule

        config = {
            "stac_lint": {
                "severity": {
                    "check_thumbnail": "error",
                }
            }
        }
        rule = StacLintRule(config=config)
        severity_map = rule._get_severity_map()
        assert severity_map["check_thumbnail"] == Severity.ERROR

    def test_check_can_be_skipped_via_config(self) -> None:
        """Checks can be disabled by setting severity to 'skip'."""
        from portolan_cli.validation.stac_rules import StacLintRule

        config = {
            "stac_lint": {
                "severity": {
                    "bloated_metadata": "skip",
                }
            }
        }
        rule = StacLintRule(config=config)
        rule._get_severity_map()  # This processes skip directives
        # Skipped checks go into _runtime_skip_checks (config-driven)
        # while SKIP_CHECKS is the class-level default
        assert "bloated_metadata" in rule._runtime_skip_checks

    def test_datetime_null_skipped_by_default(self) -> None:
        """datetime_null check is skipped (handled by ProvisionalDatetimeRule)."""
        from portolan_cli.validation.stac_rules import StacLintRule

        rule = StacLintRule()
        assert "datetime_null" in rule.SKIP_CHECKS

    def test_strict_mode_available(self, minimal_catalog: Path) -> None:
        """Strict mode can be enabled via constructor."""
        from portolan_cli.validation.stac_rules import StacLintRule

        rule = StacLintRule(strict=True)
        assert rule.strict is True
        result = rule.check(minimal_catalog)
        # Should complete without error
        assert result is not None


@pytest.mark.unit
class TestStacRulesRunner:
    """Tests for STAC rules integration with runner."""

    def test_build_rules_includes_stac_rules(self) -> None:
        """_build_rules includes both STAC rules."""
        from portolan_cli.validation.runner import _build_rules

        rules = _build_rules()
        rule_names = [r.name for r in rules]
        assert "stac_schema" in rule_names
        assert "stac_lint" in rule_names

    def test_build_rules_passes_strict_flag(self) -> None:
        """_build_rules passes strict flag to STAC rules."""
        from portolan_cli.validation.runner import _build_rules

        rules = _build_rules(strict=True)
        stac_rules = [r for r in rules if r.name in ("stac_schema", "stac_lint")]
        for rule in stac_rules:
            assert rule.strict is True

    def test_build_rules_passes_config(self) -> None:
        """_build_rules passes config to StacLintRule."""
        from portolan_cli.validation.runner import _build_rules

        config = {"stac_lint": {"severity": {"check_thumbnail": "error"}}}
        rules = _build_rules(config=config)
        lint_rule = next(r for r in rules if r.name == "stac_lint")
        assert lint_rule.config == config

    def test_check_function_accepts_strict_param(self, minimal_catalog: Path) -> None:
        """check() function accepts strict parameter."""
        from portolan_cli.validation.runner import check

        report = check(minimal_catalog, strict=True)
        assert report is not None

    def test_check_function_accepts_config_param(self, minimal_catalog: Path) -> None:
        """check() function accepts config parameter."""
        from portolan_cli.validation.runner import check

        config = {"stac_lint": {"severity": {}}}
        report = check(minimal_catalog, config=config)
        assert report is not None

    def test_default_rules_still_works(self, minimal_catalog: Path) -> None:
        """DEFAULT_RULES tuple still works for backward compatibility."""
        from portolan_cli.validation.runner import DEFAULT_RULES, check

        report = check(minimal_catalog, rules=DEFAULT_RULES)
        assert report is not None
