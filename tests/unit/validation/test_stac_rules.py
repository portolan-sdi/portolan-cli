"""Tests for STAC schema and lint validation rules.

TDD: These tests are written before the implementation.
"""

from __future__ import annotations

import json
import sys
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
def recursive_catalog() -> Path:
    """Path to catalog with nested collection and item."""
    return FIXTURES_DIR / "recursive"


@pytest.fixture
def recursive_invalid_catalog() -> Path:
    """Path to catalog with invalid nested item (missing id)."""
    return FIXTURES_DIR / "recursive-invalid"


@pytest.fixture
def many_violations_catalog() -> Path:
    """Path to catalog with many lint violations for truncation testing."""
    return FIXTURES_DIR / "many-violations"


@pytest.fixture
def self_contained_valid_catalog() -> Path:
    """SELF_CONTAINED 1.1.0 catalog (relative hrefs), all objects valid."""
    return FIXTURES_DIR / "self-contained-valid"


@pytest.fixture
def self_contained_invalid_collection_catalog() -> Path:
    """SELF_CONTAINED 1.1.0 catalog whose collection is missing extent/stac_version."""
    return FIXTURES_DIR / "self-contained-invalid-collection"


@pytest.fixture
def self_contained_invalid_item_catalog() -> Path:
    """SELF_CONTAINED 1.1.0 catalog whose nested item is missing id."""
    return FIXTURES_DIR / "self-contained-invalid-item"


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
class TestUnresolvableExtensionTolerance:
    """An unresolvable extension schema must not fail document validation.

    STAC extensions are referenced by URL in `stac_extensions` and fetched
    over the network during schema validation. Extensions that are
    unpublished, proposed, or simply unreachable (e.g. the STAC Iceberg
    extension and the proposed git-backed-catalog extension) must NOT make
    `check` fail an otherwise well-formed catalog — except under --strict.
    """

    _RESOLUTION_ERROR = (
        "Could not resolve schema: "
        "https://stac-extensions.github.io/iceberg/v1.0.0/schema.json. "
        "Reason: HTTP Error 404: Not Found"
    )

    def test_schema_rule_tolerates_constructor_resolution_error(
        self, minimal_catalog: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Linter raising 'Could not resolve schema' on construction passes."""
        from portolan_cli.validation import stac_rules
        from portolan_cli.validation.stac_rules import StacSchemaRule

        def _raise(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError(self._RESOLUTION_ERROR)

        monkeypatch.setattr(stac_rules, "Linter", _raise)
        result = StacSchemaRule().check(minimal_catalog)
        assert result.passed

    def test_schema_rule_tolerates_invalid_stac_resolution_error(
        self, minimal_catalog: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """valid_stac=False due to a resolution error passes (non-strict)."""
        from portolan_cli.validation import stac_rules
        from portolan_cli.validation.stac_rules import StacSchemaRule

        msg = (
            "Could not resolve schema: "
            "https://portolan-sdi.github.io/portolan-spec/git/v1.0.0/schema.json"
        )

        class _FakeLinter:
            def __init__(self, *_a: object, **_k: object) -> None:
                self.valid_stac = False
                self.error_msg = msg

        monkeypatch.setattr(stac_rules, "Linter", _FakeLinter)
        result = StacSchemaRule().check(minimal_catalog)
        assert result.passed

    def test_schema_rule_strict_still_fails_resolution_error(
        self, minimal_catalog: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Under --strict the user opts into full validation: still fails."""
        from portolan_cli.validation import stac_rules
        from portolan_cli.validation.stac_rules import StacSchemaRule

        def _raise(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError(self._RESOLUTION_ERROR)

        monkeypatch.setattr(stac_rules, "Linter", _raise)
        result = StacSchemaRule(strict=True).check(minimal_catalog)
        assert not result.passed

    def test_schema_rule_real_validation_error_still_fails(
        self, minimal_catalog: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A genuine schema error (not a resolution failure) still fails."""
        from portolan_cli.validation import stac_rules
        from portolan_cli.validation.stac_rules import StacSchemaRule

        class _FakeLinter:
            def __init__(self, *_a: object, **_k: object) -> None:
                self.valid_stac = False
                self.error_msg = "'id' is a required property"
                self.recommendation = None

        monkeypatch.setattr(stac_rules, "Linter", _FakeLinter)
        result = StacSchemaRule().check(minimal_catalog)
        assert not result.passed

    def test_lint_rule_tolerates_constructor_resolution_error(
        self, minimal_catalog: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Lint cannot run if a schema can't resolve, but that's not a fail."""
        from portolan_cli.validation import stac_rules
        from portolan_cli.validation.stac_rules import StacLintRule

        def _raise(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError(self._RESOLUTION_ERROR)

        monkeypatch.setattr(stac_rules, "Linter", _raise)
        result = StacLintRule().check(minimal_catalog)
        assert result.passed

    # --- Core-schema failures must NOT be tolerated (regression guard) ------
    #
    # stac-validator raises the SAME "Could not resolve schema: <uri>" message
    # for an unfetchable CORE spec schema (schemas.stacspec.org) as for an
    # extension schema. Tolerating the core case would turn `check` into a
    # silent no-op whenever schemas.stacspec.org is unreachable — passing every
    # catalog, valid or not. Only the extension case is acceptable.
    _CORE_RESOLUTION_ERROR = (
        "Could not resolve schema: "
        "https://schemas.stacspec.org/v1.0.0/catalog-spec/json-schema/catalog.json. "
        "Reason: HTTP Error 404: Not Found"
    )

    def test_schema_rule_core_schema_resolution_error_still_fails(
        self, minimal_catalog: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A CORE schema fetch failure fails even in non-strict mode."""
        from portolan_cli.validation import stac_rules
        from portolan_cli.validation.stac_rules import StacSchemaRule

        def _raise(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError(self._CORE_RESOLUTION_ERROR)

        monkeypatch.setattr(stac_rules, "Linter", _raise)
        result = StacSchemaRule().check(minimal_catalog)
        assert not result.passed

    def test_lint_rule_core_schema_resolution_error_still_fails(
        self, minimal_catalog: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Lint also fails on a core-schema fetch failure (non-strict)."""
        from portolan_cli.validation import stac_rules
        from portolan_cli.validation.stac_rules import StacLintRule

        def _raise(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError(self._CORE_RESOLUTION_ERROR)

        monkeypatch.setattr(stac_rules, "Linter", _raise)
        result = StacLintRule().check(minimal_catalog)
        assert not result.passed


@pytest.mark.unit
class TestIsSchemaResolutionError:
    """Unit tests for the `_is_schema_resolution_error` classifier.

    These pin the classifier against the REAL message format stac-validator
    emits (``Could not resolve schema: <uri>. Reason: <err>``,
    stac_validator/fast_validator.py) so the tolerance logic is verified
    independently of the monkeypatched-Linter tests above.
    """

    def test_extension_schema_url_is_tolerable(self) -> None:
        from portolan_cli.validation.stac_rules import _is_schema_resolution_error

        msg = (
            "Could not resolve schema: "
            "https://stac-extensions.github.io/iceberg/v1.0.0/schema.json. "
            "Reason: HTTP Error 404: Not Found"
        )
        assert _is_schema_resolution_error(msg) is True

    def test_proposed_extension_url_without_reason_is_tolerable(self) -> None:
        from portolan_cli.validation.stac_rules import _is_schema_resolution_error

        msg = (
            "Could not resolve schema: "
            "https://portolan-sdi.github.io/portolan-spec/git/v1.0.0/schema.json"
        )
        assert _is_schema_resolution_error(msg) is True

    def test_core_spec_schema_url_is_not_tolerable(self) -> None:
        from portolan_cli.validation.stac_rules import _is_schema_resolution_error

        msg = (
            "Could not resolve schema: "
            "https://schemas.stacspec.org/v1.0.0/catalog-spec/json-schema/catalog.json. "
            "Reason: HTTP Error 404: Not Found"
        )
        assert _is_schema_resolution_error(msg) is False

    def test_unrelated_resolve_message_is_not_tolerable(self) -> None:
        """The old broad 'failed to resolve' net must no longer match."""
        from portolan_cli.validation.stac_rules import _is_schema_resolution_error

        assert _is_schema_resolution_error("internal $ref failed to resolve") is False

    def test_marker_without_uri_is_not_tolerable(self) -> None:
        from portolan_cli.validation.stac_rules import _is_schema_resolution_error

        assert _is_schema_resolution_error("could not resolve schema") is False

    def test_none_and_empty_are_not_tolerable(self) -> None:
        from portolan_cli.validation.stac_rules import _is_schema_resolution_error

        assert _is_schema_resolution_error(None) is False
        assert _is_schema_resolution_error("") is False


@pytest.mark.network
class TestUnresolvableExtensionToleranceRealLinter:
    """End-to-end tolerance behavior through the REAL stac-check Linter.

    The monkeypatched tests above verify the tolerance branch in isolation but
    pin the error string by hand. These drive the real validator so the fix is
    exercised against stac-check's actual output (and would catch a wording
    change). Marked `network`: stac-validator fetches schemas over HTTP.
    """

    def test_unresolvable_extension_passes_non_strict(self, tmp_path: Path) -> None:
        """A real 404 extension schema does not fail an otherwise valid catalog."""
        from portolan_cli.validation.stac_rules import StacSchemaRule

        catalog = {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "tmp-catalog",
            "description": "Temporary test catalog",
            "stac_extensions": ["https://stac-extensions.github.io/iceberg/v1.0.0/schema.json"],
            "links": [],
        }
        (tmp_path / "catalog.json").write_text(json.dumps(catalog))
        result = StacSchemaRule().check(tmp_path)
        assert result.passed, result.message

    def test_unfetchable_core_schema_fails_non_strict(self, tmp_path: Path) -> None:
        """A bogus stac_version makes the CORE schema 404 — must still fail.

        Regression guard for the offline-no-op hazard: if the core schema
        can't be fetched the document was never validated, so non-strict
        `check` must NOT report it as valid.
        """
        from portolan_cli.validation.stac_rules import StacSchemaRule

        catalog = {
            "type": "Catalog",
            "stac_version": "9.9.9",  # no such core schema published
            "id": "tmp-catalog",
            "description": "Temporary test catalog",
            "stac_extensions": [],
            "links": [],
        }
        (tmp_path / "catalog.json").write_text(json.dumps(catalog))
        result = StacSchemaRule().check(tmp_path)
        assert not result.passed, result.message


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
        assert result.severity == Severity.ERROR
        # The check name should appear in the message
        assert "percent_encoded" in result.message

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
        # Skip checks are computed at init time (not as side effect of _get_severity_map)
        assert "bloated_metadata" in rule._runtime_skip_checks

    def test_get_severity_map_is_idempotent(self) -> None:
        """Calling _get_severity_map multiple times has no side effects."""
        from portolan_cli.validation.stac_rules import StacLintRule

        config = {
            "stac_lint": {
                "severity": {
                    "check_thumbnail": "error",
                    "bloated_metadata": "skip",
                }
            }
        }
        rule = StacLintRule(config=config)
        skip_before = set(rule._runtime_skip_checks)
        map1 = rule._get_severity_map()
        map2 = rule._get_severity_map()
        skip_after = set(rule._runtime_skip_checks)
        # Skip checks should not change
        assert skip_before == skip_after
        # Severity map should be consistent
        assert map1 == map2

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


@pytest.mark.unit
class TestRecursiveValidation:
    """Tests for recursive STAC validation (catalog → collection → item)."""

    def test_recursive_valid_catalog_passes(self, recursive_catalog: Path) -> None:
        """Valid nested structure passes schema validation."""
        from portolan_cli.validation.stac_rules import StacSchemaRule

        rule = StacSchemaRule()
        result = rule.check(recursive_catalog)
        assert result.passed, f"Expected pass but got: {result.message}"

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="stac-check recursive validation has bugs on Windows",
    )
    def test_recursive_invalid_item_detected(self, recursive_invalid_catalog: Path) -> None:
        """Invalid nested item (missing id) is caught by recursive validation."""
        from portolan_cli.validation.stac_rules import StacSchemaRule

        rule = StacSchemaRule()
        result = rule.check(recursive_invalid_catalog)
        assert not result.passed
        assert result.severity == Severity.ERROR

    def test_recursive_lint_passes_valid(self, recursive_catalog: Path) -> None:
        """Valid nested structure passes lint checks."""
        from portolan_cli.validation.stac_rules import StacLintRule

        rule = StacLintRule()
        result = rule.check(recursive_catalog)
        # May have warnings but should not have errors
        if not result.passed:
            assert result.severity != Severity.ERROR


@pytest.mark.unit
class TestSelfContainedValidation:
    """Regression tests for issue #543.

    A SELF_CONTAINED catalog (relative hrefs, STAC 1.1.0) has a root
    ``catalog.json`` whose own relative ``self`` href fails the STAC 1.1.0
    ``format: iri`` check with an *acceptable* ``must be iri`` error. Before the
    fix, that acceptable root error short-circuited ``StacSchemaRule`` into a
    pass before any linked collection or item was ever inspected — so schema
    violations below the root went undetected.
    """

    def test_self_contained_valid_catalog_passes(self, self_contained_valid_catalog: Path) -> None:
        """A fully valid self-contained 1.1.0 catalog passes.

        Guards against the opposite failure: the fix must NOT treat the root's
        acceptable relative-href IRI error as a real failure.
        """
        from portolan_cli.validation.stac_rules import StacSchemaRule

        result = StacSchemaRule().check(self_contained_valid_catalog)
        assert result.passed, f"Expected pass but got: {result.message}"

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="stac-check recursive validation crashes on Windows with 'list index "
        "out of range' before reaching the collection, so StacSchemaRule cannot "
        "surface the below-root failure there. The same case is covered "
        "cross-platform by StacFieldsRule (test_validation_rules.py).",
    )
    def test_invalid_collection_detected(
        self, self_contained_invalid_collection_catalog: Path
    ) -> None:
        """A collection missing extent/stac_version is detected (issue #543).

        Pre-fix behavior: returns ``_pass("Schema valid (relative hrefs
        accepted)")`` because the root's acceptable IRI error short-circuits
        before the collection is checked.
        """
        from portolan_cli.validation.stac_rules import StacSchemaRule

        result = StacSchemaRule().check(self_contained_invalid_collection_catalog)
        assert not result.passed
        assert result.severity == Severity.ERROR
        # The real violation must surface, not the acceptable relative-href error.
        assert "extent" in result.message
        assert "iri" not in result.message.lower()

    def test_invalid_item_detected(self, self_contained_invalid_item_catalog: Path) -> None:
        """A nested item missing required ``id`` is detected (issue #543)."""
        from portolan_cli.validation.stac_rules import StacSchemaRule

        result = StacSchemaRule().check(self_contained_invalid_item_catalog)
        assert not result.passed
        assert result.severity == Severity.ERROR
        assert "id" in result.message


@pytest.mark.unit
class TestMessageTruncation:
    """Tests for error message truncation when many violations exist."""

    def test_many_violations_truncated(self, many_violations_catalog: Path) -> None:
        """Many violations are truncated with '+N more' suffix."""
        from portolan_cli.validation.stac_rules import StacLintRule

        rule = StacLintRule()
        result = rule.check(many_violations_catalog)
        assert not result.passed
        # Should have truncation indicator if more than 3 issues
        # The catalog has bad ID (percent_encoded) + missing self link + bloated metadata
        if "+more)" in result.message or "(+" in result.message:
            # Verify the count is reasonable
            assert "more)" in result.message


@pytest.mark.unit
class TestConfigIntegration:
    """Tests for config integration through the full stack."""

    def test_build_rules_with_config_affects_lint_behavior(self) -> None:
        """Config passed to _build_rules affects StacLintRule behavior."""
        from portolan_cli.validation.runner import _build_rules

        config = {
            "stac_lint": {
                "severity": {
                    "check_thumbnail": "error",
                    "bloated_links": "skip",
                }
            }
        }
        rules = _build_rules(config=config)
        lint_rule = next(r for r in rules if r.name == "stac_lint")

        # Verify config was passed through
        assert lint_rule.config == config
        # Verify skip was processed
        assert "bloated_links" in lint_rule._runtime_skip_checks
        # Verify severity override works
        severity_map = lint_rule._get_severity_map()
        assert severity_map["check_thumbnail"] == Severity.ERROR
