"""Spec/code coverage tests for spec/schema/rules.yaml.

These guard the invariant from Issue #562: the spec MUST be a superset of what
the CLI enforces, never the reverse. Concretely:

- Every ERROR-severity rule the CLI runs (``DEFAULT_RULES``) has a rules.yaml
  entry, linked via the ``code_rule`` field (or is explicitly exempted because
  its coverage lives in prose / JSON Schema).
- Each entry's ``level`` matches the CLI rule's actual declared severity, so the
  spec and code cannot silently drift apart.
- The catalog/collection JSON Schemas require the mandatory human-readable
  ``title``/``description`` (ADR-0053) and titles on ``child``/``item`` links.

See ADR-0048 (CLI repo is the spec source of truth).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from portolan_cli.validation.results import Severity
from portolan_cli.validation.runner import DEFAULT_RULES

pytestmark = pytest.mark.integration

# Declared severity of every CLI rule, keyed by its ``name``.
_CLI_SEVERITY_BY_NAME: dict[str, str] = {rule.name: rule.severity.value for rule in DEFAULT_RULES}

# ERROR-severity CLI rules whose spec coverage lives in prose / JSON Schema or a
# pre-existing rules.yaml entry rather than a ``code_rule`` link. Keeping this
# list explicit turns "add an ERROR rule without documenting it" into a test
# failure: any new error rule must either add a ``code_rule`` mapping in
# rules.yaml or be consciously exempted here.
_EXEMPT_ERROR_RULES: frozenset[str] = frozenset(
    {
        "catalog_exists",  # RULE-0050 (required files)
        "catalog_json_valid",  # RULE-0050 (required files)
        "stac_fields",  # core STAC schema (catalog.schema.json)
        "stac_schema",  # core STAC schema validation
        "tabular_geospatial_flag",  # RULE-0090
        "tabular_collection_level_assets",  # RULE-0094
    }
)

# The rules added for Issue #562, with their expected (level, scope, code_rule).
_NEW_RULES: dict[str, tuple[str, str, str]] = {
    "RULE-0100": ("error", "catalog", "mandatory_titles"),
    "RULE-0101": ("error", "collection", "mandatory_titles"),
    "RULE-0102": ("error", "catalog", "mandatory_titles"),
    "RULE-0103": ("error", "collection", "mandatory_titles"),
    "RULE-0110": ("error", "catalog", "bbox_valid"),
    "RULE-0111": ("error", "collection", "bbox_valid"),
    "RULE-0112": ("error", "item", "bbox_valid"),
    "RULE-0120": ("warning", "item", "provisional_datetime"),
    "RULE-0130": ("warning", "collection", "partition_structure"),
    "RULE-0131": ("error", "collection", "partition_schema_consistency"),
}


class TestNewRuleEntries:
    """The Issue #562 rules exist in rules.yaml with the expected metadata."""

    @pytest.mark.parametrize(("rule_id", "expected"), sorted(_NEW_RULES.items()))
    def test_new_rule_present_with_expected_fields(
        self,
        validation_rules: list[dict[str, Any]],
        rule_id: str,
        expected: tuple[str, str, str],
    ) -> None:
        level, scope, code_rule = expected
        by_id = {r["id"]: r for r in validation_rules}

        assert rule_id in by_id, f"{rule_id} missing from rules.yaml"
        entry = by_id[rule_id]
        assert entry["level"] == level, f"{rule_id} level should be {level}"
        assert entry["scope"] == scope, f"{rule_id} scope should be {scope}"
        assert entry.get("code_rule") == code_rule, f"{rule_id} code_rule should be {code_rule}"

    def test_rule_ids_are_unique(self, validation_rules: list[dict[str, Any]]) -> None:
        ids = [r["id"] for r in validation_rules]
        assert len(ids) == len(set(ids)), "Duplicate rule IDs in rules.yaml"


class TestSpecCodeParity:
    """rules.yaml and the CLI rules cannot silently drift apart."""

    def test_code_rule_level_matches_cli_severity(
        self, validation_rules: list[dict[str, Any]]
    ) -> None:
        for entry in validation_rules:
            code_rule = entry.get("code_rule")
            if code_rule is None:
                continue
            assert code_rule in _CLI_SEVERITY_BY_NAME, (
                f"{entry['id']} references unknown code_rule '{code_rule}'"
            )
            assert entry["level"] == _CLI_SEVERITY_BY_NAME[code_rule], (
                f"{entry['id']} level '{entry['level']}' does not match CLI severity "
                f"'{_CLI_SEVERITY_BY_NAME[code_rule]}' for rule '{code_rule}'"
            )

    def test_spec_is_superset_of_cli_error_rules(
        self, validation_rules: list[dict[str, Any]]
    ) -> None:
        """Every ERROR rule the CLI enforces is documented in rules.yaml."""
        covered = {e["code_rule"] for e in validation_rules if e.get("code_rule")}

        undocumented = [
            rule.name
            for rule in DEFAULT_RULES
            if rule.severity is Severity.ERROR
            and rule.name not in _EXEMPT_ERROR_RULES
            and rule.name not in covered
        ]

        assert not undocumented, (
            "ERROR-severity CLI rules missing a rules.yaml entry "
            f"(add a code_rule mapping or exempt them): {undocumented}"
        )


class TestSchemaRequiresTitles:
    """The published JSON Schemas enforce the mandatory-title rules (ADR-0053)."""

    @pytest.fixture
    def catalog_schema(self, schemas_dir: Path) -> dict[str, Any]:
        result: dict[str, Any] = json.loads((schemas_dir / "catalog.schema.json").read_text())
        return result

    @pytest.fixture
    def collection_schema(self, schemas_dir: Path) -> dict[str, Any]:
        result: dict[str, Any] = json.loads((schemas_dir / "collection.schema.json").read_text())
        return result

    def test_catalog_extensions_require_title_and_description(
        self, catalog_schema: dict[str, Any]
    ) -> None:
        ext = catalog_schema["$defs"]["PortolanCatalogExtensions"]
        assert set(ext["required"]) >= {"title", "description"}

        validator = Draft202012Validator(ext)
        assert list(validator.iter_errors({"type": "Catalog", "description": "x"})), (
            "catalog extension schema should reject a document missing title"
        )
        assert not list(validator.iter_errors({"title": "Roads", "description": "x"})), (
            "catalog extension schema should accept a titled, described document"
        )

    def test_collection_extensions_require_title_and_description(
        self, collection_schema: dict[str, Any]
    ) -> None:
        ext = collection_schema["$defs"]["PortolanCollectionExtensions"]
        assert set(ext["required"]) >= {"title", "description"}

        validator = Draft202012Validator(ext)
        assert list(validator.iter_errors({"type": "Collection", "description": "x"})), (
            "collection extension schema should reject a document missing title"
        )

    def test_catalog_child_links_require_title(self, catalog_schema: dict[str, Any]) -> None:
        validator = Draft202012Validator(catalog_schema["$defs"]["CatalogLink"])

        untitled_child = {"rel": "child", "href": "./roads/collection.json"}
        assert list(validator.iter_errors(untitled_child)), (
            "child link without a title should fail (RULE-0102)"
        )

        titled_child = {"rel": "child", "href": "./roads/collection.json", "title": "Roads"}
        assert not list(validator.iter_errors(titled_child))

        # Non-structural links are unaffected by the title requirement.
        license_link = {"rel": "license", "href": "https://example.com/license"}
        assert not list(validator.iter_errors(license_link))

    def test_collection_item_links_require_title(self, collection_schema: dict[str, Any]) -> None:
        validator = Draft202012Validator(collection_schema["$defs"]["CollectionLink"])

        untitled_item = {"rel": "item", "href": "./census-2020/item.json"}
        assert list(validator.iter_errors(untitled_item)), (
            "item link without a title should fail (RULE-0103)"
        )

        titled_item = {"rel": "item", "href": "./census-2020/item.json", "title": "Census 2020"}
        assert not list(validator.iter_errors(titled_item))
