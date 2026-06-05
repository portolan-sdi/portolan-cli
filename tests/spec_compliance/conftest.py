"""Fixtures for spec compliance tests.

These tests validate CLI output against the machine-readable schemas from
the Portolan specification (see spec/schema/ in this repository).

The CLI repository is the source of truth for the spec; portolan-spec is
a read-only mirror synced via CI (see ADR-0048).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import yaml
from click.testing import CliRunner

if TYPE_CHECKING:
    from collections.abc import Callable


# =============================================================================
# Shared Test Fixtures
# =============================================================================


@pytest.fixture
def runner() -> CliRunner:
    """Create a Click test runner (shared across all spec compliance tests)."""
    return CliRunner()


# =============================================================================
# Schema Loading Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def schemas_dir() -> Path:
    """Return path to spec schemas directory (source of truth)."""
    # Navigate from tests/spec_compliance/ to repo root, then to spec/schema/
    return Path(__file__).parent.parent.parent / "spec" / "schema"


@pytest.fixture(scope="session")
def versions_schema(schemas_dir: Path) -> dict[str, Any]:
    """Load the collection-level versions.json schema.

    This schema is for collection-level versions.json (with version history).
    It is self-contained and doesn't require external references.
    """
    schema_path = schemas_dir / "versions.schema.json"
    result: dict[str, Any] = json.loads(schema_path.read_text())
    return result


@pytest.fixture(scope="session")
def catalog_versions_schema(schemas_dir: Path) -> dict[str, Any]:
    """Load the catalog-level versions.json schema.

    This schema is for root-level versions.json (collection index).
    Different structure from collection-level versions.json.
    """
    schema_path = schemas_dir / "catalog-versions.schema.json"
    result: dict[str, Any] = json.loads(schema_path.read_text())
    return result


@pytest.fixture(scope="session")
def validation_rules(schemas_dir: Path) -> list[dict[str, Any]]:
    """Load semantic validation rules from rules.yaml.

    These rules cannot be expressed in JSON Schema and require
    programmatic validation (e.g., path consistency, uniqueness).
    """
    rules_path = schemas_dir / "rules.yaml"
    data: dict[str, Any] = yaml.safe_load(rules_path.read_text())
    result: list[dict[str, Any]] = data.get("rules", [])
    return result


# =============================================================================
# Portolan-Only Schema Fixtures (without external STAC refs)
# =============================================================================


@pytest.fixture(scope="session")
def portolan_collection_schema() -> dict[str, Any]:
    """Portolan-specific collection schema without external STAC reference.

    This validates only the Portolan extensions, not the base STAC schema.
    Useful for compliance testing where we can't resolve external $refs.
    """
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["type", "stac_version", "id", "description", "license", "extent", "links"],
        "properties": {
            "type": {"const": "Collection"},
            "stac_version": {
                "type": "string",
                "pattern": "^1\\.[0-9]+\\.[0-9]+$",
            },
            "id": {"type": "string", "minLength": 1},
            "title": {"type": "string"},
            "description": {"type": "string"},
            "license": {"type": "string"},
            "extent": {
                "type": "object",
                "required": ["spatial", "temporal"],
                "properties": {
                    "spatial": {
                        "type": "object",
                        "required": ["bbox"],
                        "properties": {
                            "bbox": {
                                "type": "array",
                                "items": {
                                    "type": "array",
                                    "items": {"type": "number"},
                                },
                            }
                        },
                    },
                    "temporal": {
                        "type": "object",
                        "required": ["interval"],
                        "properties": {
                            "interval": {
                                "type": "array",
                                "items": {
                                    "type": "array",
                                    "items": {"type": ["string", "null"]},
                                },
                            }
                        },
                    },
                },
            },
            "links": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["rel", "href"],
                    "properties": {
                        "rel": {"type": "string"},
                        "href": {"type": "string", "minLength": 1},
                        "type": {"type": "string"},
                        "title": {"type": "string"},
                    },
                },
            },
            "assets": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "required": ["href"],
                    "properties": {
                        "href": {"type": "string", "minLength": 1},
                        "type": {"type": "string"},
                        "title": {"type": "string"},
                        "description": {"type": "string"},
                        "roles": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                    },
                },
            },
            "stac_extensions": {
                "type": "array",
                "items": {"type": "string"},
            },
            "summaries": {"type": "object"},
            "providers": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["name"],
                    "properties": {
                        "name": {"type": "string"},
                        "roles": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "url": {"type": "string"},
                        "description": {"type": "string"},
                    },
                },
            },
        },
    }


@pytest.fixture(scope="session")
def portolan_catalog_schema() -> dict[str, Any]:
    """Portolan-specific catalog schema without external STAC reference.

    This validates only the Portolan extensions, not the base STAC schema.
    """
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["type", "stac_version", "id", "description", "links"],
        "properties": {
            "type": {"const": "Catalog"},
            "stac_version": {
                "type": "string",
                "pattern": "^1\\.[0-9]+\\.[0-9]+$",
            },
            "id": {"type": "string", "minLength": 1},
            "title": {"type": "string"},
            "description": {"type": "string"},
            "links": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["rel", "href"],
                    "properties": {
                        "rel": {"type": "string"},
                        "href": {"type": "string", "minLength": 1},
                        "type": {"type": "string"},
                        "title": {"type": "string"},
                    },
                },
            },
        },
    }


# =============================================================================
# Schema Validation Helpers
# =============================================================================


@pytest.fixture(scope="session")
def validate_versions() -> Callable[[dict[str, Any], dict[str, Any]], list[str]]:
    """Return a validation function for versions.json.

    Returns a list of validation error messages (empty if valid).
    """
    from jsonschema import Draft202012Validator

    def _validate(data: dict[str, Any], schema: dict[str, Any]) -> list[str]:
        validator = Draft202012Validator(schema)
        errors = list(validator.iter_errors(data))
        return [f"{e.json_path}: {e.message}" for e in errors]

    return _validate


@pytest.fixture(scope="session")
def validate_stac() -> Callable[[dict[str, Any], dict[str, Any]], list[str]]:
    """Return a validation function for STAC documents (catalog/collection).

    Returns a list of validation error messages (empty if valid).
    """
    from jsonschema import Draft202012Validator

    def _validate(data: dict[str, Any], schema: dict[str, Any]) -> list[str]:
        validator = Draft202012Validator(schema)
        errors = list(validator.iter_errors(data))
        return [f"{e.json_path}: {e.message}" for e in errors]

    return _validate


# =============================================================================
# Semantic Rule Validators (from rules.yaml)
# =============================================================================


@pytest.fixture(scope="session")
def validate_rule_0012() -> Callable[[dict[str, Any]], list[str]]:
    """RULE-0012: current_version MUST match the last entry in versions array."""

    def _validate(versions_data: dict[str, Any]) -> list[str]:
        errors: list[str] = []
        current = versions_data.get("current_version")
        versions = versions_data.get("versions", [])

        if current is not None:
            if not versions:
                errors.append("RULE-0012: current_version is set but versions array is empty")
            elif current != versions[-1].get("version"):
                errors.append(
                    f"RULE-0012: current_version '{current}' does not match "
                    f"last version '{versions[-1].get('version')}'"
                )
        elif versions:
            errors.append("RULE-0012: current_version is null but versions array is not empty")

        return errors

    return _validate


@pytest.fixture(scope="session")
def validate_rule_0013() -> Callable[[dict[str, Any]], list[str]]:
    """RULE-0013: changes array MUST only reference keys that exist in assets."""

    def _validate(versions_data: dict[str, Any]) -> list[str]:
        errors: list[str] = []

        for i, version in enumerate(versions_data.get("versions", [])):
            assets = set(version.get("assets", {}).keys())
            changes = version.get("changes", [])

            for change in changes:
                if change not in assets:
                    errors.append(
                        f"RULE-0013: version[{i}].changes references '{change}' "
                        f"which is not in assets"
                    )

        return errors

    return _validate


@pytest.fixture(scope="session")
def validate_rule_0014() -> Callable[[dict[str, Any]], list[str]]:
    """RULE-0014: Version strings MUST be unique within versions array."""

    def _validate(versions_data: dict[str, Any]) -> list[str]:
        errors: list[str] = []
        versions = versions_data.get("versions", [])
        version_strings = [v.get("version") for v in versions]

        if len(version_strings) != len(set(version_strings)):
            seen: set[str] = set()
            duplicates: set[str] = set()
            for v in version_strings:
                if v in seen:
                    duplicates.add(v)
                seen.add(v)
            errors.append(f"RULE-0014: Duplicate version strings: {duplicates}")

        return errors

    return _validate


@pytest.fixture(scope="session")
def validate_rule_0040() -> Callable[[dict[str, Any]], list[str]]:
    """RULE-0040: Catalog MUST use SELF_CONTAINED type (relative links)."""
    import re

    def _validate(catalog_data: dict[str, Any]) -> list[str]:
        errors: list[str] = []

        for link in catalog_data.get("links", []):
            rel = link.get("rel", "")
            href = link.get("href", "")

            # Only check structural links
            if rel not in ("root", "self", "parent", "child"):
                continue

            # Check for absolute paths
            if href.startswith("/"):
                errors.append(f"RULE-0040: Link rel='{rel}' has Unix absolute path: {href}")
            elif href.startswith("file://"):
                errors.append(f"RULE-0040: Link rel='{rel}' has file:// URL: {href}")
            elif re.match(r"^[A-Za-z]:", href):
                errors.append(f"RULE-0040: Link rel='{rel}' has Windows absolute path: {href}")
            elif href.startswith("\\\\"):
                errors.append(f"RULE-0040: Link rel='{rel}' has Windows UNC path: {href}")
            elif re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", href):
                errors.append(f"RULE-0040: Link rel='{rel}' has URI scheme: {href}")

        return errors

    return _validate
