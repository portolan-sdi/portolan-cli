"""Negative tests for spec compliance validators.

These tests verify that validators REJECT invalid data. Without these,
the compliance tests are tautological - they could pass even if validators
were broken (always returning empty error lists).

See: ADR-0001 (agentic-first: defend against tautological tests)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable


class TestSchemaValidatorRejectsInvalid:
    """Verify that schema validators reject malformed data."""

    @pytest.mark.integration
    def test_versions_validator_rejects_missing_required(
        self,
        versions_schema: dict[str, Any],
        validate_versions: Callable[[dict[str, Any], dict[str, Any]], list[str]],
    ) -> None:
        """versions.json validator MUST reject data missing required fields."""
        invalid_data = {"spec_version": "1.0.0"}  # Missing current_version and versions

        errors = validate_versions(invalid_data, versions_schema)

        assert errors, "Validator should reject data missing required fields"
        assert any(
            "required" in e.lower() or "current_version" in e or "versions" in e for e in errors
        )

    @pytest.mark.integration
    def test_versions_validator_rejects_invalid_semver(
        self,
        versions_schema: dict[str, Any],
        validate_versions: Callable[[dict[str, Any], dict[str, Any]], list[str]],
    ) -> None:
        """versions.json validator MUST reject invalid semver patterns."""
        invalid_data = {
            "spec_version": "not-a-semver",  # Invalid pattern
            "current_version": "1.0.0",
            "versions": [],
        }

        errors = validate_versions(invalid_data, versions_schema)

        assert errors, "Validator should reject invalid semver format"

    @pytest.mark.integration
    def test_versions_validator_rejects_invalid_timestamp(
        self,
        versions_schema: dict[str, Any],
        validate_versions: Callable[[dict[str, Any], dict[str, Any]], list[str]],
    ) -> None:
        """versions.json validator MUST reject non-UTC timestamps."""
        invalid_data = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2024-01-15T10:30:00+05:00",  # NOT UTC (missing Z)
                    "breaking": False,
                    "assets": {},
                    "changes": [],
                }
            ],
        }

        errors = validate_versions(invalid_data, versions_schema)

        assert errors, "Validator should reject non-UTC timestamp (missing Z)"

    @pytest.mark.integration
    def test_versions_validator_rejects_invalid_sha256(
        self,
        versions_schema: dict[str, Any],
        validate_versions: Callable[[dict[str, Any], dict[str, Any]], list[str]],
    ) -> None:
        """versions.json validator MUST reject invalid SHA-256 checksums."""
        invalid_data = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2024-01-15T10:30:00Z",
                    "breaking": False,
                    "assets": {
                        "data.parquet": {
                            "sha256": "not-a-valid-sha256",  # Invalid format
                            "size_bytes": 1000,
                            "href": "collection/data.parquet",
                        }
                    },
                    "changes": ["data.parquet"],
                }
            ],
        }

        errors = validate_versions(invalid_data, versions_schema)

        assert errors, "Validator should reject invalid SHA-256 format"

    @pytest.mark.integration
    def test_stac_validator_rejects_wrong_type(
        self,
        catalog_schema: dict[str, Any],
        validate_stac: Callable[[dict[str, Any], dict[str, Any]], list[str]],
    ) -> None:
        """STAC validator MUST reject wrong type field."""
        invalid_data = {
            "type": "Collection",  # Should be "Catalog"
            "stac_version": "1.0.0",
            "id": "test-catalog",
            "description": "Test",
            "links": [],
        }

        errors = validate_stac(invalid_data, catalog_schema)

        assert errors, "Validator should reject wrong 'type' field"

    @pytest.mark.integration
    def test_stac_validator_rejects_invalid_stac_version(
        self,
        collection_schema: dict[str, Any],
        validate_stac: Callable[[dict[str, Any], dict[str, Any]], list[str]],
    ) -> None:
        """STAC validator MUST reject invalid stac_version pattern."""
        invalid_data = {
            "type": "Collection",
            "stac_version": "0.9.0",  # Should be 1.x.x
            "id": "test-collection",
            "description": "Test",
            "license": "MIT",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [],
        }

        errors = validate_stac(invalid_data, collection_schema)

        assert errors, "Validator should reject stac_version that doesn't match 1.x.x"


class TestSemanticRuleValidatorsRejectInvalid:
    """Verify that semantic rule validators reject violations."""

    @pytest.mark.integration
    def test_rule_0012_rejects_mismatched_current_version(
        self,
        validate_rule_0012: Callable[[dict[str, Any]], list[str]],
    ) -> None:
        """RULE-0012: MUST reject when current_version doesn't match last version."""
        invalid_data = {
            "current_version": "1.0.0",  # Mismatch!
            "versions": [
                {"version": "1.0.0"},
                {"version": "2.0.0"},  # Last version is 2.0.0
            ],
        }

        errors = validate_rule_0012(invalid_data)

        assert errors, "RULE-0012 should reject current_version mismatch"
        assert any("RULE-0012" in e for e in errors)

    @pytest.mark.integration
    def test_rule_0012_rejects_null_with_versions(
        self,
        validate_rule_0012: Callable[[dict[str, Any]], list[str]],
    ) -> None:
        """RULE-0012: MUST reject null current_version when versions exist."""
        invalid_data = {
            "current_version": None,
            "versions": [{"version": "1.0.0"}],
        }

        errors = validate_rule_0012(invalid_data)

        assert errors, "RULE-0012 should reject null current_version with non-empty versions"

    @pytest.mark.integration
    def test_rule_0013_rejects_invalid_change_reference(
        self,
        validate_rule_0013: Callable[[dict[str, Any]], list[str]],
    ) -> None:
        """RULE-0013: MUST reject changes referencing non-existent assets."""
        invalid_data = {
            "versions": [
                {
                    "assets": {"real-file.parquet": {}},
                    "changes": ["non-existent-file.parquet"],  # Not in assets!
                }
            ],
        }

        errors = validate_rule_0013(invalid_data)

        assert errors, "RULE-0013 should reject changes referencing non-existent assets"
        assert any("RULE-0013" in e for e in errors)

    @pytest.mark.integration
    def test_rule_0014_rejects_duplicate_versions(
        self,
        validate_rule_0014: Callable[[dict[str, Any]], list[str]],
    ) -> None:
        """RULE-0014: MUST reject duplicate version strings."""
        invalid_data = {
            "versions": [
                {"version": "1.0.0"},
                {"version": "1.0.0"},  # Duplicate!
            ],
        }

        errors = validate_rule_0014(invalid_data)

        assert errors, "RULE-0014 should reject duplicate version strings"
        assert any("RULE-0014" in e for e in errors)

    @pytest.mark.integration
    def test_rule_0040_rejects_absolute_unix_path(
        self,
        validate_rule_0040: Callable[[dict[str, Any]], list[str]],
    ) -> None:
        """RULE-0040: MUST reject Unix absolute paths in structural links."""
        invalid_data = {
            "links": [
                {"rel": "root", "href": "/absolute/path/catalog.json"},  # Unix absolute
            ],
        }

        errors = validate_rule_0040(invalid_data)

        assert errors, "RULE-0040 should reject Unix absolute paths"
        assert any("RULE-0040" in e for e in errors)

    @pytest.mark.integration
    def test_rule_0040_rejects_file_url(
        self,
        validate_rule_0040: Callable[[dict[str, Any]], list[str]],
    ) -> None:
        """RULE-0040: MUST reject file:// URLs in structural links."""
        invalid_data = {
            "links": [
                {"rel": "self", "href": "file:///home/user/catalog.json"},
            ],
        }

        errors = validate_rule_0040(invalid_data)

        assert errors, "RULE-0040 should reject file:// URLs"

    @pytest.mark.integration
    def test_rule_0040_rejects_windows_absolute_path(
        self,
        validate_rule_0040: Callable[[dict[str, Any]], list[str]],
    ) -> None:
        """RULE-0040: MUST reject Windows absolute paths in structural links."""
        invalid_data = {
            "links": [
                {"rel": "child", "href": "C:\\Users\\data\\collection.json"},
            ],
        }

        errors = validate_rule_0040(invalid_data)

        assert errors, "RULE-0040 should reject Windows absolute paths"

    @pytest.mark.integration
    def test_rule_0040_allows_relative_paths(
        self,
        validate_rule_0040: Callable[[dict[str, Any]], list[str]],
    ) -> None:
        """RULE-0040: SHOULD allow relative paths in structural links."""
        valid_data = {
            "links": [
                {"rel": "root", "href": "./catalog.json"},
                {"rel": "self", "href": "catalog.json"},
                {"rel": "child", "href": "points/collection.json"},
                {"rel": "parent", "href": "../catalog.json"},
            ],
        }

        errors = validate_rule_0040(valid_data)

        assert not errors, f"RULE-0040 should allow relative paths, got: {errors}"

    @pytest.mark.integration
    def test_rule_0040_ignores_non_structural_links(
        self,
        validate_rule_0040: Callable[[dict[str, Any]], list[str]],
    ) -> None:
        """RULE-0040: SHOULD ignore non-structural link relations."""
        valid_data = {
            "links": [
                {"rel": "license", "href": "https://example.com/license"},
                {"rel": "documentation", "href": "https://docs.example.com"},
            ],
        }

        errors = validate_rule_0040(valid_data)

        assert not errors, "RULE-0040 should ignore non-structural links"
