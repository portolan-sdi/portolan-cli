"""Tests for metadata.yaml schema and validation (ADR-0038).

Tests metadata validation including:
- Required field detection (contact, license only - title/description come from STAC)
- Optional field handling (known_issues, citation, doi, etc.)
- Format validation (email, SPDX license, DOI)
- Template generation for `portolan metadata init`
"""

from __future__ import annotations

from pathlib import Path

import pytest


class TestMetadataValidation:
    """Tests for validate_metadata function."""

    @pytest.mark.unit
    def test_valid_metadata_passes_validation(self) -> None:
        """validate_metadata returns no errors for valid metadata with required fields."""
        from portolan_cli.metadata_yaml import validate_metadata

        # Only contact and license are required - title/description come from STAC
        metadata = {
            "contact": {
                "name": "Data Team",
                "email": "data@example.org",
            },
            "license": "CC-BY-4.0",
        }

        errors = validate_metadata(metadata)

        assert errors == []

    @pytest.mark.unit
    def test_title_is_optional(self) -> None:
        """validate_metadata does not require title (comes from STAC)."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            # No title - should be fine
        }

        errors = validate_metadata(metadata)

        assert not any("title" in e.lower() for e in errors)

    @pytest.mark.unit
    def test_description_is_optional(self) -> None:
        """validate_metadata does not require description (comes from STAC)."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            # No description - should be fine
        }

        errors = validate_metadata(metadata)

        assert not any("description" in e.lower() for e in errors)

    @pytest.mark.unit
    def test_missing_contact_returns_error(self) -> None:
        """validate_metadata returns error when contact is missing."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "license": "CC-BY-4.0",
        }

        errors = validate_metadata(metadata)

        assert any("contact" in e.lower() for e in errors)

    @pytest.mark.unit
    def test_missing_contact_name_returns_error(self) -> None:
        """validate_metadata returns error when contact.name is missing."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"email": "data@example.org"},  # Missing name
            "license": "CC-BY-4.0",
        }

        errors = validate_metadata(metadata)

        assert any("contact.name" in e.lower() or "name" in e.lower() for e in errors)

    @pytest.mark.unit
    def test_missing_contact_email_returns_error(self) -> None:
        """validate_metadata returns error when contact.email is missing."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team"},  # Missing email
            "license": "CC-BY-4.0",
        }

        errors = validate_metadata(metadata)

        assert any("contact.email" in e.lower() or "email" in e.lower() for e in errors)

    @pytest.mark.unit
    def test_missing_license_returns_error(self) -> None:
        """validate_metadata returns error when license is missing."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
        }

        errors = validate_metadata(metadata)

        assert any("license" in e.lower() for e in errors)

    @pytest.mark.unit
    def test_invalid_email_format_returns_error(self) -> None:
        """validate_metadata returns error for invalid email format."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "not-an-email"},
            "license": "CC-BY-4.0",
        }

        errors = validate_metadata(metadata)

        assert any("email" in e.lower() for e in errors)

    @pytest.mark.unit
    def test_invalid_spdx_license_returns_error(self) -> None:
        """validate_metadata returns error for invalid SPDX license identifier."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "NOT-A-REAL-LICENSE",
        }

        errors = validate_metadata(metadata)

        assert any("license" in e.lower() or "spdx" in e.lower() for e in errors)

    @pytest.mark.unit
    def test_valid_spdx_licenses_pass(self) -> None:
        """validate_metadata accepts common SPDX license identifiers."""
        from portolan_cli.metadata_yaml import validate_metadata

        valid_licenses = ["MIT", "Apache-2.0", "CC-BY-4.0", "CC0-1.0", "GPL-3.0-only"]

        for license_id in valid_licenses:
            metadata = {
                "contact": {"name": "Data Team", "email": "data@example.org"},
                "license": license_id,
            }

            errors = validate_metadata(metadata)

            # No license-related errors
            license_errors = [e for e in errors if "license" in e.lower()]
            assert license_errors == [], f"License {license_id} should be valid"

    @pytest.mark.unit
    def test_invalid_doi_format_returns_error(self) -> None:
        """validate_metadata returns error for invalid DOI format."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            "doi": "invalid-doi",  # Should be like 10.xxxx/xxxxx
        }

        errors = validate_metadata(metadata)

        assert any("doi" in e.lower() for e in errors)

    @pytest.mark.unit
    def test_valid_doi_passes(self) -> None:
        """validate_metadata accepts valid DOI formats."""
        from portolan_cli.metadata_yaml import validate_metadata

        valid_dois = [
            "10.5281/zenodo.1234567",
            "10.1000/xyz123",
            "10.1234/example.2024.01",
        ]

        for doi in valid_dois:
            metadata = {
                "contact": {"name": "Data Team", "email": "data@example.org"},
                "license": "CC-BY-4.0",
                "doi": doi,
            }

            errors = validate_metadata(metadata)

            doi_errors = [e for e in errors if "doi" in e.lower()]
            assert doi_errors == [], f"DOI {doi} should be valid"

    @pytest.mark.unit
    def test_all_optional_fields_accepted(self) -> None:
        """validate_metadata accepts all optional fields when present."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            # Optional fields
            "license_url": "https://creativecommons.org/licenses/by/4.0/",
            "citation": "Data Team (2024). Census Data. DOI: 10.5281/zenodo.1234567",
            "doi": "10.5281/zenodo.1234567",
            "keywords": ["census", "demographics", "population"],
            "attribution": "Data provided by Census Bureau",
            "source_url": "https://census.gov/data",
            "processing_notes": "Aggregated from county-level data",
            "known_issues": "Coverage gaps in rural areas for 2020 data.",
        }

        errors = validate_metadata(metadata)

        assert errors == []

    @pytest.mark.unit
    def test_multiple_errors_returned(self) -> None:
        """validate_metadata returns all errors, not just the first one."""
        from portolan_cli.metadata_yaml import validate_metadata

        # Empty metadata - missing required fields
        metadata: dict[str, object] = {}

        errors = validate_metadata(metadata)

        # Should have errors for contact and license (2 required fields)
        assert len(errors) >= 2


class TestMetadataTemplate:
    """Tests for generate_metadata_template function."""

    @pytest.mark.unit
    def test_generates_yaml_string(self) -> None:
        """generate_metadata_template returns valid YAML string."""
        import yaml

        from portolan_cli.metadata_yaml import generate_metadata_template

        template = generate_metadata_template()

        # Should be parseable YAML
        parsed = yaml.safe_load(template)
        assert isinstance(parsed, dict)

    @pytest.mark.unit
    def test_template_has_required_fields(self) -> None:
        """generate_metadata_template includes required fields."""
        import yaml

        from portolan_cli.metadata_yaml import generate_metadata_template

        template = generate_metadata_template()
        parsed = yaml.safe_load(template)

        # Required fields should be present
        assert "contact" in parsed
        assert "license" in parsed

    @pytest.mark.unit
    def test_template_has_comments(self) -> None:
        """generate_metadata_template includes helpful comments."""
        from portolan_cli.metadata_yaml import generate_metadata_template

        template = generate_metadata_template()

        # Should have comments explaining fields
        assert "#" in template
        # Should mention optional fields
        assert "optional" in template.lower()

    @pytest.mark.unit
    def test_template_explains_stac_sourced_fields(self) -> None:
        """generate_metadata_template explains that title/description come from STAC."""
        from portolan_cli.metadata_yaml import generate_metadata_template

        template = generate_metadata_template()

        # Should mention that title/description come from STAC
        assert "stac" in template.lower() or "catalog" in template.lower()


class TestLoadAndValidate:
    """Tests for load_and_validate_metadata function."""

    @pytest.mark.unit
    def test_loads_and_validates_from_path(self, tmp_path: Path) -> None:
        """load_and_validate_metadata loads YAML from path and validates."""
        from portolan_cli.metadata_yaml import load_and_validate_metadata

        # Create catalog structure
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "metadata.yaml").write_text(
            "contact:\n  name: Data Team\n  email: data@example.org\nlicense: CC-BY-4.0\n"
        )

        metadata, errors = load_and_validate_metadata(
            path=catalog_root,
            catalog_root=catalog_root,
        )

        assert metadata["contact"]["name"] == "Data Team"
        assert errors == []

    @pytest.mark.unit
    def test_returns_errors_for_invalid_metadata(self, tmp_path: Path) -> None:
        """load_and_validate_metadata returns validation errors."""
        from portolan_cli.metadata_yaml import load_and_validate_metadata

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "metadata.yaml").write_text(
            "citation: Some citation\n"  # Missing required fields
        )

        metadata, errors = load_and_validate_metadata(
            path=catalog_root,
            catalog_root=catalog_root,
        )

        assert len(errors) > 0
        assert any("contact" in e.lower() or "license" in e.lower() for e in errors)

    @pytest.mark.unit
    def test_uses_hierarchical_merge(self, tmp_path: Path) -> None:
        """load_and_validate_metadata merges metadata from hierarchy."""
        from portolan_cli.metadata_yaml import load_and_validate_metadata

        # Setup hierarchy
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "metadata.yaml").write_text(
            "contact:\n  name: Default Contact\n  email: default@example.org\nlicense: CC-BY-4.0\n"
        )

        collection = catalog_root / "demographics"
        collection.mkdir()
        (collection / ".portolan").mkdir()
        (collection / ".portolan" / "metadata.yaml").write_text(
            "contact:\n  email: demographics@example.org\n"  # Override only email
        )

        metadata, errors = load_and_validate_metadata(
            path=collection,
            catalog_root=catalog_root,
        )

        # Should merge correctly
        assert metadata["contact"]["name"] == "Default Contact"  # Inherited
        assert metadata["contact"]["email"] == "demographics@example.org"  # Overridden
        assert errors == []

    @pytest.mark.unit
    def test_returns_empty_metadata_when_no_files(self, tmp_path: Path) -> None:
        """load_and_validate_metadata returns empty dict when no metadata.yaml exists."""
        from portolan_cli.metadata_yaml import load_and_validate_metadata

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()

        metadata, errors = load_and_validate_metadata(
            path=catalog_root,
            catalog_root=catalog_root,
        )

        assert metadata == {}
        # Should still report missing required fields
        assert len(errors) > 0
