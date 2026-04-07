"""Tests for #316 schema fields validation.

Tests the new metadata.yaml fields from GitHub issue #316:
- authors (list of dicts with name, optional orcid/email)
- related_dois (list of DOI strings)
- citations (list of strings)
- upstream_version (string)

See ADR-0038 for metadata.yaml schema.
"""

from __future__ import annotations

import pytest


class TestAuthorsValidation:
    """Tests for authors field validation."""

    @pytest.mark.unit
    def test_valid_authors_with_all_fields(self) -> None:
        """validate_metadata accepts authors with name, orcid, and email."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            "authors": [
                {
                    "name": "Jane Doe",
                    "orcid": "0000-0001-2345-6789",
                    "email": "jane.doe@university.edu",
                }
            ],
        }

        errors = validate_metadata(metadata)

        assert errors == []

    @pytest.mark.unit
    def test_valid_authors_name_only(self) -> None:
        """validate_metadata accepts authors with only name (orcid/email optional)."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            "authors": [
                {"name": "Jane Doe"},
                {"name": "John Smith"},
            ],
        }

        errors = validate_metadata(metadata)

        assert errors == []

    @pytest.mark.unit
    def test_missing_author_name_returns_error(self) -> None:
        """validate_metadata returns error when author is missing name."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            "authors": [
                {"orcid": "0000-0001-2345-6789"},  # Missing name
            ],
        }

        errors = validate_metadata(metadata)

        assert any("name" in e.lower() and "author" in e.lower() for e in errors)

    @pytest.mark.unit
    def test_invalid_orcid_format_returns_error(self) -> None:
        """validate_metadata returns error for invalid ORCID format."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            "authors": [
                {
                    "name": "Jane Doe",
                    "orcid": "invalid-orcid",  # Should be 0000-0000-0000-0000
                }
            ],
        }

        errors = validate_metadata(metadata)

        assert any("orcid" in e.lower() for e in errors)

    @pytest.mark.unit
    def test_valid_orcid_formats(self) -> None:
        """validate_metadata accepts valid ORCID formats."""
        from portolan_cli.metadata_yaml import validate_metadata

        valid_orcids = [
            "0000-0001-2345-6789",
            "0000-0002-9876-5432",
            "1234-5678-9012-3456",
        ]

        for orcid in valid_orcids:
            metadata = {
                "contact": {"name": "Data Team", "email": "data@example.org"},
                "license": "CC-BY-4.0",
                "authors": [{"name": "Jane Doe", "orcid": orcid}],
            }

            errors = validate_metadata(metadata)

            orcid_errors = [e for e in errors if "orcid" in e.lower()]
            assert orcid_errors == [], f"ORCID {orcid} should be valid"

    @pytest.mark.unit
    def test_invalid_author_email_format_returns_error(self) -> None:
        """validate_metadata returns error for invalid email in authors."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            "authors": [
                {
                    "name": "Jane Doe",
                    "email": "not-an-email",  # Invalid format
                }
            ],
        }

        errors = validate_metadata(metadata)

        assert any("email" in e.lower() and "author" in e.lower() for e in errors)

    @pytest.mark.unit
    def test_authors_must_be_list(self) -> None:
        """validate_metadata returns error if authors is not a list."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            "authors": "Jane Doe",  # Should be a list
        }

        errors = validate_metadata(metadata)

        assert any("authors" in e.lower() and "list" in e.lower() for e in errors)

    @pytest.mark.unit
    def test_author_must_be_dict(self) -> None:
        """validate_metadata returns error if author entry is not a dict."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            "authors": ["Jane Doe"],  # Should be [{"name": "Jane Doe"}]
        }

        errors = validate_metadata(metadata)

        assert any("author" in e.lower() and "mapping" in e.lower() for e in errors)

    @pytest.mark.unit
    def test_multiple_authors_validated(self) -> None:
        """validate_metadata validates all authors, reports errors for each."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            "authors": [
                {"name": "Valid Author"},
                {"orcid": "0000-0001-2345-6789"},  # Missing name
                {"name": "Another Author", "orcid": "invalid"},  # Invalid orcid
            ],
        }

        errors = validate_metadata(metadata)

        # Should have at least 2 errors (missing name + invalid orcid)
        assert len([e for e in errors if "author" in e.lower()]) >= 2


class TestRelatedDoisValidation:
    """Tests for related_dois field validation."""

    @pytest.mark.unit
    def test_valid_related_dois_list(self) -> None:
        """validate_metadata accepts valid DOI list."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            "related_dois": [
                "10.5281/zenodo.1234567",
                "10.1000/xyz123",
            ],
        }

        errors = validate_metadata(metadata)

        assert errors == []

    @pytest.mark.unit
    def test_invalid_doi_in_list_returns_error(self) -> None:
        """validate_metadata returns error for invalid DOI in related_dois."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            "related_dois": [
                "10.5281/zenodo.1234567",  # Valid
                "not-a-doi",  # Invalid
            ],
        }

        errors = validate_metadata(metadata)

        assert any("related_dois" in e.lower() or "doi" in e.lower() for e in errors)

    @pytest.mark.unit
    def test_related_dois_must_be_list(self) -> None:
        """validate_metadata returns error if related_dois is not a list."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            "related_dois": "10.5281/zenodo.1234567",  # Should be a list
        }

        errors = validate_metadata(metadata)

        assert any("related_dois" in e.lower() and "list" in e.lower() for e in errors)

    @pytest.mark.unit
    def test_empty_related_dois_passes(self) -> None:
        """validate_metadata accepts empty related_dois list."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            "related_dois": [],
        }

        errors = validate_metadata(metadata)

        assert errors == []


class TestCitationsValidation:
    """Tests for citations field validation."""

    @pytest.mark.unit
    def test_valid_citations_list(self) -> None:
        """validate_metadata accepts citations as list of strings."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            "citations": [
                "Doe, J. (2024). A Study. Journal of Studies, 1(1), 1-10.",
                "Smith, A. (2023). Another Paper. Nature, 600, 100-105.",
            ],
        }

        errors = validate_metadata(metadata)

        assert errors == []

    @pytest.mark.unit
    def test_citations_must_be_list(self) -> None:
        """validate_metadata returns error if citations is not a list."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            "citations": "A single citation string",  # Should be a list
        }

        errors = validate_metadata(metadata)

        assert any("citations" in e.lower() and "list" in e.lower() for e in errors)

    @pytest.mark.unit
    def test_citations_items_must_be_strings(self) -> None:
        """validate_metadata returns error if citations contains non-strings."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            "citations": [
                "Valid citation string",
                123,  # Should be a string
            ],
        }

        errors = validate_metadata(metadata)

        assert any("citation" in e.lower() and "string" in e.lower() for e in errors)


class TestUpstreamVersionValidation:
    """Tests for upstream_version field validation."""

    @pytest.mark.unit
    def test_valid_upstream_version(self) -> None:
        """validate_metadata accepts upstream_version as string."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            "upstream_version": "2024.1",
        }

        errors = validate_metadata(metadata)

        assert errors == []

    @pytest.mark.unit
    def test_upstream_version_must_be_string(self) -> None:
        """validate_metadata returns error if upstream_version is not a string."""
        from portolan_cli.metadata_yaml import validate_metadata

        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
            "upstream_version": 2024,  # Should be a string
        }

        errors = validate_metadata(metadata)

        assert any("upstream_version" in e.lower() and "string" in e.lower() for e in errors)


class TestOrcidPattern:
    """Tests for ORCID pattern regex."""

    @pytest.mark.unit
    def test_orcid_pattern_exists(self) -> None:
        """ORCID_PATTERN is exported from module."""
        from portolan_cli.metadata_yaml import ORCID_PATTERN

        assert ORCID_PATTERN is not None

    @pytest.mark.unit
    def test_orcid_pattern_matches_valid(self) -> None:
        """ORCID_PATTERN matches valid ORCID format."""
        from portolan_cli.metadata_yaml import ORCID_PATTERN

        valid_orcids = [
            "0000-0001-2345-6789",
            "0000-0002-9876-5432",
            "9999-9999-9999-9999",
            "0000-0001-2345-678X",  # X check digit (valid per ISO 7064 Mod 11-2)
            "0000-0002-1825-009X",  # Real ORCID with X (Josiah Carberry)
        ]

        for orcid in valid_orcids:
            assert ORCID_PATTERN.match(orcid), f"Should match valid ORCID: {orcid}"

    @pytest.mark.unit
    def test_orcid_pattern_rejects_invalid(self) -> None:
        """ORCID_PATTERN rejects invalid formats."""
        from portolan_cli.metadata_yaml import ORCID_PATTERN

        invalid_orcids = [
            "0000-0001-2345",  # Too short
            "0000-0001-2345-67890",  # Too long
            "0000-0001-234X-6789",  # X in wrong position (not last char)
            "XXXX-XXXX-XXXX-XXXX",  # All letters
            "0000000123456789",  # No dashes
        ]

        for orcid in invalid_orcids:
            assert not ORCID_PATTERN.match(orcid), f"Should reject invalid ORCID: {orcid}"
