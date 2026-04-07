"""Tests for #316 README section generators (Wave 2C).

Tests new metadata fields for citation, authors, and version sections:
- citations list (multiple citations)
- related_dois (in addition to primary DOI)
- authors with ORCID links
- upstream_version with optional URL
"""

from __future__ import annotations

import pytest


class TestCitationSectionExtended:
    """Tests for extended _add_citation_section supporting #316 fields."""

    @pytest.mark.unit
    def test_backward_compat_single_citation(self) -> None:
        """Single citation field still works (backward compat)."""
        from portolan_cli.readme import _add_citation_section

        sections: list[str] = []
        metadata = {"citation": "Smith, J. (2024). Example Dataset. Journal."}

        _add_citation_section(sections, metadata)

        assert "## Citation" in sections
        assert "Smith, J. (2024). Example Dataset. Journal." in sections

    @pytest.mark.unit
    def test_citations_list_renders_all(self) -> None:
        """Multiple citations in citations list all render."""
        from portolan_cli.readme import _add_citation_section

        sections: list[str] = []
        metadata = {
            "citations": [
                "Smith, J. (2024). First Paper.",
                "Jones, A. (2023). Second Paper.",
            ]
        }

        _add_citation_section(sections, metadata)

        assert "## Citation" in sections
        assert "Smith, J. (2024). First Paper." in sections
        assert "Jones, A. (2023). Second Paper." in sections

    @pytest.mark.unit
    def test_both_citation_and_citations_list(self) -> None:
        """Both single citation and citations list are rendered."""
        from portolan_cli.readme import _add_citation_section

        sections: list[str] = []
        metadata = {
            "citation": "Primary citation here.",
            "citations": ["Secondary citation 1.", "Secondary citation 2."],
        }

        _add_citation_section(sections, metadata)

        assert "## Citation" in sections
        assert "Primary citation here." in sections
        assert "Secondary citation 1." in sections
        assert "Secondary citation 2." in sections

    @pytest.mark.unit
    def test_related_dois_list_renders(self) -> None:
        """related_dois renders as a list with links."""
        from portolan_cli.readme import _add_citation_section

        sections: list[str] = []
        metadata = {
            "doi": "10.1234/primary",
            "related_dois": ["10.1234/related1", "10.5678/related2"],
        }

        _add_citation_section(sections, metadata)

        assert "## Citation" in sections
        # Primary DOI
        assert "**DOI**: [10.1234/primary](https://doi.org/10.1234/primary)" in sections
        # Related DOIs section
        assert "**Related DOIs**:" in sections
        assert "- [10.1234/related1](https://doi.org/10.1234/related1)" in sections
        assert "- [10.5678/related2](https://doi.org/10.5678/related2)" in sections

    @pytest.mark.unit
    def test_related_dois_without_primary_doi(self) -> None:
        """related_dois can exist without a primary doi."""
        from portolan_cli.readme import _add_citation_section

        sections: list[str] = []
        metadata = {"related_dois": ["10.1234/related"]}

        _add_citation_section(sections, metadata)

        assert "## Citation" in sections
        assert "**Related DOIs**:" in sections
        assert "- [10.1234/related](https://doi.org/10.1234/related)" in sections

    @pytest.mark.unit
    def test_no_citation_section_when_empty(self) -> None:
        """No citation section when no citation, citations, doi, or related_dois."""
        from portolan_cli.readme import _add_citation_section

        sections: list[str] = []
        metadata = {}

        _add_citation_section(sections, metadata)

        assert "## Citation" not in sections
        assert len(sections) == 0


class TestAuthorsSection:
    """Tests for _add_authors_section supporting #316 authors field."""

    @pytest.mark.unit
    def test_authors_with_orcid_renders_links(self) -> None:
        """Authors with ORCID IDs render as clickable links."""
        from portolan_cli.readme import _add_authors_section

        sections: list[str] = []
        metadata = {
            "authors": [
                {"name": "Jane Smith", "orcid": "0000-0001-2345-6789"},
                {"name": "John Doe", "orcid": "0000-0002-3456-7890"},
            ]
        }

        _add_authors_section(sections, metadata)
        result = "\n".join(sections)

        assert "## Authors" in result
        # ORCID links use the standard URL format
        assert "[Jane Smith](https://orcid.org/0000-0001-2345-6789)" in result
        assert "[John Doe](https://orcid.org/0000-0002-3456-7890)" in result

    @pytest.mark.unit
    def test_authors_without_orcid_renders_name_only(self) -> None:
        """Authors without ORCID render as plain text."""
        from portolan_cli.readme import _add_authors_section

        sections: list[str] = []
        metadata = {
            "authors": [
                {"name": "Jane Smith"},
                {"name": "John Doe", "orcid": "0000-0001-1111-2222"},
            ]
        }

        _add_authors_section(sections, metadata)
        result = "\n".join(sections)

        assert "## Authors" in result
        # Jane Smith has no ORCID, so just name (as list item)
        assert "- Jane Smith" in result
        # John Doe has ORCID
        assert "[John Doe](https://orcid.org/0000-0001-1111-2222)" in result

    @pytest.mark.unit
    def test_authors_with_affiliation(self) -> None:
        """Authors with affiliation include it in output."""
        from portolan_cli.readme import _add_authors_section

        sections: list[str] = []
        metadata = {
            "authors": [
                {"name": "Jane Smith", "affiliation": "University of Example"},
            ]
        }

        _add_authors_section(sections, metadata)
        result = "\n".join(sections)

        assert "## Authors" in result
        assert "Jane Smith" in result
        assert "University of Example" in result

    @pytest.mark.unit
    def test_no_authors_section_when_empty(self) -> None:
        """No authors section when authors field is empty or missing."""
        from portolan_cli.readme import _add_authors_section

        sections: list[str] = []
        metadata = {}

        _add_authors_section(sections, metadata)

        assert "## Authors" not in sections
        assert len(sections) == 0

    @pytest.mark.unit
    def test_no_authors_section_when_empty_list(self) -> None:
        """No authors section when authors is an empty list."""
        from portolan_cli.readme import _add_authors_section

        sections: list[str] = []
        metadata = {"authors": []}

        _add_authors_section(sections, metadata)

        assert "## Authors" not in sections


class TestVersionSection:
    """Tests for _add_version_section supporting #316 version fields."""

    @pytest.mark.unit
    def test_upstream_version_renders(self) -> None:
        """upstream_version renders as text."""
        from portolan_cli.readme import _add_version_section

        sections: list[str] = []
        metadata = {"upstream_version": "2024.1.0"}

        _add_version_section(sections, metadata)
        result = "\n".join(sections)

        assert "## Version" in result
        assert "2024.1.0" in result

    @pytest.mark.unit
    def test_upstream_version_with_url_renders_link(self) -> None:
        """upstream_version with URL renders as clickable link."""
        from portolan_cli.readme import _add_version_section

        sections: list[str] = []
        metadata = {
            "upstream_version": "2024.1.0",
            "upstream_version_url": "https://example.org/releases/2024.1.0",
        }

        _add_version_section(sections, metadata)
        result = "\n".join(sections)

        assert "## Version" in result
        assert "[2024.1.0](https://example.org/releases/2024.1.0)" in result

    @pytest.mark.unit
    def test_no_version_section_when_empty(self) -> None:
        """No version section when upstream_version is missing."""
        from portolan_cli.readme import _add_version_section

        sections: list[str] = []
        metadata = {}

        _add_version_section(sections, metadata)

        assert "## Version" not in sections
        assert len(sections) == 0

    @pytest.mark.unit
    def test_url_without_version_is_ignored(self) -> None:
        """upstream_version_url alone doesn't create section."""
        from portolan_cli.readme import _add_version_section

        sections: list[str] = []
        metadata = {"upstream_version_url": "https://example.org/releases"}

        _add_version_section(sections, metadata)

        assert "## Version" not in sections


class TestGenerateReadmeSectionOrder:
    """Tests for section ordering in generate_readme with #316 fields."""

    @pytest.mark.unit
    def test_authors_before_citation(self) -> None:
        """Authors section appears before Citation section."""
        from portolan_cli.readme import generate_readme

        stac = {"type": "Collection", "id": "test", "title": "Test"}
        metadata = {
            "authors": [{"name": "Jane Smith", "orcid": "0000-0001-2345-6789"}],
            "citation": "Smith, J. (2024). Dataset.",
            "license": "MIT",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        authors_idx = readme.find("## Authors")
        citation_idx = readme.find("## Citation")

        assert authors_idx != -1, "Authors section should exist"
        assert citation_idx != -1, "Citation section should exist"
        assert authors_idx < citation_idx, "Authors should come before Citation"

    @pytest.mark.unit
    def test_version_section_in_readme(self) -> None:
        """Version section appears in generated README."""
        from portolan_cli.readme import generate_readme

        stac = {"type": "Collection", "id": "test", "title": "Test"}
        metadata = {
            "upstream_version": "v2.0",
            "upstream_version_url": "https://example.org/v2",
            "license": "MIT",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        assert "## Version" in readme
        assert "[v2.0](https://example.org/v2)" in readme
