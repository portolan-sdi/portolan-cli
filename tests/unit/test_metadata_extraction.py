"""Tests for metadata extraction dataclasses.

Tests the ExtractedMetadata and Author dataclasses that provide
a unified abstraction for metadata extraction from various sources
(ArcGIS, Socrata, CKAN, etc.).

Addresses:
- #312: Unified metadata extraction framework
- #316: Author and citation support
"""

from __future__ import annotations

from datetime import date

import pytest


class TestAuthorDataclass:
    """Tests for the Author frozen dataclass."""

    @pytest.mark.unit
    def test_author_with_name_only(self) -> None:
        """Author can be created with just a name."""
        from portolan_cli.metadata_extraction import Author

        author = Author(name="Jane Doe")

        assert author.name == "Jane Doe"
        assert author.email is None
        assert author.orcid is None

    @pytest.mark.unit
    def test_author_with_all_fields(self) -> None:
        """Author can be created with all fields populated."""
        from portolan_cli.metadata_extraction import Author

        author = Author(
            name="Jane Doe",
            email="jane@example.org",
            orcid="0000-0001-2345-6789",
        )

        assert author.name == "Jane Doe"
        assert author.email == "jane@example.org"
        assert author.orcid == "0000-0001-2345-6789"

    @pytest.mark.unit
    def test_author_is_frozen(self) -> None:
        """Author dataclass is frozen (immutable)."""
        from portolan_cli.metadata_extraction import Author

        author = Author(name="Jane Doe")

        with pytest.raises(AttributeError):
            author.name = "John Doe"  # type: ignore[misc]

    @pytest.mark.unit
    def test_author_equality(self) -> None:
        """Two Authors with same values are equal."""
        from portolan_cli.metadata_extraction import Author

        author1 = Author(name="Jane Doe", email="jane@example.org")
        author2 = Author(name="Jane Doe", email="jane@example.org")

        assert author1 == author2


class TestExtractedMetadataDataclass:
    """Tests for the ExtractedMetadata frozen dataclass."""

    @pytest.mark.unit
    def test_minimal_extracted_metadata(self) -> None:
        """ExtractedMetadata can be created with only required fields."""
        from portolan_cli.metadata_extraction import ExtractedMetadata

        metadata = ExtractedMetadata(
            source_url="https://example.com/api/data",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
        )

        assert metadata.source_url == "https://example.com/api/data"
        assert metadata.source_type == "arcgis"
        assert metadata.extraction_date == date(2026, 4, 7)

        # All optional fields should be None by default
        assert metadata.attribution is None
        assert metadata.keywords is None
        assert metadata.contact_name is None
        assert metadata.processing_notes is None
        assert metadata.known_issues is None
        assert metadata.license_hint is None
        assert metadata.authors is None
        assert metadata.citations is None
        assert metadata.doi is None
        assert metadata.related_dois is None
        assert metadata.upstream_version is None
        assert metadata.upstream_version_url is None

    @pytest.mark.unit
    def test_extracted_metadata_with_common_fields(self) -> None:
        """ExtractedMetadata supports commonly extracted fields."""
        from portolan_cli.metadata_extraction import ExtractedMetadata

        metadata = ExtractedMetadata(
            source_url="https://example.com/api/data",
            source_type="socrata",
            extraction_date=date(2026, 4, 7),
            attribution="City of Philadelphia",
            keywords=["census", "demographics", "population"],
            contact_name="Data Team",
            processing_notes="Extracted from OpenDataPhilly",
            known_issues="Missing values in 2020 data",
            license_hint="CC-BY-4.0",
        )

        assert metadata.attribution == "City of Philadelphia"
        assert metadata.keywords == ["census", "demographics", "population"]
        assert metadata.contact_name == "Data Team"
        assert metadata.processing_notes == "Extracted from OpenDataPhilly"
        assert metadata.known_issues == "Missing values in 2020 data"
        assert metadata.license_hint == "CC-BY-4.0"

    @pytest.mark.unit
    def test_extracted_metadata_with_author_support(self) -> None:
        """ExtractedMetadata supports #316 author fields."""
        from portolan_cli.metadata_extraction import Author, ExtractedMetadata

        author1 = Author(name="Alice", orcid="0000-0001-2345-6789")
        author2 = Author(name="Bob", email="bob@example.org")

        metadata = ExtractedMetadata(
            source_url="https://zenodo.org/record/1234567",
            source_type="zenodo",
            extraction_date=date(2026, 4, 7),
            authors=[author1, author2],
            citations=["Smith et al. (2023) Nature 123:456"],
            doi="10.5281/zenodo.1234567",
            related_dois=["10.1234/related.1", "10.1234/related.2"],
            upstream_version="2.1.0",
            upstream_version_url="https://zenodo.org/record/1234567/versions",
        )

        assert len(metadata.authors) == 2
        assert metadata.authors[0].name == "Alice"
        assert metadata.authors[1].email == "bob@example.org"
        assert metadata.citations == ["Smith et al. (2023) Nature 123:456"]
        assert metadata.doi == "10.5281/zenodo.1234567"
        assert metadata.related_dois == ["10.1234/related.1", "10.1234/related.2"]
        assert metadata.upstream_version == "2.1.0"
        assert metadata.upstream_version_url == "https://zenodo.org/record/1234567/versions"

    @pytest.mark.unit
    def test_extracted_metadata_is_frozen(self) -> None:
        """ExtractedMetadata dataclass is frozen (immutable)."""
        from portolan_cli.metadata_extraction import ExtractedMetadata

        metadata = ExtractedMetadata(
            source_url="https://example.com",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
        )

        with pytest.raises(AttributeError):
            metadata.source_url = "https://other.com"  # type: ignore[misc]

    @pytest.mark.unit
    def test_extracted_metadata_equality(self) -> None:
        """Two ExtractedMetadata with same values are equal."""
        from portolan_cli.metadata_extraction import ExtractedMetadata

        metadata1 = ExtractedMetadata(
            source_url="https://example.com",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
            attribution="Test",
        )
        metadata2 = ExtractedMetadata(
            source_url="https://example.com",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
            attribution="Test",
        )

        assert metadata1 == metadata2

    @pytest.mark.unit
    def test_keywords_is_list_not_string(self) -> None:
        """Keywords field is a list of strings, not a single string."""
        from portolan_cli.metadata_extraction import ExtractedMetadata

        metadata = ExtractedMetadata(
            source_url="https://example.com",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
            keywords=["housing", "census"],
        )

        assert isinstance(metadata.keywords, list)
        assert len(metadata.keywords) == 2
        assert "housing" in metadata.keywords


class TestExtractedMetadataHelperMethods:
    """Tests for helper methods on ExtractedMetadata."""

    @pytest.mark.unit
    def test_has_authors_when_populated(self) -> None:
        """has_authors returns True when authors list is populated."""
        from portolan_cli.metadata_extraction import Author, ExtractedMetadata

        metadata = ExtractedMetadata(
            source_url="https://example.com",
            source_type="zenodo",
            extraction_date=date(2026, 4, 7),
            authors=[Author(name="Alice")],
        )

        assert metadata.has_authors() is True

    @pytest.mark.unit
    def test_has_authors_when_empty(self) -> None:
        """has_authors returns False when authors list is empty."""
        from portolan_cli.metadata_extraction import ExtractedMetadata

        metadata = ExtractedMetadata(
            source_url="https://example.com",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
            authors=[],
        )

        assert metadata.has_authors() is False

    @pytest.mark.unit
    def test_has_authors_when_none(self) -> None:
        """has_authors returns False when authors is None."""
        from portolan_cli.metadata_extraction import ExtractedMetadata

        metadata = ExtractedMetadata(
            source_url="https://example.com",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
        )

        assert metadata.has_authors() is False

    @pytest.mark.unit
    def test_has_citations_when_populated(self) -> None:
        """has_citations returns True when citations list is populated."""
        from portolan_cli.metadata_extraction import ExtractedMetadata

        metadata = ExtractedMetadata(
            source_url="https://example.com",
            source_type="zenodo",
            extraction_date=date(2026, 4, 7),
            citations=["Smith (2023)"],
        )

        assert metadata.has_citations() is True

    @pytest.mark.unit
    def test_has_citations_when_none(self) -> None:
        """has_citations returns False when citations is None."""
        from portolan_cli.metadata_extraction import ExtractedMetadata

        metadata = ExtractedMetadata(
            source_url="https://example.com",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
        )

        assert metadata.has_citations() is False
