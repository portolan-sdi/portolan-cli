"""Metadata extraction abstraction (Wave 1).

This module provides the unified data structures for metadata extracted
from external sources (ArcGIS, Socrata, CKAN, Zenodo, etc.). These
dataclasses serve as a source-agnostic intermediate representation
that can be used to seed .portolan/metadata.yaml files.

Addresses:
- #312: Unified metadata extraction framework
- #316: Author and citation support

Design principles:
- Frozen dataclasses for immutability
- All fields except source_url/source_type/extraction_date are optional
- Ready to map to metadata.yaml structure

Usage:
    from portolan_cli.metadata_extraction import Author, ExtractedMetadata

    # Create from ArcGIS extraction
    metadata = ExtractedMetadata(
        source_url="https://services.arcgis.com/.../FeatureServer",
        source_type="arcgis",
        extraction_date=date.today(),
        attribution="City of Philadelphia",
        keywords=["census", "demographics"],
    )

    # With authors (e.g., from Zenodo)
    metadata = ExtractedMetadata(
        source_url="https://zenodo.org/record/1234567",
        source_type="zenodo",
        extraction_date=date.today(),
        authors=[Author(name="Alice Smith", orcid="0000-0001-2345-6789")],
        doi="10.5281/zenodo.1234567",
    )
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Author:
    """Author information for academic-style attribution.

    Supports ORCID identifiers for unambiguous researcher identification.
    Part of #316 enhancements for citation support.

    Attributes:
        name: Author's full name (required).
        email: Contact email (optional).
        orcid: ORCID identifier in format 0000-0001-2345-6789 (optional).
    """

    name: str
    email: str | None = None
    orcid: str | None = None


@dataclass(frozen=True)
class ExtractedMetadata:
    """Unified metadata extracted from any external source.

    This dataclass represents the common denominator of metadata that can
    be extracted from various data sources. It maps to the fields in
    .portolan/metadata.yaml per ADR-0038.

    **Always populated (required):**
    - source_url: The original data source URL
    - source_type: Extractor identifier (e.g., "arcgis", "socrata", "zenodo")
    - extraction_date: When the extraction occurred

    **Commonly extracted (optional):**
    - attribution: Copyright/attribution text
    - keywords: Discovery tags (list)
    - contact_name: Contact person name (maps to contact.name)
    - processing_notes: How data was processed
    - known_issues: Data quality caveats
    - license_hint: Suggested license (may need SPDX mapping)

    **#316 enhancements (optional):**
    - authors: List of Author dataclasses
    - citations: Academic citation strings
    - doi: Digital Object Identifier
    - related_dois: Related publications
    - upstream_version: Source data version
    - upstream_version_url: URL to version info

    Attributes:
        source_url: The URL of the data source (required).
        source_type: Identifier for the extractor type (required).
        extraction_date: Date when extraction occurred (required).
        attribution: Copyright or attribution text.
        keywords: List of discovery keywords/tags.
        contact_name: Contact person or team name.
        processing_notes: Notes about data processing/transformation.
        known_issues: Known data quality issues or caveats.
        license_hint: Raw license text (may need SPDX mapping).
        authors: List of Author dataclasses for academic attribution.
        citations: List of citation strings.
        doi: Digital Object Identifier for the dataset.
        related_dois: List of related DOIs (publications, datasets).
        upstream_version: Version string from source.
        upstream_version_url: URL to version history/changelog.
    """

    # Always populated (required)
    source_url: str
    source_type: str
    extraction_date: date

    # Commonly extracted (optional)
    attribution: str | None = None
    keywords: list[str] | None = None
    contact_name: str | None = None
    processing_notes: str | None = None
    known_issues: str | None = None
    license_hint: str | None = None

    # #316 enhancements (optional)
    authors: list[Author] | None = None
    citations: list[str] | None = None
    doi: str | None = None
    related_dois: list[str] | None = None
    upstream_version: str | None = None
    upstream_version_url: str | None = None

    def has_authors(self) -> bool:
        """Check if authors list is populated and non-empty.

        Returns:
            True if authors is a non-empty list, False otherwise.
        """
        return bool(self.authors)

    def has_citations(self) -> bool:
        """Check if citations list is populated and non-empty.

        Returns:
            True if citations is a non-empty list, False otherwise.
        """
        return bool(self.citations)
