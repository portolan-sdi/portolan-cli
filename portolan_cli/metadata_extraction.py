"""Canonical metadata extraction models for metadata.yaml seeding.

This module defines the ExtractedMetadata dataclass - a unified structure
for harvested metadata that can be converted to metadata.yaml format.

Different extraction sources (FeatureServer, ImageServer) each produce
their own metadata models, then convert to ExtractedMetadata via
.to_extracted() methods for consistent downstream processing.

See ADR-0038 for the metadata.yaml enrichment strategy.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Author:
    """Contact information for a metadata author.

    Attributes:
        name: Full name of the author/contact.
        email: Optional email address.
        organization: Optional organization name.
    """

    name: str
    email: str | None = None
    organization: str | None = None


@dataclass
class ExtractedMetadata:
    """Canonical metadata harvested from external sources.

    This structure maps to metadata.yaml fields for human enrichment.
    All fields except source_url and source_type are optional - null
    indicates the field was checked but not present in the source.

    Attributes:
        source_url: URL of the original data source.
        source_type: Type of source (e.g., "arcgis_featureserver", "arcgis_imageserver").
        title: Human-readable title for the data.
        description: Detailed description of the data.
        attribution: Copyright/attribution text.
        keywords: List of keywords/tags.
        contact_name: Name of the contact person.
        contact_email: Email of the contact person.
        processing_notes: Technical notes about data processing.
        known_issues: Known data quality issues or caveats.
        license_hint: Raw license text (not yet SPDX-mapped).
        temporal_extent_start: Start date of temporal coverage (ISO 8601).
        temporal_extent_end: End date of temporal coverage (ISO 8601).
    """

    source_url: str
    source_type: str
    title: str | None = None
    description: str | None = None
    attribution: str | None = None
    keywords: list[str] | None = None
    contact_name: str | None = None
    contact_email: str | None = None
    processing_notes: str | None = None
    known_issues: str | None = None
    license_hint: str | None = None
    temporal_extent_start: str | None = None
    temporal_extent_end: str | None = None
