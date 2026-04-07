"""Canonical metadata extraction types for Portolan.

This module provides the canonical `ExtractedMetadata` dataclass that all
format-specific extractors (ArcGIS, GeoJSON, Shapefile, etc.) convert to.
This enables consistent metadata.yaml seeding across all data sources.

Usage:
    from portolan_cli.metadata_extraction import ExtractedMetadata, Author

    # From ArcGIS extractor
    metadata = arcgis_metadata.to_extracted()

    # From other extractors (future)
    metadata = geojson_metadata.to_extracted()
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Author:
    """Contact/author information for metadata attribution.

    Used when more than just a name is available (e.g., email, organization).
    """

    name: str | None = None
    """Author/contact name."""

    email: str | None = None
    """Contact email address."""

    organization: str | None = None
    """Organization name."""


@dataclass(frozen=True)
class ExtractedMetadata:
    """Canonical metadata extracted from any data source.

    This is the target type for all format-specific extractors. It maps to
    the metadata.yaml structure used by Portolan for STAC enrichment.

    All fields except source_url and source_type may be None if not available.
    """

    source_url: str
    """URL of the original data source (always populated)."""

    source_type: str
    """Type identifier for the source (e.g., 'arcgis_featureserver')."""

    attribution: str | None = None
    """Copyright/attribution text for data credit."""

    keywords: list[str] | None = None
    """Keywords/tags describing the data."""

    contact_name: str | None = None
    """Simple contact name (use author for richer contact info)."""

    author: Author | None = None
    """Rich author/contact information (name, email, org)."""

    processing_notes: str | None = None
    """Notes about data processing, update frequency, etc."""

    known_issues: str | None = None
    """Known data quality issues or access restrictions."""

    license_hint: str | None = None
    """Raw license text (not yet mapped to SPDX)."""
