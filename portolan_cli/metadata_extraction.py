"""Extracted metadata models for metadata seeding.

This module defines source-agnostic metadata models that can be populated from
various extraction sources (ArcGIS, GDAL, etc.) and used to seed metadata.yaml.

The ExtractedMetadata class serves as an intermediate representation between
source-specific metadata extractors and the metadata.yaml seeding process.

Usage:
    from portolan_cli.metadata_extraction import ExtractedMetadata

    # Create from extracted data
    extracted = ExtractedMetadata(
        source_type="arcgis_featureserver",
        source_url="https://services.arcgis.com/.../FeatureServer",
        attribution="City of Philadelphia",
        contact_name="GIS Team",
        keywords=["census", "demographics"],
    )

    # Use with seeding
    from portolan_cli.metadata_seeding import seed_metadata_yaml
    seed_metadata_yaml(extracted, Path(".portolan/metadata.yaml"))
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class ExtractedMetadata:
    """Source-agnostic metadata extracted from a data source.

    This class holds metadata that can be extracted from various sources
    (ArcGIS services, GDAL files, etc.) and used to seed metadata.yaml.

    All fields except source_type may be None if not available from the source.
    The seeding process will convert None values to TODO placeholders for
    required fields.

    Attributes:
        source_type: Identifier for the extraction source (e.g., "arcgis_featureserver").
        source_url: URL of the original data source.
        attribution: Copyright or attribution text.
        description: Human-readable description of the data.
        keywords: List of discovery keywords/tags.
        contact_name: Name of the data maintainer/author.
        contact_email: Email of the data maintainer (rarely available).
        license_raw: Raw license text (not SPDX identifier).
        processing_notes: Notes about data processing or updates.
        known_issues: Known limitations or caveats.
    """

    source_type: str
    """Identifier for the extraction source (e.g., 'arcgis_featureserver')."""

    source_url: str | None = None
    """URL of the original data source."""

    attribution: str | None = None
    """Copyright or attribution text."""

    description: str | None = None
    """Human-readable description of the data."""

    keywords: list[str] | None = None
    """List of discovery keywords/tags."""

    contact_name: str | None = None
    """Name of the data maintainer/author."""

    contact_email: str | None = None
    """Email of the data maintainer (rarely available from extraction)."""

    license_raw: str | None = None
    """Raw license text (not SPDX identifier, for human review)."""

    processing_notes: str | None = None
    """Notes about data processing or update frequency."""

    known_issues: str | None = None
    """Known limitations, caveats, or access restrictions."""

    def to_dict(self) -> dict[str, object]:
        """Convert to a dictionary, excluding None values.

        Returns:
            Dictionary with non-None fields.
        """
        return {k: v for k, v in asdict(self).items() if v is not None}
