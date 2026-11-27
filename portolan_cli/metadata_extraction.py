"""Extracted metadata models for metadata seeding."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class ExtractedMetadata:
    """Source-agnostic metadata extracted from a data source.

    Attributes:
        source_type: Identifier for the extraction source (e.g., "arcgis_featureserver").
        source_url: URL of the original data source.
        attribution: Copyright or attribution text.
        description: Human-readable description of the data.
        keywords: List of discovery keywords/tags.
        producer: Organization the source names as the author of the data.
        license_raw: Raw license text (not SPDX identifier).
        license_url: URL of the license text, read out of license_raw.
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

    producer: str | None = None
    """Organization the source names as the author of the data."""

    license_raw: str | None = None
    license_url: str | None = None
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
