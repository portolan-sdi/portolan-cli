"""WFS metadata extraction.

This module extracts metadata from WFS GetCapabilities responses
and converts it to Portolan's ExtractedMetadata format for seeding
metadata.yaml files.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from portolan_cli.metadata_extraction import ExtractedMetadata


# Patterns that indicate software-generated boilerplate, not real descriptions
_BOILERPLATE_PATTERNS = [
    "reference implementation",
    "supports all wfs operations",
    "geoserver",  # Any mention of GeoServer is boilerplate
    "mapserver",  # Any mention of MapServer is boilerplate
    "deegree",
    "qgis server",
    "arcgis server",
    "web feature service",  # Generic "Web Feature Service" title
]


def is_boilerplate_description(text: str | None) -> bool:
    """Check if description is software boilerplate rather than real content.

    Args:
        text: Description text to check.

    Returns:
        True if text matches known boilerplate patterns.
    """
    if not text:
        return False

    lower = text.lower()
    return any(pattern in lower for pattern in _BOILERPLATE_PATTERNS)


@dataclass
class WFSMetadata:
    """Metadata extracted from a WFS service.

    Attributes:
        source_url: The WFS service URL.
        service_title: Service title from GetCapabilities.
        service_abstract: Service description from GetCapabilities.
        provider_name: Provider/organization name.
        keywords: Keywords from GetCapabilities.
        fees: Fee information (often "None").
        access_constraints: Access constraints or license info.
    """

    source_url: str
    service_title: str | None = None
    service_abstract: str | None = None
    provider_name: str | None = None
    keywords: list[str] | None = None
    fees: str | None = None
    access_constraints: str | None = None

    @property
    def description(self) -> str | None:
        """Alias for service_abstract (common field name)."""
        return self.service_abstract

    @property
    def attribution(self) -> str | None:
        """Alias for provider_name (common field name)."""
        return self.provider_name

    @property
    def license_info_raw(self) -> str | None:
        """Combined license info from fees and access_constraints."""
        parts = []
        if self.fees and self.fees.lower() not in ("none", "no fee", ""):
            parts.append(f"Fees: {self.fees}")
        if self.access_constraints:
            parts.append(self.access_constraints)
        return "; ".join(parts) if parts else None

    def to_extracted(self) -> ExtractedMetadata:
        """Convert to common ExtractedMetadata format.

        Uses service_title as fallback when service_abstract is
        boilerplate (e.g., GeoServer default text) or missing.
        If both are boilerplate, description is None.

        Returns:
            ExtractedMetadata for use with metadata seeding.
        """
        from portolan_cli.metadata_extraction import ExtractedMetadata

        # Use abstract unless it's boilerplate, then fall back to title
        description = self.service_abstract
        if is_boilerplate_description(description) or not description:
            # Only use title if it's not also boilerplate
            if self.service_title and not is_boilerplate_description(self.service_title):
                description = self.service_title
            else:
                description = None

        return ExtractedMetadata(
            source_type="wfs",
            source_url=self.source_url,
            description=description,
            attribution=self.provider_name,
            keywords=self.keywords,
            contact_name=None,  # WFS doesn't typically expose contact
            processing_notes=None,
            known_issues=None,
            license_raw=self.license_info_raw,
        )


def extract_wfs_metadata(
    capabilities: dict[str, Any],
    source_url: str,
) -> WFSMetadata:
    """Extract metadata from WFS capabilities dict.

    Args:
        capabilities: Parsed GetCapabilities response.
        source_url: The WFS service URL.

    Returns:
        WFSMetadata with extracted fields.
    """
    return WFSMetadata(
        source_url=source_url,
        service_title=capabilities.get("service_title"),
        service_abstract=capabilities.get("service_abstract"),
        provider_name=capabilities.get("provider_name"),
        keywords=capabilities.get("keywords"),
        fees=capabilities.get("fees"),
        access_constraints=capabilities.get("access_constraints"),
    )
