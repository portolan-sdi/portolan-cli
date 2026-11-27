"""ArcGIS metadata extraction and mapping."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from portolan_cli.metadata_extraction import ExtractedMetadata


@dataclass(frozen=True)
class ArcGISMetadata:
    """Extracted metadata from ArcGIS REST API.

    All fields except source_url may be None if not present in the service info.
    """

    source_url: str
    """The ArcGIS service URL (always populated)."""

    attribution: str | None
    """Copyright/attribution text from copyrightText."""

    description: str | None
    """Service description (maps to STAC description)."""

    processing_notes: str | None
    """Additional context from serviceDescription."""

    producer: str | None
    """Author name from documentInfo.Author. A producer, never the contact."""

    keywords: list[str] | None
    """Keywords from documentInfo.Keywords (comma-separated -> list)."""

    known_issues: str | None
    """Access restrictions/caveats from accessInformation."""

    license_info_raw: str | None
    """Raw license text (not SPDX mapped) from licenseInfo."""

    def to_extracted(self) -> ExtractedMetadata:
        """Convert to source-agnostic ExtractedMetadata."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_core.licensing import license_url_from_text

        return ExtractedMetadata(
            source_type="arcgis_featureserver",
            source_url=self.source_url,
            attribution=self.attribution,
            description=self.description,
            keywords=self.keywords,
            producer=self.producer,
            license_raw=self.license_info_raw,
            license_url=license_url_from_text(self.license_info_raw),
            processing_notes=self.processing_notes,
            known_issues=self.known_issues,
        )


def extract_arcgis_metadata(
    service_info: dict[str, Any],
    source_url: str,
) -> ArcGISMetadata:
    """Extract metadata from ArcGIS service info."""
    document_info = _get_document_info(service_info)

    return ArcGISMetadata(
        source_url=source_url,
        attribution=_extract_string(service_info, "copyrightText"),
        description=_extract_string(service_info, "description"),
        processing_notes=_extract_string(service_info, "serviceDescription"),
        producer=_extract_string(document_info, "Author") if document_info else None,
        keywords=_extract_keywords(document_info) if document_info else None,
        known_issues=_extract_string(service_info, "accessInformation"),
        license_info_raw=_extract_string(service_info, "licenseInfo"),
    )


def _get_document_info(service_info: dict[str, Any]) -> dict[str, Any] | None:
    """Extract documentInfo dict if present and valid."""
    doc_info = service_info.get("documentInfo")
    if isinstance(doc_info, dict):
        return doc_info
    return None


def _extract_string(data: dict[str, Any], key: str) -> str | None:
    """Extract a string value, returning None for empty/invalid values."""
    value = data.get(key)
    if not isinstance(value, str):
        return None

    stripped = value.strip()
    return stripped or None


def _extract_keywords(document_info: dict[str, Any]) -> list[str] | None:
    """Extract keywords from documentInfo.Keywords."""
    keywords_raw = document_info.get("Keywords")
    if not isinstance(keywords_raw, str):
        return None

    # Split on comma and trim whitespace
    keywords = [kw.strip() for kw in keywords_raw.split(",")]

    # Filter out empty strings
    keywords = [kw for kw in keywords if kw]

    return keywords or None
