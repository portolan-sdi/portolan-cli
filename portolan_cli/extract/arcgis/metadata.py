"""ArcGIS metadata extraction and mapping.

This module provides metadata extraction from ArcGIS REST API responses for the
`portolan extract arcgis` command. It maps ArcGIS service info fields to the
metadata.yaml structure used by Portolan.

Mapping (per extract-arcgis-design.md):
    | ArcGIS Field         | metadata.yaml Field  |
    |----------------------|----------------------|
    | Service URL          | source_url           |
    | copyrightText        | attribution          |
    | description          | (STAC description)   |
    | serviceDescription   | processing_notes     |
    | documentInfo.Author  | contact.name         |
    | documentInfo.Keywords| keywords (list)      |
    | accessInformation    | known_issues         |
    | licenseInfo          | license_info_raw     |

Usage:
    from portolan_cli.extract.arcgis.metadata import (
        ArcGISMetadata,
        extract_arcgis_metadata,
    )

    service_info = {"copyrightText": "City of Philadelphia", ...}
    metadata = extract_arcgis_metadata(
        service_info,
        source_url="https://services.arcgis.com/.../FeatureServer"
    )
    print(metadata.attribution)  # "City of Philadelphia"
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


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

    contact_name: str | None
    """Author name from documentInfo.Author."""

    keywords: list[str] | None
    """Keywords from documentInfo.Keywords (comma-separated -> list)."""

    known_issues: str | None
    """Access restrictions/caveats from accessInformation."""

    license_info_raw: str | None
    """Raw license text (not SPDX mapped) from licenseInfo."""


def extract_arcgis_metadata(
    service_info: dict[str, Any],
    source_url: str,
) -> ArcGISMetadata:
    """Extract metadata from ArcGIS service info.

    Args:
        service_info: The JSON response from the ArcGIS REST API service endpoint.
        source_url: The URL of the ArcGIS service (required).

    Returns:
        ArcGISMetadata dataclass with extracted fields. Missing or empty fields
        are returned as None.

    Note:
        - Empty strings and whitespace-only strings are converted to None
        - Non-string values (numbers, lists, etc.) are ignored and return None
        - Keywords are split on commas and trimmed of whitespace
        - HTML content in description fields is preserved
    """
    document_info = _get_document_info(service_info)

    return ArcGISMetadata(
        source_url=source_url,
        attribution=_extract_string(service_info, "copyrightText"),
        description=_extract_string(service_info, "description"),
        processing_notes=_extract_string(service_info, "serviceDescription"),
        contact_name=_extract_string(document_info, "Author") if document_info else None,
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
    """Extract a string value, returning None for empty/invalid values.

    Args:
        data: Dictionary to extract from.
        key: Key to look up.

    Returns:
        The string value, or None if:
        - Key doesn't exist
        - Value is not a string
        - Value is empty or whitespace-only
    """
    value = data.get(key)
    if not isinstance(value, str):
        return None

    stripped = value.strip()
    return stripped if stripped else None


def _extract_keywords(document_info: dict[str, Any]) -> list[str] | None:
    """Extract keywords from documentInfo.Keywords.

    The Keywords field is a comma-separated string that gets split into a list.
    Empty entries and whitespace are removed.

    Args:
        document_info: The documentInfo dict.

    Returns:
        List of keywords, or None if:
        - Keywords field doesn't exist
        - Keywords field is not a string
        - All keywords are empty after splitting and trimming
    """
    keywords_raw = document_info.get("Keywords")
    if not isinstance(keywords_raw, str):
        return None

    # Split on comma and trim whitespace
    keywords = [kw.strip() for kw in keywords_raw.split(",")]

    # Filter out empty strings
    keywords = [kw for kw in keywords if kw]

    return keywords if keywords else None
