"""CSW metadata client.

This module provides functions for fetching ISO 19139 metadata from
various sources (CSW GetRecordById, static XML files, etc.) and
parsing them into ISOMetadata objects.

The main entry point is fetch_metadata_for_layer() which handles
the metadataUrls list from OWSLib layer objects.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import requests  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from portolan_cli.extract.csw.models import ISOMetadata

logger = logging.getLogger(__name__)

# Default timeout for metadata fetches (seconds)
DEFAULT_TIMEOUT = 30.0


def fetch_metadata_record(
    url: str,
    timeout: float = DEFAULT_TIMEOUT,
) -> ISOMetadata | None:
    """Fetch and parse ISO 19139 metadata from a URL.

    Args:
        url: URL to fetch (CSW GetRecordById or static XML).
        timeout: Request timeout in seconds.

    Returns:
        Parsed ISOMetadata, or None if fetch/parse fails.
    """
    from portolan_cli.extract.csw.iso_parser import ISOParseError, parse_iso19139

    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        xml_content = response.text

        return parse_iso19139(xml_content)

    except requests.exceptions.RequestException as e:
        logger.debug("CSW fetch failed for %s: %s", url, e)
        return None
    except ISOParseError as e:
        logger.debug("ISO 19139 parse failed for %s: %s", url, e)
        return None


def detect_metadata_url_type(url: str) -> str:
    """Detect the type of metadata URL.

    Args:
        url: Metadata URL to analyze.

    Returns:
        One of: "csw", "xml", "html", "geonetwork_api", "unknown"
    """
    url_lower = url.lower()

    # CSW GetRecordById
    if "getrecordbyid" in url_lower and "csw" in url_lower:
        return "csw"

    # Static XML file
    if url_lower.endswith(".xml"):
        return "xml"

    # HTML page
    if url_lower.endswith(".html") or url_lower.endswith(".htm"):
        return "html"

    # GeoNetwork REST API
    if "/geonetwork/srv/api/records/" in url_lower:
        return "geonetwork_api"

    return "unknown"


def is_metadata_url_supported(url: str) -> bool:
    """Check if a metadata URL type is supported for fetching.

    Args:
        url: Metadata URL to check.

    Returns:
        True if we can fetch and parse this URL type.
    """
    url_type = detect_metadata_url_type(url)
    return url_type in ("csw", "xml")


def fetch_metadata_for_layer(
    metadata_urls: list[dict[str, Any]] | None,
    timeout: float = DEFAULT_TIMEOUT,
) -> ISOMetadata | None:
    """Fetch metadata from a layer's metadataUrls list.

    Tries each supported URL in order until one succeeds.

    Args:
        metadata_urls: List of metadata URL dicts from OWSLib layer.
            Each dict should have a "url" key.
        timeout: Request timeout in seconds.

    Returns:
        Parsed ISOMetadata from first successful fetch, or None.
    """
    if not metadata_urls:
        return None

    for url_info in metadata_urls:
        url = url_info.get("url")
        if not url:
            continue

        if not is_metadata_url_supported(url):
            continue

        metadata = fetch_metadata_record(url, timeout=timeout)
        if metadata is not None:
            return metadata

    return None
