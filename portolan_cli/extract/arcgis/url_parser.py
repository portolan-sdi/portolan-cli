"""ArcGIS REST URL parser.

Parses ArcGIS REST URLs to determine:
- URL type (FeatureServer, MapServer, or services root)
- Service name (for default output directory naming)
- Layer ID (if a specific layer is targeted)

Per design doc (context/shared/plans/extract-arcgis-design.md):
- `*/FeatureServer` or `*/MapServer` -> single service extraction
- `*/rest/services` -> multi-service discovery and extraction
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from urllib.parse import urlparse

from portolan_cli.errors import PortolanError


class ArcGISURLType(Enum):
    """Type of ArcGIS REST endpoint."""

    FEATURE_SERVER = "FeatureServer"
    MAP_SERVER = "MapServer"
    SERVICES_ROOT = "services"


class InvalidArcGISURLError(PortolanError):
    """Raised when a URL is not a valid ArcGIS REST endpoint.

    Error code: PRTLN-EXT001
    """

    code = "PRTLN-EXT001"

    def __init__(self, url: str, reason: str) -> None:
        super().__init__(
            f"Invalid ArcGIS URL '{url}': {reason}",
            url=url,
            reason=reason,
        )


@dataclass(frozen=True)
class ParsedArcGISURL:
    """Result of parsing an ArcGIS REST URL.

    Attributes:
        url_type: Type of endpoint (FeatureServer, MapServer, or services root)
        base_url: URL without layer ID or query parameters
        service_name: Extracted service name (None for services root)
        layer_id: Layer ID if specified in URL (None otherwise)
    """

    url_type: ArcGISURLType
    base_url: str
    service_name: str | None
    layer_id: int | None

    @property
    def is_single_service(self) -> bool:
        """Whether this URL targets a single service (vs services root)."""
        return self.url_type != ArcGISURLType.SERVICES_ROOT

    @property
    def service_endpoint_name(self) -> str | None:
        """Last segment of service name (for directory naming).

        For "Demographics/Census2020", returns "Census2020".
        For "Census", returns "Census".
        For services root, returns None.
        """
        if self.service_name is None:
            return None
        # Return last segment of the path
        return self.service_name.rsplit("/", 1)[-1]


# Regex patterns for URL parsing
# Match: /rest/services/ServiceName/FeatureServer or /rest/services/Folder/Service/MapServer
_SERVICE_PATTERN = re.compile(
    r"/rest/services/(.+?)/(FeatureServer|MapServer)(?:/(\d+))?",
    re.IGNORECASE,
)

# Match: /rest/services at the end (services root)
_SERVICES_ROOT_PATTERN = re.compile(
    r"/rest/services/?$",
    re.IGNORECASE,
)

# Match ImageServer (to give helpful error)
_IMAGE_SERVER_PATTERN = re.compile(
    r"/ImageServer",
    re.IGNORECASE,
)


def parse_arcgis_url(url: str) -> ParsedArcGISURL:
    """Parse an ArcGIS REST URL to determine type and extract metadata.

    Args:
        url: ArcGIS REST URL (FeatureServer, MapServer, or services root)

    Returns:
        ParsedArcGISURL with type, base URL, service name, and optional layer ID

    Raises:
        InvalidArcGISURLError: If URL is not a recognized ArcGIS REST endpoint

    Examples:
        >>> result = parse_arcgis_url("https://example.com/rest/services/Census/FeatureServer")
        >>> result.url_type
        <ArcGISURLType.FEATURE_SERVER: 'FeatureServer'>
        >>> result.service_name
        'Census'

        >>> result = parse_arcgis_url("https://example.com/rest/services")
        >>> result.url_type
        <ArcGISURLType.SERVICES_ROOT: 'services'>
    """
    if not url:
        raise InvalidArcGISURLError(url, "URL cannot be empty")

    # Validate URL structure
    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            raise InvalidArcGISURLError(url, "not a valid URL")
    except ValueError as e:
        raise InvalidArcGISURLError(url, f"malformed URL: {e}") from e

    # Strip query parameters for pattern matching
    url_path = url.split("?")[0]

    # Check for ImageServer (not supported - raster out of scope)
    if _IMAGE_SERVER_PATTERN.search(url_path):
        raise InvalidArcGISURLError(
            url,
            "ImageServer (raster) is not supported; only FeatureServer and MapServer are supported",
        )

    # Try to match FeatureServer or MapServer
    match = _SERVICE_PATTERN.search(url_path)
    if match:
        service_name = match.group(1)
        server_type = match.group(2)
        layer_id_str = match.group(3)

        # Determine URL type
        if server_type.lower() == "featureserver":
            url_type = ArcGISURLType.FEATURE_SERVER
        else:
            url_type = ArcGISURLType.MAP_SERVER

        # Parse layer ID if present
        layer_id = int(layer_id_str) if layer_id_str else None

        # Build base URL (without layer ID or query params)
        # Find where the server type ends
        server_end = url_path.lower().find(server_type.lower()) + len(server_type)
        base_url = url_path[:server_end]

        # Normalize trailing slash
        base_url = base_url.rstrip("/")

        return ParsedArcGISURL(
            url_type=url_type,
            base_url=base_url,
            service_name=service_name,
            layer_id=layer_id,
        )

    # Try to match services root
    if _SERVICES_ROOT_PATTERN.search(url_path):
        # Normalize URL: strip trailing slash and query params
        base_url = url_path.rstrip("/")

        return ParsedArcGISURL(
            url_type=ArcGISURLType.SERVICES_ROOT,
            base_url=base_url,
            service_name=None,
            layer_id=None,
        )

    # No match - not a recognized ArcGIS URL
    raise InvalidArcGISURLError(
        url,
        "not a recognized ArcGIS REST URL; expected FeatureServer, MapServer, or rest/services",
    )
