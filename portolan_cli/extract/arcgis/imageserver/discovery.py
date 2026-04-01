"""ArcGIS ImageServer service discovery.

This module fetches raster service metadata from ArcGIS ImageServer REST API
endpoints. It's the first step in raster extraction, providing information
about band count, pixel type, extent, and resolution.

The discovery client uses httpx for HTTP requests, following the same
pattern as the FeatureServer discovery module.

Typical usage:
    # Discover metadata from an ImageServer
    metadata = discover_imageserver("https://services.arcgis.com/.../ImageServer")
    print(f"Bands: {metadata.band_count}, Type: {metadata.pixel_type}")
    print(f"Resolution: {metadata.pixel_size_x}m x {metadata.pixel_size_y}m")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, cast
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx


class ImageServerDiscoveryError(Exception):
    """Error during ArcGIS ImageServer discovery.

    Raised when:
    - HTTP request fails (connection, timeout, status errors)
    - Response is not valid JSON
    - Required fields are missing from response
    - ArcGIS returns an error response (e.g., authentication required)
    """

    pass


@dataclass
class ImageServerMetadata:
    """Metadata from an ArcGIS ImageServer service.

    Contains raster service information needed for extraction planning:
    band count, pixel type, resolution, extent, and capabilities.

    Attributes:
        name: Service name
        band_count: Number of bands in the raster
        pixel_type: Pixel data type (e.g., U8, U16, F32, F64)
        pixel_size_x: Pixel width in the service's coordinate system units
        pixel_size_y: Pixel height in the service's coordinate system units
        full_extent: Bounding box with spatial reference
            {xmin, ymin, xmax, ymax, spatialReference: {wkid, latestWkid?}}
        max_image_width: Maximum image width that can be requested
        max_image_height: Maximum image height that can be requested
        capabilities: List of service capabilities (e.g., Image, Metadata, Catalog)
        description: Optional service description
        copyright_text: Optional copyright information
        service_data_type: Optional service data type classification
    """

    name: str
    band_count: int
    pixel_type: str
    pixel_size_x: float
    pixel_size_y: float
    full_extent: dict[str, Any]
    max_image_width: int
    max_image_height: int
    capabilities: list[str] = field(default_factory=list)
    description: str | None = None
    copyright_text: str | None = None
    service_data_type: str | None = None

    def get_crs_string(self) -> str:
        """Get CRS as EPSG string from spatial reference.

        Returns:
            CRS string like 'EPSG:4326' or 'EPSG:102719'
        """
        sr = self.full_extent.get("spatialReference", {})
        wkid = sr.get("latestWkid") or sr.get("wkid")
        if wkid:
            return f"EPSG:{wkid}"
        return "EPSG:4326"  # Default fallback

    def get_bbox_tuple(self) -> tuple[float, float, float, float]:
        """Get extent as (minx, miny, maxx, maxy) tuple.

        Returns:
            Bounding box tuple (xmin, ymin, xmax, ymax).

        Raises:
            ValueError: If full_extent is missing required keys or values are not numeric.
        """
        required_keys = ("xmin", "ymin", "xmax", "ymax")
        missing = [k for k in required_keys if k not in self.full_extent]
        if missing:
            raise ValueError(
                f"full_extent missing required keys {missing}. Got: {self.full_extent}"
            )

        try:
            return (
                float(self.full_extent["xmin"]),
                float(self.full_extent["ymin"]),
                float(self.full_extent["xmax"]),
                float(self.full_extent["ymax"]),
            )
        except (TypeError, ValueError) as e:
            raise ValueError(f"full_extent values must be numeric. Got: {self.full_extent}") from e


def _ensure_json_format(url: str) -> str:
    """Ensure URL has f=json parameter for ArcGIS REST API.

    Args:
        url: Original URL

    Returns:
        URL with f=json parameter added if not present
    """
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)

    # Check if f parameter already exists
    if "f" not in query_params:
        query_params["f"] = ["json"]

    # Rebuild query string
    new_query = urlencode(query_params, doseq=True)
    new_parsed = parsed._replace(query=new_query)

    return urlunparse(new_parsed)


def _parse_capabilities(capabilities_value: str | list[str] | None) -> list[str]:
    """Parse capabilities from various formats.

    ArcGIS returns capabilities as a comma-separated string.
    Handle missing, empty, or already-parsed values.

    Args:
        capabilities_value: Raw capabilities from response

    Returns:
        List of capability strings
    """
    if not capabilities_value:
        return []

    if isinstance(capabilities_value, list):
        return capabilities_value

    # Comma-separated string
    return [cap.strip() for cap in capabilities_value.split(",") if cap.strip()]


async def _fetch_json(url: str, timeout: float) -> dict[str, Any]:
    """Fetch JSON from URL with standard error handling.

    Args:
        url: URL to fetch (will have f=json added if needed)
        timeout: Request timeout in seconds

    Returns:
        Parsed JSON response

    Raises:
        ImageServerDiscoveryError: On HTTP or parsing errors
    """
    request_url = _ensure_json_format(url)

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(request_url)
            response.raise_for_status()
            return cast(dict[str, Any], response.json())
    except httpx.HTTPStatusError as e:
        msg = f"Failed to fetch from {url}: HTTP {e.response.status_code}"
        raise ImageServerDiscoveryError(msg) from e
    except httpx.ConnectError as e:
        msg = f"Failed to connect to {url}: {e}"
        raise ImageServerDiscoveryError(msg) from e
    except httpx.TimeoutException as e:
        msg = f"Request timed out for {url}: {e}"
        raise ImageServerDiscoveryError(msg) from e
    except httpx.RequestError as e:
        msg = f"Failed to fetch from {url}: {e}"
        raise ImageServerDiscoveryError(msg) from e
    except ValueError as e:
        msg = f"Invalid JSON response from {url}: {e}"
        raise ImageServerDiscoveryError(msg) from e


def _check_arcgis_error(data: dict[str, Any], url: str) -> None:
    """Check for ArcGIS error response and raise if found.

    ArcGIS returns errors as JSON with an 'error' key containing
    code, message, and details.

    Args:
        data: Parsed JSON response
        url: Original URL (for error message)

    Raises:
        ImageServerDiscoveryError: If response contains an error
    """
    if "error" in data:
        error_info = data["error"]
        code = error_info.get("code", "unknown")
        message = error_info.get("message", "Unknown error")
        raise ImageServerDiscoveryError(f"ArcGIS error ({code}): {message}")


def _validate_required_fields(data: dict[str, Any], url: str) -> None:
    """Validate that all required fields are present.

    Args:
        data: Parsed JSON response
        url: Original URL (for error message)

    Raises:
        ImageServerDiscoveryError: If required field is missing
    """
    required_fields = [
        "name",
        "bandCount",
        "pixelType",
        "pixelSizeX",
        "pixelSizeY",
        "extent",
        "maxImageHeight",
        "maxImageWidth",
    ]

    for field_name in required_fields:
        if field_name not in data:
            raise ImageServerDiscoveryError(
                f"Missing required field '{field_name}' in ImageServer response from {url}"
            )


def parse_imageserver_response(data: dict[str, Any]) -> ImageServerMetadata:
    """Parse ImageServer metadata from a JSON response.

    Use this when you already have the JSON response (e.g., from a file
    or mocked response). For fetching from a live service, use
    `discover_imageserver()` instead.

    Args:
        data: Parsed JSON response from ImageServer REST API

    Returns:
        ImageServerMetadata with service information

    Raises:
        ImageServerDiscoveryError: If required fields are missing or
            response contains an error
    """
    # Check for ArcGIS error response
    _check_arcgis_error(data, "<parsed response>")

    # Validate required fields
    _validate_required_fields(data, "<parsed response>")

    # Parse capabilities
    capabilities = _parse_capabilities(data.get("capabilities"))

    return ImageServerMetadata(
        name=data["name"],
        band_count=data["bandCount"],
        pixel_type=data["pixelType"],
        pixel_size_x=float(data["pixelSizeX"]),
        pixel_size_y=float(data["pixelSizeY"]),
        full_extent=data["extent"],
        max_image_width=data["maxImageWidth"],
        max_image_height=data["maxImageHeight"],
        capabilities=capabilities,
        description=data.get("description"),
        copyright_text=data.get("copyrightText"),
        service_data_type=data.get("serviceDataType"),
    )


async def discover_imageserver(
    url: str,
    *,
    timeout: float = 60.0,
) -> ImageServerMetadata:
    """Discover metadata from an ArcGIS ImageServer.

    Fetches service metadata from the ArcGIS REST API and extracts
    information about the raster data: band count, pixel type,
    resolution, extent, and capabilities.

    This is the primary entry point for live service discovery. It fetches
    JSON from the URL and delegates parsing to parse_imageserver_response().

    Args:
        url: ImageServer URL (e.g., "https://services.arcgis.com/.../ImageServer")
        timeout: Request timeout in seconds

    Returns:
        ImageServerMetadata with service information

    Raises:
        ImageServerDiscoveryError: If request fails, response is invalid,
            or required fields are missing
    """
    data = await _fetch_json(url, timeout=timeout)

    # Delegate to parse_imageserver_response for validation and parsing
    # Note: parse_imageserver_response uses "<parsed response>" for error context,
    # but the URL context is already captured in _fetch_json errors
    return parse_imageserver_response(data)
