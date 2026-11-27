"""ArcGIS ImageServer service discovery."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, cast

import httpx

from portolan_cli.extract.arcgis.discovery import ensure_json_format
from portolan_cli.extract.arcgis.imageserver.tilecache import (
    TileCacheInfo,
    export_image_supported,
    parse_tile_info,
)
from portolan_cli.extract.common.urls import redact_credentials

logger = logging.getLogger(__name__)

# ArcGIS Server defaults for a service JSON that omits maxImageHeight and maxImageWidth,
# as hosted tiled imagery layers on ArcGIS Online do.
DEFAULT_MAX_IMAGE_HEIGHT = 4100
DEFAULT_MAX_IMAGE_WIDTH = 15000


class ImageServerDiscoveryError(Exception):
    """Error during ArcGIS ImageServer discovery."""


@dataclass
class ImageServerMetadata:
    """Metadata from an ArcGIS ImageServer service."""

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
    # FeatureServer-parity fields
    service_description: str | None = None
    author: str | None = None
    keywords: list[str] | None = None
    license_info: str | None = None
    access_information: str | None = None
    tile_cache: TileCacheInfo | None = None

    @property
    def export_image_supported(self) -> bool:
        """Report whether the service accepts exportImage requests."""
        return export_image_supported(self.capabilities)

    def get_crs_string(self) -> str:
        """Get CRS as EPSG string from spatial reference."""
        import logging

        sr = self.full_extent.get("spatialReference", {})
        wkid = sr.get("latestWkid") or sr.get("wkid")
        if wkid:
            return f"EPSG:{wkid}"

        # Log warning when falling back to WGS84
        logger = logging.getLogger(__name__)
        logger.warning(
            "Could not determine CRS from ImageServer metadata for '%s'. "
            "Falling back to EPSG:4326 (WGS84). Coordinates may be incorrect "
            "if the service uses a different CRS.",
            self.name,
        )
        return "EPSG:4326"  # Default fallback

    def get_bbox_tuple(self) -> tuple[float, float, float, float]:
        """Get extent as (minx, miny, maxx, maxy) tuple."""
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


def _parse_capabilities(capabilities_value: str | list[str] | None) -> list[str]:
    """Parse capabilities from various formats."""
    if not capabilities_value:
        return []

    if isinstance(capabilities_value, list):
        return capabilities_value

    # Comma-separated string
    return [cap.strip() for cap in capabilities_value.split(",") if cap.strip()]


async def _fetch_json(url: str, timeout: float) -> dict[str, Any]:
    """Fetch JSON from URL with standard error handling."""
    request_url = ensure_json_format(url)

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(request_url)
            response.raise_for_status()
            return cast("dict[str, Any]", response.json())
    except httpx.HTTPStatusError as e:
        msg = f"Failed to fetch from {url}: HTTP {e.response.status_code}"
        raise ImageServerDiscoveryError(redact_credentials(msg)) from e
    except httpx.ConnectError as e:
        msg = f"Failed to connect to {url}: {e}"
        raise ImageServerDiscoveryError(redact_credentials(msg)) from e
    except httpx.TimeoutException as e:
        msg = f"Request timed out for {url}: {e}"
        raise ImageServerDiscoveryError(redact_credentials(msg)) from e
    except httpx.RequestError as e:
        msg = f"Failed to fetch from {url}: {e}"
        raise ImageServerDiscoveryError(redact_credentials(msg)) from e
    except ValueError as e:
        msg = f"Invalid JSON response from {url}: {e}"
        raise ImageServerDiscoveryError(redact_credentials(msg)) from e


def _check_arcgis_error(data: dict[str, Any]) -> None:
    """Check for ArcGIS error response and raise if found."""
    if "error" in data:
        error_info = data["error"]
        code = error_info.get("code", "unknown")
        message = error_info.get("message", "Unknown error")
        raise ImageServerDiscoveryError(f"ArcGIS error ({code}): {message}")


def _validate_required_fields(data: dict[str, Any], url: str) -> None:
    """Validate that all required fields are present."""
    required_fields = [
        "name",
        "bandCount",
        "pixelType",
        "pixelSizeX",
        "pixelSizeY",
        "extent",
    ]

    for field_name in required_fields:
        if field_name not in data:
            raise ImageServerDiscoveryError(
                redact_credentials(
                    f"Missing required field '{field_name}' in ImageServer response from {url}"
                )
            )


def _image_size_limit(data: dict[str, Any], field_name: str, default: int) -> int:
    """Read an optional image size limit, with the ArcGIS Server default."""
    value = data.get(field_name)
    if not value:
        logger.warning(
            "ImageServer response omits '%s'. Using the ArcGIS Server default of %d px.",
            field_name,
            default,
        )
        return default
    return int(value)


def parse_imageserver_response(data: dict[str, Any]) -> ImageServerMetadata:
    """Parse ImageServer metadata from a JSON response."""
    # Check for ArcGIS error response
    _check_arcgis_error(data)

    # Validate required fields
    _validate_required_fields(data, "<parsed response>")

    # Parse capabilities
    capabilities = _parse_capabilities(data.get("capabilities"))

    # Extract documentInfo fields (author, keywords)
    doc_info = data.get("documentInfo", {})
    author = doc_info.get("Author")
    keywords_raw = doc_info.get("Keywords")

    # Parse keywords - can be comma-separated string or already a list
    keywords: list[str] | None = None
    if keywords_raw:
        if isinstance(keywords_raw, list):
            keywords = keywords_raw
        elif isinstance(keywords_raw, str):
            keywords = [k.strip() for k in keywords_raw.split(",") if k.strip()]

    return ImageServerMetadata(
        name=data["name"],
        band_count=data["bandCount"],
        pixel_type=data["pixelType"],
        pixel_size_x=float(data["pixelSizeX"]),
        pixel_size_y=float(data["pixelSizeY"]),
        full_extent=data["extent"],
        max_image_width=_image_size_limit(data, "maxImageWidth", DEFAULT_MAX_IMAGE_WIDTH),
        max_image_height=_image_size_limit(data, "maxImageHeight", DEFAULT_MAX_IMAGE_HEIGHT),
        capabilities=capabilities,
        description=data.get("description"),
        copyright_text=data.get("copyrightText"),
        service_data_type=data.get("serviceDataType"),
        # FeatureServer-parity fields
        service_description=data.get("serviceDescription"),
        author=author,
        keywords=keywords,
        license_info=data.get("licenseInfo"),
        access_information=data.get("accessInformation"),
        tile_cache=parse_tile_info(data),
    )


async def discover_imageserver(
    url: str,
    *,
    timeout: float = 60.0,
) -> ImageServerMetadata:
    """Discover metadata from an ArcGIS ImageServer."""
    data = await _fetch_json(url, timeout=timeout)

    return parse_imageserver_response(data)
