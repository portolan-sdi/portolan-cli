"""Style and legend extraction orchestration for remote services.

This module provides the high-level API for extracting styles from
WMS and ESRI REST endpoints and writing them as Mapbox GL JSON files.
Also extracts legend images from WMS GetLegendGraphic endpoint.

Style extraction flow:
1. Fetch style from source (WMS GetStyles or ESRI layer JSON)
2. Convert to Mapbox GL using format-specific converter
3. Write to {collection}/styles/{name}.json
4. Return ExtractedStyle for STAC asset registration

Legend extraction flow (Issue #498):
1. Fetch legend image from WMS GetLegendGraphic endpoint
2. Write to {collection}/legends/{name}.png
3. Return ExtractedLegend for STAC asset registration

Usage:
    # During WFS extraction
    style_info = extract_wms_style(wms_url, layer_name, collection_path)
    legend_info = extract_wms_legend(wms_url, layer_name, collection_path)

    # During ArcGIS extraction
    style_info = extract_esri_style(layer_url, collection_path)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx

from portolan_cli.extract.common.converters.esri import (
    ESRIConverterError,
    convert_esri_renderer,
)
from portolan_cli.extract.common.converters.sld import (
    SLDConverterError,
    convert_sld,
)

logger = logging.getLogger(__name__)


class StyleExtractionError(Exception):
    """Error during style extraction from remote service."""

    pass


@dataclass
class ExtractedStyle:
    """Result of style extraction.

    Attributes:
        path: Path to written style JSON file.
        name: Style name (used as asset key).
        source_format: Original format ("sld" or "esri").
        warnings: Any conversion warnings.
    """

    path: Path
    name: str
    source_format: str
    warnings: list[str]


@dataclass
class ExtractedLegend:
    """Result of legend extraction.

    Attributes:
        path: Path to written legend PNG file.
        name: Legend name (used as asset key).
        media_type: MIME type (always "image/png").
        width: Image width in pixels (None if not determined).
        height: Image height in pixels (None if not determined).
    """

    path: Path
    name: str
    media_type: str
    width: int | None = None
    height: int | None = None


def _wfs_url_to_wms_path(wfs_url: str) -> tuple[str, str]:
    """Convert WFS URL to WMS path.

    GeoServer/GeoNode typically expose WMS at the same base URL as WFS.
    We replace the service parameter and adjust the path.

    Args:
        wfs_url: WFS service endpoint URL.

    Returns:
        Tuple of (scheme://netloc, wms_path).
    """
    parsed = urlparse(wfs_url)

    # Try to detect if this is a GeoServer URL and adjust path
    # GeoServer pattern: .../geoserver/wfs -> .../geoserver/wms
    path = parsed.path
    if "/wfs" in path.lower():
        path = path.replace("/wfs", "/wms").replace("/WFS", "/wms")
    elif "/ows" in path.lower():
        path = path.replace("/ows", "/wms").replace("/OWS", "/wms")

    return f"{parsed.scheme}://{parsed.netloc}", path


def _build_wms_getstyles_url(wfs_url: str, layer_name: str) -> str:
    """Build WMS GetStyles URL from WFS endpoint.

    GeoServer/GeoNode typically expose WMS at the same base URL as WFS.
    We replace the service parameter and add GetStyles request.

    Args:
        wfs_url: WFS service endpoint URL.
        layer_name: Layer name (may include workspace prefix like "geonode:layer").

    Returns:
        WMS GetStyles URL.
    """
    parsed = urlparse(wfs_url)
    _, path = _wfs_url_to_wms_path(wfs_url)

    # Build GetStyles parameters
    params = {
        "service": "WMS",
        "version": "1.1.1",
        "request": "GetStyles",
        "layers": layer_name,
    }

    new_parsed = parsed._replace(path=path, query=urlencode(params))
    return urlunparse(new_parsed)


def _build_wms_getlegendgraphic_url(wfs_url: str, layer_name: str) -> str:
    """Build WMS GetLegendGraphic URL from WFS endpoint.

    GeoServer/GeoNode typically expose WMS at the same base URL as WFS.
    We replace the service parameter and add GetLegendGraphic request.

    Args:
        wfs_url: WFS service endpoint URL.
        layer_name: Layer name (may include workspace prefix like "geonode:layer").

    Returns:
        WMS GetLegendGraphic URL.
    """
    parsed = urlparse(wfs_url)
    _, path = _wfs_url_to_wms_path(wfs_url)

    # Build GetLegendGraphic parameters
    params = {
        "service": "WMS",
        "version": "1.1.1",
        "request": "GetLegendGraphic",
        "layer": layer_name,
        "format": "image/png",
    }

    new_parsed = parsed._replace(path=path, query=urlencode(params))
    return urlunparse(new_parsed)


def _fetch_wms_style(url: str, timeout: float = 30.0) -> str:
    """Fetch SLD XML from WMS GetStyles request.

    Args:
        url: WMS GetStyles URL.
        timeout: Request timeout in seconds.

    Returns:
        SLD XML string.

    Raises:
        StyleExtractionError: On HTTP or response errors.
    """
    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            response = client.get(url)
            response.raise_for_status()

            content_type = response.headers.get("content-type", "")

            # Check for XML response (SLD)
            if "xml" in content_type or response.text.strip().startswith("<?xml"):
                return response.text

            # Check for error response
            if "ServiceException" in response.text:
                raise StyleExtractionError(f"WMS GetStyles returned error: {response.text[:200]}")

            raise StyleExtractionError(
                f"Unexpected content type from WMS GetStyles: {content_type}"
            )

    except httpx.HTTPStatusError as e:
        raise StyleExtractionError(
            f"WMS GetStyles request failed: HTTP {e.response.status_code}"
        ) from e
    except httpx.RequestError as e:
        raise StyleExtractionError(f"WMS GetStyles request failed: {e}") from e


def _fetch_esri_layer_json(layer_url: str, timeout: float = 30.0) -> dict[str, Any]:
    """Fetch layer JSON from ESRI REST endpoint.

    Args:
        layer_url: ESRI layer URL (e.g., .../FeatureServer/0).
        timeout: Request timeout in seconds.

    Returns:
        Layer JSON dict.

    Raises:
        StyleExtractionError: On HTTP or response errors.
    """
    # Ensure f=json parameter
    parsed = urlparse(layer_url)
    params = parse_qs(parsed.query)
    params["f"] = ["json"]
    new_parsed = parsed._replace(query=urlencode(params, doseq=True))
    url = urlunparse(new_parsed)

    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            response = client.get(url)
            response.raise_for_status()
            result: dict[str, Any] = response.json()
            return result

    except httpx.HTTPStatusError as e:
        raise StyleExtractionError(
            f"ESRI layer request failed: HTTP {e.response.status_code}"
        ) from e
    except httpx.RequestError as e:
        raise StyleExtractionError(f"ESRI layer request failed: {e}") from e
    except json.JSONDecodeError as e:
        raise StyleExtractionError(f"Invalid JSON from ESRI endpoint: {e}") from e


def _write_style_file(
    style_dict: dict[str, Any],
    collection_path: Path,
    name: str,
) -> Path:
    """Write Mapbox GL style to collection's styles directory.

    Args:
        style_dict: Mapbox GL style dict.
        collection_path: Path to collection directory.
        name: Style filename (without .json extension).

    Returns:
        Path to written file.
    """
    styles_dir = collection_path / "styles"
    styles_dir.mkdir(parents=True, exist_ok=True)

    style_path = styles_dir / f"{name}.json"
    style_path.write_text(json.dumps(style_dict, indent=2))

    return style_path


def extract_wms_style(
    wfs_url: str,
    layer_name: str,
    collection_path: Path,
    *,
    source_layer: str | None = None,
    style_name: str = "default",
    timeout: float = 30.0,
) -> ExtractedStyle | None:
    """Extract style from WMS GetStyles and write as Mapbox GL JSON.

    Attempts to fetch SLD from companion WMS endpoint and convert to
    Mapbox GL format. Returns None if style cannot be extracted.

    Args:
        wfs_url: WFS service endpoint URL.
        layer_name: Layer name (may include workspace prefix).
        collection_path: Path to collection directory.
        source_layer: Source layer name for Mapbox GL (defaults to layer_name stem).
        style_name: Output filename (default "default").
        timeout: Request timeout in seconds.

    Returns:
        ExtractedStyle if successful, None if style unavailable.
    """
    # Build source layer name
    if source_layer is None:
        # Strip workspace prefix like "geonode:"
        source_layer = layer_name.split(":")[-1] if ":" in layer_name else layer_name

    # Build WMS URL
    wms_url = _build_wms_getstyles_url(wfs_url, layer_name)
    logger.debug("Fetching WMS style from: %s", wms_url)

    try:
        sld_xml = _fetch_wms_style(wms_url, timeout=timeout)
    except StyleExtractionError as e:
        logger.warning("Could not fetch WMS style for %s: %s", layer_name, e)
        return None

    # Convert SLD to Mapbox GL
    try:
        style, warnings = convert_sld(sld_xml, source_layer, return_warnings=True)
    except SLDConverterError as e:
        logger.warning("Could not convert SLD for %s: %s", layer_name, e)
        return None

    for warning in warnings:
        logger.warning("SLD conversion warning for %s: %s", layer_name, warning)

    # Write style file
    style_path = _write_style_file(style, collection_path, style_name)
    logger.info("Wrote style to %s", style_path)

    return ExtractedStyle(
        path=style_path,
        name=style_name,
        source_format="sld",
        warnings=warnings,
    )


def extract_esri_style(
    layer_url: str,
    collection_path: Path,
    *,
    source_layer: str | None = None,
    style_name: str = "default",
    timeout: float = 30.0,
) -> ExtractedStyle | None:
    """Extract style from ESRI REST layer and write as Mapbox GL JSON.

    Fetches drawingInfo.renderer from layer JSON and converts to
    Mapbox GL format. Returns None if style cannot be extracted.

    Args:
        layer_url: ESRI layer URL (e.g., .../FeatureServer/0).
        collection_path: Path to collection directory.
        source_layer: Source layer name for Mapbox GL (defaults to layer name).
        style_name: Output filename (default "default").
        timeout: Request timeout in seconds.

    Returns:
        ExtractedStyle if successful, None if style unavailable.
    """
    try:
        layer_json = _fetch_esri_layer_json(layer_url, timeout=timeout)
    except StyleExtractionError as e:
        logger.warning("Could not fetch ESRI layer JSON: %s", e)
        return None

    # Extract renderer from drawingInfo
    drawing_info = layer_json.get("drawingInfo", {})
    renderer = drawing_info.get("renderer")

    if not renderer:
        logger.debug("No renderer found in ESRI layer JSON")
        return None

    # Get source layer name
    if source_layer is None:
        source_layer = layer_json.get("name", "data")

    # Convert to Mapbox GL
    try:
        style, warnings = convert_esri_renderer(renderer, source_layer, return_warnings=True)
    except ESRIConverterError as e:
        logger.warning("Could not convert ESRI renderer: %s", e)
        return None

    for warning in warnings:
        logger.warning("ESRI conversion warning: %s", warning)

    # Write style file
    style_path = _write_style_file(style, collection_path, style_name)
    logger.info("Wrote style to %s", style_path)

    return ExtractedStyle(
        path=style_path,
        name=style_name,
        source_format="esri",
        warnings=warnings,
    )


# =============================================================================
# Legend Extraction (Issue #498)
# =============================================================================


def _fetch_wms_legend(url: str, timeout: float = 30.0) -> bytes | None:
    """Fetch legend image bytes from WMS GetLegendGraphic request.

    Args:
        url: WMS GetLegendGraphic URL.
        timeout: Request timeout in seconds.

    Returns:
        PNG image bytes if successful, None on errors.
    """
    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            response = client.get(url)
            response.raise_for_status()

            content_type = response.headers.get("content-type", "")

            # Check for image/png response
            if "image/png" in content_type:
                return response.content

            # Some servers return image without proper content-type
            # Check for PNG magic bytes
            if response.content.startswith(b"\x89PNG"):
                return response.content

            logger.warning("Unexpected content type from WMS GetLegendGraphic: %s", content_type)
            return None

    except httpx.HTTPStatusError as e:
        logger.warning("WMS GetLegendGraphic request failed: HTTP %s", e.response.status_code)
        return None
    except httpx.RequestError as e:
        logger.warning("WMS GetLegendGraphic request failed: %s", e)
        return None


def _write_legend_file(
    legend_bytes: bytes,
    collection_path: Path,
    name: str,
) -> Path:
    """Write legend PNG to collection's legends directory.

    Args:
        legend_bytes: PNG image bytes.
        collection_path: Path to collection directory.
        name: Legend filename (without .png extension).

    Returns:
        Path to written file.
    """
    legends_dir = collection_path / "legends"
    legends_dir.mkdir(parents=True, exist_ok=True)

    legend_path = legends_dir / f"{name}.png"
    legend_path.write_bytes(legend_bytes)

    return legend_path


def extract_wms_legend(
    wfs_url: str,
    layer_name: str,
    collection_path: Path,
    *,
    legend_name: str = "source",
    timeout: float = 30.0,
) -> ExtractedLegend | None:
    """Extract legend from WMS GetLegendGraphic and save to legends/ directory.

    Attempts to fetch legend image from companion WMS endpoint.
    Returns None if legend cannot be extracted.

    Args:
        wfs_url: WFS service endpoint URL.
        layer_name: Layer name (may include workspace prefix).
        collection_path: Path to collection directory.
        legend_name: Output filename (default "source").
        timeout: Request timeout in seconds.

    Returns:
        ExtractedLegend if successful, None if legend unavailable.
    """
    # Build WMS GetLegendGraphic URL
    legend_url = _build_wms_getlegendgraphic_url(wfs_url, layer_name)
    logger.debug("Fetching WMS legend from: %s", legend_url)

    # Fetch legend image
    legend_bytes = _fetch_wms_legend(legend_url, timeout=timeout)
    if legend_bytes is None:
        logger.warning("Could not fetch WMS legend for %s", layer_name)
        return None

    # Write legend file
    legend_path = _write_legend_file(legend_bytes, collection_path, legend_name)
    logger.info("Wrote legend to %s", legend_path)

    return ExtractedLegend(
        path=legend_path,
        name=legend_name,
        media_type="image/png",
    )
