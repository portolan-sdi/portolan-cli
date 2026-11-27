"""WFS layer discovery."""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET  # only for ParseError, this module parses with defusedxml
from dataclasses import dataclass, field
from typing import Any

import requests

from portolan_cli.extract.common.urls import redact_credentials


class WFSDiscoveryError(Exception):
    """Raised when WFS discovery fails."""


@dataclass
class LayerInfo:
    """Information about a WFS layer (feature type)."""

    name: str
    typename: str
    title: str | None = None
    abstract: str | None = None
    keywords: list[str] | None = None
    metadata_urls: list[dict[str, Any]] | None = None
    bbox: tuple[float, float, float, float] | None = None
    id: int = 0


@dataclass
class WFSDiscoveryResult:
    """Result of WFS service discovery."""

    service_url: str
    layers: list[LayerInfo] = field(default_factory=list)
    service_title: str | None = None
    service_abstract: str | None = None
    provider: str | None = None
    keywords: list[str] | None = None
    access_constraints: str | None = None
    fees: str | None = None


_NETWORK_ERRORS = (
    requests.exceptions.RequestException,  # Includes underlying urllib3 errors
    json.JSONDecodeError,
    ET.ParseError,
    OSError,
    TimeoutError,
    ConnectionError,
)


def gpio_list_layers(service_url: str, version: str = "1.1.0") -> list[dict[str, Any]]:
    """Wrapper for geoparquet-io's list_available_layers.

    This function exists to make mocking easier in tests.
    """
    try:
        from geoparquet_io.core.wfs import (
            WFSError,
            list_available_layers,
        )
    except ImportError as e:
        raise WFSDiscoveryError(
            "geoparquet-io is required for WFS extraction. Install with: pip install geoparquet-io"
        ) from e
    try:
        result: list[dict[str, Any]] = list_available_layers(service_url, version=version)
        return result
    except (WFSError, *_NETWORK_ERRORS) as e:
        raise WFSDiscoveryError(redact_credentials(f"Failed to list WFS layers: {e}")) from e


def list_layers(
    service_url: str,
    version: str = "1.1.0",
) -> list[LayerInfo]:
    """List available layers in a WFS service."""
    raw_layers = gpio_list_layers(service_url, version)

    layers = []
    for i, layer_dict in enumerate(raw_layers):
        typename = layer_dict.get("typename", layer_dict.get("name", ""))
        # Use full typename as name (including namespace prefix)
        # The typename is the identifier needed for WFS requests

        layers.append(
            LayerInfo(
                name=typename,
                typename=typename,
                title=layer_dict.get("title"),
                abstract=layer_dict.get("abstract"),
                bbox=layer_dict.get("bbox"),
                id=i,
            )
        )

    return layers


def _extract_service_metadata(wfs: Any) -> dict[str, Any]:
    """Extract service-level metadata from WFS capabilities object."""
    result: dict[str, Any] = {
        "service_title": None,
        "service_abstract": None,
        "provider": None,
        "keywords": None,
        "access_constraints": None,
        "fees": None,
    }

    if hasattr(wfs, "identification") and wfs.identification:
        ident = wfs.identification
        result["service_title"] = getattr(ident, "title", None)
        result["service_abstract"] = getattr(ident, "abstract", None)
        if hasattr(ident, "keywords") and ident.keywords:
            result["keywords"] = [str(kw) for kw in ident.keywords]
        result["access_constraints"] = getattr(ident, "accessconstraints", None)
        result["fees"] = getattr(ident, "fees", None)

    if hasattr(wfs, "provider") and wfs.provider:
        prov = wfs.provider
        result["provider"] = getattr(prov, "name", None)

    return result


def _build_layer_info(typename: str, layer: Any, layer_id: int) -> LayerInfo:
    """Build LayerInfo from OWSLib layer object."""
    keywords: list[str] | None = None
    if hasattr(layer, "keywords") and layer.keywords:
        keywords = [str(kw) for kw in layer.keywords]

    metadata_urls: list[dict[str, Any]] | None = None
    if hasattr(layer, "metadataUrls") and layer.metadataUrls:
        metadata_urls = list(layer.metadataUrls)

    bbox = None
    if hasattr(layer, "boundingBoxWGS84") and layer.boundingBoxWGS84:
        bbox = tuple(layer.boundingBoxWGS84)

    return LayerInfo(
        name=typename,
        typename=typename,
        title=getattr(layer, "title", None),
        abstract=getattr(layer, "abstract", None),
        keywords=keywords,
        metadata_urls=metadata_urls,
        bbox=bbox,
        id=layer_id,
    )


def discover_layers(
    service_url: str,
    version: str = "1.1.0",
) -> WFSDiscoveryResult:
    """Discover layers and service metadata from a WFS endpoint."""
    try:
        from geoparquet_io.core.wfs import WFSError, get_wfs_capabilities
    except ImportError as e:
        raise WFSDiscoveryError(
            "geoparquet-io is required for WFS extraction. Install with: pip install geoparquet-io"
        ) from e
    try:
        wfs = get_wfs_capabilities(service_url, version)
        svc = _extract_service_metadata(wfs)

        layers = [
            _build_layer_info(typename, layer, i)
            for i, (typename, layer) in enumerate(wfs.contents.items())
        ]

        return WFSDiscoveryResult(
            service_url=service_url,
            layers=layers,
            service_title=svc["service_title"],
            service_abstract=svc["service_abstract"],
            provider=svc["provider"],
            keywords=svc["keywords"],
            access_constraints=svc["access_constraints"],
            fees=svc["fees"],
        )
    except (WFSError, *_NETWORK_ERRORS) as e:
        # geoparquet-io and requests quote the service URL with its key.
        raise WFSDiscoveryError(redact_credentials(f"Failed to discover WFS layers: {e}")) from e
