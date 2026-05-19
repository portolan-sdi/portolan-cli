"""WFS layer discovery.

This module wraps geoparquet-io's WFS discovery functionality and normalizes
the terminology to match Portolan conventions (typename → layer/name).

Key functions:
- list_layers: Quick listing of available layers (typenames)
- discover_layers: Full discovery with service metadata
"""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET  # nosec B405 - only using ParseError, not parsing
from dataclasses import dataclass, field
from typing import Any

import requests  # type: ignore[import-untyped]


class WFSDiscoveryError(Exception):
    """Raised when WFS discovery fails."""

    pass


@dataclass
class LayerInfo:
    """Information about a WFS layer (feature type).

    Normalizes WFS terminology: "typename" is exposed as "name" for
    consistency with Portolan's layer-centric terminology.

    Attributes:
        name: Layer name (alias for typename, used in UI/filtering).
        typename: Original WFS typename (may include namespace prefix).
        title: Human-readable title from GetCapabilities.
        abstract: Layer description from GetCapabilities.
        keywords: Layer-specific keywords from GetCapabilities.
        metadata_urls: List of metadata URL dicts (e.g., CSW GetRecordById).
        bbox: Bounding box in WGS84 (xmin, ymin, xmax, ymax).
        id: Numeric ID for filtering compatibility (auto-assigned).
    """

    name: str
    typename: str
    title: str | None = None
    abstract: str | None = None
    keywords: list[str] | None = None
    metadata_urls: list[dict[str, Any]] | None = None
    bbox: tuple[float, float, float, float] | None = None
    id: int = 0

    def to_filter_dict(self) -> dict[str, int | str]:
        """Convert to dict format expected by filter_layers.

        Returns:
            Dict with 'id' and 'name' keys for glob filtering.
        """
        return {"id": self.id, "name": self.name}


@dataclass
class WFSDiscoveryResult:
    """Result of WFS service discovery.

    Attributes:
        service_url: The WFS service URL that was queried.
        layers: List of discovered layers.
        service_title: Service title from GetCapabilities.
        service_abstract: Service description from GetCapabilities.
        provider: Provider name from GetCapabilities.
        keywords: Keywords from GetCapabilities.
        contact_name: Contact person/organization name.
        access_constraints: Access constraints or license info.
        fees: Fee information (typically "none" for public services).
    """

    service_url: str
    layers: list[LayerInfo] = field(default_factory=list)
    service_title: str | None = None
    service_abstract: str | None = None
    provider: str | None = None
    keywords: list[str] | None = None
    contact_name: str | None = None
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
        from geoparquet_io.core.wfs import list_available_layers  # type: ignore[import-untyped]

        result: list[dict[str, Any]] = list_available_layers(service_url, version=version)
        return result
    except ImportError as e:
        raise WFSDiscoveryError(
            "geoparquet-io is required for WFS extraction. Install with: pip install geoparquet-io"
        ) from e
    except _NETWORK_ERRORS as e:
        raise WFSDiscoveryError(f"Failed to list WFS layers: {e}") from e


def list_layers(
    service_url: str,
    version: str = "1.1.0",
) -> list[LayerInfo]:
    """List available layers in a WFS service.

    Quick discovery that returns layer names and basic metadata.
    For full service metadata, use discover_layers().

    Args:
        service_url: WFS service endpoint URL.
        version: WFS version (1.0.0, 1.1.0, or 2.0.0).

    Returns:
        List of LayerInfo objects.

    Raises:
        WFSDiscoveryError: If connection or parsing fails.
    """
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
        "contact_name": None,
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
        if hasattr(prov, "contact") and prov.contact:
            contact = prov.contact
            parts = []
            if hasattr(contact, "name") and contact.name:
                parts.append(contact.name)
            if hasattr(contact, "organization") and contact.organization:
                parts.append(contact.organization)
            if parts:
                result["contact_name"] = ", ".join(parts)

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
    """Discover layers and service metadata from a WFS endpoint.

    Full discovery that retrieves service-level metadata in addition
    to layer information. Uses geoparquet-io's get_wfs_capabilities
    to parse GetCapabilities response.

    Args:
        service_url: WFS service endpoint URL.
        version: WFS version (1.0.0, 1.1.0, or 2.0.0).

    Returns:
        WFSDiscoveryResult with layers and service metadata.

    Raises:
        WFSDiscoveryError: If connection or parsing fails.
    """
    try:
        from geoparquet_io.core.wfs import get_wfs_capabilities

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
            contact_name=svc["contact_name"],
            access_constraints=svc["access_constraints"],
            fees=svc["fees"],
        )

    except ImportError as e:
        raise WFSDiscoveryError(
            "geoparquet-io is required for WFS extraction. Install with: pip install geoparquet-io"
        ) from e
    except _NETWORK_ERRORS as e:
        raise WFSDiscoveryError(f"Failed to discover WFS layers: {e}") from e
