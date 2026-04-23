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
        bbox: Bounding box in WGS84 (xmin, ymin, xmax, ymax).
        id: Numeric ID for filtering compatibility (auto-assigned).
    """

    name: str
    typename: str
    title: str | None = None
    abstract: str | None = None
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

        # Get capabilities object from OWSLib
        wfs = get_wfs_capabilities(service_url, version)

        # Extract service-level metadata
        service_title = None
        service_abstract = None
        provider = None
        keywords: list[str] | None = None
        contact_name = None
        access_constraints = None
        fees = None

        # Service identification
        if hasattr(wfs, "identification") and wfs.identification:
            ident = wfs.identification
            service_title = getattr(ident, "title", None)
            service_abstract = getattr(ident, "abstract", None)
            if hasattr(ident, "keywords") and ident.keywords:
                keywords = [str(kw) for kw in ident.keywords]
            access_constraints = getattr(ident, "accessconstraints", None)
            fees = getattr(ident, "fees", None)

        # Provider information
        if hasattr(wfs, "provider") and wfs.provider:
            prov = wfs.provider
            provider = getattr(prov, "name", None)
            # Try to get contact info
            if hasattr(prov, "contact") and prov.contact:
                contact = prov.contact
                contact_parts = []
                if hasattr(contact, "name") and contact.name:
                    contact_parts.append(contact.name)
                if hasattr(contact, "organization") and contact.organization:
                    contact_parts.append(contact.organization)
                if contact_parts:
                    contact_name = ", ".join(contact_parts)

        # Build layer list
        layers = []
        for i, (typename, layer) in enumerate(wfs.contents.items()):
            layers.append(
                LayerInfo(
                    name=typename,
                    typename=typename,
                    title=getattr(layer, "title", None),
                    abstract=getattr(layer, "abstract", None),
                    bbox=tuple(layer.boundingBoxWGS84)
                    if hasattr(layer, "boundingBoxWGS84") and layer.boundingBoxWGS84
                    else None,
                    id=i,
                )
            )

        return WFSDiscoveryResult(
            service_url=service_url,
            layers=layers,
            service_title=service_title,
            service_abstract=service_abstract,
            provider=provider,
            keywords=keywords,
            contact_name=contact_name,
            access_constraints=access_constraints,
            fees=fees,
        )

    except ImportError as e:
        raise WFSDiscoveryError(
            "geoparquet-io is required for WFS extraction. Install with: pip install geoparquet-io"
        ) from e
    except _NETWORK_ERRORS as e:
        raise WFSDiscoveryError(f"Failed to discover WFS layers: {e}") from e
