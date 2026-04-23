"""WFS layer discovery.

This module wraps geoparquet-io's WFS discovery functionality and normalizes
the terminology to match Portolan conventions (typename → layer/name).

Key functions:
- list_layers: Quick listing of available layers (typenames)
- discover_layers: Full discovery with service metadata
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


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
    """

    service_url: str
    layers: list[LayerInfo] = field(default_factory=list)
    service_title: str | None = None
    service_abstract: str | None = None
    provider: str | None = None
    keywords: list[str] | None = None


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
    except Exception as e:
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
    to layer information.

    Args:
        service_url: WFS service endpoint URL.
        version: WFS version (1.0.0, 1.1.0, or 2.0.0).

    Returns:
        WFSDiscoveryResult with layers and service metadata.

    Raises:
        WFSDiscoveryError: If connection or parsing fails.
    """
    layers = list_layers(service_url, version)

    # TODO: Extract service-level metadata from GetCapabilities
    # For now, return basic result with just layers
    return WFSDiscoveryResult(
        service_url=service_url,
        layers=layers,
        service_title=None,
        service_abstract=None,
        provider=None,
        keywords=None,
    )
