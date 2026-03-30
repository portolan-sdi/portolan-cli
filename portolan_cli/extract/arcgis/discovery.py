"""ArcGIS service and layer discovery.

This module fetches service and layer information from ArcGIS REST API
endpoints. It's the first step in the extraction flow, providing the
list of available layers that can then be filtered and extracted.

The discovery client uses httpx for HTTP requests, while actual data
extraction is delegated to geoparquet-io (gpio).

Typical usage:
    # Discover layers from a FeatureServer
    service_info = discover_layers("https://services.arcgis.com/.../FeatureServer")
    for layer in service_info.layers:
        print(f"Layer {layer.id}: {layer.name}")

    # Discover services from a services root
    services = discover_services("https://services.arcgis.com/.../rest/services")
    for service in services:
        print(f"Service: {service.name} ({service.service_type})")
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast, overload
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx

if TYPE_CHECKING:
    from collections.abc import Sequence


class ArcGISDiscoveryError(Exception):
    """Error during ArcGIS service/layer discovery."""

    pass


@dataclass(frozen=True)
class LayerInfo:
    """Information about a single layer in an ArcGIS service.

    Attributes:
        id: Numeric layer ID (used in API calls)
        name: Human-readable layer name
        layer_type: Type string (e.g., "Feature Layer", "Table")
    """

    id: int
    name: str
    layer_type: str


@dataclass(frozen=True)
class ServiceInfo:
    """Information about an ArcGIS service.

    Attributes:
        name: Service name (used in URL construction)
        service_type: Type string (e.g., "FeatureServer", "MapServer")
    """

    name: str
    service_type: str

    def get_url(self, base_url: str) -> str:
        """Generate the full service URL from a base services URL.

        Args:
            base_url: The services root URL (e.g., ".../rest/services")

        Returns:
            Full service URL (e.g., ".../rest/services/Census_2020/FeatureServer")
        """
        # Remove trailing slash if present
        base = base_url.rstrip("/")
        return f"{base}/{self.name}/{self.service_type}"


@dataclass
class ServiceDiscoveryResult:
    """Result of discovering layers from a service.

    Contains both the list of layers and service-level metadata
    that can be used for catalog generation.
    """

    layers: list[LayerInfo]
    service_description: str | None = None
    description: str | None = None
    copyright_text: str | None = None
    author: str | None = None
    keywords: str | None = None
    license_info: str | None = None
    access_information: str | None = None


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


def _fetch_json(url: str, timeout: float = 60.0) -> dict[str, Any]:
    """Fetch JSON from URL with standard error handling.

    Args:
        url: URL to fetch (will have f=json added if needed)
        timeout: Request timeout in seconds

    Returns:
        Parsed JSON response

    Raises:
        ArcGISDiscoveryError: On HTTP or parsing errors
    """
    request_url = _ensure_json_format(url)

    try:
        with httpx.Client(timeout=timeout) as client:
            response = client.get(request_url)
            response.raise_for_status()
            return cast(dict[str, Any], response.json())
    except httpx.HTTPStatusError as e:
        msg = f"Failed to fetch from {url}: HTTP {e.response.status_code}"
        raise ArcGISDiscoveryError(msg) from e
    except httpx.RequestError as e:
        msg = f"Failed to fetch from {url}: {e}"
        raise ArcGISDiscoveryError(msg) from e
    except ValueError as e:
        msg = f"Invalid JSON response from {url}: {e}"
        raise ArcGISDiscoveryError(msg) from e


def discover_layers(
    url: str,
    *,
    include_tables: bool = False,
    timeout: float = 60.0,
) -> ServiceDiscoveryResult:
    """Discover layers and metadata from an ArcGIS FeatureServer or MapServer.

    Fetches the service info from the ArcGIS REST API and extracts the list
    of available layers along with service-level metadata.

    Args:
        url: FeatureServer or MapServer URL
        include_tables: If True, include tables in addition to layers
        timeout: Request timeout in seconds

    Returns:
        ServiceDiscoveryResult containing layers and metadata

    Raises:
        ArcGISDiscoveryError: If the request fails or response is invalid
    """
    data = _fetch_json(url, timeout=timeout)

    # Extract layers
    layers: list[LayerInfo] = []

    for layer_data in data.get("layers", []):
        layers.append(
            LayerInfo(
                id=layer_data["id"],
                name=layer_data["name"],
                layer_type=layer_data.get("type", "Feature Layer"),
            )
        )

    # Include tables if requested
    if include_tables:
        for table_data in data.get("tables", []):
            layers.append(
                LayerInfo(
                    id=table_data["id"],
                    name=table_data["name"],
                    layer_type=table_data.get("type", "Table"),
                )
            )

    # Extract service-level metadata
    document_info = data.get("documentInfo", {})

    return ServiceDiscoveryResult(
        layers=layers,
        service_description=data.get("serviceDescription"),
        description=data.get("description"),
        copyright_text=data.get("copyrightText"),
        author=document_info.get("Author"),
        keywords=document_info.get("Keywords"),
        license_info=data.get("licenseInfo"),
        access_information=data.get("accessInformation"),
    )


@overload
def discover_services(
    url: str,
    *,
    service_types: Sequence[str] | None = ...,
    return_folders: Literal[False] = ...,
    timeout: float = ...,
) -> list[ServiceInfo]: ...


@overload
def discover_services(
    url: str,
    *,
    service_types: Sequence[str] | None = ...,
    return_folders: Literal[True],
    timeout: float = ...,
) -> tuple[list[ServiceInfo], list[str]]: ...


def discover_services(
    url: str,
    *,
    service_types: Sequence[str] | None = None,
    return_folders: bool = False,
    timeout: float = 60.0,
) -> list[ServiceInfo] | tuple[list[ServiceInfo], list[str]]:
    """Discover services from an ArcGIS REST services root.

    Fetches the list of available services from a services root URL.
    Optionally filters by service type and returns folder information.

    Args:
        url: Services root URL (e.g., ".../rest/services")
        service_types: If provided, only return services of these types
            (e.g., ["FeatureServer", "MapServer"])
        return_folders: If True, also return list of folder names
        timeout: Request timeout in seconds

    Returns:
        List of ServiceInfo objects (or tuple with folders if return_folders=True)

    Raises:
        ArcGISDiscoveryError: If the request fails or response is invalid
    """
    data = _fetch_json(url, timeout=timeout)

    # Extract services
    services: list[ServiceInfo] = []

    for service_data in data.get("services", []):
        service = ServiceInfo(
            name=service_data["name"],
            service_type=service_data["type"],
        )

        # Filter by type if specified
        if service_types is None or service.service_type in service_types:
            services.append(service)

    if return_folders:
        folders = data.get("folders", [])
        return services, folders

    return services


def fetch_layer_details(
    service_url: str,
    layer_id: int,
    *,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Fetch detailed information for a specific layer.

    Gets full layer metadata including fields, extent, and geometry type.
    This is used to extract field aliases and other layer-specific metadata.

    Args:
        service_url: FeatureServer or MapServer URL
        layer_id: Numeric layer ID
        timeout: Request timeout in seconds

    Returns:
        Raw layer info dictionary from ArcGIS API

    Raises:
        ArcGISDiscoveryError: If the request fails or response is invalid
    """
    layer_url = f"{service_url.rstrip('/')}/{layer_id}"
    return _fetch_json(layer_url, timeout=timeout)
