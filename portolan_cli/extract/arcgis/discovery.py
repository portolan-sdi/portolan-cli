"""ArcGIS service and layer discovery.

This module fetches service and layer information from ArcGIS REST API
endpoints. It's the first step in the extraction flow, providing the
list of available layers that can then be filtered and extracted.

The discovery client uses httpx for HTTP requests, while actual data
extraction is delegated to geoparquet-io (gpio).

Every request goes through one ``ArcGISClient``. It carries the token
(issue #855) and the retry policy (issue #811), so discovery and extraction
use the same options. A transient failure (a transport error, HTTP 429 or
5xx, or an embedded 5xx error) is retried with ``retry_with_backoff``. A
permanent failure, such as HTTP 404 or ``499 Token Required``, fails at once.

Typical usage:
    client = ArcGISClient(timeout=60.0, token=token, retries=3)
    service_info = discover_layers("https://services.arcgis.com/.../FeatureServer", client)
    for layer in service_info.layers:
        print(f"Layer {layer.id}: {layer.name}")
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, cast, overload
from urllib.parse import parse_qsl, urlparse

import httpx

from portolan_cli.extract.common.urls import merge_query

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


class ArcGISDiscoveryError(Exception):
    """Error during ArcGIS service/layer discovery."""


class ArcGISTransientError(ArcGISDiscoveryError):
    """A discovery failure that a retry can change, such as HTTP 503."""


class _EmptyListingError(ArcGISTransientError):
    """A service answer that lists no layers and no tables.

    ArcGIS Online sometimes answers a healthy service with ``"layers": []``.
    The live suite saw it for 21 of 40 requests in a row to one service. The
    client asks again, and keeps the last answer when every answer is empty.
    """

    def __init__(self, url: str, data: dict[str, Any]) -> None:
        super().__init__(f"{url} lists no layers and no tables")
        self.data = data


NO_QUERY_SKIP_REASON = "skipped: no Query capability (tiles-only)"


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
    skipped_layers: list[LayerInfo] = field(default_factory=list)
    skip_reason: str | None = None
    tables: list[LayerInfo] = field(default_factory=list)


def ensure_json_format(url: str) -> str:
    """Return ``url`` with ``f=json``, unless it already names an ``f`` format."""
    names = {name.lower() for name, _ in parse_qsl(urlparse(url).query)}
    return url if "f" in names else merge_query(url, {"f": "json"})


def _is_transient_status(code: object) -> bool:
    """Return True for an HTTP or embedded ArcGIS status that a retry can change."""
    try:
        status = int(str(code))
    except ValueError:
        return False
    return status == 429 or status >= 500


@dataclass(frozen=True)
class ArcGISClient:
    """The one HTTP client for ArcGIS discovery.

    Attributes:
        timeout: Request timeout in seconds.
        token: ArcGIS token, sent as ``token=<token>`` on every request.
        retries: Attempts per request. Only a transient failure is retried.
        retry_delay: Seconds before the first retry. Later retries double it.
        transport: An httpx transport. Tests pass a mock transport here.
    """

    timeout: float = 60.0
    token: str | None = None
    retries: int = 3
    retry_delay: float = 1.0
    transport: httpx.BaseTransport | None = None

    def get_json(self, url: str, *, listing: bool = False) -> dict[str, Any]:
        """Fetch ``url`` as JSON, with ``f=json`` and the token, and retry transient failures.

        Args:
            url: The REST URL.
            listing: The URL is a service. An answer with no layers and no
                tables is asked again, and the last answer is returned.

        Raises:
            ArcGISDiscoveryError: On a permanent failure, or on a transient
                failure that every attempt hit.
        """
        from portolan_cli.extract.common.retry import RetryConfig, retry_with_backoff

        outcome = retry_with_backoff(
            self._get_once,
            RetryConfig(max_attempts=max(self.retries, 1), initial_delay=self.retry_delay),
            url,
            listing,
            retry_on=(ArcGISTransientError,),
            on_retry=lambda attempt, err: logger.debug("Retry %d for %s: %s", attempt, url, err),
        )
        if isinstance(outcome.error, _EmptyListingError):
            return outcome.error.data
        if outcome.error is not None:
            raise outcome.error
        return cast("dict[str, Any]", outcome.value)

    def _get_once(self, url: str, listing: bool = False) -> dict[str, Any]:
        """Make one request. ArcGIS reports some errors in an HTTP 200 body."""
        request_url = ensure_json_format(url)
        if self.token:
            request_url = merge_query(request_url, {"token": self.token})
        try:
            with httpx.Client(timeout=self.timeout, transport=self.transport) as client:
                response = client.get(request_url)
        except httpx.RequestError as e:
            raise ArcGISTransientError(f"Failed to fetch from {url}: {e}") from e
        if response.status_code >= 400:
            transient = _is_transient_status(response.status_code)
            error = ArcGISTransientError if transient else ArcGISDiscoveryError
            raise error(f"Failed to fetch from {url}: HTTP {response.status_code}")
        try:
            raw = response.json()
        except ValueError as e:
            raise ArcGISDiscoveryError(f"Invalid JSON response from {url}: {e}") from e
        if not isinstance(raw, dict):
            raise ArcGISDiscoveryError(f"Expected JSON object from {url}, got {type(raw).__name__}")
        data = cast("dict[str, Any]", raw)
        embedded = data.get("error")
        if isinstance(embedded, dict):
            code = embedded.get("code", "unknown")
            message = embedded.get("message", "ArcGIS error")
            error = ArcGISTransientError if _is_transient_status(code) else ArcGISDiscoveryError
            raise error(f"ArcGIS error from {url}: {code} {message}")
        if listing and not (data.get("layers") or data.get("tables")):
            raise _EmptyListingError(url, data)
        return data


def _supports_query(capabilities: Any) -> bool:
    """Return False only when the service advertises capabilities without Query.

    A missing or empty capabilities value is treated as supported, because older
    servers do not report it.
    """
    if not isinstance(capabilities, str) or not capabilities.strip():
        return True
    return "query" in {c.strip().lower() for c in capabilities.split(",")}


def discover_layers(url: str, client: ArcGISClient | None = None) -> ServiceDiscoveryResult:
    """Discover layers, tables, and metadata from an ArcGIS FeatureServer or MapServer.

    Args:
        url: FeatureServer or MapServer URL.
        client: The client with the token and the retry policy.

    Returns:
        The layers, the tables, and the service-level metadata. The caller
        decides whether to extract the tables (issue #812).

    Raises:
        ArcGISDiscoveryError: If the request fails or response is invalid
    """
    data = (client or ArcGISClient()).get_json(url, listing=True)

    def infos(key: str, default_type: str) -> list[LayerInfo]:
        return [
            LayerInfo(id=item["id"], name=item["name"], layer_type=item.get("type", default_type))
            for item in data.get(key, [])
        ]

    layers = infos("layers", "Feature Layer")
    # Tiles-only / cached map services list "Feature Layer" layers but reject /query
    skipped_layers: list[LayerInfo] = []
    skip_reason: str | None = None
    if not _supports_query(data.get("capabilities")):
        skip_reason = NO_QUERY_SKIP_REASON
        skipped_layers, layers = layers, []

    document_info = data.get("documentInfo", {})
    return ServiceDiscoveryResult(
        layers=layers,
        tables=infos("tables", "Table"),
        skipped_layers=skipped_layers,
        skip_reason=skip_reason,
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
    client: ArcGISClient | None = ...,
    service_types: Sequence[str] | None = ...,
    return_folders: Literal[False] = ...,
) -> list[ServiceInfo]: ...


@overload
def discover_services(
    url: str,
    *,
    client: ArcGISClient | None = ...,
    service_types: Sequence[str] | None = ...,
    return_folders: Literal[True],
) -> tuple[list[ServiceInfo], list[str]]: ...


def discover_services(
    url: str,
    *,
    client: ArcGISClient | None = None,
    service_types: Sequence[str] | None = None,
    return_folders: bool = False,
) -> list[ServiceInfo] | tuple[list[ServiceInfo], list[str]]:
    """Discover services from an ArcGIS REST services root.

    Fetches the list of available services from a services root URL.
    Optionally filters by service type and returns folder information.

    Args:
        url: Services root URL (e.g., ".../rest/services")
        client: The client with the token and the retry policy.
        service_types: If provided, only return services of these types
            (e.g., ["FeatureServer", "MapServer"])
        return_folders: If True, also return list of folder names

    Returns:
        List of ServiceInfo objects (or tuple with folders if return_folders=True)

    Raises:
        ArcGISDiscoveryError: If the request fails or response is invalid
    """
    data = (client or ArcGISClient()).get_json(url)

    services = _build_service_list(data, service_types)

    if return_folders:
        folders = data.get("folders", [])
        return services, folders

    return services


@dataclass(frozen=True)
class FolderTraversal:
    """Record of a recursive services-root traversal.

    Attributes:
        visited: Folder names successfully fetched.
        skipped: (folder_name, reason) pairs for folders that errored.
        service_count: Total services discovered (root plus all folders).
    """

    visited: list[str]
    skipped: list[tuple[str, str]]
    service_count: int


def _build_service_list(
    data: dict[str, Any],
    service_types: Sequence[str] | None,
) -> list[ServiceInfo]:
    """Build a filtered ServiceInfo list from a services-root JSON payload."""
    services: list[ServiceInfo] = []
    for service_data in data.get("services", []):
        service = ServiceInfo(name=service_data["name"], service_type=service_data["type"])
        if service_types is None or service.service_type in service_types:
            services.append(service)
    return services


def discover_services_recursive(
    url: str,
    *,
    client: ArcGISClient | None = None,
    service_types: Sequence[str] | None = None,
    max_depth: int = 2,
) -> tuple[list[ServiceInfo], FolderTraversal]:
    """Discover services from a services root, recursing into folders.

    ArcGIS returns service names already qualified by folder (e.g.
    "NationalDatasets/Property"), so ServiceInfo.get_url(root) stays correct.
    Folders that error (secured, non-200, embedded error) are recorded and
    skipped, never raised. max_depth guards against pathological nesting;
    standard ArcGIS folders are single-level. Defaults to 2 because standard
    ArcGIS servers nest folders at most one level; the extra level guards against
    non-standard but valid configurations.

    Returns:
        (services, traversal) where traversal records visited/skipped folders.
    """
    client = client or ArcGISClient()
    root_base = url.split("?")[0].rstrip("/")
    root_data = client.get_json(root_base)

    services = _build_service_list(root_data, service_types)
    visited: list[str] = []
    skipped: list[tuple[str, str]] = []

    # Queue of (folder_name, depth). Folder names are paths relative to root.
    queue: list[tuple[str, int]] = [(f, 1) for f in root_data.get("folders", [])]
    while queue:
        folder, depth = queue.pop(0)
        if depth > max_depth:
            continue
        folder_url = f"{root_base}/{folder}"
        try:
            folder_data = client.get_json(folder_url)
        except ArcGISDiscoveryError as e:
            skipped.append((folder, str(e)))
            logger.warning("Skipping folder '%s': %s", folder, e)
            continue
        visited.append(folder)
        services.extend(_build_service_list(folder_data, service_types))
        queue.extend((f"{folder}/{sub}", depth + 1) for sub in folder_data.get("folders", []))

    traversal = FolderTraversal(visited=visited, skipped=skipped, service_count=len(services))
    return services, traversal


def fetch_layer_details(
    service_url: str,
    layer_id: int,
    client: ArcGISClient | None = None,
) -> dict[str, Any]:
    """Fetch detailed information for a specific layer.

    Gets full layer metadata including fields, extent, and geometry type.
    This is used to extract field aliases and other layer-specific metadata.

    Args:
        service_url: FeatureServer or MapServer URL
        layer_id: Numeric layer ID
        client: The client with the token and the retry policy.

    Returns:
        Raw layer info dictionary from ArcGIS API

    Raises:
        ArcGISDiscoveryError: If the request fails or response is invalid
    """
    layer_url = f"{service_url.rstrip('/')}/{layer_id}"
    return (client or ArcGISClient()).get_json(layer_url)
