"""ArcGIS service and layer discovery."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, cast, overload
from urllib.parse import parse_qsl, urlparse

import httpx

from portolan_cli.extract.common.retry import PERMANENT_NOTE, is_transient_status
from portolan_cli.extract.common.urls import merge_query, redact_credentials

if TYPE_CHECKING:
    from collections.abc import Sequence
from http import HTTPStatus

logger = logging.getLogger(__name__)


class ArcGISDiscoveryError(Exception):
    """Error during ArcGIS service/layer discovery."""


class ArcGISTransientError(ArcGISDiscoveryError):
    """A discovery failure that a retry can change, such as HTTP 503."""


class _EmptyListingError(ArcGISTransientError):
    """A service answer that lists no layers and no tables."""

    def __init__(self, url: str, data: dict[str, Any]) -> None:
        super().__init__(f"{url} lists no layers and no tables")
        self.data = data


NO_QUERY_SKIP_REASON = "skipped: no Query capability (tiles-only)"


@dataclass(frozen=True)
class LayerInfo:
    """Information about a single layer in an ArcGIS service."""

    id: int
    name: str
    layer_type: str


@dataclass(frozen=True)
class ServiceInfo:
    """Information about an ArcGIS service."""

    name: str
    service_type: str

    def get_url(self, base_url: str) -> str:
        """Generate the full service URL from a base services URL."""
        # Remove trailing slash if present
        base = base_url.rstrip("/")
        return f"{base}/{self.name}/{self.service_type}"


@dataclass
class ServiceDiscoveryResult:
    """Result of discovering layers from a service."""

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


@dataclass(frozen=True)
class ArcGISClient:
    """The one HTTP client for ArcGIS discovery."""

    timeout: float = 60.0
    # repr=False: a client repr reaches tracebacks and logs.
    token: str | None = field(default=None, repr=False)
    retries: int = 3
    retry_delay: float = 1.0
    transport: httpx.BaseTransport | None = None

    def get_json(self, url: str, *, listing: bool = False) -> dict[str, Any]:
        """Fetch ``url`` as JSON, with ``f=json`` and the token, and retry transient failures."""
        from portolan_cli.extract.common.retry import RetryConfig, retry_with_backoff

        outcome = retry_with_backoff(
            self._get_once,
            RetryConfig(max_attempts=max(self.retries, 1), initial_delay=self.retry_delay),
            url,
            listing,
            retry_on=(ArcGISTransientError,),
            on_retry=lambda attempt, err: logger.debug(
                "Retry %d for %s: %s", attempt, redact_credentials(url), err
            ),
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
        shown = redact_credentials(url)  # A message names the service, not its credential.
        try:
            with httpx.Client(timeout=self.timeout, transport=self.transport) as client:
                response = client.get(request_url)
        except httpx.RequestError as e:
            raise ArcGISTransientError(
                redact_credentials(f"Failed to fetch from {shown}: {e}")
            ) from e
        if response.status_code >= HTTPStatus.BAD_REQUEST:
            if is_transient_status(response.status_code):
                raise ArcGISTransientError(
                    f"Failed to fetch from {shown}: HTTP {response.status_code}"
                )
            raise ArcGISDiscoveryError(
                f"Failed to fetch from {shown}: HTTP {response.status_code}. {PERMANENT_NOTE}"
            )
        try:
            raw = response.json()
        except ValueError as e:
            raise ArcGISDiscoveryError(f"Invalid JSON response from {shown}: {e}") from e
        if not isinstance(raw, dict):
            raise ArcGISDiscoveryError(
                f"Expected JSON object from {shown}, got {type(raw).__name__}"
            )
        data = cast("dict[str, Any]", raw)
        embedded = data.get("error")
        if isinstance(embedded, dict):
            code = embedded.get("code", "unknown")
            message = embedded.get("message", "ArcGIS error")
            if is_transient_status(code):
                raise ArcGISTransientError(f"ArcGIS error from {shown}: {code} {message}")
            raise ArcGISDiscoveryError(
                f"ArcGIS error from {shown}: {code} {message}. {PERMANENT_NOTE}"
            )
        if listing and not (data.get("layers") or data.get("tables")):
            raise _EmptyListingError(url, data)
        return data


def _supports_query(capabilities: Any) -> bool:
    """Return False only when the service advertises capabilities without Query."""
    if not isinstance(capabilities, str) or not capabilities.strip():
        return True
    return "query" in {c.strip().lower() for c in capabilities.split(",")}


def discover_layers(url: str, client: ArcGISClient | None = None) -> ServiceDiscoveryResult:
    """Discover layers, tables, and metadata from an ArcGIS FeatureServer or MapServer."""
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
    """Discover services from an ArcGIS REST services root."""
    data = (client or ArcGISClient()).get_json(url)

    services = _build_service_list(data, service_types)

    if return_folders:
        folders = data.get("folders", [])
        return services, folders

    return services


@dataclass(frozen=True)
class FolderTraversal:
    """Record of a recursive services-root traversal."""

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
    """Discover services from a services root, recursing into folders."""
    client = client or ArcGISClient()
    root_base = url.split("?", maxsplit=1)[0].rstrip("/")
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
    """Fetch detailed information for a specific layer."""
    layer_url = f"{service_url.rstrip('/')}/{layer_id}"
    return (client or ArcGISClient()).get_json(layer_url)
