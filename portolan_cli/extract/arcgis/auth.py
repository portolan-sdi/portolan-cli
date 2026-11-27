"""ArcGIS authentication, minimal token pass-through."""

from __future__ import annotations

from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, cast

import httpx

from portolan_cli.extract.arcgis.discovery import ensure_json_format
from portolan_cli.extract.common.urls import redact_credentials
from portolan_core.errors import ArcGISAuthError


@dataclass
class ArcGISCredentials:
    """ArcGIS credentials. token takes precedence over username/password."""

    token: str | None = None
    username: str | None = None
    password: str | None = None

    @property
    def is_empty(self) -> bool:
        """True when no usable credentials are present."""
        return not self.token and not (self.username and self.password)


def _server_root(base_url: str) -> str:
    """Return the ArcGIS server root (everything before /rest/), case-insensitively."""
    idx = base_url.lower().find("/rest/")
    root = base_url if idx < 0 else base_url[:idx]
    return root.rstrip("/")


def _token_services_url(base_url: str, timeout: float) -> str:
    """Discover the generateToken endpoint from <server>/rest/info."""
    info_url = _server_root(base_url) + "/rest/info"
    # Each message names the server without the credential of its URL.
    root = redact_credentials(info_url)
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.get(ensure_json_format(info_url))
    except (httpx.HTTPError, ValueError) as exc:
        raise ArcGISAuthError(
            f"Failed to read token services URL from {root}: {exc}", url=root
        ) from exc

    if resp.status_code >= HTTPStatus.BAD_REQUEST:
        raise ArcGISAuthError(
            f"Failed to read token services URL from {root}: HTTP {resp.status_code}", url=root
        )

    try:
        raw = resp.json()
    except ValueError as exc:
        raise ArcGISAuthError(
            f"Failed to read token services URL from {root}: {exc}", url=root
        ) from exc
    if not isinstance(raw, dict):
        raise ArcGISAuthError(f"Invalid JSON object from {root}", url=root)
    data = cast("dict[str, Any]", raw)

    auth_info = data.get("authInfo")
    token_url = auth_info.get("tokenServicesUrl") if isinstance(auth_info, dict) else None
    if not isinstance(token_url, str) or not token_url:
        raise ArcGISAuthError(f"Server does not advertise a token endpoint at {root}", url=root)
    return token_url


def resolve_token(
    creds: ArcGISCredentials,
    base_url: str,
    timeout: float = 60.0,
) -> str | None:
    """Resolve a usable token from credentials."""
    if creds.token:
        return creds.token
    if not (creds.username and creds.password):
        return None

    token_url = _token_services_url(base_url, timeout)
    referer = _server_root(base_url)
    payload: dict[str, str] = {
        "username": creds.username,
        "password": creds.password,
        "client": "referer",
        "referer": referer,
        "expiration": "60",  # TODO: make configurable, long runs can exceed 60 min
        "f": "json",
    }

    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.post(token_url, data=payload)
    except (httpx.HTTPError, ValueError) as exc:
        raise ArcGISAuthError(f"Token request failed at {token_url}: {exc}", url=token_url) from exc

    if resp.status_code >= HTTPStatus.BAD_REQUEST:
        raise ArcGISAuthError(
            f"Token request failed at {token_url}: HTTP {resp.status_code}", url=token_url
        )

    try:
        raw = resp.json()
    except ValueError as exc:
        raise ArcGISAuthError(f"Token request failed at {token_url}: {exc}", url=token_url) from exc
    if not isinstance(raw, dict):
        raise ArcGISAuthError(f"Invalid JSON object from {token_url}", url=token_url)
    data = cast("dict[str, Any]", raw)

    error = data.get("error")
    if isinstance(error, dict):
        raise ArcGISAuthError(
            f"Token request rejected: {error.get('message', 'unknown')}", url=token_url
        )
    token = data.get("token")
    if not isinstance(token, str) or not token:
        raise ArcGISAuthError(f"No token in response from {token_url}", url=token_url)
    return token
