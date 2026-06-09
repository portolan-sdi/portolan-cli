"""ArcGIS authentication, minimal token pass-through.

Supports a pre-generated token or username/password minted via the ArcGIS
generateToken endpoint. The token is appended to discovery requests and passed
to gpio.extract_arcgis. This is a contained subset; the full unified auth design
lives in issue #311.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx

from portolan_cli.errors import ArcGISAuthError


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


def apply_token(url: str, token: str) -> str:
    """Append token=<token> to a URL.

    Note: discovery._append_query_param does the same thing generically;
    consolidate both into a shared url util when #311 reworks auth.
    """
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    params["token"] = [token]
    return urlunparse(parsed._replace(query=urlencode(params, doseq=True)))


def _with_json(url: str) -> str:
    """Add f=json to url if not already set (setdefault semantics)."""
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    params.setdefault("f", ["json"])
    return urlunparse(parsed._replace(query=urlencode(params, doseq=True)))


def _server_root(base_url: str) -> str:
    """Return the ArcGIS server root (everything before /rest/), case-insensitively.

    base_url is e.g. https://host/server/rest/services[/folder]. ArcGIS treats the
    /rest/ segment case-insensitively, and url_parser accepts /REST/ too, so match
    it the same way here. Falls back to the full URL when /rest/ is absent.
    """
    idx = base_url.lower().find("/rest/")
    root = base_url if idx < 0 else base_url[:idx]
    return root.rstrip("/")


def _token_services_url(base_url: str, timeout: float) -> str:
    """Discover the generateToken endpoint from <server>/rest/info."""
    root = _server_root(base_url) + "/rest/info"
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.get(_with_json(root))
    except (httpx.HTTPError, ValueError) as exc:
        raise ArcGISAuthError(
            f"Failed to read token services URL from {root}: {exc}", url=root
        ) from exc

    if resp.status_code >= 400:
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
    """Resolve a usable token from credentials.

    Returns the explicit token, mints one from username/password, or None when
    no credentials are present. Raises ArcGISAuthError when minting fails.
    """
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
        "expiration": "60",  # TODO(#311): make configurable, long runs can exceed 60 min
        "f": "json",
    }

    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.post(token_url, data=payload)
    except (httpx.HTTPError, ValueError) as exc:
        raise ArcGISAuthError(f"Token request failed at {token_url}: {exc}", url=token_url) from exc

    if resp.status_code >= 400:
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
