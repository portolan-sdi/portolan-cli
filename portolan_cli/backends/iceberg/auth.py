"""Token providers for the Iceberg REST catalog.

The REST client asks a provider for its request headers, and the DuckDB
session asks the same provider for the options of its
``CREATE SECRET (TYPE iceberg, ...)``, so both speak to the catalog with one
credential. Three providers cover the catalogs the backend targets:

- ``NoAuth`` for a catalog with no authentication, such as the e2e stack.
- ``StaticBearer`` for an access token an identity provider minted, which
  the caller passes in whole.
- ``OAuth2ClientCredentials`` for the client credentials grant the Iceberg
  REST specification defines, at ``/v1/oauth/tokens`` by default.

The OAuth2 provider caches its token and fetches a new one a minute before
it expires, or at once after ``invalidate()``, which the REST client calls
on a 401. No provider puts a secret in ``repr`` or in an error message.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import httpx

if TYPE_CHECKING:
    from collections.abc import Callable

    from portolan_cli.backends.iceberg.settings import IcebergSettings

#: Seconds before the expiry at which the OAuth2 provider fetches a new token.
REFRESH_MARGIN = 60.0
#: Token lifetime assumed when the server sends no ``expires_in``.
DEFAULT_LIFETIME = 3600.0


class TokenError(RuntimeError):
    """The token endpoint refused the grant or answered without a token."""


@runtime_checkable
class TokenProvider(Protocol):
    """What the REST client and the DuckDB session need from a credential."""

    def headers(self) -> dict[str, str]:
        """Return the headers to send with a catalog request."""
        ...

    def duckdb_secret_options(self) -> dict[str, str]:
        """Return the options of ``CREATE SECRET (TYPE iceberg, ...)``."""
        ...


class NoAuth:
    """No credential. The Apache REST fixture accepts every request."""

    def headers(self) -> dict[str, str]:
        """Return no headers."""
        return {}

    def duckdb_secret_options(self) -> dict[str, str]:
        """Return no options. The session attaches with ``AUTHORIZATION_TYPE 'none'``."""
        return {}


class StaticBearer:
    """A bearer token minted outside the backend."""

    def __init__(self, token: str) -> None:
        self._token = token

    def __repr__(self) -> str:
        """Name the class and hide the token."""
        return "StaticBearer(token='***')"

    def headers(self) -> dict[str, str]:
        """Return the bearer header."""
        return {"Authorization": f"Bearer {self._token}"}

    def duckdb_secret_options(self) -> dict[str, str]:
        """Return the token for DuckDB's iceberg secret."""
        return {"TOKEN": self._token}


class OAuth2ClientCredentials:
    """The OAuth2 client credentials grant, cached until the token expires.

    Args:
        token_url: The token endpoint.
        client_id: The client id.
        client_secret: The client secret.
        scope: An optional scope to request.
        http: The HTTP client to post with. The default has a 30 s timeout.
        clock: A monotonic clock, replaceable in tests.
    """

    def __init__(
        self,
        token_url: str,
        *,
        client_id: str,
        client_secret: str,
        scope: str | None = None,
        http: httpx.Client | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._http = http or httpx.Client(timeout=30.0)
        self._clock = clock
        self._token: str | None = None
        self._expires_at = 0.0

    def __repr__(self) -> str:
        """Name the endpoint and the client id, and hide the secret."""
        return (
            f"OAuth2ClientCredentials(token_url={self._token_url!r}, "
            f"client_id={self._client_id!r}, client_secret='***')"
        )

    def headers(self) -> dict[str, str]:
        """Return the bearer header, fetching a token when none is valid."""
        if self._token is None or self._clock() >= self._expires_at - REFRESH_MARGIN:
            self._fetch()
        return {"Authorization": f"Bearer {self._token}"}

    def invalidate(self) -> None:
        """Drop the cached token, so the next call fetches a new one."""
        self._token = None

    def duckdb_secret_options(self) -> dict[str, str]:
        """Return the client credentials for DuckDB's iceberg secret."""
        options = {
            "CLIENT_ID": self._client_id,
            "CLIENT_SECRET": self._client_secret,
            "OAUTH2_SERVER_URI": self._token_url,
        }
        if self._scope:
            options["OAUTH2_SCOPE"] = self._scope
        return options

    def _fetch(self) -> None:
        form = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }
        if self._scope:
            form["scope"] = self._scope
        try:
            response = self._http.post(self._token_url, data=form)
        except httpx.HTTPError as error:
            raise TokenError(
                f"The token endpoint {self._token_url} did not answer: {error}"
            ) from error
        if response.status_code != httpx.codes.OK:
            raise TokenError(
                f"The token endpoint {self._token_url} answered {response.status_code}."
            )
        body = response.json()
        token = body.get("access_token") if isinstance(body, dict) else None
        if not isinstance(token, str) or not token:
            raise TokenError(f"The token endpoint {self._token_url} answered without access_token.")
        lifetime = body.get("expires_in", DEFAULT_LIFETIME)
        self._token = token
        self._expires_at = self._clock() + float(lifetime)


def token_provider(settings: IcebergSettings, *, http: httpx.Client | None = None) -> TokenProvider:
    """Build the provider that the settings name.

    Args:
        settings: The backend settings.
        http: The HTTP client for the OAuth2 grant, or None for a default.

    Returns:
        The provider.
    """
    if settings.auth == "oauth2":
        # load_settings guarantees the three values under oauth2.
        return OAuth2ClientCredentials(
            settings.oauth2_server_uri or "",
            client_id=settings.client_id or "",
            client_secret=settings.client_secret or "",
            http=http,
        )
    if settings.auth == "bearer":
        return StaticBearer(settings.token or "")
    return NoAuth()
