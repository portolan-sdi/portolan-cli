"""Token providers for the Iceberg REST catalog.

Three modes: none (the e2e stack), a static bearer (a token an identity
provider minted), and OAuth2 client credentials. Each one also tells the
DuckDB session which ``CREATE SECRET (TYPE iceberg)`` options to write.
"""

from __future__ import annotations

from urllib.parse import parse_qs

import httpx
import pytest

from portolan_cli.backends.iceberg.auth import (
    NoAuth,
    OAuth2ClientCredentials,
    StaticBearer,
    TokenError,
)

pytestmark = pytest.mark.unit

TOKEN_URL = "https://catalog.example.org/v1/oauth/tokens"


class _FakeClock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now


class _TokenServer:
    """Answer the token endpoint and count the requests."""

    def __init__(self, *, status: int = 200, expires_in: int | None = 3600) -> None:
        self.status = status
        self.expires_in = expires_in
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.status != 200:
            return httpx.Response(self.status, json={"error": "invalid_client"})
        body: dict[str, object] = {"access_token": f"tok-{len(self.requests)}"}
        if self.expires_in is not None:
            body["expires_in"] = self.expires_in
        return httpx.Response(200, json=body)


def _provider(
    server: _TokenServer, clock: _FakeClock, *, scope: str | None = None
) -> OAuth2ClientCredentials:
    return OAuth2ClientCredentials(
        TOKEN_URL,
        client_id="id-1",
        client_secret="secret-1",
        scope=scope,
        http=httpx.Client(transport=httpx.MockTransport(server)),
        clock=clock,
    )


def test_no_auth_sends_nothing_and_writes_no_secret() -> None:
    provider = NoAuth()

    assert provider.headers() == {}
    assert provider.duckdb_secret_options() == {}


def test_static_bearer_sends_the_token() -> None:
    provider = StaticBearer("ya29.token")

    assert provider.headers() == {"Authorization": "Bearer ya29.token"}
    assert provider.duckdb_secret_options() == {"TOKEN": "ya29.token"}


def test_oauth2_posts_the_client_credentials_grant() -> None:
    server = _TokenServer()

    headers = _provider(server, _FakeClock(), scope="catalog").headers()

    assert headers == {"Authorization": "Bearer tok-1"}
    (request,) = server.requests
    assert request.method == "POST"
    assert str(request.url) == TOKEN_URL
    assert request.headers["content-type"] == "application/x-www-form-urlencoded"
    assert parse_qs(request.content.decode()) == {
        "grant_type": ["client_credentials"],
        "client_id": ["id-1"],
        "client_secret": ["secret-1"],
        "scope": ["catalog"],
    }


def test_oauth2_sends_no_scope_when_none_is_set() -> None:
    server = _TokenServer()

    _provider(server, _FakeClock()).headers()

    assert "scope" not in parse_qs(server.requests[0].content.decode())


def test_oauth2_caches_the_token_until_a_minute_before_it_expires() -> None:
    server = _TokenServer(expires_in=600)
    clock = _FakeClock()
    provider = _provider(server, clock)

    first = provider.headers()
    clock.now += 539
    second = provider.headers()
    clock.now += 2
    third = provider.headers()

    assert first == second == {"Authorization": "Bearer tok-1"}
    assert third == {"Authorization": "Bearer tok-2"}
    assert len(server.requests) == 2


def test_oauth2_assumes_an_hour_when_the_server_gives_no_expiry() -> None:
    server = _TokenServer(expires_in=None)
    clock = _FakeClock()
    provider = _provider(server, clock)

    provider.headers()
    clock.now += 3600 - 61
    provider.headers()
    clock.now += 2
    provider.headers()

    assert len(server.requests) == 2


def test_invalidate_forces_a_new_token() -> None:
    """The REST client calls this on a 401, then retries once."""
    server = _TokenServer()
    provider = _provider(server, _FakeClock())

    provider.headers()
    provider.invalidate()

    assert provider.headers() == {"Authorization": "Bearer tok-2"}


def test_a_refused_grant_names_the_endpoint_and_status_but_not_the_secret() -> None:
    server = _TokenServer(status=401)

    with pytest.raises(TokenError) as info:
        _provider(server, _FakeClock()).headers()

    message = str(info.value)
    assert TOKEN_URL in message
    assert "401" in message
    assert "secret-1" not in message


def test_a_response_without_a_token_is_an_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"token_type": "bearer"})

    provider = OAuth2ClientCredentials(
        TOKEN_URL,
        client_id="id-1",
        client_secret="secret-1",
        http=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    with pytest.raises(TokenError, match="access_token"):
        provider.headers()


def test_oauth2_writes_the_duckdb_secret_options() -> None:
    provider = _provider(_TokenServer(), _FakeClock())

    assert provider.duckdb_secret_options() == {
        "CLIENT_ID": "id-1",
        "CLIENT_SECRET": "secret-1",
        "OAUTH2_SERVER_URI": TOKEN_URL,
    }


def test_oauth2_with_a_scope_writes_it_for_duckdb() -> None:
    provider = _provider(_TokenServer(), _FakeClock(), scope="catalog")

    assert provider.duckdb_secret_options()["OAUTH2_SCOPE"] == "catalog"


def test_the_secret_stays_out_of_repr() -> None:
    provider = _provider(_TokenServer(), _FakeClock())

    assert "secret-1" not in repr(provider)
    assert "secret-1" not in repr(StaticBearer("secret-1"))
