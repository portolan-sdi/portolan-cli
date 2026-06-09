"""Tests for ArcGIS authentication module.

Covers token pass-through, username/password token minting via generateToken,
and error paths when minting fails.
"""

from __future__ import annotations

import httpx
import pytest

from portolan_cli.errors import ArcGISAuthError
from portolan_cli.extract.arcgis.auth import (
    ArcGISCredentials,
    apply_token,
    resolve_token,
)

pytestmark = pytest.mark.unit


@pytest.mark.unit
def test_apply_token_appends_param() -> None:
    assert "token=T" in apply_token("https://x/rest/services?f=json", "T")


@pytest.mark.unit
def test_resolve_token_returns_explicit_token() -> None:
    creds = ArcGISCredentials(token="EXPLICIT")
    assert resolve_token(creds, "https://x/rest/services") == "EXPLICIT"


@pytest.mark.unit
def test_resolve_token_none_when_no_credentials() -> None:
    assert resolve_token(ArcGISCredentials(), "https://x/rest/services") is None


@pytest.mark.unit
def test_resolve_token_mints_from_username_password(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(self: object, url: str) -> httpx.Response:
        # /rest/info returns the token services URL
        return httpx.Response(
            200,
            json={"authInfo": {"tokenServicesUrl": "https://x/portal/sharing/rest/generateToken"}},
        )

    def fake_post(self: object, url: str, data: dict[str, str] | None = None) -> httpx.Response:
        assert data is not None
        assert data["username"] == "u" and data["password"] == "p"
        return httpx.Response(200, json={"token": "MINTED", "expires": 1})

    monkeypatch.setattr(httpx.Client, "get", fake_get)
    monkeypatch.setattr(httpx.Client, "post", fake_post)
    creds = ArcGISCredentials(username="u", password="p")
    assert resolve_token(creds, "https://x/server/rest/services") == "MINTED"


@pytest.mark.unit
def test_resolve_token_raises_on_mint_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(self: object, url: str) -> httpx.Response:
        return httpx.Response(
            200,
            json={"authInfo": {"tokenServicesUrl": "https://x/generateToken"}},
        )

    def fake_post(self: object, url: str, data: dict[str, str] | None = None) -> httpx.Response:
        return httpx.Response(200, json={"error": {"code": 400, "message": "Invalid credentials"}})

    monkeypatch.setattr(httpx.Client, "get", fake_get)
    monkeypatch.setattr(httpx.Client, "post", fake_post)
    with pytest.raises(ArcGISAuthError, match="Invalid credentials"):
        resolve_token(
            ArcGISCredentials(username="u", password="p"), "https://x/server/rest/services"
        )
