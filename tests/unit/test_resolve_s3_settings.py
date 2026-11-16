"""``resolve_s3_settings`` exposes the S3 resolution that ``upload_file`` uses.

The Iceberg backend builds a DuckDB S3 secret from it, so one shell setup
serves the STAC upload and the warehouse. The function composes the private
resolvers of ``sync/upload.py`` and adds nothing of its own.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from portolan_cli.sync.upload import S3Settings, resolve_s3_settings

if TYPE_CHECKING:
    from pathlib import Path

_VARS = (
    "PORTOLAN_S3_ENDPOINT",
    "PORTOLAN_S3_USE_SSL",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_PROFILE",
    "AWS_ENDPOINT_URL",
    "AWS_ENDPOINT_URL_S3",
)


@pytest.fixture
def _clean(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for name in _VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "config"))


# The fixture applies to every test here. usefixtures with the function's
# __name__ keeps a reference vulture can see, which autouse=True would hide.
pytestmark = [pytest.mark.unit, pytest.mark.usefixtures(_clean.__name__)]


def test_nothing_set_gives_an_empty_result() -> None:
    assert resolve_s3_settings() == S3Settings(
        endpoint=None,
        use_ssl=True,
        region=None,
        access_key=None,
        secret_key=None,
        session_token=None,
    )


def test_environment_values_are_read(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORTOLAN_S3_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("PORTOLAN_S3_USE_SSL", "false")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "session")
    monkeypatch.setenv("AWS_REGION", "eu-west-1")

    assert resolve_s3_settings() == S3Settings(
        endpoint="localhost:9000",
        use_ssl=False,
        region="eu-west-1",
        access_key="key",
        secret_key="secret",
        session_token="session",
    )


def test_a_profile_supplies_keys_endpoint_and_region(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "credentials").write_text(
        "[lhb]\naws_access_key_id = pk\naws_secret_access_key = ps\n", encoding="utf-8"
    )
    (tmp_path / "config").write_text(
        "[profile lhb]\nregion = us-east-1\nendpoint_url = https://s3.example.org\n",
        encoding="utf-8",
    )

    settings = resolve_s3_settings(profile="lhb")

    assert settings.access_key == "pk"
    assert settings.secret_key == "ps"
    assert settings.region == "us-east-1"
    assert settings.endpoint == "https://s3.example.org"
    assert settings.use_ssl is True


def test_the_bucket_name_can_supply_the_region() -> None:
    assert resolve_s3_settings(bucket="us-west-2.opendata.source.coop").region == "us-west-2"


def test_the_secret_stays_out_of_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "very-secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "very-session")

    text = repr(resolve_s3_settings())

    assert "very-secret" not in text
    assert "very-session" not in text
    assert "key" in text
