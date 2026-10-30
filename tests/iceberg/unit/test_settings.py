"""The Iceberg backend reads its REST catalog from ``PORTOLAN_ICEBERG_*`` variables.

Environment only, no config.yaml key, so ``init --backend iceberg`` can read
the catalog before the config file exists. Secrets never reach ``repr``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from portolan_cli.backends.iceberg.settings import (
    IcebergSettingsError,
    load_settings,
)

if TYPE_CHECKING:
    from pathlib import Path

_ICEBERG_VARS = (
    "PORTOLAN_ICEBERG_URI",
    "PORTOLAN_ICEBERG_WAREHOUSE",
    "PORTOLAN_ICEBERG_NAMESPACE",
    "PORTOLAN_ICEBERG_AUTH",
    "PORTOLAN_ICEBERG_OAUTH2_SERVER_URI",
    "PORTOLAN_ICEBERG_STORAGE",
    "PORTOLAN_ICEBERG_CLIENT_ID",
    "PORTOLAN_ICEBERG_CLIENT_SECRET",
    "PORTOLAN_ICEBERG_TOKEN",
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
def _clean_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for name in _ICEBERG_VARS:
        monkeypatch.delenv(name, raising=False)
    # No AWS files from the developer's machine.
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "none-credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "none-config"))


# The fixture applies to every test here. usefixtures with the function's
# __name__ keeps a reference vulture can see, which autouse=True would hide.
pytestmark = [pytest.mark.unit, pytest.mark.usefixtures(_clean_environment.__name__)]


def test_the_uri_is_required_and_the_error_names_it() -> None:
    with pytest.raises(IcebergSettingsError) as info:
        load_settings()

    assert "PORTOLAN_ICEBERG_URI" in str(info.value)


def test_defaults_for_the_apache_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORTOLAN_ICEBERG_URI", "http://localhost:8181")

    settings = load_settings()

    assert settings.uri == "http://localhost:8181"
    assert settings.warehouse is None
    assert settings.namespace == "portolake"
    assert settings.auth == "none"
    assert settings.oauth2_server_uri is None
    assert settings.storage.kind == "s3"
    assert settings.storage.endpoint is None
    assert settings.storage.url_style == "vhost"


def test_a_trailing_slash_leaves_the_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORTOLAN_ICEBERG_URI", "https://catalog.lakehousebox.com/")

    assert load_settings().uri == "https://catalog.lakehousebox.com"


def test_a_client_id_selects_oauth2_with_the_default_token_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PORTOLAN_ICEBERG_URI", "https://catalog.lakehousebox.com")
    monkeypatch.setenv("PORTOLAN_ICEBERG_WAREHOUSE", "carto-sdi--portolan-testbed")
    monkeypatch.setenv("PORTOLAN_ICEBERG_CLIENT_ID", "id-1")
    monkeypatch.setenv("PORTOLAN_ICEBERG_CLIENT_SECRET", "secret-1")

    settings = load_settings()

    assert settings.auth == "oauth2"
    assert settings.client_id == "id-1"
    assert settings.client_secret == "secret-1"
    assert settings.oauth2_server_uri == "https://catalog.lakehousebox.com/v1/oauth/tokens"
    assert settings.warehouse == "carto-sdi--portolan-testbed"


def test_an_explicit_token_endpoint_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORTOLAN_ICEBERG_URI", "https://catalog.example.org")
    monkeypatch.setenv("PORTOLAN_ICEBERG_CLIENT_ID", "id-1")
    monkeypatch.setenv("PORTOLAN_ICEBERG_CLIENT_SECRET", "secret-1")
    monkeypatch.setenv("PORTOLAN_ICEBERG_OAUTH2_SERVER_URI", "https://auth.example.org/token")

    assert load_settings().oauth2_server_uri == "https://auth.example.org/token"


def test_oauth2_without_a_secret_names_the_missing_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PORTOLAN_ICEBERG_URI", "https://catalog.example.org")
    monkeypatch.setenv("PORTOLAN_ICEBERG_CLIENT_ID", "id-1")

    with pytest.raises(IcebergSettingsError) as info:
        load_settings()

    assert "PORTOLAN_ICEBERG_CLIENT_SECRET" in str(info.value)


def test_a_token_selects_bearer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "PORTOLAN_ICEBERG_URI", "https://biglake.googleapis.com/iceberg/v1/restcatalog"
    )
    monkeypatch.setenv("PORTOLAN_ICEBERG_TOKEN", "ya29.token")

    settings = load_settings()

    assert settings.auth == "bearer"
    assert settings.token == "ya29.token"


def test_bearer_without_a_token_names_the_missing_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PORTOLAN_ICEBERG_URI", "https://catalog.example.org")
    monkeypatch.setenv("PORTOLAN_ICEBERG_AUTH", "bearer")

    with pytest.raises(IcebergSettingsError) as info:
        load_settings()

    assert "PORTOLAN_ICEBERG_TOKEN" in str(info.value)


@pytest.mark.parametrize(
    ("variable", "value", "allowed"),
    [
        ("PORTOLAN_ICEBERG_AUTH", "basic", "oauth2, bearer, none"),
        ("PORTOLAN_ICEBERG_STORAGE", "azure", "s3, vended, gcs"),
    ],
)
def test_an_unknown_choice_lists_the_allowed_values(
    monkeypatch: pytest.MonkeyPatch, variable: str, value: str, allowed: str
) -> None:
    monkeypatch.setenv("PORTOLAN_ICEBERG_URI", "https://catalog.example.org")
    monkeypatch.setenv(variable, value)

    with pytest.raises(IcebergSettingsError) as info:
        load_settings()

    message = str(info.value)
    assert variable in message
    assert value in message
    assert allowed in message


def test_an_empty_namespace_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORTOLAN_ICEBERG_URI", "https://catalog.example.org")
    monkeypatch.setenv("PORTOLAN_ICEBERG_NAMESPACE", " ")

    with pytest.raises(IcebergSettingsError) as info:
        load_settings()

    assert "PORTOLAN_ICEBERG_NAMESPACE" in str(info.value)


def test_s3_settings_come_from_the_upload_conventions(monkeypatch: pytest.MonkeyPatch) -> None:
    """The same shell setup serves the STAC upload and the warehouse."""
    monkeypatch.setenv("PORTOLAN_ICEBERG_URI", "http://localhost:8181")
    monkeypatch.setenv("PORTOLAN_S3_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("PORTOLAN_S3_USE_SSL", "false")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "rustfsadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "rustfssecret")
    monkeypatch.setenv("AWS_REGION", "us-east-1")

    storage = load_settings().storage

    assert storage.endpoint == "localhost:9000"
    assert storage.use_ssl is False
    assert storage.url_style == "path"
    assert storage.key_id == "rustfsadmin"
    assert storage.secret == "rustfssecret"
    assert storage.session_token is None
    assert storage.region == "us-east-1"


def test_the_endpoint_scheme_is_dropped(monkeypatch: pytest.MonkeyPatch) -> None:
    """DuckDB's S3 secret takes a host, and USE_SSL carries the scheme."""
    monkeypatch.setenv("PORTOLAN_ICEBERG_URI", "https://catalog.lakehousebox.com")
    monkeypatch.setenv("PORTOLAN_S3_ENDPOINT", "https://s3.lakehousebox.com")

    storage = load_settings().storage

    assert storage.endpoint == "s3.lakehousebox.com"
    assert storage.use_ssl is True


def test_the_client_pair_is_the_s3_key_when_no_aws_key_is_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LakehouseBox issues one key pair for the catalog and its buckets."""
    monkeypatch.setenv("PORTOLAN_ICEBERG_URI", "https://catalog.lakehousebox.com")
    monkeypatch.setenv("PORTOLAN_ICEBERG_CLIENT_ID", "id-1")
    monkeypatch.setenv("PORTOLAN_ICEBERG_CLIENT_SECRET", "secret-1")
    monkeypatch.setenv("PORTOLAN_S3_ENDPOINT", "s3.lakehousebox.com")

    storage = load_settings().storage

    assert storage.key_id == "id-1"
    assert storage.secret == "secret-1"


def test_an_aws_key_wins_over_the_client_pair(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORTOLAN_ICEBERG_URI", "https://catalog.lakehousebox.com")
    monkeypatch.setenv("PORTOLAN_ICEBERG_CLIENT_ID", "id-1")
    monkeypatch.setenv("PORTOLAN_ICEBERG_CLIENT_SECRET", "secret-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA1")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws-secret")

    storage = load_settings().storage

    assert storage.key_id == "AKIA1"
    assert storage.secret == "aws-secret"


def test_vended_storage_takes_no_key_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PORTOLAN_ICEBERG_URI", "https://catalog.lakehousebox.com")
    monkeypatch.setenv("PORTOLAN_ICEBERG_TOKEN", "t")
    monkeypatch.setenv("PORTOLAN_ICEBERG_STORAGE", "vended")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA1")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws-secret")

    storage = load_settings().storage

    assert storage.kind == "vended"
    assert storage.key_id is None
    assert storage.secret is None


def test_secrets_stay_out_of_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    """A traceback or a log line that prints the settings must not leak a key."""
    monkeypatch.setenv("PORTOLAN_ICEBERG_URI", "https://catalog.lakehousebox.com")
    monkeypatch.setenv("PORTOLAN_ICEBERG_CLIENT_ID", "id-1")
    monkeypatch.setenv("PORTOLAN_ICEBERG_CLIENT_SECRET", "very-secret")
    monkeypatch.setenv("PORTOLAN_ICEBERG_TOKEN", "very-token")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "very-session")

    text = repr(load_settings())

    assert "very-secret" not in text
    assert "very-token" not in text
    assert "very-session" not in text
    assert "id-1" in text
