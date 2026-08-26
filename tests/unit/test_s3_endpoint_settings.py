"""Tests for S3-compatible endpoint settings.

The settings are environment-only because catalog configuration files are
published with the catalog.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from portolan_cli.config import resolve_s3_endpoint_settings

pytestmark = pytest.mark.unit


def test_s3_endpoint_settings_default_to_public_s3() -> None:
    """Endpoint settings use public S3 defaults when no environment exists."""
    with patch.dict("os.environ", {}, clear=True):
        settings = resolve_s3_endpoint_settings()

    assert settings.endpoint is None
    assert settings.use_ssl is True


def test_s3_endpoint_settings_read_environment() -> None:
    """Endpoint settings read a MinIO endpoint and HTTP mode from the environment."""
    with patch.dict(
        "os.environ",
        {
            "PORTOLAN_S3_ENDPOINT": "minio.example.test:9000",
            "PORTOLAN_S3_USE_SSL": "false",
        },
        clear=True,
    ):
        settings = resolve_s3_endpoint_settings()

    assert settings.endpoint == "minio.example.test:9000"
    assert settings.use_ssl is False


@pytest.mark.parametrize("value", ["", "sometimes", "1"])
def test_s3_endpoint_settings_reject_invalid_ssl_values(value: str) -> None:
    """Endpoint settings reject ambiguous TLS values."""
    with patch.dict("os.environ", {"PORTOLAN_S3_USE_SSL": value}, clear=True):
        with pytest.raises(ValueError, match="PORTOLAN_S3_USE_SSL"):
            resolve_s3_endpoint_settings()


def test_object_store_uses_environment_endpoint() -> None:
    """The common store setup sends endpoint settings to every S3 command path."""
    from portolan_cli.sync.upload import _setup_store_and_kwargs

    with (
        patch.dict(
            "os.environ",
            {
                "PORTOLAN_S3_ENDPOINT": "http://minio.example.test:9000",
                "PORTOLAN_S3_USE_SSL": "false",
            },
            clear=True,
        ),
        patch("portolan_cli.sync.upload.S3Store") as s3_store,
        patch("portolan_cli.sync.upload._load_aws_credentials_from_profile") as credentials,
    ):
        credentials.return_value = (None, None, None, None)
        _setup_store_and_kwargs("s3://example-bucket", "default", chunk_concurrency=4)

    assert s3_store.call_args.args == ("example-bucket",)
    assert s3_store.call_args.kwargs["endpoint"] == "http://minio.example.test:9000"
    assert s3_store.call_args.kwargs["virtual_hosted_style_request"] is False
    assert s3_store.call_args.kwargs["client_options"] == {"allow_http": True}


def test_default_profile_preserves_environment_credentials() -> None:
    """Implicit default profiles do not override environment credentials."""
    from portolan_cli.sync.upload import _setup_store_and_kwargs

    with (
        patch.dict(
            "os.environ",
            {
                "AWS_ACCESS_KEY_ID": "minioadmin",
                "AWS_SECRET_ACCESS_KEY": "minioadmin",
            },
            clear=True,
        ),
        patch("portolan_cli.sync.upload.S3Store") as s3_store,
        patch("portolan_cli.sync.upload._load_aws_credentials_from_profile") as credentials,
    ):
        _setup_store_and_kwargs("s3://example-bucket", "default", chunk_concurrency=4)

    credentials.assert_not_called()
    assert s3_store.call_args.kwargs["access_key_id"] == "minioadmin"
    assert s3_store.call_args.kwargs["secret_access_key"] == "minioadmin"


def test_endpoint_settings_are_sensitive() -> None:
    """Catalog configuration cannot store S3 endpoint settings."""
    from portolan_cli.config import SENSITIVE_SETTINGS

    assert {"s3_endpoint", "s3_use_ssl"} <= SENSITIVE_SETTINGS
