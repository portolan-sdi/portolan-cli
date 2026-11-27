"""The ``cleared_environ`` helper in tests/conftest.py."""

from __future__ import annotations

import os
import ssl
import sys

import pytest

from tests.conftest import cleared_environ

pytestmark = pytest.mark.unit


def test_ssl_context_builds_in_a_cleared_environment() -> None:
    with cleared_environ():
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    assert context.protocol == ssl.PROTOCOL_TLS_CLIENT


@pytest.mark.skipif(sys.platform != "win32", reason="Windows system variables")
def test_windows_system_variables_survive() -> None:
    expected = os.environ["SYSTEMROOT"]

    with cleared_environ():
        assert os.environ["SYSTEMROOT"] == expected


def test_home_variables_do_not_survive(monkeypatch: pytest.MonkeyPatch) -> None:
    """botocore finds ~/.aws through these, so the helper must drop them."""
    for key in ("HOME", "USERPROFILE", "HOMEDRIVE", "HOMEPATH"):
        monkeypatch.setenv(key, "sentinel-home")

    with cleared_environ():
        kept = {
            key for key in ("HOME", "USERPROFILE", "HOMEDRIVE", "HOMEPATH") if key in os.environ
        }

    assert kept == set()
