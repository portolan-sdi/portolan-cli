"""``get_backend("iceberg")`` builds the backend from the environment, with no network."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit


def test_get_backend_returns_an_iceberg_backend_when_the_uri_is_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from portolan_cli.backends import get_backend
    from portolan_cli.backends.iceberg import IcebergBackend

    # A port nothing listens on: construction must not connect.
    monkeypatch.setenv("PORTOLAN_ICEBERG_URI", "http://127.0.0.1:9")

    backend = get_backend("iceberg", catalog_root=tmp_path)

    assert isinstance(backend, IcebergBackend)


def test_get_backend_without_the_uri_names_the_variable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from portolan_cli.backends import get_backend

    monkeypatch.delenv("PORTOLAN_ICEBERG_URI", raising=False)

    with pytest.raises(ValueError, match="PORTOLAN_ICEBERG_URI") as info:
        get_backend("iceberg", catalog_root=tmp_path)

    assert str(info.value).startswith("The iceberg backend needs PORTOLAN_ICEBERG_URI")


def test_iceberg_backend_satisfies_the_versioning_protocol() -> None:
    from portolan_cli.backends.iceberg import IcebergBackend
    from portolan_cli.backends.protocol import VersioningBackend

    backend = IcebergBackend.__new__(IcebergBackend)

    assert isinstance(backend, VersioningBackend)
