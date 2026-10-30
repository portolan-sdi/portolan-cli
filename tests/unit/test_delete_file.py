"""``delete_file`` removes one object through the same store setup as ``upload_file``.

The Iceberg backend's prune calls it for data files that no kept snapshot
names. obstore is allowed in ``sync.upload`` only, so the helper lives there.
"""

from __future__ import annotations

from typing import Any

import pytest

import portolan_cli.sync.upload as upload

pytestmark = pytest.mark.unit


class _Store:
    pass


def test_delete_file_deletes_the_key_inside_the_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, Any] = {}
    store = _Store()

    def fake_setup(
        bucket_url: str, profile: str | None, chunk_concurrency: int, **kwargs: Any
    ) -> tuple[_Store, dict[str, int]]:
        calls["setup"] = (bucket_url, profile, chunk_concurrency, kwargs)
        return store, {}

    def fake_delete(target: object, key: str) -> None:
        calls["delete"] = (target, key)

    monkeypatch.setattr(upload, "_setup_store_and_kwargs", fake_setup)
    monkeypatch.setattr("portolan_cli.sync.upload.obs.delete", fake_delete)

    upload.delete_file(
        "s3://warehouse/portolake/t/data/x.parquet",
        profile="p",
        s3_endpoint="localhost:9000",
        s3_region="us-east-1",
        s3_use_ssl=False,
    )

    assert calls["setup"] == (
        "s3://warehouse",
        "p",
        1,
        {"s3_endpoint": "localhost:9000", "s3_region": "us-east-1", "s3_use_ssl": False},
    )
    assert calls["delete"] == (store, "portolake/t/data/x.parquet")


def test_delete_file_refuses_a_bucket_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(upload, "_setup_store_and_kwargs", lambda *a, **k: (_Store(), {}))

    with pytest.raises(ValueError, match="names no object"):
        upload.delete_file("s3://warehouse")


def test_delete_file_refuses_an_unsupported_scheme() -> None:
    with pytest.raises(ValueError, match="Unsupported URL scheme"):
        upload.delete_file("ftp://x/y")
