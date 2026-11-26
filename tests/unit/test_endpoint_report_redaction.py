"""The endpoint line that sync reports holds no credential."""

from __future__ import annotations

import pytest

from portolan_cli.sync.upload import _resolve_s3_endpoint_settings

pytestmark = pytest.mark.unit


def test_the_endpoint_line_holds_no_credential(monkeypatch: pytest.MonkeyPatch) -> None:
    """A password or token in AWS_ENDPOINT_URL_S3 does not reach the report."""
    monkeypatch.delenv("PORTOLAN_S3_ENDPOINT", raising=False)
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "https://alice:hunter2@s3.example.test?token=private")
    lines: list[tuple[str, str]] = []

    endpoint, _ = _resolve_s3_endpoint_settings(
        None, None, report=lambda level, text: lines.append((level, text))
    )

    assert endpoint is not None and "hunter2" in endpoint  # The store still gets the real URL.
    assert len(lines) == 1
    assert "hunter2" not in lines[0][1]
    assert "private" not in lines[0][1]
    assert "s3.example.test" in lines[0][1]
