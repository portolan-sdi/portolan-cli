"""Unit tests for ``is_absolute_href``, the CLI's copy of rashid's href rule."""

from __future__ import annotations

import pytest

from portolan_cli.utils import is_absolute_href

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "href",
    [
        "https://example.org/data.parquet",
        "s3://bucket/key.parquet",
        "/abs/path.json",
        # An href that does not parse is never resolved against the catalog.
        "http://[bad",
    ],
)
def test_absolute(href: str) -> None:
    assert is_absolute_href(href)


@pytest.mark.parametrize("href", ["./item.json", "../catalog.json", "data.parquet"])
def test_relative(href: str) -> None:
    assert not is_absolute_href(href)
