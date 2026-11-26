"""The href rule the CLI takes from ``rashid_core.api.is_absolute_href``.

The CLI skips an absolute href instead of resolving it against the catalog
directory. These cases pin what it relies on, so a rashid release that changes
the rule fails here and not in a fixer.
"""

from __future__ import annotations

import pytest
from rashid_core.api import is_absolute_href

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
