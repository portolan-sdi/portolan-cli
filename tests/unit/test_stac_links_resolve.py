"""Link href resolution in ``stac_links``: ``resolve_href`` and ``published_files``."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from portolan_core.stac_links import published_files, resolve_href

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("href", ["./a.json#part", "./a.json?v=2", "sub/../a.json"])
def test_the_href_names_the_file_rashid_resolves(tmp_path: Path, href: str) -> None:
    """Rashid drops the query and the fragment and normalizes the path."""
    assert resolve_href(tmp_path, href) == tmp_path / "a.json"


@pytest.mark.parametrize("href", ["%2e%2e/item.json", "%2E%2E/item.json"])
def test_an_encoded_parent_segment_reaches_the_parent(tmp_path: Path, href: str) -> None:
    """Rashid gives None for an href that leaves base_dir, and the old rule decodes it."""
    group = tmp_path / "collection" / "group"

    assert resolve_href(group, href) == (tmp_path / "collection" / "item.json").resolve()


@pytest.mark.parametrize("href", ["../../outside.parquet", "./%2e%2e/%2e%2e/outside.parquet"])
def test_an_href_that_leaves_the_catalog_names_no_published_file(href: str) -> None:
    """Push and clone transfer files of the catalog only."""
    document = {"assets": {"data": {"href": href}}, "links": [{"rel": "item", "href": href}]}

    files, items = published_files(document, "roads/collection.json")

    assert files == {"roads/README.md", "roads/AGENTS.md"}
    assert items == set()
