"""``resolve_href`` resolves a relative href with rashid's ``resolve_local_href`` rule."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from portolan_cli.stac_links import resolve_href

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("href", ["./a.json#part", "./a.json?v=2", "sub/../a.json"])
def test_the_href_names_the_file_rashid_resolves(tmp_path: Path, href: str) -> None:
    """Rashid drops the query and the fragment and normalizes the path."""
    assert resolve_href(tmp_path, href) == tmp_path / "a.json"
