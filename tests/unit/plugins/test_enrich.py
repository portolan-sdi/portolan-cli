"""Tests for the STAC enrichment hooks on files (#947)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

from portolan_cli.hookspecs import EnrichContext, hookimpl
from portolan_cli.plugins.manager import PluginWarning
from portolan_cli.stac_enrich import enrich_collection_file, enrich_item_file

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

pytestmark = pytest.mark.unit

_SCI = "https://stac-extensions.github.io/scientific/v1.0.0/schema.json"


def _item(path: Path) -> dict[str, Any]:
    doc = {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": "a",
        "properties": {"datetime": None},
        "links": [{"rel": "root", "href": "../../catalog.json"}],
        "assets": {},
    }
    path.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    return doc


def _context(path: Path) -> EnrichContext:
    return EnrichContext(
        catalog_root=path.parents[2],
        collection_id="roads",
        document_path=path,
        item_id="a",
    )


class _Sci:
    @hookimpl
    def portolan_enrich_item(self, item: dict[str, Any], context: EnrichContext) -> None:
        item.setdefault("stac_extensions", []).append(_SCI)
        item["properties"]["sci:doi"] = f"10.0/{context.collection_id}"

    @hookimpl
    def portolan_enrich_collection(
        self, collection: dict[str, Any], context: EnrichContext
    ) -> None:
        collection["sci:citation"] = "Example 2026"


class _Rewriter:
    @hookimpl
    def portolan_enrich_item(self, item: dict[str, Any]) -> None:
        item["id"] = "renamed"
        item["links"] = []
        item["properties"]["rewriter:ran"] = True


class _Raiser:
    @hookimpl
    def portolan_enrich_item(self, item: dict[str, Any]) -> None:
        item["properties"]["raiser:partial"] = True
        raise RuntimeError("lookup failed")


@pytest.fixture
def item_path(tmp_path: Path) -> Path:
    path = tmp_path / "catalog" / "roads" / "a" / "a.json"
    path.parent.mkdir(parents=True)
    return path


def test_no_hook_leaves_the_file_untouched(
    register_plugin: Callable[..., None], item_path: Path
) -> None:
    """With no enrichment plugin, the bytes on disk stay the same."""
    _item(item_path)
    before = item_path.read_bytes()

    enrich_item_file(item_path, _context(item_path))

    assert item_path.read_bytes() == before


def test_plugin_fields_are_written(register_plugin: Callable[..., None], item_path: Path) -> None:
    """A hook's changes reach the file."""
    expected = _item(item_path)
    register_plugin(_Sci(), "sci")

    enrich_item_file(item_path, _context(item_path))

    expected["stac_extensions"] = [_SCI]
    expected["properties"]["sci:doi"] = "10.0/roads"
    assert json.loads(item_path.read_text(encoding="utf-8")) == expected


def test_collection_hook_receives_the_collection(
    register_plugin: Callable[..., None], tmp_path: Path
) -> None:
    """The collection hook runs on collection.json."""
    path = tmp_path / "collection.json"
    path.write_text(json.dumps({"type": "Collection", "id": "roads", "links": []}))
    register_plugin(_Sci(), "sci")

    enrich_collection_file(
        path, EnrichContext(catalog_root=tmp_path, collection_id="roads", document_path=path)
    )

    assert json.loads(path.read_text(encoding="utf-8"))["sci:citation"] == "Example 2026"


def test_structural_fields_are_restored_with_a_warning(
    register_plugin: Callable[..., None], item_path: Path
) -> None:
    """A hook cannot change `id` or `links`. Its other changes stay."""
    expected = _item(item_path)
    register_plugin(_Rewriter(), "rewriter")

    with pytest.warns(PluginWarning, match=r"'rewriter' changed id, links"):
        enrich_item_file(item_path, _context(item_path))

    expected["properties"]["rewriter:ran"] = True
    assert json.loads(item_path.read_text(encoding="utf-8")) == expected


def test_failing_hook_is_rolled_back_and_others_run(
    register_plugin: Callable[..., None], item_path: Path
) -> None:
    """A hook that raises leaves no partial change. The next plugin still runs."""
    expected = _item(item_path)
    register_plugin(_Raiser(), "raiser")
    register_plugin(_Sci(), "sci")

    with pytest.warns(PluginWarning, match=r"'raiser' failed in portolan_enrich_item: lookup"):
        enrich_item_file(item_path, _context(item_path))

    expected["stac_extensions"] = [_SCI]
    expected["properties"]["sci:doi"] = "10.0/roads"
    assert json.loads(item_path.read_text(encoding="utf-8")) == expected
