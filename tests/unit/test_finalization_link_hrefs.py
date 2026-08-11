"""STAC link hrefs stay POSIX no matter what the host OS calls a path separator.

A STAC href is a relative URL reference, so ``..\\catalog.json`` is not a
"Windows spelling" of ``../catalog.json`` — it is a filename containing
backslashes, and rashid's ``PTL-LNK-006`` (link resolution) correctly reports it
as pointing nowhere. That is what broke the Windows CI job: the root/parent links
of every collection were built with ``os.path.relpath``, which returns the native
separator.

``PureWindowsPath`` inputs reproduce the Windows computation on any host.

The same rule catches a second way a link can point nowhere useful: naming a
real file that is the wrong object. ``root``, ``parent``, and ``collection`` are
three different targets, and the classes below hold each to its own (issue
#711).
"""

from __future__ import annotations

import json
from pathlib import Path, PureWindowsPath

import pytest

from portolan_cli.finalization import (
    _fix_collection_links,
    _fix_item_links,
    relative_root_href,
)

pytestmark = pytest.mark.unit


class TestRelativeRootHref:
    def test_windows_paths_still_produce_a_posix_href(self) -> None:
        href = relative_root_href(
            PureWindowsPath(r"C:\Users\runner\catalog"),
            PureWindowsPath(r"C:\Users\runner\catalog\roads"),
        )
        assert href == "../catalog.json"

    def test_windows_nested_collection_walks_up_in_posix(self) -> None:
        href = relative_root_href(
            PureWindowsPath(r"C:\catalog"),
            PureWindowsPath(r"C:\catalog\climate\hittekaart"),
        )
        assert href == "../../catalog.json"

    def test_posix_paths_are_unchanged(self) -> None:
        href = relative_root_href(Path("/srv/catalog"), Path("/srv/catalog/roads"))
        assert href == "../catalog.json"


def _write_collection(path: Path, links: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "type": "Collection",
                "stac_version": "1.1.0",
                "id": "roads",
                "description": "Roads.",
                "links": links,
            }
        ),
        encoding="utf-8",
    )


def _hrefs(path: Path, rel: str) -> list[str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return [link["href"] for link in data["links"] if link.get("rel") == rel]


class TestFixCollectionLinks:
    _collection = staticmethod(_write_collection)
    _hrefs = staticmethod(_hrefs)

    def test_root_and_parent_links_are_written_relative(self, tmp_path: Path) -> None:
        collection_dir = tmp_path / "roads"
        collection_json = collection_dir / "collection.json"
        self._collection(collection_json, [])

        _fix_collection_links(collection_json, tmp_path, collection_dir)

        assert self._hrefs(collection_json, "root") == ["../catalog.json"]
        assert self._hrefs(collection_json, "parent") == ["../catalog.json"]

    def test_an_existing_root_link_is_repointed(self, tmp_path: Path) -> None:
        collection_dir = tmp_path / "climate" / "hittekaart"
        collection_json = collection_dir / "collection.json"
        self._collection(collection_json, [{"rel": "root", "href": "./collection.json"}])

        _fix_collection_links(collection_json, tmp_path, collection_dir)

        assert self._hrefs(collection_json, "root") == ["../../catalog.json"]


class TestParentLinkTargetsTheContainingCatalog:
    """Issue #711: ``parent`` is the immediate container, never the root.

    A nested collection sits under an intermediate ``catalog.json`` that
    ``create_intermediate_catalogs`` writes for it, so its parent is always one
    level up. Before the fix ``parent`` was a copy of the root href, which
    walked past that intermediate catalog to the repository root and made the
    tree unwalkable upward (rashid ``PTL-LNK-006``).
    """

    _collection = staticmethod(_write_collection)
    _hrefs = staticmethod(_hrefs)

    def test_nested_collection_parent_stops_at_the_intermediate_catalog(
        self, tmp_path: Path
    ) -> None:
        collection_dir = tmp_path / "boundaries" / "adm1-attributes"
        collection_json = collection_dir / "collection.json"
        self._collection(collection_json, [])

        _fix_collection_links(collection_json, tmp_path, collection_dir)

        # Pre-fix this was "../../catalog.json", the root catalog.
        assert self._hrefs(collection_json, "parent") == ["../catalog.json"]
        assert self._hrefs(collection_json, "root") == ["../../catalog.json"]

    def test_parent_is_one_level_up_however_deep_the_collection_sits(self, tmp_path: Path) -> None:
        collection_dir = tmp_path / "env" / "air" / "quality"
        collection_json = collection_dir / "collection.json"
        self._collection(collection_json, [])

        _fix_collection_links(collection_json, tmp_path, collection_dir)

        # Pre-fix this was "../../../catalog.json".
        assert self._hrefs(collection_json, "parent") == ["../catalog.json"]
        assert self._hrefs(collection_json, "root") == ["../../../catalog.json"]

    def test_an_existing_wrong_parent_link_is_repointed(self, tmp_path: Path) -> None:
        collection_dir = tmp_path / "boundaries" / "adm1-attributes"
        collection_json = collection_dir / "collection.json"
        # PySTAC writes this href when it saves a standalone collection, so the
        # pre-fix "append only when absent" branch never corrected it.
        self._collection(collection_json, [{"rel": "parent", "href": "../../catalog.json"}])

        _fix_collection_links(collection_json, tmp_path, collection_dir)

        assert self._hrefs(collection_json, "parent") == ["../catalog.json"]

    def test_a_repointed_parent_link_carries_its_media_type(self, tmp_path: Path) -> None:
        collection_dir = tmp_path / "boundaries" / "adm1-attributes"
        collection_json = collection_dir / "collection.json"
        self._collection(collection_json, [{"rel": "parent", "href": "../../catalog.json"}])

        _fix_collection_links(collection_json, tmp_path, collection_dir)

        data = json.loads(collection_json.read_text(encoding="utf-8"))
        parent = next(link for link in data["links"] if link["rel"] == "parent")
        # PTL-LNK-003: every structural link carries application/json.
        assert parent["type"] == "application/json"


class TestFixItemLinks:
    """Issue #711: an item's ``root`` is the catalog root, not its collection.

    ``add`` loads the collection standalone, so PySTAC never sees a parent
    catalog and resolves each item's root to the collection itself. Nothing in
    the add path repaired that; only ``check --fix`` did.
    """

    @staticmethod
    def _write_item(path: Path, links: list[dict[str, str]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "type": "Feature",
                    "stac_version": "1.1.0",
                    "id": path.stem,
                    "geometry": None,
                    "properties": {"datetime": None},
                    "assets": {},
                    "links": links,
                }
            ),
            encoding="utf-8",
        )

    def test_item_root_points_at_the_catalog_root(self, tmp_path: Path) -> None:
        collection_dir = tmp_path / "env" / "air"
        collection_json = collection_dir / "collection.json"
        _write_collection(collection_json, [{"rel": "item", "href": "./scenes/scenes.json"}])
        item_json = collection_dir / "scenes" / "scenes.json"
        # The shape PySTAC emits today.
        self._write_item(
            item_json,
            [
                {"rel": "root", "href": "../collection.json"},
                {"rel": "collection", "href": "../collection.json"},
                {"rel": "parent", "href": "../collection.json"},
            ],
        )

        _fix_item_links(collection_json, tmp_path, collection_dir)

        # Pre-fix root was "../collection.json", the collection itself.
        assert _hrefs(item_json, "root") == ["../../../catalog.json"]
        assert _hrefs(item_json, "parent") == ["../collection.json"]
        assert _hrefs(item_json, "collection") == ["../collection.json"]

    def test_missing_structural_links_are_added(self, tmp_path: Path) -> None:
        collection_dir = tmp_path / "roads"
        collection_json = collection_dir / "collection.json"
        _write_collection(collection_json, [{"rel": "item", "href": "./a/a.json"}])
        item_json = collection_dir / "a" / "a.json"
        self._write_item(item_json, [])

        _fix_item_links(collection_json, tmp_path, collection_dir)

        assert _hrefs(item_json, "root") == ["../../catalog.json"]
        assert _hrefs(item_json, "parent") == ["../collection.json"]
        assert _hrefs(item_json, "collection") == ["../collection.json"]

    def test_item_links_carry_their_media_types(self, tmp_path: Path) -> None:
        collection_dir = tmp_path / "roads"
        collection_json = collection_dir / "collection.json"
        _write_collection(collection_json, [{"rel": "item", "href": "./a/a.json"}])
        item_json = collection_dir / "a" / "a.json"
        self._write_item(item_json, [])

        _fix_item_links(collection_json, tmp_path, collection_dir)

        data = json.loads(item_json.read_text(encoding="utf-8"))
        types = {link["rel"]: link["type"] for link in data["links"]}
        assert types == {
            "root": "application/json",
            "parent": "application/json",
            "collection": "application/json",
        }

    def test_a_missing_item_file_is_skipped(self, tmp_path: Path) -> None:
        collection_dir = tmp_path / "roads"
        collection_json = collection_dir / "collection.json"
        _write_collection(collection_json, [{"rel": "item", "href": "./gone/gone.json"}])

        _fix_item_links(collection_json, tmp_path, collection_dir)  # must not raise

        assert not (collection_dir / "gone").exists()
