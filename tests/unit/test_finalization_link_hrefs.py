"""STAC link hrefs stay POSIX no matter what the host OS calls a path separator.

A STAC href is a relative URL reference, so ``..\\catalog.json`` is not a
"Windows spelling" of ``../catalog.json`` — it is a filename containing
backslashes, and rashid's ``PTL-LNK-006`` (link resolution) correctly reports it
as pointing nowhere. That is what broke the Windows CI job: the root/parent links
of every collection were built with ``os.path.relpath``, which returns the native
separator.

``PureWindowsPath`` inputs reproduce the Windows computation on any host.
"""

from __future__ import annotations

import json
from pathlib import Path, PureWindowsPath

import pytest

from portolan_cli.finalization import _fix_collection_links, relative_root_href

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


class TestFixCollectionLinks:
    @staticmethod
    def _collection(path: Path, links: list[dict[str, str]]) -> None:
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

    @staticmethod
    def _hrefs(path: Path, rel: str) -> list[str]:
        data = json.loads(path.read_text(encoding="utf-8"))
        return [link["href"] for link in data["links"] if link.get("rel") == rel]

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
