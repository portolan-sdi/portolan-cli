"""Unit tests for mandatory human-readable titles (Issue #502).

Covers the check --fix repair helper, creation-time defaults, and the
metadata.yaml override. The rule itself (PTL-TTL-001/002/003) now lives in
rashid, which owns its tests.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from portolan_cli.catalog import ensure_link_titles
from portolan_cli.metadata.fix import repair_titles_and_links
from portolan_cli.metadata_yaml import _validate_title_description
from portolan_cli.stac import apply_human_titles, create_collection

if TYPE_CHECKING:
    from pathlib import Path


def _write(path: Path, data: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


def _slug_catalog(root: Path) -> None:
    """A catalog whose collection/links lack human-readable titles."""
    _write(
        root / "catalog.json",
        {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "demo",
            "description": "d",
            "links": [
                {
                    "rel": "child",
                    "href": "./publico_arbolado/collection.json",
                }
            ],
        },
    )
    _write(
        root / "publico_arbolado" / "collection.json",
        {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "publico_arbolado",
            "title": "publico_arbolado",  # raw slug
            "description": "",
            "links": [{"rel": "item", "href": "./tree_census/tree_census.json"}],
        },
    )
    _write(
        root / "publico_arbolado" / "tree_census" / "tree_census.json",
        {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "tree_census",
            "properties": {},
            "links": [],
        },
    )


@pytest.mark.unit
class TestRepairTitlesAndLinks:
    def test_humanizes_and_backfills_links(self, tmp_path: Path) -> None:
        _slug_catalog(tmp_path)
        repair_titles_and_links(tmp_path)

        catalog = json.loads((tmp_path / "catalog.json").read_text())
        coll = json.loads((tmp_path / "publico_arbolado" / "collection.json").read_text())

        # Collection title humanized; description defaulted to it.
        assert coll["title"] == "Publico Arbolado"
        assert coll["description"] == "Publico Arbolado"
        # Child link carries the target's title + type.
        child = catalog["links"][0]
        assert child["title"] == "Publico Arbolado"
        assert child["type"] == "application/json"
        # Item link carries the (now-titled) item's title.
        item_link = coll["links"][0]
        assert item_link["title"] == "Tree Census"
        assert item_link["type"] == "application/geo+json"

    def test_is_idempotent(self, tmp_path: Path) -> None:
        _slug_catalog(tmp_path)
        repair_titles_and_links(tmp_path)
        # Second run changes nothing.
        second = repair_titles_and_links(tmp_path)
        assert second == []

    def test_dry_run_writes_nothing(self, tmp_path: Path) -> None:
        _slug_catalog(tmp_path)
        before = (tmp_path / "publico_arbolado" / "collection.json").read_text()
        results = repair_titles_and_links(tmp_path, dry_run=True)
        after = (tmp_path / "publico_arbolado" / "collection.json").read_text()
        assert results  # reports what it would change
        assert before == after

    def test_preserves_existing_human_title(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "catalog.json",
            {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "demo",
                "title": "My Lovely Catalog",
                "description": "A real description",
                "links": [],
            },
        )
        repair_titles_and_links(tmp_path)
        catalog = json.loads((tmp_path / "catalog.json").read_text())
        assert catalog["title"] == "My Lovely Catalog"
        assert catalog["description"] == "A real description"


@pytest.mark.unit
class TestEnsureLinkTitlesTraversal:
    def test_ignores_href_outside_catalog_root(self, tmp_path: Path) -> None:
        """A ``../`` href that escapes the catalog is not read/backfilled."""
        catalog_root = tmp_path / "catalog"
        # A sensitive file with a title, living OUTSIDE the catalog.
        _write(tmp_path / "outside.json", {"title": "Secret"})
        _write(
            catalog_root / "catalog.json",
            {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "demo",
                "title": "Demo",
                "description": "d",
                "links": [{"rel": "child", "href": "../outside.json"}],
            },
        )

        changed = ensure_link_titles(catalog_root)

        catalog = json.loads((catalog_root / "catalog.json").read_text())
        # The outside title must NOT leak into the catalog link.
        assert "title" not in catalog["links"][0] or catalog["links"][0].get("title") != "Secret"
        assert changed in (True, False)  # may set type, but never the foreign title


@pytest.mark.unit
class TestEnsureLinkTitlesUnicode:
    def test_accented_title_roundtrips(self, tmp_path: Path) -> None:
        """Accented (UTF-8) titles backfill correctly regardless of OS locale.

        Regression: STAC files are UTF-8; a bare read_text() uses the platform
        locale (cp1252 on Windows) and chokes on byte 0x81 of e.g. 'Á'.
        """
        # Write collection.json as UTF-8 with an accented Spanish title.
        coll_path = tmp_path / "areas" / "collection.json"
        coll_path.parent.mkdir(parents=True)
        coll_path.write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "stac_version": "1.0.0",
                    "id": "areas",
                    "title": "Áreas Programáticas",
                    "description": "d",
                    "links": [],
                }
            ),
            encoding="utf-8",
        )
        _write(
            tmp_path / "catalog.json",
            {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "demo",
                "title": "Demo",
                "description": "d",
                "links": [{"rel": "child", "href": "./areas/collection.json"}],
            },
        )

        ensure_link_titles(tmp_path)

        catalog = json.loads((tmp_path / "catalog.json").read_text(encoding="utf-8"))
        assert catalog["links"][0]["title"] == "Áreas Programáticas"


def _encoded_catalog(root: Path) -> None:
    """A catalog whose link hrefs are percent-encoded, as rashid writes them."""
    _write(
        root / "catalog.json",
        {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "demo",
            "title": "Demo",
            "description": "d",
            "links": [{"rel": "child", "href": "./%C3%A9t%C3%A9/collection.json"}],
        },
    )
    _write(
        root / "été" / "collection.json",
        {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "ete",
            "title": "Summer",
            "description": "d",
            "links": [{"rel": "item", "href": "./tree%20census/tree%20census.json"}],
        },
    )
    _write(
        root / "été" / "tree census" / "tree census.json",
        {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "tree_census",
            "properties": {},
            "links": [],
        },
    )


@pytest.mark.unit
class TestPercentEncodedLinks:
    def test_the_child_link_gets_the_title_of_the_decoded_target(self, tmp_path: Path) -> None:
        _encoded_catalog(tmp_path)

        ensure_link_titles(tmp_path)

        catalog = json.loads((tmp_path / "catalog.json").read_text(encoding="utf-8"))
        assert catalog["links"][0]["title"] == "Summer"

    def test_the_item_of_an_encoded_link_gets_a_title(self, tmp_path: Path) -> None:
        _encoded_catalog(tmp_path)

        repair_titles_and_links(tmp_path)

        item = json.loads((tmp_path / "été" / "tree census" / "tree census.json").read_text())
        assert item["properties"]["title"] == "Tree Census"


@pytest.mark.unit
def test_an_item_link_outside_the_catalog_is_not_rewritten(tmp_path: Path) -> None:
    """The title repair writes only inside the catalog root."""
    outside = tmp_path / "outside.json"
    _write(outside, {"type": "Feature", "id": "x", "properties": {}, "links": []})
    before = outside.read_text()
    catalog_root = tmp_path / "catalog"
    _write(
        catalog_root / "catalog.json",
        {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "demo",
            "title": "Demo",
            "description": "d",
            "links": [{"rel": "item", "href": "../outside.json"}],
        },
    )

    repair_titles_and_links(catalog_root)

    assert outside.read_text() == before


@pytest.mark.unit
class TestValidateTitleDescription:
    def test_blank_string_is_accepted_as_absent(self) -> None:
        assert _validate_title_description({"title": "", "description": "   "}) == []

    def test_omitted_is_accepted(self) -> None:
        assert _validate_title_description({}) == []

    def test_non_string_is_rejected(self) -> None:
        errors = _validate_title_description({"title": 123})
        assert errors and "title" in errors[0]


@pytest.mark.unit
class TestCreateCollectionDefaults:
    def test_derives_title_and_description(self) -> None:
        coll = create_collection(
            collection_id="publico_arbolado",
            description="",
        )
        assert coll.title == "Publico Arbolado"
        assert coll.description == "Publico Arbolado"

    def test_respects_explicit_title(self) -> None:
        coll = create_collection(
            collection_id="publico_arbolado",
            description="Real description",
            title="Arbolado Público",
        )
        assert coll.title == "Arbolado Público"
        assert coll.description == "Real description"


@pytest.mark.unit
class TestApplyHumanTitles:
    def test_override_wins(self) -> None:
        coll = create_collection(collection_id="publico_arbolado", description="")
        apply_human_titles(
            coll, {"title": "Árboles de la Ciudad", "description": "Censo de arbolado"}
        )
        assert coll.title == "Árboles de la Ciudad"
        assert coll.description == "Censo de arbolado"

    def test_blank_override_ignored(self) -> None:
        coll = create_collection(collection_id="publico_arbolado", description="")
        apply_human_titles(coll, {"title": "  ", "description": None})
        assert coll.title == "Publico Arbolado"


def _catalog_through_symlink(tmp_path: Path, rel: str, href: str) -> tuple[Path, Path]:
    """A catalog whose ``alias`` directory is a symlink to a directory outside it."""
    outside = tmp_path / "outside"
    _write(
        outside / "item.json",
        {"type": "Feature", "id": "x", "title": "Secret", "properties": {}, "links": []},
    )
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()
    (catalog_root / "alias").symlink_to(outside, target_is_directory=True)
    _write(
        catalog_root / "catalog.json",
        {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "demo",
            "title": "Demo",
            "description": "d",
            "links": [{"rel": rel, "href": href}],
        },
    )
    return catalog_root, outside / "item.json"


@pytest.mark.unit
class TestSymlinkOutOfTheCatalog:
    def test_the_item_title_repair_does_not_write_through_the_symlink(self, tmp_path: Path) -> None:
        catalog_root, outside = _catalog_through_symlink(tmp_path, "item", "./alias/%69tem.json")
        before = outside.read_text()

        repair_titles_and_links(catalog_root)

        assert outside.read_text() == before

    def test_the_link_title_backfill_does_not_read_through_the_symlink(
        self, tmp_path: Path
    ) -> None:
        catalog_root, _outside = _catalog_through_symlink(tmp_path, "child", "./alias/item.json")

        ensure_link_titles(catalog_root)

        catalog = json.loads((catalog_root / "catalog.json").read_text())
        assert "title" not in catalog["links"][0]


@pytest.mark.unit
class TestAbsoluteLocalHrefs:
    """An absolute filesystem href inside the catalog names its file."""

    def test_the_child_link_gets_the_title_of_the_target(self, tmp_path: Path) -> None:
        _slug_catalog(tmp_path)
        catalog_json = tmp_path / "catalog.json"
        catalog = json.loads(catalog_json.read_text())
        catalog["links"][0]["href"] = str(tmp_path / "publico_arbolado" / "collection.json")
        _write(catalog_json, catalog)
        collection_json = tmp_path / "publico_arbolado" / "collection.json"
        collection = json.loads(collection_json.read_text())
        collection["title"] = "Arbolado"
        _write(collection_json, collection)

        ensure_link_titles(tmp_path)

        assert json.loads(catalog_json.read_text())["links"][0]["title"] == "Arbolado"

    def test_the_item_gets_a_title(self, tmp_path: Path) -> None:
        _slug_catalog(tmp_path)
        collection_json = tmp_path / "publico_arbolado" / "collection.json"
        item_json = tmp_path / "publico_arbolado" / "tree_census" / "tree_census.json"
        collection = json.loads(collection_json.read_text())
        collection["links"][0]["href"] = str(item_json)
        _write(collection_json, collection)

        repair_titles_and_links(tmp_path)

        assert json.loads(item_json.read_text())["properties"]["title"] == "Tree Census"
