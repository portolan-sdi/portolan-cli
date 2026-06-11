"""Unit tests for mandatory human-readable titles (Issue #502).

Covers the MandatoryTitlesRule, the check --fix repair helper, creation-time
defaults, and the metadata.yaml override.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.metadata.fix import repair_titles_and_links
from portolan_cli.stac import apply_human_titles, create_collection
from portolan_cli.validation.results import Severity
from portolan_cli.validation.stac_rules import MandatoryTitlesRule, _is_raw_slug_title


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
class TestIsRawSlugTitle:
    def test_underscore_is_raw(self) -> None:
        assert _is_raw_slug_title("publico_arbolado") is True

    def test_namespace_prefix_is_raw(self) -> None:
        assert _is_raw_slug_title("ns:LayerName") is True

    def test_humanized_is_not_raw(self) -> None:
        assert _is_raw_slug_title("Publico Arbolado") is False

    def test_short_token_is_not_raw(self) -> None:
        # Cannot be humanized further; must not loop check --fix.
        assert _is_raw_slug_title("T502") is False


@pytest.mark.unit
class TestMandatoryTitlesRule:
    def test_severity_is_error(self) -> None:
        assert MandatoryTitlesRule().severity is Severity.ERROR

    def test_fails_on_slug_catalog(self, tmp_path: Path) -> None:
        _slug_catalog(tmp_path)
        result = MandatoryTitlesRule().check(tmp_path)
        assert result.passed is False
        # Flags: catalog missing title, child link missing title,
        # collection raw-slug title, collection missing description.
        assert "title" in result.message.lower()

    def test_passes_after_repair(self, tmp_path: Path) -> None:
        _slug_catalog(tmp_path)
        repair_titles_and_links(tmp_path)
        result = MandatoryTitlesRule().check(tmp_path)
        assert result.passed is True, result.message


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
