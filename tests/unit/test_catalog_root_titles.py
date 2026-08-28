"""Unit tests for the root catalog title override (issue #815).

The root ``.portolan/metadata.yaml`` may carry ``title`` and ``description``.
``apply_catalog_human_titles`` copies them onto the root ``catalog.json``, the
way ``apply_human_titles`` already does for a collection.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from portolan_cli.catalog import apply_catalog_human_titles


def _catalog(root: Path, **overrides: object) -> None:
    data: dict[str, object] = {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "id": "phl-housing-demo",
        "title": "Phl Housing Demo",
        "description": "A Portolan-managed STAC catalog",
        "links": [],
    }
    data.update(overrides)
    root.mkdir(parents=True, exist_ok=True)
    (root / "catalog.json").write_text(json.dumps(data), encoding="utf-8")


def _metadata(root: Path, data: dict[str, object]) -> None:
    portolan = root / ".portolan"
    portolan.mkdir(parents=True, exist_ok=True)
    (portolan / "metadata.yaml").write_text(yaml.dump(data), encoding="utf-8")


@pytest.mark.unit
class TestApplyCatalogHumanTitles:
    def test_title_and_description_reach_catalog_json(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        _metadata(
            tmp_path,
            {
                "license": "CC-BY-4.0",
                "title": "Philadelphia Housing and Land Use",
                "description": "Ten collections about property and zoning.",
            },
        )

        assert apply_catalog_human_titles(tmp_path) is True

        catalog = json.loads((tmp_path / "catalog.json").read_text())
        assert catalog["title"] == "Philadelphia Housing and Land Use"
        assert catalog["description"] == "Ten collections about property and zoning."

    def test_title_alone_leaves_description(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        _metadata(tmp_path, {"title": "Philadelphia Housing"})

        assert apply_catalog_human_titles(tmp_path) is True

        catalog = json.loads((tmp_path / "catalog.json").read_text())
        assert catalog["title"] == "Philadelphia Housing"
        assert catalog["description"] == "A Portolan-managed STAC catalog"

    def test_blank_values_leave_catalog_untouched(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        _metadata(tmp_path, {"title": "   ", "description": ""})

        assert apply_catalog_human_titles(tmp_path) is False

        catalog = json.loads((tmp_path / "catalog.json").read_text())
        assert catalog["title"] == "Phl Housing Demo"
        assert catalog["description"] == "A Portolan-managed STAC catalog"

    def test_no_metadata_yaml_leaves_catalog_untouched(self, tmp_path: Path) -> None:
        _catalog(tmp_path)

        assert apply_catalog_human_titles(tmp_path) is False

        catalog = json.loads((tmp_path / "catalog.json").read_text())
        assert catalog["title"] == "Phl Housing Demo"

    def test_second_run_reports_no_change(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        _metadata(tmp_path, {"title": "Philadelphia Housing"})

        assert apply_catalog_human_titles(tmp_path) is True
        assert apply_catalog_human_titles(tmp_path) is False

    def test_non_string_values_are_ignored(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        _metadata(tmp_path, {"title": 42, "description": ["a", "b"]})

        assert apply_catalog_human_titles(tmp_path) is False

        catalog = json.loads((tmp_path / "catalog.json").read_text())
        assert catalog["title"] == "Phl Housing Demo"

    def test_subcatalog_keeps_its_own_title(self, tmp_path: Path) -> None:
        """The root title must not overwrite a subcatalog title.

        ``load_merged_metadata`` merges a parent file into a child, so a
        subcatalog with no title of its own inherits the root one. The sweep
        therefore touches the root ``catalog.json`` alone.
        """
        _catalog(tmp_path)
        _metadata(tmp_path, {"title": "Philadelphia Housing"})
        sub = tmp_path / "zoning"
        sub.mkdir()
        (sub / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "stac_version": "1.1.0",
                    "id": "zoning",
                    "title": "Zoning",
                    "description": "Catalog: zoning",
                    "links": [],
                }
            ),
            encoding="utf-8",
        )

        apply_catalog_human_titles(tmp_path)

        subcatalog = json.loads((sub / "catalog.json").read_text())
        assert subcatalog["title"] == "Zoning"

    def test_missing_catalog_json_is_a_no_op(self, tmp_path: Path) -> None:
        _metadata(tmp_path, {"title": "Philadelphia Housing"})

        assert apply_catalog_human_titles(tmp_path) is False

    def test_unreadable_catalog_json_is_a_no_op(self, tmp_path: Path) -> None:
        tmp_path.mkdir(parents=True, exist_ok=True)
        (tmp_path / "catalog.json").write_text("{not json", encoding="utf-8")
        _metadata(tmp_path, {"title": "Philadelphia Housing"})

        assert apply_catalog_human_titles(tmp_path) is False


@pytest.mark.unit
class TestTitleDoesNotInherit:
    """title/description name one object and never inherit (issue #815)."""

    def test_collection_does_not_take_the_root_title(self, tmp_path: Path) -> None:
        from portolan_cli.config import load_merged_metadata

        _metadata(tmp_path, {"license": "CC-BY-4.0", "title": "Philadelphia Housing"})
        collection = tmp_path / "dor_parcel"
        collection.mkdir()

        merged = load_merged_metadata(collection, tmp_path)

        assert "title" not in merged
        # Everything else still inherits.
        assert merged["license"] == "CC-BY-4.0"

    def test_collection_keeps_its_own_title(self, tmp_path: Path) -> None:
        from portolan_cli.config import load_merged_metadata

        _metadata(tmp_path, {"license": "CC-BY-4.0", "title": "Philadelphia Housing"})
        collection = tmp_path / "dor_parcel"
        _metadata(collection, {"title": "Property Parcels"})

        merged = load_merged_metadata(collection, tmp_path)

        assert merged["title"] == "Property Parcels"
        assert merged["license"] == "CC-BY-4.0"

    def test_root_keeps_its_own_title(self, tmp_path: Path) -> None:
        from portolan_cli.config import load_merged_metadata

        _metadata(tmp_path, {"title": "Philadelphia Housing", "description": "Ten collections."})

        merged = load_merged_metadata(tmp_path, tmp_path)

        assert merged["title"] == "Philadelphia Housing"
        assert merged["description"] == "Ten collections."

    def test_description_does_not_inherit(self, tmp_path: Path) -> None:
        from portolan_cli.config import load_merged_metadata

        _metadata(tmp_path, {"description": "Ten collections."})
        collection = tmp_path / "dor_parcel"
        collection.mkdir()

        merged = load_merged_metadata(collection, tmp_path)

        assert "description" not in merged
