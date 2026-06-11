"""Integration tests: `portolan add` produces human-readable titles (Issue #502).

Verifies the full add workflow yields:
- a humanized collection title + non-placeholder description,
- a child link in the root catalog carrying that title + type,
- and that a metadata.yaml title/description override wins.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from portolan_cli.cli import cli

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "simple.parquet"


def _init_catalog(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "stac_version": "1.1.0",
                "id": "demo-catalog",
                "title": "Demo Catalog",
                "description": "Demo",
                "links": [],
            }
        )
    )
    portolan_dir = root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text(
        yaml.dump({"version": 1, "statistics": {"enabled": False}})
    )


def _add_slug_collection(root: Path, *, metadata_yaml: dict | None = None) -> Path:
    collection_dir = root / "publico_areas_programaticas_desarrollo_social"
    collection_dir.mkdir(parents=True)
    if metadata_yaml is not None:
        portolan = collection_dir / ".portolan"
        portolan.mkdir()
        (portolan / "metadata.yaml").write_text(yaml.dump(metadata_yaml), encoding="utf-8")
    dest = collection_dir / "data.parquet"
    shutil.copy(FIXTURE, dest)

    result = CliRunner().invoke(
        cli,
        ["add", "--portolan-dir", str(root), str(dest)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    return collection_dir


@pytest.mark.integration
class TestAddProducesHumanReadableTitles:
    def test_collection_title_and_description_humanized(self, tmp_path: Path) -> None:
        root = tmp_path / "catalog"
        _init_catalog(root)
        collection_dir = _add_slug_collection(root)

        coll = json.loads((collection_dir / "collection.json").read_text())
        assert coll["title"] == "Publico Areas Programaticas Desarrollo Social"
        # Description defaults to the title, never "Collection: <slug>".
        assert coll["description"] == "Publico Areas Programaticas Desarrollo Social"
        assert "Collection:" not in coll["description"]

    def test_root_child_link_has_title_and_type(self, tmp_path: Path) -> None:
        root = tmp_path / "catalog"
        _init_catalog(root)
        _add_slug_collection(root)

        catalog = json.loads((root / "catalog.json").read_text())
        child_links = [link for link in catalog["links"] if link.get("rel") == "child"]
        assert len(child_links) == 1
        link = child_links[0]
        assert link["title"] == "Publico Areas Programaticas Desarrollo Social"
        assert link["type"] == "application/json"

    def test_metadata_yaml_title_override_wins(self, tmp_path: Path) -> None:
        root = tmp_path / "catalog"
        _init_catalog(root)
        collection_dir = _add_slug_collection(
            root,
            metadata_yaml={
                "contact": {"name": "IGN", "email": "x@example.com"},
                "license": "CC-BY-4.0",
                "title": "Áreas Programáticas de Desarrollo Social",
                "description": "Polígonos de áreas programáticas.",
            },
        )

        coll = json.loads((collection_dir / "collection.json").read_text(encoding="utf-8"))
        assert coll["title"] == "Áreas Programáticas de Desarrollo Social"
        assert coll["description"] == "Polígonos de áreas programáticas."

        # And the override propagates to the parent child link.
        catalog = json.loads((root / "catalog.json").read_text(encoding="utf-8"))
        child = next(link for link in catalog["links"] if link.get("rel") == "child")
        assert child["title"] == "Áreas Programáticas de Desarrollo Social"
