"""Unit tests for AGENTS.md scaffolding, link injection, and gap detection.

Covers the framework-free leaf ``portolan_cli.agents_md`` and the ``check --fix``
repair ``portolan_cli.metadata.fix.repair_agents_md`` (ADR-0052, RULE-0080/0081).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.agents_md import (
    AGENTS_LINK_REL,
    AGENTS_MD_FILENAME,
    AGENTS_MEDIA_TYPE,
    agents_md_gap,
    build_agents_link,
    ensure_agents_md,
    find_agents_link,
    scaffold_content,
)

pytestmark = pytest.mark.unit


def _write(path: Path, data: dict) -> Path:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return path


def _catalog(**overrides: object) -> dict:
    base = {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "id": "demo",
        "title": "Demo Catalog",
        "description": "d",
        "links": [{"rel": "root", "href": "./catalog.json", "type": "application/json"}],
    }
    base.update(overrides)
    return base


class TestScaffoldContent:
    def test_catalog_and_collection_differ(self) -> None:
        cat = scaffold_content("My Catalog", is_catalog=True)
        coll = scaffold_content("My Coll", is_catalog=False)
        assert cat.startswith("# AGENTS.md — My Catalog")
        assert "## Collections" in cat
        assert "catalog" in cat.lower()
        # Collection template seeds agent-oriented sections not in the README.
        assert "## Schema & field notes" in coll
        assert "## Example queries" in coll
        assert "## Related collections" in coll
        assert cat != coll

    def test_title_is_embedded(self) -> None:
        assert "# AGENTS.md — Census 2020" in scaffold_content("Census 2020", is_catalog=False)


class TestLinkHelpers:
    def test_build_agents_link_shape(self) -> None:
        link = build_agents_link()
        assert link["rel"] == AGENTS_LINK_REL == "agents"
        assert link["href"] == f"./{AGENTS_MD_FILENAME}"
        assert link["type"] == AGENTS_MEDIA_TYPE == "text/markdown"
        assert link["title"]

    def test_find_agents_link(self) -> None:
        links = [{"rel": "root"}, build_agents_link(), {"rel": "self"}]
        assert find_agents_link(links) is not None
        assert find_agents_link([{"rel": "root"}]) is None
        assert find_agents_link([]) is None


class TestAgentsMdGap:
    def test_no_gap_when_link_and_file_present(self, tmp_path: Path) -> None:
        (tmp_path / AGENTS_MD_FILENAME).write_text("# hi", encoding="utf-8")
        cj = _write(
            tmp_path / "catalog.json",
            _catalog(links=[build_agents_link()]),
        )
        assert agents_md_gap(cj) is None

    def test_missing_link_is_a_gap(self, tmp_path: Path) -> None:
        (tmp_path / AGENTS_MD_FILENAME).write_text("# hi", encoding="utf-8")
        cj = _write(tmp_path / "catalog.json", _catalog())
        gap = agents_md_gap(cj)
        assert gap is not None
        assert "missing" in gap.lower()

    def test_missing_file_is_a_gap(self, tmp_path: Path) -> None:
        # Link present but AGENTS.md file does not exist on disk.
        cj = _write(tmp_path / "catalog.json", _catalog(links=[build_agents_link()]))
        gap = agents_md_gap(cj)
        assert gap is not None
        assert "missing file" in gap.lower()

    def test_malformed_link_is_a_gap(self, tmp_path: Path) -> None:
        (tmp_path / AGENTS_MD_FILENAME).write_text("# hi", encoding="utf-8")
        bad = {"rel": "agents", "href": "./llms.txt", "type": "text/markdown"}
        cj = _write(tmp_path / "catalog.json", _catalog(links=[bad]))
        gap = agents_md_gap(cj)
        assert gap is not None
        assert "AGENTS.md" in gap

    def test_unreadable_json_yields_none(self, tmp_path: Path) -> None:
        cj = tmp_path / "catalog.json"
        cj.write_text("{ not json", encoding="utf-8")
        # Malformed JSON is another rule's problem, not ours.
        assert agents_md_gap(cj) is None


class TestEnsureAgentsMd:
    def test_creates_file_and_link(self, tmp_path: Path) -> None:
        cj = _write(tmp_path / "catalog.json", _catalog())
        changed = ensure_agents_md(cj)
        assert changed is True
        agents = tmp_path / AGENTS_MD_FILENAME
        assert agents.exists()
        assert "# AGENTS.md — Demo Catalog" in agents.read_text(encoding="utf-8")
        data = json.loads(cj.read_text(encoding="utf-8"))
        link = find_agents_link(data["links"])
        assert link is not None
        assert link["href"] == "./AGENTS.md"
        assert link["type"] == "text/markdown"

    def test_collection_scaffold_uses_collection_template(self, tmp_path: Path) -> None:
        coll = {
            "type": "Collection",
            "stac_version": "1.1.0",
            "id": "roads",
            "title": "Roads",
            "description": "d",
            "links": [],
        }
        cj = _write(tmp_path / "collection.json", coll)
        ensure_agents_md(cj)
        text = (tmp_path / AGENTS_MD_FILENAME).read_text(encoding="utf-8")
        assert "## Schema & field notes" in text  # collection-only section

    def test_idempotent_and_never_overwrites(self, tmp_path: Path) -> None:
        cj = _write(tmp_path / "catalog.json", _catalog())
        ensure_agents_md(cj)
        # Human edits the scaffolded file.
        agents = tmp_path / AGENTS_MD_FILENAME
        agents.write_text("# Hand-authored, do not clobber", encoding="utf-8")
        # Second run must be a no-op: link already present, file already exists.
        changed = ensure_agents_md(cj)
        assert changed is False
        assert agents.read_text(encoding="utf-8") == "# Hand-authored, do not clobber"

    def test_normalizes_malformed_link_without_duplicating(self, tmp_path: Path) -> None:
        (tmp_path / AGENTS_MD_FILENAME).write_text("# hi", encoding="utf-8")
        bad = {"rel": "agents", "href": "./llms.txt", "type": "text/plain"}
        cj = _write(tmp_path / "catalog.json", _catalog(links=[bad]))
        changed = ensure_agents_md(cj)
        assert changed is True
        data = json.loads(cj.read_text(encoding="utf-8"))
        agents_links = [link for link in data["links"] if link["rel"] == "agents"]
        assert len(agents_links) == 1  # normalized in place, not duplicated
        assert agents_links[0]["href"] == "./AGENTS.md"
        assert agents_links[0]["type"] == "text/markdown"


class TestRepairAgentsMd:
    def test_repairs_catalog_and_collection(self, tmp_path: Path) -> None:
        from portolan_cli.metadata.fix import repair_agents_md

        _write(tmp_path / "catalog.json", _catalog())
        coll_dir = tmp_path / "roads"
        coll_dir.mkdir()
        _write(
            coll_dir / "collection.json",
            {
                "type": "Collection",
                "stac_version": "1.1.0",
                "id": "roads",
                "title": "Roads",
                "description": "d",
                "links": [],
            },
        )

        results = repair_agents_md(tmp_path)

        assert len(results) == 2
        assert (tmp_path / AGENTS_MD_FILENAME).exists()
        assert (coll_dir / AGENTS_MD_FILENAME).exists()
        # Re-running is a no-op now that everything is compliant.
        assert repair_agents_md(tmp_path) == []

    def test_dry_run_reports_without_writing(self, tmp_path: Path) -> None:
        from portolan_cli.metadata.fix import repair_agents_md

        _write(tmp_path / "catalog.json", _catalog())
        results = repair_agents_md(tmp_path, dry_run=True)
        assert len(results) == 1
        assert not (tmp_path / AGENTS_MD_FILENAME).exists()

    def test_skips_hidden_dirs(self, tmp_path: Path) -> None:
        from portolan_cli.metadata.fix import repair_agents_md

        _write(tmp_path / "catalog.json", _catalog(links=[build_agents_link()]))
        (tmp_path / AGENTS_MD_FILENAME).write_text("# hi", encoding="utf-8")
        # A stray catalog.json under .portolan/ must be ignored.
        hidden = tmp_path / ".portolan"
        hidden.mkdir()
        _write(hidden / "catalog.json", _catalog())
        assert repair_agents_md(tmp_path) == []
