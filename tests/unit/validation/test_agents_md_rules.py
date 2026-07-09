"""Unit tests for the AGENTS.md validation rules (RULE-0080/0081).

CatalogAgentsMdLinkRule and CollectionAgentsMdLinkRule enforce that every
catalog.json / collection.json references an AGENTS.md file via a well-formed
``rel="agents"`` link (ADR-0052).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.agents_md import build_agents_link
from portolan_cli.validation.results import Severity
from portolan_cli.validation.rules import (
    CatalogAgentsMdLinkRule,
    CollectionAgentsMdLinkRule,
)

pytestmark = pytest.mark.unit

FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "validation" / "stac"


def _write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _catalog(links: list) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "id": "demo",
        "title": "Demo",
        "description": "d",
        "links": links,
    }


def _collection(links: list) -> dict:
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "id": "roads",
        "title": "Roads",
        "description": "d",
        "links": links,
    }


class TestCatalogAgentsMdLinkRule:
    def test_severity_is_error(self) -> None:
        assert CatalogAgentsMdLinkRule().severity == Severity.ERROR
        assert CatalogAgentsMdLinkRule().name == "agents_md_catalog_link"

    def test_passes_with_link_and_file(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", _catalog([build_agents_link()]))
        (tmp_path / "AGENTS.md").write_text("# guide", encoding="utf-8")
        result = CatalogAgentsMdLinkRule().check(tmp_path)
        assert result.passed

    def test_fails_when_link_missing(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", _catalog([]))
        result = CatalogAgentsMdLinkRule().check(tmp_path)
        assert not result.passed
        assert "AGENTS.md" in result.message

    def test_fails_when_file_missing(self, tmp_path: Path) -> None:
        # Link present, file absent — still a failure (fixable by --fix).
        _write(tmp_path / "catalog.json", _catalog([build_agents_link()]))
        result = CatalogAgentsMdLinkRule().check(tmp_path)
        assert not result.passed

    def test_checks_nested_intermediate_catalogs(self, tmp_path: Path) -> None:
        # Root is compliant; a nested (intermediate) catalog is not.
        _write(tmp_path / "catalog.json", _catalog([build_agents_link()]))
        (tmp_path / "AGENTS.md").write_text("# guide", encoding="utf-8")
        _write(tmp_path / "region" / "catalog.json", _catalog([]))
        result = CatalogAgentsMdLinkRule().check(tmp_path)
        assert not result.passed
        assert "region" in result.message

    def test_ignores_hidden_dirs(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", _catalog([build_agents_link()]))
        (tmp_path / "AGENTS.md").write_text("# guide", encoding="utf-8")
        _write(tmp_path / ".portolan" / "catalog.json", _catalog([]))
        assert CatalogAgentsMdLinkRule().check(tmp_path).passed

    def test_flags_present_file_without_link(self) -> None:
        # Regression for the portolan-nl gap (cli#479): AGENTS.md exists on disk
        # but the root catalog.json has no rel="agents" link. RULE-0080 must fail
        # even though the file is present.
        fixture = FIXTURES_DIR / "agents-md-missing-root-link"
        assert (fixture / "AGENTS.md").exists()  # file is present...
        result = CatalogAgentsMdLinkRule().check(fixture)
        assert not result.passed  # ...but the link is not
        assert "missing" in result.message.lower()


class TestCollectionAgentsMdLinkRule:
    def test_severity_and_name(self) -> None:
        rule = CollectionAgentsMdLinkRule()
        assert rule.severity == Severity.ERROR
        assert rule.name == "agents_md_collection_link"

    def test_passes_with_link_and_file(self, tmp_path: Path) -> None:
        coll_dir = tmp_path / "roads"
        _write(coll_dir / "collection.json", _collection([build_agents_link()]))
        (coll_dir / "AGENTS.md").write_text("# guide", encoding="utf-8")
        assert CollectionAgentsMdLinkRule().check(tmp_path).passed

    def test_fails_when_link_missing(self, tmp_path: Path) -> None:
        coll_dir = tmp_path / "roads"
        _write(coll_dir / "collection.json", _collection([]))
        result = CollectionAgentsMdLinkRule().check(tmp_path)
        assert not result.passed
        assert "roads" in result.message

    def test_passes_when_no_collections_present(self, tmp_path: Path) -> None:
        # A catalog with no collections cannot violate the collection rule.
        _write(tmp_path / "catalog.json", _catalog([build_agents_link()]))
        assert CollectionAgentsMdLinkRule().check(tmp_path).passed
