"""Unit tests for README scaffolding and its ``describedby`` link (issue #654).

rashid requires a README.md next to every catalog and collection (PTL-FIL-001)
referenced by a ``rel="describedby"`` markdown link (PTL-FIL-003).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.readme import ensure_readmes

pytestmark = pytest.mark.unit


def _write(path: Path, data: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _links(path: Path) -> list[dict[str, str]]:
    loaded: list[dict[str, str]] = json.loads(path.read_text(encoding="utf-8"))["links"]
    return loaded


def _catalog(identifier: str = "root") -> dict[str, object]:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "id": identifier,
        "title": identifier.title(),
        "description": "A catalog.",
        "links": [],
    }


def _collection(identifier: str = "roads") -> dict[str, object]:
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "id": identifier,
        "title": identifier.title(),
        "description": "A collection.",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-1.0, -1.0, 1.0, 1.0]]},
            "temporal": {"interval": [[None, None]]},
        },
        "links": [],
        "assets": {},
    }


class TestEnsureReadmes:
    def test_scaffolds_readme_and_link_for_catalog_and_collection(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", _catalog())
        _write(tmp_path / "roads" / "collection.json", _collection())

        assert ensure_readmes(tmp_path) is True

        for directory in (tmp_path, tmp_path / "roads"):
            readme = directory / "README.md"
            assert readme.exists()
            assert readme.read_text(encoding="utf-8").lstrip().startswith("#")

        for stac in (tmp_path / "catalog.json", tmp_path / "roads" / "collection.json"):
            described = [link for link in _links(stac) if link["rel"] == "describedby"]
            assert described == [
                {
                    "rel": "describedby",
                    "href": "./README.md",
                    "type": "text/markdown",
                    "title": "Human-readable documentation",
                }
            ]

    def test_is_idempotent(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", _catalog())

        assert ensure_readmes(tmp_path) is True
        assert ensure_readmes(tmp_path) is False

    def test_never_overwrites_a_human_authored_readme(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", _catalog())
        readme = tmp_path / "README.md"
        readme.write_text("# Hand written\n", encoding="utf-8")

        ensure_readmes(tmp_path)

        assert readme.read_text(encoding="utf-8") == "# Hand written\n"

    def test_normalizes_a_malformed_describedby_link(self, tmp_path: Path) -> None:
        catalog = _catalog()
        catalog["links"] = [{"rel": "describedby", "href": "./readme.txt"}]
        _write(tmp_path / "catalog.json", catalog)

        assert ensure_readmes(tmp_path) is True

        described = [
            link for link in _links(tmp_path / "catalog.json") if link["rel"] == "describedby"
        ]
        assert described[0]["href"] == "./README.md"
        assert described[0]["type"] == "text/markdown"

    def test_skips_unparseable_files(self, tmp_path: Path) -> None:
        (tmp_path / "catalog.json").write_text("{not json", encoding="utf-8")

        assert ensure_readmes(tmp_path) is False
