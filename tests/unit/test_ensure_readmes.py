"""Unit tests for README scaffolding and its ``describedby`` link (issue #654).

rashid requires a README.md next to every catalog and collection (PTL-FIL-001)
referenced by a ``rel="describedby"`` markdown link (PTL-FIL-003).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.readme import ensure_readmes, readme_link_gap

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

    def test_normalizes_a_wrong_type_readme_link(self, tmp_path: Path) -> None:
        catalog = _catalog()
        catalog["links"] = [{"rel": "describedby", "href": "./README.md", "type": "text/plain"}]
        _write(tmp_path / "catalog.json", catalog)

        assert ensure_readmes(tmp_path) is True

        described = [
            link for link in _links(tmp_path / "catalog.json") if link["rel"] == "describedby"
        ]
        assert len(described) == 1
        assert described[0]["href"] == "./README.md"
        assert described[0]["type"] == "text/markdown"

    def test_preserves_a_foreign_describedby_link(self, tmp_path: Path) -> None:
        """A non-README ``describedby`` link (e.g. a data dictionary) must survive.

        Regression: the link scan used to take the *first* ``describedby`` link
        and overwrite it with the README link, destroying the publisher's
        pointer to their own documentation on every ``add``.
        """
        data_dictionary = {
            "rel": "describedby",
            "href": "./data-dictionary.pdf",
            "type": "application/pdf",
            "title": "Data dictionary",
        }
        catalog = _catalog()
        catalog["links"] = [dict(data_dictionary)]
        _write(tmp_path / "catalog.json", catalog)

        assert ensure_readmes(tmp_path) is True

        described = [
            link for link in _links(tmp_path / "catalog.json") if link["rel"] == "describedby"
        ]
        assert data_dictionary in described
        assert {
            "rel": "describedby",
            "href": "./README.md",
            "type": "text/markdown",
            "title": "Human-readable documentation",
        } in described

    def test_preserves_a_describedby_link_to_another_directorys_readme(
        self, tmp_path: Path
    ) -> None:
        """``../shared/README.md`` is a different file, basename notwithstanding.

        Regression: the resolved-path comparison was followed by an
        unconditional basename fallback, so any href ending in ``README.md``
        matched — and the publisher's pointer to a shared README one directory
        up was rewritten to ``./README.md`` on every ``add``.
        """
        (tmp_path / "shared").mkdir()
        (tmp_path / "shared" / "README.md").write_text("# Shared\n", encoding="utf-8")
        foreign = {
            "rel": "describedby",
            "href": "./shared/README.md",
            "type": "text/markdown",
            "title": "Programme documentation",
        }
        catalog = _catalog()
        catalog["links"] = [dict(foreign)]
        _write(tmp_path / "catalog.json", catalog)

        assert ensure_readmes(tmp_path) is True

        described = [
            link for link in _links(tmp_path / "catalog.json") if link["rel"] == "describedby"
        ]
        assert len(described) == 2
        assert foreign in described
        assert {
            "rel": "describedby",
            "href": "./README.md",
            "type": "text/markdown",
            "title": "Human-readable documentation",
        } in described

    def test_normalizes_the_readme_link_beside_a_foreign_one(self, tmp_path: Path) -> None:
        catalog = _catalog()
        catalog["links"] = [
            {"rel": "describedby", "href": "./data-dictionary.pdf", "type": "application/pdf"},
            {"rel": "describedby", "href": "./README.md", "type": "text/plain"},
        ]
        _write(tmp_path / "catalog.json", catalog)

        assert ensure_readmes(tmp_path) is True

        described = [
            link for link in _links(tmp_path / "catalog.json") if link["rel"] == "describedby"
        ]
        assert len(described) == 2
        assert described[0]["type"] == "application/pdf"
        assert described[1]["type"] == "text/markdown"

    def test_skips_unparseable_files(self, tmp_path: Path) -> None:
        (tmp_path / "catalog.json").write_text("{not json", encoding="utf-8")

        assert ensure_readmes(tmp_path) is False

    def test_ignores_stac_files_in_hidden_directories(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", _catalog())
        hidden = tmp_path / ".portolan" / "cache" / "collection.json"
        _write(hidden, _collection())
        before = hidden.read_text(encoding="utf-8")

        ensure_readmes(tmp_path)

        assert hidden.read_text(encoding="utf-8") == before
        assert not (hidden.parent / "README.md").exists()


class TestReadmeLinkGap:
    """The four cases rashid PTL-FIL-003 flags (see readme_link_gap)."""

    def _catalog_with(self, tmp_path: Path, links: list[dict[str, str]]) -> dict[str, object]:
        catalog = _catalog()
        catalog["links"] = links
        return catalog

    def test_no_gap_when_link_and_file_are_correct(self, tmp_path: Path) -> None:
        (tmp_path / "README.md").write_text("# Root\n", encoding="utf-8")
        data = self._catalog_with(
            tmp_path,
            [{"rel": "describedby", "href": "./README.md", "type": "text/markdown"}],
        )

        assert readme_link_gap(tmp_path / "catalog.json", data) is False

    def test_gap_when_link_is_absent(self, tmp_path: Path) -> None:
        (tmp_path / "README.md").write_text("# Root\n", encoding="utf-8")

        assert readme_link_gap(tmp_path / "catalog.json", self._catalog_with(tmp_path, [])) is True

    def test_gap_when_type_is_wrong(self, tmp_path: Path) -> None:
        (tmp_path / "README.md").write_text("# Root\n", encoding="utf-8")
        data = self._catalog_with(
            tmp_path,
            [{"rel": "describedby", "href": "./README.md", "type": "text/plain"}],
        )

        assert readme_link_gap(tmp_path / "catalog.json", data) is True

    @pytest.mark.parametrize("href", ["", "/abs/README.md", "https://example.com/README.md"])
    def test_gap_when_href_is_missing_empty_or_absolute(self, tmp_path: Path, href: str) -> None:
        (tmp_path / "README.md").write_text("# Root\n", encoding="utf-8")
        data = self._catalog_with(
            tmp_path,
            [{"rel": "describedby", "href": href, "type": "text/markdown"}],
        )

        assert readme_link_gap(tmp_path / "catalog.json", data) is True

    def test_gap_when_href_key_is_absent(self, tmp_path: Path) -> None:
        (tmp_path / "README.md").write_text("# Root\n", encoding="utf-8")
        data = self._catalog_with(tmp_path, [{"rel": "describedby", "type": "text/markdown"}])

        assert readme_link_gap(tmp_path / "catalog.json", data) is True

    def test_gap_when_href_does_not_resolve_to_the_sibling_readme(self, tmp_path: Path) -> None:
        (tmp_path / "README.md").write_text("# Root\n", encoding="utf-8")
        data = self._catalog_with(
            tmp_path,
            [{"rel": "describedby", "href": "../README.md", "type": "text/markdown"}],
        )

        assert readme_link_gap(tmp_path / "catalog.json", data) is True

    def test_gap_when_the_readme_file_is_missing(self, tmp_path: Path) -> None:
        data = self._catalog_with(
            tmp_path,
            [{"rel": "describedby", "href": "./README.md", "type": "text/markdown"}],
        )

        assert readme_link_gap(tmp_path / "catalog.json", data) is True
