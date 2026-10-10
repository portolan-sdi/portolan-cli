"""The shared catalog walker, and the commands that read through it (issue #944).

Before the fix, each command found collections its own way. ``list`` read only
top-level item subdirectories, ``status`` globbed for ``versions.json``, and
``check`` walked the containment graph. A catalog with one unlinked
collection directory made them disagree.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner
from rashid.catalog import CatalogGraph

from portolan_cli.cli import cli
from portolan_cli.stac_links import (
    catalog_collections,
    iter_links,
    iter_stac_objects,
    resolve_href,
)

if TYPE_CHECKING:
    from pathlib import Path

_EXTENT = {"spatial": {"bbox": [[0, 0, 1, 1]]}, "temporal": {"interval": [[None, None]]}}


def _stac(path: Path, kind: str, *children: str, **extra: object) -> None:
    """Write a minimal Catalog or Collection at ``path`` with child links."""
    obj: dict[str, object] = {
        "type": kind,
        "id": path.parent.name,
        "stac_version": "1.1.0",
        "description": "d",
        "links": [{"rel": "child", "href": href} for href in children],
        **extra,
    }
    if kind == "Collection":
        obj.update(license="CC-BY-4.0", extent=_EXTENT)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj), encoding="utf-8")


def _track(catalog_root: Path, collection_id: str) -> None:
    """Give a collection one collection-level asset and a versions.json that tracks it."""
    col_dir = catalog_root / collection_id
    (col_dir / "data.parquet").write_bytes(b"PAR1")
    asset = {"sha256": "0" * 64, "size_bytes": 4, "href": f"{collection_id}/data.parquet"}
    version = {
        "version": "1.0.0",
        "created": "2026-10-10T00:00:00Z",
        "breaking": False,
        "assets": {"data.parquet": asset},
        "changes": ["data.parquet"],
    }
    manifest = {"spec_version": "1.0.0", "current_version": "1.0.0", "versions": [version]}
    (col_dir / "versions.json").write_text(json.dumps(manifest), encoding="utf-8")


@pytest.fixture
def mixed_catalog(tmp_path: Path) -> Path:
    """A managed catalog with a linked, an unlinked, a nested, and a hidden collection."""
    (tmp_path / ".portolan").mkdir()
    (tmp_path / ".portolan" / "config.yaml").write_text("version: 1\n")
    _stac(
        tmp_path / "catalog.json",
        "Catalog",
        "./buildings/collection.json",
        "./climate/catalog.json",
    )
    _stac(tmp_path / "climate" / "catalog.json", "Catalog", "./heat/collection.json")
    # roads has no child link from the root. check reports PTL-LNK-002 for it.
    for collection_id in ("buildings", "roads", "climate/heat"):
        _stac(tmp_path / collection_id / "collection.json", "Collection")
        _track(tmp_path, collection_id)
    # A dot-directory holds caches and backups, never a published collection.
    _stac(tmp_path / ".portolan" / "backup" / "collection.json", "Collection")
    return tmp_path


_EXPECTED = ["buildings", "climate/heat", "roads"]


def _invoke(catalog_root: Path, *args: str) -> dict[str, object]:
    result = CliRunner().invoke(cli, [*args, "--catalog", str(catalog_root), "--json"])
    assert result.exit_code == 0, result.output
    data: dict[str, object] = json.loads(result.output)["data"]
    return data


class TestCatalogCollections:
    """``catalog_collections`` returns the collection set ``check`` sees."""

    @pytest.mark.unit
    def test_finds_linked_unlinked_and_nested(self, mixed_catalog: Path) -> None:
        """Every visible collection counts. A child link is not necessary."""
        assert catalog_collections(mixed_catalog) == _EXPECTED

    @pytest.mark.unit
    def test_matches_the_check_graph(self, mixed_catalog: Path) -> None:
        """The set equals the collections in rashid's CatalogGraph, which check walks."""
        graph = CatalogGraph.load(mixed_catalog)
        expected = sorted(str(node.path.parent) for node in graph.iter("collection"))

        assert catalog_collections(mixed_catalog) == expected

    @pytest.mark.unit
    def test_root_collection_json_is_not_a_member(self, tmp_path: Path) -> None:
        """A collection.json at the root is not a collection below the root."""
        _stac(tmp_path / "catalog.json", "Catalog")
        _stac(tmp_path / "collection.json", "Collection")

        assert catalog_collections(tmp_path) == []


class TestIterStacObjects:
    """``iter_stac_objects`` yields each visible STAC object that parses."""

    @pytest.mark.unit
    def test_skips_unparseable_and_non_object_files(self, tmp_path: Path) -> None:
        """Broken JSON and a top-level array are skipped. A valid catalog is yielded."""
        _stac(tmp_path / "catalog.json", "Catalog")
        (tmp_path / "broken").mkdir()
        (tmp_path / "broken" / "collection.json").write_text("{not json", encoding="utf-8")
        (tmp_path / "array").mkdir()
        (tmp_path / "array" / "collection.json").write_text("[]", encoding="utf-8")

        found = [
            (path.relative_to(tmp_path).as_posix(), data["type"])
            for path, data in iter_stac_objects(tmp_path)
        ]

        assert found == [("catalog.json", "Catalog")]


class TestIterLinks:
    """``iter_links`` filters the links of one STAC object."""

    @pytest.mark.unit
    def test_yields_wanted_rels_with_resolved_paths(self, tmp_path: Path) -> None:
        """Only the wanted rels come back, and each path resolves against base_dir."""
        data: dict[str, object] = {
            "links": [
                {"rel": "self", "href": "./catalog.json"},
                {"rel": "child", "href": "./a/collection.json"},
                {"rel": "item", "href": "b/b.json"},
            ]
        }

        result = [(href, path) for _link, href, path in iter_links(data, tmp_path, ("child",))]

        assert result == [("./a/collection.json", tmp_path / "a" / "collection.json")]

    @pytest.mark.unit
    def test_skips_links_without_a_string_href(self, tmp_path: Path) -> None:
        """A missing, empty, or non-string href is not a link to follow."""
        data: dict[str, object] = {
            "links": [
                {"rel": "item"},
                {"rel": "item", "href": ""},
                {"rel": "item", "href": 7},
                "not-a-dict",
                {"rel": "item", "href": "./ok/ok.json"},
            ]
        }

        hrefs = [href for _link, href, _path in iter_links(data, tmp_path, ("item",))]

        assert hrefs == ["./ok/ok.json"]

    @pytest.mark.unit
    def test_yields_the_link_dict_for_in_place_edits(self, tmp_path: Path) -> None:
        """The yielded link is the dict inside ``data``."""
        link = {"rel": "child", "href": "./a/collection.json"}
        data: dict[str, object] = {"links": [link]}

        (yielded, _href, _path) = next(iter_links(data, tmp_path, ("child",)))

        assert yielded is link

    @pytest.mark.unit
    def test_links_that_are_not_a_list(self, tmp_path: Path) -> None:
        """A malformed ``links`` value yields nothing."""
        assert list(iter_links({"links": {"rel": "child"}}, tmp_path, ("child",))) == []


class TestResolveHref:
    """``resolve_href`` resolves a relative href against the linking directory."""

    @pytest.mark.unit
    def test_dot_slash(self, tmp_path: Path) -> None:
        """A ``./`` href stays below base_dir."""
        assert resolve_href(tmp_path, "./a/b.json") == tmp_path / "a" / "b.json"

    @pytest.mark.unit
    def test_parent_href_resolves(self, tmp_path: Path) -> None:
        """A ``../`` href resolves to an absolute path above base_dir."""
        assert (
            resolve_href(tmp_path / "sub", "../catalog.json")
            == (tmp_path / "catalog.json").resolve()
        )

    @pytest.mark.unit
    def test_bare_href(self, tmp_path: Path) -> None:
        """A bare href joins base_dir."""
        assert resolve_href(tmp_path, "item.json") == tmp_path / "item.json"


class TestCommandsAgree:
    """``list``, ``status``, and ``check`` report the same collections."""

    @pytest.mark.integration
    def test_list_and_status_report_the_check_collections(self, mixed_catalog: Path) -> None:
        """All three commands see buildings, climate/heat, and the unlinked roads."""
        graph = CatalogGraph.load(mixed_catalog)
        check_ids = sorted(str(node.path.parent) for node in graph.iter("collection"))

        list_data = _invoke(mixed_catalog, "list")
        status_data = _invoke(mixed_catalog, "status", "--offline")
        assert isinstance(list_data["collections"], list)
        assert isinstance(status_data["collections"], list)

        assert check_ids == _EXPECTED
        assert sorted(col["id"] for col in list_data["collections"]) == _EXPECTED
        assert sorted(col["collection"] for col in status_data["collections"]) == _EXPECTED

    @pytest.mark.integration
    def test_check_flags_the_unlinked_collection(self, mixed_catalog: Path) -> None:
        """``check`` still reports the missing child link for roads as PTL-LNK-002."""
        result = CliRunner().invoke(
            cli, ["check", str(mixed_catalog), "--metadata", "--no-data", "--json"]
        )
        findings = json.loads(result.output)["data"]["findings"]

        assert [f["message"] for f in findings if f["rule_id"] == "PTL-LNK-002"] == [
            "contained object 'roads/collection.json' has no child link"
        ]

    @pytest.mark.integration
    def test_list_shows_collection_level_files(self, mixed_catalog: Path) -> None:
        """A collection-level asset appears in ``list``, so the collection is not empty."""
        collections = _invoke(mixed_catalog, "list")["collections"]
        assert isinstance(collections, list)
        roads = next(col for col in collections if col["id"] == "roads")

        assert roads["assets"] == [
            {"path": "data.parquet", "status": "tracked", "format": "GeoParquet", "size": 4}
        ]
        assert roads["items"] == []


class TestStatusMatchesPush:
    """``status`` reports every collection ``check`` sees and every one ``push`` uploads.

    ``push`` finds a collection by its ``versions.json``. ``check`` finds it by
    its ``collection.json``. A collection that holds only one of the two must
    still appear in ``status``, which previews ``push``.
    """

    @pytest.mark.unit
    def test_versioned_collections(self, tmp_path: Path) -> None:
        """Every visible directory below the root that holds versions.json."""
        from portolan_cli.stac_links import versioned_collections

        for rel in ("orphan", "a/nested", ".portolan/backup", ""):
            (tmp_path / rel).mkdir(parents=True, exist_ok=True)
            (tmp_path / rel / "versions.json").write_text("{}")
        _stac(tmp_path / "draft" / "collection.json", "Collection")

        assert versioned_collections(tmp_path) == ["a/nested", "orphan"]

    @pytest.mark.integration
    def test_status_reports_the_check_and_push_collections(self, mixed_catalog: Path) -> None:
        """A versions.json without collection.json, and the reverse, both appear."""
        from portolan_cli.sync.push import discover_collections

        (mixed_catalog / "orphan").mkdir()
        _track(mixed_catalog, "orphan")
        _stac(mixed_catalog / "draft" / "collection.json", "Collection")

        status_data = _invoke(mixed_catalog, "status", "--offline")
        assert isinstance(status_data["collections"], list)
        status_ids = sorted(col["collection"] for col in status_data["collections"])

        assert "orphan" in discover_collections(mixed_catalog)
        assert "draft" in catalog_collections(mixed_catalog)
        assert status_ids == ["buildings", "climate/heat", "draft", "orphan", "roads"]


class TestManagedFiles:
    """``list`` and ``status`` agree that Portolan's own files are not untracked data."""

    @pytest.mark.integration
    def test_list_and_status_skip_untracked_managed_files(self, mixed_catalog: Path) -> None:
        """README.md, AGENTS.md, and metadata.yaml are not reported as untracked."""
        roads = mixed_catalog / "roads"
        for name in ("README.md", "AGENTS.md", "metadata.yaml"):
            (roads / name).write_text("x")
        (roads / "extra.csv").write_text("a,b\n")

        list_cols = _invoke(mixed_catalog, "list")["collections"]
        status_cols = _invoke(mixed_catalog, "status", "--offline")["collections"]
        assert isinstance(list_cols, list)
        assert isinstance(status_cols, list)
        list_roads = next(col for col in list_cols if col["id"] == "roads")
        status_roads = next(col for col in status_cols if col["collection"] == "roads")

        assert [(a["path"], a["status"]) for a in list_roads["assets"]] == [
            ("data.parquet", "tracked"),
            ("extra.csv", "untracked"),
        ]
        assert status_roads["untracked_files"] == ["extra.csv"]

    @pytest.mark.integration
    def test_list_shows_a_tracked_managed_file(self, mixed_catalog: Path) -> None:
        """A managed file that versions.json tracks still appears as tracked."""
        roads = mixed_catalog / "roads"
        (roads / "README.md").write_text("x")
        manifest = json.loads((roads / "versions.json").read_text())
        manifest["versions"][0]["assets"]["README.md"] = {
            "sha256": "0" * 64,
            "size_bytes": 1,
            "href": "roads/README.md",
        }
        (roads / "versions.json").write_text(json.dumps(manifest))

        list_cols = _invoke(mixed_catalog, "list")["collections"]
        assert isinstance(list_cols, list)
        list_roads = next(col for col in list_cols if col["id"] == "roads")

        assert [(a["path"], a["status"]) for a in list_roads["assets"]] == [
            ("README.md", "tracked"),
            ("data.parquet", "tracked"),
        ]
