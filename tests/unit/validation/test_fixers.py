"""The fixer registry: the mechanical half of `portolan check --fix`.

Each fixer is a whole-catalog sweep keyed by the name an AUTO row in
``RULE_REMEDIATION`` names. These tests corrupt one aspect of a catalog, run the
fixer that owns it, and assert the specific repair — plus the two completeness
gates that keep the registry and the remediation table in agreement.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from rashid.model import Finding, Severity

from portolan_cli.metadata.fix import FixAction, FixResult
from portolan_cli.validation import fixers as fixers_module
from portolan_cli.validation.fixers import FIXERS, apply_fixers, auto_fixer_keys
from portolan_cli.validation.remediation import RULE_REMEDIATION, Bucket

pytestmark = pytest.mark.unit


def _finding(rule_id: str, path: str = "collection.json") -> Finding:
    return Finding(
        rule_id=rule_id,
        severity=Severity.ERROR,
        message=f"{rule_id} fired",
        path=path,
    )


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return loaded


def _snapshot(root: Path) -> dict[str, bytes]:
    """Every file under ``root``, by relative path, for dry-run comparison."""
    return {
        str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()
    }


def _tiny_catalog(root: Path, *, collection: str = "roads") -> Path:
    """A two-level tree: root catalog + one collection, minimal but parseable."""
    _write_json(
        root / "catalog.json",
        {"type": "Catalog", "id": "cat", "stac_version": "1.1.0", "description": "c", "links": []},
    )
    _write_json(
        root / collection / "collection.json",
        {
            "type": "Collection",
            "id": collection,
            "stac_version": "1.1.0",
            "description": "d",
            "license": "CC-BY-4.0",
            "extent": {
                "spatial": {"bbox": [[0.0, 0.0, 1.0, 1.0]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [],
        },
    )
    return root / collection


def _organizing_catalog(root: Path, *, collection: str = "roads") -> Path:
    """A collection whose items sit under an organizing catalog (core.md:168-170).

    ``roads/collection.json`` -> ``roads/US/catalog.json`` -> ``roads/US/US_AB.json``.
    Returns the item's path. Links are written already correct, so any change the
    fixer makes to the item's ``collection`` link is a regression.
    """
    collection_dir = _tiny_catalog(root, collection=collection)
    _write_json(
        collection_dir / "US" / "catalog.json",
        {
            "type": "Catalog",
            "id": f"{collection}-US",
            "stac_version": "1.1.0",
            "description": "US",
            "links": [],
        },
    )
    item_json = collection_dir / "US" / "US_AB.json"
    _write_json(
        item_json,
        {
            "type": "Feature",
            "id": "US_AB",
            "stac_version": "1.1.0",
            "geometry": None,
            "bbox": [0.0, 0.0, 1.0, 1.0],
            "properties": {"datetime": "2024-01-01T00:00:00Z"},
            "assets": {},
            "collection": collection,
            "links": [
                {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
                {"rel": "parent", "href": "./catalog.json", "type": "application/json"},
                {"rel": "collection", "href": "../collection.json", "type": "application/json"},
            ],
        },
    )
    return item_json


class TestRegistryCompleteness:
    """The registry and the remediation table must name the same fixers."""

    def test_every_auto_rule_has_a_fixer(self) -> None:
        missing = {
            rule_id: remediation.fixer
            for rule_id, remediation in RULE_REMEDIATION.items()
            if remediation.bucket is Bucket.AUTO and remediation.fixer not in FIXERS
        }
        assert missing == {}

    def test_every_auto_rule_names_a_fixer(self) -> None:
        unnamed = [
            rule_id
            for rule_id, remediation in RULE_REMEDIATION.items()
            if remediation.bucket is Bucket.AUTO and not remediation.fixer
        ]
        assert unnamed == []

    def test_every_fixer_is_reachable_from_a_rule(self) -> None:
        referenced = {
            remediation.fixer
            for remediation in RULE_REMEDIATION.values()
            if remediation.bucket is Bucket.AUTO
        }
        assert set(FIXERS) - referenced == set()

    def test_non_auto_rules_name_no_fixer(self) -> None:
        stray = [
            rule_id
            for rule_id, remediation in RULE_REMEDIATION.items()
            if remediation.bucket is not Bucket.AUTO and remediation.fixer is not None
        ]
        assert stray == []


class TestAutoFixerKeys:
    def test_collects_distinct_keys_in_registry_order(self) -> None:
        keys = auto_fixer_keys(
            [_finding("PTL-TTL-003"), _finding("PTL-CNF-001"), _finding("PTL-TTL-001")]
        )
        assert keys == ["schema_uri", "titles"]

    def test_ignores_instruct_and_external_findings(self) -> None:
        assert auto_fixer_keys([_finding("PTL-LIC-001"), _finding("PTL-LIV-001")]) == []

    def test_unknown_rule_id_is_not_auto(self) -> None:
        assert auto_fixer_keys([_finding("PTL-ZZZ-999")]) == []


class TestApplyFixers:
    """Orchestration: each distinct key runs once, results merge into a FixReport."""

    def test_each_key_runs_once(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[str] = []

        def make(name: str) -> Any:
            def fixer(root: Path, dry_run: bool) -> list[FixResult]:
                calls.append(name)
                return [
                    FixResult(
                        file_path=root / name,
                        action=FixAction.UPDATED,
                        success=True,
                        message=name,
                    )
                ]

            return fixer

        monkeypatch.setattr(
            fixers_module, "FIXERS", {"schema_uri": make("schema_uri"), "titles": make("titles")}
        )

        run = apply_fixers(
            tmp_path,
            [_finding("PTL-TTL-001"), _finding("PTL-TTL-003"), _finding("PTL-CNF-001")],
            dry_run=False,
        )

        assert calls == ["schema_uri", "titles"]
        assert [r.message for r in run.report.results] == ["schema_uri", "titles"]

    def test_skip_excludes_a_key(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[str] = []

        def fixer(root: Path, dry_run: bool) -> list[FixResult]:
            calls.append("convert")
            return []

        monkeypatch.setattr(fixers_module, "FIXERS", {"convert": fixer})

        apply_fixers(tmp_path, [_finding("PTL-DAT-003")], dry_run=False, skip={"convert"})

        assert calls == []

    def test_dry_run_reaches_the_fixer(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen: list[bool] = []

        def fixer(root: Path, dry_run: bool) -> list[FixResult]:
            seen.append(dry_run)
            return []

        monkeypatch.setattr(fixers_module, "FIXERS", {"titles": fixer})
        apply_fixers(tmp_path, [_finding("PTL-TTL-001")], dry_run=True)

        assert seen == [True]

    def test_a_raising_fixer_becomes_a_failed_result(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def boom(root: Path, dry_run: bool) -> list[FixResult]:
            raise OSError("disk went away")

        monkeypatch.setattr(fixers_module, "FIXERS", {"titles": boom})
        run = apply_fixers(tmp_path, [_finding("PTL-TTL-001")], dry_run=False)

        assert run.report.failure_count == 1
        assert "disk went away" in run.report.results[0].message

    def test_no_auto_findings_produces_an_empty_report(self, tmp_path: Path) -> None:
        run = apply_fixers(tmp_path, [_finding("PTL-LIC-001")], dry_run=False)
        assert run.report.results == []

    def test_dry_run_writes_nothing(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        data = _read_json(collection / "collection.json")
        data["links"] = [{"rel": "self", "href": "./collection.json"}]
        data["assets"] = {"data": {"href": "./roads.parquet"}}
        _write_json(collection / "collection.json", data)

        before = _snapshot(tmp_path)
        apply_fixers(
            tmp_path,
            [
                _finding("PTL-LNK-005"),
                _finding("PTL-TTL-001"),
                _finding("PTL-CNF-001"),
                _finding("PTL-FIL-001"),
                _finding("PTL-AST-001"),
            ],
            dry_run=True,
        )

        assert _snapshot(tmp_path) == before


class TestFixerRun:
    """The run tells the caller which fixers were asked for, which acted, and why not."""

    @staticmethod
    def _acting(root: Path, dry_run: bool) -> list[FixResult]:
        return [FixResult(root / "acted", FixAction.UPDATED, True, "acted")]

    @staticmethod
    def _skipping(root: Path, dry_run: bool) -> list[FixResult]:
        return [
            FixResult(root / "a.json", FixAction.SKIPPED, True, "nothing to derive it from"),
            FixResult(root / "b.json", FixAction.SKIPPED, True, "nothing to derive it from"),
        ]

    def _patch(self, monkeypatch: pytest.MonkeyPatch, registry: dict[str, Any]) -> None:
        monkeypatch.setattr(fixers_module, "FIXERS", registry)

    def test_applied_lists_only_fixers_that_changed_something(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._patch(monkeypatch, {"schema_uri": self._acting, "titles": self._skipping})

        run = apply_fixers(
            tmp_path, [_finding("PTL-CNF-001"), _finding("PTL-TTL-001")], dry_run=False
        )

        assert run.selected == ["schema_uri", "titles"]
        assert run.applied == ["schema_uri"]

    def test_skip_reasons_dedupe_a_fully_skipped_fixer(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._patch(monkeypatch, {"titles": self._skipping})

        run = apply_fixers(tmp_path, [_finding("PTL-TTL-001")], dry_run=False)

        assert run.skip_reasons == {"titles": ["nothing to derive it from"]}

    def test_a_fixer_that_acted_has_no_skip_reason(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._patch(monkeypatch, {"schema_uri": self._acting})

        run = apply_fixers(tmp_path, [_finding("PTL-CNF-001")], dry_run=False)

        assert run.skip_reasons == {}

    def test_skip_parameter_removes_the_key_from_selected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._patch(monkeypatch, {"schema_uri": self._acting, "titles": self._acting})

        run = apply_fixers(
            tmp_path,
            [_finding("PTL-CNF-001"), _finding("PTL-TTL-001")],
            dry_run=False,
            skip={"titles"},
        )

        assert run.selected == ["schema_uri"]

    def test_a_raising_fixer_is_selected_but_not_applied(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def boom(root: Path, dry_run: bool) -> list[FixResult]:
            raise OSError("disk went away")

        self._patch(monkeypatch, {"titles": boom})

        run = apply_fixers(tmp_path, [_finding("PTL-TTL-001")], dry_run=False)

        assert run.selected == ["titles"]
        assert run.applied == []
        assert run.report.failure_count == 1


class TestComposedFixers:
    """`required_files` already runs `agents` and `readme`; it must not run them twice."""

    def test_composite_absorbs_its_members(self) -> None:
        keys = auto_fixer_keys(
            [_finding("PTL-FIL-001"), _finding("PTL-FIL-002"), _finding("PTL-FIL-003")]
        )
        assert keys == ["required_files"]

    def test_members_still_run_without_the_composite(self) -> None:
        assert auto_fixer_keys([_finding("PTL-FIL-003")]) == ["readme"]

    def test_registry_still_exposes_every_member(self) -> None:
        assert {"required_files", "agents", "readme"} <= set(FIXERS)


class TestLinksFixer:
    """PTL-LNK-001..006: the structural link block is derivable from the tree."""

    def test_self_link_is_removed(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        data = _read_json(collection / "collection.json")
        data["links"] = [{"rel": "self", "href": "./collection.json", "type": "application/json"}]
        _write_json(collection / "collection.json", data)

        FIXERS["links"](tmp_path, False)

        rels = [link["rel"] for link in _read_json(collection / "collection.json")["links"]]
        assert "self" not in rels

    def test_root_and_parent_links_are_added(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)

        FIXERS["links"](tmp_path, False)

        links = _read_json(collection / "collection.json")["links"]
        by_rel = {link["rel"]: link for link in links}
        assert by_rel["root"]["href"] == "../catalog.json"
        assert by_rel["root"]["type"] == "application/json"
        assert by_rel["parent"]["href"] == "../catalog.json"

    def test_child_link_is_backfilled_on_the_parent(self, tmp_path: Path) -> None:
        _tiny_catalog(tmp_path)

        FIXERS["links"](tmp_path, False)

        links = _read_json(tmp_path / "catalog.json")["links"]
        child = [link for link in links if link["rel"] == "child"]
        assert child == [
            {
                "rel": "child",
                "href": "./roads/collection.json",
                "type": "application/json",
                "title": "roads",
            }
        ]

    def test_item_link_uses_the_geojson_media_type(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        _write_json(
            collection / "scene-a" / "scene-a.json",
            {
                "type": "Feature",
                "stac_version": "1.1.0",
                "id": "scene-a",
                "geometry": None,
                "bbox": [0.0, 0.0, 1.0, 1.0],
                "properties": {"datetime": "2024-01-01T00:00:00Z"},
                "assets": {},
                "links": [],
            },
        )

        FIXERS["links"](tmp_path, False)

        links = _read_json(collection / "collection.json")["links"]
        item = next(link for link in links if link["rel"] == "item")
        assert item["href"] == "./scene-a/scene-a.json"
        assert item["type"] == "application/geo+json"

    def test_item_gets_root_parent_and_collection_links(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        item_json = collection / "scene-a" / "scene-a.json"
        _write_json(
            item_json,
            {
                "type": "Feature",
                "stac_version": "1.1.0",
                "id": "scene-a",
                "geometry": None,
                "bbox": [0.0, 0.0, 1.0, 1.0],
                "properties": {"datetime": "2024-01-01T00:00:00Z"},
                "assets": {},
                "links": [],
            },
        )

        FIXERS["links"](tmp_path, False)

        by_rel = {link["rel"]: link for link in _read_json(item_json)["links"]}
        assert by_rel["root"]["href"] == "../../catalog.json"
        assert by_rel["parent"]["href"] == "../collection.json"
        assert by_rel["collection"]["href"] == "../collection.json"

    def test_item_under_an_organizing_catalog_keeps_its_collection_link(
        self, tmp_path: Path
    ) -> None:
        """A catalog may organize a collection's items (core.md:168-170).

        The item's parent is then that catalog while its collection link points
        past it to the enclosing collection. The rebuild used to emit no
        ``collection`` pair for such an item and then drop the existing link as
        an unexpected structural rel, stripping a link PORTO-CORE-035 requires.
        """
        item_json = _organizing_catalog(tmp_path)

        FIXERS["links"](tmp_path, False)

        by_rel = {link["rel"]: link for link in _read_json(item_json)["links"]}
        assert by_rel["parent"]["href"] == "./catalog.json"
        assert by_rel["collection"]["href"] == "../collection.json"
        assert by_rel["collection"]["type"] == "application/json"

    def test_organizing_catalog_gets_no_collection_link(self, tmp_path: Path) -> None:
        """Only items carry a collection link; the catalog between keeps parent."""
        _organizing_catalog(tmp_path)

        FIXERS["links"](tmp_path, False)

        links = _read_json(tmp_path / "roads" / "US" / "catalog.json")["links"]
        by_rel = {link["rel"]: link for link in links}
        assert "collection" not in by_rel
        assert by_rel["parent"]["href"] == "../collection.json"

    def test_rebuild_survives_a_non_tuple_structural_rels(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``STRUCTURAL_RELS`` is rashid's to shape; we only need membership.

        Concatenating it with a tuple raised ``TypeError`` the moment upstream
        exported anything but a tuple.
        """
        monkeypatch.setattr(
            fixers_module, "STRUCTURAL_RELS", frozenset(fixers_module.STRUCTURAL_RELS)
        )
        collection = _tiny_catalog(tmp_path)
        data = _read_json(collection / "collection.json")
        data["links"] = [
            {"rel": "self", "href": "./collection.json", "type": "application/json"},
            {"rel": "license", "href": "https://example.org/license", "type": "text/html"},
        ]
        _write_json(collection / "collection.json", data)

        FIXERS["links"](tmp_path, False)

        rels = [link["rel"] for link in _read_json(collection / "collection.json")["links"]]
        assert "self" not in rels
        assert "license" in rels

    def test_absolute_href_is_relativized(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        data = _read_json(collection / "collection.json")
        data["links"] = [
            {
                "rel": "root",
                "href": str(tmp_path / "catalog.json"),
                "type": "application/json",
            }
        ]
        _write_json(collection / "collection.json", data)

        FIXERS["links"](tmp_path, False)

        root_link = next(
            link
            for link in _read_json(collection / "collection.json")["links"]
            if link["rel"] == "root"
        )
        assert root_link["href"] == "../catalog.json"

    def test_wrong_media_type_is_corrected(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        data = _read_json(collection / "collection.json")
        data["links"] = [{"rel": "root", "href": "../catalog.json", "type": "text/html"}]
        _write_json(collection / "collection.json", data)

        FIXERS["links"](tmp_path, False)

        root_link = next(
            link
            for link in _read_json(collection / "collection.json")["links"]
            if link["rel"] == "root"
        )
        assert root_link["type"] == "application/json"

    def test_title_survives_an_equivalent_href_spelling(self, tmp_path: Path) -> None:
        """`roads/collection.json` and `./roads/collection.json` are the same link."""
        _tiny_catalog(tmp_path)
        data = _read_json(tmp_path / "catalog.json")
        data["links"] = [
            {
                "rel": "child",
                "href": "roads/collection.json",
                "type": "application/json",
                "title": "Hand-written title",
            }
        ]
        _write_json(tmp_path / "catalog.json", data)

        FIXERS["links"](tmp_path, False)

        child = next(
            link
            for link in _read_json(tmp_path / "catalog.json")["links"]
            if link["rel"] == "child"
        )
        assert child["title"] == "Hand-written title"
        assert child["href"] == "./roads/collection.json"

    def test_non_structural_links_survive(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        data = _read_json(collection / "collection.json")
        data["links"] = [
            {"rel": "license", "href": "https://example.org/license", "type": "text/html"}
        ]
        _write_json(collection / "collection.json", data)

        FIXERS["links"](tmp_path, False)

        rels = [link["rel"] for link in _read_json(collection / "collection.json")["links"]]
        assert "license" in rels

    def test_link_title_is_preserved(self, tmp_path: Path) -> None:
        _tiny_catalog(tmp_path)
        data = _read_json(tmp_path / "catalog.json")
        data["links"] = [
            {
                "rel": "child",
                "href": "./roads/collection.json",
                "type": "application/json",
                "title": "Hand-written title",
            }
        ]
        _write_json(tmp_path / "catalog.json", data)

        FIXERS["links"](tmp_path, False)

        child = next(
            link
            for link in _read_json(tmp_path / "catalog.json")["links"]
            if link["rel"] == "child"
        )
        assert child["title"] == "Hand-written title"

    def test_dry_run_writes_nothing_but_reports(self, tmp_path: Path) -> None:
        _tiny_catalog(tmp_path)
        before = _snapshot(tmp_path)

        results = FIXERS["links"](tmp_path, True)

        assert _snapshot(tmp_path) == before
        assert [r.file_path.name for r in results] == ["catalog.json", "collection.json"]


class TestBboxFixer:
    """PTL-BBX-001: a collection extent is the union of what it contains."""

    def _collection_with_items(self, tmp_path: Path, extent_bbox: list[float]) -> Path:
        collection = _tiny_catalog(tmp_path)
        data = _read_json(collection / "collection.json")
        data["extent"]["spatial"]["bbox"] = [extent_bbox]
        _write_json(collection / "collection.json", data)
        for name, bbox in (("a", [0.0, 0.0, 2.0, 2.0]), ("b", [1.0, -1.0, 3.0, 1.0])):
            _write_json(
                collection / name / f"{name}.json",
                {
                    "type": "Feature",
                    "stac_version": "1.1.0",
                    "id": name,
                    "geometry": None,
                    "bbox": bbox,
                    "properties": {"datetime": "2024-01-01T00:00:00Z"},
                    "assets": {},
                    "links": [],
                },
            )
        return collection

    def test_extent_is_recomputed_from_item_bboxes(self, tmp_path: Path) -> None:
        collection = self._collection_with_items(tmp_path, [200.0, -95.0, 300.0, 95.0])

        FIXERS["bbox"](tmp_path, False)

        extent = _read_json(collection / "collection.json")["extent"]["spatial"]["bbox"]
        assert extent == [[0.0, -1.0, 3.0, 2.0]]

    def test_extent_covers_items_under_an_organizing_catalog(self, tmp_path: Path) -> None:
        """A broken extent must recompute from items held through a catalog.

        Walking direct children only found the organizing catalog, which carries
        no extent of its own, so the union came back empty and the repair was
        reported as skipped instead of recomputed.
        """
        _organizing_catalog(tmp_path)
        collection_json = tmp_path / "roads" / "collection.json"
        data = _read_json(collection_json)
        data["extent"]["spatial"]["bbox"] = [[0.0, 0.0, 200.0, 100.0]]
        _write_json(collection_json, data)

        results = FIXERS["bbox"](tmp_path, False)

        assert _read_json(collection_json)["extent"]["spatial"]["bbox"] == [[0.0, 0.0, 1.0, 1.0]]
        assert [r.action for r in results] == [FixAction.UPDATED]

    def test_valid_extent_is_left_alone(self, tmp_path: Path) -> None:
        self._collection_with_items(tmp_path, [0.0, -1.0, 3.0, 2.0])
        before = _snapshot(tmp_path)

        results = FIXERS["bbox"](tmp_path, False)

        assert _snapshot(tmp_path) == before
        assert results == []

    def test_no_derivable_source_is_skipped_not_invented(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        data = _read_json(collection / "collection.json")
        data["extent"]["spatial"]["bbox"] = [[float("1e309"), 0.0, 1.0, 1.0]]
        _write_json(collection / "collection.json", data)

        results = FIXERS["bbox"](tmp_path, False)

        assert [r.action for r in results] == [FixAction.SKIPPED]
        assert "nothing readable to derive it from" in results[0].message

    def test_dry_run_writes_nothing(self, tmp_path: Path) -> None:
        self._collection_with_items(tmp_path, [200.0, -95.0, 300.0, 95.0])
        before = _snapshot(tmp_path)

        results = FIXERS["bbox"](tmp_path, True)

        assert _snapshot(tmp_path) == before
        assert len(results) == 1


class TestAssetsFixer:
    """PTL-AST-001: media type and roles are derivable from the file extension."""

    def _collection_with_asset(self, tmp_path: Path, asset: dict[str, Any]) -> Path:
        collection = _tiny_catalog(tmp_path)
        data = _read_json(collection / "collection.json")
        data["assets"] = {"data": asset}
        _write_json(collection / "collection.json", data)
        return collection

    def test_missing_type_is_derived_from_the_extension(self, tmp_path: Path) -> None:
        collection = self._collection_with_asset(
            tmp_path, {"href": "./roads.parquet", "roles": ["data"]}
        )

        FIXERS["assets"](tmp_path, False)

        asset = _read_json(collection / "collection.json")["assets"]["data"]
        assert asset["type"] == "application/vnd.apache.parquet"

    def test_missing_roles_are_derived_from_the_extension(self, tmp_path: Path) -> None:
        collection = self._collection_with_asset(
            tmp_path, {"href": "./thumbnail.png", "type": "image/png"}
        )

        FIXERS["assets"](tmp_path, False)

        asset = _read_json(collection / "collection.json")["assets"]["data"]
        assert asset["roles"] == ["thumbnail"]

    def test_existing_type_is_not_overwritten(self, tmp_path: Path) -> None:
        collection = self._collection_with_asset(
            tmp_path,
            {"href": "./roads.parquet", "type": "application/x-custom", "roles": ["data"]},
        )

        results = FIXERS["assets"](tmp_path, False)

        asset = _read_json(collection / "collection.json")["assets"]["data"]
        assert asset["type"] == "application/x-custom"
        assert results == []

    def test_unknown_extension_is_skipped_not_guessed(self, tmp_path: Path) -> None:
        """`application/octet-stream` carries no information; do not write it (#682)."""
        collection = self._collection_with_asset(tmp_path, {"href": "./notes.qqq"})

        results = FIXERS["assets"](tmp_path, False)

        asset = _read_json(collection / "collection.json")["assets"]["data"]
        assert "type" not in asset
        assert "roles" not in asset
        assert [r.action for r in results] == [FixAction.SKIPPED]
        assert "not in the extension registry" in results[0].message

    def test_a_known_asset_is_still_typed_beside_an_unknown_one(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        data = _read_json(collection / "collection.json")
        data["assets"] = {
            "data": {"href": "./roads.parquet"},
            "notes": {"href": "./notes.qqq"},
        }
        _write_json(collection / "collection.json", data)

        results = FIXERS["assets"](tmp_path, False)

        assets = _read_json(collection / "collection.json")["assets"]
        assert assets["data"]["type"] == "application/vnd.apache.parquet"
        assert {r.action for r in results} == {FixAction.UPDATED, FixAction.SKIPPED}

    def test_dry_run_writes_nothing(self, tmp_path: Path) -> None:
        self._collection_with_asset(tmp_path, {"href": "./roads.parquet"})
        before = _snapshot(tmp_path)

        results = FIXERS["assets"](tmp_path, True)

        assert _snapshot(tmp_path) == before
        assert len(results) == 1


class TestChecksumFixer:
    """PTL-AST-003/004 and PTL-DAT-001/002: size and checksum come from the bytes."""

    def _collection_with_file(self, tmp_path: Path, asset: dict[str, Any]) -> Path:
        collection = _tiny_catalog(tmp_path)
        (collection / "roads.parquet").write_bytes(b"hello portolan")
        data = _read_json(collection / "collection.json")
        data["assets"] = {"data": {"href": "./roads.parquet", **asset}}
        _write_json(collection / "collection.json", data)
        return collection

    def test_size_and_checksum_are_computed(self, tmp_path: Path) -> None:
        import hashlib

        collection = self._collection_with_file(tmp_path, {})

        FIXERS["checksum"](tmp_path, False)

        asset = _read_json(collection / "collection.json")["assets"]["data"]
        assert asset["file:size"] == len(b"hello portolan")
        assert asset["file:checksum"] == "1220" + hashlib.sha256(b"hello portolan").hexdigest()

    def test_bare_digest_is_reencoded_as_multihash(self, tmp_path: Path) -> None:
        import hashlib

        digest = hashlib.sha256(b"hello portolan").hexdigest()
        collection = self._collection_with_file(
            tmp_path, {"file:checksum": digest, "file:size": len(b"hello portolan")}
        )

        FIXERS["checksum"](tmp_path, False)

        asset = _read_json(collection / "collection.json")["assets"]["data"]
        assert asset["file:checksum"] == f"1220{digest}"

    def test_stale_checksum_is_recomputed(self, tmp_path: Path) -> None:
        collection = self._collection_with_file(
            tmp_path, {"file:checksum": "1220" + "0" * 64, "file:size": 3}
        )

        FIXERS["checksum"](tmp_path, False)

        asset = _read_json(collection / "collection.json")["assets"]["data"]
        assert asset["file:size"] == len(b"hello portolan")
        assert asset["file:checksum"] != "1220" + "0" * 64

    def test_correct_asset_is_left_alone(self, tmp_path: Path) -> None:
        import hashlib

        self._collection_with_file(
            tmp_path,
            {
                "file:size": len(b"hello portolan"),
                "file:checksum": "1220" + hashlib.sha256(b"hello portolan").hexdigest(),
            },
        )
        before = _snapshot(tmp_path)

        results = FIXERS["checksum"](tmp_path, False)

        assert _snapshot(tmp_path) == before
        assert results == []

    def test_remote_href_is_skipped(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        data = _read_json(collection / "collection.json")
        data["assets"] = {"data": {"href": "https://example.org/roads.parquet"}}
        _write_json(collection / "collection.json", data)

        results = FIXERS["checksum"](tmp_path, False)

        assert results == []
        assert "file:size" not in _read_json(collection / "collection.json")["assets"]["data"]

    def test_dry_run_writes_nothing(self, tmp_path: Path) -> None:
        self._collection_with_file(tmp_path, {})
        before = _snapshot(tmp_path)

        results = FIXERS["checksum"](tmp_path, True)

        assert _snapshot(tmp_path) == before
        assert len(results) == 1


class TestStylesFixer:
    """PTL-VIZ-005: a style asset in a PMTiles collection is a MapLibre style."""

    def test_style_media_type_is_corrected(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        data = _read_json(collection / "collection.json")
        data["assets"] = {
            "tiles": {"href": "./roads.pmtiles", "type": "application/vnd.pmtiles"},
            "styles/default": {
                "href": "./styles/default.json",
                "type": "application/json",
                "roles": ["style"],
            },
        }
        _write_json(collection / "collection.json", data)

        FIXERS["styles"](tmp_path, False)

        assets = _read_json(collection / "collection.json")["assets"]
        assert assets["styles/default"]["type"] == "application/vnd.mapbox.style+json"

    def test_collection_without_pmtiles_is_untouched(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        data = _read_json(collection / "collection.json")
        data["assets"] = {
            "styles/default": {
                "href": "./styles/default.json",
                "type": "application/json",
                "roles": ["style"],
            }
        }
        _write_json(collection / "collection.json", data)
        before = _snapshot(tmp_path)

        assert FIXERS["styles"](tmp_path, False) == []
        assert _snapshot(tmp_path) == before


class TestItemMirrorFixer:
    """PTL-MIR-002: a published mirror carries the collection-mirror role."""

    def test_mirror_role_and_type_are_set(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        (collection / "items.parquet").write_bytes(b"parquet")
        data = _read_json(collection / "collection.json")
        data["assets"] = {"geoparquet-items": {"href": "./items.parquet", "roles": ["stac-items"]}}
        _write_json(collection / "collection.json", data)

        FIXERS["item_mirror"](tmp_path, False)

        asset = _read_json(collection / "collection.json")["assets"]["geoparquet-items"]
        assert "collection-mirror" in asset["roles"]
        assert asset["type"] == "application/vnd.apache.parquet"

    def test_conformant_mirror_is_left_alone(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        (collection / "items.parquet").write_bytes(b"parquet")
        data = _read_json(collection / "collection.json")
        data["assets"] = {
            "geoparquet-items": {
                "href": "./items.parquet",
                "roles": ["stac-items", "collection-mirror"],
                "type": "application/vnd.apache.parquet",
            }
        }
        _write_json(collection / "collection.json", data)
        before = _snapshot(tmp_path)

        assert FIXERS["item_mirror"](tmp_path, False) == []
        assert _snapshot(tmp_path) == before


class TestPartitionFixer:
    """PTL-PRT-001: the partition block is readable off the Hive layout on disk."""

    def test_fields_are_backfilled_from_the_layout(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        (collection / "year=2024").mkdir()
        (collection / "year=2024" / "part.parquet").write_bytes(b"x")
        data = _read_json(collection / "collection.json")
        data["partition:scheme"] = "hive"
        _write_json(collection / "collection.json", data)

        FIXERS["partition"](tmp_path, False)

        fixed = _read_json(collection / "collection.json")
        assert [key["name"] for key in fixed["partition:keys"]] == ["year"]
        assert fixed["partition:glob"] == "./year=*/*.parquet"
        assert any(
            uri.startswith("https://schemas.portolan-sdi.org/incubating/partition/")
            for uri in fixed["stac_extensions"]
        )

    def test_unpartitioned_collection_is_untouched(self, tmp_path: Path) -> None:
        _tiny_catalog(tmp_path)
        before = _snapshot(tmp_path)

        assert FIXERS["partition"](tmp_path, False) == []
        assert _snapshot(tmp_path) == before


class TestSchemaUriFixer:
    def test_schema_uri_is_stamped(self, tmp_path: Path) -> None:
        from portolan_cli.constants import PORTOLAN_SCHEMA_URI

        _tiny_catalog(tmp_path)

        results = FIXERS["schema_uri"](tmp_path, False)

        extensions = _read_json(tmp_path / "catalog.json")["stac_extensions"]
        assert PORTOLAN_SCHEMA_URI in extensions
        assert len(results) == 2

    def test_dry_run_writes_nothing(self, tmp_path: Path) -> None:
        _tiny_catalog(tmp_path)
        before = _snapshot(tmp_path)

        results = FIXERS["schema_uri"](tmp_path, True)

        assert _snapshot(tmp_path) == before
        assert len(results) == 2


class TestRequiredFilesFixer:
    def test_readme_and_agents_md_are_scaffolded(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)

        FIXERS["required_files"](tmp_path, False)

        for directory in (tmp_path, collection):
            assert (directory / "README.md").exists()
            assert (directory / "AGENTS.md").exists()

    def test_dry_run_writes_nothing(self, tmp_path: Path) -> None:
        _tiny_catalog(tmp_path)
        before = _snapshot(tmp_path)

        results = FIXERS["required_files"](tmp_path, True)

        assert _snapshot(tmp_path) == before
        assert results != []


class TestMarkdownLinkGapParity:
    """The readme/agents fixers must fire on every case rashid PTL-FIL-002/-003 flags."""

    def _scaffolded(self, tmp_path: Path) -> Path:
        """A catalog whose README/AGENTS files and links already conform."""
        _tiny_catalog(tmp_path)
        FIXERS["required_files"](tmp_path, False)
        return tmp_path / "catalog.json"

    def _mutate_link(self, catalog_json: Path, rel: str, **changes: Any) -> None:
        data = _read_json(catalog_json)
        for link in data["links"]:
            if link.get("rel") == rel:
                link.update(changes)
        _write_json(catalog_json, data)

    def test_conforming_tree_is_left_alone(self, tmp_path: Path) -> None:
        self._scaffolded(tmp_path)

        assert FIXERS["readme"](tmp_path, False) == []
        assert FIXERS["agents"](tmp_path, False) == []

    @pytest.mark.parametrize(
        ("fixer", "rel", "changes"),
        [
            ("readme", "describedby", {"type": "text/html"}),
            ("readme", "describedby", {"href": "/etc/README.md"}),
            ("readme", "describedby", {"href": "./nowhere/README.md"}),
            ("agents", "agents", {"type": "text/html"}),
            ("agents", "agents", {"href": "/etc/AGENTS.md"}),
            ("agents", "agents", {"href": "./nowhere/AGENTS.md"}),
        ],
    )
    def test_a_broken_link_is_detected(
        self, tmp_path: Path, fixer: str, rel: str, changes: dict[str, Any]
    ) -> None:
        catalog_json = self._scaffolded(tmp_path)
        self._mutate_link(catalog_json, rel, **changes)

        results = FIXERS[fixer](tmp_path, False)

        assert [r.file_path for r in results] == [catalog_json]

    def test_a_missing_file_is_detected(self, tmp_path: Path) -> None:
        self._scaffolded(tmp_path)
        (tmp_path / "README.md").unlink()

        results = FIXERS["readme"](tmp_path, False)

        assert [r.file_path for r in results] == [tmp_path / "catalog.json"]
        assert (tmp_path / "README.md").exists()

    def test_a_gap_the_repair_cannot_close_is_reported_skipped(self, tmp_path: Path) -> None:
        """`check --fix` must not claim a repair whose finding will survive."""
        catalog_json = self._scaffolded(tmp_path)
        self._mutate_link(catalog_json, "agents", href=str(tmp_path / "AGENTS.md"))

        results = FIXERS["agents"](tmp_path, False)

        assert [r.action for r in results] == [FixAction.SKIPPED]
        assert "by hand" in results[0].message


class TestConvertFixer:
    """`convert` is registered but does nothing: conversion belongs to the geo-asset pass."""

    def test_convertible_bytes_are_untouched(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        source = collection / "roads.geojson"
        source.write_text('{"type": "FeatureCollection", "features": []}', encoding="utf-8")
        before = _snapshot(tmp_path)

        results = FIXERS["convert"](tmp_path, False)

        assert _snapshot(tmp_path) == before
        assert [r.action for r in results] == [FixAction.SKIPPED]

    def test_the_skip_names_the_pass_that_owns_conversion(self, tmp_path: Path) -> None:
        _tiny_catalog(tmp_path)

        results = FIXERS["convert"](tmp_path, False)

        assert results[0].message == (
            "Conversion runs in the geo-asset pass; re-run `portolan check --fix` "
            "without --metadata"
        )


def _copy_fixture(name: str, destination: Path) -> Path:
    """Copy a repo test fixture next to a STAC object under ``tmp_path``."""
    import shutil

    source = Path(__file__).resolve().parents[2] / "fixtures" / name
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(source, destination)
    return destination


#: A GeoParquet fixture and the WGS84 footprint actually recorded in its
#: metadata — not a default, not a whole-world stand-in the fixer could invent.
_FIXTURE = "scan/clean_flat/example.parquet"
_FIXTURE_BBOX = (-180.0, -90.0, 180.0, 83.6451)


class TestBboxFixerFromAssets:
    """PTL-BBX-001 also fires on items and on asset-only collections."""

    def _item(self, collection: Path, *, bbox: Any, assets: dict[str, Any], **extra: Any) -> Path:
        item_json = collection / "scene-a" / "scene-a.json"
        data: dict[str, Any] = {
            "type": "Feature",
            "stac_version": "1.1.0",
            "id": "scene-a",
            "properties": {"datetime": "2024-01-01T00:00:00Z"},
            "assets": assets,
            "links": [],
            **extra,
        }
        if bbox is not None:
            data["bbox"] = bbox
        _write_json(item_json, data)
        return item_json

    def test_item_bbox_is_recomputed_from_its_own_asset(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        _copy_fixture(_FIXTURE, collection / "scene-a" / "data.parquet")
        item_json = self._item(
            collection,
            bbox=[200.0, -95.0, 300.0, 95.0],
            assets={"data": {"href": "./data.parquet"}},
        )

        FIXERS["bbox"](tmp_path, False)

        assert _read_json(item_json)["bbox"] == pytest.approx(_FIXTURE_BBOX)

    def test_a_valid_item_bbox_is_left_alone(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        _copy_fixture(_FIXTURE, collection / "scene-a" / "data.parquet")
        self._item(
            collection, bbox=[0.0, 0.0, 1.0, 1.0], assets={"data": {"href": "./data.parquet"}}
        )
        before = _snapshot(tmp_path)

        FIXERS["bbox"](tmp_path, False)

        assert _snapshot(tmp_path) == before

    def test_geometry_is_synthesized_when_absent(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        _copy_fixture(_FIXTURE, collection / "scene-a" / "data.parquet")
        item_json = self._item(
            collection,
            bbox=[200.0, -95.0, 300.0, 95.0],
            assets={"data": {"href": "./data.parquet"}},
        )

        FIXERS["bbox"](tmp_path, False)

        geometry = _read_json(item_json)["geometry"]
        assert geometry["type"] == "Polygon"
        assert geometry["coordinates"][0][0] == pytest.approx([_FIXTURE_BBOX[0], _FIXTURE_BBOX[1]])

    def test_an_existing_geometry_is_never_overwritten(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        _copy_fixture(_FIXTURE, collection / "scene-a" / "data.parquet")
        hand_written = {"type": "Point", "coordinates": [4.3, 52.07]}
        item_json = self._item(
            collection,
            bbox=[200.0, -95.0, 300.0, 95.0],
            assets={"data": {"href": "./data.parquet"}},
            geometry=hand_written,
        )

        FIXERS["bbox"](tmp_path, False)

        assert _read_json(item_json)["geometry"] == hand_written

    def test_an_unreadable_suffix_is_skipped_never_invented(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        (collection / "scene-a").mkdir(parents=True, exist_ok=True)
        (collection / "scene-a" / "notes.qqq").write_bytes(b"not geospatial")
        item_json = self._item(
            collection,
            bbox=[200.0, -95.0, 300.0, 95.0],
            assets={"data": {"href": "./notes.qqq"}},
        )

        results = FIXERS["bbox"](tmp_path, False)

        assert _read_json(item_json)["bbox"] == [200.0, -95.0, 300.0, 95.0]
        assert item_json in [r.file_path for r in results if r.action is FixAction.SKIPPED]

    def test_asset_only_collection_extent_is_recomputed(self, tmp_path: Path) -> None:
        """a vector collection carries the data itself and has no children."""
        collection = _tiny_catalog(tmp_path)
        _copy_fixture(_FIXTURE, collection / "roads.parquet")
        data = _read_json(collection / "collection.json")
        data["extent"]["spatial"]["bbox"] = [[200.0, -95.0, 300.0, 95.0]]
        data["assets"] = {"data": {"href": "./roads.parquet"}}
        _write_json(collection / "collection.json", data)

        FIXERS["bbox"](tmp_path, False)

        extent = _read_json(collection / "collection.json")["extent"]["spatial"]["bbox"]
        assert extent[0] == pytest.approx(list(_FIXTURE_BBOX))

    def test_child_bboxes_still_win_over_collection_assets(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        _copy_fixture(_FIXTURE, collection / "roads.parquet")
        data = _read_json(collection / "collection.json")
        data["extent"]["spatial"]["bbox"] = [[200.0, -95.0, 300.0, 95.0]]
        data["assets"] = {"data": {"href": "./roads.parquet"}}
        _write_json(collection / "collection.json", data)
        self._item(collection, bbox=[0.0, 0.0, 2.0, 2.0], assets={})

        FIXERS["bbox"](tmp_path, False)

        extent = _read_json(collection / "collection.json")["extent"]["spatial"]["bbox"]
        assert extent == [[0.0, 0.0, 2.0, 2.0]]

    def test_a_repaired_item_feeds_the_collection_extent(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        _copy_fixture(_FIXTURE, collection / "scene-a" / "data.parquet")
        self._item(
            collection,
            bbox=[200.0, -95.0, 300.0, 95.0],
            assets={"data": {"href": "./data.parquet"}},
        )
        data = _read_json(collection / "collection.json")
        data["extent"]["spatial"]["bbox"] = [[200.0, -95.0, 300.0, 95.0]]
        _write_json(collection / "collection.json", data)

        FIXERS["bbox"](tmp_path, False)

        extent = _read_json(collection / "collection.json")["extent"]["spatial"]["bbox"]
        assert extent[0] == pytest.approx(list(_FIXTURE_BBOX))

    def test_dry_run_writes_nothing(self, tmp_path: Path) -> None:
        collection = _tiny_catalog(tmp_path)
        _copy_fixture(_FIXTURE, collection / "scene-a" / "data.parquet")
        self._item(
            collection,
            bbox=[200.0, -95.0, 300.0, 95.0],
            assets={"data": {"href": "./data.parquet"}},
        )
        before = _snapshot(tmp_path)

        results = FIXERS["bbox"](tmp_path, True)

        assert _snapshot(tmp_path) == before
        assert [r.action for r in results] == [FixAction.UPDATED]


class TestItemMirrorCogPredicate:
    """PTL-MIR-001 is scoped to COG scene items, not to any TIFF."""

    def _collection_with_raster_item(self, tmp_path: Path, media_type: str) -> Path:
        collection = _tiny_catalog(tmp_path)
        _write_json(
            collection / "scene-a" / "scene-a.json",
            {
                "type": "Feature",
                "stac_version": "1.1.0",
                "id": "scene-a",
                "geometry": None,
                "bbox": [0.0, 0.0, 1.0, 1.0],
                "properties": {"datetime": "2024-01-01T00:00:00Z"},
                "assets": {"data": {"href": "./scene-a.tif", "type": media_type}},
                "links": [],
            },
        )
        return collection

    def test_a_plain_tiff_does_not_request_a_mirror(self, tmp_path: Path) -> None:
        self._collection_with_raster_item(tmp_path, "image/tiff")

        assert FIXERS["item_mirror"](tmp_path, True) == []

    def test_a_geotiff_without_the_profile_does_not_request_a_mirror(self, tmp_path: Path) -> None:
        # what item.py writes for a .tif, so it is the value that actually
        # reaches this predicate on a real catalog
        self._collection_with_raster_item(tmp_path, "image/tiff; application=geotiff")
        before = _snapshot(tmp_path)

        assert FIXERS["item_mirror"](tmp_path, False) == []
        assert _snapshot(tmp_path) == before

    def test_a_cog_profile_requests_a_mirror(self, tmp_path: Path) -> None:
        self._collection_with_raster_item(
            tmp_path, "image/tiff; application=geotiff; profile=cloud-optimized"
        )

        results = FIXERS["item_mirror"](tmp_path, True)

        assert [r.action for r in results] == [FixAction.CREATED]

    def test_a_cog_under_an_organizing_catalog_requests_a_mirror(self, tmp_path: Path) -> None:
        """PTL-MIR-001 scopes by ownership, not by directory depth.

        A scene collection that groups its items under year catalogs still owes
        a mirror. Probing direct children only saw the catalog, never the COG.
        """
        collection = _tiny_catalog(tmp_path)
        _write_json(
            collection / "2024" / "catalog.json",
            {
                "type": "Catalog",
                "stac_version": "1.1.0",
                "id": "roads-2024",
                "description": "2024",
                "links": [],
            },
        )
        _write_json(
            collection / "2024" / "scene-a" / "scene-a.json",
            {
                "type": "Feature",
                "stac_version": "1.1.0",
                "id": "scene-a",
                "geometry": None,
                "bbox": [0.0, 0.0, 1.0, 1.0],
                "properties": {"datetime": "2024-01-01T00:00:00Z"},
                "assets": {
                    "data": {
                        "href": "./scene-a.tif",
                        "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                    }
                },
                "links": [],
            },
        )

        results = FIXERS["item_mirror"](tmp_path, True)

        assert [r.action for r in results] == [FixAction.CREATED]

    def test_dry_run_reports_the_action_the_real_run_takes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from portolan_cli import stac_parquet

        monkeypatch.setattr(stac_parquet, "generate_items_parquet", lambda directory: None)
        monkeypatch.setattr(stac_parquet, "add_parquet_link_to_collection", lambda directory: None)
        self._collection_with_raster_item(
            tmp_path, "image/tiff; application=geotiff; profile=cloud-optimized"
        )

        dry = FIXERS["item_mirror"](tmp_path, True)
        real = FIXERS["item_mirror"](tmp_path, False)

        assert [r.action for r in dry] == [r.action for r in real] == [FixAction.CREATED]


class TestPmtilesFixer:
    """PTL-VIZ-003: a PMTiles asset carries a collection-level rel='pmtiles' link."""

    def _collection_with_pmtiles(self, tmp_path: Path) -> Path:
        collection = _tiny_catalog(tmp_path)
        (collection / "roads.pmtiles").write_bytes(b"pmtiles")
        data = _read_json(collection / "collection.json")
        data["assets"] = {"tiles": {"href": "./roads.pmtiles", "type": "application/vnd.pmtiles"}}
        _write_json(collection / "collection.json", data)
        return collection

    def test_the_link_and_extension_are_added(self, tmp_path: Path) -> None:
        collection = self._collection_with_pmtiles(tmp_path)

        results = FIXERS["pmtiles"](tmp_path, False)

        fixed = _read_json(collection / "collection.json")
        assert [link["href"] for link in fixed["links"] if link["rel"] == "pmtiles"] == [
            "./roads.pmtiles"
        ]
        assert [r.action for r in results] == [FixAction.UPDATED]

    def test_a_collection_without_pmtiles_is_untouched(self, tmp_path: Path) -> None:
        _tiny_catalog(tmp_path)
        before = _snapshot(tmp_path)

        assert FIXERS["pmtiles"](tmp_path, False) == []
        assert _snapshot(tmp_path) == before

    def test_a_registered_link_is_not_duplicated(self, tmp_path: Path) -> None:
        collection = self._collection_with_pmtiles(tmp_path)
        FIXERS["pmtiles"](tmp_path, False)
        before = _snapshot(tmp_path)

        assert FIXERS["pmtiles"](tmp_path, False) == []
        assert _snapshot(tmp_path) == before
        assert collection.exists()

    def test_dry_run_writes_nothing(self, tmp_path: Path) -> None:
        self._collection_with_pmtiles(tmp_path)
        before = _snapshot(tmp_path)

        results = FIXERS["pmtiles"](tmp_path, True)

        assert _snapshot(tmp_path) == before
        assert len(results) == 1


class TestNodeWriting:
    def test_non_ascii_titles_survive_a_fixer_write(self, tmp_path: Path) -> None:
        """`write_json_atomic` keeps `ensure_ascii=False`, so "Córdoba" stays literal."""
        collection = _tiny_catalog(tmp_path)
        data = _read_json(collection / "collection.json")
        data["title"] = "Córdoba"
        data["links"] = [{"rel": "self", "href": "./collection.json"}]
        _write_json(collection / "collection.json", data)

        FIXERS["links"](tmp_path, False)

        raw = (collection / "collection.json").read_text(encoding="utf-8")
        assert "Córdoba" in raw
        assert "\\u00f3" not in raw


class TestThumbnailFixer:
    """PTL-VIZ-001 has two repairs: retype a mistyped thumbnail, or make one."""

    def _collection_with_thumbnail(self, tmp_path: Path, media_type: str) -> Path:
        collection_dir = _tiny_catalog(tmp_path)
        (collection_dir / "roads.thumb.jpg").write_bytes(b"\xff\xd8\xff fake-jpeg")
        data = _read_json(collection_dir / "collection.json")
        data["assets"] = {
            "thumbnail": {
                "href": "./roads.thumb.jpg",
                "type": media_type,
                "roles": ["thumbnail"],
            }
        }
        _write_json(collection_dir / "collection.json", data)
        return collection_dir

    def test_retypes_from_the_file_it_points_at(self, tmp_path: Path) -> None:
        collection_dir = self._collection_with_thumbnail(tmp_path, "image/gif")

        results = FIXERS["thumbnail"](tmp_path, False)

        assert [r.action for r in results] == [FixAction.UPDATED]
        asset = _read_json(collection_dir / "collection.json")["assets"]["thumbnail"]
        assert asset["type"] == "image/jpeg"

    def test_a_correctly_typed_thumbnail_is_left_alone(self, tmp_path: Path) -> None:
        collection_dir = self._collection_with_thumbnail(tmp_path, "image/jpeg")
        before = _snapshot(tmp_path)

        assert FIXERS["thumbnail"](tmp_path, False) == []
        assert _snapshot(tmp_path) == before
        assert collection_dir.exists()

    def test_an_href_that_is_not_an_image_is_not_retyped(self, tmp_path: Path) -> None:
        """Retyping a thumbnail that points at a CSV would state a falsehood."""
        collection_dir = _tiny_catalog(tmp_path)
        (collection_dir / "notes.csv").write_text("a,b\n1,2\n", encoding="utf-8")
        data = _read_json(collection_dir / "collection.json")
        data["assets"] = {
            "thumbnail": {
                "href": "./notes.csv",
                "type": "text/csv",
                "roles": ["thumbnail"],
            }
        }
        _write_json(collection_dir / "collection.json", data)

        assert FIXERS["thumbnail"](tmp_path, False) == []
        asset = _read_json(collection_dir / "collection.json")["assets"]["thumbnail"]
        assert asset["type"] == "text/csv"

    def test_dry_run_reports_only_what_the_real_run_would_do(self, tmp_path: Path) -> None:
        """A tabular collection offers no thumbnail source, so neither run acts.

        The dry-run branch used to skip the orchestrator's gates and promise a
        thumbnail for every collection that lacked one.
        """
        collection_dir = _tiny_catalog(tmp_path)
        (collection_dir / "rates.csv").write_text("a,b\n1,2\n", encoding="utf-8")
        data = _read_json(collection_dir / "collection.json")
        data["assets"] = {"rates": {"href": "./rates.csv", "roles": ["data"]}}
        _write_json(collection_dir / "collection.json", data)

        assert FIXERS["thumbnail"](tmp_path, True) == []
        assert FIXERS["thumbnail"](tmp_path, False) == []

    def test_dry_run_writes_nothing_for_a_collection_it_would_fix(self, tmp_path: Path) -> None:
        collection_dir = self._collection_with_thumbnail(tmp_path, "image/gif")
        before = _snapshot(tmp_path)

        results = FIXERS["thumbnail"](tmp_path, True)

        assert [r.action for r in results] == [FixAction.UPDATED]
        assert _snapshot(tmp_path) == before
        assert collection_dir.exists()
