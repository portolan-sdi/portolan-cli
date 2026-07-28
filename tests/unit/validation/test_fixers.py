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

        report = apply_fixers(
            tmp_path,
            [_finding("PTL-TTL-001"), _finding("PTL-TTL-003"), _finding("PTL-CNF-001")],
            dry_run=False,
        )

        assert calls == ["schema_uri", "titles"]
        assert [r.message for r in report.results] == ["schema_uri", "titles"]

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
        report = apply_fixers(tmp_path, [_finding("PTL-TTL-001")], dry_run=False)

        assert report.failure_count == 1
        assert "disk went away" in report.results[0].message

    def test_no_auto_findings_produces_an_empty_report(self, tmp_path: Path) -> None:
        report = apply_fixers(tmp_path, [_finding("PTL-LIC-001")], dry_run=False)
        assert report.results == []

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
        assert "no valid child bbox" in results[0].message

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

    def test_unknown_extension_falls_back_to_octet_stream(self, tmp_path: Path) -> None:
        collection = self._collection_with_asset(tmp_path, {"href": "./notes.qqq"})

        FIXERS["assets"](tmp_path, False)

        asset = _read_json(collection / "collection.json")["assets"]["data"]
        assert asset["type"] == "application/octet-stream"
        assert asset["roles"] == ["data"]

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
