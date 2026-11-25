"""Unit tests for ``update_catalog_provenance`` (issue #684, PORTO-CORE-057).

A mirror sync sets ``updated`` on every catalog and collection it checks.
rashid's PTL-PRO-003 applies that to every catalog whose collections are all
mirrors, the root or a sub-catalog. A catalog that holds any official
collection is not a mirror and keeps no stamp.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

from portolan_cli.stac import update_catalog_provenance

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit

MIRROR = [
    {"name": "INDEC", "roles": ["producer", "licensor"]},
    {"name": "Example City GIS", "roles": ["host"]},
]
OFFICIAL = [{"name": "Example City GIS", "roles": ["producer", "licensor", "host"]}]


def _write(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _catalog(path: Path) -> None:
    _write(path / "catalog.json", {"type": "Catalog", "id": path.name, "links": []})


def _collection(path: Path, providers: list[dict[str, Any]], updated: str | None) -> None:
    data: dict[str, Any] = {"type": "Collection", "id": path.name, "providers": providers}
    if updated is not None:
        data["updated"] = updated
    _write(path / "collection.json", data)


def _updated(path: Path) -> object:
    return json.loads((path / "catalog.json").read_text(encoding="utf-8")).get("updated")


class TestSubCatalogs:
    def test_a_sub_catalog_of_mirrors_gets_the_newest_sync_time(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        _catalog(tmp_path / "env")
        _collection(tmp_path / "env" / "air", MIRROR, "2026-01-01T00:00:00Z")
        _collection(tmp_path / "env" / "water", MIRROR, "2026-02-01T00:00:00Z")

        update_catalog_provenance(tmp_path)

        assert _updated(tmp_path / "env") == "2026-02-01T00:00:00Z"
        assert _updated(tmp_path) == "2026-02-01T00:00:00Z"

    def test_a_mirror_sub_catalog_is_stamped_under_a_mixed_root(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        _catalog(tmp_path / "env")
        _collection(tmp_path / "env" / "air", MIRROR, "2026-01-01T00:00:00Z")
        _collection(tmp_path / "roads", OFFICIAL, None)

        update_catalog_provenance(tmp_path)

        assert _updated(tmp_path / "env") == "2026-01-01T00:00:00Z"
        assert _updated(tmp_path) is None

    def test_a_sub_catalog_with_an_official_collection_is_left_alone(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        _catalog(tmp_path / "env")
        _collection(tmp_path / "env" / "air", MIRROR, "2026-01-01T00:00:00Z")
        _collection(tmp_path / "env" / "noise", OFFICIAL, None)

        update_catalog_provenance(tmp_path)

        assert _updated(tmp_path / "env") is None
        assert _updated(tmp_path) is None

    def test_a_catalog_with_no_collections_is_left_alone(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        _catalog(tmp_path / "empty")
        _collection(tmp_path / "roads", MIRROR, "2026-01-01T00:00:00Z")

        update_catalog_provenance(tmp_path)

        assert _updated(tmp_path / "empty") is None
        assert _updated(tmp_path) == "2026-01-01T00:00:00Z"


class TestRoot:
    def test_a_root_of_mirrors_is_stamped(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        _collection(tmp_path / "roads", MIRROR, "2026-03-01T00:00:00Z")

        update_catalog_provenance(tmp_path)

        assert _updated(tmp_path) == "2026-03-01T00:00:00Z"

    def test_a_mixed_root_is_left_alone(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        _collection(tmp_path / "roads", MIRROR, "2026-03-01T00:00:00Z")
        _collection(tmp_path / "parks", OFFICIAL, None)

        update_catalog_provenance(tmp_path)

        assert _updated(tmp_path) is None

    def test_a_missing_root_catalog_is_a_no_op(self, tmp_path: Path) -> None:
        _collection(tmp_path / "roads", MIRROR, "2026-03-01T00:00:00Z")

        update_catalog_provenance(tmp_path)

        assert not (tmp_path / "catalog.json").exists()

    def test_unreadable_json_is_skipped(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        _collection(tmp_path / "roads", MIRROR, "2026-03-01T00:00:00Z")
        (tmp_path / "broken").mkdir()
        (tmp_path / "broken" / "collection.json").write_text("{", encoding="utf-8")
        _collection(tmp_path / "bad" / "air", MIRROR, "2026-01-01T00:00:00Z")
        (tmp_path / "bad" / "catalog.json").write_text("{", encoding="utf-8")

        update_catalog_provenance(tmp_path)

        assert _updated(tmp_path) == "2026-03-01T00:00:00Z"
        assert (tmp_path / "bad" / "catalog.json").read_text(encoding="utf-8") == "{"

    def test_mirrors_with_no_sync_time_leave_the_root_alone(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        _collection(tmp_path / "roads", MIRROR, None)

        update_catalog_provenance(tmp_path)

        assert _updated(tmp_path) is None

    def test_a_catalog_json_that_is_not_a_catalog_is_left_alone(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        _write(tmp_path / "env" / "catalog.json", {"type": "Feature", "id": "env"})
        _collection(tmp_path / "env" / "air", MIRROR, "2026-01-01T00:00:00Z")

        update_catalog_provenance(tmp_path)

        assert _updated(tmp_path / "env") is None
        assert _updated(tmp_path) == "2026-01-01T00:00:00Z"
