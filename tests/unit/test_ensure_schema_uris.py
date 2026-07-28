"""Unit tests for the catalog-wide schema-URI sweep (issue #654)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.catalog import ensure_schema_uris
from portolan_cli.constants import PORTOLAN_SCHEMA_URI

pytestmark = pytest.mark.unit


def _write(path: Path, data: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _extensions(path: Path) -> list[str]:
    loaded: list[str] = json.loads(path.read_text(encoding="utf-8"))["stac_extensions"]
    return loaded


class TestEnsureSchemaUris:
    def test_stamps_catalogs_collections_and_subcatalogs(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", {"type": "Catalog", "id": "root"})
        _write(tmp_path / "climate" / "catalog.json", {"type": "Catalog", "id": "climate"})
        _write(
            tmp_path / "climate" / "heat" / "collection.json",
            {"type": "Collection", "id": "heat"},
        )

        assert ensure_schema_uris(tmp_path) is True
        for rel in ("catalog.json", "climate/catalog.json", "climate/heat/collection.json"):
            assert _extensions(tmp_path / rel) == [PORTOLAN_SCHEMA_URI]

    def test_leaves_items_alone(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", {"type": "Catalog", "id": "root"})
        item = tmp_path / "roads" / "seg" / "item.json"
        _write(item, {"type": "Feature", "id": "seg", "stac_extensions": []})

        ensure_schema_uris(tmp_path)

        assert _extensions(item) == []

    def test_is_idempotent(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", {"type": "Catalog", "id": "root"})

        assert ensure_schema_uris(tmp_path) is True
        assert ensure_schema_uris(tmp_path) is False

    def test_skips_unparseable_files(self, tmp_path: Path) -> None:
        (tmp_path / "catalog.json").write_text("{not json", encoding="utf-8")

        assert ensure_schema_uris(tmp_path) is False
