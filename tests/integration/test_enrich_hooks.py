"""The STAC enrichment hooks run in the `add` flow (#947)."""

from __future__ import annotations

import json
import shutil
from typing import TYPE_CHECKING, Any

import pytest

from portolan_cli.add import add_files
from portolan_cli.hookspecs import EnrichContext, hookimpl

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

pytestmark = pytest.mark.integration


class _Recorder:
    """Write the asset keys and the context into each document."""

    @hookimpl
    def portolan_enrich_item(self, item: dict[str, Any], context: EnrichContext) -> None:
        item["properties"]["test:assets"] = sorted(item.get("assets", {}))
        item["properties"]["test:item_id"] = context.item_id
        item["properties"]["test:collection_id"] = context.collection_id

    @hookimpl
    def portolan_enrich_collection(
        self, collection: dict[str, Any], context: EnrichContext
    ) -> None:
        collection["test:assets"] = sorted(collection.get("assets", {}))
        collection["test:collection_id"] = context.collection_id


def _read(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return data


@pytest.fixture
def catalog(conformant_catalog: Path, register_plugin: Callable[..., None]) -> Path:
    register_plugin(_Recorder(), "recorder")
    return conformant_catalog


def _add_raster(catalog: Path, fixtures_dir: Path, item: str) -> Path:
    item_dir = catalog / "imagery" / item
    item_dir.mkdir(parents=True)
    raster = item_dir / f"{item}.tif"
    shutil.copy(fixtures_dir / "raster" / "valid" / "singleband.tif", raster)
    _, _, failures = add_files(paths=[raster], catalog_root=catalog, collection_id="imagery")
    assert failures == []
    return item_dir / f"{item}.json"


def test_add_enriches_items_and_the_collection(catalog: Path, fixtures_dir: Path) -> None:
    """Both hooks run on the files `add` writes."""
    item_json = _add_raster(catalog, fixtures_dir, "scene-a")

    item = _read(item_json)
    collection = _read(catalog / "imagery" / "collection.json")

    assert item["properties"]["test:item_id"] == "scene-a"
    assert item["properties"]["test:collection_id"] == "imagery"
    assert item["properties"]["test:assets"] == sorted(item["assets"])
    assert collection["test:collection_id"] == "imagery"


def test_enrichment_survives_a_later_add(catalog: Path, fixtures_dir: Path) -> None:
    """A second add re-serializes the first item. Its enriched fields stay."""
    first = _add_raster(catalog, fixtures_dir, "scene-a")
    _add_raster(catalog, fixtures_dir, "scene-b")

    assert _read(first)["properties"]["test:item_id"] == "scene-a"


def test_enrichment_runs_before_the_backend_publishes(
    catalog: Path, fixtures_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A backend that uploads STAC JSON on publish sees the enriched files."""
    from portolan_cli import finalization

    seen: list[object] = []
    original = finalization._publish_collection_version

    def _spy(catalog_root: Path, collection_id: str, collection_dir: Path, *args: Any) -> None:
        item = _read(collection_dir / "scene-a" / "scene-a.json")
        seen.append(item["properties"].get("test:item_id"))
        seen.append(_read(collection_dir / "collection.json").get("test:collection_id"))
        original(catalog_root, collection_id, collection_dir, *args)

    monkeypatch.setattr(finalization, "_publish_collection_version", _spy)

    _add_raster(catalog, fixtures_dir, "scene-a")

    assert seen == ["scene-a", "imagery"]


def test_companion_asset_reruns_the_item_hook(catalog: Path, fixtures_dir: Path) -> None:
    """A CSV next to a raster joins the item, and the hook sees the new asset."""
    item_dir = catalog / "imagery" / "scene-a"
    item_dir.mkdir(parents=True)
    raster = item_dir / "scene-a.tif"
    shutil.copy(fixtures_dir / "raster" / "valid" / "singleband.tif", raster)
    (item_dir / "notes.csv").write_text("name,value\nband,1\n", encoding="utf-8")

    _, skipped, failures = add_files(
        paths=[item_dir], catalog_root=catalog, collection_id="imagery"
    )

    assert failures == []
    assert [path.name for path in skipped] == ["notes.csv"]
    item = _read(item_dir / "scene-a.json")
    # The asset key for a companion file differs between environments
    # (`notes` locally, `scene-a` on the CI runners), so find it by its file.
    hrefs = {key: asset["href"] for key, asset in item["assets"].items()}
    assert any(href.endswith("notes.csv") for href in hrefs.values()), hrefs
    assert item["properties"]["test:assets"] == sorted(item["assets"])


def test_standalone_tabular_collection_is_enriched(catalog: Path) -> None:
    """A tabular-only collection runs the collection hook with its asset."""
    config = catalog / ".portolan" / "config.yaml"
    config.write_text(config.read_text(encoding="utf-8") + "tabular:\n  enabled: true\n")
    collection_dir = catalog / "lookup"
    collection_dir.mkdir()
    table = collection_dir / "codes.csv"
    table.write_text("code,label\n1,road\n2,river\n", encoding="utf-8")

    _, _, failures = add_files(paths=[table], catalog_root=catalog, collection_id="lookup")

    assert failures == []
    collection = _read(collection_dir / "collection.json")
    assert collection["test:collection_id"] == "lookup"
    assert collection["test:assets"] == sorted(collection["assets"])
    assert collection["test:assets"] != []


def test_metadata_fix_enriches_a_created_item(catalog: Path, fixtures_dir: Path) -> None:
    """`metadata fix` creates a missing item, and the item hook runs on it."""
    from portolan_cli.metadata.update import create_missing_item

    _add_raster(catalog, fixtures_dir, "scene-a")
    loose = catalog / "imagery" / "scene-b" / "scene-b.tif"
    loose.parent.mkdir()
    shutil.copy(fixtures_dir / "raster" / "valid" / "singleband.tif", loose)

    created = create_missing_item(loose, catalog / "imagery")

    assert _read(created)["properties"]["test:item_id"] == "scene-b"


def test_add_external_enriches_the_collection(catalog: Path) -> None:
    """`add --external` runs the collection hook on the file it writes."""
    from portolan_cli.external import add_external

    result = add_external(
        url="https://data.example.org/roads.parquet",
        catalog_root=catalog,
        collection_id="roads",
        license="CC0-1.0",
    )

    collection = _read(result.collection_path)
    assert collection["test:collection_id"] == "roads"
    assert collection["test:assets"] == sorted(collection["assets"])
