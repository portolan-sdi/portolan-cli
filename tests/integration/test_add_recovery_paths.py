"""Recovery must use published identities and converge after partial index failures."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from portolan_cli.add import add_files
from portolan_cli.cli import cli
from portolan_cli.run_state import RunState
from portolan_cli.versions import read_versions

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ["same-batch", "new-companion", "edited-companion"])
def test_custom_item_id_companion_is_tracked(
    conformant_catalog: Path, valid_rgb_cog: Path, mode: str
) -> None:
    root = conformant_catalog
    item_dir = root / "imagery" / "scene"
    item_dir.mkdir(parents=True)
    raster = item_dir / "scene.tif"
    raster.write_bytes(valid_rgb_cog.read_bytes())
    companion = item_dir / "attributes.csv"
    if mode != "new-companion":
        companion.write_text("id,name\n1,Scene\n")
    if mode != "same-batch":
        assert add_files(paths=[raster], catalog_root=root, item_id="custom")[2] == []
        companion.write_text("id,name\n1,New Scene\n2,Other Scene\n")
        paths = [companion]
    else:
        paths = [raster, companion]

    with patch("portolan_cli.add.enrich_item_file") as enrich:
        _added, skipped, failures = add_files(paths=paths, catalog_root=root, item_id="custom")

    assert failures == []
    assert companion in skipped
    item_path = item_dir / "custom.json"
    item = json.loads(item_path.read_text())
    assert (item_dir / item["assets"]["attributes"]["href"]).resolve() == companion.resolve()
    history = read_versions(root / "imagery" / "versions.json").versions
    assert len(history) == (1 if mode == "same-batch" else 2)
    version = history[-1]
    assert "scene/attributes.csv" in version.assets, "companion was reported tracked but lost"
    assert version.assets["scene/attributes.csv"].sha256 == RunState.open(root).checksum(companion)
    assert not (root / "imagery" / "custom").exists(), "item ID became a directory"
    assert enrich.call_count == 1, "recovered companion skipped item enrichment"
    assert enrich.call_args.args[0] == item_path


def test_unchanged_add_repairs_failed_catalog_index(
    conformant_catalog: Path, valid_points_parquet: Path
) -> None:
    root = conformant_catalog
    source = root / "roads" / "roads.parquet"
    source.parent.mkdir()
    source.write_bytes(valid_points_parquet.read_bytes())
    with patch("portolan_cli.catalog.update_catalog_index", side_effect=OSError("index failed")):
        assert add_files(paths=[source], catalog_root=root)[2] == []
    versions_path = source.parent / "versions.json"
    before = versions_path.read_bytes()
    assert "roads" not in json.loads((root / "versions.json").read_text())["collections"]

    assert add_files(paths=[source], catalog_root=root) == ([], [source], [])

    index = json.loads((root / "versions.json").read_text())
    assert "roads" in index["collections"], "unchanged add left the root index stale"
    assert (
        index["collections"]["roads"]["current_version"]
        == read_versions(versions_path).current_version
    )
    assert versions_path.read_bytes() == before
    index_before = (root / "versions.json").stat().st_mtime_ns
    add_files(paths=[source], catalog_root=root)
    assert (root / "versions.json").stat().st_mtime_ns == index_before


def test_metadata_repair_opens_one_run_for_the_whole_command(
    conformant_catalog: Path, valid_rgb_cog: Path
) -> None:
    root = conformant_catalog
    rasters = []
    for name in ("one", "two", "three"):
        raster = root / "imagery" / name / f"{name}.tif"
        raster.parent.mkdir(parents=True)
        raster.write_bytes(valid_rgb_cog.read_bytes())
        rasters.append(raster)
    assert add_files(paths=rasters, catalog_root=root)[2] == []
    for raster in rasters:
        raster.with_suffix(".json").unlink()

    with patch.object(RunState, "open", wraps=RunState.open) as opens:
        result = CliRunner().invoke(
            cli,
            ["check", str(root), "--metadata", "--fix", "--no-data", "--no-structural"],
            catch_exceptions=False,
        )

    assert all(raster.with_suffix(".json").exists() for raster in rasters), result.output
    assert opens.call_count == 1, (
        f"metadata repair opened {opens.call_count} independent run states"
    )


def test_same_batch_companion_does_not_rehash_assets(
    conformant_catalog: Path, valid_rgb_cog: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The deferred item scan shares hashes already computed during preparation."""
    from pathlib import Path
    from typing import IO, Any

    root = conformant_catalog
    item_dir = root / "imagery" / "scene"
    item_dir.mkdir(parents=True)
    raster = item_dir / "scene.tif"
    raster.write_bytes(valid_rgb_cog.read_bytes())
    companion = item_dir / "attributes.csv"
    companion.write_text("id,name\n1,Scene\n")
    reads: list[Path] = []
    original_open = Path.open

    def record_open(path: Path, mode: str = "r", *args: Any, **kwargs: Any) -> IO[Any]:
        if mode == "rb" and path in (raster, companion):
            reads.append(path)
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", record_open)
    assert add_files(paths=[raster, companion], catalog_root=root)[2] == []

    assert reads.count(raster) == 1, (
        f"deferred companion hashed the raster {reads.count(raster)} times"
    )
    assert reads.count(companion) == 1, "deferred companion was hashed twice"


def test_companion_recovers_item_below_an_organizing_directory(
    conformant_catalog: Path, valid_rgb_cog: Path
) -> None:
    """The owning collection may be more than one level above its item."""
    root = conformant_catalog
    item_dir = root / "imagery" / "group" / "scene"
    item_dir.mkdir(parents=True)
    raster = item_dir / "scene.tif"
    raster.write_bytes(valid_rgb_cog.read_bytes())
    assert add_files(paths=[raster], catalog_root=root, collection_id="imagery")[2] == []
    companion = item_dir / "attributes.csv"
    companion.write_text("id,name\n1,Scene\n")

    _added, skipped, failures = add_files(paths=[companion], catalog_root=root)

    assert failures == [], "nested companion lost its owning collection"
    assert companion in skipped
    item = json.loads((item_dir / "scene.json").read_text())
    assert "attributes" in item["assets"]
    version = read_versions(root / "imagery" / "versions.json").versions[-1]
    assert "group/scene/attributes.csv" in version.assets


def test_nested_unchanged_companion_is_skipped_without_writes(
    conformant_catalog: Path, valid_rgb_cog: Path
) -> None:
    root = conformant_catalog
    item_dir = root / "imagery" / "group" / "scene"
    item_dir.mkdir(parents=True)
    raster = item_dir / "scene.tif"
    raster.write_bytes(valid_rgb_cog.read_bytes())
    companion = item_dir / "attributes.csv"
    companion.write_text("id,name\n1,Scene\n")
    assert add_files(paths=[raster, companion], catalog_root=root, collection_id="imagery")[2] == []
    before = {p: p.stat().st_mtime_ns for p in root.rglob("*") if p.is_file()}

    added, skipped, failures = add_files(paths=[companion], catalog_root=root)

    assert (added, skipped, failures) == ([], [companion], [])
    assert {p: p.stat().st_mtime_ns for p in before} == before, (
        "unchanged nested companion rewrote STAC"
    )


def test_index_recovery_read_failure_does_not_fail_unchanged_add(
    conformant_catalog: Path, valid_points_parquet: Path
) -> None:
    root = conformant_catalog
    source = root / "roads" / "roads.parquet"
    source.parent.mkdir()
    source.write_bytes(valid_points_parquet.read_bytes())
    assert add_files(paths=[source], catalog_root=root)[2] == []

    with patch("portolan_cli.catalog.collection_summaries", side_effect=OSError("read failed")):
        assert add_files(paths=[source], catalog_root=root) == ([], [source], [])


def test_companion_recovery_preserves_conversion_provenance(
    conformant_catalog: Path, valid_rgb_cog: Path
) -> None:
    """Adding a CSV must retain the ownership that permits a later reconversion."""
    root = conformant_catalog
    item_dir = root / "imagery" / "scene"
    item_dir.mkdir(parents=True)
    import rasterio

    from portolan_cli.convert import claim_output

    source = item_dir / "scene.jp2"
    with (
        rasterio.open(valid_rgb_cog) as src,
        rasterio.open(
            source,
            "w",
            driver="JP2OpenJPEG",
            width=src.width,
            height=src.height,
            count=src.count,
            dtype=src.dtypes[0],
            crs=src.crs,
            transform=src.transform,
        ) as dst,
    ):
        dst.write(src.read())
    assert add_files(paths=[source], catalog_root=root)[2] == []
    versions_path = root / "imagery" / "versions.json"
    before = read_versions(versions_path).versions[-1].assets["scene/scene.tif"]
    assert before.source_path == "scene/scene.jp2"
    companion = item_dir / "attributes.csv"
    companion.write_text("id,name\n1,Scene\n")

    assert add_files(paths=[companion], catalog_root=root)[2] == []

    after = read_versions(versions_path).versions[-1].assets["scene/scene.tif"]
    assert after.source_path == before.source_path, (
        "companion recovery erased conversion provenance"
    )
    output = item_dir / "scene.tif"
    assert claim_output(source, output, state=RunState.open(root)) == output
