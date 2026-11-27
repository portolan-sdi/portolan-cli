"""``check --fix`` rewrites the data files that a finding names, and records the new bytes."""

from __future__ import annotations

import hashlib
import json
import random
import shutil
from typing import TYPE_CHECKING, Any

import pyarrow.parquet as pq
import pytest

from portolan_cli.sync.push import _get_assets_to_upload
from tests.conftest import CliHarness
from tests.property import breakers

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def core(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The property-test core base, built once through the CLI."""
    home = tmp_path_factory.mktemp("rewrite-core")
    root = home / "catalog"
    breakers.build_core(root, CliHarness(home))
    return root


@pytest.fixture
def catalog(core: Path, tmp_path: Path) -> tuple[CliHarness, Path]:
    root = tmp_path / "catalog"
    shutil.copytree(core, root, symlinks=True)
    return CliHarness(tmp_path), root


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _versions(collection: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads((collection / "versions.json").read_text())
    return loaded


def _recorded(collection: Path, key: str) -> tuple[str, int]:
    asset = _versions(collection)["versions"][-1]["assets"][key]
    return asset["sha256"], asset["size_bytes"]


def _shuffle(path: Path) -> None:
    """Append random points, so that rashid judges the rows unordered (PTL-DAT-006)."""
    original = breakers._original_coords(path)
    xs, ys = [x for x, _ in original], [y for _, y in original]
    rng = random.Random(7)
    extra = [(rng.uniform(min(xs), max(xs)), rng.uniform(min(ys), max(ys))) for _ in range(800)]
    breakers._rewrite_points(path, original + extra)


def _drop_overviews(path: Path) -> None:
    """Rewrite a scene as a tiled GeoTIFF with no overviews (PTL-DAT-011)."""
    breakers._rewrite_raster(
        path, width=600, height=600, tiled=True, blockxsize=256, blockysize=256
    )


def _findings(run: CliHarness, root: Path) -> set[tuple[str, str]]:
    payload = json.loads(run("check", str(root), "--json", expect=None).stdout)
    return {
        (f["rule_id"], f["path"]) for f in payload["data"]["findings"] if f["severity"] != "info"
    }


@pytest.mark.parametrize("workers", ["1", "2"])
def test_fix_records_the_rewritten_bytes_so_push_uploads_them(
    catalog: tuple[CliHarness, Path], workers: str
) -> None:
    run, root = catalog
    roads, scene = root / "roads", root / "imagery" / "scene-a" / "rgb.tif"
    pushed = _versions(roads)
    _shuffle(roads / "roads.parquet")
    _drop_overviews(scene)
    broken = (_sha(roads / "roads.parquet"), _sha(scene))

    run("check", str(root), "--fix", "--workers", workers, expect=None)

    assert (_sha(roads / "roads.parquet"), _sha(scene)) != broken
    for collection, path, key in (
        (roads, roads / "roads.parquet", "roads.parquet"),
        (root / "imagery", scene, "scene-a/rgb.tif"),
    ):
        assert _recorded(collection, key) == (_sha(path), path.stat().st_size)
    uploads = _get_assets_to_upload(root, _versions(roads), remote_versions_data=pushed)
    assert roads / "roads.parquet" in uploads
    remaining = _findings(run, root)
    assert ("PTL-DAT-006", "roads/collection.json") not in remaining
    assert ("PTL-DAT-011", "imagery/scene-a/scene-a.json") not in remaining


def test_force_records_the_reencoded_cog(catalog: tuple[CliHarness, Path]) -> None:
    run, root = catalog
    scene = root / "imagery" / "scene-b" / "singleband.tif"
    before = _sha(scene)

    run("check", str(root), "--fix", "--force", expect=None)

    assert _sha(scene) != before
    assert _recorded(root / "imagery", "scene-b/singleband.tif") == (
        _sha(scene),
        scene.stat().st_size,
    )


def test_fix_keeps_a_file_that_the_settings_cannot_order(
    catalog: tuple[CliHarness, Path],
) -> None:
    run, root = catalog
    config = root / ".portolan" / "config.yaml"
    config.write_text(config.read_text() + "\nconversion:\n  vector:\n    sort: none\n")
    roads = root / "roads" / "roads.parquet"
    _shuffle(roads)
    broken = _sha(roads)

    result = run("check", str(root), "--fix", expect=None)

    assert _sha(roads) == broken
    assert "Rewriting roads.parquet" not in result.output
    assert (
        "Kept roads.parquet: PTL-DAT-006 needs conversion.vector.sort set to hilbert or "
        "quadkey" in result.output
    )
    assert ("PTL-DAT-006", "roads/collection.json") in _findings(run, root)


def test_a_failed_rewrite_is_reported_as_failed(
    catalog: tuple[CliHarness, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    from portolan_cli.convert import ConversionStatus
    from portolan_cli.validation.workflow import run_check_workflow

    def refuse(*_args: object) -> None:
        raise RuntimeError("no space left")

    _, root = catalog
    roads = root / "roads" / "roads.parquet"
    _shuffle(roads)
    broken = _sha(roads)
    recorded = _recorded(root / "roads", "roads.parquet")
    monkeypatch.setattr("portolan_cli.preparation._rewrite_parquet_in_place", refuse)

    result = run_check_workflow(root, fix=True)

    conversion = result.payload["fix"]["conversion"]["conversion"]["results"]
    assert [(r["source"], r["status"]) for r in conversion] == [
        (str(roads), ConversionStatus.FAILED.value)
    ]
    assert result.failed
    assert _sha(roads) == broken
    assert _recorded(root / "roads", "roads.parquet") == recorded


def test_fix_refreshes_a_named_mirror_with_the_mirror_writer(
    catalog: tuple[CliHarness, Path],
) -> None:
    run, root = catalog
    mirror = root / "imagery" / "items.parquet"
    pq.write_table(pq.read_table(mirror), mirror, write_statistics=False)
    assert ("PTL-DAT-007", "imagery/collection.json") in _findings(run, root)

    result = run("check", str(root), "--fix", expect=None)

    assert "Rewriting items.parquet" not in result.output
    assert ("PTL-DAT-007", "imagery/collection.json") not in _findings(run, root)
    assert _recorded(root / "imagery", "items.parquet") == (_sha(mirror), mirror.stat().st_size)


def test_fix_does_not_rewrite_through_a_symlink(
    catalog: tuple[CliHarness, Path], tmp_path: Path
) -> None:
    run, root = catalog
    roads = root / "roads" / "roads.parquet"
    target = tmp_path / "elsewhere.parquet"
    shutil.move(roads, target)
    roads.symlink_to(target)
    _shuffle(target)
    broken = _sha(target)

    result = run("check", str(root), "--fix", expect=None)

    assert roads.is_symlink()
    assert _sha(target) == broken
    assert "roads.parquet: a symlink, not rewritten in place" in result.output
