"""Unit tests for ``_remove_from_versions`` asset-key matching.

Regression coverage for issue #589: item-level assets are keyed
``{item_id}/{filename}`` in versions.json (where ``item_id`` is the item's
*directory* name, e.g. a Hive partition dir), but ``_remove_from_versions``
used to reconstruct the key from the file *stem* — so ``portolan rm`` of any
item whose directory name != file stem (every real Hive partition and nested
item layout) left a phantom entry behind.

The matcher now compares each tracked asset's ``href`` (catalog-root-relative,
which already encodes the true item dir) against the removed file's actual
location, so it is correct regardless of how ``add`` derived the item id.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from portolan_cli.remove import _remove_from_versions, _remove_one_file
from portolan_cli.versions import (
    SPEC_VERSION,
    Asset,
    Version,
    VersionsFile,
    read_versions,
    write_versions,
)


def _catalog_with_assets(tmp_path: Path, asset_keys: list[str]) -> tuple[Path, Path]:
    """Build a managed catalog whose one collection tracks the given assets.

    Each asset is keyed ``asset_key`` with an href of ``mycoll/{asset_key}``
    (the shape ``add._batch_update_versions`` writes: catalog-root-relative,
    including the collection dir). Returns ``(collection_dir, versions_path)``.
    """
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("{}\n", encoding="utf-8")

    coll_dir = tmp_path / "mycoll"
    coll_dir.mkdir()

    assets = {key: Asset(sha256="abc", size_bytes=1, href=f"mycoll/{key}") for key in asset_keys}
    version = Version(
        version="1.0.0",
        created=datetime.now(timezone.utc),
        breaking=False,
        assets=assets,
        changes=list(assets),
    )
    versions_file = VersionsFile(
        spec_version=SPEC_VERSION,
        current_version="1.0.0",
        versions=[version],
    )
    versions_path = coll_dir / "versions.json"
    write_versions(versions_path, versions_file)
    return coll_dir, versions_path


def _touch(coll_dir: Path, rel: str) -> Path:
    """Create an on-disk file at ``coll_dir/rel`` (making parents) and return it."""
    file_path = coll_dir / rel
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"fake")
    return file_path


@pytest.mark.unit
def test_remove_item_scoped_key_dropped(tmp_path: Path) -> None:
    """rm of an item-level asset removes its ``{item_dir}/{filename}`` key.

    The item dir (``boundaries_2020``) differs from the file stem
    (``districts``) — the realistic layout. Pre-fix the matcher reconstructed
    ``districts/districts.parquet`` from the stem, missed the tracked
    ``boundaries_2020/districts.parquet`` key, and left it as a phantom entry.
    """
    key = "boundaries_2020/districts.parquet"
    coll_dir, versions_path = _catalog_with_assets(tmp_path, [key])
    file_path = _touch(coll_dir, key)  # mycoll/boundaries_2020/districts.parquet

    _remove_from_versions(file_path, versions_path)

    latest = read_versions(versions_path).versions[-1]
    assert key not in latest.assets


@pytest.mark.unit
def test_remove_hive_partition_key_dropped(tmp_path: Path) -> None:
    """rm of a Hive-partitioned item removes its ``{cell=...}/{filename}`` key.

    Acceptance criterion for #589. ``add`` derives the item id from the Hive
    partition directory name (``kdtree_cell=0000000000``), so the stem-based
    matcher never matched it. Pre-fix this assertion fails (phantom survives).
    """
    key = "kdtree_cell=0000000000/data.parquet"
    coll_dir, versions_path = _catalog_with_assets(tmp_path, [key])
    file_path = _touch(coll_dir, key)  # mycoll/kdtree_cell=0000000000/data.parquet

    _remove_from_versions(file_path, versions_path)

    latest = read_versions(versions_path).versions[-1]
    assert key not in latest.assets


@pytest.mark.unit
def test_remove_does_not_over_match_sibling_item(tmp_path: Path) -> None:
    """Removing one item's asset must not touch a sibling with the same filename.

    Guards against a naive ``endswith('/data.parquet')`` matcher: ``district_a``
    and ``district_b`` both track a ``data.parquet``; removing the file under
    ``district_a`` must drop only that key.
    """
    coll_dir, versions_path = _catalog_with_assets(
        tmp_path, ["district_a/data.parquet", "district_b/data.parquet"]
    )
    file_path = _touch(coll_dir, "district_a/data.parquet")

    _remove_from_versions(file_path, versions_path)

    latest = read_versions(versions_path).versions[-1]
    assert "district_a/data.parquet" not in latest.assets
    assert "district_b/data.parquet" in latest.assets


@pytest.mark.unit
def test_remove_collection_level_key_dropped(tmp_path: Path) -> None:
    """rm of a single-file (collection-level) vector asset drops its bare key."""
    coll_dir, versions_path = _catalog_with_assets(tmp_path, ["tunnels.parquet"])
    file_path = _touch(coll_dir, "tunnels.parquet")  # mycoll/tunnels.parquet

    _remove_from_versions(file_path, versions_path)

    latest = read_versions(versions_path).versions[-1]
    assert "tunnels.parquet" not in latest.assets


def _nested_catalog(tmp_path: Path, asset_key: str) -> tuple[Path, Path]:
    """Build a catalog whose collection sits under a subcatalog (``sub/coll``).

    Mirrors what ``add`` writes for a nested collection id (ADR-0032): the
    collection dir carries ``collection.json`` and ``versions.json``, and the
    tracked href includes both path segments. Returns ``(coll_dir, versions_path)``.
    """
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("{}\n", encoding="utf-8")
    (tmp_path / "catalog.json").write_text("{}\n", encoding="utf-8")

    coll_dir = tmp_path / "sub" / "coll"
    coll_dir.mkdir(parents=True)
    (coll_dir / "collection.json").write_text("{}\n", encoding="utf-8")

    assets = {asset_key: Asset(sha256="abc", size_bytes=1, href=f"sub/coll/{asset_key}")}
    version = Version(
        version="1.0.0",
        created=datetime.now(timezone.utc),
        breaking=False,
        assets=assets,
        changes=[asset_key],
    )
    versions_path = coll_dir / "versions.json"
    write_versions(
        versions_path,
        VersionsFile(spec_version=SPEC_VERSION, current_version="1.0.0", versions=[version]),
    )
    return coll_dir, versions_path


@pytest.mark.unit
def test_nested_collection_resolves_the_real_catalog_root(tmp_path: Path) -> None:
    """Publishing a removal for ``sub/coll`` writes that collection's versions.json.

    Acceptance criterion for #723. ``JsonFileBackend._versions_path`` used to
    flatten the nested id through ``Path(collection).name``, so the new version
    landed in ``{catalog_root}/coll/versions.json`` and the real file kept the
    asset as a phantom entry.
    """
    coll_dir, versions_path = _nested_catalog(tmp_path, "tunnels.parquet")
    file_path = _touch(coll_dir, "tunnels.parquet")

    _remove_from_versions(file_path, versions_path)

    latest = read_versions(versions_path).versions[-1]
    assert "tunnels.parquet" not in latest.assets
    assert not (tmp_path / "coll").exists(), "removal wrote to a flattened collection dir"


@pytest.mark.unit
def test_remove_one_file_untracks_asset_in_a_nested_collection(tmp_path: Path) -> None:
    """``rm`` finds the versions.json of a collection under a subcatalog (#723).

    ``_remove_one_file`` built the path from ``resolve_collection_id``, which
    returns only the first component (``sub``). It looked for
    ``{catalog_root}/sub/versions.json``, found nothing, and skipped untracking
    while still deleting the file — the phantom entry in the issue transcript.
    """
    coll_dir, versions_path = _nested_catalog(tmp_path, "polygons.parquet")
    file_path = _touch(coll_dir, "polygons.parquet")

    removed = _remove_one_file(file_path, catalog_root=tmp_path, keep=False, dry_run=False)

    assert removed is True
    assert not file_path.exists()
    versions_file = read_versions(versions_path)
    assert len(versions_file.versions) == 2, "removal published no new version"
    assert "polygons.parquet" not in versions_file.versions[-1].assets


@pytest.mark.unit
def test_remove_one_file_untracks_item_asset_in_a_nested_collection(tmp_path: Path) -> None:
    """The collection dir is found from an item subdir, not just the file's parent."""
    coll_dir, versions_path = _nested_catalog(tmp_path, "scene-001/band.parquet")
    file_path = _touch(coll_dir, "scene-001/band.parquet")

    removed = _remove_one_file(file_path, catalog_root=tmp_path, keep=False, dry_run=False)

    assert removed is True
    latest = read_versions(versions_path).versions[-1]
    assert "scene-001/band.parquet" not in latest.assets


@pytest.mark.unit
def test_remove_source_drops_converted_parquet_asset(tmp_path: Path) -> None:
    """Removing a non-cloud-native source drops its converted ``.parquet`` asset.

    ``add`` converts e.g. ``roads.shp`` to ``roads.parquet`` and tracks the
    parquet. ``rm roads.shp`` must still untrack ``roads.parquet`` (suffix-swap
    fallback), or the parquet becomes a phantom entry.
    """
    coll_dir, versions_path = _catalog_with_assets(tmp_path, ["roads.parquet"])
    file_path = _touch(coll_dir, "roads.shp")  # source still on disk, tracked as parquet

    _remove_from_versions(file_path, versions_path)

    latest = read_versions(versions_path).versions[-1]
    assert "roads.parquet" not in latest.assets
