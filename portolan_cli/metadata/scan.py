"""Manifest-driven metadata scanner (ADR-0041).

This module is the single source of truth for "what assets does this catalog
have, and is each asset's STAC metadata fresh?"

It walks the STAC manifest tree (catalog.json -> collection.json -> item.json)
rather than the filesystem with extension filters. Anything registered in a
manifest is checked for freshness; anything on disk under a collection but
NOT registered is reported as ORPHANED.

Both `MetadataFreshRule.check()` and the `--fix` flow consume the
`MetadataReport` produced here, eliminating the historical asymmetry where
`check` could report MISSING for files `--fix` never saw (issue #384) and
where collection-level rollup assets like `items.parquet` were treated as
items-needing-JSON (issue #345).
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from portolan_cli.metadata.detection import (
    check_file_metadata,
    detect_changes,
    get_current_metadata,
    is_stale,
)
from portolan_cli.metadata.models import (
    FileMetadataState,
    MetadataCheckResult,
    MetadataReport,
    MetadataStatus,
)

_DATA_EXTENSIONS = frozenset(
    {
        # Cloud-native + tile formats portolan tracks directly.
        ".parquet",
        ".tif",
        ".tiff",
        ".pmtiles",
        # Vector source formats accepted per ADR-0014. Orphan-checked but
        # not freshness-checked (see _is_freshness_checkable) — there is
        # no extractor for these in detection.py, mirroring .pmtiles.
        ".gpkg",
        ".shp",
        ".geojson",
        ".fgb",
    }
)

_SYSTEM_FILES = frozenset(
    {"catalog.json", "collection.json", "versions.json", "config.yaml", "metadata.yaml"}
)


def scan_catalog_metadata(catalog_path: Path) -> MetadataReport:
    """Scan a catalog using STAC manifests as ground truth.

    Walks the catalog tree from `catalog.json`, descends into nested
    catalogs, and at each collection emits one `MetadataCheckResult` per
    registered or stray data asset.

    Args:
        catalog_path: Catalog root (must contain `catalog.json`).

    Returns:
        Aggregate `MetadataReport`.

    Raises:
        FileNotFoundError: If `catalog_path` has no `catalog.json`. The
            scanner refuses to silently return an empty report for a
            non-catalog path — callers (CLI, validation rule) pre-check
            so this exception only fires for direct misuse.
    """
    if not (catalog_path / "catalog.json").exists():
        raise FileNotFoundError(f"Not a portolan catalog: {catalog_path} has no catalog.json")
    report = MetadataReport()
    _scan_node(catalog_path, report)
    return report


def _scan_node(node_dir: Path, report: MetadataReport) -> None:
    if (node_dir / "collection.json").exists():
        _scan_collection(node_dir, report)
        return
    if (node_dir / "catalog.json").exists():
        # ADR-0032 Pattern 1: catalog above collections. Each significant
        # child is either a sub-catalog or a collection — recurse via
        # _scan_node. Pattern 2 (sub-catalog *inside* a collection) is
        # handled by _scan_collection_child where the collection's own
        # context is preserved.
        for child in _iter_significant_subdirs(node_dir):
            _scan_node(child, report)


def _iter_significant_subdirs(directory: Path) -> Iterator[Path]:
    for sub in sorted(directory.iterdir()):
        if not sub.is_dir():
            continue
        if sub.name.startswith("."):
            continue
        yield sub


def _scan_collection(collection_dir: Path, report: MetadataReport) -> None:
    registered: set[Path] = set()

    collection = _safe_read_json(collection_dir / "collection.json") or {}
    for asset in collection.get("assets", {}).values():
        href = _href(asset)
        if not href:
            continue
        asset_path = (collection_dir / href).resolve()
        registered.add(asset_path)
        if not asset_path.exists():
            report.results.append(
                MetadataCheckResult(
                    file_path=asset_path,
                    status=MetadataStatus.MISSING,
                    message="Asset registered in collection.json but file missing",
                    fix_hint="Restore the file or remove the asset entry",
                )
            )
            continue
        if not _is_freshness_checkable(asset_path):
            continue
        result = _check_collection_level_asset(asset_path, collection_dir)
        if result is not None:
            report.results.append(result)

    for sub in _iter_significant_subdirs(collection_dir):
        _scan_collection_child(sub, collection_dir, registered, report)

    _emit_orphans(collection_dir, registered, report)


def _scan_collection_child(
    child: Path,
    collection_dir: Path,
    registered: set[Path],
    report: MetadataReport,
) -> None:
    if (child / "catalog.json").exists():
        # Pattern 2 sub-catalog under a collection: organize items by year,
        # theme, etc. The DATA-OWNING unit is still `collection_dir`, so
        # versions.json + item assets are resolved against it.
        for grandchild in _iter_significant_subdirs(child):
            _scan_collection_child(grandchild, collection_dir, registered, report)
        _emit_orphans(child, registered, report)
        return

    if (child / "collection.json").exists():
        # Nested collection owns its own contents; recurse and let it emit
        # its own orphans. The outer `_emit_orphans` only walks top-level
        # files in collection_dir (no recursion), so nested files are out
        # of its sweep regardless — no need to mark them registered.
        _scan_collection(child, report)
        return

    item_id = child.name
    item_json_path = child / f"{item_id}.json"
    if item_json_path.exists():
        _scan_item(item_json_path, child, collection_dir, registered, report)
        return

    # Heuristic: a subdir is treated as an item-needing-JSON only if it
    # contains a data file whose stem matches the dir name (the convention
    # `add` writes). Otherwise the subdir is a stray container and its
    # data files are reported as ORPHANED rather than coerced into MISSING
    # — coercion would invite `--fix` to create a wrong item.json.
    data_files = [p for p in child.iterdir() if p.is_file() and _is_data_file(p)]
    matching = [p for p in data_files if p.stem == item_id]
    if not matching:
        _emit_orphans(child, registered, report)
        return

    for data_path in matching:
        registered.add(data_path.resolve())
        report.results.append(
            MetadataCheckResult(
                file_path=data_path,
                status=MetadataStatus.MISSING,
                message=f"Item directory has data but no {item_id}.json",
                fix_hint=(f"Run 'portolan check --metadata --fix' to create {item_id}.json"),
            )
        )
    # Any extra data files in an item-shaped dir that don't match the
    # expected stem are still orphans of the item.
    _emit_orphans(child, registered, report)


def _scan_item(
    item_json_path: Path,
    item_dir: Path,
    collection_dir: Path,
    registered: set[Path],
    report: MetadataReport,
) -> None:
    registered.add(item_json_path.resolve())
    item = _safe_read_json(item_json_path)
    if item is None:
        return

    for asset in item.get("assets", {}).values():
        href = _href(asset)
        if not href:
            continue
        asset_path = (item_dir / href).resolve()
        registered.add(asset_path)
        if not asset_path.exists():
            report.results.append(
                MetadataCheckResult(
                    file_path=asset_path,
                    status=MetadataStatus.MISSING,
                    message="Asset registered in item.json but file missing",
                    fix_hint="Restore the file or remove the asset entry",
                )
            )
            continue
        if not _is_freshness_checkable(asset_path):
            continue
        # versions.json + item.json lookup happen via `collection_dir` so
        # the data-owning unit (per ADR-0032) is the truth, even when the
        # item lives under a Pattern 2 sub-catalog.
        report.results.append(check_file_metadata(asset_path, collection_dir))

    _emit_orphans(item_dir, registered, report)


def _emit_orphans(
    directory: Path,
    registered: set[Path],
    report: MetadataReport,
) -> None:
    for entry in directory.iterdir():
        if not entry.is_file():
            continue
        if entry.name in _SYSTEM_FILES:
            continue
        if not _is_data_file(entry):
            continue
        if entry.resolve() in registered:
            continue
        report.results.append(
            MetadataCheckResult(
                file_path=entry,
                status=MetadataStatus.ORPHANED,
                message="File present but not registered in any STAC manifest",
                fix_hint=(
                    "Register it in collection.json/item.json (e.g., via "
                    "'portolan add'), or delete the file"
                ),
            )
        )


def _check_collection_level_asset(
    asset_path: Path,
    collection_dir: Path,
) -> MetadataCheckResult | None:
    """Freshness check for a registered collection-level asset.

    Reads stored values from `versions.json` directly; collection-level
    assets have no companion `item.json` by design (ADR-0031), so the
    item-centric `check_file_metadata` path does not apply.

    Returns None if the asset is registered but untracked in versions.json
    — registration alone is not a freshness claim, and emitting STALE for
    every rollup index (e.g., `items.parquet`) would be noise.
    """
    versions_path = collection_dir / "versions.json"
    stored = _read_versions_entry(versions_path, asset_path.name)
    if stored is None:
        return None

    current = get_current_metadata(asset_path)
    state = FileMetadataState(
        file_path=asset_path,
        current_mtime=current.current_mtime,
        stored_mtime=stored.get("source_mtime"),
        # Collection-level assets have no item.json bbox source. Heuristics
        # fall through to feature-count + schema fingerprint comparisons,
        # which is sufficient signal for STALE/BREAKING detection.
        current_bbox=None,
        stored_bbox=None,
        current_feature_count=current.current_feature_count,
        stored_feature_count=stored.get("feature_count"),
        current_schema_fingerprint=current.current_schema_fingerprint,
        stored_schema_fingerprint=stored.get("schema_fingerprint"),
    )
    stale, reason = is_stale(state)
    if not stale:
        return MetadataCheckResult(
            file_path=asset_path,
            status=MetadataStatus.FRESH,
            message=f"Metadata is up to date ({reason})",
        )
    changes = detect_changes(state)
    if reason == "schema_changed":
        return MetadataCheckResult(
            file_path=asset_path,
            status=MetadataStatus.BREAKING,
            message="Schema has breaking changes",
            changes=changes,
            fix_hint="Run 'portolan add' to regenerate the asset",
        )
    return MetadataCheckResult(
        file_path=asset_path,
        status=MetadataStatus.STALE,
        message=f"Metadata is stale: {', '.join(changes)}",
        changes=changes,
        fix_hint="Run 'portolan add' to refresh the collection-level asset",
    )


def _read_versions_entry(versions_path: Path, asset_filename: str) -> dict[str, Any] | None:
    """Return the current-version asset entry for `asset_filename`, or None."""
    if not versions_path.exists():
        return None
    data = _safe_read_json(versions_path)
    if data is None:
        return None
    versions = data.get("versions") or []
    if not isinstance(versions, list) or not versions:
        return None
    current_id = data.get("current_version")
    selected = None
    if current_id:
        for v in versions:
            if isinstance(v, dict) and v.get("version") == current_id:
                selected = v
                break
    if selected is None:
        selected = versions[-1] if isinstance(versions[-1], dict) else None
    if selected is None:
        return None
    assets = selected.get("assets")
    if not isinstance(assets, dict):
        return None
    entry = assets.get(asset_filename)
    return entry if isinstance(entry, dict) else None


def _is_data_file(path: Path) -> bool:
    return path.suffix.lower() in _DATA_EXTENSIONS


def _is_freshness_checkable(path: Path) -> bool:
    return path.suffix.lower() in {".parquet", ".tif", ".tiff"}


def _href(asset: Any) -> str | None:
    if not isinstance(asset, dict):
        return None
    href = asset.get("href")
    if not isinstance(href, str):
        return None
    return href[2:] if href.startswith("./") else href


def _safe_read_json(path: Path) -> dict[str, Any] | None:
    try:
        with open(path) as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    return data if isinstance(data, dict) else None
