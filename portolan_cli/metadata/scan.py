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

from portolan_cli.metadata.detection import check_file_metadata
from portolan_cli.metadata.models import (
    MetadataCheckResult,
    MetadataReport,
    MetadataStatus,
)

_DATA_EXTENSIONS = frozenset({".parquet", ".tif", ".tiff", ".pmtiles"})

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
        Aggregate `MetadataReport`. Empty if no `catalog.json` is found.
    """
    report = MetadataReport()
    if not (catalog_path / "catalog.json").exists():
        return report
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
        _scan_collection(child, report)
        for path in child.rglob("*"):
            registered.add(path.resolve())
        return

    item_id = child.name
    item_json_path = child / f"{item_id}.json"
    if item_json_path.exists():
        _scan_item(item_json_path, child, collection_dir, registered, report)
        return

    data_files = [p for p in child.iterdir() if p.is_file() and _is_data_file(p)]
    for data_path in data_files:
        registered.add(data_path.resolve())
        report.results.append(
            MetadataCheckResult(
                file_path=data_path,
                status=MetadataStatus.MISSING,
                message=f"Item directory has data but no {item_id}.json",
                fix_hint=(f"Run 'portolan check --metadata --fix' to create {item_id}.json"),
            )
        )


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
