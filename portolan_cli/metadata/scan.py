"""Manifest-driven metadata scanner."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

from portolan_cli.metadata.detection import (
    check_file_metadata,
    detect_changes,
    get_current_metadata,
    judge_staleness,
    load_current_assets,
    stored_baseline_mtime,
)
from portolan_cli.metadata.models import (
    FileMetadataState,
    MetadataCheckResult,
    MetadataReport,
    MetadataStatus,
)
from portolan_core.run_state import RunState, stat_matches

if TYPE_CHECKING:
    from collections.abc import Iterator

_DATA_EXTENSIONS = frozenset(
    {
        # Cloud-native + tile formats portolan tracks directly.
        ".parquet",
        ".tif",
        ".tiff",
        ".pmtiles",
        # Vector source formats: orphan-checked, not freshness-checked.
        ".gpkg",
        ".shp",
        ".geojson",
        ".fgb",
    }
)

_SYSTEM_FILES = frozenset(
    {"catalog.json", "collection.json", "versions.json", "config.yaml", "metadata.yaml"}
)

# Media types whose assets are not local files, such as the Iceberg table asset.
# Check skips them, so a missing Iceberg warehouse is the backend's to report.
_NON_LOCAL_MEDIA_TYPES = frozenset(
    {
        "application/x-iceberg",
    }
)


def scan_catalog_metadata(catalog_path: Path, *, state: RunState | None = None) -> MetadataReport:
    """Scan a catalog using STAC manifests as ground truth."""
    if not (catalog_path / "catalog.json").exists():
        raise FileNotFoundError(f"Not a portolan catalog: {catalog_path} has no catalog.json")
    report = MetadataReport()
    _scan_node(catalog_path, report, state or RunState.open(catalog_path))
    return report


def _scan_node(node_dir: Path, report: MetadataReport, state: RunState) -> None:
    if (node_dir / "collection.json").exists():
        _scan_collection(node_dir, report, state)
        return
    if (node_dir / "catalog.json").exists():
        # Each child of a catalog is a sub-catalog or a collection; recurse.
        for child in _iter_significant_subdirs(node_dir):
            _scan_node(child, report, state)


def _iter_significant_subdirs(directory: Path) -> Iterator[Path]:
    for sub in sorted(directory.iterdir()):
        if not sub.is_dir():
            continue
        if sub.name.startswith("."):
            continue
        yield sub


def _scan_collection(collection_dir: Path, report: MetadataReport, state: RunState) -> None:
    registered: set[Path] = set()

    collection = _safe_read_json(collection_dir / "collection.json") or {}
    for asset in collection.get("assets", {}).values():
        href = _href(asset)
        if not href:
            continue
        if _is_non_local_asset(asset):
            # The asset is not a local file (scheme-qualified href or backend-managed media type).
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
        result = _check_collection_level_asset(asset_path, collection_dir, state)
        if result is not None:
            report.results.append(result)

    for sub in _iter_significant_subdirs(collection_dir):
        _scan_collection_child(sub, collection_dir, registered, report, state)

    _emit_orphans(collection_dir, registered, report)


def _scan_collection_child(
    child: Path,
    collection_dir: Path,
    registered: set[Path],
    report: MetadataReport,
    state: RunState,
) -> None:
    if (child / "catalog.json").exists():
        # The collection stays the data-owning unit for versions.json and item assets.
        for grandchild in _iter_significant_subdirs(child):
            _scan_collection_child(grandchild, collection_dir, registered, report, state)
        _emit_orphans(child, registered, report)
        return

    if (child / "collection.json").exists():
        # A nested collection emits its own orphans.
        _scan_collection(child, report, state)
        return

    item_id = child.name
    item_json_path = child / f"{item_id}.json"
    if item_json_path.exists():
        _scan_item(item_json_path, child, collection_dir, registered, report, state)
        return

    # `add --item-id` names the item file after the id, so any STAC Item in the directory covers it.
    named_items = _item_jsons_in(child)
    if named_items:
        for path in named_items:
            _scan_item(path, child, collection_dir, registered, report, state)
        return

    # A subdir needs an item JSON only if it holds a data file named like the dir; else its files are ORPHANED.
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


def _item_jsons_in(directory: Path) -> list[Path]:
    """Every STAC Item JSON directly inside ``directory``, in a stable order."""
    found: list[Path] = []
    for path in sorted(directory.glob("*.json")):
        data = _safe_read_json(path)
        if not isinstance(data, dict) or data.get("type") != "Feature":
            continue
        if isinstance(data.get("stac_version"), str):
            found.append(path)
    return found


def _scan_item(
    item_json_path: Path,
    item_dir: Path,
    collection_dir: Path,
    registered: set[Path],
    report: MetadataReport,
    state: RunState,
) -> None:
    registered.add(item_json_path.resolve())
    item = _safe_read_json(item_json_path)
    if item is None:
        return

    for asset in item.get("assets", {}).values():
        href = _href(asset)
        if not href:
            continue
        if _is_non_local_asset(asset):
            # Scheme-qualified or extension/backend-managed (e.g.
            # application/x-iceberg) asset: not a local file on disk.
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
        # Resolve versions.json and item JSON through `collection_dir`, even under a sub-catalog.
        report.results.append(check_file_metadata(asset_path, collection_dir, state=state))

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
    state: RunState,
) -> MetadataCheckResult | None:
    """Freshness check for a registered collection-level asset."""
    assets = load_current_assets(collection_dir, state)
    stored = assets.get(asset_path.name) if assets else None
    if stored is None:
        return None

    # `add` records `mtime`, `--fix` records `source_mtime`; prefer the latter and fall back.
    stored_mtime = stored_baseline_mtime(stored)
    stored_size = stored.get("size_bytes")
    st = asset_path.stat()
    if stat_matches(st, stored_mtime, stored_size):
        return MetadataCheckResult(
            file_path=asset_path,
            status=MetadataStatus.FRESH,
            message="Metadata is up to date (mtime_unchanged)",
        )
    resized = stored_size is not None and st.st_size != stored_size

    current = get_current_metadata(asset_path, state=state)
    stored_schema_fingerprint = stored.get("schema_fingerprint")
    file_state = FileMetadataState(
        file_path=asset_path,
        current_mtime=current.current_mtime,
        stored_mtime=stored_mtime,
        # No item bbox source; feature count and schema fingerprint decide.
        current_bbox=None,
        stored_bbox=None,
        current_feature_count=current.current_feature_count,
        stored_feature_count=stored.get("feature_count"),
        current_schema_fingerprint=current.current_schema_fingerprint,
        stored_schema_fingerprint=stored_schema_fingerprint,
    )
    stale, reason = judge_staleness(file_state, resized=resized)
    if not stale:
        return MetadataCheckResult(
            file_path=asset_path,
            status=MetadataStatus.FRESH,
            message=f"Metadata is up to date ({reason})",
        )
    # Without a fingerprint baseline, a matching stored content hash reads FRESH.
    stored_sha256 = stored.get("sha256")
    if (
        stored_schema_fingerprint is None
        and isinstance(stored_sha256, str)
        and _content_hash_matches(asset_path, stored_sha256, state)
    ):
        return MetadataCheckResult(
            file_path=asset_path,
            status=MetadataStatus.FRESH,
            message="Metadata is up to date (content unchanged)",
        )
    changes = detect_changes(file_state)
    # Without a fingerprint baseline, report STALE, not BREAKING.
    if reason == "schema_changed" and stored_schema_fingerprint is not None:
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


def _content_hash_matches(asset_path: Path, stored_sha256: str, state: RunState) -> bool:
    """True if the asset file's SHA-256 equals the stored checksum."""
    if not asset_path.is_file():
        return False
    try:
        return state.checksum(asset_path) == stored_sha256
    except (ValueError, OSError):
        return False


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
    return href.removeprefix("./")


_URI_SCHEME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.\-]*://")


def _is_scheme_qualified(href: str) -> bool:
    """True if href is an absolute URI (file://, gs://, s3://, https://)."""
    return bool(_URI_SCHEME_RE.match(href))


def _is_non_local_asset(asset: Any) -> bool:
    """True if the asset should not be treated as a local file on disk."""
    if not isinstance(asset, dict):
        return False
    media_type = asset.get("type")
    if isinstance(media_type, str):
        # Media types are case-insensitive and may carry parameters (RFC 9110).
        normalized = media_type.split(";", 1)[0].strip().lower()
        if normalized in _NON_LOCAL_MEDIA_TYPES:
            return True
    href = _href(asset)
    return href is not None and _is_scheme_qualified(href)


def _safe_read_json(path: Path) -> dict[str, Any] | None:
    try:
        with Path(path).open(encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    return data if isinstance(data, dict) else None
