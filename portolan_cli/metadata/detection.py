"""Metadata detection functions.

Provides detection and staleness checking for geo-asset metadata:
- get_stored_metadata(): Read existing STAC item + versions.json data
- get_current_metadata(): Extract fresh metadata from file
- is_stale(): MTIME check + heuristic fallback
- detect_changes(): Return list of what changed
- check_file_metadata(): Return MetadataCheckResult for single file
- compute_schema_fingerprint(): Generate hash of file schema
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, Any

from portolan_cli.metadata.cog import extract_cog_metadata
from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata
from portolan_cli.run_state import stat_matches
from portolan_cli.versions import (
    VersionsFile,
    read_versions,
    versions_asset_key,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from portolan_cli.metadata.cog import COGMetadata
    from portolan_cli.run_state import RunState
from portolan_cli.metadata.models import (
    FileMetadataState,
    MetadataCheckResult,
    MetadataStatus,
)


@dataclass
class StoredMetadata:
    """Metadata stored in STAC item and versions.json.

    Bridges STAC item data with versions.json tracking information.

    Attributes:
        item_id: STAC item ID.
        bbox: Bounding box [west, south, east, north].
        source_mtime: Source file modification time from versions.json.
        sha256: File checksum from versions.json.
        feature_count: Number of features/pixels.
        schema_fingerprint: Hash of the schema.
        size_bytes: File size from versions.json, for the stat check.
    """

    item_id: str
    bbox: list[float] | None
    source_mtime: float | None
    sha256: str | None
    feature_count: int | None
    schema_fingerprint: str | None
    size_bytes: int | None = None


def current_assets(versions_file: VersionsFile) -> dict[str, dict[str, Any]]:
    """The assets of the current version, as the dicts versions.json holds.

    The version that ``current_version`` names wins, then the last version.
    """
    if not versions_file.versions:
        return {}
    current = next(
        (v for v in versions_file.versions if v.version == versions_file.current_version),
        versions_file.versions[-1],
    )
    return {
        key: {name: value for name, value in asdict(asset).items() if value is not None}
        for key, asset in current.assets.items()
    }


def load_current_assets(
    collection_dir: Path, state: RunState | None = None
) -> dict[str, dict[str, Any]] | None:
    """The current assets of a collection's versions.json, or None without one.

    With ``state``, the run reads each versions.json once. A file that does not
    parse counts as absent, as before.
    """
    try:
        if state is not None:
            if not (collection_dir / "versions.json").is_file():
                return None
            return current_assets(state.versions(collection_dir, include_staged=False))
        versions_path = collection_dir / "versions.json"
        if not versions_path.is_file():
            return None
        return current_assets(read_versions(versions_path))
    except (ValueError, OSError, KeyError):
        return None


def versions_asset_lookup_keys(file_path: Path, collection_dir: Path) -> tuple[str, ...]:
    """The versions.json keys that can track ``file_path``, most authoritative first.

    The collection-relative key is what ``add`` writes. The bare file name covers
    a hand-written or older versions.json, and a nested item whose versions.json
    keys by basename (a sub-catalog inside a collection).

    Drop the bare file name only when a different file already sits at
    ``collection_dir / file_path.name``. That file owns the bare collection-level
    key, so an item-level asset with the same name must not fall back onto it and
    read the wrong baseline (issue #709).
    """
    relative = versions_asset_key(file_path, collection_dir)
    collision = collection_dir / file_path.name
    if collision.exists() and collision.resolve() != file_path.resolve():
        return (relative,)
    return (relative, file_path.name)


def find_versions_asset(
    assets: dict[str, Any],
    file_path: Path,
    collection_dir: Path,
) -> dict[str, Any] | None:
    """The versions.json entry that tracks ``file_path``, or None.

    Three lookups, in order of authority. The collection-relative key is what
    ``add`` writes. The bare file name covers a hand-written or older
    versions.json. The ``href`` sweep covers a nested layout, where the key an
    item carries is not the whole path down from the collection.

    Reading only the bare file name was the #709 defect. An item-level asset is
    keyed ``{item_id}/{filename}``, so the lookup never matched, every
    item-level asset read as STALE, and ``check --fix`` rewrote every item on
    every run.

    The bare file name resolves only when no other file owns it. A collection-level
    file with the same name owns the bare key, so an item-level asset never falls
    back onto it and reads the wrong baseline.
    """
    relative = versions_asset_key(file_path, collection_dir)
    for key in versions_asset_lookup_keys(file_path, collection_dir):
        entry = assets.get(key)
        if isinstance(entry, dict):
            return entry
    for entry in assets.values():
        if not isinstance(entry, dict):
            continue
        href = entry.get("href")
        if isinstance(href, str) and PurePath(href).as_posix().endswith(relative):
            return entry
    return None


def stored_baseline_mtime(entry: dict[str, Any]) -> float | None:
    """The freshness baseline on a versions.json entry.

    ``add`` records the asset's own mtime under ``mtime``, while
    ``update_versions_tracking`` records it under ``source_mtime``. One entry may
    carry either or both. ``_check_collection_level_asset`` (scan.py) already
    reads them in this order for #512; the item path needs the same rule.
    """
    source_mtime = entry.get("source_mtime")
    baseline = source_mtime if source_mtime is not None else entry.get("mtime")
    return baseline if isinstance(baseline, (int, float)) else None


def _item_registering(file_path: Path) -> dict[str, Any] | None:
    """The STAC item beside ``file_path`` that registers it, or None.

    ``add`` names the item ``<stem>.json``, but ``add --item-id`` names it
    after the id. Then the item is the one whose asset href resolves to the
    file. Asking only for ``<stem>.json`` reported such a file as MISSING, and
    ``check --fix`` wrote a second item for it (issue #840).
    """
    named = file_path.parent / f"{file_path.stem}.json"
    candidates = [named] if named.exists() else sorted(file_path.parent.glob("*.json"))
    target = file_path.resolve()
    for path in candidates:
        try:
            item = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError, UnicodeDecodeError):
            continue
        if not isinstance(item, dict) or item.get("type") != "Feature":
            continue
        if path == named:
            return item
        assets = item.get("assets")
        hrefs = (
            [a.get("href") for a in assets.values() if isinstance(a, dict)]
            if isinstance(assets, dict)
            else []
        )
        if any(isinstance(h, str) and (path.parent / h).resolve() == target for h in hrefs):
            return item
    return None


def get_stored_metadata(
    file_path: Path,
    collection_dir: Path,
    *,
    state: RunState | None = None,
) -> StoredMetadata | None:
    """Read existing STAC item and versions.json metadata.

    Looks for the STAC item beside the asset that registers it,
    and extracts tracking data from versions.json if present.

    Args:
        file_path: Path to the geo-asset file.
        collection_dir: Directory containing STAC item and versions.json.
        state: The run state, for one versions.json read per collection.

    Returns:
        StoredMetadata if found, None if no metadata exists.
    """
    # Item.json sits next to the data file in the hierarchical layout
    # produced by `add` ({item_dir}/{item_id}.json). Per the
    # scanner is the single source of truth for layout discovery; the
    # legacy flat sibling-JSON layout is intentionally not supported here
    # — the scanner reports such files as ORPHANED and directs the user
    # to migrate via `portolan add`.
    item_data = _item_registering(file_path)
    if item_data is None:
        return None

    # Extract bbox from item (None if not explicitly provided)
    bbox = item_data.get("bbox")

    # Default values for optional fields
    source_mtime: float | None = None
    sha256: str | None = None
    feature_count: int | None = None
    schema_fingerprint: str | None = None
    size_bytes: int | None = None

    assets = load_current_assets(collection_dir, state)
    asset_data = find_versions_asset(assets, file_path, collection_dir) if assets else None
    if asset_data is not None:
        source_mtime = stored_baseline_mtime(asset_data)
        sha256 = asset_data.get("sha256")
        feature_count = asset_data.get("feature_count")
        schema_fingerprint = asset_data.get("schema_fingerprint")
        size_bytes = asset_data.get("size_bytes")

    return StoredMetadata(
        item_id=item_data.get("id", file_path.stem),
        bbox=bbox,  # None if not explicitly provided (avoid spurious heuristics_changed)
        source_mtime=source_mtime,
        sha256=sha256,
        feature_count=feature_count,
        schema_fingerprint=schema_fingerprint,
        size_bytes=size_bytes,
    )


def get_current_metadata(file_path: Path, *, state: RunState | None = None) -> FileMetadataState:
    """Extract fresh metadata from a geo-asset file.

    Supports GeoParquet (.parquet) and COG (.tif, .tiff) formats. One read of
    the file gives every field. With ``state``, the run reads each file once.

    Args:
        file_path: Path to the geo-asset file.
        state: The run state, for one read per file and run.

    Returns:
        FileMetadataState with current file metadata.

    Raises:
        FileNotFoundError: If file doesn't exist.
        ValueError: If file format is not supported.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    suffix = file_path.suffix.lower()
    current_mtime = file_path.stat().st_mtime

    if suffix == ".parquet":
        gp_metadata = extract_geoparquet_metadata(file_path, state=state)
        return FileMetadataState(
            file_path=file_path,
            current_mtime=current_mtime,
            stored_mtime=None,
            current_bbox=_comparable_bbox(gp_metadata.bbox, gp_metadata.crs, unset_is_wgs84=True),
            stored_bbox=None,
            current_feature_count=gp_metadata.feature_count,
            stored_feature_count=None,
            current_schema_fingerprint=gp_metadata.schema_fingerprint,
            stored_schema_fingerprint=None,
        )

    if suffix in (".tif", ".tiff"):
        cog_metadata = (
            state.memo("cog.metadata", file_path, extract_cog_metadata)
            if state is not None
            else extract_cog_metadata(file_path)
        )
        pixel_count = (
            cog_metadata.width * cog_metadata.height
            if cog_metadata.width and cog_metadata.height
            else None
        )
        return FileMetadataState(
            file_path=file_path,
            current_mtime=current_mtime,
            stored_mtime=None,
            current_bbox=_comparable_bbox(
                cog_metadata.bbox, cog_metadata.crs, unset_is_wgs84=False
            ),
            stored_bbox=None,
            current_feature_count=pixel_count,
            stored_feature_count=None,
            # Pass pre-extracted metadata to avoid re-reading the COG
            current_schema_fingerprint=compute_schema_fingerprint(
                file_path, cog_metadata=cog_metadata
            ),
            stored_schema_fingerprint=None,
        )

    raise ValueError(f"Unsupported format: {suffix}")


def _comparable_bbox(
    bbox: Sequence[float] | None,
    crs: object,
    *,
    unset_is_wgs84: bool,
) -> list[float] | None:
    """The file's bbox when it is in WGS84, else None.

    The item stores its bbox in WGS84. A file bbox in another CRS, such as the
    metres of a UTM raster, never equals it, so every touched file read as
    changed (issue #839). Freshness compares only bboxes in one CRS. For any
    other file the pixel or feature count and the schema fingerprint carry the
    signal.

    Args:
        bbox: The bbox the file declares, in its own CRS.
        crs: The file's CRS: an EPSG or WKT string, a PROJJSON dict, or None.
        unset_is_wgs84: Read a missing CRS as WGS84. GeoParquet defines an
            absent ``crs`` as OGC:CRS84. A GeoTIFF without a CRS has no frame.
    """
    if not bbox:
        return None
    if crs is None:
        return list(bbox) if unset_is_wgs84 else None
    from pyproj import CRS
    from pyproj.exceptions import CRSError

    from portolan_cli.crs import _is_wgs84

    try:
        parsed = CRS.from_user_input(crs)
    except (CRSError, TypeError):
        return None
    return list(bbox) if _is_wgs84(parsed) else None


def compute_schema_fingerprint(
    file_path: Path,
    cog_metadata: COGMetadata | None = None,
) -> str:
    """Generate a hash fingerprint of the file schema.

    For GeoParquet: hash of column names and types.
    For COG: hash of band count, dtype, and CRS.

    Args:
        file_path: Path to the geo-asset file.
        cog_metadata: Optional pre-extracted COG metadata to avoid re-extraction.

    Returns:
        Hexadecimal hash string representing the schema.
    """
    suffix = file_path.suffix.lower()

    if suffix == ".parquet":
        fingerprint = extract_geoparquet_metadata(file_path).schema_fingerprint
        assert fingerprint is not None  # noqa: S101 - the extractor always sets it
        return fingerprint

    if suffix in (".tif", ".tiff"):
        # Use provided metadata if available, otherwise extract
        if cog_metadata is None:
            cog_metadata = extract_cog_metadata(file_path)
        schema_parts = [
            str(cog_metadata.band_count),
            cog_metadata.dtype or "",
            cog_metadata.crs or "",
        ]
        schema_str = "|".join(schema_parts)
        return hashlib.sha256(schema_str.encode()).hexdigest()[:16]

    # For unknown formats, hash the first 1KB
    with Path(file_path).open("rb") as f:
        content = f.read(1024)
    return hashlib.sha256(content).hexdigest()[:16]


def is_stale(state: FileMetadataState) -> tuple[bool, str]:
    """Check if file metadata is stale using MTIME + heuristics.

    Detection strategy:
    1. If stored_mtime is None → new file
    2. If mtime unchanged → not stale (fast path)
    3. If mtime changed but heuristics unchanged → touched but not modified
    4. If mtime changed AND heuristics changed → stale

    Args:
        state: FileMetadataState comparing current vs stored values.

    Returns:
        Tuple of (is_stale: bool, reason: str).
        Reasons: "new_file", "mtime_unchanged", "touched_unchanged",
                 "content_changed", "schema_changed"
    """
    # Check for new file (no stored metadata)
    if state.stored_mtime is None:
        return (True, "new_file")

    # Fast path: mtime unchanged means file hasn't been touched. The one mtime
    # predicate of the code base decides (run_state.file_mtime_matches).
    if not state.mtime_changed:
        return (False, "mtime_unchanged")

    # MTIME changed - check if schema changed (breaking change)
    if state.schema_changed:
        return (True, "schema_changed")

    # Check if heuristics changed (bbox or feature count)
    if state.heuristics_changed:
        return (True, "content_changed")

    # MTIME changed but content appears unchanged (file touched but not modified)
    return (False, "touched_unchanged")


def judge_staleness(state: FileMetadataState, *, resized: bool) -> tuple[bool, str]:
    """Run :func:`is_stale`, except that a new size is a change whatever the mtime.

    ``run_state.stat_matches`` needs the size and the mtime to match.
    ``FileMetadataState`` holds no size, so the caller passes the comparison.

    Args:
        state: The current and stored values.
        resized: True when the file size differs from the size versions.json holds.

    Returns:
        ``(is_stale, reason)`` with the reasons of :func:`is_stale`.
    """
    if not resized:
        return is_stale(state)
    if state.stored_mtime is None:
        return (True, "new_file")
    if state.schema_changed:
        return (True, "schema_changed")
    return (True, "content_changed")


def detect_changes(state: FileMetadataState) -> list[str]:
    """Detect what changed between stored and current metadata.

    Compares each metadata field and returns a list of changed fields.

    Args:
        state: FileMetadataState comparing current vs stored values.

    Returns:
        List of changed field names: ["mtime", "bbox", "feature_count", "schema"]
    """
    changes: list[str] = []

    # Check mtime
    if state.mtime_changed:
        changes.append("mtime")

    # Check bbox with tolerance-aware comparison. A file bbox in another CRS
    # than the item's WGS84 one is None, and gives no bbox signal (#839).
    from portolan_cli.metadata.models import BBOX_TOLERANCE, _bboxes_equal

    if state.current_bbox is not None and (
        state.stored_bbox is None
        or not _bboxes_equal(state.stored_bbox, state.current_bbox, BBOX_TOLERANCE)
    ):
        changes.append("bbox")

    # Check feature count
    if (
        state.stored_feature_count is None
        and state.current_feature_count is not None
        or state.current_feature_count != state.stored_feature_count
    ):
        changes.append("feature_count")

    # Check schema
    if state.schema_changed:
        changes.append("schema")

    return changes


def check_file_metadata(
    file_path: Path,
    collection_dir: Path,
    *,
    state: RunState | None = None,
) -> MetadataCheckResult:
    """Check metadata status for a single geo-asset file.

    Combines get_stored_metadata and get_current_metadata to determine
    if the file's STAC metadata is up to date. A stat that matches the stored
    size and mtime proves the file unchanged, so the file is not opened
    (issue #995).

    Args:
        file_path: Path to the geo-asset file.
        collection_dir: Directory containing STAC metadata.
        state: The run state, for one versions.json read per collection.

    Returns:
        MetadataCheckResult with status and details.

    Raises:
        FileNotFoundError: If file doesn't exist.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Get stored metadata
    stored = get_stored_metadata(file_path, collection_dir, state=state)

    if stored is None:
        return MetadataCheckResult(
            file_path=file_path,
            status=MetadataStatus.MISSING,
            message="Missing STAC metadata for file",
            fix_hint="Run 'portolan fix' to create STAC item",
        )

    st = file_path.stat()
    if stat_matches(st, stored.source_mtime, stored.size_bytes):
        return MetadataCheckResult(
            file_path=file_path,
            status=MetadataStatus.FRESH,
            message="Metadata is up to date (mtime_unchanged)",
        )
    resized = stored.size_bytes is not None and st.st_size != stored.size_bytes

    current = get_current_metadata(file_path, state=state)

    # Update current state with stored values
    file_state = FileMetadataState(
        file_path=file_path,
        current_mtime=current.current_mtime,
        stored_mtime=stored.source_mtime,
        current_bbox=current.current_bbox,
        stored_bbox=stored.bbox,
        current_feature_count=current.current_feature_count,
        stored_feature_count=stored.feature_count,
        current_schema_fingerprint=current.current_schema_fingerprint,
        stored_schema_fingerprint=stored.schema_fingerprint,
    )

    # Check staleness
    stale, reason = judge_staleness(file_state, resized=resized)

    if not stale:
        return MetadataCheckResult(
            file_path=file_path,
            status=MetadataStatus.FRESH,
            message=f"Metadata is up to date ({reason})",
        )

    # `add` persists no schema fingerprint for an item-level asset, so a moved
    # mtime alone cannot prove a change. Settle it with the stored content hash,
    # the same tiebreaker `_check_collection_level_asset` uses for #512. A
    # byte-identical file (after a `git clone`, which resets mtimes) then reads
    # FRESH rather than driving a rewrite that alters nothing (#709).
    if (
        stored.schema_fingerprint is None
        and stored.sha256
        and _content_matches(file_path, stored.sha256, state=state)
    ):
        return MetadataCheckResult(
            file_path=file_path,
            status=MetadataStatus.FRESH,
            message="Metadata is up to date (content unchanged)",
        )

    # Determine what changed
    changes = detect_changes(file_state)

    # BREAKING needs a fingerprint to compare against. Without a baseline the
    # mtime change is unexplained, not proof of a schema break, so it reports
    # STALE rather than a blocking error.
    if reason == "schema_changed" and stored.schema_fingerprint is not None:
        return MetadataCheckResult(
            file_path=file_path,
            status=MetadataStatus.BREAKING,
            message="Schema has breaking changes",
            changes=changes,
            fix_hint="Run 'portolan fix --breaking' to handle schema changes",
        )

    return MetadataCheckResult(
        file_path=file_path,
        status=MetadataStatus.STALE,
        message=f"Metadata is stale: {', '.join(changes)}",
        changes=changes,
        fix_hint="Run 'portolan fix' to update STAC metadata",
    )


def _content_matches(file_path: Path, stored_sha256: str, *, state: RunState | None = None) -> bool:
    """True when the file's SHA-256 equals the checksum versions.json recorded."""
    from portolan_cli.sync.checksums import compute_checksum

    if not file_path.is_file():
        return False
    try:
        checksum = state.checksum(file_path) if state is not None else compute_checksum(file_path)
        return checksum == stored_sha256
    except (ValueError, OSError):
        return False
