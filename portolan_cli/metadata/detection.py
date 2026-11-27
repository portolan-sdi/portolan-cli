"""Metadata detection functions."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, Any

from portolan_cli.metadata.cog import extract_cog_metadata
from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata
from portolan_core.run_state import stat_matches
from portolan_core.trace import count
from portolan_core.versions import (
    VersionsFile,
    read_versions,
    versions_asset_key,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from portolan_cli.metadata.cog import COGMetadata
    from portolan_core.run_state import RunState
from portolan_cli.metadata.models import (
    FileMetadataState,
    MetadataCheckResult,
    MetadataStatus,
)


@dataclass
class StoredMetadata:
    """Metadata stored in STAC item and versions.json."""

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
    count("versions.asset_maps")
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
    """The current assets of a collection's versions.json, or None without one."""
    try:
        if state is not None:
            versions_path = collection_dir / "versions.json"
            if not versions_path.is_file():
                return None
            return state.memo(
                "versions.current_assets",
                versions_path,
                lambda _path: current_assets(state.versions(collection_dir, include_staged=False)),
            )
        versions_path = collection_dir / "versions.json"
        if not versions_path.is_file():
            return None
        return current_assets(read_versions(versions_path))
    except (ValueError, OSError, KeyError):
        return None


def versions_asset_lookup_keys(file_path: Path, collection_dir: Path) -> tuple[str, ...]:
    """The versions.json keys that can track ``file_path``, most authoritative first."""
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
    """The versions.json entry that tracks ``file_path``, or None."""
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
    """The freshness baseline on a versions.json entry."""
    source_mtime = entry.get("source_mtime")
    baseline = source_mtime if source_mtime is not None else entry.get("mtime")
    return baseline if isinstance(baseline, (int, float)) else None


def _item_registering(file_path: Path) -> dict[str, Any] | None:
    """The STAC item beside ``file_path`` that registers it, or None."""
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
    """Read existing STAC item and versions.json metadata."""
    # Item JSON sits at {item_dir}/{item_id}.json; the flat sibling layout is reported as ORPHANED.
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
    """Extract fresh metadata from a geo-asset file."""
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
    """The file's bbox when it is in WGS84, else None."""
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
    """Generate a hash fingerprint of the file schema."""
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
    """Check if file metadata is stale using MTIME + heuristics."""
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
    """Run :func:`is_stale`, except that a new size is a change whatever the mtime."""
    if not resized:
        return is_stale(state)
    if state.stored_mtime is None:
        return (True, "new_file")
    if state.schema_changed:
        return (True, "schema_changed")
    return (True, "content_changed")


def detect_changes(state: FileMetadataState) -> list[str]:
    """Detect what changed between stored and current metadata."""
    changes: list[str] = []

    # Check mtime
    if state.mtime_changed:
        changes.append("mtime")

    # A file bbox in another CRS is None and gives no bbox signal.
    from portolan_cli.metadata.models import BBOX_TOLERANCE, _bboxes_equal

    if state.current_bbox is not None and (
        state.stored_bbox is None
        or not _bboxes_equal(state.stored_bbox, state.current_bbox, BBOX_TOLERANCE)
    ):
        changes.append("bbox")

    # Check feature count
    if (
        state.stored_feature_count is None and state.current_feature_count is not None
    ) or state.current_feature_count != state.stored_feature_count:
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
    """Check metadata status for a single geo-asset file."""
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

    # No stored fingerprint, so mtime alone proves nothing; a matching content hash reads FRESH.
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

    # Without a fingerprint baseline, report STALE, not BREAKING.
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
    from portolan_core.checksums import compute_checksum

    if not file_path.is_file():
        return False
    try:
        checksum = state.checksum(file_path) if state is not None else compute_checksum(file_path)
        return checksum == stored_sha256
    except (ValueError, OSError):
        return False
