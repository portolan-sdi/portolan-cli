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
from dataclasses import dataclass
from pathlib import Path

from portolan_cli.metadata.cog import extract_cog_metadata
from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata
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
    """

    item_id: str
    bbox: list[float] | None
    source_mtime: float | None
    sha256: str | None
    feature_count: int | None
    schema_fingerprint: str | None


def get_stored_metadata(
    file_path: Path,
    collection_dir: Path,
) -> StoredMetadata | None:
    """Read existing STAC item and versions.json metadata.

    Looks for a STAC item JSON file matching the asset filename,
    and extracts tracking data from versions.json if present.

    Args:
        file_path: Path to the geo-asset file.
        collection_dir: Directory containing STAC item and versions.json.

    Returns:
        StoredMetadata if found, None if no metadata exists.
    """
    # Look for item JSON matching the file stem
    item_name = file_path.stem + ".json"
    item_path = collection_dir / item_name

    if not item_path.exists():
        return None

    try:
        with open(item_path) as f:
            item_data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None

    # Validate it's a STAC item
    if item_data.get("type") != "Feature":
        return None

    # Extract bbox from item (None if not explicitly provided)
    bbox = item_data.get("bbox")

    # Default values for optional fields
    source_mtime: float | None = None
    sha256: str | None = None
    feature_count: int | None = None
    schema_fingerprint: str | None = None

    # Try to read versions.json for tracking data
    versions_path = collection_dir / "versions.json"
    if versions_path.exists():
        try:
            with open(versions_path) as f:
                versions_data = json.load(f)

            # Find asset entry in current version (use current_version field)
            versions = versions_data.get("versions", [])
            current_version_id = versions_data.get("current_version")

            # Find the version matching current_version, fallback to last in list
            current_version = None
            if current_version_id:
                for v in versions:
                    if v.get("version") == current_version_id:
                        current_version = v
                        break
            if current_version is None and versions:
                current_version = versions[-1]  # Fallback: last is most recent

            if current_version:
                assets = current_version.get("assets", {})
                asset_key = file_path.name
                if asset_key in assets:
                    asset_data = assets[asset_key]
                    source_mtime = asset_data.get("source_mtime")
                    sha256 = asset_data.get("sha256")
                    feature_count = asset_data.get("feature_count")
                    schema_fingerprint = asset_data.get("schema_fingerprint")
        except (json.JSONDecodeError, OSError, KeyError):
            pass

    return StoredMetadata(
        item_id=item_data.get("id", file_path.stem),
        bbox=bbox,  # None if not explicitly provided (avoid spurious heuristics_changed)
        source_mtime=source_mtime,
        sha256=sha256,
        feature_count=feature_count,
        schema_fingerprint=schema_fingerprint,
    )


def get_current_metadata(file_path: Path) -> FileMetadataState:
    """Extract fresh metadata from a geo-asset file.

    Supports GeoParquet (.parquet) and COG (.tif, .tiff) formats.

    Args:
        file_path: Path to the geo-asset file.

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
        # Extract GeoParquet metadata
        gp_metadata = extract_geoparquet_metadata(file_path)
        return FileMetadataState(
            file_path=file_path,
            current_mtime=current_mtime,
            stored_mtime=None,
            current_bbox=list(gp_metadata.bbox) if gp_metadata.bbox else None,
            stored_bbox=None,
            current_feature_count=gp_metadata.feature_count,
            stored_feature_count=None,
            current_schema_fingerprint=compute_schema_fingerprint(file_path),
            stored_schema_fingerprint=None,
        )

    elif suffix in (".tif", ".tiff"):
        # Extract COG metadata
        cog_metadata = extract_cog_metadata(file_path)
        pixel_count = (
            cog_metadata.width * cog_metadata.height
            if cog_metadata.width and cog_metadata.height
            else None
        )
        return FileMetadataState(
            file_path=file_path,
            current_mtime=current_mtime,
            stored_mtime=None,
            current_bbox=list(cog_metadata.bbox) if cog_metadata.bbox else None,
            stored_bbox=None,
            current_feature_count=pixel_count,
            stored_feature_count=None,
            current_schema_fingerprint=compute_schema_fingerprint(file_path),
            stored_schema_fingerprint=None,
        )

    else:
        raise ValueError(f"Unsupported format: {suffix}")


def compute_schema_fingerprint(file_path: Path) -> str:
    """Generate a hash fingerprint of the file schema.

    For GeoParquet: hash of column names and types.
    For COG: hash of band count, dtype, and CRS.

    Args:
        file_path: Path to the geo-asset file.

    Returns:
        Hexadecimal hash string representing the schema.
    """
    suffix = file_path.suffix.lower()

    if suffix == ".parquet":
        import pyarrow.parquet as pq

        # Use ParquetFile to read only schema, not the full table (O(1) vs O(n))
        pf = pq.ParquetFile(file_path)
        schema_str = str(pf.schema_arrow)
        return hashlib.sha256(schema_str.encode()).hexdigest()[:16]

    elif suffix in (".tif", ".tiff"):
        metadata = extract_cog_metadata(file_path)
        schema_parts = [
            str(metadata.band_count),
            metadata.dtype or "",
            metadata.crs or "",
        ]
        schema_str = "|".join(schema_parts)
        return hashlib.sha256(schema_str.encode()).hexdigest()[:16]

    else:
        # For unknown formats, hash the first 1KB
        with open(file_path, "rb") as f:
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

    # Fast path: mtime unchanged means file hasn't been touched
    if state.current_mtime == state.stored_mtime:
        return (False, "mtime_unchanged")

    # MTIME changed - check if schema changed (breaking change)
    if state.schema_changed:
        return (True, "schema_changed")

    # Check if heuristics changed (bbox or feature count)
    if state.heuristics_changed:
        return (True, "content_changed")

    # MTIME changed but content appears unchanged (file touched but not modified)
    return (False, "touched_unchanged")


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

    # Check bbox
    if state.stored_bbox is None and state.current_bbox is not None:
        changes.append("bbox")
    elif state.current_bbox != state.stored_bbox:
        changes.append("bbox")

    # Check feature count
    if state.stored_feature_count is None and state.current_feature_count is not None:
        changes.append("feature_count")
    elif state.current_feature_count != state.stored_feature_count:
        changes.append("feature_count")

    # Check schema
    if state.schema_changed:
        changes.append("schema")

    return changes


def check_file_metadata(
    file_path: Path,
    collection_dir: Path,
) -> MetadataCheckResult:
    """Check metadata status for a single geo-asset file.

    Combines get_stored_metadata and get_current_metadata to determine
    if the file's STAC metadata is up to date.

    Args:
        file_path: Path to the geo-asset file.
        collection_dir: Directory containing STAC metadata.

    Returns:
        MetadataCheckResult with status and details.

    Raises:
        FileNotFoundError: If file doesn't exist.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Get stored metadata
    stored = get_stored_metadata(file_path, collection_dir)

    if stored is None:
        return MetadataCheckResult(
            file_path=file_path,
            status=MetadataStatus.MISSING,
            message="Missing STAC metadata for file",
            fix_hint="Run 'portolan fix' to create STAC item",
        )

    # Get current metadata
    current = get_current_metadata(file_path)

    # Update current state with stored values
    state = FileMetadataState(
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
    stale, reason = is_stale(state)

    if not stale:
        return MetadataCheckResult(
            file_path=file_path,
            status=MetadataStatus.FRESH,
            message=f"Metadata is up to date ({reason})",
        )

    # Determine what changed
    changes = detect_changes(state)

    if reason == "schema_changed":
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
