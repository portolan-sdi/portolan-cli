"""Versions module - manages versions.json for collection versioning.

The versions.json file is the single source of truth for collection versioning,
sync state, and integrity checksums.

Structure:
    {
        "spec_version": "1.0.0",
        "current_version": "2.1.0",
        "versions": [
            {
                "version": "2.1.0",
                "created": "2024-01-15T10:30:00Z",
                "breaking": false,
                "message": "Data update, no schema changes",
                "schema": {
                    "type": "geoparquet",
                    "fingerprint": {
                        "columns": [...]
                    }
                },
                "assets": {
                    "data.parquet": {
                        "sha256": "abc123...",
                        "size_bytes": 1048576,
                        "href": "s3://bucket/dataset/data.parquet"
                    }
                },
                "changes": ["data.parquet"]
            }
        ]
    }
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from portolan_cli.json_io import write_json_atomic

# Spec version constant (MINOR #12)
SPEC_VERSION = "1.0.0"


@dataclass(frozen=True)
class SchemaInfo:
    """Schema information for breaking change detection.

    Attributes:
        type: Schema type identifier (e.g., "geoparquet", "cog").
        fingerprint: Type-specific schema fingerprint for change detection.
    """

    type: str
    fingerprint: dict[str, Any]


@dataclass(frozen=True)
class Asset:
    """A single asset (file) within a version.

    Attributes:
        sha256: SHA-256 checksum of the file content.
        size_bytes: File size in bytes.
        href: Path to the asset, relative to catalog root
            (e.g., "collection_id/item_id/filename.parquet").
            push.py and pull.py resolve this via ``catalog_root / href``.
        source_path: Optional relative path to the original source file
            (e.g., the GeoJSON that was converted to this GeoParquet).
        source_mtime: Optional Unix timestamp of the source file when
            conversion occurred. Used to detect when source has changed.
        mtime: Optional Unix timestamp of the asset file itself.
            Used for fast-path change detection.
        feature_count: Optional feature/row count (pixel count for rasters)
            captured when the asset was tracked. Lets a touched-but-identical
            asset read FRESH instead of a spurious STALE (heuristics).
        schema_fingerprint: Optional hash of the asset schema captured when the
            asset was tracked. Used to detect breaking schema changes.
    """

    sha256: str
    size_bytes: int
    href: str
    source_path: str | None = None
    source_mtime: float | None = None
    mtime: float | None = None
    feature_count: int | None = None
    schema_fingerprint: str | None = None


@dataclass(frozen=True)
class Version:
    """A single version entry in the versions history.

    Attributes:
        version: Semantic version string (e.g., "1.0.0").
        created: UTC timestamp when this version was created.
        breaking: Whether this version has breaking changes.
        assets: Mapping of filename to Asset metadata.
        changes: List of filenames that changed in this version.
        schema: Optional schema fingerprint for breaking change detection.
        message: Optional human-readable description of the change.
    """

    version: str
    created: datetime
    breaking: bool
    assets: dict[str, Asset]
    changes: list[str]
    schema: SchemaInfo | None = None
    message: str | None = None


@dataclass
class VersionsFile:
    """The complete versions.json file structure.

    Attributes:
        spec_version: Schema version for the versions.json format.
        current_version: The current/latest version string, or None if no versions.
        versions: List of Version entries, oldest first.
    """

    spec_version: str
    current_version: str | None
    versions: list[Version] = field(default_factory=list)


def read_versions(path: Path) -> VersionsFile:
    """Read and parse a versions.json file.

    Args:
        path: Path to the versions.json file.

    Returns:
        Parsed VersionsFile object.

    Raises:
        FileNotFoundError: If the file doesn't exist.
        ValueError: If the JSON is invalid or doesn't match the schema.
    """
    if not path.exists():
        raise FileNotFoundError(f"versions.json not found: {path}")

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in versions.json: {e}") from e

    return _parse_versions_file(data)


def _parse_versions_file(data: dict[str, Any]) -> VersionsFile:
    """Parse a dictionary into a VersionsFile object.

    Args:
        data: Parsed JSON dictionary.

    Returns:
        VersionsFile object.

    Raises:
        ValueError: If required fields are missing or invalid.
    """
    try:
        spec_version = data["spec_version"]
        current_version = data["current_version"]
        versions_data = data["versions"]
    except KeyError as e:
        raise ValueError(f"Invalid versions.json schema: missing field {e}") from e

    versions = []
    for v in versions_data:
        try:
            assets = {
                name: Asset(
                    sha256=asset_data["sha256"],
                    size_bytes=asset_data["size_bytes"],
                    href=asset_data["href"],
                    # Optional source tracking fields with defaults
                    source_path=asset_data.get("source_path"),
                    source_mtime=asset_data.get("source_mtime"),
                    # Optional asset mtime for fast-path
                    mtime=asset_data.get("mtime"),
                    # Optional freshness heuristics
                    feature_count=asset_data.get("feature_count"),
                    schema_fingerprint=asset_data.get("schema_fingerprint"),
                )
                for name, asset_data in v["assets"].items()
            }

            # Parse optional schema
            schema_data = v.get("schema")
            schema = None
            if schema_data is not None:
                schema = SchemaInfo(
                    type=schema_data["type"],
                    fingerprint=schema_data["fingerprint"],
                )

            version = Version(
                version=v["version"],
                created=datetime.fromisoformat(v["created"].replace("Z", "+00:00")),
                breaking=v["breaking"],
                assets=assets,
                changes=v["changes"],
                schema=schema,
                message=v.get("message"),
            )
            versions.append(version)
        except (KeyError, TypeError) as e:
            raise ValueError(f"Invalid versions.json schema: {e}") from e

    return VersionsFile(
        spec_version=spec_version,
        current_version=current_version,
        versions=versions,
    )


def write_versions(path: Path, versions_file: VersionsFile) -> None:
    """Write a VersionsFile to disk as JSON atomically.

    Uses atomic write pattern (write to temp file, then rename) to prevent
    corruption from interrupted writes (CRITICAL #2 - TOCTOU race condition).

    Creates parent directories if they don't exist.

    Args:
        path: Destination path for the versions.json file.
        versions_file: The VersionsFile to serialize.
    """
    write_json_atomic(path, _serialize_versions_file(versions_file))


def _serialize_asset(asset: Asset) -> dict[str, Any]:
    """Serialize an Asset to a JSON-compatible dictionary.

    Only includes optional fields (source_path, source_mtime, mtime,
    feature_count, schema_fingerprint) when not None.

    Args:
        asset: The Asset to serialize.

    Returns:
        Dictionary suitable for JSON serialization.
    """
    data: dict[str, Any] = {
        "sha256": asset.sha256,
        "size_bytes": asset.size_bytes,
        "href": asset.href,
    }
    # Only include optional fields when present
    if asset.source_path is not None:
        data["source_path"] = asset.source_path
    if asset.source_mtime is not None:
        data["source_mtime"] = asset.source_mtime
    if asset.mtime is not None:
        data["mtime"] = asset.mtime
    if asset.feature_count is not None:
        data["feature_count"] = asset.feature_count
    if asset.schema_fingerprint is not None:
        data["schema_fingerprint"] = asset.schema_fingerprint
    return data


def _serialize_version(v: Version) -> dict[str, Any]:
    """Serialize a Version to a JSON-compatible dictionary.

    Only includes optional fields (schema, message) when they are not None.

    Args:
        v: The Version to serialize.

    Returns:
        Dictionary suitable for JSON serialization.
    """
    data: dict[str, Any] = {
        "version": v.version,
        "created": v.created.isoformat().replace("+00:00", "Z"),
        "breaking": v.breaking,
        "assets": {name: _serialize_asset(asset) for name, asset in v.assets.items()},
        "changes": v.changes,
    }
    # Only include optional fields when present
    if v.schema is not None:
        data["schema"] = {
            "type": v.schema.type,
            "fingerprint": v.schema.fingerprint,
        }
    if v.message is not None:
        data["message"] = v.message
    return data


def _serialize_versions_file(versions_file: VersionsFile) -> dict[str, Any]:
    """Serialize a VersionsFile to a JSON-compatible dictionary.

    Args:
        versions_file: The VersionsFile to serialize.

    Returns:
        Dictionary suitable for JSON serialization.
    """
    return {
        "spec_version": versions_file.spec_version,
        "current_version": versions_file.current_version,
        "versions": [_serialize_version(v) for v in versions_file.versions],
    }


def add_version(
    versions_file: VersionsFile,
    *,
    version: str,
    assets: dict[str, Asset],
    breaking: bool,
    schema: SchemaInfo | None = None,
    message: str | None = None,
    removed: set[str] | None = None,
) -> VersionsFile:
    """Add a new version to a VersionsFile.

    This function is immutable - it returns a new VersionsFile rather than
    modifying the input.

    Each version is a complete SNAPSHOT of all assets at that point in time. New assets are merged with the previous version's assets,
    and any assets in `removed` are excluded.

    Args:
        versions_file: The existing VersionsFile.
        version: The new version string (e.g., "1.1.0").
        assets: Mapping of filename to Asset to add or update in this version.
        breaking: Whether this version has breaking changes.
        schema: Optional schema fingerprint for breaking change detection.
        message: Optional human-readable description of the change.
        removed: Optional set of asset keys to remove from the snapshot.

    Returns:
        A new VersionsFile with the version added.
    """
    # Build complete snapshot: start with previous assets, apply changes
    if versions_file.versions:
        # Copy previous version's assets as base
        merged_assets = dict(versions_file.versions[-1].assets)
        # Update with new/modified assets
        merged_assets.update(assets)
        # Remove any assets marked for removal
        if removed:
            for key in removed:
                merged_assets.pop(key, None)
    else:
        # First version: just use the provided assets
        merged_assets = dict(assets)

    # Compute which files changed (new or different checksum)
    changes = _compute_changes(versions_file, assets)

    # Early return if nothing changed (no new/modified assets and no removals)
    # This prevents creating no-op versions when re-adding unchanged files
    # BUT: allow versions when metadata indicates explicit request (message, breaking, schema)
    if not changes and not removed and versions_file.versions:
        # Only skip if truly a no-op: no message, not breaking, no schema change
        if not message and not breaking and not schema:
            return versions_file

    new_version = Version(
        version=version,
        created=datetime.now(timezone.utc),
        breaking=breaking,
        assets=merged_assets,
        changes=changes,
        schema=schema,
        message=message,
    )

    return VersionsFile(
        spec_version=versions_file.spec_version,
        current_version=version,
        versions=[*versions_file.versions, new_version],
    )


def parse_version(version_str: str) -> tuple[int, int, int]:
    """Parse a semantic version string into (major, minor, patch) tuple.

    Handles standard semver, pre-release versions (CRITICAL #3), and build metadata.
    Returns (0, 0, 0) for invalid versions instead of raising.

    Examples:
        >>> parse_version("1.2.3")
        (1, 2, 3)
        >>> parse_version("1.0.0-beta")  # Pre-release stripped
        (1, 0, 0)
        >>> parse_version("1.0.0+build.123")  # Build metadata stripped
        (1, 0, 0)
        >>> parse_version("invalid")
        (0, 0, 0)

    Args:
        version_str: Semantic version string (e.g., "1.2.3", "1.0.0-beta").

    Returns:
        Tuple of (major, minor, patch) integers. Returns (0, 0, 0) if parsing fails.
    """
    if not version_str:
        return (0, 0, 0)

    # Strip pre-release (-beta, -alpha.1, etc.) and build metadata (+build.123)
    # Per semver spec: version = major.minor.patch[-prerelease][+buildmetadata]
    base_version = version_str.split("-")[0].split("+")[0]

    # Match major.minor.patch pattern
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)$", base_version)
    if not match:
        return (0, 0, 0)

    try:
        # Use int() which handles arbitrarily large numbers in Python (MAJOR #4)
        major = int(match.group(1))
        minor = int(match.group(2))
        patch = int(match.group(3))
        return (major, minor, patch)
    except (ValueError, OverflowError):
        # Shouldn't happen with \d+ regex, but handle gracefully
        return (0, 0, 0)


def _increment_version(version: str) -> str:
    """Safely increment a semantic version string.

    Handles standard semver (1.2.3) and pre-release versions (1.0.0-beta.1).

    Args:
        version: Current version string.

    Returns:
        Incremented version string.
    """
    if not version:
        return "0.0.1"

    # Handle pre-release versions (e.g., 1.0.0-beta.1)
    if "-" in version:
        base, prerelease = version.split("-", 1)
        # Try to increment the prerelease number
        prerelease_parts = prerelease.rsplit(".", 1)
        if len(prerelease_parts) == 2 and prerelease_parts[1].isdigit():
            prerelease_parts[1] = str(int(prerelease_parts[1]) + 1)
            return f"{base}-{'.'.join(prerelease_parts)}"
        else:
            # No numeric suffix: 1.0.0-beta → 1.0.0-beta.1
            # Preserve the prerelease tag by appending .1
            return f"{base}-{prerelease}.1"

    # Standard semver: increment patch
    parts = version.split(".")
    if len(parts) >= 3 and parts[-1].isdigit():
        parts[-1] = str(int(parts[-1]) + 1)
        return ".".join(parts)
    if len(parts) < 3:
        # Pad to 3 parts if needed
        while len(parts) < 3:
            parts.append("0")
        parts[-1] = "1"
        return ".".join(parts)

    # Three-plus parts but the last segment is not purely numeric
    # (e.g. "1.2.3rc1", "1.2.foo"). A plain join here would return the input
    # unchanged - a silent no-op that breaks version bumps. Normalize through
    # parse_version and bump the patch so the result always differs.
    major, minor, patch = parse_version(version)
    return f"{major}.{minor}.{patch + 1}"


def _compute_changes(versions_file: VersionsFile, new_assets: dict[str, Asset]) -> list[str]:
    """Compute which files changed compared to the previous version.

    A file is considered "changed" if:
    - It's new (not in the previous version)
    - Its SHA-256 checksum differs from the previous version

    Args:
        versions_file: The existing VersionsFile.
        new_assets: The assets for the new version.

    Returns:
        List of filenames that changed.
    """
    if not versions_file.versions:
        # First version - all assets are "changes"
        return list(new_assets.keys())

    previous_assets = versions_file.versions[-1].assets
    changes = []

    for name, asset in new_assets.items():
        if name not in previous_assets:
            # New file
            changes.append(name)
        elif previous_assets[name].sha256 != asset.sha256:
            # Modified file
            changes.append(name)

    return changes


def _compute_sha256(path: Path) -> str:
    """Stream a file in 64KB chunks and return its SHA-256 hex digest.

    Chunked to avoid loading large PMTiles/thumbnail files fully into memory.
    """
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):  # 64KB chunks
            hasher.update(chunk)
    return hasher.hexdigest()


def track_generated_assets(
    collection_path: Path,
    asset_paths: list[Path],
    catalog_root: Path,
    *,
    message: str,
    only_if_missing: bool = False,
) -> None:
    """Track generated side-step assets (PMTiles, thumbnail) in versions.json.

    Computes SHA-256, size, mtime and path records in a *single* new version
    snapshot. A PMTiles and its thumbnail come from the same side-step on the
    same source asset, so they belong in one version, not two (Issue #519).
    ``add_version`` carries forward the previous version's assets, so the result
    is a complete snapshot with the assets added or updated.

    Lives here rather than beside either caller because both the PMTiles
    side-step (``viz.pmtiles``) and the collection-thumbnail orchestrator
    (``collection_thumbnail``) must record derived assets the same way. An
    untracked derived asset breaks ``push`` (Issues #519, #735).

    Args:
        collection_path: Path to the collection directory.
        asset_paths: Paths of the generated files to track.
        catalog_root: Path to the catalog root (hrefs are catalog-root-relative).
        message: Human-readable description of the change.
        only_if_missing: If True, only track assets whose filename is not already
            present in the latest version snapshot, and create no version at all
            if every asset is already tracked. Used by the skip path to backfill
            artifacts generated before tracking existed, without bumping a
            version on every unchanged ``add`` (Issue #519).

    Raises:
        FileNotFoundError: If any asset path doesn't exist.
    """
    for asset_path in asset_paths:
        if not asset_path.exists():
            raise FileNotFoundError(f"File not found at {asset_path}")

    versions_path = collection_path / "versions.json"

    # If no versions.json, create a minimal one
    if not versions_path.exists():
        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version=None,
            versions=[],
        )
    else:
        versions_file = read_versions(versions_path)

    # Backfill mode: skip assets already tracked, and create no version if none
    # are missing (otherwise the message would force a no-op version bump).
    paths_to_track = asset_paths
    if only_if_missing and versions_file.versions:
        tracked = versions_file.versions[-1].assets
        paths_to_track = [p for p in asset_paths if p.name not in tracked]
    if not paths_to_track:
        return

    assets: dict[str, Asset] = {}
    for asset_path in paths_to_track:
        stat = asset_path.stat()
        # Href is relative to catalog root
        try:
            rel_path = asset_path.relative_to(catalog_root)
        except ValueError:
            # Fallback if not relative
            rel_path = asset_path.relative_to(collection_path.parent)
        assets[asset_path.name] = Asset(
            sha256=_compute_sha256(asset_path),
            size_bytes=stat.st_size,
            href=rel_path.as_posix(),
            mtime=stat.st_mtime,
        )

    # Determine next version
    if versions_file.current_version:
        major, minor, patch = parse_version(versions_file.current_version)
        new_version = f"{major}.{minor}.{patch + 1}"
    else:
        new_version = "1.0.0"

    updated = add_version(
        versions_file,
        version=new_version,
        assets=assets,
        breaking=False,
        message=message,
    )

    write_versions(versions_path, updated)
