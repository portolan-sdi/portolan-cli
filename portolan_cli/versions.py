"""Versions module - manages versions.json for dataset versioning.

The versions.json file is the single source of truth for dataset versioning,
sync state, and integrity checksums (see ADR-0005).

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

import json
import os
import re
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Spec version constant (MINOR #12)
SPEC_VERSION = "1.0.0"


@dataclass(frozen=True)
class SchemaInfo:
    """Schema information for breaking change detection (ADR-0005).

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
        href: Relative or absolute path/URL to the asset.
        source_path: Optional relative path to the original source file
            (e.g., the GeoJSON that was converted to this GeoParquet).
        source_mtime: Optional Unix timestamp of the source file when
            conversion occurred. Used to detect when source has changed.
    """

    sha256: str
    size_bytes: int
    href: str
    source_path: str | None = None
    source_mtime: float | None = None


@dataclass(frozen=True)
class Version:
    """A single version entry in the versions history.

    Attributes:
        version: Semantic version string (e.g., "1.0.0").
        created: UTC timestamp when this version was created.
        breaking: Whether this version has breaking changes.
        assets: Mapping of filename to Asset metadata.
        changes: List of filenames that changed in this version.
        schema: Optional schema fingerprint for breaking change detection (ADR-0005).
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
                )
                for name, asset_data in v["assets"].items()
            }

            # Parse optional schema (ADR-0005)
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
    path.parent.mkdir(parents=True, exist_ok=True)

    data = _serialize_versions_file(versions_file)
    content = json.dumps(data, indent=2, ensure_ascii=False) + "\n"

    # Atomic write: write to temp file in same directory, then rename
    # This ensures the file is never in a partial/corrupted state
    fd, tmp_path = tempfile.mkstemp(
        dir=path.parent,
        prefix=".versions_",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        # Atomic rename (POSIX guarantees atomicity for same-filesystem renames)
        os.replace(tmp_path, path)
    except Exception:
        # Clean up temp file on failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _serialize_asset(asset: Asset) -> dict[str, Any]:
    """Serialize an Asset to a JSON-compatible dictionary.

    Only includes source_path and source_mtime when they are not None.

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
    # Only include source tracking fields when present
    if asset.source_path is not None:
        data["source_path"] = asset.source_path
    if asset.source_mtime is not None:
        data["source_mtime"] = asset.source_mtime
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
    # Only include optional fields when present (ADR-0005)
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
) -> VersionsFile:
    """Add a new version to a VersionsFile.

    This function is immutable - it returns a new VersionsFile rather than
    modifying the input.

    Args:
        versions_file: The existing VersionsFile.
        version: The new version string (e.g., "1.1.0").
        assets: Mapping of filename to Asset for this version.
        breaking: Whether this version has breaking changes.
        schema: Optional schema fingerprint for breaking change detection (ADR-0005).
        message: Optional human-readable description of the change.

    Returns:
        A new VersionsFile with the version added.
    """
    # Compute which files changed (new or different checksum)
    changes = _compute_changes(versions_file, assets)

    new_version = Version(
        version=version,
        created=datetime.now(timezone.utc),
        breaking=breaking,
        assets=assets,
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
