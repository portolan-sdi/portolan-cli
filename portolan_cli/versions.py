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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Asset:
    """A single asset (file) within a version.

    Attributes:
        sha256: SHA-256 checksum of the file content.
        size_bytes: File size in bytes.
        href: Relative or absolute path/URL to the asset.
    """

    sha256: str
    size_bytes: int
    href: str


@dataclass(frozen=True)
class Version:
    """A single version entry in the versions history.

    Attributes:
        version: Semantic version string (e.g., "1.0.0").
        created: UTC timestamp when this version was created.
        breaking: Whether this version has breaking changes.
        assets: Mapping of filename to Asset metadata.
        changes: List of filenames that changed in this version.
    """

    version: str
    created: datetime
    breaking: bool
    assets: dict[str, Asset]
    changes: list[str]


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
        data = json.loads(path.read_text())
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
                )
                for name, asset_data in v["assets"].items()
            }
            version = Version(
                version=v["version"],
                created=datetime.fromisoformat(v["created"].replace("Z", "+00:00")),
                breaking=v["breaking"],
                assets=assets,
                changes=v["changes"],
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
    """Write a VersionsFile to disk as JSON.

    Creates parent directories if they don't exist.

    Args:
        path: Destination path for the versions.json file.
        versions_file: The VersionsFile to serialize.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    data = _serialize_versions_file(versions_file)
    path.write_text(json.dumps(data, indent=2) + "\n")


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
        "versions": [
            {
                "version": v.version,
                "created": v.created.isoformat().replace("+00:00", "Z"),
                "breaking": v.breaking,
                "assets": {
                    name: {
                        "sha256": asset.sha256,
                        "size_bytes": asset.size_bytes,
                        "href": asset.href,
                    }
                    for name, asset in v.assets.items()
                },
                "changes": v.changes,
            }
            for v in versions_file.versions
        ],
    }


def add_version(
    versions_file: VersionsFile,
    *,
    version: str,
    assets: dict[str, Asset],
    breaking: bool,
) -> VersionsFile:
    """Add a new version to a VersionsFile.

    This function is immutable - it returns a new VersionsFile rather than
    modifying the input.

    Args:
        versions_file: The existing VersionsFile.
        version: The new version string (e.g., "1.1.0").
        assets: Mapping of filename to Asset for this version.
        breaking: Whether this version has breaking changes.

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
    )

    return VersionsFile(
        spec_version=versions_file.spec_version,
        current_version=version,
        versions=[*versions_file.versions, new_version],
    )


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
