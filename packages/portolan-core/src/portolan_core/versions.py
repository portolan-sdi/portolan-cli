"""Read, write, and update ``versions.json``, the source of truth for versions and checksums.

The portolan-spec repo defines its structure.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from portolan_core.checksums import compute_checksum
from portolan_core.json_io import write_json_atomic
from portolan_core.trace import count

if TYPE_CHECKING:
    from pathlib import Path

    from portolan_core.run_state import RunState

_PRERELEASE_PARTS = 2
_SEMVER_PARTS = 3

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
        href: Path relative to the catalog root, resolved as ``catalog_root / href``.
        source_path: Optional relative path of the source file it was converted from.
        source_mtime: Optional Unix mtime of the source at conversion, to detect a source change.
        mtime: Optional Unix mtime of the asset file, for fast-path change detection.
        feature_count: Optional feature, row, or pixel count, so a touched but identical asset is FRESH.
        schema_fingerprint: Optional hash of the asset schema, to detect breaking schema changes.
        source_fingerprint: Optional hash of a generated asset's inputs; it rebuilds when this differs.
    """

    sha256: str
    size_bytes: int
    href: str
    source_path: str | None = None
    source_mtime: float | None = None
    mtime: float | None = None
    feature_count: int | None = None
    schema_fingerprint: str | None = None
    source_fingerprint: str | None = None


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
    """The complete versions.json file: spec version, current version or None, versions oldest first."""

    spec_version: str
    current_version: str | None
    versions: list[Version] = field(default_factory=list)


def read_versions(path: Path) -> VersionsFile:
    """Read and parse a versions.json file; raise ``FileNotFoundError`` or ``ValueError``."""
    if not path.exists():
        raise FileNotFoundError(f"versions.json not found: {path}")

    count("versions_json.reads")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in versions.json: {e}") from e

    return _parse_versions_file(data)


def _parse_versions_file(data: dict[str, Any]) -> VersionsFile:
    """Parse a dictionary into a VersionsFile; raise ``ValueError`` on a missing or bad field."""
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
                    source_path=asset_data.get("source_path"),
                    source_mtime=asset_data.get("source_mtime"),
                    mtime=asset_data.get("mtime"),
                    feature_count=asset_data.get("feature_count"),
                    schema_fingerprint=asset_data.get("schema_fingerprint"),
                    source_fingerprint=asset_data.get("source_fingerprint"),
                )
                for name, asset_data in v["assets"].items()
            }

            schema_data = v.get("schema")
            schema = None
            if schema_data is not None:
                schema = SchemaInfo(
                    type=schema_data["type"],
                    fingerprint=schema_data["fingerprint"],
                )

            version = Version(
                version=v["version"],
                created=datetime.fromisoformat(v["created"]),
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
    """Write a VersionsFile atomically, so an interrupted write cannot corrupt it."""
    write_json_atomic(path, _serialize_versions_file(versions_file))


def _serialize_asset(asset: Asset) -> dict[str, Any]:
    """Serialize an Asset to a JSON dictionary, leaving out optional fields that are None."""
    data: dict[str, Any] = {
        "sha256": asset.sha256,
        "size_bytes": asset.size_bytes,
        "href": asset.href,
    }
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
    if asset.source_fingerprint is not None:
        data["source_fingerprint"] = asset.source_fingerprint
    return data


def _serialize_version(v: Version) -> dict[str, Any]:
    """Serialize a Version to a JSON dictionary, leaving out ``schema`` and ``message`` if None."""
    data: dict[str, Any] = {
        "version": v.version,
        "created": v.created.isoformat().replace("+00:00", "Z"),
        "breaking": v.breaking,
        "assets": {name: _serialize_asset(asset) for name, asset in v.assets.items()},
        "changes": v.changes,
    }
    if v.schema is not None:
        data["schema"] = {
            "type": v.schema.type,
            "fingerprint": v.schema.fingerprint,
        }
    if v.message is not None:
        data["message"] = v.message
    return data


def _serialize_versions_file(versions_file: VersionsFile) -> dict[str, Any]:
    """Serialize a VersionsFile to a JSON-compatible dictionary."""
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
    """Return a new VersionsFile with a full snapshot: prior assets plus ``assets``, less ``removed``.

    The writer of the result counts ``versions.published``, so a projection counts nothing.
    """
    if versions_file.versions:
        merged_assets = dict(versions_file.versions[-1].assets)
        merged_assets.update(assets)
        if removed:
            for key in removed:
                merged_assets.pop(key, None)
    else:
        merged_assets = dict(assets)

    changes = _compute_changes(versions_file, assets)

    # Skip a no-op version, unless a message, breaking flag, or schema asks for one.
    explicit_request = bool(message or breaking or schema)
    if not changes and not removed and versions_file.versions and not explicit_request:
        return versions_file

    new_version = Version(
        version=version,
        created=datetime.now(UTC),
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
    """Parse a semver string into ``(major, minor, patch)``, dropping pre-release and build tags.

    ``"1.0.0-beta"`` and ``"1.0.0+build.123"`` give ``(1, 0, 0)``.

    Args:
        version_str: Semantic version string (e.g., "1.2.3", "1.0.0-beta").

    Returns:
        Tuple of (major, minor, patch) integers. Returns (0, 0, 0) if parsing fails.
    """
    if not version_str:
        return (0, 0, 0)

    base_version = version_str.split("-", maxsplit=1)[0].split("+", maxsplit=1)[0]

    match = re.match(r"^(\d+)\.(\d+)\.(\d+)$", base_version)
    if not match:
        return (0, 0, 0)

    try:
        major = int(match.group(1))
        minor = int(match.group(2))
        patch = int(match.group(3))
        return (major, minor, patch)
    except (ValueError, OverflowError):
        return (0, 0, 0)


def _increment_version(version: str) -> str:
    """Increment a semver string; a pre-release such as ``1.0.0-beta.1`` keeps its tag."""
    if not version:
        return "0.0.1"

    if "-" in version:
        base, prerelease = version.split("-", 1)
        prerelease_parts = prerelease.rsplit(".", 1)
        if len(prerelease_parts) == _PRERELEASE_PARTS and prerelease_parts[1].isdigit():
            prerelease_parts[1] = str(int(prerelease_parts[1]) + 1)
            return f"{base}-{'.'.join(prerelease_parts)}"
        # No numeric suffix: 1.0.0-beta becomes 1.0.0-beta.1.
        return f"{base}-{prerelease}.1"

    parts = version.split(".")
    if len(parts) >= _SEMVER_PARTS and parts[-1].isdigit():
        parts[-1] = str(int(parts[-1]) + 1)
        return ".".join(parts)
    if len(parts) < _SEMVER_PARTS:
        while len(parts) < _SEMVER_PARTS:
            parts.append("0")
        parts[-1] = "1"
        return ".".join(parts)

    # A non-numeric last part ("1.2.3rc1") must still bump, so normalize it.
    major, minor, patch = parse_version(version)
    return f"{major}.{minor}.{patch + 1}"


def next_version(versions_file: VersionsFile) -> str:
    """The next version of ``versions_file``: ``1.0.0`` first, then a patch bump."""
    if versions_file.current_version is None:
        return "1.0.0"
    return _increment_version(versions_file.current_version)


def _compute_changes(versions_file: VersionsFile, new_assets: dict[str, Asset]) -> list[str]:
    """List the asset names that are new or whose SHA-256 differs from the previous version."""
    if not versions_file.versions:
        return list(new_assets.keys())

    previous_assets = versions_file.versions[-1].assets
    changes = []

    for name, asset in new_assets.items():
        if name not in previous_assets or previous_assets[name].sha256 != asset.sha256:
            changes.append(name)

    return changes


def _asset_href(asset_path: Path, catalog_root: Path, collection_path: Path) -> str:
    """The catalog-root-relative href of a generated asset; both sides resolve for symlinked roots."""
    asset_path = asset_path.resolve()
    try:
        rel_path = asset_path.relative_to(catalog_root.resolve())
    except ValueError:
        # Fallback when the asset is not under the catalog root.
        rel_path = asset_path.relative_to(collection_path.resolve().parent)
    return rel_path.as_posix()


def versions_asset_key(asset_path: Path, collection_path: Path) -> str:
    """The versions.json key of a file: its POSIX path below the collection, else its name.

    A bare-name key would give an item file a second key at the collection root.
    """
    try:
        return asset_path.resolve().relative_to(collection_path.resolve()).as_posix()
    except ValueError:
        return asset_path.name


def _is_tracked(
    asset_path: Path,
    tracked: dict[str, Asset],
    catalog_root: Path,
    collection_path: Path,
) -> bool:
    """True when the snapshot records this exact file, matched by href and not by basename."""
    existing = tracked.get(versions_asset_key(asset_path, collection_path))
    if existing is None:
        return False
    return existing.href == _asset_href(asset_path, catalog_root, collection_path)


def generated_asset_record(
    asset_path: Path, catalog_root: Path, collection_path: Path, sha256: str
) -> tuple[str, Asset]:
    """The versions.json key and record of a generated file, with its size and mtime from disk."""
    stat = asset_path.stat()
    return versions_asset_key(asset_path, collection_path), Asset(
        sha256=sha256,
        size_bytes=stat.st_size,
        href=_asset_href(asset_path, catalog_root, collection_path),
        mtime=stat.st_mtime,
    )


def amend_version(
    versions_file: VersionsFile,
    assets: dict[str, Asset],
    removed: set[str] | None = None,
) -> VersionsFile:
    """Fold asset changes into the latest version; amend only a version this command wrote.

    The input returns when the snapshot does not change, so ``result is versions_file`` skips a write.
    """
    latest = versions_file.versions[-1]
    merged = {**latest.assets, **assets}
    for key in removed or ():
        merged.pop(key, None)
    if merged == latest.assets:
        return versions_file
    amended = replace(
        latest,
        assets=merged,
        changes=list(dict.fromkeys([*latest.changes, *assets])),
    )
    return replace(versions_file, versions=[*versions_file.versions[:-1], amended])


def track_generated_assets(
    collection_path: Path,
    asset_paths: list[Path],
    catalog_root: Path,
    *,
    message: str,
    only_if_missing: bool = False,
    amend_latest: bool = False,
    state: RunState | None = None,
) -> None:
    """Track generated side-step assets (PMTiles, thumbnail) in one versions.json version.

    Pass ``amend_latest`` only for a version this command wrote; a file-backend ``state`` stages them.
    """
    for asset_path in asset_paths:
        if not asset_path.exists():
            raise FileNotFoundError(f"File not found at {asset_path}")

    versions_path = collection_path / "versions.json"

    staged = state is not None and state.backend(collection_path) == "file"
    if not versions_path.exists():
        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version=None,
            versions=[],
        )
    elif state is not None and staged:
        versions_file = state.versions(collection_path, include_staged=False)
    else:
        versions_file = read_versions(versions_path)
    if state is not None and staged:
        # The records that the run staged count as tracked too.
        tracked = state.tracked_assets(collection_path)
    else:
        tracked = dict(versions_file.versions[-1].assets) if versions_file.versions else {}

    # Backfill: match the href, not the basename, since items share thumbnail names.
    paths_to_track = asset_paths
    if only_if_missing and tracked:
        paths_to_track = [
            p for p in asset_paths if not _is_tracked(p, tracked, catalog_root, collection_path)
        ]
    if not paths_to_track:
        return

    checksum = state.checksum if state is not None else compute_checksum
    assets = dict(
        generated_asset_record(p, catalog_root, collection_path, checksum(p))
        for p in paths_to_track
    )

    if state is not None and staged:
        state.stage(
            collection_path.resolve().relative_to(catalog_root.resolve()).as_posix(),
            assets=assets,
            message=None if amend_latest else message,
            amend=amend_latest,
        )
        return

    if amend_latest and versions_file.versions:
        # One `add` is one version, so a derived asset folds into its snapshot.
        write_versions(versions_path, amend_version(versions_file, assets))
        return

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
    if updated is not versions_file:
        count("versions.published")
