"""Versioning helpers: semver logic and snapshot<->Version conversion.

Converts between PyIceberg Snapshot summary properties and portolan-cli
Version/Asset/SchemaInfo dataclasses.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from portolan_cli.versions import Asset, SchemaInfo, Version, parse_version

if TYPE_CHECKING:
    from pyiceberg.table.snapshots import Snapshot


def compute_next_version(current: str | None, breaking: bool) -> str:
    """Compute the next semantic version.

    Args:
        current: Current version string, or None for first version.
        breaking: Whether this is a breaking change.

    Returns:
        Next version string.
    """
    if current is None:
        return "1.0.0"
    major, minor, _patch = parse_version(current)
    if breaking:
        return f"{major + 1}.0.0"
    return f"{major}.{minor + 1}.0"


def version_to_snapshot_properties(
    version: str,
    breaking: bool,
    message: str,
    assets: dict[str, Asset],
    schema: SchemaInfo | None,
    changes: list[str],
) -> dict[str, str]:
    """Convert portolan Version fields to Iceberg snapshot summary properties.

    All values are strings (Iceberg requirement).
    """
    props: dict[str, str] = {
        "portolake.version": version,
        "portolake.breaking": str(breaking).lower(),
        "portolake.message": message or "",
        "portolake.changes": json.dumps(changes),
    }
    # Serialize assets
    assets_dict: dict[str, dict[str, Any]] = {}
    for name, asset in assets.items():
        asset_data: dict[str, Any] = {
            "sha256": asset.sha256,
            "size_bytes": asset.size_bytes,
            "href": asset.href,
        }
        if asset.source_path is not None:
            asset_data["source_path"] = asset.source_path
        if asset.source_mtime is not None:
            asset_data["source_mtime"] = asset.source_mtime
        if asset.mtime is not None:
            asset_data["mtime"] = asset.mtime
        assets_dict[name] = asset_data
    props["portolake.assets"] = json.dumps(assets_dict)
    # Serialize schema
    if schema:
        props["portolake.schema"] = json.dumps(
            {"type": schema.type, "fingerprint": schema.fingerprint}
        )
    else:
        props["portolake.schema"] = ""
    return props


def snapshot_to_version(snapshot: Snapshot) -> Version:
    """Convert an Iceberg Snapshot to a portolan Version.

    Extracts portolake.* properties from the snapshot summary and
    deserializes them into a Version dataclass.
    """
    if snapshot.summary is None:
        raise ValueError("Snapshot has no summary metadata")
    props = snapshot.summary.additional_properties

    version_str = props["portolake.version"]
    breaking = props.get("portolake.breaking", "false") == "true"
    message = props.get("portolake.message", "") or None
    created = datetime.fromtimestamp(snapshot.timestamp_ms / 1000, tz=timezone.utc)

    # Deserialize assets
    assets_json = props.get("portolake.assets", "{}")
    assets_data = json.loads(assets_json)
    assets: dict[str, Asset] = {}
    for name, adata in assets_data.items():
        assets[name] = Asset(
            sha256=adata["sha256"],
            size_bytes=adata["size_bytes"],
            href=adata["href"],
            source_path=adata.get("source_path"),
            source_mtime=adata.get("source_mtime"),
            mtime=adata.get("mtime"),
        )

    # Deserialize schema
    schema_json = props.get("portolake.schema", "")
    schema: SchemaInfo | None = None
    if schema_json:
        sdata = json.loads(schema_json)
        schema = SchemaInfo(type=sdata["type"], fingerprint=sdata["fingerprint"])

    # Deserialize changes
    changes_json = props.get("portolake.changes", "[]")
    changes: list[str] = json.loads(changes_json)

    return Version(
        version=version_str,
        created=created,
        breaking=breaking,
        assets=assets,
        changes=changes,
        schema=schema,
        message=message,
    )


def build_assets(
    assets_input: dict[str, str], collection: str
) -> tuple[dict[str, Asset], list[str]]:
    """Build Asset objects from {name: path} mapping.

    Produces catalog-root-relative hrefs as ``{collection}/{name}``,
    matching the convention used by JsonFileBackend.

    For local files, computes SHA-256 checksum and file size.
    For remote paths (non-existent locally), uses empty checksum and zero size.

    Returns:
        Tuple of (asset_dict, changes_list).
    """
    asset_objects: dict[str, Asset] = {}
    changes: list[str] = []
    for name, path_str in assets_input.items():
        asset_path = Path(path_str)
        if asset_path.exists():
            sha256 = _compute_sha256(asset_path)
            size_bytes = asset_path.stat().st_size
        else:
            sha256 = ""
            size_bytes = 0
        asset_objects[name] = Asset(
            sha256=sha256,
            size_bytes=size_bytes,
            href=f"{collection}/{name}",
        )
        changes.append(name)
    return asset_objects, changes


def _compute_sha256(path: Path) -> str:
    """Compute SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
