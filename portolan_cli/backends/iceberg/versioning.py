"""The version record of a collection, as a table property and a snapshot tag.

Version ``1.2.0`` of a collection is two entries in the table metadata: the
table property ``portolan.version.1.2.0``, whose value is the JSON record
(assets, schema, message, changes), and the tag ``v1.2.0`` on the snapshot
that holds the rows of that version. A version exists only when both are
present. ``publish`` writes the two in one REST commit after DuckDB has
committed the rows, so a reader never sees one without the other.

Why not the snapshot summary, where the pyiceberg backend kept the record:
DuckDB writes the snapshot and offers no way to add a custom summary
property (duckdb/duckdb-iceberg#1249). A table property is a metadata
update any catalog accepts, and a tag is what keeps the snapshot alive. A
hosted catalog's maintenance expires an untagged snapshot after a window;
a tagged one stays, so the tag is the anchor of the version, not a
convenience. The tag also feeds ``iceberg:ref`` once the STAC extension
has the field.

A table the pyiceberg backend wrote (format version 2, records in the
snapshot summaries under ``portolake.*``) is read-only here.
:func:`legacy_versions_from_metadata` reads it, and the backend refuses to
write to it, because an in-place upgrade keeps its untyped geometry column
and buys nothing.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from portolan_cli.api import Asset, SchemaInfo, Version, compute_checksum, parse_version

#: The table property that holds the record of one version.
PROPERTY_PREFIX = "portolan.version."
#: The tag that holds the snapshot of one version.
TAG_PREFIX = "v"
#: The branch every reader follows and every publish moves.
MAIN_BRANCH = "main"

_SEMVER = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")

_OPTIONAL_ASSET_FIELDS = (
    "source_path",
    "source_mtime",
    "mtime",
    "feature_count",
    "schema_fingerprint",
    "source_fingerprint",
)


def property_key(version: str) -> str:
    """Return the table property that holds the record of ``version``."""
    return f"{PROPERTY_PREFIX}{version}"


def tag_name(version: str) -> str:
    """Return the tag that holds the snapshot of ``version``."""
    return f"{TAG_PREFIX}{version}"


def version_of_tag(ref_name: str) -> str | None:
    """Return the version a tag name carries, or None for any other ref name."""
    if not ref_name.startswith(TAG_PREFIX):
        return None
    version = ref_name[len(TAG_PREFIX) :]
    return version if _SEMVER.match(version) else None


def _semver_key(version: str) -> tuple[int, int, int]:
    major, minor, patch = parse_version(version)
    return (major, minor, patch)


def compute_next_version(current: str | None, breaking: bool) -> str:
    """Return the version after ``current``: a major bump when breaking, else a minor one."""
    if current is None:
        return "1.0.0"
    major, minor, _patch = parse_version(current)
    if breaking:
        return f"{major + 1}.0.0"
    return f"{major}.{minor + 1}.0"


# ---------------------------------------------------------------- the record


def _serialize_asset(asset: Asset) -> dict[str, Any]:
    data: dict[str, Any] = {
        "sha256": asset.sha256,
        "size_bytes": asset.size_bytes,
        "href": asset.href,
    }
    for name in _OPTIONAL_ASSET_FIELDS:
        value = getattr(asset, name)
        if value is not None:
            data[name] = value
    return data


def _asset_from(data: dict[str, Any]) -> Asset:
    return Asset(
        sha256=data["sha256"],
        size_bytes=data["size_bytes"],
        href=data["href"],
        **{name: data.get(name) for name in _OPTIONAL_ASSET_FIELDS},
    )


def _schema_from(data: dict[str, Any] | None) -> SchemaInfo | None:
    if not data:
        return None
    return SchemaInfo(type=data["type"], fingerprint=data["fingerprint"])


def version_payload(
    version: str,
    breaking: bool,
    message: str,
    assets: dict[str, Asset],
    schema: SchemaInfo | None,
    changes: list[str],
) -> str:
    """Return the JSON record of a version, the value of its table property.

    The property name carries the version, so the value does not repeat it.
    ``version`` is accepted so a caller cannot pair a record with the wrong
    name by accident, and it is checked to be a semver string.
    """
    if not _SEMVER.match(version):
        raise ValueError(f"{version!r} is not a semantic version")
    record: dict[str, Any] = {
        "breaking": breaking,
        "message": message or "",
        "assets": {name: _serialize_asset(asset) for name, asset in assets.items()},
        "changes": list(changes),
    }
    if schema is not None:
        record["schema"] = {"type": schema.type, "fingerprint": schema.fingerprint}
    return json.dumps(record, separators=(",", ":"), sort_keys=True)


def version_from_payload(version: str, payload: str, created: datetime | None) -> Version:
    """Return the ``Version`` that a property value records.

    Args:
        version: The version the property name carries.
        payload: The property value.
        created: The timestamp of the tagged snapshot, or None when unknown.
    """
    record = json.loads(payload)
    return Version(
        version=version,
        created=created or datetime.fromtimestamp(0, tz=UTC),
        breaking=bool(record.get("breaking", False)),
        assets={name: _asset_from(data) for name, data in record.get("assets", {}).items()},
        changes=list(record.get("changes", [])),
        schema=_schema_from(record.get("schema")),
        message=record.get("message") or None,
    )


# ---------------------------------------------------------------- the metadata


def _snapshot_timestamps(metadata: dict[str, Any]) -> dict[int, datetime]:
    return {
        int(snapshot["snapshot-id"]): datetime.fromtimestamp(
            int(snapshot["timestamp-ms"]) / 1000, tz=UTC
        )
        for snapshot in metadata.get("snapshots", [])
    }


def _tagged_snapshots(metadata: dict[str, Any]) -> dict[str, int]:
    """Return version -> snapshot id for every ``v<semver>`` tag."""
    tags: dict[str, int] = {}
    for name, ref in metadata.get("refs", {}).items():
        version = version_of_tag(str(name))
        if version is not None and ref.get("type") == "tag":
            tags[version] = int(ref["snapshot-id"])
    return tags


def _recorded_versions(metadata: dict[str, Any]) -> dict[str, str]:
    """Return version -> payload for every ``portolan.version.*`` property."""
    records: dict[str, str] = {}
    for key, value in metadata.get("properties", {}).items():
        if str(key).startswith(PROPERTY_PREFIX):
            version = str(key)[len(PROPERTY_PREFIX) :]
            if _SEMVER.match(version):
                records[version] = str(value)
    return records


def versions_from_metadata(metadata: dict[str, Any]) -> list[Version]:
    """Return every version of the table, oldest version number first.

    A version needs its tag and its property. A tag with no record, or a
    record with no tag, is not a version: the first is a tag someone else
    made, the second is the trace of a publish that lost its tag step.
    """
    tags = _tagged_snapshots(metadata)
    records = _recorded_versions(metadata)
    timestamps = _snapshot_timestamps(metadata)
    versions = [
        version_from_payload(version, records[version], timestamps.get(snapshot_id))
        for version, snapshot_id in tags.items()
        if version in records
    ]
    return sorted(versions, key=lambda v: _semver_key(v.version))


def snapshot_id_of(metadata: dict[str, Any], version: str) -> int | None:
    """Return the snapshot the tag of ``version`` points at, or None."""
    return _tagged_snapshots(metadata).get(version)


def current_snapshot_id(metadata: dict[str, Any]) -> int | None:
    """Return the snapshot ``main`` points at, or None for a table with no snapshot."""
    main = metadata.get("refs", {}).get(MAIN_BRANCH)
    if main is not None:
        return int(main["snapshot-id"])
    current = metadata.get("current-snapshot-id")
    if current is None or int(current) == -1:
        return None
    return int(current)


def current_version_from_metadata(metadata: dict[str, Any]) -> Version | None:
    """Return the version whose tag sits on the current snapshot, or None.

    A rollback moves ``main`` back to an older tagged snapshot, so the
    current version is read from ``main``, not from the newest tag. A
    current snapshot with no tag is one another writer made, and no
    Portolan version describes it.
    """
    current = current_snapshot_id(metadata)
    if current is None:
        return None
    for version in versions_from_metadata(metadata):
        if snapshot_id_of(metadata, version.version) == current:
            return version
    return None


# ---------------------------------------------------------------- legacy tables


_LEGACY_VERSION = "portolake.version"


def _legacy_version(snapshot: dict[str, Any]) -> Version | None:
    summary = snapshot.get("summary") or {}
    version = summary.get(_LEGACY_VERSION)
    if version is None:
        return None
    assets = json.loads(summary.get("portolake.assets", "{}") or "{}")
    schema_json = summary.get("portolake.schema", "")
    return Version(
        version=str(version),
        created=datetime.fromtimestamp(int(snapshot["timestamp-ms"]) / 1000, tz=UTC),
        breaking=summary.get("portolake.breaking", "false") == "true",
        assets={name: _asset_from(data) for name, data in assets.items()},
        changes=list(json.loads(summary.get("portolake.changes", "[]") or "[]")),
        schema=_schema_from(json.loads(schema_json) if schema_json else None),
        message=summary.get("portolake.message") or None,
    )


def legacy_versions_from_metadata(metadata: dict[str, Any]) -> list[Version]:
    """Return the versions a pyiceberg-era table records in its snapshot summaries."""
    versions = [
        version
        for version in (_legacy_version(s) for s in metadata.get("snapshots", []))
        if version is not None
    ]
    return sorted(versions, key=lambda v: v.created)


def legacy_current_version(metadata: dict[str, Any]) -> Version | None:
    """Return the legacy version of the current snapshot, or None."""
    current = current_snapshot_id(metadata)
    for snapshot in metadata.get("snapshots", []):
        if int(snapshot["snapshot-id"]) == current:
            return _legacy_version(snapshot)
    return None


def is_legacy_table(metadata: dict[str, Any]) -> bool:
    """Whether the table holds pyiceberg-era records and no property record.

    Such a table is read-only for this backend.
    """
    if _recorded_versions(metadata):
        return False
    return any(
        (snapshot.get("summary") or {}).get(_LEGACY_VERSION) is not None
        for snapshot in metadata.get("snapshots", [])
    )


# ---------------------------------------------------------------- assets


def build_assets(
    assets_input: dict[str, str], collection: str
) -> tuple[dict[str, Asset], list[str]]:
    """Build Asset objects from ``{name: path}``.

    The href is ``{collection}/{name}``, relative to the catalog root, as the
    file backend records it. A local file gets its SHA-256 and size. A path
    that does not exist locally gets an empty checksum and size zero.

    Returns:
        The assets by name, and the list of names, in input order.
    """
    asset_objects: dict[str, Asset] = {}
    changes: list[str] = []
    for name, path_str in assets_input.items():
        asset_path = Path(path_str)
        if asset_path.exists():
            sha256 = compute_checksum(asset_path)
            size_bytes = asset_path.stat().st_size
        else:
            sha256 = ""
            size_bytes = 0
        asset_objects[name] = Asset(
            sha256=sha256, size_bytes=size_bytes, href=f"{collection}/{name}"
        )
        changes.append(name)
    return asset_objects, changes
