"""The version record of a collection: one table property and one tag per version.

A version ``1.2.0`` is the table property ``portolan.version.1.2.0``, whose
value is the JSON record, and the tag ``v1.2.0`` on the snapshot that
holds its rows. The helpers here read both from a ``loadTable`` metadata
document and build the record a publish writes.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest

from portolan_cli.backends.iceberg.versioning import (
    build_assets,
    compute_next_version,
    current_version_from_metadata,
    is_legacy_table,
    legacy_versions_from_metadata,
    property_key,
    snapshot_id_of,
    tag_name,
    version_from_payload,
    version_of_tag,
    version_payload,
    versions_from_metadata,
)
from portolan_cli.versions import Asset, SchemaInfo

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit

T0 = int(datetime(2026, 6, 15, 12, 0, 0, tzinfo=UTC).timestamp() * 1000)


def _metadata(
    *,
    versions: dict[str, int],
    current: int | None,
    extra_properties: dict[str, str] | None = None,
    snapshots: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """A metadata document with one tag and one property per version."""
    refs: dict[str, Any] = {
        tag_name(v): {"snapshot-id": sid, "type": "tag"} for v, sid in versions.items()
    }
    if current is not None:
        refs["main"] = {"snapshot-id": current, "type": "branch"}
    properties = {
        property_key(v): version_payload(v, False, f"message {v}", {}, None, []) for v in versions
    }
    properties.update(extra_properties or {})
    if snapshots is None:
        snapshots = [
            {"snapshot-id": sid, "timestamp-ms": T0 + 1000 * i, "summary": {"operation": "append"}}
            for i, sid in enumerate(sorted(set(versions.values())))
        ]
    return {
        "format-version": 3,
        "current-snapshot-id": current if current is not None else -1,
        "refs": refs,
        "properties": properties,
        "snapshots": snapshots,
    }


# --- names ---


def test_property_and_tag_names() -> None:
    assert property_key("1.2.0") == "portolan.version.1.2.0"
    assert tag_name("1.2.0") == "v1.2.0"


@pytest.mark.parametrize(
    ("ref", "version"),
    [("v1.2.0", "1.2.0"), ("v10.0.3", "10.0.3"), ("main", None), ("v1.2", None), ("release", None)],
)
def test_version_of_tag_accepts_only_a_semver_tag(ref: str, version: str | None) -> None:
    assert version_of_tag(ref) == version


# --- payload ---


def test_payload_round_trips_every_asset_field() -> None:
    assets = {
        "data.parquet": Asset(sha256="abc", size_bytes=1024, href="c/data.parquet"),
        "items.parquet": Asset(
            sha256="a",
            size_bytes=1,
            href="c/items.parquet",
            source_path="src.geojson",
            source_mtime=1.5,
            mtime=2.5,
            feature_count=3,
            schema_fingerprint="fp",
            source_fingerprint="ab12",
        ),
    }
    schema = SchemaInfo(type="geoparquet", fingerprint={"columns": ["id"]})
    payload = version_payload("2.1.0", True, "Breaking", assets, schema, ["data.parquet"])
    created = datetime(2026, 6, 15, tzinfo=UTC)

    version = version_from_payload("2.1.0", payload, created)

    assert version.version == "2.1.0"
    assert version.created == created
    assert version.breaking is True
    assert version.message == "Breaking"
    assert version.assets == assets
    assert version.schema == schema
    assert version.changes == ["data.parquet"]


def test_payload_is_one_json_document_with_no_version_inside() -> None:
    """The property name carries the version, so the value does not repeat it."""
    payload = version_payload("1.0.0", False, "", {}, None, [])

    assert json.loads(payload) == {"breaking": False, "message": "", "assets": {}, "changes": []}


def test_an_empty_message_reads_back_as_none() -> None:
    version = version_from_payload("1.0.0", version_payload("1.0.0", False, "", {}, None, []), None)

    assert version.message is None
    assert version.schema is None


# --- versions_from_metadata ---


def test_versions_are_ordered_by_semver_not_by_text() -> None:
    metadata = _metadata(versions={"1.9.0": 19, "1.10.0": 110, "2.0.0": 200}, current=200)

    assert [v.version for v in versions_from_metadata(metadata)] == ["1.9.0", "1.10.0", "2.0.0"]


def test_created_comes_from_the_tagged_snapshot() -> None:
    metadata = _metadata(versions={"1.0.0": 19, "1.1.0": 110}, current=110)

    first, second = versions_from_metadata(metadata)

    assert first.created == datetime.fromtimestamp(T0 / 1000, tz=UTC)
    assert second.created == datetime.fromtimestamp((T0 + 1000) / 1000, tz=UTC)
    assert first.message == "message 1.0.0"


def test_a_tag_without_a_property_is_not_a_version() -> None:
    metadata = _metadata(versions={"1.0.0": 19}, current=19)
    metadata["refs"]["v9.9.9"] = {"snapshot-id": 19, "type": "tag"}

    assert [v.version for v in versions_from_metadata(metadata)] == ["1.0.0"]


def test_a_property_without_a_tag_is_not_a_version() -> None:
    metadata = _metadata(versions={"1.0.0": 19}, current=19)
    metadata["properties"][property_key("1.1.0")] = version_payload(
        "1.1.0", False, "", {}, None, []
    )

    assert [v.version for v in versions_from_metadata(metadata)] == ["1.0.0"]


def test_a_branch_named_like_a_version_is_not_a_tag() -> None:
    metadata = _metadata(versions={"1.0.0": 19}, current=19)
    metadata["refs"]["v1.1.0"] = {"snapshot-id": 19, "type": "branch"}
    metadata["properties"][property_key("1.1.0")] = version_payload(
        "1.1.0", False, "", {}, None, []
    )

    assert [v.version for v in versions_from_metadata(metadata)] == ["1.0.0"]


def test_an_unrelated_property_is_ignored() -> None:
    metadata = _metadata(
        versions={"1.0.0": 19}, current=19, extra_properties={"write.parquet.compression": "zstd"}
    )

    assert len(versions_from_metadata(metadata)) == 1


def test_snapshot_id_of_a_version() -> None:
    metadata = _metadata(versions={"1.0.0": 19, "1.1.0": 110}, current=110)

    assert snapshot_id_of(metadata, "1.0.0") == 19
    assert snapshot_id_of(metadata, "3.0.0") is None


# --- current_version_from_metadata ---


def test_current_is_the_version_tagged_at_main() -> None:
    """After a rollback, main points at the older snapshot, so that version is current."""
    metadata = _metadata(versions={"1.0.0": 19, "1.1.0": 110}, current=19)

    current = current_version_from_metadata(metadata)

    assert current is not None
    assert current.version == "1.0.0"


def test_current_is_none_when_main_is_untagged() -> None:
    """A snapshot another writer made is not a Portolan version."""
    metadata = _metadata(versions={"1.0.0": 19}, current=77)
    metadata["snapshots"].append({"snapshot-id": 77, "timestamp-ms": T0 + 5000, "summary": {}})

    assert current_version_from_metadata(metadata) is None


def test_current_is_none_for_a_table_with_no_snapshot() -> None:
    assert current_version_from_metadata(_metadata(versions={}, current=None)) is None


# --- legacy tables, written by the pyiceberg backend ---


def _legacy_metadata() -> dict[str, Any]:
    summary = {
        "operation": "append",
        "portolake.version": "1.0.0",
        "portolake.breaking": "false",
        "portolake.message": "first",
        "portolake.changes": json.dumps(["data.parquet"]),
        "portolake.assets": json.dumps(
            {"data.parquet": {"sha256": "abc", "size_bytes": 10, "href": "c/data.parquet"}}
        ),
        "portolake.schema": json.dumps({"type": "geoparquet", "fingerprint": {"columns": ["id"]}}),
    }
    return {
        "format-version": 2,
        "current-snapshot-id": 5,
        "refs": {"main": {"snapshot-id": 5, "type": "branch"}},
        "properties": {},
        "snapshots": [
            {"snapshot-id": 4, "timestamp-ms": T0, "summary": {"operation": "append"}},
            {"snapshot-id": 5, "timestamp-ms": T0 + 1000, "summary": summary},
        ],
    }


def test_legacy_versions_come_from_the_snapshot_summaries() -> None:
    versions = legacy_versions_from_metadata(_legacy_metadata())

    assert [v.version for v in versions] == ["1.0.0"]
    assert versions[0].message == "first"
    assert versions[0].assets["data.parquet"].sha256 == "abc"
    assert versions[0].schema == SchemaInfo(type="geoparquet", fingerprint={"columns": ["id"]})
    assert versions[0].created == datetime.fromtimestamp((T0 + 1000) / 1000, tz=UTC)


def test_a_table_with_summary_records_and_no_property_is_legacy() -> None:
    assert is_legacy_table(_legacy_metadata()) is True


def test_a_table_with_a_version_property_is_not_legacy() -> None:
    metadata = _legacy_metadata()
    metadata["properties"][property_key("1.1.0")] = version_payload(
        "1.1.0", False, "", {}, None, []
    )
    metadata["refs"]["v1.1.0"] = {"snapshot-id": 5, "type": "tag"}

    assert is_legacy_table(metadata) is False


def test_an_empty_table_is_not_legacy() -> None:
    assert is_legacy_table(_metadata(versions={}, current=None)) is False


# --- compute_next_version ---


def test_compute_next_version_first_is_1_0_0() -> None:
    assert compute_next_version(None, breaking=False) == "1.0.0"
    assert compute_next_version(None, breaking=True) == "1.0.0"


def test_compute_next_version_minor_bump() -> None:
    assert compute_next_version("1.0.0", breaking=False) == "1.1.0"
    assert compute_next_version("2.5.3", breaking=False) == "2.6.0"


def test_compute_next_version_major_bump_on_breaking() -> None:
    assert compute_next_version("1.2.3", breaking=True) == "2.0.0"


# --- build_assets ---


def test_build_assets_creates_asset_objects_with_relative_hrefs(tmp_path: Path) -> None:
    test_file = tmp_path / "data.parquet"
    test_file.write_bytes(b"fake parquet content")

    assets, changes = build_assets({"data.parquet": str(test_file)}, collection="my-collection")

    assert assets["data.parquet"].size_bytes == 20
    assert assets["data.parquet"].sha256 != ""
    assert assets["data.parquet"].href == "my-collection/data.parquet"
    assert changes == ["data.parquet"]


def test_build_assets_remote_asset_with_relative_hrefs() -> None:
    assets, changes = build_assets(
        {"remote.parquet": "s3://bucket/remote.parquet"}, collection="boundaries"
    )

    assert assets["remote.parquet"].sha256 == ""
    assert assets["remote.parquet"].size_bytes == 0
    assert assets["remote.parquet"].href == "boundaries/remote.parquet"
    assert changes == ["remote.parquet"]


def test_build_assets_nested_asset_key(tmp_path: Path) -> None:
    test_file = tmp_path / "data.parquet"
    test_file.write_bytes(b"content")

    assets, _ = build_assets({"item1/data.parquet": str(test_file)}, collection="boundaries")

    assert assets["item1/data.parquet"].href == "boundaries/item1/data.parquet"
