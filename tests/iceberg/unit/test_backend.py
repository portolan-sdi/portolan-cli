"""``IcebergBackend`` on the fake catalog and the fake DuckDB connection.

Each test runs the real backend code. The catalog is ``FakeCatalog``, which
applies the REST commits the backend sends, and DuckDB is
``FakeConnection``, which turns the backend's SQL into snapshots on that
catalog. The tests assert the SQL and the commit bodies, which is what a
real catalog receives.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

from portolan_cli.backends.iceberg.backend import (
    LegacyTableError,
    SchemaChangeError,
)
from portolan_cli.backends.iceberg.rest import CommitConflictError
from portolan_cli.backends.iceberg.versioning import property_key, tag_name
from portolan_cli.backends.protocol import VersioningBackend
from tests.iceberg.fake_lake import POINTS, SCHEMA, Lake

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit


def _guard(main_at: int) -> list[dict[str, Any]]:
    """The requirements of a commit on the roads table, guarded on ``main``."""
    return [
        {"type": "assert-table-uuid", "uuid": "uuid-portolake-roads"},
        {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": main_at},
    ]


# --- construction ---


def test_the_backend_satisfies_the_protocol(lake: Lake) -> None:
    assert isinstance(lake.backend, VersioningBackend)


def test_construction_makes_no_request_and_runs_no_sql(lake: Lake) -> None:
    assert lake.catalog.requests == []
    assert lake.connection.statements == []


def test_an_empty_or_traversing_collection_name_is_refused(lake: Lake) -> None:
    with pytest.raises(ValueError, match="empty"):
        lake.backend.list_versions("")
    with pytest.raises(ValueError, match="Invalid"):
        lake.backend.list_versions("../other")


# --- reads on a missing table ---


def test_list_versions_is_empty_without_a_table(lake: Lake) -> None:
    assert lake.backend.list_versions("roads") == []


def test_get_current_version_raises_without_a_table(lake: Lake) -> None:
    with pytest.raises(FileNotFoundError, match="roads"):
        lake.backend.get_current_version("roads")


# --- the first publish ---


def test_the_first_publish_creates_a_v3_table_from_the_file_schema(lake: Lake) -> None:
    version = lake.publish()

    assert version.version == "1.0.0"
    assert version.message == "first"
    assert version.changes == ["roads.parquet"]
    assert version.assets["roads.parquet"].href == "roads/roads.parquet"
    assert lake.sql[0].startswith("DESCRIBE SELECT * FROM read_parquet([")
    assert lake.sql[1] == (
        'CREATE TABLE lake."portolake"."roads" ("id" BIGINT, "name" VARCHAR, '
        "\"geometry\" GEOMETRY('OGC:CRS84')) WITH ('format-version' = 3)"
    )
    assert lake.metadata()["format-version"] == 3
    assert lake.catalog.columns("portolake", "roads") == [
        ("id", "long"),
        ("name", "string"),
        ("geometry", "geometry(OGC:CRS84)"),
    ]


def test_the_first_publish_inserts_without_a_truncate(lake: Lake) -> None:
    path = lake.source("roads", "roads.parquet")

    lake.publish(assets={"roads.parquet": path})

    writes = [s for s in lake.sql if s.startswith(("BEGIN", "TRUNCATE", "INSERT", "COMMIT"))]
    assert writes == [
        "BEGIN",
        f'INSERT INTO lake."portolake"."roads" BY NAME SELECT * FROM read_parquet([\'{path}\'], '
        "union_by_name = true, hive_partitioning = false)",
        "COMMIT",
    ]


def test_the_version_record_is_one_commit_after_the_rows(lake: Lake) -> None:
    """The property and the tag land together, guarded on the snapshot DuckDB made."""
    version = lake.publish()

    metadata = lake.metadata()
    snapshot_id = metadata["current-snapshot-id"]
    assert lake.commits[-1] == {
        "requirements": _guard(snapshot_id),
        "updates": [
            {
                "action": "set-properties",
                "updates": {property_key("1.0.0"): metadata["properties"][property_key("1.0.0")]},
            },
            {
                "action": "set-snapshot-ref",
                "ref-name": tag_name("1.0.0"),
                "type": "tag",
                "snapshot-id": snapshot_id,
            },
        ],
    }
    record = json.loads(metadata["properties"][property_key("1.0.0")])
    assert record["assets"]["roads.parquet"]["href"] == "roads/roads.parquet"
    assert version.created.timestamp() * 1000 == metadata["snapshots"][-1]["timestamp-ms"]


def test_the_namespace_is_created_once(lake: Lake) -> None:
    lake.publish()
    lake.publish(assets={"more.parquet": lake.source("roads", "more.parquet")}, message="second")

    creates = [
        r
        for r in lake.catalog.requests
        if r.method == "POST" and r.url.path.endswith("/namespaces")
    ]
    assert len(creates) == 1


def test_a_publish_with_no_parquet_on_a_new_table_is_refused(lake: Lake) -> None:
    path = lake.root / "roads" / "thumb.png"
    path.parent.mkdir()
    path.write_bytes(b"png")

    with pytest.raises(ValueError, match="no Parquet data asset"):
        lake.backend.publish("roads", {"thumb.png": str(path)}, SCHEMA, False, "m")

    assert lake.sql == []


def _two_files(lake: Lake) -> tuple[str, str]:
    """Publish ``roads.parquet`` as 1.0.0 and return it with a second, unpublished file."""
    first = lake.source("roads", "roads.parquet")
    lake.publish(assets={"roads.parquet": first})
    return first, lake.source("roads", "more.parquet")


# --- later publishes ---


def test_a_later_publish_replaces_the_rows_with_the_merged_asset_set(lake: Lake) -> None:
    first, second = _two_files(lake)

    version = lake.publish(assets={"more.parquet": second}, message="second")

    assert version.version == "1.1.0"
    assert sorted(version.assets) == ["more.parquet", "roads.parquet"]
    writes = [s for s in lake.sql if s.startswith(("TRUNCATE", "INSERT"))][-2:]
    assert writes[0] == 'TRUNCATE lake."portolake"."roads"'
    assert f"'{first}', '{second}'" in writes[1]
    assert [v.version for v in lake.backend.list_versions("roads")] == ["1.0.0", "1.1.0"]


def test_a_breaking_publish_bumps_the_major(lake: Lake) -> None:
    lake.publish()

    version = lake.publish(breaking=True, message="breaking")

    assert version.version == "2.0.0"
    assert version.breaking is True


def test_an_explicit_version_is_used_and_a_duplicate_is_refused(lake: Lake) -> None:
    assert lake.publish(version="3.1.0").version == "3.1.0"

    with pytest.raises(ValueError, match="3.1.0"):
        lake.publish(version="3.1.0")


def test_removed_assets_leave_the_record_and_the_rows(lake: Lake) -> None:
    first, second = _two_files(lake)

    version = lake.publish(
        assets={"more.parquet": second}, removed={"roads.parquet"}, message="replace"
    )

    assert list(version.assets) == ["more.parquet"]
    insert = [s for s in lake.sql if s.startswith("INSERT")][-1]
    assert first not in insert
    assert second in insert


def test_a_publish_that_only_removes_truncates_and_tags_the_delete_snapshot(lake: Lake) -> None:
    lake.publish()

    version = lake.backend.publish("roads", {}, SCHEMA, False, "drop", removed={"roads.parquet"})

    assert version.version == "1.1.0"
    assert version.assets == {}
    writes = [s for s in lake.sql if s.startswith(("TRUNCATE", "INSERT"))][-1:]
    assert writes == ['TRUNCATE lake."portolake"."roads"']
    assert lake.metadata()["snapshots"][-1]["summary"]["operation"] == "delete"
    assert (
        lake.metadata()["refs"]["v1.1.0"]["snapshot-id"] == lake.metadata()["current-snapshot-id"]
    )


def test_a_carried_over_asset_must_exist_locally(lake: Lake) -> None:
    first = lake.source("roads", "roads.parquet")
    lake.publish(assets={"roads.parquet": first})
    (lake.root / "roads" / "roads.parquet").unlink()
    second = lake.source("roads", "more.parquet")

    with pytest.raises(ValueError, match="roads/roads.parquet"):
        lake.publish(assets={"more.parquet": second})


def test_mirrors_and_companions_are_not_table_rows(lake: Lake) -> None:
    data = lake.source("roads", "roads.parquet")
    mirror = lake.source("roads", "items.parquet", columns=[("id", "VARCHAR")])
    thumb = lake.root / "roads" / "roads.thumb.jpg"
    thumb.write_bytes(b"jpg")

    version = lake.publish(
        assets={"roads.parquet": data, "items.parquet": mirror, "roads.thumb.jpg": str(thumb)}
    )

    insert = [s for s in lake.sql if s.startswith("INSERT")][-1]
    assert data in insert
    assert mirror not in insert
    assert "thumb" not in insert
    assert sorted(version.assets) == ["items.parquet", "roads.parquet", "roads.thumb.jpg"]


# --- schema evolution ---


def test_a_new_column_is_added_before_the_rows(lake: Lake) -> None:
    lake.publish()
    wider = lake.source("roads", "wider.parquet", columns=[*POINTS, ("lanes", "INTEGER")])

    lake.publish(assets={"wider.parquet": wider})

    assert 'ALTER TABLE lake."portolake"."roads" ADD COLUMN "lanes" INTEGER' in lake.sql
    assert lake.catalog.columns("portolake", "roads")[-1] == ("lanes", "int")


def test_a_changed_column_type_refuses_the_publish_before_any_write(lake: Lake) -> None:
    lake.publish()
    changed = lake.source("roads", "changed.parquet", columns=[("id", "VARCHAR"), *POINTS[1:]])
    snapshots = len(lake.metadata()["snapshots"])

    with pytest.raises(SchemaChangeError, match="id"):
        lake.publish(assets={"changed.parquet": changed}, removed={"roads.parquet"})

    assert len(lake.metadata()["snapshots"]) == snapshots
    assert not any(s.startswith(("TRUNCATE", "INSERT")) for s in lake.sql[-3:])


# --- partitions ---


def test_partition_keys_in_collection_json_make_an_identity_spec_and_a_hive_read(
    lake: Lake,
) -> None:
    (lake.root / "roads").mkdir()
    (lake.root / "roads" / "collection.json").write_text(
        json.dumps({"partition:keys": [{"name": "region", "type": "string"}]}), encoding="utf-8"
    )
    path = lake.source("roads", "region=a/part.parquet", columns=[*POINTS, ("region", "VARCHAR")])

    lake.publish(assets={"region=a/part.parquet": path})

    assert "hive_partitioning = true" in lake.sql[0]
    source_id = next(
        f["id"] for f in lake.metadata()["schemas"][-1]["fields"] if f["name"] == "region"
    )
    spec_commit = lake.commits[0]
    assert spec_commit["updates"] == [
        {
            "action": "add-spec",
            "spec": {
                "spec-id": 1,
                "fields": [
                    {
                        "name": "region",
                        "transform": "identity",
                        "source-id": source_id,
                        "field-id": 1000,
                    }
                ],
            },
        },
        {"action": "set-default-spec", "spec-id": -1},
        {"action": "set-properties", "updates": {"portolan.record": "1"}},
    ]
    assert lake.metadata()["default-spec-id"] == 1


def test_an_unpartitioned_collection_gets_only_the_record_marker(lake: Lake) -> None:
    lake.publish()

    assert lake.commits[0]["updates"] == [
        {"action": "set-properties", "updates": {"portolan.record": "1"}}
    ]


# --- concurrency ---


def test_a_main_that_moved_between_the_rows_and_the_record_is_a_conflict(lake: Lake) -> None:
    lake.publish()
    original = lake.catalog.add_snapshot

    def racing(ns: str, name: str, operation: str) -> int:
        snapshot_id = original(ns, name, operation)
        if operation == "append":
            original(ns, name, "append")  # another writer lands right after
        return snapshot_id

    lake.catalog.add_snapshot = racing  # type: ignore[method-assign, assignment]

    with pytest.raises(CommitConflictError):
        lake.publish(message="second")


# --- current and rollback ---


def test_the_current_version_follows_main(lake: Lake) -> None:
    lake.publish()
    lake.publish(message="second")

    assert lake.backend.get_current_version("roads").version == "1.1.0"


def test_rollback_moves_main_to_the_tagged_snapshot(lake: Lake) -> None:
    lake.publish()
    lake.publish(message="second")
    before = lake.metadata()["current-snapshot-id"]
    target = lake.metadata()["refs"]["v1.0.0"]["snapshot-id"]

    restored = lake.backend.rollback("roads", "1.0.0")

    assert restored.version == "1.0.0"
    assert lake.commits[-1] == {
        "requirements": _guard(before),
        "updates": [
            {
                "action": "set-snapshot-ref",
                "ref-name": "main",
                "type": "branch",
                "snapshot-id": target,
            }
        ],
    }
    assert lake.backend.get_current_version("roads").version == "1.0.0"
    assert [v.version for v in lake.backend.list_versions("roads")] == ["1.0.0", "1.1.0"]


def test_a_publish_after_a_rollback_bumps_from_the_newest_version(lake: Lake) -> None:
    lake.publish()
    lake.publish(message="second")
    lake.backend.rollback("roads", "1.0.0")

    version = lake.publish(message="third")

    assert version.version == "1.2.0"
    assert sorted(version.assets) == ["roads.parquet"]


def test_rollback_to_an_unknown_version_raises(lake: Lake) -> None:
    lake.publish()

    with pytest.raises(ValueError, match="9.9.9"):
        lake.backend.rollback("roads", "9.9.9")


def test_rollback_without_a_table_raises_file_not_found(lake: Lake) -> None:
    with pytest.raises(FileNotFoundError):
        lake.backend.rollback("roads", "1.0.0")


def test_an_untagged_main_reports_the_newest_version(lake: Lake) -> None:
    """A snapshot another writer made is current, but it is not a Portolan version."""
    lake.publish()
    lake.catalog.add_snapshot("portolake", "roads", "append")

    assert lake.backend.get_current_version("roads").version == "1.0.0"


# --- prune ---


def _three_versions(lake: Lake) -> None:
    lake.publish()
    lake.publish(message="second")
    lake.publish(message="third")


def test_prune_dry_run_names_the_victims_and_commits_nothing(lake: Lake) -> None:
    _three_versions(lake)
    commits = len(lake.commits)

    pruned = lake.backend.prune("roads", keep=1, dry_run=True)

    assert [v.version for v in pruned] == ["1.0.0", "1.1.0"]
    assert len(lake.commits) == commits
    assert len(lake.backend.list_versions("roads")) == 3


def test_prune_removes_the_tags_the_records_and_the_snapshots(lake: Lake) -> None:
    _three_versions(lake)
    metadata = lake.metadata()
    newest = metadata["refs"]["v1.2.0"]["snapshot-id"]
    newest_ms = next(s["timestamp-ms"] for s in metadata["snapshots"] if s["snapshot-id"] == newest)
    # The two tagged victims and the two untagged delete snapshots before them.
    victims = [s["snapshot-id"] for s in metadata["snapshots"] if s["timestamp-ms"] < newest_ms]
    assert len(victims) == 4

    pruned = lake.backend.prune("roads", keep=1, dry_run=False)

    assert [v.version for v in pruned] == ["1.0.0", "1.1.0"]
    assert lake.commits[-1]["updates"] == [
        {"action": "remove-snapshot-ref", "ref-name": "v1.0.0"},
        {"action": "remove-snapshot-ref", "ref-name": "v1.1.0"},
        {"action": "remove-properties", "removals": [property_key("1.0.0"), property_key("1.1.0")]},
        {"action": "remove-snapshots", "snapshot-ids": victims},
    ]
    assert [v.version for v in lake.backend.list_versions("roads")] == ["1.2.0"]
    assert {s["snapshot-id"] for s in lake.metadata()["snapshots"]}.isdisjoint(victims)


def test_prune_keeps_the_current_version_even_when_it_is_old(lake: Lake) -> None:
    _three_versions(lake)
    lake.backend.rollback("roads", "1.0.0")

    pruned = lake.backend.prune("roads", keep=1, dry_run=False)

    assert [v.version for v in pruned] == ["1.1.0"]
    assert [v.version for v in lake.backend.list_versions("roads")] == ["1.0.0", "1.2.0"]


def test_prune_with_nothing_beyond_keep_returns_nothing(lake: Lake) -> None:
    _three_versions(lake)

    assert lake.backend.prune("roads", keep=3, dry_run=False) == []
    assert lake.backend.prune("missing", keep=1, dry_run=False) == []


def test_prune_deletes_the_data_files_only_the_victims_reach(
    lake: Lake, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Merge-on-read keeps an old data file live until something deletes it."""
    _three_versions(lake)
    metadata = lake.metadata()
    first = metadata["refs"]["v1.0.0"]["snapshot-id"]
    second = metadata["refs"]["v1.1.0"]["snapshot-id"]
    third = metadata["refs"]["v1.2.0"]["snapshot-id"]
    shared = "s3://warehouse/portolake/roads/data/shared.parquet"
    gone = "s3://warehouse/portolake/roads/data/gone.parquet"
    deletes = "s3://warehouse/portolake/roads/data/gone-deletes.puffin"
    lake.connection.files[("portolake", "roads", first)] = [("DATA", "ADDED", "EXISTING", gone)]
    lake.connection.files[("portolake", "roads", second)] = [
        ("DATA", "EXISTING", "EXISTING", gone),
        ("DELETE", "ADDED", "POSITION_DELETES", deletes),
        ("DATA", "ADDED", "EXISTING", shared),
    ]
    lake.connection.files[("portolake", "roads", third)] = [
        ("DATA", "EXISTING", "EXISTING", shared)
    ]
    deleted: list[str] = []
    monkeypatch.setattr("portolan_cli.api.delete_file", lambda url, **kwargs: deleted.append(url))

    lake.backend.prune("roads", keep=1, dry_run=False)

    assert sorted(deleted) == sorted([gone, deletes])


def test_prune_never_deletes_a_file_outside_the_table_location(
    lake: Lake, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The manifests name the files. A path outside the table is skipped, not deleted."""
    _three_versions(lake)
    first = lake.metadata()["refs"]["v1.0.0"]["snapshot-id"]
    inside = "s3://warehouse/portolake/roads/data/gone.parquet"
    elsewhere = "s3://warehouse/portolake/other/data/theirs.parquet"
    traversal = "s3://warehouse/portolake/roads/../other/data/theirs.parquet"
    lake.connection.files[("portolake", "roads", first)] = [
        ("DATA", "ADDED", "EXISTING", inside),
        ("DATA", "ADDED", "EXISTING", elsewhere),
        ("DATA", "ADDED", "EXISTING", traversal),
    ]
    deleted: list[str] = []
    monkeypatch.setattr("portolan_cli.api.delete_file", lambda url, **kwargs: deleted.append(url))

    lake.backend.prune("roads", keep=2, dry_run=False)

    assert deleted == [inside]
    assert elsewhere in caplog.text
    assert traversal in caplog.text


def test_prune_deletes_nothing_when_the_catalog_moves_the_table_location(
    lake: Lake, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The location comes from the catalog too. One that is not <warehouse>/ns/name is refused."""
    _three_versions(lake)
    lake.metadata()["location"] = "s3://warehouse"
    first = lake.metadata()["refs"]["v1.0.0"]["snapshot-id"]
    lake.connection.files[("portolake", "roads", first)] = [
        ("DATA", "ADDED", "EXISTING", "s3://warehouse/other/data/theirs.parquet")
    ]
    deleted: list[str] = []
    monkeypatch.setattr("portolan_cli.api.delete_file", lambda url, **kwargs: deleted.append(url))

    lake.backend.prune("roads", keep=2, dry_run=False)

    assert deleted == []
    assert "deleted no file" in caplog.text


def test_a_record_href_that_leaves_the_catalog_is_refused(lake: Lake, tmp_path: Path) -> None:
    """The record is catalog data. An href with .. must not read a file outside the catalog."""
    lake.publish()
    record = json.loads(lake.metadata()["properties"][property_key("1.0.0")])
    record["assets"]["roads.parquet"]["href"] = "../../outside.parquet"
    lake.metadata()["properties"][property_key("1.0.0")] = json.dumps(record)
    (tmp_path / "outside.parquet").write_bytes(b"x")

    with pytest.raises(ValueError, match="does not stay inside the catalog"):
        lake.publish(assets={}, message="second")

    with pytest.raises(ValueError, match="does not stay inside the catalog"):
        lake.backend.pull(remote_url="gs://b/c", local_root=tmp_path / "pulled", collection="roads")


def test_a_failed_file_delete_does_not_fail_the_prune(
    lake: Lake, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _three_versions(lake)
    first = lake.metadata()["refs"]["v1.0.0"]["snapshot-id"]
    lake.connection.files[("portolake", "roads", first)] = [
        ("DATA", "ADDED", "EXISTING", "s3://warehouse/portolake/roads/data/gone.parquet")
    ]

    def refuse(url: str, **kwargs: Any) -> None:
        raise OSError("access denied")

    monkeypatch.setattr("portolan_cli.api.delete_file", refuse)

    pruned = lake.backend.prune("roads", keep=2, dry_run=False)

    assert [v.version for v in pruned] == ["1.0.0"]
    assert "gone.parquet" in caplog.text


# --- legacy tables ---


def _legacy_table(lake: Lake) -> None:
    lake.catalog.create_table(
        "portolake", "old", [("id", "long"), ("geometry", "binary")], format_version=2
    )
    snapshot_id = lake.catalog.add_snapshot("portolake", "old", "append")
    summary = lake.metadata("old")["snapshots"][-1]["summary"]
    summary["portolake.version"] = "1.0.0"
    summary["portolake.assets"] = json.dumps(
        {"old.parquet": {"sha256": "a", "size_bytes": 1, "href": "old/old.parquet"}}
    )
    assert snapshot_id


def test_a_legacy_table_is_listed_and_current_but_refuses_writes(lake: Lake) -> None:
    _legacy_table(lake)

    assert [v.version for v in lake.backend.list_versions("old")] == ["1.0.0"]
    assert lake.backend.get_current_version("old").assets["old.parquet"].href == "old/old.parquet"
    with pytest.raises(LegacyTableError, match="read-only"):
        lake.publish("old")
    with pytest.raises(LegacyTableError):
        lake.backend.rollback("old", "1.0.0")
    with pytest.raises(LegacyTableError):
        lake.backend.prune("old", keep=1, dry_run=False)


# --- drift and push ---


def test_check_drift_reports_the_current_version(lake: Lake) -> None:
    lake.publish()

    report = lake.backend.check_drift("roads")

    assert report["has_drift"] is False
    assert report["local_version"] == "1.0.0"
    assert lake.backend.check_drift("missing")["local_version"] is None


def test_push_is_blocked_with_a_message_for_each_case(lake: Lake) -> None:
    assert lake.backend.supports_push() is False
    assert "add" in lake.backend.push_blocked_message("gs://bucket/c").lower()
    assert "not supported" in lake.backend.push_blocked_message(None).lower()
