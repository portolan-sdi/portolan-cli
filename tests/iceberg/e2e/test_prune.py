"""Prune on the stack: the catalog forgets the versions, and only unreachable files go.

DuckDB's writes are merge-on-read: a truncate writes a deletion vector and
keeps the old data file, so the current snapshot still reaches the data
files of older versions. Prune removes the version records, the tags, the
tagged snapshots, and the untagged delete snapshots before the oldest kept
version. Then it deletes the files that no remaining snapshot reaches. A
file the current snapshot still reaches, through a deletion vector, stays.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import duckdb
import pytest

from portolan_cli.backends.iceberg.versioning import property_key, tag_name
from tests.iceberg.e2e.conftest import publish_rows

if TYPE_CHECKING:
    from pathlib import Path

    from portolan_cli.backends.iceberg.backend import IcebergBackend
    from tests.iceberg_stack import IcebergStack

pytestmark = pytest.mark.e2e


def _three_versions(tmp_path: Path, collection: str) -> IcebergBackend:
    for rows in (1, 2, 3):
        backend = publish_rows(tmp_path, collection, rows, message=f"{rows}")
    return backend


def _reachable(stack: IcebergStack, collection: str, snapshot_ids: list[int]) -> set[str]:
    """Every file the given snapshots list, through DuckDB's ``iceberg_metadata``."""
    lane = stack.lane
    location = stack.load_table(collection).metadata_location
    connection = duckdb.connect()
    try:
        connection.execute("LOAD httpfs")
        connection.execute("LOAD iceberg")
        connection.execute(
            "CREATE SECRET lane (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, URL_STYLE 'path', "
            "USE_SSL ?, REGION 'us-east-1')",
            [lane.key, lane.secret, lane.s3_host, lane.s3_use_ssl],
        )
        files: set[str] = set()
        for snapshot_id in snapshot_ids:
            rows = connection.execute(
                f"SELECT file_path FROM iceberg_metadata(?, snapshot_from_id => {int(snapshot_id)})",
                [location],
            ).fetchall()
            files |= {str(row[0]) for row in rows}
        return files
    finally:
        connection.close()


def test_a_dry_run_changes_nothing(iceberg_stack: IcebergStack, tmp_path: Path) -> None:
    backend = _three_versions(tmp_path, "dry")
    before = iceberg_stack.metadata("dry")

    pruned = backend.prune("dry", keep=1, dry_run=True)

    assert [v.version for v in pruned] == ["1.0.0", "1.1.0"]
    assert iceberg_stack.metadata("dry") == before


def test_prune_forgets_the_versions_and_keeps_the_current_rows(
    iceberg_stack: IcebergStack, tmp_path: Path
) -> None:
    backend = _three_versions(tmp_path, "prune")
    before = iceberg_stack.metadata("prune")
    newest = before["refs"][tag_name("1.2.0")]["snapshot-id"]
    old_snapshots = [s["snapshot-id"] for s in before["snapshots"] if s["snapshot-id"] != newest]
    objects_before = set(iceberg_stack.objects("prune"))

    pruned = backend.prune("prune", keep=1, dry_run=False)

    assert [v.version for v in pruned] == ["1.0.0", "1.1.0"]
    after = iceberg_stack.metadata("prune")
    assert [s["snapshot-id"] for s in after["snapshots"]] == [newest]
    assert set(after["refs"]) == {"main", tag_name("1.2.0")}
    assert property_key("1.0.0") not in after["properties"]
    assert property_key("1.1.0") not in after["properties"]
    assert [v.version for v in backend.list_versions("prune")] == ["1.2.0"]
    # The rows of the kept version still read, so no live file was deleted.
    assert iceberg_stack.row_count("prune") == 3
    # Under merge-on-read the kept snapshot still lists every older data file
    # and deletion vector (seen 2026-11-04 on DuckDB 1.5.6), so nothing is
    # unreachable and nothing is deleted. Whatever is deleted must be unreachable.
    kept_files = _reachable(iceberg_stack, "prune", [newest])
    objects_after = set(iceberg_stack.objects("prune"))
    deleted = objects_before - objects_after
    bucket = iceberg_stack.lane.bucket_url
    assert all(f"{bucket}{key}" not in kept_files for key in deleted)
    assert {
        f"{bucket}{key}" for key in objects_after if key.endswith((".parquet", ".puffin"))
    } <= kept_files
    _ = old_snapshots


def test_prune_keeps_the_current_version_after_a_rollback(
    iceberg_stack: IcebergStack, tmp_path: Path
) -> None:
    backend = _three_versions(tmp_path, "rolled")
    backend.rollback("rolled", "1.0.0")

    pruned = backend.prune("rolled", keep=1, dry_run=False)

    assert [v.version for v in pruned] == ["1.1.0"]
    assert [v.version for v in backend.list_versions("rolled")] == ["1.0.0", "1.2.0"]
    assert backend.get_current_version("rolled").version == "1.0.0"
    assert iceberg_stack.row_count("rolled") == 1
