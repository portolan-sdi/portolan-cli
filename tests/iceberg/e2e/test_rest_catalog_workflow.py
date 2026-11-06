"""The version lifecycle against the REST catalog and the object store of the stack.

Each test gets its own namespace on the compose stack through
``iceberg_stack``, builds the backend from the environment the fixture
set, and reads the result back the two ways a user has: the ``loadTable``
document, and DuckDB's ``iceberg_scan`` over the metadata file.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from portolan_cli.backends.iceberg.backend import IcebergBackend
from portolan_cli.backends.iceberg.versioning import property_key, tag_name
from tests.iceberg.e2e.conftest import SCHEMA, publish_rows, write_test_parquet

if TYPE_CHECKING:
    from pathlib import Path

    from tests.iceberg_stack import IcebergStack

pytestmark = pytest.mark.e2e


def test_publish_creates_a_v3_table_with_the_record_and_the_tag(
    iceberg_stack: IcebergStack, tmp_path: Path
) -> None:
    backend = publish_rows(tmp_path, "rest-publish", 3, message="First version via REST")

    metadata = iceberg_stack.metadata("rest-publish")
    assert metadata["format-version"] == 3
    assert property_key("1.0.0") in metadata["properties"]
    assert metadata["properties"]["portolan.record"] == "1"
    assert metadata["refs"][tag_name("1.0.0")]["type"] == "tag"
    assert metadata["refs"][tag_name("1.0.0")]["snapshot-id"] == metadata["current-snapshot-id"]
    assert iceberg_stack.row_count("rest-publish") == 3
    current = backend.get_current_version("rest-publish")
    assert current.version == "1.0.0"
    assert current.message == "First version via REST"
    assert current.assets["data.parquet"].href == "rest-publish/data.parquet"


def test_a_later_publish_replaces_the_rows_and_a_rollback_restores_them(
    iceberg_stack: IcebergStack, tmp_path: Path
) -> None:
    """Three publishes of 1, 2, and 3 rows under one key: each version holds its own rows."""
    for rows in (1, 2, 3):
        backend = publish_rows(tmp_path, "lifecycle", rows, message=f"{rows} rows")

    versions = backend.list_versions("lifecycle")
    assert [v.version for v in versions] == ["1.0.0", "1.1.0", "1.2.0"]
    assert iceberg_stack.row_count("lifecycle") == 3

    restored = backend.rollback("lifecycle", "1.0.0")

    assert restored.version == "1.0.0"
    assert backend.get_current_version("lifecycle").version == "1.0.0"
    # main moved, so a serverless reader of the metadata file sees the old rows.
    assert iceberg_stack.row_count("lifecycle") == 1
    assert [v.version for v in backend.list_versions("lifecycle")] == ["1.0.0", "1.1.0", "1.2.0"]


def test_a_publish_after_a_rollback_continues_from_the_newest_version(
    iceberg_stack: IcebergStack, tmp_path: Path
) -> None:
    publish_rows(tmp_path, "after-rollback", 1)
    backend = publish_rows(tmp_path, "after-rollback", 2)
    backend.rollback("after-rollback", "1.0.0")
    # The rows come from the local files. A pull restores the file of 1.0.0.
    write_test_parquet(tmp_path / "after-rollback" / "data.parquet", rows=1)

    version = backend.publish("after-rollback", {}, SCHEMA, False, "keep the old rows")

    assert version.version == "1.2.0"
    # The merged asset set came from the current version, 1.0.0, with one row.
    assert iceberg_stack.row_count("after-rollback") == 1


def test_a_new_column_evolves_the_table_schema(iceberg_stack: IcebergStack, tmp_path: Path) -> None:
    backend = publish_rows(tmp_path, "schema-evo", 2)
    wider = pa.table(
        {
            "id": pa.array([10, 20], type=pa.int64()),
            "val": pa.array(["a", "b"], type=pa.string()),
            "extra": pa.array([1.0, 2.0], type=pa.float64()),
        }
    )
    path = tmp_path / "schema-evo" / "data.parquet"
    pq.write_table(wider, path)

    version = backend.publish("schema-evo", {"data.parquet": str(path)}, SCHEMA, False, "v2")

    assert version.version == "1.1.0"
    fields = {
        f["name"]: f["type"] for f in iceberg_stack.metadata("schema-evo")["schemas"][-1]["fields"]
    }
    assert fields == {"id": "long", "val": "string", "extra": "double"}
    assert iceberg_stack.rows("schema-evo", "SELECT sum(extra) FROM t") == [(3.0,)]


def test_the_merged_asset_set_is_what_the_table_holds(
    iceberg_stack: IcebergStack, tmp_path: Path
) -> None:
    """A second file joins the rows of the first. A removal drops them."""
    backend = IcebergBackend(catalog_root=tmp_path)
    (tmp_path / "merge").mkdir()
    first = write_test_parquet(tmp_path / "merge" / "a.parquet", rows=2)
    backend.publish("merge", {"a.parquet": str(first)}, SCHEMA, False, "a")
    second = write_test_parquet(tmp_path / "merge" / "b.parquet", rows=3, start=100)

    backend.publish("merge", {"b.parquet": str(second)}, SCHEMA, False, "a+b")
    assert iceberg_stack.row_count("merge") == 5

    backend.publish("merge", {}, SCHEMA, False, "b", removed={"a.parquet"})
    assert iceberg_stack.row_count("merge") == 3
    assert iceberg_stack.rows("merge", "SELECT min(id) FROM t") == [(100,)]
