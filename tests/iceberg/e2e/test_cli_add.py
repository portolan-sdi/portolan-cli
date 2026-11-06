"""The CLI path on the stack: ``init --backend iceberg``, ``add``, and the version commands.

This is the proof the feature works for a user: a catalog initialized on
the Iceberg backend, a GeoParquet added to it, the Iceberg fields on the
collection, and the version commands over the table. The conformance gate
in ``tests/integration/test_generated_catalog_conformance.py`` runs rashid
over the same catalog.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from tests.integration.test_stdout_purity import assert_one_json_document

if TYPE_CHECKING:
    from tests.iceberg_stack import IcebergStack

pytestmark = pytest.mark.e2e

POINTS = Path(__file__).resolve().parents[2] / "fixtures" / "vector" / "valid" / "points.parquet"


def _run(*args: str, expect: int | None = 0) -> Any:
    result = CliRunner().invoke(cli, list(args), catch_exceptions=False)
    if expect is not None:
        assert result.exit_code == expect, f"portolan {' '.join(args)}\n{result.output}"
    return result


@pytest.fixture
def catalog(iceberg_stack: IcebergStack, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A catalog on the Iceberg backend with the ``roads`` collection added once."""
    _ = iceberg_stack
    root = tmp_path / "catalog"
    monkeypatch.chdir(tmp_path)
    _run("init", str(root), "--auto", "--backend", "iceberg", "--license", "CC0-1.0")
    (root / "roads").mkdir()
    shutil.copy(POINTS, root / "roads" / "roads.parquet")
    _run("add", "--portolan-dir", str(root), str(root / "roads" / "roads.parquet"))
    return root


def _collection(root: Path) -> dict[str, Any]:
    document: dict[str, Any] = json.loads(
        (root / "roads" / "collection.json").read_text(encoding="utf-8")
    )
    return document


def test_add_writes_the_iceberg_fields_of_a_v3_table(
    catalog: Path, iceberg_stack: IcebergStack
) -> None:
    document = _collection(catalog)
    metadata = iceberg_stack.metadata("roads")

    assert document["iceberg:catalog_type"] == "rest"
    assert document["iceberg:catalog_uri"] == iceberg_stack.lane.uri
    assert document["iceberg:authorization_type"] == iceberg_stack.lane.authorization_type
    assert document["iceberg:table_id"] == f"{iceberg_stack.namespace}.roads"
    assert document["iceberg:format_version"] == 3
    assert document["iceberg:current_snapshot_id"] == str(metadata["current-snapshot-id"])
    location = iceberg_stack.lane.published_location(
        iceberg_stack.load_table("roads").metadata_location
    )
    assert document["iceberg:metadata_location"] == location
    assert document["table:row_count"] == iceberg_stack.row_count("roads")
    assert document["table:primary_geometry"] == "geometry"
    assert any(
        c["name"] == "geometry" and c["type"] == "geometry" for c in document["table:columns"]
    )
    # An s3:// location is not fetchable over the web, so no asset names it (PTL-AST-002).
    # An https location is, so the asset names it.
    if location.startswith("https://"):
        assert document["assets"]["iceberg"]["href"] == location
    else:
        assert "iceberg" not in document["assets"]
    assert {"schema.json" in url for url in document["stac_extensions"]} == {True}
    assert (catalog / "roads" / "README.md").exists()


def test_version_commands_work_over_the_table(catalog: Path) -> None:
    shutil.copy(POINTS, catalog / "roads" / "more.parquet")
    _run("add", "--portolan-dir", str(catalog), str(catalog / "roads" / "more.parquet"))

    listed = _run("version", "list", "roads", "--catalog", str(catalog)).output
    assert "1.0.0" in listed and "1.1.0" in listed
    assert "1.1.0" in _run("version", "current", "roads", "--catalog", str(catalog)).output

    pruned = _run(
        "version", "prune", "roads", "--keep", "1", "--dry-run", "--catalog", str(catalog), "--json"
    )
    assert_one_json_document(pruned.stdout)
    assert [v["version"] for v in json.loads(pruned.stdout)["data"]["pruned"]] == ["1.0.0"]

    rolled = _run("version", "rollback", "roads", "1.0.0", "--catalog", str(catalog), "--json")
    assert_one_json_document(rolled.stdout)
    assert json.loads(rolled.stdout)["data"]["restored_version"] == "1.0.0"
    assert "1.0.0" in _run("version", "current", "roads", "--catalog", str(catalog)).output


def test_a_rollback_to_an_unknown_version_exits_one(catalog: Path) -> None:
    result = _run("version", "rollback", "roads", "99.0.0", "--catalog", str(catalog), expect=1)

    assert "99.0.0" in result.output


def test_a_second_add_of_the_same_file_keeps_one_row_per_feature(
    catalog: Path, iceberg_stack: IcebergStack
) -> None:
    """A re-add replaces the rows. The old backend appended them and doubled the count."""
    before = iceberg_stack.row_count("roads")
    shutil.copy(POINTS, catalog / "roads" / "roads.parquet")

    _run("add", "--portolan-dir", str(catalog), "--force", str(catalog / "roads" / "roads.parquet"))

    assert iceberg_stack.row_count("roads") == before
    assert _collection(catalog)["table:row_count"] == before


def test_refresh_follows_a_new_metadata_file(catalog: Path, iceberg_stack: IcebergStack) -> None:
    """A property commit writes a new metadata file, as the maintenance of a catalog does."""
    before = _collection(catalog)["iceberg:metadata_location"]
    iceberg_stack.admin.catalog.commit_table(
        iceberg_stack.namespace,
        "roads",
        requirements=[],
        updates=[{"action": "set-properties", "updates": {"maintenance.run": "1"}}],
    )
    moved = iceberg_stack.lane.published_location(
        iceberg_stack.load_table("roads").metadata_location
    )

    result = _run("version", "refresh", "roads", "--json", "--catalog", str(catalog))

    data = assert_one_json_document(result.output)["data"]
    assert moved != before
    assert data == {
        "collection": "roads",
        "previous_location": before,
        "metadata_location": moved,
        "changed": True,
        "remote": None,
    }
    assert _collection(catalog)["iceberg:metadata_location"] == moved
    assert _collection(catalog)["table:row_count"] == iceberg_stack.row_count("roads")
