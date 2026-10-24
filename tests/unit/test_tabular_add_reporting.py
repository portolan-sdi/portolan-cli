"""Regression tests for standalone tabular add reporting (issue #712).

A first, successful add of a standalone tabular file reported it as already
tracked and unchanged, because `_process_deferred_non_geo_files` appended it to
`skipped` rather than `added`. The add itself worked, only the reporting lied.

The spec puts a tabular collection on "the single-file collection pattern, no
item directory or item JSON" (portolan-spec `specs/portolan/formats.md`,
Tabular). A single-file *geo* collection asset already reports through `added`
despite writing no item JSON, so tabular follows the same path.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from portolan_cli import cli
from portolan_cli.add import add_files

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit


def _tabular_catalog(tmp_path: Path) -> tuple[Path, Path]:
    """A catalog with tabular support on and one standalone non-geo Parquet."""
    root = tmp_path / "catalog"
    runner = CliRunner()
    runner.invoke(cli, ["init", str(root), "--auto", "--license", "CC-BY-4.0", "--title", "Repro"])
    (root / ".portolan" / "config.yaml").write_text("tabular:\n  enabled: true\n")

    collection_dir = root / "boundaries" / "adm1-attributes"
    collection_dir.mkdir(parents=True)
    parquet = collection_dir / "adm1-attributes.parquet"
    pq.write_table(pa.table({"adm1_code": ["A", "B"], "pop": [1, 2]}), parquet)
    return root, parquet


def test_first_tabular_add_reports_added(tmp_path: Path) -> None:
    """The asset lands in `added`, not `skipped`, on a first successful add."""
    root, parquet = _tabular_catalog(tmp_path)

    added, skipped, failures = add_files(paths=[parquet], catalog_root=root)

    assert failures == []
    assert parquet not in skipped, "a brand new asset is not a no-op"
    assert [i.item_id for i in added] == ["adm1-attributes"]
    assert [i.collection_id for i in added] == ["boundaries/adm1-attributes"]


def test_first_tabular_add_does_not_claim_unchanged(tmp_path: Path) -> None:
    """The issue's reproduction: the human summary must not say 'already tracked'."""
    root, parquet = _tabular_catalog(tmp_path)

    result = CliRunner().invoke(cli, ["add", str(parquet), "--portolan-dir", str(root)])

    assert "already tracked" not in result.output
    assert "Added 1 file to 1 collection" in result.output


def test_tabular_add_carries_the_collection_aoi_bbox(tmp_path: Path) -> None:
    """Spec: a tabular collection MUST still carry extent.spatial.bbox (an AOI)."""
    root, parquet = _tabular_catalog(tmp_path)

    added, _skipped, _failures = add_files(paths=[parquet], catalog_root=root)

    extent = json.loads((parquet.parent / "collection.json").read_text())["extent"]
    assert added[0].bbox == extent["spatial"]["bbox"][0]


def test_genuine_no_op_still_reports_unchanged(tmp_path: Path) -> None:
    """A second add of the same bytes is still a real no-op."""
    root, parquet = _tabular_catalog(tmp_path)
    add_files(paths=[parquet], catalog_root=root)

    result = CliRunner().invoke(cli, ["add", str(parquet), "--portolan-dir", str(root)])

    assert "already tracked" in result.output


def test_a_companion_that_meets_an_asset_key_collision_is_a_skip(tmp_path: Path) -> None:
    """The rescan of the item raised the collision, and add reported a failure.

    A single file reports the same collision as a skip. The copy goes again,
    so the item directory and the item JSON stay as they were.
    """
    from portolan_cli.add import _process_deferred_non_geo_files
    from portolan_cli.stac import MergeStrategy

    item_dir = tmp_path / "land" / "scene"
    item_dir.mkdir(parents=True)
    (item_dir / "scene.tif").write_bytes(b"cog")
    (item_dir / "data").write_bytes(b"notes")
    item_json = json.dumps({"assets": {"data": {"href": "scene.tif", "roles": ["data"]}}})
    (item_dir / "scene.json").write_text(item_json)
    source_dir = tmp_path / "incoming"
    source_dir.mkdir()
    companion = source_dir / "prices.csv"
    companion.write_text("a\n1\n")
    skipped: list[Path] = []
    failures: list[object] = []
    reasons: dict[Path, str] = {}

    _process_deferred_non_geo_files(
        deferred_non_geo=[(companion, source_dir, "land")],
        source_to_item_dir={source_dir: (item_dir, "land", "scene")},
        source_to_collection_dir={},
        catalog_root=tmp_path,
        added=[],
        skipped=skipped,
        failures=failures,
        merge_strategy=MergeStrategy.SMART,
        skip_reasons=reasons,
    )

    assert failures == []
    assert skipped == [companion]
    assert reasons == {
        companion: (
            "Collection 'land': asset key 'data' already holds 'scene.tif'. Adding 'data' "
            "under the same key would remove that asset. Remove one of the two files from "
            "the collection, or rename it."
        )
    }
    assert sorted(p.name for p in item_dir.iterdir()) == ["data", "scene.json", "scene.tif"]
    assert (item_dir / "scene.json").read_text() == item_json
