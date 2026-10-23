"""`check --fix` must not make a catalog worse (issue #709).

Two defects hid behind one command.

`portolan add` records an item-level asset in versions.json under the key
``{item_id}/{filename}``, while the freshness reader looked it up under
``{filename}``. The lookup never matched, so every item-level asset read as
STALE forever. `check --fix` then rewrote each item on every run, stamped
``properties.datetime`` with the time of the run, and left items.parquet behind,
which raised two PTL-DAT-016 errors.

Separately, the scan asked only for ``{directory name}.json``. An item JSON
named after a ``--item-id`` override does not answer to that name, so `--fix`
created a second item for the same data file.

The catalogs here come from the real `init` + `add` path. A hand-built tree
would not carry the versions.json key that causes the bug.
"""

from __future__ import annotations

import json
import shutil
from typing import TYPE_CHECKING

import pytest

from portolan_cli.metadata.models import MetadataStatus
from portolan_cli.metadata.scan import scan_catalog_metadata

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from tests.catalog_invariants import RunCli

pytestmark = [pytest.mark.integration, pytest.mark.realdata]

# `add --item-id` writes asset hrefs relative to a directory that does not
# exist, so run_cli must not check those catalogs at teardown.
ITEM_ID_REASON = "item-id: hrefs point at a directory that does not exist (#840)"
# The test deletes the item JSON and leaves the collection's item link dangling.
DELETED_ITEM_REASON = "check_fix: the test deletes the item JSON on purpose"
STALE_MIRROR_REASON = (
    "check_fix: --fix rewrites items.parquet and leaves versions.json stale (#1046)"
)


def _added_raster_catalog(run_cli: RunCli, root: Path, cog: Path, *, item_id: str = "") -> Path:
    """A catalog holding one COG item, built the way an operator builds one."""
    root.mkdir(parents=True, exist_ok=True)
    run_cli("init", "--auto", str(root), "--license", "CC-BY-4.0", "--id", "regress")
    item_dir = root / "imagery" / "rapidai4eo"
    item_dir.mkdir(parents=True)
    shutil.copy(cog, item_dir / "rapidai4eo.tif")

    args = ["add", str(item_dir / "rapidai4eo.tif"), "--portolan-dir", str(root)]
    if item_id:
        args += ["--item-id", item_id]
    run_cli(*args)
    return root


def _counts(run_cli: RunCli, root: Path) -> tuple[int, int]:
    """The (error, warning) totals `check` reports for the catalog."""
    result = run_cli("check", str(root), "--json", expect=None)
    payload = json.loads(result.stdout)
    findings = payload["data"]["findings"]
    errors = sum(1 for f in findings if f["severity"] == "error")
    warnings = sum(1 for f in findings if f["severity"] == "warning")
    return errors, warnings


def _tree_bytes(root: Path) -> dict[str, bytes]:
    """Every JSON file under the catalog, keyed by its path relative to root."""
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*.json"))
        if ".portolan" not in path.parts
    }


def _item_json(root: Path) -> dict:
    return json.loads((root / "imagery" / "rapidai4eo" / "rapidai4eo.json").read_text())


class TestFreshnessBaseline:
    """An item `add` just wrote is FRESH, so `--fix` has nothing to rewrite."""

    def test_added_item_scans_as_fresh(
        self, run_cli: RunCli, tmp_path: Path, valid_rgb_cog: Path
    ) -> None:
        """The scan resolves the ``{item_id}/{filename}`` versions.json key.

        Before the fix the scan reported STALE with the reason
        ``mtime, bbox, feature_count, schema``, because the lookup used the bare
        file name and found no entry.
        """
        root = _added_raster_catalog(run_cli, tmp_path / "cat", valid_rgb_cog)

        report = scan_catalog_metadata(root)
        tif = root / "imagery" / "rapidai4eo" / "rapidai4eo.tif"
        statuses = {r.file_path.resolve(): r.status for r in report.results}

        assert statuses[tif.resolve()] == MetadataStatus.FRESH

    def test_untouched_item_survives_fix_unchanged(
        self, run_cli: RunCli, tmp_path: Path, valid_rgb_cog: Path
    ) -> None:
        """`--fix` writes no byte into an item nobody touched."""
        root = _added_raster_catalog(run_cli, tmp_path / "cat", valid_rgb_cog)
        before = _tree_bytes(root)

        run_cli("check", str(root), "--fix", expect=None)

        assert _tree_bytes(root) == before

    def test_fix_is_idempotent(self, run_cli: RunCli, tmp_path: Path, valid_rgb_cog: Path) -> None:
        """Two runs of `--fix` leave the same bytes.

        Before the fix, ``properties.datetime`` moved on every run.
        """
        root = _added_raster_catalog(run_cli, tmp_path / "cat", valid_rgb_cog)

        run_cli("check", str(root), "--fix", expect=None)
        after_first = _tree_bytes(root)
        run_cli("check", str(root), "--fix", expect=None)

        assert _tree_bytes(root) == after_first

    def test_fix_does_not_raise_the_error_count(
        self, run_cli: RunCli, tmp_path: Path, valid_rgb_cog: Path
    ) -> None:
        """The headline promise of #709.

        Before the fix the count went from 3 to 5, because the rewritten item
        no longer agreed with items.parquet.
        """
        root = _added_raster_catalog(run_cli, tmp_path / "cat", valid_rgb_cog)
        errors_before, warnings_before = _counts(run_cli, root)

        run_cli("check", str(root), "--fix", expect=None)
        errors_after, warnings_after = _counts(run_cli, root)

        assert errors_after <= errors_before
        assert warnings_after <= warnings_before

    def test_fix_keeps_a_null_datetime(
        self, run_cli: RunCli, tmp_path: Path, valid_rgb_cog: Path
    ) -> None:
        """`add` writes ``datetime: null`` without ``--datetime``.

        The refresh must not invent an acquisition date. Before the fix it wrote
        the wall-clock time of the run.
        """
        root = _added_raster_catalog(run_cli, tmp_path / "cat", valid_rgb_cog)
        assert _item_json(root)["properties"]["datetime"] is None

        run_cli("check", str(root), "--fix", expect=None)

        assert _item_json(root)["properties"]["datetime"] is None


class TestNoFabricatedItem:
    """`--fix` never writes a second item JSON for one data file."""

    def test_item_id_override_does_not_gain_a_second_item(
        self,
        run_cli: RunCli,
        tmp_path: Path,
        valid_rgb_cog: Path,
        build_damaged_catalog: Callable[[Path, str], Path],
    ) -> None:
        """`add --item-id scene` writes ``scene.json`` in directory ``rapidai4eo``.

        The scan asked only for ``rapidai4eo.json``, called the COG MISSING, and
        `--fix` fabricated a second item beside ``scene.json``.
        """
        root = _added_raster_catalog(
            run_cli,
            build_damaged_catalog(tmp_path / "cat", ITEM_ID_REASON),
            valid_rgb_cog,
            item_id="scene",
        )
        item_dir = root / "imagery" / "rapidai4eo"
        assert (item_dir / "scene.json").exists()

        run_cli("check", str(root), "--fix", expect=None)

        assert sorted(p.name for p in item_dir.glob("*.json")) == ["scene.json"]

    def test_item_id_override_is_not_an_item_needing_json(
        self,
        run_cli: RunCli,
        tmp_path: Path,
        valid_rgb_cog: Path,
        build_damaged_catalog: Callable[[Path, str], Path],
    ) -> None:
        """The renamed item covers its directory, so no data file needs one.

        `add --item-id` also writes each asset href relative to a directory named
        after the id, which does not exist. That is a separate defect (#840), and
        the MISSING results it produces stay reported. This test pins only the
        trigger #709 named: no data file may read as an item that needs a JSON.
        """
        root = _added_raster_catalog(
            run_cli,
            build_damaged_catalog(tmp_path / "cat", ITEM_ID_REASON),
            valid_rgb_cog,
            item_id="scene",
        )

        report = scan_catalog_metadata(root)

        needs_json = [
            r
            for r in report.filter_by_status(MetadataStatus.MISSING)
            if "no rapidai4eo.json" in r.message
        ]
        assert needs_json == []

    def test_plain_geojson_does_not_stand_in_for_the_item(
        self,
        run_cli: RunCli,
        tmp_path: Path,
        valid_rgb_cog: Path,
        build_damaged_catalog: Callable[[Path, str], Path],
    ) -> None:
        """A GeoJSON Feature is not a STAC Item, so it covers nothing.

        Only `type: "Feature"` plus a string `stac_version` counts. Without the
        second test a hand-dropped `footprint.json` would suppress the MISSING
        result, and `--fix` would never create the item the data file needs.
        """
        root = _added_raster_catalog(
            run_cli,
            build_damaged_catalog(tmp_path / "cat", DELETED_ITEM_REASON),
            valid_rgb_cog,
        )
        item_dir = root / "imagery" / "rapidai4eo"
        (item_dir / "rapidai4eo.json").unlink()
        (item_dir / "footprint.json").write_text(
            json.dumps(
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                    "properties": {},
                }
            )
        )

        report = scan_catalog_metadata(root)

        missing = [
            r
            for r in report.filter_by_status(MetadataStatus.MISSING)
            if "no rapidai4eo.json" in r.message
        ]
        assert len(missing) == 1

    def test_item_id_override_does_not_raise_the_error_count(
        self,
        run_cli: RunCli,
        tmp_path: Path,
        valid_rgb_cog: Path,
        build_damaged_catalog: Callable[[Path, str], Path],
    ) -> None:
        """Before the fix the count went from 3 to 10."""
        root = _added_raster_catalog(
            run_cli,
            build_damaged_catalog(tmp_path / "cat", ITEM_ID_REASON),
            valid_rgb_cog,
            item_id="scene",
        )
        errors_before, _ = _counts(run_cli, root)

        run_cli("check", str(root), "--fix", expect=None)
        errors_after, _ = _counts(run_cli, root)

        assert errors_after <= errors_before


class TestCreatedItemConforms:
    """A genuinely missing item comes back whole, not as a new set of defects."""

    @pytest.fixture
    def stale_mirror_catalog(
        self,
        run_cli: RunCli,
        tmp_path: Path,
        valid_rgb_cog: Path,
        build_damaged_catalog: Callable[[Path, str], Path],
    ) -> Path:
        """A raster catalog that run_cli does not check at teardown.

        ``--fix`` rewrites items.parquet and leaves its versions.json digest
        stale (#1046), and each test below runs ``--fix``.
        """
        root = build_damaged_catalog(tmp_path / "cat", STALE_MIRROR_REASON)
        return _added_raster_catalog(run_cli, root, valid_rgb_cog)

    def test_recreated_item_restores_the_original_error_count(
        self, run_cli: RunCli, stale_mirror_catalog: Path
    ) -> None:
        """Delete the item JSON, run `--fix`, and land back where you started.

        Before the fix the count went from 4 errors to 10, because
        `create_missing_item` wrote an item with no links and no checksums.
        """
        root = stale_mirror_catalog
        errors_before, warnings_before = _counts(run_cli, root)

        (root / "imagery" / "rapidai4eo" / "rapidai4eo.json").unlink()
        run_cli("check", str(root), "--fix", expect=None)
        errors_after, warnings_after = _counts(run_cli, root)

        assert (errors_after, warnings_after) == (errors_before, warnings_before)

    def test_recreated_item_carries_its_structural_links(
        self, run_cli: RunCli, stale_mirror_catalog: Path
    ) -> None:
        """PTL-LNK-001 requires root, parent and collection on an item."""
        root = stale_mirror_catalog

        (root / "imagery" / "rapidai4eo" / "rapidai4eo.json").unlink()
        run_cli("check", str(root), "--fix", expect=None)

        rels = {link["rel"] for link in _item_json(root)["links"]}
        assert {"root", "parent", "collection"} <= rels

    def test_recreated_item_is_linked_from_its_collection(
        self, run_cli: RunCli, stale_mirror_catalog: Path
    ) -> None:
        """PTL-LNK-002 requires a rel='item' link for every contained object."""
        root = stale_mirror_catalog

        (root / "imagery" / "rapidai4eo" / "rapidai4eo.json").unlink()
        run_cli("check", str(root), "--fix", expect=None)

        collection = json.loads((root / "imagery" / "collection.json").read_text())
        item_hrefs = [link["href"] for link in collection["links"] if link["rel"] == "item"]
        assert any("rapidai4eo.json" in href for href in item_hrefs)

    def test_recreated_item_carries_file_size_and_checksum(
        self, run_cli: RunCli, stale_mirror_catalog: Path
    ) -> None:
        """PTL-AST-003 requires file:size and file:checksum on every asset."""
        root = stale_mirror_catalog

        (root / "imagery" / "rapidai4eo" / "rapidai4eo.json").unlink()
        run_cli("check", str(root), "--fix", expect=None)

        data_asset = _item_json(root)["assets"]["data"]
        assert "file:size" in data_asset
        assert "file:checksum" in data_asset
