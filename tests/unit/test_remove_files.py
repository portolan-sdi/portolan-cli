"""Unit tests for the `portolan rm` file-expansion and deletion helpers.

Covers `_gather_removable_files`, `_remove_one_file`, and `remove_files`.
Version-tracking behavior lives in `test_remove_versions.py`; these tests build
catalogs without a `versions.json` unless the assertion needs one, so each one
isolates the on-disk effect.

The layout under test throughout:

    tmp_path/
      .portolan/config.yaml      catalog-root sentinel (ADR-0029)
      mycoll/
        collection.json          marks the collection dir
        tunnels.parquet          collection-level asset (ADR-0031)
        districts/               item dir
          item.json
          districts.parquet
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from portolan_cli.remove import _gather_removable_files, _remove_one_file, remove_files
from portolan_cli.versions import (
    SPEC_VERSION,
    Asset,
    Version,
    VersionsFile,
    read_versions,
    write_versions,
)

pytestmark = pytest.mark.unit


@pytest.fixture
def catalog(tmp_path: Path) -> Path:
    """A managed catalog with one collection holding both asset levels.

    The catalog sits in a subdirectory so `tmp_path` itself stays available as
    genuinely-outside-the-catalog scratch space.
    """
    root = tmp_path / "catalog"
    portolan_dir = root / ".portolan"
    portolan_dir.mkdir(parents=True)
    (portolan_dir / "config.yaml").write_text("{}\n", encoding="utf-8")
    # find_catalog_root needs both the sentinel and catalog.json to call this
    # an operational catalog.
    (root / "catalog.json").write_text("{}\n", encoding="utf-8")

    coll_dir = root / "mycoll"
    coll_dir.mkdir()
    (coll_dir / "collection.json").write_text("{}\n", encoding="utf-8")
    (coll_dir / "tunnels.parquet").write_bytes(b"fake")

    item_dir = coll_dir / "districts"
    item_dir.mkdir()
    (item_dir / "item.json").write_text("{}\n", encoding="utf-8")
    (item_dir / "districts.parquet").write_bytes(b"fake")

    return root


class TestGatherRemovableFiles:
    """Expansion of a removal target into concrete files."""

    def test_directory_expands_to_every_file_beneath_it(self, catalog: Path) -> None:
        """A directory yields all nested files and no directories."""
        gathered = set(_gather_removable_files(catalog / "mycoll"))

        assert gathered == {
            catalog / "mycoll" / "collection.json",
            catalog / "mycoll" / "tunnels.parquet",
            catalog / "mycoll" / "districts" / "item.json",
            catalog / "mycoll" / "districts" / "districts.parquet",
        }

    def test_directory_recurses_past_one_level(self, catalog: Path) -> None:
        """Files nested two levels down are still gathered."""
        deep = catalog / "mycoll" / "districts" / "nested"
        deep.mkdir()
        (deep / "deep.parquet").write_bytes(b"fake")

        assert deep / "deep.parquet" in _gather_removable_files(catalog / "mycoll")

    def test_empty_directory_yields_nothing(self, tmp_path: Path) -> None:
        """A directory with no files expands to an empty list."""
        empty = tmp_path / "empty"
        empty.mkdir()

        assert _gather_removable_files(empty) == []

    def test_file_is_paired_with_its_sidecars(self, tmp_path: Path) -> None:
        """A shapefile drags its sidecars along, primary file first."""
        shp = tmp_path / "roads.shp"
        shp.write_bytes(b"fake")
        for ext in (".dbf", ".shx", ".prj"):
            (tmp_path / f"roads{ext}").write_bytes(b"fake")

        gathered = _gather_removable_files(shp)

        assert gathered[0] == shp
        assert set(gathered[1:]) == {
            tmp_path / "roads.dbf",
            tmp_path / "roads.shx",
            tmp_path / "roads.prj",
        }

    def test_file_without_sidecars_yields_only_itself(self, tmp_path: Path) -> None:
        """A format with no sidecar convention yields a one-element list."""
        parquet = tmp_path / "data.parquet"
        parquet.write_bytes(b"fake")

        assert _gather_removable_files(parquet) == [parquet]

    def test_missing_file_still_yields_itself(self, tmp_path: Path) -> None:
        """A path that does not exist is returned so it can be untracked.

        `rm --keep` on an already-deleted file must still reach
        `_remove_one_file` to drop the versions.json entry.
        """
        missing = tmp_path / "gone.parquet"

        assert _gather_removable_files(missing) == [missing]


class TestRemoveOneFile:
    """Guards and deletion behavior of `_remove_one_file`."""

    def test_item_level_asset_removes_the_whole_item_dir(self, catalog: Path) -> None:
        """An item-level asset takes its item dir, and item.json, with it."""
        item_dir = catalog / "mycoll" / "districts"

        removed = _remove_one_file(
            item_dir / "districts.parquet", catalog_root=catalog, keep=False, dry_run=False
        )

        assert removed is True
        assert not item_dir.exists()

    def test_collection_level_asset_keeps_the_collection_dir(self, catalog: Path) -> None:
        """A file beside collection.json is deleted alone, not with its parent."""
        coll_dir = catalog / "mycoll"

        removed = _remove_one_file(
            coll_dir / "tunnels.parquet", catalog_root=catalog, keep=False, dry_run=False
        )

        assert removed is True
        assert not (coll_dir / "tunnels.parquet").exists()
        assert (coll_dir / "collection.json").exists()
        assert (coll_dir / "districts" / "districts.parquet").exists()

    def test_collection_level_asset_takes_its_sidecars(self, catalog: Path) -> None:
        """Deleting a shapefile beside collection.json removes its sidecars too."""
        coll_dir = catalog / "mycoll"
        shp = coll_dir / "roads.shp"
        shp.write_bytes(b"fake")
        for ext in (".dbf", ".shx"):
            (coll_dir / f"roads{ext}").write_bytes(b"fake")

        _remove_one_file(shp, catalog_root=catalog, keep=False, dry_run=False)

        assert not shp.exists()
        assert not (coll_dir / "roads.dbf").exists()
        assert not (coll_dir / "roads.shx").exists()

    def test_keep_preserves_the_file_on_disk(self, catalog: Path) -> None:
        """keep=True untracks without deleting anything."""
        item_dir = catalog / "mycoll" / "districts"

        removed = _remove_one_file(
            item_dir / "districts.parquet", catalog_root=catalog, keep=True, dry_run=False
        )

        assert removed is True
        assert (item_dir / "districts.parquet").exists()
        assert (item_dir / "item.json").exists()

    def test_dry_run_mutates_nothing(self, catalog: Path) -> None:
        """dry_run reports success without touching the filesystem."""
        item_dir = catalog / "mycoll" / "districts"

        removed = _remove_one_file(
            item_dir / "districts.parquet", catalog_root=catalog, keep=False, dry_run=True
        )

        assert removed is True
        assert (item_dir / "districts.parquet").exists()

    def test_missing_and_untracked_file_is_skipped(self, catalog: Path) -> None:
        """A file that is gone and never tracked is skipped, not reported removed.

        The fixture writes no versions.json for ``mycoll``, so ``gone.parquet``
        is neither on disk nor tracked. Removing it is a no-op (#803).
        """
        removed = _remove_one_file(
            catalog / "mycoll" / "gone.parquet",
            catalog_root=catalog,
            keep=False,
            dry_run=False,
        )

        assert removed is False

    @pytest.mark.parametrize("keep", [False, True])
    def test_missing_but_tracked_file_is_untracked(self, catalog: Path, keep: bool) -> None:
        """A tracked file that is gone from disk is untracked (#803).

        This is the acceptance case: a user deletes an asset by hand, then runs
        ``rm`` to drop the phantom versions.json entry. The ``keep`` flag makes
        no difference because there is nothing on disk to preserve or delete.
        """
        coll_dir = catalog / "mycoll"
        versions_path = coll_dir / "versions.json"
        write_versions(
            versions_path,
            VersionsFile(
                spec_version=SPEC_VERSION,
                current_version="1.0.0",
                versions=[
                    Version(
                        version="1.0.0",
                        created=datetime.now(timezone.utc),
                        breaking=False,
                        assets={
                            "gone.pmtiles": Asset(
                                sha256="abc", size_bytes=1, href="mycoll/gone.pmtiles"
                            )
                        },
                        changes=["gone.pmtiles"],
                    )
                ],
            ),
        )
        # The asset was never written to disk, so it is "missing from disk".
        assert not (coll_dir / "gone.pmtiles").exists()

        removed = _remove_one_file(
            coll_dir / "gone.pmtiles",
            catalog_root=catalog,
            keep=keep,
            dry_run=False,
        )

        assert removed is True
        latest = read_versions(versions_path).versions[-1]
        assert "gone.pmtiles" not in latest.assets

    def test_missing_but_tracked_file_dry_run_reports_removal(self, catalog: Path) -> None:
        """dry-run on a tracked-but-missing file reports removal without publishing."""
        coll_dir = catalog / "mycoll"
        versions_path = coll_dir / "versions.json"
        write_versions(
            versions_path,
            VersionsFile(
                spec_version=SPEC_VERSION,
                current_version="1.0.0",
                versions=[
                    Version(
                        version="1.0.0",
                        created=datetime.now(timezone.utc),
                        breaking=False,
                        assets={
                            "gone.pmtiles": Asset(
                                sha256="abc", size_bytes=1, href="mycoll/gone.pmtiles"
                            )
                        },
                        changes=["gone.pmtiles"],
                    )
                ],
            ),
        )

        removed = _remove_one_file(
            coll_dir / "gone.pmtiles",
            catalog_root=catalog,
            keep=False,
            dry_run=True,
        )

        assert removed is True
        assert len(read_versions(versions_path).versions) == 1

    def test_missing_and_untracked_file_dry_run_is_skipped(self, catalog: Path) -> None:
        """dry-run on a missing, untracked file reports skip, not a phantom removal."""
        removed = _remove_one_file(
            catalog / "mycoll" / "gone.pmtiles",
            catalog_root=catalog,
            keep=False,
            dry_run=True,
        )

        assert removed is False

    def test_symlink_is_refused(self, catalog: Path) -> None:
        """A symlink is left alone rather than followed into a delete.

        The target here is inside the catalog, so nothing but the symlink guard
        can account for the refusal.
        """
        target = catalog / "mycoll" / "tunnels.parquet"
        link = catalog / "mycoll" / "link.parquet"
        link.symlink_to(target)

        removed = _remove_one_file(link, catalog_root=catalog, keep=False, dry_run=False)

        assert removed is False
        assert link.is_symlink()
        assert target.exists()

    def test_symlink_can_be_untracked_with_keep(self, catalog: Path) -> None:
        """keep=True untracks a symlink without deleting it or its target."""
        target = catalog / "mycoll" / "tunnels.parquet"
        link = catalog / "mycoll" / "link.parquet"
        link.symlink_to(target)

        removed = _remove_one_file(link, catalog_root=catalog, keep=True, dry_run=False)

        assert removed is True
        assert link.is_symlink()
        assert target.exists()

    def test_symlink_pointing_outside_the_catalog_is_skipped(
        self, catalog: Path, tmp_path: Path
    ) -> None:
        """Collection resolution follows the link, so an external target has none."""
        target = tmp_path / "outside.parquet"
        target.write_bytes(b"fake")
        link = catalog / "mycoll" / "link.parquet"
        link.symlink_to(target)

        removed = _remove_one_file(link, catalog_root=catalog, keep=True, dry_run=False)

        assert removed is False
        assert target.exists()

    def test_file_outside_catalog_is_skipped(self, catalog: Path, tmp_path: Path) -> None:
        """A path outside the catalog root is skipped instead of raising."""
        outside = tmp_path / "elsewhere.parquet"
        outside.write_bytes(b"fake")

        removed = _remove_one_file(outside, catalog_root=catalog, keep=False, dry_run=False)

        assert removed is False
        assert outside.exists()

    def test_removal_untracks_the_asset_in_versions_json(self, catalog: Path) -> None:
        """Deleting a tracked file drops its entry from the collection's versions.json.

        This is the only test that exercises the versions.json lookup by name,
        so a wrong filename there shows up here rather than as silent no-op
        untracking.
        """
        coll_dir = catalog / "mycoll"
        versions_path = coll_dir / "versions.json"
        write_versions(
            versions_path,
            VersionsFile(
                spec_version=SPEC_VERSION,
                current_version="1.0.0",
                versions=[
                    Version(
                        version="1.0.0",
                        created=datetime.now(timezone.utc),
                        breaking=False,
                        assets={
                            "tunnels.parquet": Asset(
                                sha256="abc", size_bytes=1, href="mycoll/tunnels.parquet"
                            )
                        },
                        changes=["tunnels.parquet"],
                    )
                ],
            ),
        )

        _remove_one_file(
            coll_dir / "tunnels.parquet", catalog_root=catalog, keep=False, dry_run=False
        )

        latest = read_versions(versions_path).versions[-1]
        assert "tunnels.parquet" not in latest.assets

    def test_dry_run_leaves_versions_json_untouched(self, catalog: Path) -> None:
        """A preview publishes no new version."""
        coll_dir = catalog / "mycoll"
        versions_path = coll_dir / "versions.json"
        write_versions(
            versions_path,
            VersionsFile(
                spec_version=SPEC_VERSION,
                current_version="1.0.0",
                versions=[
                    Version(
                        version="1.0.0",
                        created=datetime.now(timezone.utc),
                        breaking=False,
                        assets={
                            "tunnels.parquet": Asset(
                                sha256="abc", size_bytes=1, href="mycoll/tunnels.parquet"
                            )
                        },
                        changes=["tunnels.parquet"],
                    )
                ],
            ),
        )

        _remove_one_file(
            coll_dir / "tunnels.parquet", catalog_root=catalog, keep=True, dry_run=True
        )

        assert len(read_versions(versions_path).versions) == 1

    def test_file_at_catalog_root_is_skipped(self, catalog: Path) -> None:
        """A file with no collection directory above it is skipped."""
        stray = catalog / "stray.parquet"
        stray.write_bytes(b"fake")

        removed = _remove_one_file(stray, catalog_root=catalog, keep=False, dry_run=False)

        assert removed is False
        assert stray.exists()


class TestRemoveFiles:
    """The `remove_files` entry point."""

    def test_returns_removed_and_skipped_in_that_order(self, catalog: Path) -> None:
        """Successes land in the first list, skips in the second."""
        good = catalog / "mycoll" / "tunnels.parquet"
        stray = catalog / "stray.parquet"
        stray.write_bytes(b"fake")

        removed, skipped = remove_files(paths=[good, stray], catalog_root=catalog)

        assert removed == [good]
        assert skipped == [stray]

    def test_expands_a_directory_argument(self, catalog: Path) -> None:
        """Passing an item dir accounts for every file inside it and clears the dir.

        The first file removed takes the whole item dir with it (item-level
        assets own their directory), so the remaining files report as skipped.
        Which file comes first depends on `rglob` order, so assert the partition
        covers both files rather than pinning either side.
        """
        item_dir = catalog / "mycoll" / "districts"
        contents = {item_dir / "item.json", item_dir / "districts.parquet"}

        removed, skipped = remove_files(paths=[item_dir], catalog_root=catalog)

        assert set(removed) | set(skipped) == contents
        assert len(removed) == 1
        assert not item_dir.exists()

    def test_processes_every_path_argument(self, catalog: Path) -> None:
        """Multiple targets are all visited, not just the first."""
        coll_dir = catalog / "mycoll"
        second = coll_dir / "bridges.parquet"
        second.write_bytes(b"fake")

        removed, _ = remove_files(
            paths=[coll_dir / "tunnels.parquet", second], catalog_root=catalog
        )

        assert set(removed) == {coll_dir / "tunnels.parquet", second}
        assert not (coll_dir / "tunnels.parquet").exists()
        assert not second.exists()

    def test_keep_defaults_to_false(self, catalog: Path) -> None:
        """The default call deletes, git-style."""
        target = catalog / "mycoll" / "tunnels.parquet"

        remove_files(paths=[target], catalog_root=catalog)

        assert not target.exists()

    def test_dry_run_defaults_to_false(self, catalog: Path) -> None:
        """The default call is not a preview."""
        item_dir = catalog / "mycoll" / "districts"

        remove_files(paths=[item_dir / "districts.parquet"], catalog_root=catalog)

        assert not item_dir.exists()

    def test_dry_run_reports_without_deleting(self, catalog: Path) -> None:
        """dry_run lists what would go without removing it."""
        target = catalog / "mycoll" / "tunnels.parquet"

        removed, skipped = remove_files(paths=[target], catalog_root=catalog, dry_run=True)

        assert removed == [target]
        assert skipped == []
        assert target.exists()

    def test_keep_untracks_without_deleting(self, catalog: Path) -> None:
        """keep reports removal from tracking while the file stays put."""
        target = catalog / "mycoll" / "tunnels.parquet"

        removed, _ = remove_files(paths=[target], catalog_root=catalog, keep=True)

        assert removed == [target]
        assert target.exists()

    def test_no_paths_returns_two_empty_lists(self, catalog: Path) -> None:
        """An empty request is a no-op."""
        assert remove_files(paths=[], catalog_root=catalog) == ([], [])
