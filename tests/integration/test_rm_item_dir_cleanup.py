"""Regression tests for issue #602: ``rm`` must delete the item directory.

``_remove_one_file`` used to locate the STAC item directory from the file
*stem* (``catalog/coll/<stem>``), but ``add`` derives the item id from the
file's *parent directory* name (a Hive partition ``key=value/`` or a nested
item dir). The stem only matches in the degenerate case, so real item/partition
directories — and their ``item.json`` sidecars — were orphaned on disk after a
``rm`` that only unlinked the data file.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def initialized_catalog(tmp_path: Path) -> Path:
    """Create an initialized Portolan catalog using CLI."""
    result = CliRunner().invoke(cli, ["init", str(tmp_path), "--auto"])
    assert result.exit_code == 0, f"Init failed: {result.output}"
    return tmp_path


def _write_partition(partition_dir: Path) -> None:
    """Write a tiny valid GeoParquet file into ``partition_dir``."""
    import geopandas as gpd
    from shapely.geometry import Point

    partition_dir.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame(
        {"id": [1, 2]},
        geometry=[Point(0, 0), Point(1, 1)],
        crs="EPSG:4326",
    ).to_parquet(partition_dir / "data.parquet")


class TestRmItemDirCleanup:
    """rm must remove the whole item/partition dir, not just the data file."""

    @pytest.mark.integration
    def test_rm_hive_partition_removes_partition_dir(
        self, runner: CliRunner, initialized_catalog: Path
    ) -> None:
        """rm of a Hive-partition asset deletes the partition dir and its item.json."""
        collection_dir = initialized_catalog / "sites"
        partition_dir = collection_dir / "gms_feature_id=abc"
        _write_partition(partition_dir)
        # Second partition so the collection is not emptied entirely.
        _write_partition(collection_dir / "gms_feature_id=def")

        add_result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(collection_dir)],
            catch_exceptions=False,
        )
        assert add_result.exit_code == 0, add_result.output
        # add derives an item per partition: item.json is {item_id}.json.
        item_json = partition_dir / "gms_feature_id=abc.json"
        assert item_json.exists(), "precondition: item.json written into partition dir"

        rm_result = runner.invoke(
            cli,
            [
                "rm",
                "--portolan-dir",
                str(initialized_catalog),
                "--force",
                str(partition_dir / "data.parquet"),
            ],
            catch_exceptions=False,
        )
        assert rm_result.exit_code == 0, rm_result.output

        # The whole partition dir (data + item.json) must be gone from disk.
        assert not (partition_dir / "data.parquet").exists()
        assert not item_json.exists(), "item.json orphaned after rm (#602)"
        assert not partition_dir.exists(), "empty partition dir orphaned after rm (#602)"
        # Sibling partition and the collection itself are untouched.
        assert (collection_dir / "gms_feature_id=def" / "data.parquet").exists()
        assert (collection_dir / "collection.json").exists()

    @pytest.mark.integration
    def test_rm_nested_item_removes_item_dir(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        valid_singleband_cog: Path,
    ) -> None:
        """rm of a raster item (item dir name != file stem) deletes the item dir."""
        import shutil

        collection_dir = initialized_catalog / "imagery"
        item_dir = collection_dir / "scene-001"
        item_dir.mkdir(parents=True)
        raster = item_dir / "band.tif"
        shutil.copy(valid_singleband_cog, raster)

        add_result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(raster)],
            catch_exceptions=False,
        )
        assert add_result.exit_code == 0, add_result.output
        assert item_dir.is_dir(), "precondition: item dir created by add"
        # item dir name differs from the file stem — the crux of #602.
        assert item_dir.name != raster.stem

        rm_result = runner.invoke(
            cli,
            ["rm", "--portolan-dir", str(initialized_catalog), "--force", str(raster)],
            catch_exceptions=False,
        )
        assert rm_result.exit_code == 0, rm_result.output

        assert not item_dir.exists(), "item dir orphaned after rm (#602)"
        assert (collection_dir / "collection.json").exists()

    @pytest.mark.integration
    def test_rm_collection_level_asset_keeps_collection_dir(
        self, runner: CliRunner, initialized_catalog: Path
    ) -> None:
        """rm of a file sitting directly in the collection dir removes only the file."""
        collection_dir = initialized_catalog / "vectors"
        collection_dir.mkdir()
        _write_partition(collection_dir)  # writes collection_dir/data.parquet directly
        asset = collection_dir / "data.parquet"

        add_result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(asset)],
            catch_exceptions=False,
        )
        assert add_result.exit_code == 0, add_result.output
        # Collection-level asset: no item dir, file stays in the collection dir.
        assert asset.exists()

        rm_result = runner.invoke(
            cli,
            ["rm", "--portolan-dir", str(initialized_catalog), "--force", str(asset)],
            catch_exceptions=False,
        )
        assert rm_result.exit_code == 0, rm_result.output

        # File is gone, but the collection dir and its collection.json survive.
        assert not asset.exists()
        assert collection_dir.is_dir()
        assert (collection_dir / "collection.json").exists()
