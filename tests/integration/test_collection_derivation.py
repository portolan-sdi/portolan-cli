"""Regression tests: every writer derives the collection fields from its members.

Each test runs the real CLI on the data an issue reported, and asserts the
exact value the issue expected. On origin/main at ff0c61b each one fails.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
import pytest
import shapely
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.metadata.detection import check_file_metadata, get_current_metadata
from portolan_cli.metadata.fix import fix_metadata
from portolan_cli.metadata.models import MetadataStatus
from portolan_cli.metadata.scan import scan_catalog_metadata

pytestmark = pytest.mark.integration

REALDATA = Path(__file__).resolve().parents[1] / "fixtures" / "realdata"
HOUSING = REALDATA / "philadelphia-housing" / "affordable_housing.parquet"
DISTRICTS = REALDATA / "philadelphia-housing" / "council_districts_2024.parquet"
RAPIDAI4EO = REALDATA / "rapidai4eo-sample.tif"
FIELDMAPS = REALDATA / "fieldmaps-boundaries.parquet"


def _run(*args: str) -> str:
    result = CliRunner().invoke(cli, list(args), catch_exceptions=False)
    assert result.exit_code == 0, result.output
    return result.output


def _catalog(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    _run("init", str(root), "--auto", "--license", "CC0-1.0", "--id", "derive")
    return root


def _load(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return data


def _extent(collection_dir: Path) -> list[list[float]]:
    bboxes: list[list[float]] = _load(collection_dir / "collection.json")["extent"]["spatial"][
        "bbox"
    ]
    return bboxes


def _added_raster(root: Path, collection: str, *flags: str) -> Path:
    """Add rapidai4eo-sample.tif as the item ``rapidai4eo`` and return the file."""
    tif = root / collection / "rapidai4eo" / "rapidai4eo.tif"
    tif.parent.mkdir(parents=True)
    shutil.copy(RAPIDAI4EO, tif)
    _run("add", "--portolan-dir", str(root), str(tif), *flags)
    return tif


def _points(path: Path, coords: list[tuple[float, float]], **columns: list[Any]) -> None:
    frame = gpd.GeoDataFrame(
        columns, geometry=[shapely.Point(x, y) for x, y in coords], crs="EPSG:4326"
    )
    frame.to_parquet(path)


class TestExtentFollowsTheMembers:
    """Issue #801: an extent can shrink, and a wrong one does not survive a rebuild."""

    def test_add_force_replaces_a_widened_extent(self, tmp_path: Path) -> None:
        """The issue's own procedure: widen the stored extent, then add --force."""
        root = _catalog(tmp_path / "cat")
        collection_dir = root / "forestal" / "housing"
        collection_dir.mkdir(parents=True)
        shutil.copy(HOUSING, collection_dir / "affordable_housing.parquet")
        _run("add", "--portolan-dir", str(root), str(collection_dir), "--no-thumbnails")
        correct = _extent(collection_dir)

        path = collection_dir / "collection.json"
        data = _load(path)
        data["extent"]["spatial"]["bbox"] = [[-20.0, 30.0, 10.0, 50.0]]
        path.write_text(json.dumps(data), encoding="utf-8")
        _run("add", "--portolan-dir", str(root), str(root), "--force", "--no-thumbnails")

        assert _extent(collection_dir) == correct

    def test_rm_shrinks_the_extent_to_the_files_that_remain(self, tmp_path: Path) -> None:
        """West at lon -7 and east at lon 20, added in two runs, as in the issue."""
        root = _catalog(tmp_path / "cat")
        collection_dir = root / "coll"
        collection_dir.mkdir()
        _points(collection_dir / "west.parquet", [(-7.1, 37.9), (-6.9, 38.1)], id=[1, 2])
        _run("add", "--portolan-dir", str(root), str(collection_dir / "west.parquet"))
        _points(collection_dir / "east.parquet", [(19.9, 49.9), (20.1, 50.1)], id=[1, 2])
        _run("add", "--portolan-dir", str(root), str(collection_dir / "east.parquet"))
        assert _extent(collection_dir) == [[-7.1, 37.9, 20.1, 50.1]]

        _run("rm", "--portolan-dir", str(root), "--force", str(collection_dir / "east.parquet"))

        assert _extent(collection_dir) == [[-7.1, 37.9, -6.9, 38.1]]
        assert sorted(_load(collection_dir / "collection.json")["assets"]) == ["thumbnail", "west"]

    def test_rm_of_an_item_equals_a_fresh_build_of_the_rest(self, tmp_path: Path) -> None:
        """``rm`` deletes the item directory, so its link, its files and its row go too."""
        sources = {"a": RAPIDAI4EO, "b": REALDATA.parent / "raster" / "valid" / "rgb.tif"}
        catalogs = {}
        for name, items in (("rebuilt", "ab"), ("fresh", "a")):
            root = _catalog(tmp_path / name)
            for item in items:
                (root / "imagery" / item).mkdir(parents=True)
                shutil.copy(sources[item], root / "imagery" / item / f"{item}.tif")
            _run("add", "--portolan-dir", str(root), str(root / "imagery"))
            catalogs[name] = root / "imagery"
        _run(
            "rm",
            "--portolan-dir",
            str(tmp_path / "rebuilt"),
            "--force",
            str(catalogs["rebuilt"] / "b" / "b.tif"),
        )

        def state(collection_dir: Path) -> dict[str, Any]:
            data = _load(collection_dir / "collection.json")
            versions = _load(collection_dir / "versions.json")["versions"][-1]["assets"]
            return {
                "links": [link for link in data["links"] if link["rel"] == "item"],
                "extent": data["extent"],
                "summaries": data["summaries"],
                "versions": sorted(versions),
                "mirror rows": pd.read_parquet(collection_dir / "items.parquet")["id"].tolist(),
            }

        assert state(catalogs["rebuilt"]) == state(catalogs["fresh"])
        # The snapshot records the bytes of the rewritten mirror, so push sends them.
        mirror = catalogs["rebuilt"] / "items.parquet"
        recorded = _load(catalogs["rebuilt"] / "versions.json")["versions"][-1]["assets"]
        assert (
            recorded["items.parquet"]["sha256"] == hashlib.sha256(mirror.read_bytes()).hexdigest()
        )

    def test_each_multi_file_asset_counts_once(self, tmp_path: Path) -> None:
        """The philadelphia-housing pair: the extent is the union of both files.

        Adding the second file alone must not drop the first file's coverage,
        so the extent reads the first file from disk.
        """
        root = _catalog(tmp_path / "cat")
        collection_dir = root / "housing"
        collection_dir.mkdir()
        shutil.copy(DISTRICTS, collection_dir / "council_districts_2024.parquet")
        _run("add", "--portolan-dir", str(root), str(collection_dir), "--no-thumbnails")
        districts = _extent(collection_dir)[0]
        shutil.copy(HOUSING, collection_dir / "affordable_housing.parquet")
        _run(
            "add",
            "--portolan-dir",
            str(root),
            str(collection_dir / "affordable_housing.parquet"),
            "--no-thumbnails",
        )

        assert _extent(collection_dir) == [districts]


class TestAntimeridianExtent:
    """Issue #852: a crossing extent is one bbox or three, never two."""

    def test_measured_crossing_writes_three_bboxes(self, tmp_path: Path) -> None:
        """FieldMaps in EPSG:4269: add measures the extent, which crosses 180."""
        root = _catalog(tmp_path / "cat")
        collection_dir = root / "boundaries"
        collection_dir.mkdir()
        gpd.read_parquet(FIELDMAPS).to_crs(4269).to_parquet(collection_dir / "boundaries.parquet")

        _run("add", "--portolan-dir", str(root), str(collection_dir), "--no-thumbnails")

        bboxes = _extent(collection_dir)
        assert len(bboxes) == 3
        envelope, western, eastern = bboxes
        assert (envelope[0], envelope[2]) == (-180.0, 180.0)
        assert (western[0], eastern[2]) == (-180.0, 180.0)
        assert western[2] < eastern[0]
        assert envelope[1::2] == western[1::2] == eastern[1::2]


class TestRemovedColumns:
    """Issue #931: a column the data lost leaves table:columns under every strategy."""

    @pytest.mark.parametrize("strategy", ["smart", "keep", "overwrite"])
    def test_force_drops_the_removed_column(self, tmp_path: Path, strategy: str) -> None:
        root = _catalog(tmp_path / "cat")
        collection_dir = root / "coll"
        collection_dir.mkdir()
        data = collection_dir / "coll.parquet"
        _points(data, [(0, 0), (1, 1)], id=[1, 2], a=[3, 4])
        _run("add", "--portolan-dir", str(root), str(collection_dir), "--no-thumbnails")
        _points(data, [(0, 0), (1, 1)], a=[3, 4])

        _run(
            "add",
            "--portolan-dir",
            str(root),
            str(collection_dir),
            "--force",
            "--merge-strategy",
            strategy,
            "--no-thumbnails",
        )

        columns = _load(collection_dir / "collection.json")["table:columns"]
        # geoparquet-io 1.7.0 appends the bbox column after the geometry.
        assert [column["name"] for column in columns] == ["a", "geometry", "bbox"]


_PROVIDERS_YAML = """providers:
  - name: City of Philadelphia
    roles: [producer, licensor]
"""
_PROVIDERS = [
    {"name": "City of Philadelphia", "roles": ["producer", "licensor"]},
]


class TestMetadataYamlOnForce:
    """Issue #813: add --force applies metadata.yaml to every collection kind."""

    def test_tabular_collection_takes_the_providers(self, tmp_path: Path) -> None:
        root = _catalog(tmp_path / "cat")
        config = root / ".portolan" / "config.yaml"
        config.write_text(config.read_text(encoding="utf-8") + "tabular:\n  enabled: true\n")
        collection_dir = root / "zoning_descriptions"
        collection_dir.mkdir()
        pd.DataFrame({"code": ["A", "B"], "label": ["x", "y"]}).to_parquet(
            collection_dir / "zoning.parquet"
        )
        _run("add", "--portolan-dir", str(root), str(collection_dir))
        (collection_dir / ".portolan").mkdir()
        (collection_dir / ".portolan" / "metadata.yaml").write_text(
            _PROVIDERS_YAML, encoding="utf-8"
        )

        _run("add", "--portolan-dir", str(root), str(collection_dir), "--force")

        assert _load(collection_dir / "collection.json")["providers"] == _PROVIDERS

    def test_inherited_providers_replace_a_seeded_host(self, tmp_path: Path) -> None:
        """A root providers array beats the host an inherited contact seeded."""
        root = tmp_path / "cat"
        (root / ".portolan").mkdir(parents=True)
        metadata = root / ".portolan" / "metadata.yaml"
        metadata.write_text("license: CC0-1.0\ncontact: data@example.org\n", encoding="utf-8")
        _catalog(root)
        collection_dir = root / "housing"
        collection_dir.mkdir()
        shutil.copy(HOUSING, collection_dir / "affordable_housing.parquet")
        _run("add", "--portolan-dir", str(root), str(collection_dir), "--no-thumbnails")
        assert [p["roles"] for p in _load(collection_dir / "collection.json")["providers"]] == [
            ["host"]
        ]
        metadata.write_text(metadata.read_text(encoding="utf-8") + _PROVIDERS_YAML)

        _run("add", "--portolan-dir", str(root), str(collection_dir), "--force", "--no-thumbnails")

        assert _load(collection_dir / "collection.json")["providers"] == [
            *_PROVIDERS,
            {"name": "data@example.org", "roles": ["host"], "email": "data@example.org"},
        ]


class TestRasterSummaries:
    """Issue #1023: a grid field is never summarized as a set of numbers."""

    def test_proj_transform_is_not_flattened(self, tmp_path: Path) -> None:
        root = _catalog(tmp_path / "cat")
        _added_raster(root, "imagery")

        summaries = _load(root / "imagery" / "collection.json")["summaries"]
        assert "proj:transform" not in summaries
        assert "proj:shape" not in summaries
        assert summaries["proj:code"] == ["EPSG:32633"]


class TestItemIdLayout:
    """Issue #840: --item-id names the item, not a directory."""

    def test_versions_keys_follow_the_files(self, tmp_path: Path) -> None:
        root = _catalog(tmp_path / "cat")
        item_dir = _added_raster(root, "imagery", "--item-id", "scene", "--no-thumbnails").parent

        versions = _load(root / "imagery" / "versions.json")
        assets = versions["versions"][-1]["assets"]
        assert assets["rapidai4eo/rapidai4eo.tif"]["href"] == "imagery/rapidai4eo/rapidai4eo.tif"
        assert not any(key.startswith("scene/") for key in assets)
        # The item sits beside its data, once, and its hrefs resolve there.
        assert sorted(p.name for p in (root / "imagery").rglob("scene.json")) == ["scene.json"]
        item = _load(item_dir / "scene.json")
        for asset in item["assets"].values():
            assert (item_dir / asset["href"]).is_file(), asset["href"]
        assert item["assets"]["data"]["href"] in ("rapidai4eo.tif", "./rapidai4eo.tif")


class TestCheckFixRecordsTheMirror:
    """Issue #1046: check --fix records the items.parquet it rewrites."""

    def test_snapshot_matches_the_regenerated_mirror(self, tmp_path: Path) -> None:
        """The issue's repro: delete the item, run check --fix, compare sizes.

        The recreated item is compared field by field here. The byte
        comparison with an added item is in test_check_fix_no_regression.py.
        """
        root = _catalog(tmp_path / "cat")
        item_dir = _added_raster(root, "imagery").parent
        mirror = root / "imagery" / "items.parquet"
        item_before = _load(item_dir / "rapidai4eo.json")
        (item_dir / "rapidai4eo.json").unlink()

        CliRunner().invoke(cli, ["check", str(root), "--fix"], catch_exceptions=False)

        versions = _load(root / "imagery" / "versions.json")
        snapshot = versions["versions"][-1]["assets"]
        assert snapshot["items.parquet"]["size_bytes"] == mirror.stat().st_size
        # The recreated item is the one add wrote: same datetime, proj:* and
        # raster:* fields, and the same data asset with its bands.
        item_after = _load(item_dir / "rapidai4eo.json")
        assert item_after["properties"] == item_before["properties"]
        data_before = dict(item_before["assets"]["data"])
        data_after = dict(item_after["assets"]["data"])
        # The first add computes the statistics. The second reads the stored ones.
        bands_before, bands_after = data_before.pop("bands"), data_after.pop("bands")
        data_before.pop("href"), data_after.pop("href")
        assert data_after == data_before
        assert [b["name"] for b in bands_after] == [b["name"] for b in bands_before]
        # The snapshot records exactly the files the recreated item declares.
        declared = {
            (item_dir / asset["href"])
            .resolve()
            .relative_to((root / "imagery").resolve())
            .as_posix()
            for asset in item_after["assets"].values()
        }
        tracked = {key for key in snapshot if key.startswith("rapidai4eo/")}
        assert tracked == declared
        for key in tracked:
            assert snapshot[key]["size_bytes"] == (root / "imagery" / key).stat().st_size
        for after, before in zip(bands_after, bands_before, strict=True):
            assert after["statistics"] == pytest.approx(before["statistics"])

    def test_a_nested_collection_records_its_full_id(self, tmp_path: Path) -> None:
        """The recreated item finds the catalog root, not the parent directory."""
        root = _catalog(tmp_path / "cat")
        collection_dir = root / "env" / "imagery"
        tif = _added_raster(root, "env/imagery", "--no-thumbnails")
        (tif.parent / "rapidai4eo.json").unlink()

        fix_metadata(root, scan_catalog_metadata(root))

        snapshot = _load(collection_dir / "versions.json")["versions"][-1]["assets"]
        assert snapshot["rapidai4eo/rapidai4eo.tif"]["href"] == (
            "env/imagery/rapidai4eo/rapidai4eo.tif"
        )


class TestRasterFreshness:
    """Issue #839: freshness never compares a native-CRS bbox with a WGS84 one."""

    def test_touched_projected_raster_is_fresh(self, tmp_path: Path) -> None:
        """The issue's own verification, with the hash fallback disabled.

        The stored sha256 does not match and a pixel count and a schema
        fingerprint are stored, so only the heuristics can decide.
        On main the UTM bbox in metres never equals the item's WGS84 bbox, so
        the touched raster read as changed content.
        """
        root = _catalog(tmp_path / "cat")
        tif = _added_raster(root, "imagery", "--no-thumbnails")

        versions_path = root / "imagery" / "versions.json"
        versions = _load(versions_path)
        entry = versions["versions"][-1]["assets"]["rapidai4eo/rapidai4eo.tif"]
        entry["sha256"] = "0" * 64
        current = get_current_metadata(tif)
        entry["feature_count"] = current.current_feature_count
        entry["schema_fingerprint"] = current.current_schema_fingerprint
        versions_path.write_text(json.dumps(versions), encoding="utf-8")
        stat = tif.stat()
        os.utime(tif, (stat.st_atime, stat.st_mtime + 10))

        result = check_file_metadata(tif, root / "imagery")

        assert result.status == MetadataStatus.FRESH
        assert result.message == "Metadata is up to date (touched_unchanged)"
