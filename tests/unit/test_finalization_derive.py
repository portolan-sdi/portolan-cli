"""Tests for the finalization writers outside finalize_items (#801, #1046).

``refresh_collection_fields`` reports whether it changed collection.json,
because ``check --fix`` turns that answer into an UPDATED result.
``_batch_update_versions`` writes the version snapshot that ``push`` trusts.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import geopandas as gpd
import pystac
import pytest
import shapely

from portolan_cli.finalization import (
    _batch_update_versions,
    refresh_collection_fields,
)
from portolan_cli.metadata.detection import get_current_metadata
from portolan_cli.preparation import PreparedItem
from portolan_cli.stac import create_collection
from portolan_cli.sync.checksums import compute_checksum

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit


def _load(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return data


def _collection_with_asset(root: Path, extent: list[float]) -> Path:
    """A saved collection with one GeoParquet asset at lon 1..2, lat 1..2."""
    collection_dir = root / "coll"
    collection_dir.mkdir(parents=True)
    gpd.GeoDataFrame(
        {"id": [1, 2]}, geometry=[shapely.Point(1, 1), shapely.Point(2, 2)], crs="EPSG:4326"
    ).to_parquet(collection_dir / "data.parquet")
    collection = create_collection(collection_id="coll", description="d", bbox=extent)
    collection.add_asset(
        "data",
        pystac.Asset(
            href="./data.parquet", media_type="application/vnd.apache.parquet", roles=["data"]
        ),
    )
    data = collection.to_dict(include_self_link=False, transform_hrefs=False)
    (collection_dir / "collection.json").write_text(json.dumps(data), encoding="utf-8")
    return collection_dir


class TestRefreshCollectionFields:
    def test_a_wrong_extent_is_replaced_and_reported(self, tmp_path: Path) -> None:
        collection_dir = _collection_with_asset(tmp_path, [-20.0, 30.0, 10.0, 50.0])

        assert refresh_collection_fields(tmp_path, collection_dir) is True
        extent = _load(collection_dir / "collection.json")["extent"]["spatial"]["bbox"]
        assert extent == [[1.0, 1.0, 2.0, 2.0]]

    def test_a_derived_collection_reports_no_change(self, tmp_path: Path) -> None:
        collection_dir = _collection_with_asset(tmp_path, [1.0, 1.0, 2.0, 2.0])
        before = (collection_dir / "collection.json").read_text(encoding="utf-8")

        assert refresh_collection_fields(tmp_path, collection_dir) is False
        assert (collection_dir / "collection.json").read_text(encoding="utf-8") == before

    def test_no_collection_json_reports_no_change(self, tmp_path: Path) -> None:
        assert refresh_collection_fields(tmp_path, tmp_path / "missing") is False

    def test_an_unreadable_collection_reports_no_change(self, tmp_path: Path) -> None:
        collection_dir = tmp_path / "coll"
        collection_dir.mkdir()
        (collection_dir / "collection.json").write_text("{}", encoding="utf-8")

        assert refresh_collection_fields(tmp_path, collection_dir) is False
        assert (collection_dir / "collection.json").read_text(encoding="utf-8") == "{}"


def _prepared(collection_dir: Path) -> PreparedItem:
    """A collection-level GeoParquet asset, prepared as add prepares it."""
    data = collection_dir / "data.parquet"
    return PreparedItem(
        item_id="data",
        collection_id="coll",
        format_type="vector",
        bbox=[1.0, 1.0, 2.0, 2.0],
        asset_files={"data.parquet": (data, compute_checksum(data), data.stat().st_size)},
        item_json_path=None,
        is_collection_level_asset=True,
    )


class TestBatchUpdateVersions:
    def test_the_first_version_records_every_field(self, tmp_path: Path) -> None:
        collection_dir = _collection_with_asset(tmp_path, [1.0, 1.0, 2.0, 2.0])
        prepared = _prepared(collection_dir)
        data = collection_dir / "data.parquet"
        size = data.stat().st_size

        result = _batch_update_versions(collection_dir, "coll", [prepared])

        assert result == ("1.0.0", 1, size)
        versions = _load(collection_dir / "versions.json")
        assert (versions["spec_version"], versions["current_version"]) == ("1.0.0", "1.0.0")
        latest = versions["versions"][-1]
        assert (latest["version"], latest["breaking"]) == ("1.0.0", False)
        entry = latest["assets"]["data.parquet"]
        current = get_current_metadata(data)
        assert entry["sha256"] == compute_checksum(data)
        assert entry["size_bytes"] == size
        assert entry["href"] == "coll/data.parquet"
        assert entry["mtime"] == data.stat().st_mtime
        assert entry["source_mtime"] == data.stat().st_mtime
        assert entry["feature_count"] == 2
        assert entry["schema_fingerprint"] == current.current_schema_fingerprint

    def test_a_changed_file_gets_the_next_version(self, tmp_path: Path) -> None:
        collection_dir = _collection_with_asset(tmp_path, [1.0, 1.0, 2.0, 2.0])
        _batch_update_versions(collection_dir, "coll", [_prepared(collection_dir)])
        gpd.GeoDataFrame({"id": [1]}, geometry=[shapely.Point(1, 1)], crs="EPSG:4326").to_parquet(
            collection_dir / "data.parquet"
        )

        version, count, _size = _batch_update_versions(
            collection_dir, "coll", [_prepared(collection_dir)]
        )

        assert (version, count) == ("1.0.1", 1)
        assert _load(collection_dir / "versions.json")["current_version"] == "1.0.1"

    def test_an_unchanged_file_keeps_the_current_version(self, tmp_path: Path) -> None:
        """add_version records no new version, so the result names the current one."""
        collection_dir = _collection_with_asset(tmp_path, [1.0, 1.0, 2.0, 2.0])
        _batch_update_versions(collection_dir, "coll", [_prepared(collection_dir)])

        version, _count, _size = _batch_update_versions(
            collection_dir, "coll", [_prepared(collection_dir)]
        )

        assert version == "1.0.0"

    def test_no_asset_files_record_an_empty_version(self, tmp_path: Path) -> None:
        collection_dir = _collection_with_asset(tmp_path, [1.0, 1.0, 2.0, 2.0])
        empty = PreparedItem(
            item_id="data",
            collection_id="coll",
            format_type="vector",
            bbox=[1.0, 1.0, 2.0, 2.0],
            asset_files={},
            item_json_path=None,
            is_collection_level_asset=True,
        )

        assert _batch_update_versions(collection_dir, "coll", [empty]) == ("1.0.0", 0, 0)
