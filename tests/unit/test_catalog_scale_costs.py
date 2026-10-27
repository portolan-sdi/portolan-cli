"""Counter tests for the catalog-scale suspects of issue #995 in the Lane B modules.

Each test names the suspect it holds and the pre-fix count that turns it red.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from portolan_cli import trace
from portolan_cli.versions import Asset, Version, VersionsFile, read_versions, write_versions

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

pytestmark = pytest.mark.unit

_OLD_NS = 1_600_000_000 * 1_000_000_000


@pytest.fixture
def counters() -> Iterator[Callable[[str], int]]:
    """Turn the trace counters on and return a reader for one counter."""
    trace.enable()
    try:
        yield lambda name: trace.counters().get(name, 0)
    finally:
        trace.reset()


def _tracked_collection(root: Path, n: int) -> Path:
    """A collection of ``n`` tracked files whose records match the disk."""
    coll = root / "c"
    coll.mkdir(parents=True)
    assets = {}
    for i in range(n):
        f = coll / f"f{i}.bin"
        data = f"file {i}".encode()
        f.write_bytes(data)
        st = f.stat()
        assets[f.name] = Asset(
            sha256=hashlib.sha256(data).hexdigest(),
            size_bytes=st.st_size,
            href=f"c/{f.name}",
            mtime=st.st_mtime,
        )
    write_versions(
        coll / "versions.json",
        VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime(2026, 1, 1, tzinfo=timezone.utc),
                    breaking=False,
                    assets=assets,
                    changes=list(assets),
                )
            ],
        ),
    )
    return coll


class TestStatusHashesNothingWhenUnchanged:
    """#995-1: detect_modified_files hashed every asset (10 here)."""

    def test_unchanged_collection(self, tmp_path: Path, counters: Callable[[str], int]) -> None:
        from portolan_cli.status import detect_modified_files

        coll = _tracked_collection(tmp_path, 10)
        assert detect_modified_files(coll, read_versions(coll / "versions.json")) == []
        assert counters("checksum.files") == 0

    def test_modified_file_is_still_found(self, tmp_path: Path) -> None:
        from portolan_cli.status import detect_modified_files

        coll = _tracked_collection(tmp_path, 3)
        (coll / "f1.bin").write_bytes(b"changed!")
        assert detect_modified_files(coll, read_versions(coll / "versions.json")) == ["f1.bin"]


class TestIsCurrentReadsVersionsOnce:
    """#995-2: is_current read versions.json once per candidate file (10 here)."""

    def test_shared_state(self, tmp_path: Path, counters: Callable[[str], int]) -> None:
        from portolan_cli.query import is_current
        from portolan_cli.run_state import RunState

        coll = _tracked_collection(tmp_path, 10)
        state = RunState.open(tmp_path)
        results = [
            is_current(coll / f"f{i}.bin", coll / "versions.json", state=state) for i in range(10)
        ]
        assert results == [True] * 10
        assert counters("versions_json.reads") == 1
        assert counters("checksum.files") == 0


class TestPublishReusesRecordedChecksums:
    """#995-5: JsonFileBackend.publish hashed every asset it got (5 here)."""

    def test_unchanged_assets(self, tmp_path: Path, counters: Callable[[str], int]) -> None:
        from portolan_cli.backends.json_file import JsonFileBackend

        coll = _tracked_collection(tmp_path, 5)
        version = JsonFileBackend(tmp_path).publish(
            "c",
            {f"f{i}.bin": str(coll / f"f{i}.bin") for i in range(5)},
            {"columns": [], "types": {}, "hash": "unknown"},
            False,
            "republish",
        )
        recorded = read_versions(coll / "versions.json").versions[0].assets
        assert {k: a.sha256 for k, a in version.assets.items()} == {
            k: a.sha256 for k, a in recorded.items()
        }
        assert counters("checksum.files") == 0

    def test_changed_asset_is_hashed(self, tmp_path: Path, counters: Callable[[str], int]) -> None:
        from portolan_cli.backends.json_file import JsonFileBackend

        coll = _tracked_collection(tmp_path, 2)
        (coll / "f0.bin").write_bytes(b"new bytes")
        version = JsonFileBackend(tmp_path).publish(
            "c",
            {"f0.bin": str(coll / "f0.bin")},
            {"columns": [], "types": {}, "hash": "unknown"},
            False,
            "change",
        )
        assert version.assets["f0.bin"].sha256 == hashlib.sha256(b"new bytes").hexdigest()
        assert counters("checksum.files") == 1


class TestPullFastPath:
    """#995-11: pull allowed 1 ms where the shared predicate allows 1 us or 2 s."""

    def test_whole_second_record_inside_two_seconds_is_not_hashed(
        self, tmp_path: Path, counters: Callable[[str], int]
    ) -> None:
        from portolan_cli.sync.pull import detect_uncommitted_changes

        coll = _tracked_collection(tmp_path, 1)
        f = coll / "f0.bin"
        os.utime(f, (1_600_000_000.0, 1_600_000_000.0))
        vf = read_versions(coll / "versions.json")
        asset = vf.versions[0].assets["f0.bin"]
        vf.versions[0].assets["f0.bin"] = Asset(
            sha256=asset.sha256, size_bytes=asset.size_bytes, href=asset.href, mtime=1_599_999_999.0
        )
        write_versions(coll / "versions.json", vf)

        assert detect_uncommitted_changes(tmp_path, "c") == []
        assert counters("checksum.files") == 0


class TestMetadataMtimePredicate:
    """#995-11: metadata freshness compared mtimes with an exact ``!=``."""

    def test_whole_second_record_inside_two_seconds_is_unchanged(self) -> None:
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
            file_path=Path("x.parquet"),
            current_mtime=1_600_000_001.5,
            stored_mtime=1_600_000_000.0,
            current_bbox=None,
            stored_bbox=None,
            current_feature_count=None,
            stored_feature_count=None,
            current_schema_fingerprint=None,
            stored_schema_fingerprint=None,
        )
        assert state.mtime_changed is False

    def test_fractional_record_one_millisecond_off_is_changed(self) -> None:
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
            file_path=Path("x.parquet"),
            current_mtime=1_600_000_000.251,
            stored_mtime=1_600_000_000.25,
            current_bbox=None,
            stored_bbox=None,
            current_feature_count=None,
            stored_feature_count=None,
            current_schema_fingerprint=None,
            stored_schema_fingerprint=None,
        )
        assert state.mtime_changed is True


def _items_collection(root: Path, n: int) -> Path:
    """A catalog with one collection of ``n`` items and a file-backend index."""
    (root / ".portolan").mkdir(parents=True)
    (root / ".portolan" / "config.yaml").write_text("{}\n")
    (root / "versions.json").write_text(json.dumps({"collections": {}}))
    coll = root / "c"
    coll.mkdir()
    links = []
    for i in range(n):
        item_dir = coll / f"i{i}"
        item_dir.mkdir()
        item = {
            "type": "Feature",
            "stac_version": "1.1.0",
            "id": f"i{i}",
            "geometry": {"type": "Point", "coordinates": [i, i]},
            "bbox": [i, i, i, i],
            "properties": {"datetime": "2024-01-01T00:00:00Z", "title": f"Item {i}"},
            "assets": {"data": {"href": f"./i{i}.tif", "roles": ["data"]}},
            "links": [],
            "collection": "c",
        }
        (item_dir / f"i{i}.json").write_text(json.dumps(item))
        links.append({"rel": "item", "href": f"./i{i}/i{i}.json"})
    collection = {
        "type": "Collection",
        "stac_version": "1.1.0",
        "id": "c",
        "description": "c",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[0, 0, n, n]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": links,
    }
    (coll / "collection.json").write_text(json.dumps(collection))
    return coll


class TestItemsParquetMirror:
    """#995-6 and #995-7: the mirror rebuilt on every run and hashed twice or more."""

    def test_mirror_is_hashed_once_per_run(
        self, tmp_path: Path, counters: Callable[[str], int]
    ) -> None:
        """Before the fix: 2 hashes on the first run (stamp, track), 2 on the next."""
        from portolan_cli.stac_parquet import generate_parquet_mirrors

        _items_collection(tmp_path, 3)
        generate_parquet_mirrors(tmp_path, {"c"}, generate_parquet=True)
        assert counters("checksum.files") == 1
        generate_parquet_mirrors(tmp_path, {"c"}, generate_parquet=True)
        assert counters("checksum.files") == 2

    def test_unchanged_items_do_not_rebuild(self, tmp_path: Path) -> None:
        from portolan_cli.stac_parquet import generate_items_parquet

        coll = _items_collection(tmp_path, 3)
        out = generate_items_parquet(coll)
        os.utime(out, ns=(_OLD_NS, _OLD_NS))
        generate_items_parquet(coll)
        assert out.stat().st_mtime_ns == _OLD_NS

    def test_changed_item_rebuilds(self, tmp_path: Path) -> None:
        import pyarrow.parquet as pq

        from portolan_cli.stac_parquet import generate_items_parquet

        coll = _items_collection(tmp_path, 3)
        out = generate_items_parquet(coll)
        item = coll / "i1" / "i1.json"
        data = json.loads(item.read_text())
        data["properties"]["title"] = "Renamed"
        item.write_text(json.dumps(data))
        generate_items_parquet(coll)
        titles = pq.read_table(out, columns=["title"]).column("title").to_pylist()
        assert titles == ["Item 0", "Renamed", "Item 2"]

    def test_added_item_rebuilds(self, tmp_path: Path) -> None:
        import pyarrow.parquet as pq

        from portolan_cli.stac_parquet import generate_items_parquet

        coll = _items_collection(tmp_path, 2)
        out = generate_items_parquet(coll)
        item_dir = coll / "i9"
        item_dir.mkdir()
        (item_dir / "i9.json").write_text((coll / "i0" / "i0.json").read_text().replace("i0", "i9"))
        doc = json.loads((coll / "collection.json").read_text())
        doc["links"].append({"rel": "item", "href": "./i9/i9.json"})
        (coll / "collection.json").write_text(json.dumps(doc))
        generate_items_parquet(coll)
        assert pq.read_table(out, columns=["id"]).column("id").to_pylist() == ["i0", "i1", "i9"]
