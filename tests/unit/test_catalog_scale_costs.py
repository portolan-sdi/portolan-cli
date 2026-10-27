"""Counter tests for the catalog-scale suspects of issue #995 in the Lane B modules.

Each test names the suspect it holds and the pre-fix count that turns it red.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from portolan_cli.versions import Asset, read_versions, write_versions

if TYPE_CHECKING:
    from collections.abc import Callable

pytestmark = pytest.mark.unit


@pytest.fixture
def tracked(
    tmp_path: Path, write_snapshot: Callable[[Path, dict[str, Asset]], None]
) -> Callable[[int], Path]:
    """Return a maker of a collection of ``n`` files whose records match the disk."""

    def make(n: int) -> Path:
        coll = tmp_path / "c"
        coll.mkdir(parents=True)
        assets = {}
        for i in range(n):
            f = coll / f"f{i}.bin"
            data = f"file {i}".encode()
            f.write_bytes(data)
            st = f.stat()
            assets[f.name] = Asset(
                hashlib.sha256(data).hexdigest(), st.st_size, f"c/{f.name}", mtime=st.st_mtime
            )
        write_snapshot(coll, assets)
        return coll

    return make


class TestStatusHashesNothingWhenUnchanged:
    """#995-1: detect_modified_files hashed every asset (10 here)."""

    def test_unchanged_collection(
        self, tmp_path: Path, tracked: Callable[[int], Path], counters: Callable[[str], int]
    ) -> None:
        from portolan_cli.status import detect_modified_files

        coll = tracked(10)
        assert detect_modified_files(coll, read_versions(coll / "versions.json")) == []
        assert counters("checksum.files") == 0

    def test_modified_file_is_still_found(
        self, tmp_path: Path, tracked: Callable[[int], Path]
    ) -> None:
        from portolan_cli.status import detect_modified_files

        coll = tracked(3)
        (coll / "f1.bin").write_bytes(b"changed!")
        assert detect_modified_files(coll, read_versions(coll / "versions.json")) == ["f1.bin"]


class TestIsCurrentReadsVersionsOnce:
    """#995-2: is_current read versions.json once per candidate file (10 here)."""

    def test_shared_state(
        self, tmp_path: Path, tracked: Callable[[int], Path], counters: Callable[[str], int]
    ) -> None:
        from portolan_cli.query import is_current
        from portolan_cli.run_state import RunState

        coll = tracked(10)
        state = RunState.open(tmp_path)
        results = [
            is_current(coll / f"f{i}.bin", coll / "versions.json", state=state) for i in range(10)
        ]
        assert results == [True] * 10
        assert counters("versions_json.reads") == 1
        assert counters("checksum.files") == 0


class TestPublishReusesRecordedChecksums:
    """#995-5: JsonFileBackend.publish hashed every asset it got (5 here)."""

    def test_unchanged_assets(
        self, tmp_path: Path, tracked: Callable[[int], Path], counters: Callable[[str], int]
    ) -> None:
        from portolan_cli.backends.json_file import JsonFileBackend

        coll = tracked(5)
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

    def test_changed_asset_is_hashed(
        self, tmp_path: Path, tracked: Callable[[int], Path], counters: Callable[[str], int]
    ) -> None:
        from portolan_cli.backends.json_file import JsonFileBackend

        coll = tracked(2)
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
        self, tmp_path: Path, tracked: Callable[[int], Path], counters: Callable[[str], int]
    ) -> None:
        from portolan_cli.sync.pull import detect_uncommitted_changes

        coll = tracked(1)
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
            current_mtime=1_600_000_001.0,
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
    """One collection of ``n`` items under the catalog at ``root``."""
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


@pytest.fixture
def mirror_writes(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    """Record each ``items.parquet`` that ``stac_geoparquet.arrow.to_parquet`` writes."""
    import stac_geoparquet.arrow

    writes: list[Path] = []
    write = stac_geoparquet.arrow.to_parquet

    def spy(table: Any, output_path: str | Path, **kwargs: Any) -> None:
        writes.append(Path(output_path))
        write(table, output_path, **kwargs)

    monkeypatch.setattr(stac_geoparquet.arrow, "to_parquet", spy)
    return writes


class TestItemsParquetMirror:
    """#995-6 and #995-7: the mirror rebuilt on every run and hashed twice or more."""

    def test_mirror_is_hashed_once_per_run(
        self, catalog: Path, counters: Callable[[str], int]
    ) -> None:
        """Before the fix: 2 hashes on the first run (stamp, track), 2 on the next."""
        from portolan_cli.stac_parquet import generate_parquet_mirrors

        _items_collection(catalog, 3)
        generate_parquet_mirrors(catalog, {"c"}, generate_parquet=True)
        assert counters("checksum.files") == 1
        generate_parquet_mirrors(catalog, {"c"}, generate_parquet=True)
        assert counters("checksum.files") == 2

    def test_unchanged_rerun_writes_no_mirror(
        self, catalog: Path, mirror_writes: list[Path], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The second run with unchanged items writes no ``items.parquet``.

        The private ``stac_geoparquet.arrow._to_parquet`` module is made
        unimportable, as a stac-geoparquet that moves it would do. Before the
        fix, the footer fingerprint needed that module, so the second run
        rebuilt the file.
        """
        from portolan_cli.stac_parquet import generate_parquet_mirrors

        monkeypatch.setitem(sys.modules, "stac_geoparquet.arrow._to_parquet", None)
        coll = _items_collection(catalog, 3)
        generate_parquet_mirrors(catalog, {"c"}, generate_parquet=True)
        assert mirror_writes == [coll / "items.parquet"]
        generate_parquet_mirrors(catalog, {"c"}, generate_parquet=True)
        assert mirror_writes == [coll / "items.parquet"]

    def test_fingerprint_is_in_the_versions_record(self, catalog: Path) -> None:
        import pyarrow.parquet as pq

        from portolan_cli.stac_parquet import generate_parquet_mirrors

        coll = _items_collection(catalog, 3)
        generate_parquet_mirrors(catalog, {"c"}, generate_parquet=True)
        record = read_versions(coll / "versions.json").versions[-1].assets["items.parquet"]
        assert record.source_fingerprint is not None
        assert len(record.source_fingerprint) == 64
        metadata = pq.read_schema(coll / "items.parquet").metadata
        assert sorted(metadata) == [b"geo", b"stac-geoparquet"]

    def test_replaced_mirror_rebuilds(self, catalog: Path, mirror_writes: list[Path]) -> None:
        from portolan_cli.stac_parquet import generate_parquet_mirrors

        coll = _items_collection(catalog, 3)
        generate_parquet_mirrors(catalog, {"c"}, generate_parquet=True)
        (coll / "items.parquet").write_bytes(b"not the recorded bytes")
        generate_parquet_mirrors(catalog, {"c"}, generate_parquet=True)
        assert mirror_writes == [coll / "items.parquet"] * 2

    def test_changed_item_rebuilds(self, catalog: Path) -> None:
        import pyarrow.parquet as pq

        from portolan_cli.stac_parquet import generate_parquet_mirrors

        coll = _items_collection(catalog, 3)
        generate_parquet_mirrors(catalog, {"c"}, generate_parquet=True)
        item = coll / "i1" / "i1.json"
        data = json.loads(item.read_text())
        data["properties"]["title"] = "Renamed"
        item.write_text(json.dumps(data))
        generate_parquet_mirrors(catalog, {"c"}, generate_parquet=True)
        titles = pq.read_table(coll / "items.parquet", columns=["title"]).column("title")
        assert titles.to_pylist() == ["Item 0", "Renamed", "Item 2"]

    def test_added_item_rebuilds(self, catalog: Path) -> None:
        import pyarrow.parquet as pq

        from portolan_cli.stac_parquet import generate_parquet_mirrors

        coll = _items_collection(catalog, 2)
        generate_parquet_mirrors(catalog, {"c"}, generate_parquet=True)
        item_dir = coll / "i9"
        item_dir.mkdir()
        (item_dir / "i9.json").write_text((coll / "i0" / "i0.json").read_text().replace("i0", "i9"))
        doc = json.loads((coll / "collection.json").read_text())
        doc["links"].append({"rel": "item", "href": "./i9/i9.json"})
        (coll / "collection.json").write_text(json.dumps(doc))
        generate_parquet_mirrors(catalog, {"c"}, generate_parquet=True)
        ids = pq.read_table(coll / "items.parquet", columns=["id"]).column("id").to_pylist()
        assert ids == ["i0", "i1", "i9"]
