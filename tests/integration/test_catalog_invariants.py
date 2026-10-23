"""Prove that each catalog invariant can fail, and that each known gap still fires.

``TestEachCodeFires`` builds a small catalog by hand that breaks no
invariant, changes one field, and asserts that exactly one code fires. A
code with no such test could be vacuous.

``TestKnownGapsStillFire`` reproduces each open bug in
``KNOWN_INVARIANT_GAPS`` through the real CLI and asserts that its code
fires. A fix deletes the entry and its test in the same commit.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from tests.catalog_invariants import (
    KNOWN_INVARIANT_GAPS,
    _contains,
    _union,
    check_catalog_invariants,
    codes,
)
from tests.properties.test_rebuild_idempotence import shrink_geoparquet

if TYPE_CHECKING:
    from tests.catalog_invariants import RunCli
    from tests.conftest import CliHarness

STALE_REASON = "catalog_invariants: a clean root that proves the stale-exemption check"

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
FIXTURES = REPO / "tests" / "fixtures"
POINTS = FIXTURES / "vector" / "valid" / "points.parquet"
RGB_COG = FIXTURES / "raster" / "valid" / "rgb.tif"
SINGLEBAND_COG = FIXTURES / "raster" / "valid" / "singleband.tif"

FILE_EXT = "https://stac-extensions.github.io/file/v2.1.0/schema.json"
TABLE_EXT = "https://stac-extensions.github.io/table/v1.2.0/schema.json"
PROJ_EXT = "https://stac-extensions.github.io/projection/v2.0.0/schema.json"
PARQUET = "application/vnd.apache.parquet"


def _dump(path: Path, data: dict[str, Any]) -> None:
    """Write ``data`` as JSON at ``path``, with its parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _parse(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(path.read_bytes())
    return data


def _write_geoparquet(path: Path, bbox: list[float]) -> None:
    """Write a two-column Parquet whose ``geo`` metadata declares ``bbox``."""
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": [], "bbox": bbox}},
    }
    table = pa.table({"id": [1, 2], "geometry": [b"\x00", b"\x00"]})
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    pq.write_table(table, path)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_clean_catalog(root: Path) -> Path:
    """Build a catalog that breaks no invariant: one collection, one item.

    The collection holds one collection-level GeoParquet asset with the bbox
    [0, 0, 2, 2]. The item holds a text asset and the bbox [0, 0, 1, 1].
    """
    (root / ".portolan").mkdir(parents=True)
    _dump(
        root / "catalog.json",
        {
            "type": "Catalog",
            "id": "clean",
            "stac_version": "1.1.0",
            "description": "A clean catalog.",
            "links": [{"rel": "child", "href": "./roads/collection.json"}],
        },
    )
    data = root / "roads" / "roads.parquet"
    data.parent.mkdir()
    _write_geoparquet(data, [0.0, 0.0, 2.0, 2.0])
    notes = root / "roads" / "scene" / "notes.txt"
    notes.parent.mkdir()
    notes.write_text("notes", encoding="utf-8")
    _dump(
        root / "roads" / "collection.json",
        {
            "type": "Collection",
            "id": "roads",
            "stac_version": "1.1.0",
            "stac_extensions": [FILE_EXT, TABLE_EXT, PROJ_EXT],
            "description": "Roads.",
            "license": "CC-BY-4.0",
            "extent": {
                "spatial": {"bbox": [[0.0, 0.0, 2.0, 2.0]]},
                "temporal": {"interval": [["2020-01-01T00:00:00Z", "2020-12-31T00:00:00Z"]]},
            },
            "table:columns": [{"name": "id"}, {"name": "geometry"}],
            "summaries": {"proj:code": ["EPSG:4326"]},
            "links": [
                {"rel": "root", "href": "../catalog.json"},
                {"rel": "parent", "href": "../catalog.json"},
                {"rel": "item", "href": "./scene/scene.json"},
            ],
            "assets": {
                "roads": {
                    "href": "./roads.parquet",
                    "type": PARQUET,
                    "roles": ["data"],
                    "file:size": data.stat().st_size,
                    "file:checksum": "1220" + _sha256(data),
                }
            },
        },
    )
    _dump(
        root / "roads" / "scene" / "scene.json",
        {
            "type": "Feature",
            "stac_version": "1.1.0",
            "stac_extensions": [FILE_EXT, PROJ_EXT],
            "id": "scene",
            "collection": "roads",
            "bbox": [0.0, 0.0, 1.0, 1.0],
            "geometry": None,
            "properties": {"datetime": "2020-06-01T00:00:00Z", "proj:code": "EPSG:4326"},
            "links": [{"rel": "collection", "href": "../collection.json"}],
            "assets": {
                "notes": {
                    "href": "./notes.txt",
                    "type": "text/plain",
                    "roles": ["metadata"],
                    "file:size": notes.stat().st_size,
                }
            },
        },
    )
    _dump(
        root / "roads" / "versions.json",
        {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "roads.parquet": {
                            "sha256": _sha256(data),
                            "size_bytes": data.stat().st_size,
                            "href": "roads/roads.parquet",
                        }
                    },
                }
            ],
        },
    )
    return root


Tamper = Callable[[Path], None]


def _edit(relative: str, change: Callable[[dict[str, Any]], None]) -> Tamper:
    def tamper(root: Path) -> None:
        path = root / relative
        data = _parse(path)
        change(data)
        _dump(path, data)

    return tamper


COLLECTION = "roads/collection.json"
ITEM = "roads/scene/scene.json"


def _roads(data: dict[str, Any]) -> dict[str, Any]:
    asset: dict[str, Any] = data["assets"]["roads"]
    return asset


def _source_asset(root: Path) -> None:
    (root / "roads" / "roads.geojson").write_text("{}", encoding="utf-8")
    _edit(
        COLLECTION,
        lambda c: c["assets"].update(
            source={"href": "./roads.geojson", "type": "application/geo+json", "roles": ["data"]}
        ),
    )(root)


def _override(root: Path, box: list[float]) -> None:
    """Declare ``box`` as the collection's metadata.yaml extent, and store it too."""
    metadata = root / "roads" / ".portolan" / "metadata.yaml"
    metadata.parent.mkdir()
    metadata.write_text(yaml.dump({"extent": {"spatial": {"bbox": [box]}}}), encoding="utf-8")
    _edit(COLLECTION, lambda c: c["extent"]["spatial"].update(bbox=[box]))(root)


def _world_override(root: Path) -> None:
    """Declare an override past the antimeridian, and store the same value."""
    _override(root, [-180.5, -90.0, 180.0, 90.0])


def _versions_size(data: dict[str, Any]) -> None:
    data["versions"][0]["assets"]["roads.parquet"]["size_bytes"] += 1


def _versions_second_key(data: dict[str, Any]) -> None:
    """Key the one data file a second time, as the #1053 thumbnail is."""
    assets = data["versions"][0]["assets"]
    assets["other.parquet"] = dict(assets["roads.parquet"])


TAMPERS: dict[str, Tamper] = {
    "P-SIZE": _edit(COLLECTION, lambda c: _roads(c).update({"file:size": 1})),
    "P-CHECKSUM": lambda root: _edit(
        COLLECTION,
        lambda c: _roads(c).update(
            {"file:checksum": "sha256:" + _sha256(root / "roads" / "roads.parquet")}
        ),
    )(root),
    "P-COLUMNS": _edit(COLLECTION, lambda c: c.update({"table:columns": [{"name": "id"}]})),
    # The #840 shape: an absolute self link from the machine that ran add.
    "P-NO-LOCAL-PATH": _edit(
        COLLECTION,
        lambda c: c["links"].append(
            {"rel": "self", "href": "/home/user/catalog/roads/collection.json"}
        ),
    ),
    "P-NO-CREDENTIAL": _edit(
        COLLECTION,
        lambda c: c["links"].append(
            {"rel": "via", "href": "https://example.org/wfs?service=WFS&API_KEY=secret"}
        ),
    ),
    "P-VERSIONS-MATCH": _edit("roads/versions.json", _versions_size),
    "P-VERSIONS-KEY": _edit("roads/versions.json", _versions_second_key),
    "P-DATA-ROLE-TYPE": _source_asset,
    "P-SUMMARY-FROM-ITEMS": _edit(
        COLLECTION, lambda c: c.update(summaries={"proj:code": ["EPSG:3857"]})
    ),
    "S-BBOX-COUNT": _edit(
        COLLECTION,
        lambda c: c["extent"]["spatial"].update(bbox=[[0.0, 0.0, 2.0, 2.0], [0.0, 0.0, 1.0, 1.0]]),
    ),
    "S-BBOX-RANGE": _world_override,
    "S-HREF-RESOLVES": _edit(
        ITEM, lambda i: i["links"].append({"rel": "license", "href": "./LICENSE"})
    ),
    "S-EXTENT-CONTAINS": _edit(ITEM, lambda i: i.update(bbox=[0.0, 0.0, 3.0, 1.0])),
    "S-EXTENT-NOT-STALE": _edit(
        COLLECTION, lambda c: c["extent"]["spatial"].update(bbox=[[-1.0, 0.0, 2.0, 2.0]])
    ),
    "S-TEMPORAL": _edit(
        COLLECTION,
        lambda c: c["extent"]["temporal"].update(
            interval=[["2021-01-01T00:00:00Z", "2020-01-01T00:00:00Z"]]
        ),
    ),
    "S-EXT-DECLARED": _edit(COLLECTION, lambda c: c.update(stac_extensions=[TABLE_EXT, PROJ_EXT])),
}


class TestEachCodeFires:
    def test_the_clean_catalog_breaks_no_invariant(self, tmp_path: Path) -> None:
        """The baseline the tampers start from, so each tamper owns its code."""
        assert check_catalog_invariants(build_clean_catalog(tmp_path / "clean")) == []

    @pytest.mark.parametrize("code", sorted(TAMPERS))
    def test_one_tamper_fires_exactly_its_code(self, code: str, tmp_path: Path) -> None:
        root = build_clean_catalog(tmp_path / "clean")
        TAMPERS[code](root)

        assert codes(check_catalog_invariants(root)) == {code}

    @pytest.mark.parametrize(
        ("stem", "codes_fired"), [("roads", {"P-DATA-ROLE-TYPE"}), ("census", set())]
    )
    def test_a_csv_is_data_only_without_a_parquet_twin(
        self, stem: str, codes_fired: set[str], tmp_path: Path
    ) -> None:
        """tabular.convert: false keeps census.csv as the data. roads.csv has a Parquet twin."""
        root = build_clean_catalog(tmp_path / "clean")
        (root / "roads" / f"{stem}.csv").write_text("a,b\n1,2\n", encoding="utf-8")
        _edit(
            COLLECTION,
            lambda c: c["assets"].update(
                table={"href": f"./{stem}.csv", "type": "text/csv", "roles": ["data"]}
            ),
        )(root)

        assert codes(check_catalog_invariants(root)) == codes_fired

    def test_a_declared_override_may_widen_the_extent(self, tmp_path: Path) -> None:
        """A human metadata.yaml override is the one way to widen an extent."""
        root = build_clean_catalog(tmp_path / "clean")
        box = [-1.0, 0.0, 2.0, 2.0]
        _override(root, box)

        assert check_catalog_invariants(root) == []

    def test_an_override_that_misses_the_data_fires(self, tmp_path: Path) -> None:
        root = build_clean_catalog(tmp_path / "clean")
        box = [0.0, 0.0, 1.0, 1.0]
        _override(root, box)

        # The override and the stored bbox agree and still contain the item,
        # but the data reaches [2, 2].
        assert codes(check_catalog_invariants(root)) == {"S-EXTENT-NOT-STALE"}

    def test_a_stale_exemption_fails_the_teardown(
        self,
        tmp_path: Path,
        run_cli: CliHarness,
        build_damaged_catalog: Callable[[Path, str], Path],
    ) -> None:
        """An exempt root that breaks no invariant fails, so a fixed bug drops its exemption."""
        root = build_damaged_catalog(tmp_path / "catalog", STALE_REASON)
        run_cli("init", str(root), "--auto", "--license", "CC-BY-4.0")

        with pytest.raises(pytest.fail.Exception) as raised:
            run_cli.verify()

        assert str(raised.value) == f"{root.resolve()}: exemption no longer needed: {STALE_REASON}"
        # The root is clean, so the teardown check passes without the exemption.
        run_cli.exempt.clear()

    def test_the_command_line_entry_point_exits_1_on_a_violation(
        self,
        tmp_path: Path,
        run_cli: RunCli,
        build_damaged_catalog: Callable[[Path, str], Path],
    ) -> None:
        """The form the nightly gate self-test runs: a real catalog, one size changed."""
        root = build_damaged_catalog(
            tmp_path / "catalog", "catalog_invariants: file:size set to 1 for the __main__ test"
        )
        (root / "roads").mkdir(parents=True)
        shutil.copy(POINTS, root / "roads" / "roads.parquet")
        run_cli("init", str(root), "--auto", "--license", "CC-BY-4.0")
        run_cli("add", "--portolan-dir", str(root), str(root / "roads" / "roads.parquet"))
        _edit("roads/collection.json", lambda c: _roads(c).update({"file:size": 1}))(root)

        result = subprocess.run(
            [sys.executable, "-m", "tests.catalog_invariants", str(root)],
            cwd=REPO,
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 1, result.stdout + result.stderr
        assert "P-SIZE roads/collection.json: asset 'roads': file:size 1 != " in result.stdout


class TestAntimeridianGeometry:
    def test_a_crossing_union_keeps_one_crossing_box(self) -> None:
        assert _union([(170.0, -10.0, -170.0, 0.0), (175.0, -5.0, 179.0, 5.0)]) == (
            170.0,
            -10.0,
            -170.0,
            5.0,
        )

    def test_a_crossing_box_contains_both_of_its_segments(self) -> None:
        crossing = (170.0, -10.0, -170.0, 0.0)

        assert _contains(crossing, (171.0, -9.0, 179.0, -1.0))
        assert _contains(crossing, (-179.0, -9.0, -171.0, -1.0))
        assert not _contains(crossing, (0.0, -9.0, 1.0, -1.0))


class TestKnownGapsStillFire:
    """Each ``KNOWN_INVARIANT_GAPS`` code still fires through the real CLI.

    The run_cli teardown drops these codes everywhere, so these tests are what
    keep the list honest. A fix deletes the entry and its test together.
    """

    @pytest.fixture
    def raster_catalog(self, tmp_path: Path, run_cli: RunCli) -> Path:
        """``add`` two raster scenes, the shape where both open gaps show."""
        root = tmp_path / "catalog"
        for scene, source in (("scene-a", RGB_COG), ("scene-b", SINGLEBAND_COG)):
            (root / "imagery" / scene).mkdir(parents=True)
            shutil.copy(source, root / "imagery" / scene / source.name)
        run_cli("init", str(root), "--auto", "--license", "CC-BY-4.0")
        run_cli("add", "--portolan-dir", str(root), str(root / "imagery"))
        return root

    def test_every_known_gap_has_a_test_here(self) -> None:
        assert set(KNOWN_INVARIANT_GAPS) == set(GAP_TESTS)

    def test_s_ext_declared(self, raster_catalog: Path) -> None:
        """#1037: the scene item lists file:size with no file extension URI."""
        fired = [v for v in check_catalog_invariants(raster_catalog) if v.code == "S-EXT-DECLARED"]

        assert sorted(v.path for v in fired) == [
            "imagery/scene-a/scene-a.json",
            "imagery/scene-b/scene-b.json",
        ]

    def test_p_summary_from_items(self, raster_catalog: Path) -> None:
        """#1023: the summary holds the numbers of proj:transform, not the arrays."""
        item = _parse(raster_catalog / "imagery" / "scene-b" / "scene-b.json")
        transform = item["properties"]["proj:transform"]
        summaries = _parse(raster_catalog / "imagery" / "collection.json")["summaries"]
        fired = {
            v.detail
            for v in check_catalog_invariants(raster_catalog)
            if v.code == "P-SUMMARY-FROM-ITEMS"
        }

        assert transform not in summaries["proj:transform"]
        assert f"summaries['proj:transform'] value {transform[0]!r} is on no member item" in fired

    def test_p_versions_key(self, raster_catalog: Path) -> None:
        """#1053: the scene thumbnail gets a second key at the collection root."""
        fired = [
            v.detail for v in check_catalog_invariants(raster_catalog) if v.code == "P-VERSIONS-KEY"
        ]

        assert sorted(fired) == [
            "key 'rgb.thumb.jpg' names a file other than imagery/scene-a/rgb.thumb.jpg",
        ]

    def test_p_data_role_type(self, tmp_path: Path, run_cli: RunCli) -> None:
        """#960: the converted GeoJSON stays registered as a data asset."""
        root = tmp_path / "catalog"
        (root / "places").mkdir(parents=True)
        shutil.copy(
            FIXTURES / "vector" / "valid" / "points.geojson", root / "places" / "points.geojson"
        )
        run_cli("init", str(root), "--auto", "--license", "CC-BY-4.0")
        run_cli("add", "--portolan-dir", str(root), str(root / "places" / "points.geojson"))

        fired = [v for v in check_catalog_invariants(root) if v.code == "P-DATA-ROLE-TYPE"]

        assert [v.detail for v in fired] == [
            "asset 'points.geojson': role data on './points.geojson' with type 'application/geo+json'"
        ]

    def test_s_extent_not_stale(self, tmp_path: Path, run_cli: RunCli) -> None:
        """#801: after the rows shrink, add --force keeps the old extent."""
        root = tmp_path / "catalog"
        data = root / "roads" / "roads.parquet"
        data.parent.mkdir(parents=True)
        shutil.copy(POINTS, data)
        run_cli("init", str(root), "--auto", "--license", "CC-BY-4.0")
        run_cli("add", "--portolan-dir", str(root), str(data))
        shrink_geoparquet(data)
        run_cli("add", "--force", "--portolan-dir", str(root), str(data))

        fired = [v for v in check_catalog_invariants(root) if v.code == "S-EXTENT-NOT-STALE"]

        assert [v.detail for v in fired] == [
            "bbox [-122.5, 37.7, -122.35, 37.85] != data union [-122.5, 37.7, -122.425, 37.7]"
        ]

    def test_p_no_local_path(self, tmp_path: Path, run_cli: RunCli) -> None:
        """#939: iceberg:catalog_uri names the sqlite file on this machine."""
        pytest.importorskip("pyiceberg")
        if sys.platform == "win32":
            pytest.skip("PyIceberg SQL catalog warehouse paths break on Windows")
        root = tmp_path / "catalog"
        data = root / "roads" / "roads.parquet"
        data.parent.mkdir(parents=True)
        shutil.copy(POINTS, data)
        run_cli("init", str(root), "--auto", "--license", "CC-BY-4.0")
        run_cli("config", "set", "backend", "iceberg", cwd=root)
        run_cli("add", "--portolan-dir", str(root), str(data))

        fired = [v for v in check_catalog_invariants(root) if v.code == "P-NO-LOCAL-PATH"]

        sqlite = f"sqlite:///{root.resolve()}/.portolan/iceberg.db"
        assert [v.detail for v in fired] == [f"local path {sqlite!r}"]


GAP_TESTS = {
    "P-VERSIONS-KEY": TestKnownGapsStillFire.test_p_versions_key,
    "P-NO-LOCAL-PATH": TestKnownGapsStillFire.test_p_no_local_path,
    "S-EXT-DECLARED": TestKnownGapsStillFire.test_s_ext_declared,
    "P-SUMMARY-FROM-ITEMS": TestKnownGapsStillFire.test_p_summary_from_items,
    "P-DATA-ROLE-TYPE": TestKnownGapsStillFire.test_p_data_role_type,
    "S-EXTENT-NOT-STALE": TestKnownGapsStillFire.test_s_extent_not_stale,
}
