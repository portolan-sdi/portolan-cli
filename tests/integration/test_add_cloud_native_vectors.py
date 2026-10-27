"""Integration tests for adding cloud-native vector formats (PMTiles, FlatGeobuf).

Per Issue #368: Adding PMTiles/FlatGeobuf should work without conversion.
Vector files are collection-level assets.

These tests verify:
1. PMTiles/FlatGeobuf are NOT converted to GeoParquet
2. Files are copied to collection directory
3. Metadata is correctly extracted
4. Assets are registered in collection.json
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pyarrow.parquet as pq
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
    result = CliRunner().invoke(cli, ["init", str(tmp_path), "--auto", "--license", "CC-BY-4.0"])
    assert result.exit_code == 0, f"Init failed: {result.output}"
    return tmp_path


@pytest.fixture
def sample_pmtiles() -> Path:
    """Path to sample PMTiles fixture."""
    fixture_path = Path(__file__).parent.parent / "fixtures" / "cloud_native" / "sample.pmtiles"
    if not fixture_path.exists():
        pytest.skip("PMTiles fixture not found")
    return fixture_path


@pytest.fixture
def sample_fgb() -> Path:
    """Path to sample FlatGeobuf fixture."""
    fixture_path = Path(__file__).parent.parent / "fixtures" / "cloud_native" / "sample.fgb"
    if not fixture_path.exists():
        pytest.skip("FlatGeobuf fixture not found")
    return fixture_path


def _tiles_beside_geoparquet(catalog: Path, fixtures_dir: Path, sample_pmtiles: Path) -> Path:
    """Place ``tiles/map.parquet`` and its ``map.pmtiles``, and return the tiles."""
    collection_dir = catalog / "tiles"
    collection_dir.mkdir()
    shutil.copy(
        fixtures_dir / "vector" / "valid" / "points.parquet", collection_dir / "map.parquet"
    )
    tiles = collection_dir / "map.pmtiles"
    shutil.copy(sample_pmtiles, tiles)
    return tiles


def _add_tiles(runner: CliRunner, catalog: Path, tiles: Path) -> None:
    """Add ``tiles``, and check that the add succeeds."""
    result = runner.invoke(
        cli, ["add", "--portolan-dir", str(catalog), str(tiles)], catch_exceptions=False
    )
    assert result.exit_code == 0, f"Add failed: {result.output}"
    assert "No CRS found" not in result.output


class TestAddPMTiles:
    """Tests for adding PMTiles files beside the GeoParquet they render."""

    @pytest.mark.integration
    def test_add_pmtiles_keeps_the_file_and_converts_nothing(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_pmtiles: Path,
    ) -> None:
        """PMTiles are not converted, and the add raises no CRS error (#368)."""
        tiles = _tiles_beside_geoparquet(initialized_catalog, fixtures_dir, sample_pmtiles)

        _add_tiles(runner, initialized_catalog, tiles)

        assert tiles.read_bytes() == sample_pmtiles.read_bytes()
        assert sorted(p.name for p in tiles.parent.glob("*.parquet")) == ["map.parquet"]

    @pytest.mark.integration
    def test_add_pmtiles_registers_the_visual_and_its_web_map_link(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_pmtiles: Path,
    ) -> None:
        """core.md "Roles": ``visual`` for the PMTiles, ``data`` for the GeoParquet.

        formats.md "PMTiles" registers the tiles through a collection-level
        ``rel: "pmtiles"`` link.
        """
        tiles = _tiles_beside_geoparquet(initialized_catalog, fixtures_dir, sample_pmtiles)

        _add_tiles(runner, initialized_catalog, tiles)
        data = json.loads((tiles.parent / "collection.json").read_text())

        assert data["type"] == "Collection"
        assert {
            key: (a["href"], a["roles"])
            for key, a in data["assets"].items()
            if key.startswith("map")
        } == {
            "map": ("./map.parquet", ["data"]),
            "map-tiles": ("./map.pmtiles", ["visual"]),
        }
        assert [link for link in data["links"] if link["rel"] == "pmtiles"] == [
            {
                "rel": "pmtiles",
                "href": "./map.pmtiles",
                "type": "application/vnd.pmtiles",
                "pmtiles:layers": ["sample"],
            }
        ]
        assert (
            "https://stac-extensions.github.io/web-map-links/v1.3.0/schema.json"
            in data["stac_extensions"]
        )
        assert not list(tiles.parent.rglob("item.json"))

    @pytest.mark.integration
    def test_add_pmtiles_idempotent(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_pmtiles: Path,
    ) -> None:
        """Adding same PMTiles file twice is idempotent (no error, no duplicate)."""
        tiles = _tiles_beside_geoparquet(initialized_catalog, fixtures_dir, sample_pmtiles)
        collection_path = tiles.parent / "collection.json"
        _add_tiles(runner, initialized_catalog, tiles)
        first = json.loads(collection_path.read_text())

        _add_tiles(runner, initialized_catalog, tiles)

        assert json.loads(collection_path.read_text())["assets"].keys() == first["assets"].keys()


class TestAddFlatGeobuf:
    """Tests for adding FlatGeobuf files."""

    @pytest.mark.integration
    def test_add_flatgeobuf_no_conversion_error(
        self, runner: CliRunner, initialized_catalog: Path, sample_fgb: Path
    ) -> None:
        """Adding FlatGeobuf should NOT fail with CRS extraction error."""
        collection_dir = initialized_catalog / "boundaries"
        collection_dir.mkdir()
        test_file = collection_dir / "borders.fgb"
        shutil.copy(sample_fgb, test_file)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, f"Add failed: {result.output}"

    @pytest.mark.integration
    def test_add_flatgeobuf_preserves_format(
        self, runner: CliRunner, initialized_catalog: Path, sample_fgb: Path
    ) -> None:
        """FlatGeobuf should be copied, not converted to GeoParquet."""
        collection_dir = initialized_catalog / "boundaries"
        collection_dir.mkdir()
        test_file = collection_dir / "borders.fgb"
        shutil.copy(sample_fgb, test_file)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        # FlatGeobuf should still exist (not converted)
        assert test_file.exists() or (collection_dir / "borders.fgb").exists()
        # Should NOT have created a .parquet file from conversion
        parquet_files = list(collection_dir.rglob("*.parquet"))
        data_parquets = [p for p in parquet_files if p.name != "items.parquet"]
        assert len(data_parquets) == 0, f"Unexpected parquet conversion: {data_parquets}"

    @pytest.mark.integration
    def test_add_flatgeobuf_extracts_crs(
        self, runner: CliRunner, initialized_catalog: Path, sample_fgb: Path
    ) -> None:
        """FlatGeobuf CRS should be extracted and recorded in collection."""
        collection_dir = initialized_catalog / "boundaries"
        collection_dir.mkdir()
        test_file = collection_dir / "borders.fgb"
        shutil.copy(sample_fgb, test_file)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        # CRS lands as proj:code on the data asset, never top-level (#654)
        collection_json = collection_dir / "collection.json"
        assert collection_json.exists()

        data = json.loads(collection_json.read_text())
        assert "proj:epsg" not in data
        assert data["assets"]["borders"]["proj:code"] == "EPSG:4326"
        assert data.get("flatgeobuf:geometry_type") == "Point"
        assert data.get("flatgeobuf:feature_count") == 3


class TestCollectionLevelAssetBehavior:
    """Tests for: Collection-level asset registration."""

    @pytest.mark.integration
    def test_pmtiles_stac_properties_in_collection(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_pmtiles: Path,
    ) -> None:
        """PMTiles properties should be in collection.json (not item)."""
        tiles = _tiles_beside_geoparquet(initialized_catalog, fixtures_dir, sample_pmtiles)

        _add_tiles(runner, initialized_catalog, tiles)
        data = json.loads((tiles.parent / "collection.json").read_text())

        # The tile CRS is a visualization artifact and contributes no
        # projection field (#488, #654)
        assert "proj:epsg" not in data
        assert "proj:code" not in data
        assert "proj:code" not in data["assets"]["map-tiles"]

        # PMTiles-specific properties
        assert data.get("pmtiles:min_zoom") == 4
        assert data.get("pmtiles:max_zoom") == 8
        assert data.get("pmtiles:tile_type") == "mvt"

    @pytest.mark.integration
    def test_flatgeobuf_is_collection_level_asset(
        self, runner: CliRunner, initialized_catalog: Path, sample_fgb: Path
    ) -> None:
        """FlatGeobuf should be registered as collection-level asset (no item.json)."""
        collection_dir = initialized_catalog / "boundaries"
        collection_dir.mkdir()
        test_file = collection_dir / "borders.fgb"
        shutil.copy(sample_fgb, test_file)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        # No item.json should exist
        item_jsons = list(collection_dir.rglob("item.json"))
        assert len(item_jsons) == 0, f"Unexpected item.json: {item_jsons}"

        # Asset should be in collection.json
        data = json.loads((collection_dir / "collection.json").read_text())
        assert "borders" in data.get("assets", {})
        assert data["assets"]["borders"]["type"] == "application/vnd.flatgeobuf"


class TestSameStemGeoParquetAndPMTiles:
    """Issue #933: a same-stem PMTiles never replaces the GeoParquet asset.

    ``iterdir`` order decided which of ``<stem>.parquet`` and ``<stem>.pmtiles``
    kept the ``<stem>`` key. Eight collections give several orders on one
    filesystem, so the pre-fix code loses the GeoParquet in some of them.
    """

    _STEMS = [f"zone{i}" for i in range(8)]

    def _make_collections(self, catalog: Path, fixtures_dir: Path, sample_pmtiles: Path) -> None:
        parquet = fixtures_dir / "realdata" / "road-detections.parquet"
        for stem in self._STEMS:
            collection_dir = catalog / stem
            collection_dir.mkdir()
            shutil.copy(parquet, collection_dir / f"{stem}.parquet")
            shutil.copy(sample_pmtiles, collection_dir / f"{stem}.pmtiles")

    @staticmethod
    def _shape(collection_dir: Path) -> dict[str, tuple[str, str, list[str]]]:
        """Data and visual assets only. ``add`` also writes a thumbnail asset."""
        data = json.loads((collection_dir / "collection.json").read_text())
        return {
            key: (asset["href"], asset["type"], asset["roles"])
            for key, asset in data["assets"].items()
            if {"data", "visual"} & set(asset["roles"])
        }

    @staticmethod
    def _expected(stem: str) -> dict[str, tuple[str, str, list[str]]]:
        return {
            stem: (f"./{stem}.parquet", "application/vnd.apache.parquet", ["data"]),
            f"{stem}-tiles": (f"./{stem}.pmtiles", "application/vnd.pmtiles", ["visual"]),
        }

    @pytest.mark.integration
    def test_add_registers_both_assets_with_distinct_keys(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_pmtiles: Path,
    ) -> None:
        self._make_collections(initialized_catalog, fixtures_dir, sample_pmtiles)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(initialized_catalog)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output

        for stem in self._STEMS:
            assert self._shape(initialized_catalog / stem) == self._expected(stem), stem

    @pytest.mark.integration
    def test_add_force_repairs_a_collection_damaged_by_933(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_pmtiles: Path,
    ) -> None:
        """A 0.8.0 catalog with the PMTiles under the parquet key recovers."""
        self._make_collections(initialized_catalog, fixtures_dir, sample_pmtiles)
        args = ["add", "--portolan-dir", str(initialized_catalog), str(initialized_catalog)]
        assert runner.invoke(cli, args, catch_exceptions=False).exit_code == 0

        # Write the exact damage the issue reports into one collection.
        damaged = initialized_catalog / "zone0"
        collection_path = damaged / "collection.json"
        data = json.loads(collection_path.read_text())
        tiles = data["assets"].pop("zone0-tiles")
        tiles["roles"] = ["data"]
        data["assets"]["zone0"] = tiles
        collection_path.write_text(json.dumps(data))

        result = runner.invoke(cli, [*args, "--force"], catch_exceptions=False)
        assert result.exit_code == 0, result.output

        assert self._shape(damaged) == self._expected("zone0")

    @pytest.mark.integration
    def test_a_file_named_like_the_tiles_key_skips_the_add(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_pmtiles: Path,
    ) -> None:
        """The file ``zoning-tiles`` replaced the PMTiles asset with no error.

        The skip comes before any write, and the converted output goes too.
        """
        collection_dir = initialized_catalog / "land"
        collection_dir.mkdir()
        shutil.copy(
            fixtures_dir / "vector" / "valid" / "points.geojson", collection_dir / "zoning.geojson"
        )
        shutil.copy(sample_pmtiles, collection_dir / "zoning.pmtiles")
        (collection_dir / "zoning-tiles").write_bytes(b"notes")

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                str(collection_dir / "zoning.geojson"),
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        assert (
            "Skipping zoning.geojson: Collection 'land': asset key 'zoning-tiles' already holds "
            "'zoning.pmtiles'. Adding 'zoning-tiles' under the same key would remove that "
            "asset. Remove one of the two files from the collection, or rename it."
        ) in " ".join(result.output.split())
        assert sorted(p.name for p in collection_dir.iterdir()) == [
            "zoning-tiles",
            "zoning.geojson",
            "zoning.pmtiles",
        ]

    @staticmethod
    def _track_under_key(
        runner: CliRunner, catalog: Path, collection_dir: Path, sample_fgb: Path, key: str
    ) -> None:
        """Track ``legacy.fgb`` under ``key``, as a hand edit of collection.json can."""
        collection_dir.mkdir()
        shutil.copy(sample_fgb, collection_dir / "legacy.fgb")
        legacy = str(collection_dir / "legacy.fgb")
        result = runner.invoke(cli, ["add", "--portolan-dir", str(catalog), legacy])
        assert result.exit_code == 0, result.output
        collection_path = collection_dir / "collection.json"
        data = json.loads(collection_path.read_text())
        data["assets"][key] = data["assets"].pop("legacy")
        collection_path.write_text(json.dumps(data))

    @pytest.mark.integration
    def test_add_refuses_a_parquet_that_would_replace_another_file(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_fgb: Path,
    ) -> None:
        """A GeoParquet never silently replaces the file its key already holds."""
        collection_dir = initialized_catalog / "roads"
        self._track_under_key(runner, initialized_catalog, collection_dir, sample_fgb, "roads")
        base = ["--portolan-dir", str(initialized_catalog)]
        collection_before = (collection_dir / "collection.json").read_text()
        versions_before = (collection_dir / "versions.json").read_text()

        # This fixture carries a bbox covering. add then rewrites nothing and
        # prints no progress line before the JSON envelope.
        parquet = collection_dir / "roads.parquet"
        housing = fixtures_dir / "realdata" / "philadelphia-housing"
        shutil.copy(housing / "council_districts_2024.parquet", parquet)
        result = runner.invoke(cli, ["--format=json", "add", *base, str(parquet)])

        assert result.exit_code == 1, result.output
        envelope = json.loads(result.stdout)
        assert envelope["success"] is False
        assert envelope["errors"][0]["type"] == "AssetKeyCollisionError"
        assert envelope["errors"][0]["code"] == "PRTLN-COL003"
        assert (collection_dir / "collection.json").read_text() == collection_before
        assert (collection_dir / "versions.json").read_text() == versions_before

    @pytest.mark.integration
    def test_parquet_added_after_a_same_stem_flatgeobuf_keeps_both(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_fgb: Path,
    ) -> None:
        """Two adds give the same keys as one add of both files."""
        collection_dir = initialized_catalog / "roads"
        collection_dir.mkdir()
        shutil.copy(sample_fgb, collection_dir / "roads.fgb")
        base = ["add", "--portolan-dir", str(initialized_catalog)]
        result = runner.invoke(cli, [*base, str(collection_dir / "roads.fgb")])
        assert result.exit_code == 0, result.output

        housing = fixtures_dir / "realdata" / "philadelphia-housing"
        parquet = collection_dir / "roads.parquet"
        shutil.copy(housing / "council_districts_2024.parquet", parquet)
        result = runner.invoke(cli, [*base, str(parquet)])
        assert result.exit_code == 0, result.output

        data = json.loads((collection_dir / "collection.json").read_text())
        assert data["assets"]["roads"]["href"] == "./roads.parquet"
        assert data["assets"]["roads.fgb"]["href"] == "./roads.fgb"
        assert data["assets"]["roads.fgb"]["type"] == "application/vnd.flatgeobuf"

    @pytest.mark.integration
    def test_add_force_repairs_a_shapefile_under_the_parquet_key(
        self, runner: CliRunner, initialized_catalog: Path, fixtures_dir: Path
    ) -> None:
        """0.8.0 could leave the source or a sidecar under the GeoParquet key."""
        collection_dir = initialized_catalog / "radios"
        collection_dir.mkdir()
        source = fixtures_dir / "scan" / "complete_shapefile_wgs84"
        for part in source.glob("radios_sample.*"):
            shutil.copy(part, collection_dir / part.name)
        args = ["add", "--portolan-dir", str(initialized_catalog), str(collection_dir)]
        assert runner.invoke(cli, args, catch_exceptions=False).exit_code == 0

        # Write the damage: the .dbf sidecar holds the GeoParquet key.
        collection_path = collection_dir / "collection.json"
        data = json.loads(collection_path.read_text())
        data["assets"]["radios_sample"] = data["assets"].pop("radios_sample.dbf")
        collection_path.write_text(json.dumps(data))

        result = runner.invoke(cli, [*args, "--force"], catch_exceptions=False)
        assert result.exit_code == 0, result.output

        assets = json.loads(collection_path.read_text())["assets"]
        assert assets["radios_sample"]["href"] == "./radios_sample.parquet"
        assert assets["radios_sample"]["type"] == "application/vnd.apache.parquet"
        assert assets["radios_sample.dbf"]["href"] == "./radios_sample.dbf"

    @pytest.mark.integration
    def test_collision_in_one_collection_writes_no_collection(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_fgb: Path,
    ) -> None:
        """A refused add leaves every collection of the batch unwritten."""
        base = ["--portolan-dir", str(initialized_catalog)]
        colliding = initialized_catalog / "zzz"
        self._track_under_key(runner, initialized_catalog, colliding, sample_fgb, "roads")

        housing = fixtures_dir / "realdata" / "philadelphia-housing"
        clean = initialized_catalog / "aaa"
        clean.mkdir()
        shutil.copy(housing / "council_districts_2024.parquet", clean / "zone.parquet")
        shutil.copy(housing / "council_districts_2024.parquet", colliding / "roads.parquet")

        result = runner.invoke(
            cli,
            ["add", *base, str(clean / "zone.parquet"), str(colliding / "roads.parquet")],
        )

        assert result.exit_code == 1, result.output
        assert not (clean / "collection.json").exists()
        assert not (clean / "versions.json").exists()

    @pytest.mark.integration
    def test_single_file_add_keys_another_stems_pair_correctly(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_pmtiles: Path,
    ) -> None:
        """The scan of ``b.parquet`` also sees ``aN.parquet`` and ``aN.pmtiles``.

        Six collections give several ``iterdir`` orders of the pair.
        """
        housing = fixtures_dir / "realdata" / "philadelphia-housing"
        base = ["add", "--portolan-dir", str(initialized_catalog)]
        for i in range(6):
            collection_dir = initialized_catalog / f"c{i}"
            collection_dir.mkdir()
            shutil.copy(
                housing / "council_districts_2024.parquet", collection_dir / f"a{i}.parquet"
            )
            result = runner.invoke(cli, [*base, str(collection_dir / f"a{i}.parquet")])
            assert result.exit_code == 0, result.output
            shutil.copy(sample_pmtiles, collection_dir / f"a{i}.pmtiles")
            shutil.copy(housing / "council_districts_2024.parquet", collection_dir / "b.parquet")

            result = runner.invoke(cli, [*base, str(collection_dir / "b.parquet")])

            assert result.exit_code == 0, result.output
            assets = json.loads((collection_dir / "collection.json").read_text())["assets"]
            assert assets[f"a{i}"]["href"] == f"./a{i}.parquet"
            assert assets[f"a{i}-tiles"]["href"] == f"./a{i}.pmtiles"
            assert assets[f"a{i}-tiles"]["roles"] == ["visual"]

    @pytest.mark.integration
    def test_tiles_beside_a_plain_table_are_refused(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_pmtiles: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A Parquet without GeoParquet metadata is no source of the tiles."""
        monkeypatch.chdir(initialized_catalog)
        result = runner.invoke(cli, ["config", "set", "tabular.enabled", "true"])
        assert result.exit_code == 0, result.output
        collection_dir = initialized_catalog / "roads"
        collection_dir.mkdir()
        table = fixtures_dir / "scan" / "geoparquet_with_companions" / "lookup.parquet"
        shutil.copy(table, collection_dir / "roads.parquet")
        base = ["add", "--portolan-dir", str(initialized_catalog)]
        result = runner.invoke(cli, [*base, str(collection_dir / "roads.parquet")])
        assert result.exit_code == 0, result.output
        shutil.copy(sample_pmtiles, collection_dir / "roads.pmtiles")

        result = runner.invoke(cli, [*base, str(collection_dir / "roads.pmtiles")])

        assert result.exit_code == 0, result.output
        assert (
            "Skipping roads.pmtiles: PMTiles is a visualization derivative. "
            "Add the GeoParquet it renders, and keep the tiles beside it."
            in " ".join(result.output.split())
        )
        assets = json.loads((collection_dir / "collection.json").read_text())["assets"]
        assert {
            key: (a["href"], a["roles"]) for key, a in assets.items() if key.startswith("roads")
        } == {
            "roads": ("./roads.parquet", ["data"]),
        }


def _write_providers(catalog: Path) -> None:
    """Declare one producer and host, so PTL-PRV-001 does not mask the findings."""
    (catalog / ".portolan" / "metadata.yaml").write_text(
        "license: CC-BY-4.0\n"
        "source: Example open-data portal\n"
        "providers:\n"
        "  - name: Example Agency\n"
        "    roles: [producer, host]\n"
        "    url: https://example.org\n",
        encoding="utf-8",
    )


def _errors(runner: CliRunner, catalog: Path) -> list[str]:
    """Rule ids of the error findings ``portolan check`` reports."""
    result = runner.invoke(cli, ["--format=json", "check", str(catalog)])
    findings = json.loads(result.stdout)["data"]["findings"]
    return sorted(f["rule_id"] for f in findings if f["severity"] == "error")


class TestAssetLineage:
    """The converted file is data, its source is source, its tiles are visual."""

    @pytest.mark.integration
    def test_a_failed_add_keeps_the_tiles_beside_its_source(
        self, runner: CliRunner, initialized_catalog: Path, sample_pmtiles: Path
    ) -> None:
        """The cleanup of an output with no geometry removed every same-stem file.

        The GeoJSON passes the pre-check, because its one geometry is not null.
        The conversion gives no bbox, and the cleanup deleted ``zoning.pmtiles``.
        """
        collection_dir = initialized_catalog / "land"
        collection_dir.mkdir()
        empty = {"type": "GeometryCollection", "geometries": []}
        feature = {"type": "Feature", "properties": {"a": 1}, "geometry": empty}
        source = {"type": "FeatureCollection", "features": [feature]}
        (collection_dir / "zoning.geojson").write_text(json.dumps(source))
        shutil.copy(sample_pmtiles, collection_dir / "zoning.pmtiles")

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                str(collection_dir / "zoning.geojson"),
            ],
        )

        assert "missing bounding box" in result.output
        assert sorted(p.name for p in collection_dir.iterdir()) == [
            "zoning.geojson",
            "zoning.pmtiles",
        ]
        assert (collection_dir / "zoning.pmtiles").read_bytes() == sample_pmtiles.read_bytes()

    @pytest.mark.integration
    def test_converted_geojson_is_the_source_of_the_geoparquet(
        self, runner: CliRunner, initialized_catalog: Path, fixtures_dir: Path
    ) -> None:
        """Issue #960: the GeoJSON carried the data role beside its GeoParquet."""
        collection_dir = initialized_catalog / "points"
        collection_dir.mkdir()
        shutil.copy(fixtures_dir / "vector" / "valid" / "points.geojson", collection_dir)

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                str(collection_dir / "points.geojson"),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output

        assets = json.loads((collection_dir / "collection.json").read_text())["assets"]
        assert {key: (a["href"], a["type"], a["roles"]) for key, a in assets.items()} == {
            "points": ("./points.parquet", "application/vnd.apache.parquet", ["data"]),
            "points.geojson": ("./points.geojson", "application/geo+json", ["source"]),
            "thumbnail": ("./points.thumb.jpg", "image/jpeg", ["thumbnail"]),
        }
        check = runner.invoke(cli, ["check", str(initialized_catalog), "--metadata"])
        assert "not registered" not in check.output

    @pytest.mark.integration
    def test_prebuilt_sibling_pmtiles_passes_check(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_pmtiles: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Issues #929 and #934: tiles built outside Portolan, no tippecanoe, no flag."""
        monkeypatch.setattr("portolan_cli.viz.pmtiles.shutil.which", lambda _name: None)
        _write_providers(initialized_catalog)
        collection_dir = initialized_catalog / "points"
        collection_dir.mkdir()
        shutil.copy(fixtures_dir / "vector" / "valid" / "points.parquet", collection_dir)
        shutil.copy(sample_pmtiles, collection_dir / "points.pmtiles")

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                str(collection_dir / "points.parquet"),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output

        collection = json.loads((collection_dir / "collection.json").read_text())
        assert collection["assets"]["points-tiles"]["roles"] == ["visual"]
        assert [link["href"] for link in collection["links"] if link["rel"] == "pmtiles"] == [
            "./points.pmtiles"
        ]
        assert _errors(runner, initialized_catalog) == []

    @pytest.mark.integration
    def test_pmtiles_flag_registers_prebuilt_tiles_without_tippecanoe(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_pmtiles: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Issue #934: the tippecanoe preflight ran before the up-to-date skip."""
        monkeypatch.setattr("portolan_cli.viz.pmtiles.shutil.which", lambda _name: None)
        collection_dir = initialized_catalog / "points"
        collection_dir.mkdir()
        shutil.copy(fixtures_dir / "vector" / "valid" / "points.parquet", collection_dir)
        args = ["add", "--portolan-dir", str(initialized_catalog)]
        parquet = str(collection_dir / "points.parquet")
        assert runner.invoke(cli, [*args, parquet], catch_exceptions=False).exit_code == 0
        # The tiles are newer than the GeoParquet, so nothing needs tippecanoe.
        shutil.copy(sample_pmtiles, collection_dir / "points.pmtiles")

        result = runner.invoke(cli, [*args, "--pmtiles", parquet], catch_exceptions=False)

        assert result.exit_code == 0, result.output
        assert "tippecanoe" not in result.output
        collection = json.loads((collection_dir / "collection.json").read_text())
        assert collection["assets"]["points-tiles"]["href"] == "./points.pmtiles"

    @staticmethod
    def _prebuilt_tiles(
        catalog: Path, fixtures_dir: Path, sample_pmtiles: Path, mtime: int
    ) -> Path:
        """Place ``points.parquet`` and pre-built tiles with a pinned mtime."""
        import os

        collection_dir = catalog / "points"
        collection_dir.mkdir()
        shutil.copy(fixtures_dir / "vector" / "valid" / "points.parquet", collection_dir)
        tiles = collection_dir / "points.pmtiles"
        shutil.copy(sample_pmtiles, tiles)
        os.utime(tiles, (mtime, mtime))
        return collection_dir

    @staticmethod
    def _verbose_add(
        runner: CliRunner, catalog: Path, collection_dir: Path, *flags: str
    ) -> list[str]:
        """Run verbose ``add`` on ``points.parquet`` and return the stripped lines."""
        result = runner.invoke(
            cli,
            [
                "add",
                "--verbose",
                *flags,
                "--portolan-dir",
                str(catalog),
                str(collection_dir / "points.parquet"),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        return [line.strip() for line in result.output.splitlines()]

    @staticmethod
    def _record_builds(monkeypatch: pytest.MonkeyPatch, sample_pmtiles: Path) -> list[str]:
        """Let a build pass the preflight and record the tiles each build writes."""
        built: list[str] = []

        def record(parquet_path: Path, pmtiles_path: Path, **_kwargs: object) -> None:
            built.append(f"{parquet_path.name} -> {pmtiles_path.name}")
            shutil.copy(sample_pmtiles, pmtiles_path)

        monkeypatch.setattr("portolan_cli.viz.pmtiles.check_pmtiles_available", lambda: None)
        monkeypatch.setattr("portolan_cli.viz.pmtiles.generate_pmtiles", record)
        return built

    @pytest.mark.integration
    def test_pmtiles_flag_rebuilds_tiles_older_than_the_geoparquet(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_pmtiles: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The report line did not say why the tiles were built again."""
        collection_dir = self._prebuilt_tiles(initialized_catalog, fixtures_dir, sample_pmtiles, 1)
        built = self._record_builds(monkeypatch, sample_pmtiles)

        lines = self._verbose_add(runner, initialized_catalog, collection_dir, "--pmtiles")

        assert built == ["points.parquet -> points.pmtiles"]
        assert "✓ Rebuilt PMTiles: points.pmtiles was older than points.parquet" in lines

    @pytest.mark.integration
    def test_plain_add_keeps_tiles_older_than_the_geoparquet_and_says_so(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_pmtiles: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Verbose add called tiles older than the GeoParquet up to date.

        Plain add never rebuilds pre-built tiles (#929). Its in-place bbox
        rewrite (#805) makes them look older although the data is the same.
        """
        collection_dir = self._prebuilt_tiles(initialized_catalog, fixtures_dir, sample_pmtiles, 1)
        before = (collection_dir / "points.pmtiles").read_bytes()
        built = self._record_builds(monkeypatch, sample_pmtiles)

        lines = self._verbose_add(runner, initialized_catalog, collection_dir)

        assert built == []
        assert (collection_dir / "points.pmtiles").read_bytes() == before
        assert (
            "⚠ Kept PMTiles: points.pmtiles is older than points.parquet. "
            "Pass --pmtiles to rebuild it."
        ) in lines
        assert not [line for line in lines if "Skipped PMTiles" in line]
        collection = json.loads((collection_dir / "collection.json").read_text())
        assert collection["assets"]["points-tiles"]["href"] == "./points.pmtiles"

    @pytest.mark.integration
    def test_verbose_add_names_the_comparison_for_current_tiles(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_pmtiles: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("portolan_cli.viz.pmtiles.shutil.which", lambda _name: None)
        future = 4_000_000_000
        collection_dir = self._prebuilt_tiles(
            initialized_catalog, fixtures_dir, sample_pmtiles, future
        )

        lines = self._verbose_add(runner, initialized_catalog, collection_dir)

        assert "→ Skipped PMTiles: points.pmtiles is not older than points.parquet" in lines

    @pytest.mark.integration
    def test_force_rebuild_registers_what_the_first_add_registered(
        self, runner: CliRunner, initialized_catalog: Path, fixtures_dir: Path
    ) -> None:
        """Issue #1041: add --force registered the README the first add wrote."""
        collection_dir = initialized_catalog / "roads"
        collection_dir.mkdir()
        shutil.copy(
            fixtures_dir / "vector" / "valid" / "points.parquet", collection_dir / "roads.parquet"
        )
        args = [
            "add",
            "--portolan-dir",
            str(initialized_catalog),
            str(collection_dir / "roads.parquet"),
        ]

        def keys() -> tuple[list[str], list[str]]:
            collection = json.loads((collection_dir / "collection.json").read_text())
            versions = json.loads((collection_dir / "versions.json").read_text())
            return sorted(collection["assets"]), sorted(versions["versions"][-1]["assets"])

        assert runner.invoke(cli, args, catch_exceptions=False).exit_code == 0
        first = keys()
        assert runner.invoke(cli, [*args, "--force"], catch_exceptions=False).exit_code == 0

        assert keys() == first
        assert first == (["roads", "thumbnail"], ["roads.parquet", "roads.thumb.jpg"])

    @pytest.mark.integration
    def test_item_declares_the_file_extension_of_its_assets(
        self, runner: CliRunner, initialized_catalog: Path, fixtures_dir: Path
    ) -> None:
        """Issue #1037: only the collection declared the file extension."""
        item_dir = initialized_catalog / "imagery" / "scene-a"
        item_dir.mkdir(parents=True)
        shutil.copy(fixtures_dir / "raster" / "valid" / "rgb.tif", item_dir / "rgb.tif")

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(item_dir / "rgb.tif")],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        item = json.loads((item_dir / "scene-a.json").read_text())
        assert all("file:size" in asset for asset in item["assets"].values())
        assert (
            "https://stac-extensions.github.io/file/v2.1.0/schema.json" in item["stac_extensions"]
        )


class TestVersionsKeys:
    """Issue #1053: every versions.json key is the file's path in the collection."""

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "scenes",
        [
            pytest.param({"rapidai4eo": "realdata/rapidai4eo-sample.tif"}, id="one-scene"),
            pytest.param(
                {"scene-a": "raster/valid/rgb.tif", "scene-b": "raster/valid/singleband.tif"},
                id="two-scenes",
            ),
        ],
    )
    def test_the_adopted_item_thumbnail_keeps_its_item_key(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        scenes: dict[str, str],
    ) -> None:
        """The collection thumbnail step tracked an item sidecar a second time.

        It keyed the file by its name, so versions.json gained a key at the
        collection root where no file is.
        """
        collection_dir = initialized_catalog / "imagery"
        paths = []
        for scene, fixture in scenes.items():
            (collection_dir / scene).mkdir(parents=True)
            paths.append(str(collection_dir / scene / f"{scene}.tif"))
            shutil.copy(fixtures_dir / fixture, paths[-1])

        result = runner.invoke(
            cli, ["add", "--portolan-dir", str(initialized_catalog), *paths], catch_exceptions=False
        )

        assert result.exit_code == 0, result.output
        versions = json.loads((collection_dir / "versions.json").read_text())
        keys = sorted(versions["versions"][-1]["assets"])
        expected = ["items.parquet"]
        for scene in scenes:
            expected += [f"{scene}/{scene}.thumb.jpg", f"{scene}/{scene}.tif"]
        assert keys == sorted(expected)
        assert all((collection_dir / key).is_file() for key in keys)


class TestRasterRebuild:
    """``add --force`` writes the band statistics the first ``add`` wrote."""

    @pytest.mark.integration
    def test_force_keeps_the_band_statistics(
        self, runner: CliRunner, initialized_catalog: Path, fixtures_dir: Path
    ) -> None:
        """The first add computes the statistics, and GDAL stores them in .aux.xml.

        GDAL stores 14 significant digits, so a later add read 460.38215 where
        the first add wrote 460.38215000000383.
        """
        item_dir = initialized_catalog / "imagery" / "rapidai4eo"
        item_dir.mkdir(parents=True)
        tif = item_dir / "rapidai4eo.tif"
        shutil.copy(fixtures_dir / "realdata" / "rapidai4eo-sample.tif", tif)
        args = ["add", "--portolan-dir", str(initialized_catalog), str(tif)]

        def bands() -> list[dict[str, object]]:
            item = json.loads((item_dir / "rapidai4eo.json").read_text())
            result: list[dict[str, object]] = item["assets"]["data"]["bands"]
            return result

        assert runner.invoke(cli, args, catch_exceptions=False).exit_code == 0
        first = bands()
        assert runner.invoke(cli, [*args, "--force"], catch_exceptions=False).exit_code == 0

        assert bands() == first
        assert first[0]["statistics"] == {
            "minimum": 302.0,
            "maximum": 1015.0,
            "mean": 460.38215,
            "stddev": 51.769646139195,
        }


def _sha(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


class TestNoConversionReplacesAForeignFile:
    """A conversion never writes over a file that its source did not make.

    The output of ``b1.csv`` is ``b1.parquet``. The add converted the CSV into
    that name over the operator's GeoParquet, and then deleted the result,
    because the CSV holds no geometry. Only ``b1.csv`` stayed on disk.
    """

    @staticmethod
    def _points(path: Path, count: int) -> None:
        features = [
            {
                "type": "Feature",
                "properties": {"i": i},
                "geometry": {"type": "Point", "coordinates": [i + 0.5, 10.5]},
            }
            for i in range(count)
        ]
        path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))

    @staticmethod
    def _tracked(collection_dir: Path) -> dict[str, dict[str, object]]:
        versions = json.loads((collection_dir / "versions.json").read_text())
        assets: dict[str, dict[str, object]] = versions["versions"][-1]["assets"]
        return assets

    @pytest.mark.integration
    def test_a_same_stem_csv_is_a_companion_of_the_geoparquet(
        self, runner: CliRunner, initialized_catalog: Path, fixtures_dir: Path
    ) -> None:
        collection_dir = initialized_catalog / "buildings"
        collection_dir.mkdir()
        geoparquet = collection_dir / "b1.parquet"
        shutil.copy(fixtures_dir / "realdata" / "open-buildings.parquet", geoparquet)
        (collection_dir / "b1.csv").write_text("id,name\n1,a\n2,b\n")
        rows = pq.read_metadata(geoparquet).num_rows

        result = runner.invoke(
            cli, ["add", "--portolan-dir", str(initialized_catalog), str(collection_dir)]
        )

        assert result.exit_code == 0, result.output
        # The add rewrites the GeoParquet in place to add a bbox column (#805),
        # so the bytes change. The rows stay, and versions.json tracks them.
        assert pq.read_metadata(geoparquet).num_rows == rows
        assert (collection_dir / "b1.csv").read_text() == "id,name\n1,a\n2,b\n"
        tracked = self._tracked(collection_dir)
        assert tracked["b1.parquet"]["sha256"] == _sha(geoparquet)
        assert tracked["b1.csv"]["sha256"] == _sha(collection_dir / "b1.csv")
        assert sorted(p.name for p in collection_dir.glob("*.parquet")) == ["b1.parquet"]

    @pytest.mark.integration
    def test_a_geojson_beside_a_foreign_parquet_converts_to_a_distinct_name(
        self, runner: CliRunner, initialized_catalog: Path, fixtures_dir: Path
    ) -> None:
        collection_dir = initialized_catalog / "places"
        collection_dir.mkdir()
        foreign = collection_dir / "points.parquet"
        shutil.copy(fixtures_dir / "realdata" / "open-buildings.parquet", foreign)
        source = collection_dir / "points.geojson"
        self._points(source, 1)
        before = _sha(foreign)
        args = ["add", "--portolan-dir", str(initialized_catalog), str(source)]

        first = runner.invoke(cli, args, catch_exceptions=False)
        self._points(source, 2)
        second = runner.invoke(cli, args, catch_exceptions=False)

        assert first.exit_code == 0, first.output
        assert second.exit_code == 0, second.output
        assert _sha(foreign) == before
        output = collection_dir / "points-geojson.parquet"
        assert pq.read_metadata(output).num_rows == 2
        assert sorted(p.name for p in collection_dir.glob("*.parquet")) == [
            "points-geojson.parquet",
            "points.parquet",
        ]
        collection = json.loads((collection_dir / "collection.json").read_text())
        assert collection["assets"]["points-geojson"]["href"] == "./points-geojson.parquet"
        assert collection["assets"]["points.geojson"]["roles"] == ["source"]
        assert "points.parquet" not in self._tracked(collection_dir)

    @pytest.mark.integration
    def test_a_changed_source_converts_again_into_its_own_output(
        self, runner: CliRunner, initialized_catalog: Path
    ) -> None:
        collection_dir = initialized_catalog / "pts"
        collection_dir.mkdir()
        source = collection_dir / "pts.geojson"
        args = ["add", "--portolan-dir", str(initialized_catalog), str(source)]

        self._points(source, 1)
        assert runner.invoke(cli, args, catch_exceptions=False).exit_code == 0
        self._points(source, 2)
        result = runner.invoke(cli, args, catch_exceptions=False)

        assert result.exit_code == 0, result.output
        assert pq.read_metadata(collection_dir / "pts.parquet").num_rows == 2
        assert sorted(p.name for p in collection_dir.glob("*.parquet")) == ["pts.parquet"]

    @pytest.mark.integration
    def test_an_add_after_a_crash_before_the_commit_reuses_the_output(
        self, runner: CliRunner, initialized_catalog: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The output of a killed run carries its source, so the next add replaces it."""
        from portolan_cli import add

        collection_dir = initialized_catalog / "pts"
        collection_dir.mkdir()
        source = collection_dir / "pts.geojson"
        self._points(source, 1)
        args = ["add", "--portolan-dir", str(initialized_catalog), str(source)]

        def killed(*_args: object, **_kwargs: object) -> None:
            raise KeyboardInterrupt

        with monkeypatch.context() as patch:
            patch.setattr(add, "finalize_items", killed)
            crashed = runner.invoke(cli, args)
        assert (collection_dir / "pts.parquet").is_file()
        assert not (collection_dir / "versions.json").exists(), crashed.output

        result = runner.invoke(cli, args, catch_exceptions=False)

        assert result.exit_code == 0, result.output
        assert sorted(p.name for p in collection_dir.glob("*.parquet")) == ["pts.parquet"]
        assert self._tracked(collection_dir)["pts.parquet"]["sha256"] == _sha(
            collection_dir / "pts.parquet"
        )


def _make_legacy(collection_dir: Path) -> None:
    """Give a collection the shape that Portolan 0.8.0 wrote.

    0.8.0 wrote no ``portolan:source`` footer key and no ``source_path`` in
    versions.json. The output entry has ``source_mtime`` and the source entry
    has only ``mtime``, as in a catalog that 0.8.0 built.
    """
    import os

    from portolan_cli.convert import PROVENANCE_KEY

    versions_path = collection_dir / "versions.json"
    versions = json.loads(versions_path.read_text())
    assets = versions["versions"][-1]["assets"]
    for parquet in collection_dir.glob("*.parquet"):
        table = pq.read_table(parquet)
        metadata = dict(table.schema.metadata or {})
        if metadata.pop(PROVENANCE_KEY.encode(), None) is None:
            continue
        mtime = parquet.stat().st_mtime
        pq.write_table(table.replace_schema_metadata(metadata), parquet)
        os.utime(parquet, (mtime, mtime))
        entry = assets[parquet.name]
        entry["sha256"] = _sha(parquet)
        entry["size_bytes"] = parquet.stat().st_size
    for entry in assets.values():
        entry.pop("source_path", None)
    versions_path.write_text(json.dumps(versions))


class TestALegacyOutputIsItsSourceOwn:
    """An output that 0.8.0 wrote has no record. versions.json tells whose it is."""

    @staticmethod
    def _add(runner: CliRunner, catalog: Path, target: Path) -> None:
        result = runner.invoke(
            cli, ["add", "--portolan-dir", str(catalog), str(target)], catch_exceptions=False
        )
        assert result.exit_code == 0, result.output

    @staticmethod
    def _parquets(collection_dir: Path) -> list[str]:
        return sorted(p.name for p in collection_dir.glob("*.parquet"))

    @pytest.mark.integration
    def test_an_edited_geojson_replaces_its_legacy_output(
        self, runner: CliRunner, initialized_catalog: Path
    ) -> None:
        collection_dir = initialized_catalog / "pts"
        collection_dir.mkdir()
        source = collection_dir / "pts.geojson"
        TestNoConversionReplacesAForeignFile._points(source, 1)
        self._add(runner, initialized_catalog, source)
        _make_legacy(collection_dir)

        TestNoConversionReplacesAForeignFile._points(source, 2)
        self._add(runner, initialized_catalog, source)

        assert self._parquets(collection_dir) == ["pts.parquet"]
        assert pq.read_metadata(collection_dir / "pts.parquet").num_rows == 2

    @pytest.mark.integration
    def test_an_edited_geo_csv_replaces_its_legacy_output(
        self, runner: CliRunner, initialized_catalog: Path
    ) -> None:
        collection_dir = initialized_catalog / "sites"
        collection_dir.mkdir()
        source = collection_dir / "sites.csv"
        source.write_text("id,lat,lon\n1,10.5,1.5\n")
        self._add(runner, initialized_catalog, source)
        _make_legacy(collection_dir)

        source.write_text("id,lat,lon\n1,10.5,1.5\n2,11.5,2.5\n")
        self._add(runner, initialized_catalog, source)

        assert self._parquets(collection_dir) == ["sites.parquet"]
        assert pq.read_metadata(collection_dir / "sites.parquet").num_rows == 2

    @pytest.mark.integration
    def test_an_edited_companion_csv_leaves_the_geoparquet(
        self, runner: CliRunner, initialized_catalog: Path, fixtures_dir: Path
    ) -> None:
        collection_dir = initialized_catalog / "buildings"
        collection_dir.mkdir()
        geoparquet = collection_dir / "b1.parquet"
        shutil.copy(fixtures_dir / "realdata" / "open-buildings.parquet", geoparquet)
        companion = collection_dir / "b1.csv"
        companion.write_text("id,name\n1,a\n")
        self._add(runner, initialized_catalog, collection_dir)
        _make_legacy(collection_dir)
        before = _sha(geoparquet)

        companion.write_text("id,name\n1,a\n2,b\n")
        # The add fails the CSV here. See test_an_edited_companion_csv_is_added_again.
        runner.invoke(cli, ["add", "--portolan-dir", str(initialized_catalog), str(collection_dir)])

        assert _sha(geoparquet) == before
        assert self._parquets(collection_dir) == ["b1.parquet"]

    @pytest.mark.integration
    @pytest.mark.xfail(
        strict=True,
        reason=(
            "https://github.com/portolan-sdi/portolan-cli/pull/1066: phase 3 of add "
            "counts only the geo files of this run as companions of a directory. "
            "Lane D1 fixes it in add.py and removes this marker. "
            "Repro: scratchpad/d2/repro2.sh of the perf run."
        ),
    )
    def test_an_edited_companion_csv_is_added_again(
        self, runner: CliRunner, initialized_catalog: Path, fixtures_dir: Path
    ) -> None:
        collection_dir = initialized_catalog / "buildings"
        collection_dir.mkdir()
        shutil.copy(
            fixtures_dir / "realdata" / "open-buildings.parquet", collection_dir / "b1.parquet"
        )
        companion = collection_dir / "b1.csv"
        companion.write_text("id,name\n1,a\n")
        self._add(runner, initialized_catalog, collection_dir)

        companion.write_text("id,name\n1,a\n2,b\n")
        self._add(runner, initialized_catalog, collection_dir)

        tracked = TestNoConversionReplacesAForeignFile._tracked(collection_dir)
        assert tracked["b1.csv"]["sha256"] == _sha(companion)

    @pytest.mark.integration
    def test_a_companion_csv_that_gains_geometry_converts_to_a_distinct_name(
        self, runner: CliRunner, initialized_catalog: Path, fixtures_dir: Path
    ) -> None:
        """The one ambiguous case. The add takes the path that destroys nothing."""
        collection_dir = initialized_catalog / "buildings"
        collection_dir.mkdir()
        geoparquet = collection_dir / "b1.parquet"
        shutil.copy(fixtures_dir / "realdata" / "open-buildings.parquet", geoparquet)
        companion = collection_dir / "b1.csv"
        companion.write_text("id,name\n1,a\n")
        self._add(runner, initialized_catalog, collection_dir)
        _make_legacy(collection_dir)
        before = _sha(geoparquet)

        companion.write_text("id,name,lat,lon\n1,a,10.5,1.5\n")
        self._add(runner, initialized_catalog, collection_dir)

        assert _sha(geoparquet) == before
        assert self._parquets(collection_dir) == ["b1-csv.parquet", "b1.parquet"]

    @pytest.mark.integration
    def test_a_new_geojson_leaves_a_legacy_geoparquet_of_its_stem(
        self, runner: CliRunner, initialized_catalog: Path, fixtures_dir: Path
    ) -> None:
        collection_dir = initialized_catalog / "places"
        collection_dir.mkdir()
        geoparquet = collection_dir / "points.parquet"
        shutil.copy(fixtures_dir / "realdata" / "open-buildings.parquet", geoparquet)
        self._add(runner, initialized_catalog, geoparquet)
        _make_legacy(collection_dir)
        before = _sha(geoparquet)
        source = collection_dir / "points.geojson"
        TestNoConversionReplacesAForeignFile._points(source, 1)

        self._add(runner, initialized_catalog, source)

        assert _sha(geoparquet) == before
        assert self._parquets(collection_dir) == ["points-geojson.parquet", "points.parquet"]


class TestAConvertedCogPassesItsOwnCheck:
    """A COG that add converts from a GTiff embeds its statistics (issue #748).

    A striped 600 x 600 GTiff is no COG, so add converts it. A GTiff under
    512 pixels already validates as a COG, and add keeps it as it is.
    """

    @pytest.mark.integration
    def test_check_metadata_reports_no_missing_statistics(
        self, runner: CliRunner, initialized_catalog: Path
    ) -> None:
        import numpy as np
        import rasterio
        from rasterio.transform import from_origin

        rng = np.random.default_rng(7)
        for i in range(3):
            path = initialized_catalog / "scenes" / f"s{i}" / f"s{i}.tif"
            path.parent.mkdir(parents=True)
            with rasterio.open(
                path,
                "w",
                driver="GTiff",
                width=600,
                height=600,
                count=1,
                dtype="uint8",
                crs="EPSG:4326",
                transform=from_origin(10.0 + i, 50.0, 0.001, 0.001),
            ) as dst:
                dst.write(rng.integers(0, 255, size=(1, 600, 600), dtype="uint8"))
        added = runner.invoke(
            cli, ["add", "--portolan-dir", str(initialized_catalog), str(initialized_catalog)]
        )
        assert added.exit_code == 0, added.output

        result = runner.invoke(
            cli, ["--format", "json", "check", "--metadata", str(initialized_catalog)]
        )

        assert "PTL-DAT-009" not in result.output
        assert "PTL-DAT-010" not in result.output
        assert sorted(p.name for p in initialized_catalog.rglob("*.aux.xml")) == []
