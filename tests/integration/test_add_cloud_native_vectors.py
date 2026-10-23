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


class TestAddPMTiles:
    """Tests for adding PMTiles files."""

    @pytest.mark.integration
    def test_add_pmtiles_no_conversion_error(
        self, runner: CliRunner, initialized_catalog: Path, sample_pmtiles: Path
    ) -> None:
        """Adding PMTiles should NOT fail with CRS extraction error (issue #368)."""
        # Set up: copy PMTiles to catalog
        collection_dir = initialized_catalog / "tiles"
        collection_dir.mkdir()
        test_file = collection_dir / "map.pmtiles"
        shutil.copy(sample_pmtiles, test_file)

        # Act
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )

        # Assert - should NOT fail
        assert result.exit_code == 0, f"Add failed: {result.output}"
        assert "No CRS found" not in result.output

    @pytest.mark.integration
    def test_add_pmtiles_preserves_format(
        self, runner: CliRunner, initialized_catalog: Path, sample_pmtiles: Path
    ) -> None:
        """PMTiles should be copied, not converted to GeoParquet."""
        collection_dir = initialized_catalog / "tiles"
        collection_dir.mkdir()
        test_file = collection_dir / "map.pmtiles"
        shutil.copy(sample_pmtiles, test_file)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        # PMTiles should still exist (not converted)
        assert test_file.exists() or (collection_dir / "map.pmtiles").exists()
        # Should NOT have created a .parquet file from conversion
        parquet_files = list(collection_dir.rglob("*.parquet"))
        # Filter out items.parquet which is a STAC index file
        data_parquets = [p for p in parquet_files if p.name != "items.parquet"]
        assert len(data_parquets) == 0, f"Unexpected parquet conversion: {data_parquets}"

    @pytest.mark.integration
    def test_add_pmtiles_creates_collection(
        self, runner: CliRunner, initialized_catalog: Path, sample_pmtiles: Path
    ) -> None:
        """Adding PMTiles creates collection.json with correct metadata."""
        collection_dir = initialized_catalog / "tiles"
        collection_dir.mkdir()
        test_file = collection_dir / "map.pmtiles"
        shutil.copy(sample_pmtiles, test_file)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        # Collection should be created
        collection_json = collection_dir / "collection.json"
        assert collection_json.exists()

        # Load and verify
        data = json.loads(collection_json.read_text())
        assert data["type"] == "Collection"
        assert "extent" in data
        assert "spatial" in data["extent"]

    @pytest.mark.integration
    def test_add_pmtiles_idempotent(
        self, runner: CliRunner, initialized_catalog: Path, sample_pmtiles: Path
    ) -> None:
        """Adding same PMTiles file twice is idempotent (no error, no duplicate)."""
        collection_dir = initialized_catalog / "tiles"
        collection_dir.mkdir()
        test_file = collection_dir / "map.pmtiles"
        shutil.copy(sample_pmtiles, test_file)

        # First add
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        # Second add of same file should succeed (idempotent)
        result2 = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result2.exit_code == 0


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
    def test_pmtiles_is_collection_level_asset(
        self, runner: CliRunner, initialized_catalog: Path, sample_pmtiles: Path
    ) -> None:
        """PMTiles should be registered as collection-level asset (no item.json)."""
        collection_dir = initialized_catalog / "tiles"
        collection_dir.mkdir()
        test_file = collection_dir / "map.pmtiles"
        shutil.copy(sample_pmtiles, test_file)

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
        assert "map" in data.get("assets", {})
        assert data["assets"]["map"]["type"] == "application/vnd.pmtiles"

    @pytest.mark.integration
    def test_pmtiles_stac_properties_in_collection(
        self, runner: CliRunner, initialized_catalog: Path, sample_pmtiles: Path
    ) -> None:
        """PMTiles properties should be in collection.json (not item)."""
        collection_dir = initialized_catalog / "tiles"
        collection_dir.mkdir()
        test_file = collection_dir / "map.pmtiles"
        shutil.copy(sample_pmtiles, test_file)

        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )

        data = json.loads((collection_dir / "collection.json").read_text())

        # The tile CRS is a visualization artifact and contributes no
        # projection field (#488, #654)
        assert "proj:epsg" not in data
        assert "proj:code" not in data

        # PMTiles-specific properties
        assert data.get("pmtiles:min_zoom") == 4
        assert data.get("pmtiles:max_zoom") == 8
        assert data.get("pmtiles:tile_type") == "mvt"

        # No projection field written, so no projection declaration
        assert "https://stac-extensions.github.io/projection/v2.0.0/schema.json" not in data.get(
            "stac_extensions", []
        )

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

    @staticmethod
    def _pmtiles_owns_roads(collection_dir: Path) -> dict[str, dict[str, object]]:
        """Assert that the PMTiles holds ``roads`` as data, and return the assets."""
        data = json.loads((collection_dir / "collection.json").read_text())
        assets: dict[str, dict[str, object]] = data["assets"]
        assert assets["roads"]["href"] == "./roads.pmtiles"
        assert assets["roads"]["roles"] == ["data"]
        return assets

    @pytest.mark.integration
    def test_plain_table_beside_pmtiles_leaves_the_pmtiles_primary(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_pmtiles: Path,
    ) -> None:
        """A Parquet without GeoParquet metadata is no source of the tiles."""
        collection_dir = initialized_catalog / "roads"
        collection_dir.mkdir()
        shutil.copy(sample_pmtiles, collection_dir / "roads.pmtiles")
        table = fixtures_dir / "scan" / "geoparquet_with_companions" / "lookup.parquet"
        shutil.copy(table, collection_dir / "roads.parquet")
        args = ["add", "--portolan-dir", str(initialized_catalog), str(collection_dir)]

        result = runner.invoke(cli, args)

        assert result.exit_code == 0, result.output
        assets = self._pmtiles_owns_roads(collection_dir)
        assert "roads-tiles" not in assets

    @pytest.mark.integration
    def test_pmtiles_added_after_a_same_stem_plain_table_takes_the_stem(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        fixtures_dir: Path,
        sample_pmtiles: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Two adds give the same keys as one add of a table and its PMTiles."""
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
        assets = self._pmtiles_owns_roads(collection_dir)
        assert assets["roads.parquet"]["href"] == "./roads.parquet"


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
        assert "tippecanoe is required" not in result.output
        collection = json.loads((collection_dir / "collection.json").read_text())
        assert collection["assets"]["points-tiles"]["href"] == "./points.pmtiles"

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
