"""Integration tests for collection-level asset workflow (Issue #250, Issue #364).

Tests the complete workflow of adding collection-level vector assets,
verifying STAC metadata generation, and ensuring correct path structure.

- Single vector files (GeoParquet, Shapefile, GeoPackage) are collection-level assets
- No item.json created - asset goes directly in collection.json
- items.parquet is NOT generated (no items to index)
"""

from __future__ import annotations

import json
import shutil
from typing import TYPE_CHECKING

import pytest
from rashid import validate

from portolan_cli.add import add

if TYPE_CHECKING:
    from pathlib import Path


def _assert_no_item_json(collection_dir: Path) -> None:
    """Assert no item.json files exist in collection (Issue #364)."""
    item_json_files = [
        f
        for f in collection_dir.rglob("*.json")
        if f.name not in ("collection.json", "versions.json", "catalog.json")
    ]
    assert len(item_json_files) == 0, (
        f"Should NOT create item.json for collection-level vector, found: {[f.name for f in item_json_files]}"
    )


def _assert_collection_assets(collection_dir: Path, expected_assets: dict[str, str]) -> None:
    """Assert collection.json has expected assets with correct hrefs."""
    collection_json = collection_dir / "collection.json"
    with open(collection_json) as f:
        collection_data = json.load(f)

    assets = collection_data.get("assets", {})
    assert len(assets) == len(expected_assets), (
        f"Expected {len(expected_assets)} assets, got {len(assets)}: {list(assets.keys())}"
    )

    for key, expected_href in expected_assets.items():
        assert key in assets, f"Missing '{key}' asset, got: {list(assets.keys())}"
        assert assets[key]["href"] == expected_href, (
            f"{key} href should be '{expected_href}', got: {assets[key]['href']}"
        )

    # Verify NO item links
    links = collection_data.get("links", [])
    item_links = [link for link in links if link.get("rel") == "item"]
    assert len(item_links) == 0, f"Should NOT have item links, got: {item_links}"


@pytest.mark.integration
def _raster_item_with_csv(catalog: Path, fixtures_dir: Path) -> Path:
    """Write the item imagery/scene with one raster and one CSV. Return the CSV."""
    item_dir = catalog / "imagery" / "scene"
    item_dir.mkdir(parents=True)
    (item_dir / "scene.tif").write_bytes(
        (fixtures_dir / "raster" / "valid" / "rgb.tif").read_bytes()
    )
    csv = item_dir / "bands.csv"
    csv.write_text("band,name\n1,red\n")
    return csv


class TestCollectionLevelAssetWorkflow:
    """Test complete workflow for collection-level vector assets."""

    def test_end_to_end_collection_level_asset(
        self, fresh_catalog_no_versions, fixtures_dir, tmp_path
    ):
        """Test complete workflow: add file → verify structure → verify metadata.

        This integration test covers:
        1. Adding a collection-level vector file (demographics/census.parquet)
        2. Verifying no double-nested directories (demographics/demographics/)
        3. Verifying versions.json has correct href (demographics/census.parquet)
        4. Verifying collection.json has asset in "assets" field (Issue #364)
        5. Verifying NO item.json exists (Issue #364)

        Collection-level assets should be organized as:
            demographics/
                census.parquet # Asset at collection level
                collection.json # Has assets.data pointing to census.parquet
                versions.json
        """
        # Setup: Create collection directory
        collection_dir = fresh_catalog_no_versions / "demographics"
        collection_dir.mkdir()

        # Copy test fixture to collection directory
        test_file = fixtures_dir / "simple.parquet"
        target_file = collection_dir / "census.parquet"
        target_file.write_bytes(test_file.read_bytes())

        # Execute: Add collection-level asset
        result = add(
            catalog_root=fresh_catalog_no_versions,
            path=target_file,
            collection_id="demographics",
            item_id=None,  # Auto-derive from file stem
            title="Census Data 2020",
            description="Population demographics from 2020 census",
        )

        # Verify: No double nesting (no demographics/demographics/ directory)
        assert not (collection_dir / "demographics").exists(), (
            "Should NOT have nested demographics/demographics/ directory (Bug #250)"
        )
        assert target_file.exists(), "Asset file should exist at collection level"

        # Verify: NO item.json exists (Issue #364)
        _assert_no_item_json(collection_dir)

        # Verify: versions.json has correct href
        versions_file = collection_dir / "versions.json"
        assert versions_file.exists(), "versions.json should exist"

        with open(versions_file) as f:
            versions_data = json.load(f)

        assert len(versions_data["versions"]) == 1, "Should have one version"
        version = versions_data["versions"][0]

        # Asset key is collection-relative, href is catalog-relative (Issue #354)
        assert "census.parquet" in version["assets"]
        assert version["assets"]["census.parquet"]["href"] == "demographics/census.parquet"

        # Verify collection.json has asset (Issue #364)
        _assert_collection_assets(collection_dir, {"census": "./census.parquet"})

        # Verify: Result contains correct info
        assert result.item_id == "census", (
            f"Item ID should be 'census' (from file stem), got '{result.item_id}'"
        )
        assert result.collection_id == "demographics"

    def test_multiple_collection_level_assets_same_collection(
        self, fresh_catalog_no_versions, fixtures_dir
    ):
        """Test adding multiple collection-level assets to same collection.

        Verifies that multiple vector files in the same collection directory
        are each tracked correctly without interference, and both appear
        in collection.json assets (Issue #364).
        """
        collection_dir = fresh_catalog_no_versions / "demographics"
        collection_dir.mkdir()

        # Add first asset
        test_file = fixtures_dir / "simple.parquet"
        census_file = collection_dir / "census.parquet"
        census_file.write_bytes(test_file.read_bytes())

        add(
            catalog_root=fresh_catalog_no_versions,
            path=census_file,
            collection_id="demographics",
            item_id=None,
            title="Census Data",
            description="Population census",
        )

        # Add second asset
        parcels_file = collection_dir / "parcels.parquet"
        parcels_file.write_bytes(test_file.read_bytes())

        add(
            catalog_root=fresh_catalog_no_versions,
            path=parcels_file,
            collection_id="demographics",
            item_id=None,
            title="Parcel Data",
            description="Land parcels",
        )

        # Verify versions.json tracks both assets
        versions_file = collection_dir / "versions.json"
        with open(versions_file) as f:
            versions_data = json.load(f)

        assert len(versions_data["versions"]) == 2, "Should have two versions"

        all_assets = {}
        for version in versions_data["versions"]:
            all_assets.update(version["assets"])

        assert "census.parquet" in all_assets
        assert "parcels.parquet" in all_assets
        assert all_assets["census.parquet"]["href"] == "demographics/census.parquet"
        assert all_assets["parcels.parquet"]["href"] == "demographics/parcels.parquet"

        # Verify no double nesting
        assert not (collection_dir / "demographics").exists()

        # Verify NO item.json and correct collection.json assets
        _assert_no_item_json(collection_dir)
        # The generated README.md (issue #654) is a describedby link, not an
        # asset. The second add once registered it (issue #1041).
        _assert_collection_assets(
            collection_dir,
            {
                "census": "./census.parquet",
                "parcels": "./parcels.parquet",
            },
        )

        # Verify items.parquet does NOT exist
        assert not (collection_dir / "items.parquet").exists()

    def test_mixed_collection_and_item_level_assets(self, fresh_catalog_no_versions, fixtures_dir):
        """Test collection with both collection-level and item-level assets.

        Verifies that a collection can have:
        - Collection-level assets (demographics/census.parquet)
        - Item-level assets (demographics/survey/responses.parquet)

        And both are tracked correctly without path conflicts.
        """
        collection_dir = fresh_catalog_no_versions / "demographics"
        collection_dir.mkdir()

        # Add collection-level asset
        test_file = fixtures_dir / "simple.parquet"
        census_file = collection_dir / "census.parquet"
        census_file.write_bytes(test_file.read_bytes())

        add(
            catalog_root=fresh_catalog_no_versions,
            path=census_file,
            collection_id="demographics",
            item_id=None,
            title="Census Data",
            description="Population census",
        )

        # Add item-level asset (traditional organization)
        item_dir = collection_dir / "survey"
        item_dir.mkdir()
        survey_file = item_dir / "responses.parquet"
        survey_file.write_bytes(test_file.read_bytes())

        add(
            catalog_root=fresh_catalog_no_versions,
            path=survey_file,
            collection_id="demographics",
            item_id="survey",
            title="Survey Responses",
            description="Survey data",
        )

        # Verify versions.json tracks both correctly
        versions_file = collection_dir / "versions.json"
        with open(versions_file) as f:
            versions_data = json.load(f)

        all_assets = {}
        for version in versions_data["versions"]:
            all_assets.update(version["assets"])

        # Collection-level asset key is filename only (per Issue #354)
        assert "census.parquet" in all_assets, (
            "Collection-level asset key should be filename (collection-relative)"
        )
        assert all_assets["census.parquet"]["href"] == "demographics/census.parquet", (
            "Collection-level asset href should be catalog-relative"
        )

        # Item-level asset key is relative to item (not full path from catalog root)
        assert "survey/responses.parquet" in all_assets, (
            "Item-level asset should be tracked (key is relative to item)"
        )

        # Verify the href is correct for item-level asset
        item_asset = all_assets["survey/responses.parquet"]
        assert item_asset["href"] == "demographics/survey/responses.parquet", (
            "Item-level asset href should be full path from catalog root"
        )

        # Verify directory structure
        assert census_file.exists(), "Collection-level file exists at collection root"
        assert survey_file.exists(), "Item-level file exists in item directory"
        assert not (collection_dir / "demographics").exists(), (
            "Should NOT have double-nested demographics/demographics/ directory"
        )


# =============================================================================
# Issue #383: Non-geo parquet companions with collection-level geo files
# =============================================================================


@pytest.mark.integration
class TestCollectionLevelNonGeoCompanionWorkflow:
    """Integration tests for issue #383: non-geo parquet companion tracking.

    Bug: Non-geo parquet files fail to track when their companion geo file is
    collection-level because `source_to_item_dir` doesn't include collection-level
    sources.

    Repro: User has data.parquet (geo) + stats.parquet (non-geo) in same dir.
    When added as collection-level, stats.parquet should register as asset.
    """

    def test_geo_plus_tabular_parquets_both_tracked(self, fresh_catalog_no_versions, fixtures_dir):
        """Both geo and non-geo parquets in same dir are tracked as collection-level.

        This is the primary repro case for issue #383.
        """
        from portolan_cli.add import add_files

        # Set up collection directory
        collection_dir = fresh_catalog_no_versions / "tunnels"
        collection_dir.mkdir()

        # Use simple.parquet as geo fixture (it's a clean GeoParquet)
        geo_fixture = fixtures_dir / "simple.parquet"
        # For non-geo, we use the lookup.parquet fixture but need to handle
        # the case where it might be detected as geo. Instead, create a
        # minimal parquet without geometry.
        import pyarrow as pa
        import pyarrow.parquet as pq

        geo_file = collection_dir / "tunnels.parquet"
        geo_file.write_bytes(geo_fixture.read_bytes())

        # Create a minimal non-geo parquet file
        nongeo_file = collection_dir / "lookup.parquet"
        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        pq.write_table(table, nongeo_file)

        # Add the directory (both files)
        added, skipped, failures = add_files(
            paths=[collection_dir],
            catalog_root=fresh_catalog_no_versions,
            collection_id="tunnels",
        )

        # Verify: geo file was added successfully
        assert len(added) >= 1, f"Geo file should be added, got failures={failures}"
        assert len(failures) == 0, f"No failures expected, got: {failures}"

        # Verify: non-geo file was tracked (in skipped, not failures)
        assert nongeo_file in skipped, (
            f"Non-geo file should be in skipped (tracked), got skipped={skipped}"
        )

        # Verify: collection.json has both assets
        collection_json = collection_dir / "collection.json"
        assert collection_json.exists(), "collection.json should exist"

        with open(collection_json) as f:
            collection_data = json.load(f)

        assets = collection_data.get("assets", {})
        assert "tunnels" in assets, (
            f"Geo asset 'tunnels' should be in assets, got: {list(assets.keys())}"
        )
        assert "lookup" in assets, (
            f"Non-geo asset 'lookup' should be in assets, got: {list(assets.keys())}"
        )

        # Verify: both have correct hrefs
        assert assets["tunnels"]["href"] == "./tunnels.parquet"
        assert assets["lookup"]["href"] == "./lookup.parquet"

        # Verify: NO item.json exists (both are collection-level)
        _assert_no_item_json(collection_dir)

    def test_nongeo_parquet_error_message_before_fix(self, fresh_catalog_no_versions, fixtures_dir):
        """Verify the error message that issue #383 reported (sanity check).

        Before fix: User would see "no geospatial file in same directory"
        even when a geo file EXISTS in the same directory.

        After fix: Both files track correctly, no error.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        from portolan_cli.add import add_files

        collection_dir = fresh_catalog_no_versions / "data"
        collection_dir.mkdir()

        # Use simple.parquet as geo fixture
        geo_fixture = fixtures_dir / "simple.parquet"
        (collection_dir / "data.parquet").write_bytes(geo_fixture.read_bytes())

        # Create minimal non-geo parquet
        nongeo_file = collection_dir / "stats.parquet"
        table = pa.table({"count": [100, 200], "avg": [1.5, 2.5]})
        pq.write_table(table, nongeo_file)

        # Add directory
        added, skipped, failures = add_files(
            paths=[collection_dir],
            catalog_root=fresh_catalog_no_versions,
            collection_id="data",
        )

        # After fix: no error about "no geospatial file"
        # Both files should be tracked (added or skipped)
        assert len(failures) == 0, f"Should have no failures after fix, got: {failures}"

        # Non-geo file should be in skipped (tracked as companion)
        assert nongeo_file in skipped, "Non-geo should be tracked as skipped"

    def test_edited_companion_beside_unchanged_geo_file(
        self, fresh_catalog_no_versions: Path, fixtures_dir: Path
    ) -> None:
        """A re-add tracks an edited CSV whose GeoParquet did not change.

        Phase 1 skips the unchanged GeoParquet, so no prepared item names the
        directory. Phase 3 must still find the geo file that versions.json
        tracks there, and track the CSV as its companion.
        """
        from portolan_cli.add import add_files
        from portolan_cli.versions import read_versions

        catalog = fresh_catalog_no_versions
        collection_dir = catalog / "buildings"
        collection_dir.mkdir()
        (collection_dir / "b1.parquet").write_bytes((fixtures_dir / "simple.parquet").read_bytes())
        csv = collection_dir / "a1.csv"
        csv.write_text("id,name\n1,a\n")
        _, _, failures = add_files(paths=[collection_dir], catalog_root=catalog)
        assert failures == []

        csv.write_text("id,name\n1,a\n2,b\n")
        _, skipped, failures = add_files(paths=[collection_dir], catalog_root=catalog)

        assert failures == []
        assert csv in skipped
        tracked = read_versions(collection_dir / "versions.json").versions[-1].assets
        assert tracked["a1.csv"].size_bytes == csv.stat().st_size

    def test_edited_companion_beside_unchanged_raster_item(
        self, fresh_catalog_no_versions: Path, fixtures_dir: Path
    ) -> None:
        """The same holds for an item directory: the raster item keeps its CSV."""
        from portolan_cli.add import add_files
        from portolan_cli.versions import read_versions

        catalog = fresh_catalog_no_versions
        csv = _raster_item_with_csv(catalog, fixtures_dir)
        _, _, failures = add_files(paths=[catalog / "imagery"], catalog_root=catalog)
        assert failures == []

        csv.write_text("band,name\n1,red\n2,green\n")
        _, skipped, failures = add_files(paths=[catalog / "imagery"], catalog_root=catalog)

        assert failures == []
        assert csv in skipped
        tracked = read_versions(catalog / "imagery" / "versions.json").versions[-1].assets
        assert tracked["scene/bands.csv"].size_bytes == csv.stat().st_size

    def test_a_companion_in_an_item_directory_makes_no_catalog(
        self, fresh_catalog_no_versions: Path, fixtures_dir: Path
    ) -> None:
        """A CSV beside a raster item joins the item. Its directory is no collection.

        Phase 1 infers ``imagery/scene`` as the collection of the CSV. When that
        phase wrote the intermediate catalogs, ``imagery/catalog.json`` landed
        next to ``imagery/collection.json``, and check reported PTL-LNK-002 and
        PTL-LNK-006.
        """
        from portolan_cli.add import add_files

        catalog = fresh_catalog_no_versions
        item_dir = catalog / "imagery" / "scene"
        item_dir.mkdir(parents=True)
        (item_dir / "scene.tif").write_bytes(
            (fixtures_dir / "raster" / "valid" / "rgb.tif").read_bytes()
        )
        (item_dir / "bands.csv").write_text("band,name\n1,red\n")

        _, _, failures = add_files(paths=[catalog / "imagery"], catalog_root=catalog)

        assert failures == []
        assert sorted(p.name for p in (catalog / "imagery").glob("*.json")) == [
            "collection.json",
            "versions.json",
        ]
        children = [
            link["href"]
            for link in json.loads((catalog / "catalog.json").read_text())["links"]
            if link["rel"] == "child"
        ]
        assert children == ["./imagery/collection.json"]
        link_rules = sorted(
            f.rule_id
            for f in validate(catalog, structural=True).findings
            if f.rule_id in {"PTL-LNK-002", "PTL-LNK-006"}
        )
        assert link_rules == []

    def test_a_failed_tabular_conversion_writes_no_collection(
        self, fresh_catalog_no_versions: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A standalone CSV whose conversion fails is a failure and nothing more.

        The plan registered the collection before the conversion ran. The add
        then wrote an empty collection and an empty version 1.0.0.
        """
        from portolan_cli.add import add_files

        catalog = fresh_catalog_no_versions
        (catalog / ".portolan" / "config.yaml").write_text(
            "tabular:\n  enabled: true\n  convert: true\n"
        )
        collection_dir = catalog / "records"
        collection_dir.mkdir()
        csv = collection_dir / "records.csv"
        csv.write_text("id,name\n1,a\n")

        def no_space(*_args: object, **_kwargs: object) -> Path:
            raise OSError("No space left on device")

        monkeypatch.setattr("portolan_cli.add.convert_tabular", no_space)

        added, _skipped, failures = add_files(paths=[collection_dir], catalog_root=catalog)

        assert added == []
        assert [(f.path, f.error) for f in failures] == [(csv, "No space left on device")]
        assert sorted(p.name for p in collection_dir.iterdir()) == ["records.csv"]
        # The fixture holds no catalog index, and the add commits no version.
        assert not (catalog / "versions.json").exists()


def _init_catalog(root: Path) -> None:
    from click.testing import CliRunner

    from portolan_cli.cli import cli

    result = CliRunner().invoke(
        cli,
        ["init", str(root), "--auto", "--title", "One version", "--license", "CC-BY-4.0"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output


def _add_cli(root: Path, target: Path) -> None:
    from click.testing import CliRunner

    from portolan_cli.cli import cli

    result = CliRunner().invoke(
        cli, ["add", "--portolan-dir", str(root), str(target)], catch_exceptions=False
    )
    assert result.exit_code == 0, result.output


def _stub_thumbnail(monkeypatch: pytest.MonkeyPatch) -> None:
    """Draw a 4 px JPEG where the renderer would, with no basemap and no network."""
    from PIL import Image

    from portolan_cli.viz.thumbnail import thumbnail_path_for

    def render(*, geoparquet_path: Path, **_: object) -> Path:
        out = thumbnail_path_for(geoparquet_path)
        Image.new("RGB", (4, 4)).save(out, "JPEG")
        return out

    monkeypatch.setattr("portolan_cli.collection_thumbnail.generate_vector_thumbnail", render)


def _index_entry(root: Path, collection_id: str) -> dict[str, object] | None:
    collections = json.loads((root / "versions.json").read_text())["collections"] or {}
    entry: dict[str, object] | None = collections.get(collection_id)
    return entry


class _CrashError(Exception):
    """The fault that the crash tests inject after collection.json is on disk."""


def _crashed_add(
    tmp_path: Path, fixtures_dir: Path, patcher: pytest.MonkeyPatch
) -> tuple[Path, Path]:
    """Run an add that raises after collection.json is on disk.

    Returns:
        The catalog root and the collection directory.
    """
    from portolan_cli.add import add_files

    root = tmp_path / "catalog"
    _init_catalog(root)
    collection_dir = root / "roads"
    collection_dir.mkdir()
    shutil.copy(
        fixtures_dir / "vector" / "valid" / "points.geojson", collection_dir / "roads.geojson"
    )

    def crash(*_args: object, **_kwargs: object) -> None:
        raise _CrashError

    patcher.setattr("portolan_cli.add._write_deferred", crash)
    with pytest.raises(_CrashError):
        add_files(paths=[collection_dir], catalog_root=root)
    return root, collection_dir


@pytest.mark.integration
class TestOneAddIsOneVersion:
    """One add writes one version per collection, with every side step in it (#995)."""

    def test_the_pmtiles_and_the_thumbnail_join_the_add_version(
        self, tmp_path: Path, fixtures_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from portolan_cli.versions import read_versions

        root = tmp_path / "catalog"
        _init_catalog(root)
        collection_dir = root / "roads"
        collection_dir.mkdir()
        shutil.copy(
            fixtures_dir / "vector" / "valid" / "points.parquet", collection_dir / "roads.parquet"
        )
        shutil.copy(
            fixtures_dir / "cloud_native" / "sample.pmtiles", collection_dir / "roads.pmtiles"
        )
        _stub_thumbnail(monkeypatch)

        from portolan_cli import json_io

        written: list[Path] = []
        write_text_atomic = json_io.write_text_atomic

        def recorded(path: Path, content: str) -> None:
            written.append(path)
            write_text_atomic(path, content)

        monkeypatch.setattr(json_io, "write_text_atomic", recorded)

        _add_cli(root, collection_dir)

        # One write of the collection record and one of the catalog index.
        # The thumbnail amended the record in a second write before (#995).
        assert [p for p in written if p.name == "versions.json"] == [
            collection_dir / "versions.json",
            root / "versions.json",
        ]
        versions_file = read_versions(collection_dir / "versions.json")
        assert [v.version for v in versions_file.versions] == ["1.0.0"]
        latest = versions_file.versions[-1]
        assert sorted(latest.assets) == ["roads.parquet", "roads.pmtiles", "roads.thumb.jpg"]
        # One commit writes the catalog index after the side steps, so it
        # counts every asset of the version.
        assert _index_entry(root, "roads") == {
            "updated": _index_entry(root, "roads")["updated"],  # type: ignore[index]
            "current_version": "1.0.0",
            "asset_count": 3,
            "total_size_bytes": sum(a.size_bytes for a in latest.assets.values()),
        }

    def test_the_catalog_index_matches_each_current_version(
        self, tmp_path: Path, fixtures_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """commit_run writes the index that the removed sync_catalog_index wrote.

        The index entry of each collection summarizes its current version,
        side steps included. A re-add that changes nothing leaves the index bytes.
        """
        from portolan_cli.catalog import CollectionSummary
        from portolan_cli.versions import read_versions

        root = tmp_path / "catalog"
        _init_catalog(root)
        for name in ("roads", "parks"):
            (root / name).mkdir()
            shutil.copy(
                fixtures_dir / "vector" / "valid" / "points.parquet",
                root / name / f"{name}.parquet",
            )
        shutil.copy(
            fixtures_dir / "cloud_native" / "sample.pmtiles", root / "roads" / "roads.pmtiles"
        )
        _stub_thumbnail(monkeypatch)

        _add_cli(root, root)

        for name in ("roads", "parks"):
            versions_file = read_versions(root / name / "versions.json")
            current = versions_file.versions[-1]
            entry = _index_entry(root, name)
            assert entry is not None
            assert CollectionSummary(
                str(entry["current_version"]),
                int(entry["asset_count"]),  # type: ignore[call-overload]
                int(entry["total_size_bytes"]),  # type: ignore[call-overload]
            ) == CollectionSummary.of(current)
        before = (root / "versions.json").read_bytes()

        _add_cli(root, root)

        assert (root / "versions.json").read_bytes() == before

    def test_a_failed_file_still_commits_the_others(
        self, tmp_path: Path, fixtures_dir: Path
    ) -> None:
        from portolan_cli.add import add_files
        from portolan_cli.versions import read_versions

        root = tmp_path / "catalog"
        _init_catalog(root)
        (root / "good").mkdir()
        shutil.copy(
            fixtures_dir / "vector" / "valid" / "points.geojson", root / "good" / "good.geojson"
        )
        (root / "bad").mkdir()
        bad = root / "bad" / "bad.geojson"
        bad.write_text("{not json")

        _added, _skipped, failures = add_files(
            paths=[root / "good", root / "bad"], catalog_root=root
        )

        assert [f.path for f in failures] == [bad]
        assert read_versions(root / "good" / "versions.json").current_version == "1.0.0"
        assert not (root / "bad" / "versions.json").exists()
        assert _index_entry(root, "bad") is None

    def test_an_unhandled_error_commits_nothing(
        self, tmp_path: Path, fixtures_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        root, collection_dir = _crashed_add(tmp_path, fixtures_dir, monkeypatch)

        assert (collection_dir / "collection.json").exists()
        assert not (collection_dir / "versions.json").exists()
        assert _index_entry(root, "roads") is None

    def test_an_add_after_a_crash_converges(
        self, tmp_path: Path, fixtures_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The crash leaves collection.json and the converted file with no version.

        The next add replaces its own output, which carries the source
        provenance record, and writes the version. versions.json and
        collection.json then agree.
        """
        from portolan_cli.add import add_files
        from portolan_cli.sync.checksums import compute_checksum
        from portolan_cli.versions import read_versions

        with monkeypatch.context() as patched:
            root, collection_dir = _crashed_add(tmp_path, fixtures_dir, patched)

        _added, _skipped, failures = add_files(paths=[collection_dir], catalog_root=root)

        assert failures == []
        assert sorted(p.name for p in collection_dir.glob("*.parquet")) == ["roads.parquet"]
        tracked = read_versions(collection_dir / "versions.json").versions
        assert len(tracked) == 1
        records = tracked[-1].assets
        stac = json.loads((collection_dir / "collection.json").read_text())["assets"]
        tracked_hrefs = {f"./{key}" for key in records}
        assert {asset["href"] for asset in stac.values()} <= tracked_hrefs
        assert records["roads.parquet"].sha256 == compute_checksum(collection_dir / "roads.parquet")
        assert _index_entry(root, "roads")["current_version"] == "1.0.0"  # type: ignore[index]


@pytest.mark.integration
class TestTheBumpOfAnAdd:
    """One add writes one version, and its number bumps the part that main bumped.

    On main the geo files of an add went through finalize, which bumped the
    patch. A companion went through ``publish_version``, which bumped the
    minor. ``rm`` bumps the minor. One staged version takes the larger bump,
    so it ends on the number that the chain of versions on main ended on.
    """

    @staticmethod
    def _collection(catalog: Path, fixtures_dir: Path) -> tuple[Path, Path, Path]:
        """Add a GeoParquet with a CSV companion. Return the dir, the file, the CSV."""
        from portolan_cli.add import add_files

        collection_dir = catalog / "buildings"
        collection_dir.mkdir()
        geo = collection_dir / "b1.parquet"
        geo.write_bytes((fixtures_dir / "simple.parquet").read_bytes())
        csv = collection_dir / "a1.csv"
        csv.write_text("id,name\n1,a\n")
        _, _, failures = add_files(paths=[collection_dir], catalog_root=catalog)
        assert failures == []
        return collection_dir, geo, csv

    @staticmethod
    def _versions(collection_dir: Path) -> list[str]:
        from portolan_cli.versions import read_versions

        return [v.version for v in read_versions(collection_dir / "versions.json").versions]

    @staticmethod
    def _readd(catalog: Path, target: Path) -> None:
        from portolan_cli.add import add_files

        _, _, failures = add_files(paths=[target], catalog_root=catalog)
        assert failures == []

    def test_an_added_companion_bumps_the_minor(
        self, fresh_catalog_no_versions: Path, fixtures_dir: Path
    ) -> None:
        catalog = fresh_catalog_no_versions
        collection_dir, _geo, _csv = self._collection(catalog, fixtures_dir)
        (collection_dir / "b2.csv").write_text("id\n1\n")

        self._readd(catalog, collection_dir)

        assert self._versions(collection_dir) == ["1.0.0", "1.1.0"]

    def test_a_changed_companion_bumps_the_minor(
        self, fresh_catalog_no_versions: Path, fixtures_dir: Path
    ) -> None:
        catalog = fresh_catalog_no_versions
        collection_dir, _geo, csv = self._collection(catalog, fixtures_dir)
        csv.write_text("id,name\n1,a\n2,b\n")

        self._readd(catalog, collection_dir)

        assert self._versions(collection_dir) == ["1.0.0", "1.1.0"]

    def test_a_changed_item_companion_bumps_the_minor(
        self, fresh_catalog_no_versions: Path, fixtures_dir: Path
    ) -> None:
        catalog = fresh_catalog_no_versions
        csv = _raster_item_with_csv(catalog, fixtures_dir)
        self._readd(catalog, catalog / "imagery")
        csv.write_text("band,name\n1,red\n2,green\n")

        self._readd(catalog, catalog / "imagery")

        assert self._versions(catalog / "imagery") == ["1.0.0", "1.1.0"]

    def test_a_changed_geo_file_bumps_the_patch(
        self, fresh_catalog_no_versions: Path, fixtures_dir: Path
    ) -> None:
        catalog = fresh_catalog_no_versions
        collection_dir, geo, _csv = self._collection(catalog, fixtures_dir)
        geo.write_bytes((fixtures_dir / "realdata" / "open-buildings.parquet").read_bytes())

        self._readd(catalog, collection_dir)

        assert self._versions(collection_dir) == ["1.0.0", "1.0.1"]

    def test_a_geo_file_and_a_companion_take_the_minor(
        self, fresh_catalog_no_versions: Path, fixtures_dir: Path
    ) -> None:
        """Main wrote 1.0.1 for the geo file, then 1.1.0 for the companion."""
        catalog = fresh_catalog_no_versions
        collection_dir, geo, csv = self._collection(catalog, fixtures_dir)
        geo.write_bytes((fixtures_dir / "realdata" / "open-buildings.parquet").read_bytes())
        csv.write_text("id,name\n1,a\n2,b\n")

        self._readd(catalog, collection_dir)

        assert self._versions(collection_dir) == ["1.0.0", "1.1.0"]

    def test_a_removal_bumps_the_minor(
        self, fresh_catalog_no_versions: Path, fixtures_dir: Path
    ) -> None:
        from click.testing import CliRunner

        from portolan_cli.cli import cli

        catalog = fresh_catalog_no_versions
        collection_dir, _geo, csv = self._collection(catalog, fixtures_dir)

        result = CliRunner().invoke(
            cli, ["rm", "--keep", "--portolan-dir", str(catalog), str(csv)], catch_exceptions=False
        )

        assert result.exit_code == 0, result.output
        assert self._versions(collection_dir) == ["1.0.0", "1.1.0"]
