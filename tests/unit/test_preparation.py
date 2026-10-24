"""Unit tests for the preparation module (issue #623).

`preparation.py` holds the per-item conversion/metadata-routing extracted from
add.py. These tests exercise the routing and STAC-shaping logic directly against
the new module's seam, independent of the full add() orchestration.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from portolan_cli.errors import AssetKeyCollisionError
from portolan_cli.plugins.builtin.formats import COG, FLATGEOBUF, GEOPARQUET, PMTILES
from portolan_cli.preparation import (
    _add_statistics_to_properties,
    _convert_and_extract_metadata,
    _extract_bbox_wgs84,
    _extract_statistics_best_effort,
    _get_asset_role,
    _scan_item_assets,
    _validate_collection_id,
)

pytestmark = pytest.mark.unit


class TestConvertAndExtractRouting:
    """_convert_and_extract_metadata routes by format + extension (issue #368)."""

    def test_pmtiles_copied_and_extracted_as_cloud_native(self, tmp_path: Path) -> None:
        """.pmtiles is copied as-is (no GeoParquet conversion) and metadata read."""
        source = tmp_path / "tiles.pmtiles"
        source.write_bytes(b"pmtiles-bytes")
        item_dir = tmp_path / "item"
        item_dir.mkdir()
        sentinel = MagicMock(name="pmtiles-metadata")

        with patch("portolan_cli.api.extract_pmtiles_metadata", return_value=sentinel) as extract:
            output_path, metadata = _convert_and_extract_metadata(source, item_dir, PMTILES)

        assert output_path == item_dir / "tiles.pmtiles"
        assert output_path.read_bytes() == b"pmtiles-bytes"  # copied verbatim
        assert metadata is sentinel
        extract.assert_called_once_with(output_path)

    def test_flatgeobuf_routes_to_flatgeobuf_extractor(self, tmp_path: Path) -> None:
        """.fgb is treated as cloud-native and uses the FlatGeobuf extractor."""
        source = tmp_path / "roads.fgb"
        source.write_bytes(b"fgb-bytes")
        item_dir = tmp_path / "item"
        item_dir.mkdir()
        sentinel = MagicMock(name="fgb-metadata")

        with patch(
            "portolan_cli.api.extract_flatgeobuf_metadata", return_value=sentinel
        ) as extract:
            output_path, metadata = _convert_and_extract_metadata(source, item_dir, FLATGEOBUF)

        assert output_path == item_dir / "roads.fgb"
        assert metadata is sentinel
        extract.assert_called_once_with(output_path)

    def test_generic_vector_converts_to_geoparquet(self, tmp_path: Path) -> None:
        """Non-cloud-native vectors go through convert_vector -> GeoParquet."""
        source = tmp_path / "cities.geojson"
        source.write_text("{}")
        item_dir = tmp_path / "item"
        item_dir.mkdir()
        converted = item_dir / "cities.parquet"
        meta = MagicMock(name="gpq-metadata")

        with (
            patch("portolan_cli.api.convert_vector", return_value=converted) as convert,
            patch("portolan_cli.api.extract_geoparquet_metadata", return_value=meta) as extract,
        ):
            output_path, metadata = _convert_and_extract_metadata(source, item_dir, GEOPARQUET)

        assert output_path == converted
        assert metadata is meta
        convert.assert_called_once_with(source, item_dir, None)
        extract.assert_called_once_with(converted)

    def test_raster_converts_to_cog(self, tmp_path: Path) -> None:
        """Rasters route through convert_raster -> COG."""
        source = tmp_path / "dem.tif"
        source.write_bytes(b"tif")
        item_dir = tmp_path / "item"
        item_dir.mkdir()
        converted = item_dir / "dem.tif"
        meta = MagicMock(name="cog-metadata")

        with (
            patch("portolan_cli.api.convert_raster", return_value=converted) as convert,
            patch("portolan_cli.api.extract_cog_metadata", return_value=meta) as extract,
            patch("portolan_cli.api.validate_cog", return_value=None),
            patch("portolan_cli.api.write_raster_thumbnail"),
        ):
            output_path, metadata = _convert_and_extract_metadata(source, item_dir, COG)

        assert output_path == converted
        assert metadata is meta
        convert.assert_called_once_with(source, item_dir, None)
        extract.assert_called_once_with(converted)


class TestStatisticsShaping:
    """_extract_statistics_best_effort + _add_statistics_to_properties."""

    def test_disabled_stats_returns_empty(self, tmp_path: Path) -> None:
        with patch("portolan_cli.preparation.get_setting", return_value=False):
            band_stats, parquet_stats = _extract_statistics_best_effort(
                tmp_path / "x.parquet", "vector", tmp_path
            )
        assert band_stats == []
        assert parquet_stats == {}

    def test_extraction_failure_is_swallowed(self, tmp_path: Path) -> None:
        """A failing extractor yields empty stats, never propagates (best-effort)."""
        with (
            patch("portolan_cli.preparation.get_setting", return_value=True),
            patch(
                "portolan_cli.preparation.extract_parquet_statistics",
                side_effect=RuntimeError("boom"),
            ),
        ):
            band_stats, parquet_stats = _extract_statistics_best_effort(
                tmp_path / "x.parquet", "vector", tmp_path
            )
        assert band_stats == []
        assert parquet_stats == {}

    def test_raster_band_stats_attached_to_bands(self) -> None:
        props: dict = {"bands": [{"data_type": "uint8"}, {"data_type": "uint8"}]}
        stat0 = MagicMock()
        stat0.to_stac_dict.return_value = {"minimum": 0, "maximum": 255}
        stat1 = MagicMock()
        stat1.to_stac_dict.return_value = {"minimum": 1, "maximum": 200}

        _add_statistics_to_properties(props, "raster", [stat0, stat1], {}, stats_enabled=True)

        assert props["bands"][0]["statistics"] == {"minimum": 0, "maximum": 255}
        assert props["bands"][1]["statistics"] == {"minimum": 1, "maximum": 200}

    def test_vector_column_stats_attached(self) -> None:
        props: dict = {}
        stat = MagicMock()
        stat.to_stac_dict.return_value = {"min": 0, "max": 9}

        _add_statistics_to_properties(props, "vector", [], {"pop": stat}, stats_enabled=True)

        assert props["table:column_statistics"] == {"pop": {"min": 0, "max": 9}}

    def test_disabled_flag_skips_mutation(self) -> None:
        props: dict = {"bands": [{"data_type": "uint8"}]}
        stat = MagicMock()
        stat.to_stac_dict.return_value = {"minimum": 0}

        _add_statistics_to_properties(props, "raster", [stat], {}, stats_enabled=False)

        assert "statistics" not in props["bands"][0]


_GEOPARQUET_FIXTURE = Path(__file__).parents[1] / "fixtures" / "vector" / "valid" / "points.parquet"


def _write(directory: Path, *names: str) -> None:
    """Write each named file. ``table*.parquet`` is plain Parquet, any other
    ``.parquet`` is GeoParquet, and every other name holds a few bytes."""
    import shutil

    import pyarrow as pa
    import pyarrow.parquet as pq

    directory.mkdir(parents=True, exist_ok=True)
    for name in names:
        path = directory / name
        if path.suffix.lower() != ".parquet":
            path.write_bytes(name.encode())
        elif name.startswith("table"):
            pq.write_table(pa.table({"a": [1]}), path)
        else:
            shutil.copy(_GEOPARQUET_FIXTURE, path)


def _scan(directory: Path, primary: str, *, collection_level: bool = True) -> dict[str, Any]:
    """Scan ``directory`` and return ``{key: (href, roles)}``."""
    assets, _, _ = _scan_item_assets(
        item_dir=directory,
        item_id=Path(primary).stem,
        primary_file=directory / primary,
        collection_dir=directory if collection_level else directory.parent,
        collection_id="land",
        collection_level=collection_level,
    )
    return {key: (asset.href, asset.roles) for key, asset in assets.items()}


class TestStemGroupResolver:
    """One resolver gives every scanned file its key and role (#933, #960, #929).

    The answer depends on the files only. It must not depend on the ``iterdir``
    order or on which file of the group is the primary, because every item of a
    flat collection scans the same files.
    """

    @pytest.mark.parametrize("primary", ["points.parquet", "points.geojson"])
    def test_converted_geoparquet_is_data_and_its_source_is_source(
        self, tmp_path: Path, primary: str
    ) -> None:
        """Issue #960: the source GeoJSON was registered with the data role."""
        _write(tmp_path, "points.parquet", "points.geojson")

        assert _scan(tmp_path, primary) == {
            "points": ("./points.parquet", ["data"]),
            "points.geojson": ("./points.geojson", ["source"]),
        }

    def test_shapefile_and_its_sidecars_are_sources(self, tmp_path: Path) -> None:
        _write(tmp_path, "roads.parquet", "roads.shp", "roads.dbf", "roads.shx", "roads.prj")

        assert _scan(tmp_path, "roads.parquet") == {
            "roads": ("./roads.parquet", ["data"]),
            "roads.dbf": ("./roads.dbf", ["source"]),
            "roads.prj": ("./roads.prj", ["source"]),
            "roads.shp": ("./roads.shp", ["source"]),
            "roads.shx": ("./roads.shx", ["source"]),
        }

    def test_csv_beside_its_plain_parquet_is_the_source(self, tmp_path: Path) -> None:
        """The spec keeps a converted CSV as the source of the Parquet table."""
        _write(tmp_path, "table-prices.parquet", "table-prices.csv")

        assert _scan(tmp_path, "table-prices.parquet") == {
            "table-prices": ("./table-prices.parquet", ["data"]),
            "table-prices.csv": ("./table-prices.csv", ["source"]),
        }

    @pytest.mark.parametrize("primary", ["zoning.parquet", "zoning.pmtiles"])
    def test_pmtiles_beside_geoparquet_is_the_visual_derivative(
        self, tmp_path: Path, primary: str
    ) -> None:
        """Issues #929 and #933: the PMTiles never takes the data key or role."""
        _write(tmp_path, "zoning.parquet", "zoning.pmtiles")

        assert _scan(tmp_path, primary) == {
            "zoning": ("./zoning.parquet", ["data"]),
            "zoning-tiles": ("./zoning.pmtiles", ["visual"]),
        }

    def test_other_stem_companions_get_the_keys_of_their_own_item(self, tmp_path: Path) -> None:
        _write(tmp_path, "a.parquet", "a.pmtiles", "b.parquet")

        assert _scan(tmp_path, "b.parquet") == {
            "a": ("./a.parquet", ["data"]),
            "a-tiles": ("./a.pmtiles", ["visual"]),
            "b": ("./b.parquet", ["data"]),
        }

    def test_a_pmtiles_beside_a_plain_table_is_still_the_visual(self, tmp_path: Path) -> None:
        """core.md "Roles": the PMTiles is ``visual``, never ``data``."""
        _write(tmp_path, "table.pmtiles", "table.parquet")

        assert _scan(tmp_path, "table.pmtiles") == {
            "table": ("./table.parquet", ["data"]),
            "table-tiles": ("./table.pmtiles", ["visual"]),
        }

    @pytest.mark.parametrize("primary", ["roads.fgb", "roads.pmtiles"])
    def test_flatgeobuf_owns_the_stem_over_pmtiles(self, tmp_path: Path, primary: str) -> None:
        _write(tmp_path, "roads.fgb", "roads.pmtiles")

        assert _scan(tmp_path, primary) == {
            "roads": ("./roads.fgb", ["data"]),
            "roads-tiles": ("./roads.pmtiles", ["visual"]),
        }

    def test_two_parquet_files_of_one_stem_give_the_stem_to_the_primary(
        self, tmp_path: Path
    ) -> None:
        """An uppercase ``.PARQUET`` source converts to a same-stem output."""
        probe = tmp_path / "probe"
        probe.write_bytes(b"")
        case_insensitive = (tmp_path / "PROBE").exists()
        probe.unlink()
        if case_insensitive:
            pytest.skip("a case-insensitive filesystem holds one file for both names")
        _write(tmp_path, "a1.parquet", "a1.PARQUET")

        assert _scan(tmp_path, "a1.parquet") == {
            "a1": ("./a1.parquet", ["data"]),
            "a1.PARQUET": ("./a1.PARQUET", ["data"]),
        }

    def test_a_same_rank_tie_without_the_primary_goes_to_the_parquet_name(
        self, tmp_path: Path
    ) -> None:
        """The name order alone would give the stem to ``a1.PARQUET``."""
        probe = tmp_path / "probe"
        probe.write_bytes(b"")
        case_insensitive = (tmp_path / "PROBE").exists()
        probe.unlink()
        if case_insensitive:
            pytest.skip("a case-insensitive filesystem holds one file for both names")
        _write(tmp_path, "a1.parquet", "a1.PARQUET", "b.parquet")

        assert _scan(tmp_path, "b.parquet") == {
            "a1": ("./a1.parquet", ["data"]),
            "a1.PARQUET": ("./a1.PARQUET", ["data"]),
            "b": ("./b.parquet", ["data"]),
        }

    def test_a_file_beside_an_owner_that_is_not_cloud_native_keeps_its_role(
        self, tmp_path: Path
    ) -> None:
        """A GeoPackage kept as it is converts nothing, so its CSV is no source."""
        _write(tmp_path, "roads.gpkg", "roads.csv")

        assert _scan(tmp_path, "roads.gpkg") == {
            "roads": ("./roads.gpkg", ["data"]),
            "roads.csv": ("./roads.csv", ["data"]),
        }

    def test_the_item_primary_keeps_data_over_a_file_whose_stem_is_data(
        self, tmp_path: Path
    ) -> None:
        """The primary group goes first, though ``data`` sorts before ``scene``."""
        item_dir = tmp_path / "scene"
        _write(item_dir, "scene.tif", "data.csv")

        assert _scan(item_dir, "scene.tif", collection_level=False) == {
            "data": ("scene.tif", ["data"]),
            "data.csv": ("data.csv", ["data"]),
        }

    def test_role_files_never_own_the_stem(self, tmp_path: Path) -> None:
        _write(tmp_path, "roads.parquet", "roads.md", "roads.txt")

        assert _scan(tmp_path, "roads.parquet") == {
            "roads": ("./roads.parquet", ["data"]),
            "documentation": ("./roads.md", ["documentation"]),
            "roads.txt": ("./roads.txt", ["documentation"]),
        }

    def test_a_lone_pmtiles_is_the_visual(self, tmp_path: Path) -> None:
        """The scan of another stem sees tiles that render no GeoParquet yet."""
        _write(tmp_path, "basemap.pmtiles", "roads.parquet")

        assert _scan(tmp_path, "roads.parquet") == {
            "basemap-tiles": ("./basemap.pmtiles", ["visual"]),
            "roads": ("./roads.parquet", ["data"]),
        }

    def test_extension_role_of_a_lone_file_is_the_role_the_scan_gives(self, tmp_path: Path) -> None:
        """``check --fix`` backfills a missing role from the extension alone.

        It must give a PMTiles the role a fresh ``add`` gives it.
        """
        _write(tmp_path, "basemap.pmtiles")

        _href, roles = _scan(tmp_path, "basemap.pmtiles")["basemap-tiles"]
        assert [_get_asset_role(tmp_path / "basemap.pmtiles")] == roles

    def test_the_item_mirror_is_never_a_scanned_asset(self, tmp_path: Path) -> None:
        """``items.parquet`` is GeoParquet, so the scan gave the mirror the data role.

        ``register_mirror_asset`` is its one writer, with the role
        ``collection-mirror``. The PMTiles and thumbnail side-steps read data
        assets, so a mirror with the data role became a tile source.
        """
        _write(tmp_path, "roads.parquet", "items.parquet")

        assert _scan(tmp_path, "roads.parquet") == {"roads": ("./roads.parquet", ["data"])}

    def test_a_gdal_statistics_sidecar_is_never_an_asset(self, tmp_path: Path) -> None:
        """GDAL writes ``.aux.xml`` after the first scan, so a rebuild differed."""
        item_dir = tmp_path / "scene"
        _write(item_dir, "scene.tif", "scene.tif.aux.xml")

        assert _scan(item_dir, "scene.tif", collection_level=False) == {
            "data": ("scene.tif", ["data"]),
        }

    def test_item_level_primary_is_data_and_its_source_is_source(self, tmp_path: Path) -> None:
        item_dir = tmp_path / "scene"
        _write(item_dir, "scene.tif", "scene.jp2", "scene.thumb.jpg")

        assert _scan(item_dir, "scene.tif", collection_level=False) == {
            "data": ("scene.tif", ["data"]),
            "scene.jp2": ("scene.jp2", ["source"]),
            "thumbnail": ("scene.thumb.jpg", ["thumbnail"]),
        }

    @pytest.mark.parametrize(
        ("level", "names", "primary", "key", "holder", "loser"),
        [
            # The item primary takes ``data``. The file ``data`` then wants the
            # stem key and its filename, and both are ``data``.
            ("item", ("scene.tif", "data"), "scene.tif", "data", "scene.tif", "data"),
            # The PMTiles takes ``zoning-tiles``. The file ``zoning-tiles`` then
            # wants the stem key and its filename, and both are ``zoning-tiles``.
            (
                "collection",
                ("zoning.parquet", "zoning.pmtiles", "zoning-tiles"),
                "zoning.parquet",
                "zoning-tiles",
                "zoning.pmtiles",
                "zoning-tiles",
            ),
        ],
    )
    def test_two_files_under_one_key_raise_the_collision(
        self,
        tmp_path: Path,
        level: str,
        names: tuple[str, ...],
        primary: str,
        key: str,
        holder: str,
        loser: str,
    ) -> None:
        """The second file replaced the first in the asset dict, with no error.

        versions.json then tracked both files and the STAC document held one.
        """
        directory = tmp_path / "scene" if level == "item" else tmp_path
        _write(directory, *names)

        with pytest.raises(AssetKeyCollisionError) as excinfo:
            _scan(directory, primary, collection_level=level == "collection")

        assert excinfo.value.context == {
            "collection_id": "land",
            "asset_key": key,
            "existing_href": holder,
            "new_href": loser,
        }


class TestValidateCollectionId:
    """_validate_collection_id security + STAC-compliance checks."""

    def test_valid_nested_id_passes(self) -> None:
        _validate_collection_id("boundaries/districts")  # no raise

    @pytest.mark.parametrize("bad", ["", "..", "a\\b", "a/../b"])
    def test_rejects_unsafe_ids(self, bad: str) -> None:
        with pytest.raises(ValueError):
            _validate_collection_id(bad)


class TestScanItemAssetsSymlinks:
    """_scan_item_assets rejects symlinks before is_dir()/is_file() branching."""

    def test_symlinked_filegdb_directory_is_not_tracked(self, tmp_path: Path) -> None:
        """A symlinked .gdb dir must be skipped, not checksummed as a container.

        is_dir() follows symlinks, so before the fix a symlinked FileGDB
        directory reached the is_filegdb() container path and got tracked,
        escaping the item boundary. Pin that it is now excluded.
        """
        collection_dir = tmp_path / "roads"
        collection_dir.mkdir()
        primary = collection_dir / "roads.parquet"
        primary.write_bytes(b"parquet")

        # A real FileGDB lives outside the item; a symlink to it sits inside.
        real_gdb = tmp_path / "external" / "roads.gdb"
        real_gdb.mkdir(parents=True)
        (real_gdb / "a00000001.gdbtable").write_bytes(b"gdbtable")
        (collection_dir / "linked.gdb").symlink_to(real_gdb, target_is_directory=True)

        stac_assets, asset_files, _ = _scan_item_assets(
            item_dir=collection_dir,
            item_id="roads",
            primary_file=primary,
            collection_dir=collection_dir,
            collection_id="land",
        )

        assert "linked.gdb" not in asset_files
        assert all("linked.gdb" not in a.href for a in stac_assets.values())
        # The genuine primary file is still tracked.
        assert "roads.parquet" in asset_files


class TestExtractBboxWgs84:
    """_extract_bbox_wgs84 CRS handling."""

    def test_projjson_dict_crs_is_passed_through(self) -> None:
        """A PROJJSON dict reaches the CRS layer rather than raising.

        geoparquet-io 1.4.0 keeps the source CRS on write, so a CRS with no
        authority code now arrives as a PROJJSON dict. pyproj reads PROJJSON
        directly (Issue #810).
        """
        projjson = {"type": "ProjectedCRS", "name": "POSGAR 94 / Argentina 5"}
        meta = MagicMock()
        meta.crs = projjson
        meta.bbox = (5677585.0, 6132182.0, 5686964.0, 6140159.0)
        meta.geometry_column = None
        with patch(
            "portolan_cli.preparation.transform_bbox_to_wgs84",
            return_value=(-58.06, -34.94, -57.95, -34.87),
        ) as transform:
            result = _extract_bbox_wgs84(meta)
        assert result == [-58.06, -34.94, -57.95, -34.87]
        transform.assert_called_once_with(meta.bbox, projjson)

    def test_string_crs_transformed(self) -> None:
        meta = MagicMock()
        meta.crs = "EPSG:3857"
        meta.bbox = (0.0, 0.0, 1.0, 1.0)
        with patch(
            "portolan_cli.preparation.transform_bbox_to_wgs84",
            return_value=(-1.0, -1.0, 2.0, 2.0),
        ) as transform:
            result = _extract_bbox_wgs84(meta)
        assert result == [-1.0, -1.0, 2.0, 2.0]
        transform.assert_called_once_with(meta.bbox, "EPSG:3857")


class TestConvertVectorRetriesTransientInterrupt:
    """The add pipeline's ``convert_vector`` retries a transient DuckDB interrupt.

    Regression guard for the Issue #339 nightly ``test_add_1000_files_*`` flake:
    ``add`` and ``check --fix`` convert single-layer vectors via
    ``preparation.convert_vector``, so the bounded transient-interrupt retry
    must protect this code path. A single ``InterruptException`` on the
    first ``gpio.convert`` must be retried and succeed; a non-transient error must
    still fail fast.
    """

    def test_convert_vector_retries_then_succeeds(self, tmp_path: Path) -> None:
        import geoparquet_io as gpio

        from portolan_cli.preparation import convert_vector

        class InterruptException(Exception):
            pass

        class _FakeTable:
            def add_bbox(self) -> _FakeTable:
                return self

            def sort_hilbert(self) -> _FakeTable:
                return self

            def write(self, path: str) -> None:
                Path(path).write_text("parquet-bytes")

        calls = {"n": 0}

        def fake_convert(src: str) -> _FakeTable:
            calls["n"] += 1
            if calls["n"] == 1:
                raise InterruptException("Query interrupted")
            return _FakeTable()

        with patch.object(gpio, "convert", fake_convert):
            out = convert_vector(tmp_path / "in.geojson", tmp_path)

        assert calls["n"] == 2, "should retry exactly once after the transient interrupt"
        assert out == tmp_path / "in.parquet"
        assert out.exists()

    def test_convert_vector_does_not_retry_non_transient(self, tmp_path: Path) -> None:
        import geoparquet_io as gpio

        from portolan_cli.preparation import convert_vector

        calls = {"n": 0}

        def fake_convert(src: str) -> None:
            calls["n"] += 1
            raise ValueError("No CRS found")

        with (
            patch.object(gpio, "convert", fake_convert),
            pytest.raises(ValueError, match="No CRS found"),
        ):
            convert_vector(tmp_path / "in.geojson", tmp_path)

        assert calls["n"] == 1

    def test_convert_tabular_retries_then_succeeds(self, tmp_path: Path) -> None:
        import geoparquet_io as gpio

        from portolan_cli.preparation import convert_tabular

        class InterruptException(Exception):
            pass

        class _FakeTable:
            geometry_column = None

            def write(self, path: str) -> None:
                Path(path).write_text("parquet-bytes")

        calls = {"n": 0}

        def fake_convert(src: str) -> _FakeTable:
            calls["n"] += 1
            if calls["n"] == 1:
                raise InterruptException("Query interrupted")
            return _FakeTable()

        with patch.object(gpio, "convert", fake_convert):
            out = convert_tabular(tmp_path / "in.csv", tmp_path)

        assert calls["n"] == 2, "should retry exactly once after the transient interrupt"
        assert out == tmp_path / "in.parquet"
        assert out.exists()


class TestIsRewriteTemp:
    """`add` must recognize the scratch file of an in-place rewrite.

    `_rewrite_parquet_in_place` writes a hidden sibling and swaps it in. A hard
    kill skips the cleanup, and `add` reads the collection directory before it
    converts anything, so a leftover would be ingested as a source (issue #805).
    """

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "name",
        [
            ".roads.portolan-rewrite.parquet",
            "roads.portolan-rewrite.parquet",
            ".roads.portolan-rewrite.parquet.meta",
        ],
    )
    def test_it_recognizes_the_marker(self, name: str) -> None:
        """The hidden name, an older visible one, and the metadata pass."""
        from portolan_cli.preparation import is_rewrite_temp

        assert is_rewrite_temp(Path(name)) is True

    @pytest.mark.unit
    @pytest.mark.parametrize("name", ["roads.parquet", "portolan.parquet", "rewrite.parquet"])
    def test_it_leaves_real_data_alone(self, name: str) -> None:
        """A name that merely resembles the marker is still data."""
        from portolan_cli.preparation import is_rewrite_temp

        assert is_rewrite_temp(Path(name)) is False


class TestForceRule:
    """`add --force` keeps an existing output, for every format handler (#386, #805, #947).

    `--force` means "ignore change detection", not "convert again". The handler
    converts the kept output again only when it reads that format, which brings
    an existing GeoParquet up to the profile.
    """

    @staticmethod
    def _keep(target: Path, layout: object) -> MagicMock:
        from portolan_cli.preparation import _convert_and_extract_metadata

        with (
            patch("portolan_cli.preparation.read_spatial_layout", return_value=layout),
            patch("portolan_cli.preparation._rewrite_parquet_in_place") as rewrite,
            patch("portolan_cli.api.is_geoparquet", return_value=False),
            patch("portolan_cli.api.extract_geoparquet_metadata"),
        ):
            _convert_and_extract_metadata(target, target.parent, GEOPARQUET, force=True)
        return rewrite

    @pytest.mark.unit
    def test_geoparquet_without_covering_column_is_rewritten(self, tmp_path: Path) -> None:
        from portolan_cli.metadata import SpatialLayout

        target = tmp_path / "roads.parquet"
        target.write_bytes(b"PAR1")

        rewrite = self._keep(target, SpatialLayout(is_geoparquet=True, has_bbox_covering=False))

        rewrite.assert_called_once()

    @pytest.mark.unit
    def test_conforming_geoparquet_is_left_alone(self, tmp_path: Path) -> None:
        from portolan_cli.metadata import SpatialLayout

        target = tmp_path / "roads.parquet"
        target.write_bytes(b"PAR1")

        rewrite = self._keep(target, SpatialLayout(is_geoparquet=True, has_bbox_covering=True))

        rewrite.assert_not_called()

    @pytest.mark.unit
    def test_plugin_output_is_kept_without_converting(self, tmp_path: Path) -> None:
        """A plugin's own output is not a format it reads, so --force keeps it as it is."""
        from dataclasses import dataclass

        from portolan_cli.preparation import _convert_and_extract_metadata
        from tests.unit.plugins.stub_handler import StubHandler

        @dataclass
        class _Xyz(StubHandler):
            def output_path(self, source: Path, output_dir: Path) -> Path:
                return output_dir / f"{source.stem}.parquet"

            def convert(self, source: Path, output_dir: Path, catalog_root: Path | None) -> Path:
                raise AssertionError("convert must not run")

        source = tmp_path / "survey.xyz"
        source.write_text("1 2 3\n", encoding="utf-8")
        (tmp_path / "survey.parquet").write_bytes(b"PAR1")

        output, _ = _convert_and_extract_metadata(
            source, tmp_path, _Xyz("xyz", frozenset({".xyz"})), force=True
        )

        assert output == tmp_path / "survey.parquet"


class TestAssertRewriteKeptEverything:
    """The swap gate compares the source and the rewritten file.

    geoparquet-io writes no `crs` key, and the GeoParquet specification reads
    an absent `crs` as OGC:CRS84. A projected file would come back labelled as
    longitude and latitude, so the gate refuses the swap (issue #805).
    """

    @pytest.mark.unit
    def test_it_accepts_a_faithful_rewrite(self, tmp_path: Path) -> None:
        """Same rows, same columns, same CRS."""
        from portolan_cli.metadata import RewriteFidelity
        from portolan_cli.preparation import _assert_rewrite_kept_everything

        before = RewriteFidelity(row_count=10, crs={"id": 1}, columns=frozenset({"geometry"}))
        after = RewriteFidelity(
            row_count=10, crs={"id": 1}, columns=frozenset({"geometry", "bbox"})
        )

        with patch("portolan_cli.preparation.read_rewrite_fidelity", return_value=after):
            _assert_rewrite_kept_everything(tmp_path / "a.parquet", tmp_path / "b.parquet", before)

    @pytest.mark.unit
    def test_it_refuses_a_lost_crs(self, tmp_path: Path) -> None:
        """A projected CRS that vanishes relabels every coordinate."""
        from pyproj import CRS

        from portolan_cli.errors import RewriteFidelityError
        from portolan_cli.metadata import RewriteFidelity
        from portolan_cli.preparation import _assert_rewrite_kept_everything

        projected = CRS.from_epsg(3857).to_json_dict()
        before = RewriteFidelity(row_count=10, crs=projected, columns=frozenset({"geometry"}))
        after = RewriteFidelity(row_count=10, crs=None, columns=frozenset({"geometry", "bbox"}))

        with (
            patch("portolan_cli.preparation.read_rewrite_fidelity", return_value=after),
            pytest.raises(RewriteFidelityError, match="CRS"),
        ):
            _assert_rewrite_kept_everything(tmp_path / "a.parquet", tmp_path / "b.parquet", before)

    @pytest.mark.unit
    def test_it_refuses_an_unparsable_lost_crs(self, tmp_path: Path) -> None:
        """A CRS pyproj cannot read is not provably WGS84, so the gate holds."""
        from portolan_cli.errors import RewriteFidelityError
        from portolan_cli.metadata import RewriteFidelity
        from portolan_cli.preparation import _assert_rewrite_kept_everything

        before = RewriteFidelity(row_count=10, crs={"id": 1}, columns=frozenset({"geometry"}))
        after = RewriteFidelity(row_count=10, crs=None, columns=frozenset({"geometry", "bbox"}))

        with (
            patch("portolan_cli.preparation.read_rewrite_fidelity", return_value=after),
            pytest.raises(RewriteFidelityError, match="CRS"),
        ):
            _assert_rewrite_kept_everything(tmp_path / "a.parquet", tmp_path / "b.parquet", before)

    @pytest.mark.unit
    @pytest.mark.parametrize("epsg", [4326, "OGC:CRS84"])
    def test_a_dropped_wgs84_crs_is_not_a_loss(self, tmp_path: Path, epsg: object) -> None:
        """An absent ``crs`` means OGC:CRS84, so dropping WGS84 loses nothing.

        geoparquet-io omits the key for WGS84 data. Treating that as a loss
        refused the rewrite of every WGS84 file, so the file never gained its
        bbox covering column. A partitioned ``add`` then rewrote each partition
        on its own, which made a 100,000-row add take 248s rather than 11s.
        """
        from pyproj import CRS

        from portolan_cli.metadata import RewriteFidelity
        from portolan_cli.preparation import _assert_rewrite_kept_everything

        wgs84 = CRS.from_user_input(epsg if isinstance(epsg, str) else 4326).to_json_dict()
        before = RewriteFidelity(row_count=10, crs=wgs84, columns=frozenset({"geometry"}))
        after = RewriteFidelity(row_count=10, crs=None, columns=frozenset({"geometry", "bbox"}))

        with patch("portolan_cli.preparation.read_rewrite_fidelity", return_value=after):
            _assert_rewrite_kept_everything(tmp_path / "a.parquet", tmp_path / "b.parquet", before)

    @pytest.mark.unit
    def test_it_refuses_a_changed_row_count(self, tmp_path: Path) -> None:
        """A rewrite that drops rows destroys data."""
        from portolan_cli.errors import RewriteFidelityError
        from portolan_cli.metadata import RewriteFidelity
        from portolan_cli.preparation import _assert_rewrite_kept_everything

        before = RewriteFidelity(row_count=10, crs=None, columns=frozenset({"geometry"}))
        after = RewriteFidelity(row_count=9, crs=None, columns=frozenset({"geometry"}))

        with (
            patch("portolan_cli.preparation.read_rewrite_fidelity", return_value=after),
            pytest.raises(RewriteFidelityError, match="rows"),
        ):
            _assert_rewrite_kept_everything(tmp_path / "a.parquet", tmp_path / "b.parquet", before)

    @pytest.mark.unit
    def test_it_refuses_a_dropped_column(self, tmp_path: Path) -> None:
        """An attribute column must survive. A new one may appear."""
        from portolan_cli.errors import RewriteFidelityError
        from portolan_cli.metadata import RewriteFidelity
        from portolan_cli.preparation import _assert_rewrite_kept_everything

        before = RewriteFidelity(row_count=1, crs=None, columns=frozenset({"geometry", "name"}))
        after = RewriteFidelity(row_count=1, crs=None, columns=frozenset({"geometry", "bbox"}))

        with (
            patch("portolan_cli.preparation.read_rewrite_fidelity", return_value=after),
            pytest.raises(RewriteFidelityError, match="name"),
        ):
            _assert_rewrite_kept_everything(tmp_path / "a.parquet", tmp_path / "b.parquet", before)

    @pytest.mark.unit
    def test_an_unreadable_source_skips_the_gate(self, tmp_path: Path) -> None:
        """With nothing to compare against, the gate cannot judge."""
        from portolan_cli.preparation import _assert_rewrite_kept_everything

        with patch("portolan_cli.preparation.read_rewrite_fidelity") as read:
            _assert_rewrite_kept_everything(tmp_path / "a.parquet", tmp_path / "b.parquet", None)

        read.assert_not_called()


class TestRewriteOrKeep:
    """A failed rewrite keeps the file. It never fails the whole `add`."""

    @pytest.mark.unit
    def test_a_fidelity_failure_keeps_the_file(self, tmp_path: Path, capsys: Any) -> None:
        """The operator reads what the rewrite would have lost."""
        from portolan_cli.conversion_config import VectorSettings
        from portolan_cli.errors import RewriteFidelityError
        from portolan_cli.preparation import _rewrite_or_keep

        source = tmp_path / "roads.parquet"
        source.write_bytes(b"PAR1")
        error = RewriteFidelityError("roads.parquet", "the CRS it declared")

        with patch("portolan_cli.preparation._rewrite_parquet_in_place", side_effect=error):
            _rewrite_or_keep(source, VectorSettings())

        captured = capsys.readouterr()
        assert "Kept roads.parquet as it is" in captured.out + captured.err
        assert "the CRS it declared" in captured.out + captured.err

    @pytest.mark.unit
    def test_any_other_failure_also_keeps_the_file(self, tmp_path: Path, capsys: Any) -> None:
        """A malformed geometry must not turn `add` into a failure."""
        from portolan_cli.conversion_config import VectorSettings
        from portolan_cli.preparation import _rewrite_or_keep

        source = tmp_path / "roads.parquet"
        source.write_bytes(b"PAR1")
        error = RuntimeError("Invalid Input Error: Skipping beyond end of binary data")

        with patch("portolan_cli.preparation._rewrite_parquet_in_place", side_effect=error):
            _rewrite_or_keep(source, VectorSettings())

        captured = capsys.readouterr()
        assert "Kept roads.parquet as it is" in captured.out + captured.err
