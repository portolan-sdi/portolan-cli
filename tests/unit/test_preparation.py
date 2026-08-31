"""Unit tests for the preparation module (issue #623).

`preparation.py` holds the per-item conversion/metadata-routing extracted from
add.py. These tests exercise the routing and STAC-shaping logic directly against
the new module's seam, independent of the full add() orchestration.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pystac
import pytest

from portolan_cli.formats import FormatType
from portolan_cli.preparation import (
    _add_statistics_to_properties,
    _convert_and_extract_metadata,
    _extract_bbox_wgs84,
    _extract_statistics_best_effort,
    _fix_collection_level_asset_hrefs,
    _handle_cloud_native_vector,
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

        with patch(
            "portolan_cli.preparation.extract_pmtiles_metadata", return_value=sentinel
        ) as extract:
            output_path, metadata = _convert_and_extract_metadata(
                source, item_dir, FormatType.VECTOR
            )

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
            "portolan_cli.preparation.extract_flatgeobuf_metadata", return_value=sentinel
        ) as extract:
            output_path, metadata = _convert_and_extract_metadata(
                source, item_dir, FormatType.VECTOR
            )

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
            patch("portolan_cli.preparation.convert_vector", return_value=converted) as convert,
            patch(
                "portolan_cli.preparation.extract_geoparquet_metadata", return_value=meta
            ) as extract,
        ):
            output_path, metadata = _convert_and_extract_metadata(
                source, item_dir, FormatType.VECTOR
            )

        assert output_path == converted
        assert metadata is meta
        convert.assert_called_once_with(source, item_dir, None, reconvert=False)
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
            patch("portolan_cli.preparation.convert_raster", return_value=converted) as convert,
            patch("portolan_cli.preparation.extract_cog_metadata", return_value=meta) as extract,
        ):
            output_path, metadata = _convert_and_extract_metadata(
                source, item_dir, FormatType.RASTER
            )

        assert output_path == converted
        assert metadata is meta
        convert.assert_called_once_with(source, item_dir)
        extract.assert_called_once_with(converted)


class TestHandleCloudNativeVector:
    """Force/reconvert semantics for copy-as-is cloud-native vectors (issue #386)."""

    def test_copies_when_output_absent(self, tmp_path: Path) -> None:
        source = tmp_path / "a.pmtiles"
        source.write_bytes(b"payload")
        output = tmp_path / "item" / "a.pmtiles"
        output.parent.mkdir()

        result = _handle_cloud_native_vector(
            source, output, lambda p: f"meta:{p.name}", force=False, reconvert=False
        )

        assert output.exists()
        assert output.read_bytes() == b"payload"
        assert result == "meta:a.pmtiles"

    def test_existing_output_without_force_raises(self, tmp_path: Path) -> None:
        source = tmp_path / "a.pmtiles"
        source.write_bytes(b"new")
        output = tmp_path / "item" / "a.pmtiles"
        output.parent.mkdir()
        output.write_bytes(b"old")

        with pytest.raises(FileExistsError, match="already exists"):
            _handle_cloud_native_vector(
                source, output, lambda p: "meta", force=False, reconvert=False
            )
        assert output.read_bytes() == b"old"  # untouched

    def test_force_reconvert_recopies_from_source(self, tmp_path: Path) -> None:
        source = tmp_path / "a.pmtiles"
        source.write_bytes(b"new")
        output = tmp_path / "item" / "a.pmtiles"
        output.parent.mkdir()
        output.write_bytes(b"old")

        _handle_cloud_native_vector(source, output, lambda p: "meta", force=True, reconvert=True)

        assert output.read_bytes() == b"new"  # re-copied from source


class TestStatisticsShaping:
    """_extract_statistics_best_effort + _add_statistics_to_properties."""

    def test_disabled_stats_returns_empty(self, tmp_path: Path) -> None:
        with patch("portolan_cli.preparation.get_setting", return_value=False):
            band_stats, parquet_stats = _extract_statistics_best_effort(
                tmp_path / "x.parquet", FormatType.VECTOR, tmp_path
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
                tmp_path / "x.parquet", FormatType.VECTOR, tmp_path
            )
        assert band_stats == []
        assert parquet_stats == {}

    def test_raster_band_stats_attached_to_bands(self) -> None:
        props: dict = {"bands": [{"data_type": "uint8"}, {"data_type": "uint8"}]}
        stat0 = MagicMock()
        stat0.to_stac_dict.return_value = {"minimum": 0, "maximum": 255}
        stat1 = MagicMock()
        stat1.to_stac_dict.return_value = {"minimum": 1, "maximum": 200}

        _add_statistics_to_properties(
            props, FormatType.RASTER, [stat0, stat1], {}, stats_enabled=True
        )

        assert props["bands"][0]["statistics"] == {"minimum": 0, "maximum": 255}
        assert props["bands"][1]["statistics"] == {"minimum": 1, "maximum": 200}

    def test_vector_column_stats_attached(self) -> None:
        props: dict = {}
        stat = MagicMock()
        stat.to_stac_dict.return_value = {"min": 0, "max": 9}

        _add_statistics_to_properties(
            props, FormatType.VECTOR, [], {"pop": stat}, stats_enabled=True
        )

        assert props["table:column_statistics"] == {"pop": {"min": 0, "max": 9}}

    def test_disabled_flag_skips_mutation(self) -> None:
        props: dict = {"bands": [{"data_type": "uint8"}]}
        stat = MagicMock()
        stat.to_stac_dict.return_value = {"minimum": 0}

        _add_statistics_to_properties(props, FormatType.RASTER, [stat], {}, stats_enabled=False)

        assert "statistics" not in props["bands"][0]


class TestFixCollectionLevelAssetHrefs:
    """/ RULE-0010: collection-level hrefs are ./file and keys are stems."""

    def test_primary_data_key_becomes_stem_and_href_normalized(self) -> None:
        assets = {
            "data": pystac.Asset(href="../census.parquet", media_type="x", roles=["data"]),
        }

        fixed = _fix_collection_level_asset_hrefs(assets)

        assert "data" not in fixed
        assert "census" in fixed  # key derived from stem, not literal "data"
        assert fixed["census"].href == "./census.parquet"

    def test_non_data_key_preserved_href_prefixed(self) -> None:
        assets = {
            "thumbnail": pystac.Asset(
                href="thumb.png", media_type="image/png", roles=["thumbnail"]
            ),
        }

        fixed = _fix_collection_level_asset_hrefs(assets)

        assert set(fixed) == {"thumbnail"}
        assert fixed["thumbnail"].href == "./thumb.png"


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
    ``add`` converts single-layer vectors via ``preparation.convert_vector`` (not
    ``convert.convert_file``/``_convert_vector``), so the bounded transient-interrupt
    retry must protect *this* code path too. A single ``InterruptException`` on the
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

        with patch.object(gpio, "convert", fake_convert):
            with pytest.raises(ValueError, match="No CRS found"):
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


class TestEnsureConformingGeoparquet:
    """`add --force` skips conversion, so it needs its own conformance check.

    `--force` means "ignore change detection", not "convert again", and for a
    single-file collection the output is the source. Without this the skip
    handed back a file with no covering column (issue #805).
    """

    @pytest.mark.unit
    def test_it_rewrites_a_file_with_no_covering_column(self, tmp_path: Path) -> None:
        """The rewrite runs and the operator is told."""
        from portolan_cli.metadata import SpatialLayout
        from portolan_cli.preparation import _ensure_conforming_geoparquet

        target = tmp_path / "roads.parquet"
        target.write_bytes(b"PAR1")
        layout = SpatialLayout(is_geoparquet=True, has_bbox_covering=False)

        with (
            patch("portolan_cli.preparation.read_spatial_layout", return_value=layout),
            patch("portolan_cli.preparation._rewrite_parquet_in_place") as rewrite,
        ):
            _ensure_conforming_geoparquet(target, None)

        rewrite.assert_called_once()

    @pytest.mark.unit
    def test_it_leaves_a_conforming_file_alone(self, tmp_path: Path) -> None:
        """A file that already carries the column costs nothing."""
        from portolan_cli.metadata import SpatialLayout
        from portolan_cli.preparation import _ensure_conforming_geoparquet

        target = tmp_path / "roads.parquet"
        target.write_bytes(b"PAR1")
        layout = SpatialLayout(is_geoparquet=True, has_bbox_covering=True)

        with (
            patch("portolan_cli.preparation.read_spatial_layout", return_value=layout),
            patch("portolan_cli.preparation._rewrite_parquet_in_place") as rewrite,
        ):
            _ensure_conforming_geoparquet(target, None)

        rewrite.assert_not_called()

    @pytest.mark.unit
    def test_it_ignores_a_non_parquet_output(self, tmp_path: Path) -> None:
        """A COG reaches this path too, and has no covering column to gain."""
        from portolan_cli.preparation import _ensure_conforming_geoparquet

        target = tmp_path / "dem.tif"
        target.write_bytes(b"II*\x00")

        with patch("portolan_cli.preparation.read_spatial_layout") as layout:
            _ensure_conforming_geoparquet(target, None)

        layout.assert_not_called()


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
        """The upstream writer drops the CRS, so the swap must not happen."""
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
