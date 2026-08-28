"""`add` writes GeoParquet that conforms to the Portolan profile (issue #805).

The default workflow used to produce data that failed Portolan's own ``check``.
``add`` copied every ``.parquet`` source through untouched and never read the
catalog's ``conversion.vector`` settings, so the written file carried no bbox
covering column. rashid resolves both ``PTL-DAT-006`` and ``PTL-DAT-007``
through that column, so its absence failed one rule and left the other
unevaluated.

These tests read the written Parquet footer rather than the rashid verdict, so
they name the mechanism. The end-to-end proof lives in
``test_generated_catalog_conformance.py``.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
POINTS = FIXTURES / "vector" / "valid" / "points.parquet"


def _init(root: Path) -> None:
    result = CliRunner().invoke(
        cli,
        [
            "init",
            str(root),
            "--auto",
            "--title",
            "Optimization Catalog",
            "--description",
            "Proves add writes a covering column.",
            "--license",
            "CC-BY-4.0",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output


def _add(root: Path, target: Path, *args: str) -> str:
    result = CliRunner().invoke(
        cli,
        ["add", "--portolan-dir", str(root), str(target), *args],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    return result.output


def _covering(path: Path) -> dict[str, object] | None:
    parquet = pq.ParquetFile(path)
    geo = json.loads((parquet.schema_arrow.metadata or {})[b"geo"].decode("utf-8"))
    covering = geo["columns"][geo["primary_column"]].get("covering")
    return covering if isinstance(covering, dict) else None


class TestDefaultAddWritesACoveringColumn:
    def test_single_file_collection_is_rewritten_in_place(self, tmp_path: Path) -> None:
        """The source is its own destination, so the rewrite swaps the file."""
        root = tmp_path / "catalog"
        collection = root / "roads"
        collection.mkdir(parents=True)
        shutil.copy(POINTS, collection / "roads.parquet")
        _init(root)

        assert _covering(collection / "roads.parquet") is None
        _add(root, collection / "roads.parquet")

        written = collection / "roads.parquet"
        assert "bbox" in pq.ParquetFile(written).schema_arrow.names
        assert _covering(written) is not None
        # The rewrite leaves nothing behind, hidden scratch file included.
        assert list(collection.glob("*portolan-rewrite*")) == []

    def test_row_count_and_columns_survive_the_rewrite(self, tmp_path: Path) -> None:
        """A rewrite adds a column. It must not drop rows or attributes."""
        root = tmp_path / "catalog"
        collection = root / "roads"
        collection.mkdir(parents=True)
        shutil.copy(POINTS, collection / "roads.parquet")
        _init(root)
        before = pq.ParquetFile(POINTS)

        _add(root, collection / "roads.parquet")

        after = pq.ParquetFile(collection / "roads.parquet")
        assert after.metadata.num_rows == before.metadata.num_rows
        assert set(before.schema_arrow.names) <= set(after.schema_arrow.names)

    def test_add_reports_the_rewrite(self, tmp_path: Path) -> None:
        """An operator whose file changed on disk is told why."""
        root = tmp_path / "catalog"
        collection = root / "roads"
        collection.mkdir(parents=True)
        shutil.copy(POINTS, collection / "roads.parquet")
        _init(root)

        output = _add(root, collection / "roads.parquet")

        assert "no bbox covering column" in output


class TestAddLeavesFilesAlone:
    def test_conformant_geoparquet_is_copied_untouched(self, tmp_path: Path) -> None:
        """A file that already carries the column costs nothing to add."""
        import geoparquet_io as gpio  # type: ignore[import-untyped]

        root = tmp_path / "catalog"
        collection = root / "roads"
        collection.mkdir(parents=True)
        target = collection / "roads.parquet"
        gpio.convert(str(POINTS)).add_bbox().sort_hilbert().write(str(target))
        _init(root)
        before = target.read_bytes()

        _add(root, target)

        assert target.read_bytes() == before

    def test_tabular_parquet_is_not_given_a_bbox(self, tmp_path: Path) -> None:
        """A Parquet file with no geometry cannot carry a covering column."""
        root = tmp_path / "catalog"
        collection = root / "demographics"
        collection.mkdir(parents=True)
        target = collection / "census.parquet"
        pq.write_table(pa.table({"tract": ["001", "002"], "population": [1, 2]}), target)
        _init(root)
        portolan_dir = root / ".portolan"
        config = portolan_dir / "config.yaml"
        config.write_text(config.read_text() + "tabular:\n  enabled: true\n", encoding="utf-8")
        before = target.read_bytes()

        _add(root, target)

        assert target.read_bytes() == before

    def test_opting_out_keeps_the_copy(self, tmp_path: Path) -> None:
        """`add_bbox: false` and `sort: none` restore the old passthrough."""
        root = tmp_path / "catalog"
        collection = root / "roads"
        collection.mkdir(parents=True)
        target = collection / "roads.parquet"
        shutil.copy(POINTS, target)
        _init(root)
        config = root / ".portolan" / "config.yaml"
        config.write_text(
            config.read_text() + "conversion:\n  vector:\n    sort: none\n    add_bbox: false\n",
            encoding="utf-8",
        )
        before = target.read_bytes()

        _add(root, target)

        assert target.read_bytes() == before
        assert _covering(target) is None


class TestReconvertRepairsRowOrder:
    """`add --force --reconvert` is the documented repair for unsorted rows.

    The footer test cannot see row order, so a file that carries the covering
    column but holds unordered rows is copied and still fails PTL-DAT-006. The
    operator's answer is an explicit re-convert, and the docs and the `convert`
    fixer's decline message both name it, so it has to work.
    """

    @pytest.mark.integration
    def test_reconvert_reorders_a_file_that_already_has_the_column(self, tmp_path: Path) -> None:
        """A shuffled file keeps its order through `add`, and loses it on reconvert."""
        import geoparquet_io as gpio

        root = tmp_path / "catalog"
        collection = root / "roads"
        collection.mkdir(parents=True)
        target = collection / "roads.parquet"

        # Seed a file that carries the column but holds rows in reverse order,
        # which is what `add` cannot detect from the footer.
        gpio.convert(str(POINTS)).add_bbox().write(str(target))
        table = pq.read_table(target)
        reversed_rows = table.take(list(reversed(range(table.num_rows))))
        pq.write_table(reversed_rows.replace_schema_metadata(table.schema.metadata), target)
        seeded_ids = pq.read_table(target).column("id").to_pylist()
        _init(root)

        # A plain add copies it: the column is there, so nothing looks wrong.
        _add(root, target)
        assert pq.read_table(target).column("id").to_pylist() == seeded_ids

        output = _add(root, target, "--force", "--reconvert")

        assert "re-convert requested" in output
        assert pq.read_table(target).column("id").to_pylist() != seeded_ids
        assert sorted(pq.read_table(target).column("id").to_pylist()) == sorted(seeded_ids)
        assert _covering(target) is not None


class TestForceStillWritesAConformingFile:
    """`add --force` must not hand back a file that fails `check`.

    `--force` means "ignore change detection", not "convert again", so the add
    pipeline skips conversion when the output already exists (issue #386). For
    a single-file collection the output *is* the source, so that skip used to
    return a file with no covering column and no warning. The catalog then
    failed its own `check` on PTL-DAT-007, which is the failure #805 removes.
    """

    def test_force_rewrites_a_file_it_did_not_convert(self, tmp_path: Path) -> None:
        """A never-added, non-conforming file gets the column under `--force`."""
        root = tmp_path / "catalog"
        collection = root / "roads"
        collection.mkdir(parents=True)
        target = collection / "roads.parquet"
        shutil.copy(POINTS, target)
        _init(root)
        assert _covering(target) is None

        output = _add(root, target, "--force")

        assert "no bbox covering column" in output
        assert _covering(target) is not None
        assert "bbox" in pq.ParquetFile(target).schema_arrow.names

    def test_force_leaves_a_conforming_file_alone(self, tmp_path: Path) -> None:
        """The check is the footer, so `--force` still copies a good file."""
        import geoparquet_io as gpio

        root = tmp_path / "catalog"
        collection = root / "roads"
        collection.mkdir(parents=True)
        target = collection / "roads.parquet"
        gpio.convert(str(POINTS)).add_bbox().sort_hilbert().write(str(target))
        _init(root)
        before = target.read_bytes()

        _add(root, target, "--force")

        assert target.read_bytes() == before

    def test_force_does_not_touch_tabular_parquet(self, tmp_path: Path) -> None:
        """A geometry-less table has no covering column to gain."""
        root = tmp_path / "catalog"
        collection = root / "demographics"
        collection.mkdir(parents=True)
        target = collection / "census.parquet"
        pq.write_table(pa.table({"tract": ["001", "002"], "population": [1, 2]}), target)
        _init(root)
        config = root / ".portolan" / "config.yaml"
        config.write_text(config.read_text() + "tabular:\n  enabled: true\n", encoding="utf-8")
        before = target.read_bytes()

        _add(root, target, "--force")

        assert target.read_bytes() == before


class TestRewriteScratchFileIsNeverIngested:
    """A leftover scratch file must not become a source or an asset.

    `_rewrite_parquet_in_place` writes a sibling and swaps it in. `add` reads
    the collection directory before it converts anything, and a hard kill skips
    the cleanup, so a leftover would otherwise be collected as a source. The
    rewrite then deletes it mid-run and the add fails on a missing file.
    """

    @pytest.mark.parametrize(
        "leftover", [".roads.portolan-rewrite.parquet", "x.portolan-rewrite.parquet"]
    )
    def test_a_leftover_scratch_file_does_not_break_add(
        self, tmp_path: Path, leftover: str
    ) -> None:
        """Both the hidden name and an older visible one are ignored."""
        root = tmp_path / "catalog"
        collection = root / "roads"
        collection.mkdir(parents=True)
        target = collection / "roads.parquet"
        shutil.copy(POINTS, target)
        (collection / leftover).write_bytes(b"")
        _init(root)

        output = _add(root, target)

        assert _covering(target) is not None
        collection_json = json.loads((collection / "collection.json").read_text(encoding="utf-8"))
        hrefs = [asset.get("href", "") for asset in collection_json["assets"].values()]
        assert not any("portolan-rewrite" in href for href in hrefs), output


class TestRewritePreservesPublisherMetadata:
    """geoparquet-io writes a fresh `geo` key and drops every other one.

    Publishers do set other keys. geopandas writes `pandas`, which carries the
    index and dtype information `pd.read_parquet` restores. The rewrite mutates
    the operator's own file, so it must put those keys back.
    """

    def test_extra_schema_metadata_survives_the_rewrite(self, tmp_path: Path) -> None:
        """A publisher key set on the source is still there afterwards."""
        root = tmp_path / "catalog"
        collection = root / "roads"
        collection.mkdir(parents=True)
        target = collection / "roads.parquet"

        table = pq.read_table(POINTS)
        metadata = dict(table.schema.metadata or {})
        metadata[b"publisher:provenance"] = b"national cadastre 2024"
        pq.write_table(table.replace_schema_metadata(metadata), target)
        _init(root)

        _add(root, target)

        after = pq.ParquetFile(target).schema_arrow.metadata or {}
        assert after[b"publisher:provenance"] == b"national cadastre 2024"
        # The writer still owns `geo`: the rewrite added the covering column.
        assert _covering(target) is not None

    def test_the_geo_key_is_not_restored_from_the_source(self, tmp_path: Path) -> None:
        """Restoring the old `geo` would contradict the column just added."""
        root = tmp_path / "catalog"
        collection = root / "roads"
        collection.mkdir(parents=True)
        target = collection / "roads.parquet"
        shutil.copy(POINTS, target)
        source_geo = (pq.ParquetFile(target).schema_arrow.metadata or {})[b"geo"]
        _init(root)

        _add(root, target)

        assert (pq.ParquetFile(target).schema_arrow.metadata or {})[b"geo"] != source_geo


class TestPartialOptOutKeepsTheCopy:
    """`add_bbox: false` with sorting on must not rewrite on every add.

    The footer is the only thing the decision can read cheaply. With the bbox
    column off there is nothing to detect and no outcome a rewrite would
    change, so rewriting anyway would repeat forever and report a reason the
    rewrite cannot fix.
    """

    @pytest.mark.parametrize(
        "vector_config",
        [
            "    sort: hilbert\n    add_bbox: false\n",
            "    sort: none\n    add_bbox: false\n    spatial_index: h3\n",
        ],
    )
    def test_bbox_off_keeps_the_copy(self, tmp_path: Path, vector_config: str) -> None:
        """No rewrite runs, and the operator is told nothing misleading."""
        root = tmp_path / "catalog"
        collection = root / "roads"
        collection.mkdir(parents=True)
        target = collection / "roads.parquet"
        shutil.copy(POINTS, target)
        _init(root)
        config = root / ".portolan" / "config.yaml"
        config.write_text(
            config.read_text() + f"conversion:\n  vector:\n{vector_config}",
            encoding="utf-8",
        )
        before = target.read_bytes()

        output = _add(root, target)

        assert target.read_bytes() == before
        assert "no bbox covering column" not in output

    def test_reconvert_still_repairs_a_sort_only_catalog(self, tmp_path: Path) -> None:
        """The explicit repair path stays open when `add_bbox` is off."""
        root = tmp_path / "catalog"
        collection = root / "roads"
        collection.mkdir(parents=True)
        target = collection / "roads.parquet"
        shutil.copy(POINTS, target)
        _init(root)
        config = root / ".portolan" / "config.yaml"
        config.write_text(
            config.read_text() + "conversion:\n  vector:\n    sort: hilbert\n    add_bbox: false\n",
            encoding="utf-8",
        )
        _add(root, target)

        output = _add(root, target, "--force", "--reconvert")

        assert "re-convert requested" in output
        # Sorting ran, and `add_bbox: false` was honored.
        assert "bbox" not in pq.ParquetFile(target).schema_arrow.names


class TestTheRewriteNeverLosesData:
    """The rewrite replaces the operator's file, so it proves it lost nothing.

    geoparquet-io writes no `crs` key. The GeoParquet specification reads an
    absent `crs` as OGC:CRS84, so a projected file comes back labelled as
    longitude and latitude. See
    `context/shared/known-issues/geoparquet-io-write-drops-crs.md`, and issue
    #810 for the pin bump that removes the bug.
    """

    def test_a_projected_file_keeps_its_crs(self, tmp_path: Path) -> None:
        """`add` keeps the file rather than relabel EPSG:3857 as WGS84.

        This test asserts on an upstream bug, so it fails when the bug goes
        away. Issue #810 bumps the geoparquet-io pin to a version that writes
        the CRS. After that bump the guard stops firing, `add` rewrites the
        file, and the byte comparison below fails. That failure is the pin
        bump working. Replace the assertions with a check that the rewritten
        file still declares EPSG:3857.
        """
        import geopandas as gpd
        from shapely.geometry import Point

        root = tmp_path / "catalog"
        collection = root / "roads"
        collection.mkdir(parents=True)
        target = collection / "roads.parquet"
        gpd.GeoDataFrame(
            {"n": [1, 2]},
            geometry=[Point(-8238310, 4970072), Point(-8237000, 4971000)],
            crs="EPSG:3857",
        ).to_parquet(target)
        _init(root)
        before = target.read_bytes()

        output = _add(root, target)

        assert target.read_bytes() == before
        assert "Kept roads.parquet as it is" in output
        assert "CRS" in output
        assert gpd.read_parquet(target).crs.to_epsg() == 3857

    def test_a_file_geoparquet_io_cannot_read_is_kept(self, tmp_path: Path) -> None:
        """A malformed geometry must not turn `add` into a failure."""
        root = tmp_path / "catalog"
        collection = root / "roads"
        collection.mkdir(parents=True)
        target = collection / "roads.parquet"

        # A truncated WKB point. geoparquet-io raises on it, and `add` used to
        # copy such a file through untouched.
        table = pa.table(
            {
                "geometry": pa.array([bytes.fromhex("0101000000000000000000")], type=pa.binary()),
                "id": pa.array([1], type=pa.int64()),
            }
        )
        geo = {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {"encoding": "WKB", "geometry_types": ["Point"], "bbox": [0, 0, 1, 1]}
            },
        }
        pq.write_table(
            table.cast(table.schema.with_metadata({b"geo": json.dumps(geo).encode()})), target
        )
        _init(root)
        before = target.read_bytes()

        output = _add(root, target)

        assert target.read_bytes() == before
        assert "Kept roads.parquet as it is" in output

    def test_the_scratch_file_is_gone_after_a_refused_rewrite(self, tmp_path: Path) -> None:
        """A refused swap still cleans up after itself."""
        import geopandas as gpd
        from shapely.geometry import Point

        root = tmp_path / "catalog"
        collection = root / "roads"
        collection.mkdir(parents=True)
        target = collection / "roads.parquet"
        gpd.GeoDataFrame(
            {"n": [1]}, geometry=[Point(-8238310, 4970072)], crs="EPSG:3857"
        ).to_parquet(target)
        _init(root)

        _add(root, target)

        assert list(collection.glob("*portolan-rewrite*")) == []
        assert list(collection.glob("*.meta")) == []


class TestTheGuardsCoverEveryRewritePath:
    """Findings from the adversarial review of PR #808."""

    def test_a_different_destination_keeps_the_crs_too(self, tmp_path: Path) -> None:
        """`convert_vector` gates the copy path, not only the in-place path.

        Converting straight into a new destination used to skip
        `_assert_rewrite_kept_everything`, so a `.parquet` source lost its CRS
        on that path while the same file kept it in place.
        """
        import geopandas as gpd
        from shapely.geometry import Point

        from portolan_cli.preparation import convert_vector

        source_dir = tmp_path / "src"
        source_dir.mkdir()
        source = source_dir / "roads.parquet"
        gpd.GeoDataFrame(
            {"n": [1, 2]},
            geometry=[Point(-8238310, 4970072), Point(-8237000, 4971000)],
            crs="EPSG:3857",
        ).to_parquet(source)

        dest = tmp_path / "dest"
        dest.mkdir()
        output = convert_vector(source, dest)

        assert gpd.read_parquet(output).crs.to_epsg() == 3857

    def test_a_changed_crs_is_refused_like_a_dropped_one(self, tmp_path: Path) -> None:
        """The gate compares the CRS value, not only whether one is present."""
        import geopandas as gpd
        import pytest
        from shapely.geometry import Point

        from portolan_cli.errors import RewriteFidelityError
        from portolan_cli.metadata.geoparquet import read_rewrite_fidelity
        from portolan_cli.preparation import _assert_rewrite_kept_everything

        source = tmp_path / "a.parquet"
        rewritten = tmp_path / "b.parquet"
        frame = gpd.GeoDataFrame({"n": [1]}, geometry=[Point(0, 0)], crs="EPSG:3857")
        frame.to_parquet(source)
        frame.to_crs("EPSG:4326").to_parquet(rewritten)

        before = read_rewrite_fidelity(source)
        with pytest.raises(RewriteFidelityError, match="changed"):
            _assert_rewrite_kept_everything(source, rewritten, before)

    def test_an_equivalent_crs_encoding_is_not_a_change(self, tmp_path: Path) -> None:
        """Two encodings of one CRS must not trip the gate."""
        import geopandas as gpd
        from shapely.geometry import Point

        from portolan_cli.metadata.geoparquet import read_rewrite_fidelity
        from portolan_cli.preparation import _assert_rewrite_kept_everything

        source = tmp_path / "a.parquet"
        rewritten = tmp_path / "b.parquet"
        frame = gpd.GeoDataFrame({"n": [1]}, geometry=[Point(0, 0)], crs="EPSG:4326")
        frame.to_parquet(source)
        frame.to_parquet(rewritten)

        before = read_rewrite_fidelity(source)
        _assert_rewrite_kept_everything(source, rewritten, before)

    def test_an_uncompressed_file_can_be_rewritten(self, tmp_path: Path) -> None:
        """`ParquetFile` says UNCOMPRESSED; `ParquetWriter` only accepts none."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from portolan_cli.preparation import _restore_schema_metadata

        target = tmp_path / "plain.parquet"
        table = pa.table({"n": [1, 2, 3]})
        pq.write_table(table, target, compression="NONE")

        _restore_schema_metadata(target, {b"pandas": b"{}"})

        assert pq.ParquetFile(target).schema_arrow.metadata[b"pandas"] == b"{}"
        assert pq.ParquetFile(target).metadata.num_rows == 3

    def test_a_misspelled_setting_warns_once_per_catalog(self, tmp_path: Path) -> None:
        """`add` reads the settings per file, so the console must not repeat."""
        from portolan_cli.conversion_config import (
            get_vector_settings,
            reset_config_warning_cache,
        )
        from portolan_cli.output import warn

        root = tmp_path / "catalog"
        (root / ".portolan").mkdir(parents=True)
        (root / ".portolan" / "config.yaml").write_text(
            "conversion:\n  vector:\n    sort: hilbrt\n"
        )

        reset_config_warning_cache()
        shown: list[str] = []
        original = warn

        import portolan_cli.output as output_module

        output_module.warn = lambda message: shown.append(message)  # type: ignore[assignment]
        try:
            for _ in range(5):
                get_vector_settings(root)
        finally:
            output_module.warn = original  # type: ignore[assignment]

        assert len(shown) == 1, shown
