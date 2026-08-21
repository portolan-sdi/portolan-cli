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
        # The rewrite leaves nothing behind.
        assert list(collection.glob("*.portolan-rewrite.parquet")) == []

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
