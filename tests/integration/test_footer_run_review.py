"""Adversarial checks for the footer memo and destructive Parquet rewrites."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from portolan_cli.formats import current_run, in_run, parquet_footer, run_scope
from portolan_cli.preparation import convert_vector
from portolan_cli.run_state import RunState

pytestmark = pytest.mark.integration

POINTS = Path(__file__).resolve().parents[1] / "fixtures/vector/valid/points.parquet"


def test_rewrite_keeps_nested_field_metadata(tmp_path: Path) -> None:
    """A struct child's units are publisher metadata too."""
    source = tmp_path / "points.parquet"
    table = pq.read_table(POINTS)
    field = pa.field("height", pa.float64(), metadata={b"units": b"metres"})
    values = pa.array([{"height": 1.0}] * len(table), type=pa.struct([field]))
    pq.write_table(table.append_column("measurement", values), source)

    convert_vector(source, tmp_path)

    child = pq.read_schema(source).field("measurement").type.field("height")
    assert child.metadata == {b"units": b"metres"}


def test_rewrite_keeps_arrow_extension_metadata(tmp_path: Path) -> None:
    """An Arrow extension declaration defines the meaning of a column."""
    source = tmp_path / "points.parquet"
    table = pq.read_table(POINTS)
    field = pa.field(
        "measurement",
        pa.int64(),
        metadata={
            b"ARROW:extension:name": b"publisher.measurement",
            b"ARROW:extension:metadata": b'{"units":"metres"}',
        },
    )
    pq.write_table(table.append_column(field, pa.array([1] * len(table))), source)

    convert_vector(source, tmp_path)

    assert pq.read_schema(source).field("measurement").metadata == field.metadata


@pytest.mark.parametrize(
    "declaration",
    [b'{"publisher":"survey"}', b'{"crs_type":"publisher.other"}'],
    ids=["publisher", "unknown-crs-type"],
)
def test_unreadable_extension_declaration_is_kept(tmp_path: Path, declaration: bytes) -> None:
    """A declaration the gate cannot normalize must be compared as raw bytes."""
    import geoarrow.pyarrow  # noqa: F401 - register the types as geoparquet-io does

    from portolan_cli.metadata.geoparquet import read_rewrite_fidelity

    source = tmp_path / "points.parquet"
    table = pq.read_table(POINTS)
    index = table.schema.get_field_index("geometry")
    metadata = {
        b"ARROW:extension:name": b"geoarrow.wkb",
        b"ARROW:extension:metadata": declaration,
    }
    field = table.schema.field(index).with_metadata(metadata)
    pq.write_table(table.set_column(index, field, table.column(index)), source)

    fidelity = read_rewrite_fidelity(source)

    assert fidelity is not None
    assert fidelity.field_metadata is not None
    assert fidelity.field_metadata["geometry"] == metadata


def test_rewrite_invalidates_footer_when_stat_key_is_unchanged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Writers must invalidate even when a coarse filesystem reuses a stat key."""
    import random

    import geoparquet_io as gpio
    from geoparquet_io.api.table import Table

    import portolan_cli.run_state as run_state

    source = tmp_path / "points.parquet"
    table = gpio.convert(POINTS).add_bbox()
    order = list(range(table.table.num_rows))
    random.Random(3).shuffle(order)
    Table(table.table.take(order), geometry_column=table.geometry_column, crs=table.crs).write(
        source
    )
    original = source.read_bytes()
    state = RunState.open(tmp_path)
    # Model a coarse filesystem without stable inode IDs. Retain the size
    # component: a larger output must always miss the memo, even there.
    monkeypatch.setattr(run_state, "_stat_key", lambda st: (0, st.st_size, 0, 0))
    with in_run(state):
        checksum = state.checksum(source)
        parquet_footer(source)
        convert_vector(source, tmp_path, reconvert=True)
        assert source.stat().st_size == len(original)
        assert source.read_bytes() != original
        assert parquet_footer(source).metadata.equals(pq.read_metadata(source))
        assert state.checksum(source) != checksum


def test_copy_invalidates_destination_footer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Copying an existing Parquet must drop reads of the previous output."""
    import portolan_cli.run_state as run_state
    from portolan_cli.conversion_config import VectorSettings
    from portolan_cli.convert import PROVENANCE_KEY, provenance_record

    source = tmp_path / "points.parquet"
    table = pq.read_table(POINTS)
    pq.write_table(table, source)
    # This is a previous output of this source, so the ownership guard may
    # replace it. Give both tables the same metadata to retain equal sizes.
    table = table.replace_schema_metadata(
        {
            **(table.schema.metadata or {}),
            PROVENANCE_KEY.encode(): provenance_record(source).encode(),
        }
    )
    pq.write_table(table, source)
    destination = tmp_path / "destination"
    destination.mkdir()
    output = destination / source.name
    renamed = table.rename_columns(["town" if n == "name" else n for n in table.column_names])
    pq.write_table(renamed.replace_schema_metadata(table.schema.metadata), output)
    assert output.stat().st_size == source.stat().st_size
    state = RunState.open(tmp_path)
    monkeypatch.setattr(run_state, "_stat_key", lambda st: (0, st.st_size, 0, 0))
    with in_run(state):
        assert "town" in parquet_footer(output).schema_arrow.names
        convert_vector(source, destination, settings=VectorSettings(add_bbox=False))
        assert parquet_footer(output).schema_arrow.names == pq.read_schema(source).names


def test_reused_scratch_path_does_not_reuse_a_deleted_footer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A prior scratch footer cannot supply the gate or replacement metadata."""
    import portolan_cli.run_state as run_state

    source = tmp_path / "points.parquet"
    source.write_bytes(POINTS.read_bytes())
    scratch = tmp_path / ".points.portolan-rewrite.parquet"
    pq.write_table(pa.table({"old": [1]}), scratch)
    state = RunState.open(tmp_path)
    monkeypatch.setattr(run_state, "_stat_key", lambda _st: (1, 1, 1, 1))
    with in_run(state):
        assert parquet_footer(scratch).schema_arrow.names == ["old"]
        scratch.unlink()
        convert_vector(source, tmp_path)
        assert "bbox" in pq.read_schema(source).names


def test_run_scopes_restore_after_nested_exception_and_thread_work(tmp_path: Path) -> None:
    """Nested scopes restore their parent and worker submissions copy the run."""
    from portolan_cli.trace import submit

    outer, inner = RunState.open(tmp_path), RunState.open(tmp_path)
    assert current_run() is None
    with in_run(outer):
        with pytest.raises(RuntimeError, match="scope failure"), in_run(inner):
            assert current_run() is inner
            raise RuntimeError("scope failure")
        with run_scope(None):
            assert current_run() is outer
        with ThreadPoolExecutor(max_workers=1) as pool:
            assert pool.submit(current_run).result() is None
            assert submit(pool, current_run).result() is outer
            assert pool.submit(current_run).result() is None
    assert current_run() is None


def test_provenance_checksum_is_scoped_to_each_run(tmp_path: Path) -> None:
    """A second Python API run must hash the new bytes at the same path."""
    import hashlib
    import json

    from portolan_cli.convert import provenance_record

    source = tmp_path / "source.geojson"
    for content in (b"first", b"other"):
        source.write_bytes(content)
        with in_run(RunState.open(tmp_path)):
            assert (
                json.loads(provenance_record(source))["sha256"]
                == hashlib.sha256(content).hexdigest()
            )
    assert current_run() is None


def test_pmtiles_style_reuses_the_discovery_footer(tmp_path: Path) -> None:
    """The PMTiles side step must keep its run scope through style creation."""
    import json

    from portolan_cli.trace import counters, tracing
    from portolan_cli.viz.pmtiles import generate_or_suggest_pmtiles

    collection = tmp_path / "places"
    collection.mkdir()
    (collection / "points.parquet").write_bytes(POINTS.read_bytes())
    tiles = POINTS.parents[2] / "cloud_native/sample.pmtiles"
    (collection / "points.pmtiles").write_bytes(tiles.read_bytes())
    (collection / "collection.json").write_text(
        json.dumps(
            {
                "type": "Collection",
                "id": "places",
                "links": [],
                "assets": {"points": {"href": "./points.parquet", "roles": ["data"]}},
            }
        )
    )
    state = RunState.open(tmp_path)
    with tracing():
        generate_or_suggest_pmtiles(
            tmp_path,
            {"places"},
            generate_pmtiles=False,
            force=False,
            verbose=False,
            state=state,
        )
        opens = counters()["parquet.opens"]
    assert (collection / "styles/default.json").exists()
    assert opens == 1


def test_registered_nested_extensions_keep_raw_declarations(tmp_path: Path) -> None:
    """Registration must not consume publisher declarations at any field depth."""
    from portolan_cli.metadata.geoparquet import read_rewrite_fidelity

    class PublisherType(pa.ExtensionType):
        def __init__(self) -> None:
            super().__init__(pa.binary(), "publisher.fidelity")

        def __arrow_ext_serialize__(self) -> bytes:
            return b"{}"

        @classmethod
        def __arrow_ext_deserialize__(
            cls, _storage_type: pa.DataType, _serialized: bytes
        ) -> PublisherType:
            return cls()

    metadata = {
        b"ARROW:extension:name": b"publisher.fidelity",
        b"ARROW:extension:metadata": b'{"publisher":"survey"}',
        b"quoted": b"ARROW:extension:name",
        b"collision": b"____________________",
    }
    field = pa.field("ARROW:extension:name", pa.binary(), metadata=metadata)
    table = pq.read_table(POINTS)
    table = table.append_column(field, pa.array([b"value"] * len(table)))
    nested = pa.array(
        [{"value": b"value"}] * len(table), type=pa.struct([field.with_name("value")])
    )
    source = tmp_path / "points.parquet"
    pq.write_table(table.append_column("nested", nested), source)
    extension = PublisherType()
    pa.register_extension_type(extension)
    try:
        fidelity = read_rewrite_fidelity(source)
        assert fidelity is not None
        assert fidelity.field_metadata is not None
        assert fidelity.field_metadata["ARROW:extension:name"] == metadata
        assert fidelity.field_metadata[("nested", "0")] == metadata
        # The fidelity reader must leave registered types usable by other readers.
        assert pq.read_schema(source).field("ARROW:extension:name").type == extension
    finally:
        pa.unregister_extension_type("publisher.fidelity")


def test_registered_projjson_declaration_allows_a_lossless_rewrite(tmp_path: Path) -> None:
    """GeoArrow's CRS representation tag does not add semantics beyond ``geo``."""
    import geoarrow.pyarrow  # noqa: F401 - force extension-aware Arrow serialization
    import geopandas as gpd
    from shapely.geometry import Point

    source = tmp_path / "points.parquet"
    frame = gpd.GeoDataFrame(
        {"a": [1, 2, 3]}, geometry=[Point(0, 0), Point(1, 1), Point(2, 2)], crs=3857
    )
    frame.to_parquet(source)
    # Reserializing a registered type adds crs_type=projjson to its declaration.
    pq.write_table(pq.read_table(source), source)

    convert_vector(source, tmp_path)

    assert "bbox" in pq.read_schema(source).names
