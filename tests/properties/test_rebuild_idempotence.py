"""A rebuild after a data change equals a fresh build of the changed data (#801, #931, #813).

Build A runs ``init`` and ``add`` on the original file, changes the file in
place, and runs ``add --force``. Build B runs ``init`` and ``add`` on the
changed file alone. Every field that ``add`` derives from the data must then
agree: the extent, the bbox, ``table:columns``, ``table:row_count``, the
summaries, the data assets, the item properties, and the declared
extensions. Version history differs by design, so only the current snapshot
counts.

A rebuild that patches derived state in place, and does not compute it again
from the data, fails here. The extent that never shrinks (#801) is that
shape.
"""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests.catalog_invariants import RunCli

pytestmark = [pytest.mark.integration]

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
POINTS = FIXTURES / "vector" / "valid" / "points.parquet"
# The attribute columns of points.parquet that a mutation may drop.
COLUMNS = ("name", "category", "rating", "open", "visitors")
START = datetime(2024, 1, 1, tzinfo=UTC)


def _bbox_of(table: pa.Table) -> list[float]:
    """Return the union of a table's ``bbox`` covering column."""
    boxes = table.column("bbox").to_pylist()
    return [
        min(b["xmin"] for b in boxes),
        min(b["ymin"] for b in boxes),
        max(b["xmax"] for b in boxes),
        max(b["ymax"] for b in boxes),
    ]


def write_geoparquet(table: pa.Table, metadata: dict[bytes, bytes], path: Path) -> None:
    """Write ``table`` with ``metadata``, its ``geo`` bbox set from the data."""
    metadata = dict(metadata)
    geo = json.loads(metadata[b"geo"])
    geo["columns"][geo["primary_column"]]["bbox"] = _bbox_of(table)
    metadata[b"geo"] = json.dumps(geo).encode()
    pq.write_table(table.replace_schema_metadata(metadata), path)


def shrink_geoparquet(path: Path, *, drop: str | None = None) -> None:
    """Keep the south-west quadrant of the rows, and drop one column if named.

    The file must carry a ``bbox`` covering column, as every file that
    ``add`` rewrites does. The ``geo`` metadata gets the new bbox, so the
    file tells the truth about its smaller extent.
    """
    table = pq.read_table(path)
    xmin, ymin, xmax, ymax = _bbox_of(table)
    xmid, ymid = (xmin + xmax) / 2, (ymin + ymax) / 2
    keep = [b["xmax"] <= xmid and b["ymax"] <= ymid for b in table.column("bbox").to_pylist()]
    smaller = table.filter(pa.array(keep))
    if drop is not None:
        smaller = smaller.drop_columns([drop])
    write_geoparquet(smaller, dict(table.schema.metadata or {}), path)


def drop_columns(path: Path, names: set[str]) -> None:
    table = pq.read_table(path)
    write_geoparquet(table.drop_columns(sorted(names)), dict(table.schema.metadata or {}), path)


def append_rows(path: Path) -> None:
    """Append a copy of the rows, moved one degree north-east."""
    table = pq.read_table(path)
    moved = table.to_pylist()
    for row in moved:
        box = row["bbox"]
        row["bbox"] = {k: v + 1.0 for k, v in box.items()}
        row["id"] = row["id"] + 1000
        # The WKB point: byte order, type, then x and y as doubles.
        x, y = _point(row["geometry"])
        row["geometry"] = _wkb_point(x + 1.0, y + 1.0)
    grown = pa.concat_tables([table, pa.Table.from_pylist(moved, schema=table.schema)])
    write_geoparquet(grown, dict(table.schema.metadata or {}), path)


def _point(wkb: bytes) -> tuple[float, float]:
    import struct

    order = "<" if wkb[0] == 1 else ">"
    x, y = struct.unpack(f"{order}dd", wkb[5:21])
    return x, y


def _wkb_point(x: float, y: float) -> bytes:
    import struct

    return struct.pack("<BIdd", 1, 1, x, y)


def change_timestamp_range(path: Path) -> None:
    """Move every ``observed_at`` value one year later."""
    table = pq.read_table(path)
    later = [value + timedelta(days=366) for value in table.column("observed_at").to_pylist()]
    index = table.schema.get_field_index("observed_at")
    changed = table.set_column(index, "observed_at", pa.array(later, pa.timestamp("us", tz="UTC")))
    write_geoparquet(changed, dict(table.schema.metadata or {}), path)


MUTATIONS: dict[str, Callable[[Path], None]] = {
    "drop_column": lambda path: drop_columns(path, {"visitors"}),
    "shrink_extent": shrink_geoparquet,
    "append_rows": append_rows,
    "change_timestamp_range": change_timestamp_range,
}


def _source(path: Path) -> Path:
    """Write points.parquet with an ``observed_at`` timestamp column."""
    table = pq.read_table(POINTS)
    stamps = [START + timedelta(days=30 * i) for i in range(table.num_rows)]
    table = table.append_column("observed_at", pa.array(stamps, pa.timestamp("us", tz="UTC")))
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table.replace_schema_metadata(POINTS_METADATA), path)
    return path


POINTS_METADATA = pq.read_schema(POINTS).metadata


def _init_and_add(root: Path, data: Path, run_cli: RunCli) -> None:
    run_cli("init", str(root), "--auto", "--license", "CC-BY-4.0")
    run_cli("add", "--portolan-dir", str(root), str(data))


# Fields that record when a build ran, not what the data holds.
CLOCK_FIELDS = frozenset({"datetime", "created", "updated"})


def normalize(root: Path) -> dict[str, Any]:
    """Return every data-derived field of the collection at ``root/roads``."""
    collection = json.loads((root / "roads" / "collection.json").read_text(encoding="utf-8"))
    assets = {
        key: {
            field: value
            for field, value in asset.items()
            if field in {"href", "type", "roles"} or field.startswith("file:")
        }
        for key, asset in collection.get("assets", {}).items()
        # A thumbnail is a drawing of the extent, not data, and its bytes vary.
        if "thumbnail" not in asset.get("roles", [])
    }
    items = {
        path.name: {
            key: value
            for key, value in json.loads(path.read_text(encoding="utf-8"))["properties"].items()
            if key not in CLOCK_FIELDS
        }
        for path in sorted((root / "roads").rglob("*.json"))
        if path.name not in {"collection.json", "versions.json"}
    }
    return {
        "extent": collection["extent"],
        "table:columns": collection.get("table:columns"),
        "table:row_count": collection.get("table:row_count"),
        "summaries": collection.get("summaries"),
        "assets": assets,
        "items": items,
        "stac_extensions": sorted(collection.get("stac_extensions", [])),
    }


def _differences(rebuilt: dict[str, Any], fresh: dict[str, Any]) -> dict[str, Any]:
    """Return each field that differs, as {field: (rebuilt, fresh)}."""
    return {key: (rebuilt[key], fresh[key]) for key in rebuilt if rebuilt[key] != fresh[key]}


def _rebuild_and_fresh(
    tmp_path: Path, run_cli: RunCli, mutate: Callable[[Path], None]
) -> tuple[dict[str, Any], dict[str, Any]]:
    a = tmp_path / "rebuilt"
    data_a = _source(a / "roads" / "roads.parquet")
    _init_and_add(a, data_a, run_cli)
    mutate(data_a)
    run_cli("add", "--force", "--portolan-dir", str(a), str(data_a))

    b = tmp_path / "fresh"
    data_b = b / "roads" / "roads.parquet"
    data_b.parent.mkdir(parents=True)
    shutil.copy(data_a, data_b)
    _init_and_add(b, data_b, run_cli)
    return normalize(a), normalize(b)


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param("append_rows"),
        pytest.param("change_timestamp_range"),
        pytest.param("drop_column"),
        pytest.param("shrink_extent"),
    ],
)
def test_a_rebuild_equals_a_fresh_build(mutation: str, tmp_path: Path, run_cli: RunCli) -> None:
    rebuilt, fresh = _rebuild_and_fresh(tmp_path, run_cli, MUTATIONS[mutation])

    assert _differences(rebuilt, fresh) == {}


@settings(
    max_examples=5,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.differing_executors],
)
@given(dropped=st.sets(st.sampled_from(COLUMNS), min_size=1))
def test_dropping_any_columns_rebuilds_like_a_fresh_build(
    dropped: set[str], tmp_path_factory: pytest.TempPathFactory, run_cli: RunCli
) -> None:
    tmp_path = tmp_path_factory.mktemp("drop")
    rebuilt, fresh = _rebuild_and_fresh(tmp_path, run_cli, lambda path: drop_columns(path, dropped))

    assert _differences(rebuilt, fresh) == {}
