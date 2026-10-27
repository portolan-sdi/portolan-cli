"""Seeded scenario inputs for the perf gates, the nightly job, and the profiler (#992).

Each scenario builds its input under an empty directory from a size ``n`` and
a seed. The same size and seed give the same bytes, and
``test_determinism.py`` checks this with checksums. Vector files come from
``tests/fixtures/realdata/open-buildings.parquet`` (1,000 polygons in one row
group), copied and translated by a seeded offset. Raster files are small
seeded GeoTIFFs.

A scenario that needs a catalog first (``re-add-unchanged``, ``check-metadata``,
``status``) builds it with ``portolan`` itself, through ``runner.py``, so the
catalog is what the code under test writes today.

Every scenario turns vector thumbnails off, except ``add-many-collections-thumbs``
and ``thumbnail-large``. Those two draw the default basemap from the stub tile
source in ``runner.py``. A raster scenario still writes the COG thumbnail,
because ``conversion.cog.generate_thumbnail`` controls it and is on by
default. PMTiles stay off, the default, so tippecanoe on the machine does not
change a count.

The module docstring of ``test_scale_invariance.py`` explains how the gates
use the sizes.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_VECTOR = REPO_ROOT / "tests" / "fixtures" / "realdata" / "open-buildings.parquet"
SEED = 992
SMALL_FILE_ROWS = 100
LICENSE = "CC-BY-4.0"
THUMBNAILS_OFF = "thumbnails:\n  enabled: false\n"
RASTER_SIZE = 64
#: The catalog metadata.yaml. It names a contact, providers, and a source, so
#: ``check`` reports no finding that the scenario itself causes.
METADATA_YAML = """\
contact:
  name: "Perf harness"
  email: "perf@example.org"
license: "CC-BY-4.0"
source_url: "https://source.coop/vida/google-microsoft-osm-open-buildings"
providers:
  - name: "Perf harness"
    roles: [producer, host]
    url: "https://example.org"
"""

# ---------------------------------------------------------------------------
# Input writers
# ---------------------------------------------------------------------------


def seeded_rng(seed: int, salt: int) -> Any:
    import numpy as np

    return np.random.default_rng([seed, salt])


def write_vector(path: Path, rows: int, rng: Any) -> None:
    """Write ``rows`` open-buildings polygons to ``path``, moved by one seeded offset.

    Above 1,000 rows the source repeats on a 0.01 degree grid, so the copies
    sit side by side.
    """
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    import shapely

    source = pq.read_table(SOURCE_VECTOR)
    geo = json.loads(source.schema.metadata[b"geo"])
    dx, dy = rng.uniform(-60.0, 60.0), rng.uniform(-30.0, 30.0)
    repeats = -(-rows // source.num_rows)
    table = pa.concat_tables([source] * repeats).slice(0, rows)
    copy = np.repeat(np.arange(repeats), source.num_rows)[:rows]
    shift_x = dx + (copy % 100) * 0.01
    shift_y = dy + (copy // 100) * 0.01

    geoms = shapely.from_wkb(table.column("geometry").to_numpy(zero_copy_only=False))
    coords_index = shapely.get_coordinates(geoms, return_index=True)[1]
    moved = shapely.transform(
        geoms,
        lambda xy: xy + np.column_stack((shift_x[coords_index], shift_y[coords_index])),
    )
    bounds = shapely.bounds(moved)
    bbox = pa.StructArray.from_arrays(
        [pa.array(bounds[:, i]) for i in range(4)], names=["xmin", "ymin", "xmax", "ymax"]
    )
    table = table.set_column(
        table.schema.get_field_index("geometry"),
        "geometry",
        pa.array(shapely.to_wkb(moved), type=pa.binary()),
    )
    table = table.set_column(table.schema.get_field_index("bbox"), "bbox", bbox)
    geo["columns"]["geometry"]["bbox"] = [
        float(bounds[:, 0].min()),
        float(bounds[:, 1].min()),
        float(bounds[:, 2].max()),
        float(bounds[:, 3].max()),
    ]
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def write_csv(path: Path, rows: int, rng: Any) -> None:
    """Write a small table with no geometry, so ``add`` defers it as a companion."""
    values = rng.integers(0, 10_000, size=rows)
    lines = ["id,name,value", *(f"{i},row-{i},{v}" for i, v in enumerate(values))]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_geojson(path: Path, rows: int, rng: Any) -> None:
    """Write ``rows`` seeded points as GeoJSON, a format that ``check --fix`` converts."""
    xs, ys = rng.uniform(-170.0, 170.0, size=rows), rng.uniform(-80.0, 80.0, size=rows)
    features = [
        {
            "type": "Feature",
            "properties": {"id": i},
            "geometry": {"type": "Point", "coordinates": [float(x), float(y)]},
        }
        for i, (x, y) in enumerate(zip(xs, ys, strict=True))
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features}), "utf-8")


def write_raster(path: Path, rng: Any) -> None:
    """Write one small single-band GeoTIFF at a seeded place."""
    import rasterio
    from rasterio.transform import from_origin

    west, north = rng.uniform(-170.0, 160.0), rng.uniform(-50.0, 70.0)
    data = rng.integers(0, 255, size=(1, RASTER_SIZE, RASTER_SIZE), dtype="uint8")
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=RASTER_SIZE,
        height=RASTER_SIZE,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(west, north, 0.001, 0.001),
    ) as dst:
        dst.write(data)


# ---------------------------------------------------------------------------
# Catalog setup through the runner
# ---------------------------------------------------------------------------


def run_portolan(
    cwd: Path, args: list[str], out: Path, *, mem: bool = False, env: dict[str, str] | None = None
) -> dict[str, Any]:
    """Run ``portolan <args>`` in a cold child process and return the runner record."""
    command = [sys.executable, "-m", "tests.perf.runner", "--out", str(out)]
    if mem:
        command.append("--mem")
    child_env = dict(os.environ if env is None else env)
    # The child imports tests.perf.runner from this checkout, wherever it runs.
    child_env["PYTHONPATH"] = os.pathsep.join(
        filter(None, (str(REPO_ROOT), child_env.get("PYTHONPATH")))
    )
    proc = subprocess.run(
        [*command, "--", *args],
        cwd=cwd,
        env=child_env,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0 or not out.exists():
        raise RuntimeError(
            f"runner failed for {args}: exit {proc.returncode}\n{proc.stdout}\n{proc.stderr}"
        )
    record: dict[str, Any] = json.loads(out.read_text(encoding="utf-8"))
    record["stdout"] = proc.stdout
    return record


def init_catalog(root: Path, config: str = THUMBNAILS_OFF) -> None:
    """Create a catalog at ``root``, append ``config`` to its config.yaml, and fill metadata.yaml."""
    root.mkdir(parents=True, exist_ok=True)
    record = run_portolan(
        root, ["init", "--auto", "--license", LICENSE], root.parent / f".{root.name}-init.json"
    )
    _require_success(record)
    with (root / ".portolan" / "config.yaml").open("a", encoding="utf-8") as handle:
        handle.write(config)
    (root / ".portolan" / "metadata.yaml").write_text(METADATA_YAML, encoding="utf-8")


def _require_success(record: dict[str, Any]) -> None:
    if record["exit_code"] != 0 or record["network"]:
        raise RuntimeError(
            f"setup step {record['argv']} failed: exit {record['exit_code']}, "
            f"network {record['network']}\n{record['stdout'][-2000:]}"
        )


def setup_add(root: Path) -> None:
    """Add every file under ``root``, as the setup of a scenario that needs a catalog."""
    _require_success(
        run_portolan(root, ["add", ".", "--workers", "4"], root.parent / f".{root.name}-setup.json")
    )


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


#: History entries that ``re-add-unchanged`` puts in front of the real version.
#: The nightly job passes 50.
HISTORY_VERSIONS = 10


def pad_versions(root: Path, count: int) -> None:
    """Give every collection ``count`` earlier versions with the same asset snapshot.

    ``versions.json`` stores a full snapshot per version, so the history grows
    as versions times assets, as on a catalog that saw many adds.
    """
    for path in sorted(root.glob("*/versions.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        first = data["versions"][0]
        history = [
            {**first, "version": f"0.0.{i}", "changes": [], "message": "history"}
            for i in range(1, count + 1)
        ]
        data["versions"] = [*history, *data["versions"]]
        path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def _setup_add(root: Path, history: int) -> None:
    setup_add(root)


def _setup_re_add(root: Path, history: int) -> None:
    setup_add(root)
    pad_versions(root, history)


@dataclass(frozen=True)
class Scenario:
    """One fixed input and the ``portolan`` command that the gates measure.

    ``inputs(root, n, seed)`` writes the data files at size ``n``.
    ``setup(root, history)`` turns them into a catalog when the command needs
    one. ``exit_code`` is the exit status the command has today.
    """

    name: str
    inputs: Callable[[Path, int, int], None]
    command: tuple[str, ...]
    setup: Callable[[Path, int], None] | None = None
    config: str = THUMBNAILS_OFF
    exit_code: int = 0

    def build(self, root: Path, n: int, seed: int = SEED, history: int = HISTORY_VERSIONS) -> None:
        """Write the inputs at size ``n`` under ``root`` and prepare the catalog."""
        init_catalog(root, self.config)
        self.inputs(root, n, seed)
        if self.setup is not None:
            self.setup(root, history)

    @property
    def runs_phase_two(self) -> bool:
        """Whether the command is an ``add``, whose phase 2 takes ``--workers``."""
        return self.command[0] == "add"


def _many_collections(root: Path, n: int, seed: int) -> None:
    for i in range(n):
        write_vector(root / f"c{i:04d}" / "buildings.parquet", SMALL_FILE_ROWS, seeded_rng(seed, i))


def _one_collection(root: Path, n: int, seed: int) -> None:
    for i in range(n):
        write_vector(root / "buildings" / f"b{i:04d}.parquet", SMALL_FILE_ROWS, seeded_rng(seed, i))


def _one_collection_with_csv(root: Path, n: int, seed: int) -> None:
    _one_collection(root, n, seed)
    for i in range(n):
        # A distinct stem: a same-stem CSV overwrites the GeoParquet on add.
        write_csv(root / "buildings" / f"b{i:04d}-attrs.csv", 20, seeded_rng(seed, 100_000 + i))


def _large_vector(root: Path, n: int, seed: int) -> None:
    write_vector(root / "buildings" / "buildings.parquet", n, seeded_rng(seed, 0))


def _geojson_collections(root: Path, n: int, seed: int) -> None:
    for i in range(n):
        write_geojson(root / f"c{i:04d}" / "points.geojson", 20, seeded_rng(seed, i))


def _rasters(root: Path, n: int, seed: int) -> None:
    for i in range(n):
        write_raster(root / "scenes" / f"s{i:04d}" / f"s{i:04d}.tif", seeded_rng(seed, i))


ADD = ("add", ".", "--workers", "1")

SCENARIOS: dict[str, Scenario] = {
    s.name: s
    for s in (
        # S1: k collections with one small GeoParquet file each.
        Scenario("add-many-collections", _many_collections, ADD),
        # S1 with thumbnails and the default basemap, from the stub tile source.
        Scenario("add-many-collections-thumbs", _many_collections, ADD, config=""),
        # S2: N small GeoParquet files in one collection.
        Scenario("add-one-collection", _one_collection, ADD),
        # S2b: N GeoParquet files and N CSV companions in one collection.
        Scenario("add-with-companions", _one_collection_with_csv, ADD),
        # S2 large: one GeoParquet file with n polygons.
        Scenario("add-large-vector", _large_vector, ADD),
        # S3: n raster items with history, then an add that changes nothing.
        Scenario("re-add-unchanged", _rasters, ADD, setup=_setup_re_add),
        # S4: check --metadata on an unchanged catalog of n raster items, with
        # thumbnails on, as PTL-VIZ-001 asks. add embeds the band statistics
        # in each COG (#748), so check exits 0.
        Scenario(
            "check-metadata",
            _rasters,
            ("check", "--metadata"),
            setup=_setup_add,
            config="",
        ),
        # check --fix converts n GeoJSON files to GeoParquet in one process.
        Scenario(
            "check-fix-geo-assets",
            _geojson_collections,
            ("check", "--geo-assets", "--fix", "--workers", "1"),
        ),
        # S6: status on an unchanged catalog.
        Scenario("status", _one_collection, ("status",), setup=_setup_add),
        # S8: n small rasters, one item each (#775).
        Scenario("add-rasters", _rasters, ADD),
        # One file of n polygons with a thumbnail and the stub basemap.
        Scenario("thumbnail-large", _large_vector, ADD, config=""),
    )
}
