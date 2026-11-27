"""The base catalogs and one breaker per AUTO rule id.

Each base is built once per session through the real CLI, the way a user
builds a catalog. Each breaker edits a copy of a base so that its rule fires.
It draws from Hypothesis to choose which node, asset, or value to break, so
the property holds for every choice and not for one hand-picked file.
"""

from __future__ import annotations

import hashlib
import json
import random
import re
import struct
from typing import TYPE_CHECKING, Any, TypeVar

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
from hypothesis import strategies as st
from rasterio.transform import from_bounds

from tests.integration import test_generated_catalog_conformance as gen
from tests.property.registry import breaks

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from pathlib import Path

    from hypothesis.strategies import DataObject

    from tests.catalog_invariants import RunCli

T = TypeVar("T")

PORTOLAN_URI = re.compile(
    r"^https://schemas\.portolan-sdi\.org/portolan/v\d+\.\d+\.\d+/schema\.json$"
)
STRUCTURAL = ("root", "parent", "child", "item", "collection")
PARTITION_FIELDS = ("partition:scheme", "partition:keys", "partition:glob")


# ---------------------------------------------------------------------------
# Bases
# ---------------------------------------------------------------------------


def build_core(root: Path, run: RunCli) -> None:
    """A vector collection with a PMTiles sibling, and a raster collection of two scenes."""
    roads = gen._copy(gen.POINTS, root / "roads" / "roads.parquet")
    gen._copy(gen.SAMPLE_PMTILES, root / "roads" / "roads.pmtiles")
    scene_a = gen._copy(gen.RGB_COG, root / "imagery" / "scene-a" / "rgb.tif")
    scene_b = gen._copy(gen.SINGLEBAND_COG, root / "imagery" / "scene-b" / "singleband.tif")
    gen._init(root, run)
    gen._add(root, run, roads, scene_a, scene_b)


def build_partitioned(root: Path, run: RunCli) -> None:
    """A Hive-partitioned collection, after one ``check --fix``.

    ``add`` leaves partition:glob for ``push`` (#914), so PTL-PRT-001 fires
    on a fresh build. One ``--fix`` writes the glob from the disk layout.
    """
    gen.build_partitioned(root, run)
    run("check", str(root), "--fix", expect=None)


BASES: dict[str, Callable[[Path, RunCli], None]] = {
    "core": build_core,
    "partitioned": build_partitioned,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load(path: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return loaded


def _edit(path: Path, mutate: Callable[[dict[str, Any]], None]) -> None:
    data = _load(path)
    mutate(data)
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def _stac(root: Path, *types: str) -> list[Path]:
    """Every STAC file under ``root`` whose ``type`` is one of ``types``."""
    return [
        path
        for path in sorted(root.rglob("*.json"))
        if ".portolan" not in path.relative_to(root).parts
        and path.name != "versions.json"
        and path.parent.name != "styles"
        and _load(path).get("type") in types
    ]


def _containers(root: Path) -> list[Path]:
    return _stac(root, "Catalog", "Collection")


def _choose(data: DataObject, options: Iterable[T]) -> T:
    return data.draw(st.sampled_from(list(options)))


def _drop_links(rel: str) -> Callable[[dict[str, Any]], None]:
    def mutate(node: dict[str, Any]) -> None:
        node["links"] = [link for link in node["links"] if link.get("rel") != rel]

    return mutate


def _local_assets(root: Path) -> list[tuple[Path, str]]:
    """(STAC file, asset key) for every asset whose href names a file in the tree."""
    return [
        (path, key)
        for path in _stac(root, "Catalog", "Collection", "Feature")
        for key, asset in _load(path).get("assets", {}).items()
        if "file:checksum" in asset and (path.parent / asset["href"]).is_file()
    ]


def _set_asset(path: Path, key: str, field: str, value: Any) -> None:
    def mutate(node: dict[str, Any]) -> None:
        node["assets"][key][field] = value

    _edit(path, mutate)


def _scene(root: Path, data: DataObject) -> Path:
    """The data file of one drawn raster scene."""
    item = _choose(data, _stac(root / "imagery", "Feature"))
    href: str = _load(item)["assets"]["data"]["href"]
    return item.parent / href


def _rewrite_raster(path: Path, **profile: Any) -> None:
    """Rewrite ``path`` over the same bounds with ``profile`` overriding the source's."""
    with rasterio.open(path) as src:
        bounds, crs, count, dtype = src.bounds, src.crs, src.count, src.dtypes[0]
    width = profile.pop("width", 64)
    height = profile.pop("height", 64)
    out = {
        "driver": "GTiff",
        "width": width,
        "height": height,
        "count": count,
        "dtype": dtype,
        "crs": crs,
        "transform": from_bounds(*bounds, width, height),
        "compress": "deflate",
        **profile,
    }
    path.unlink()
    with rasterio.open(path, "w", **out) as dst:
        pixels = (np.add.outer(np.arange(height), np.arange(width)) % 200).astype(dtype)
        for band in range(1, count + 1):
            dst.write(pixels, band)


def _roads_parquet(root: Path) -> Path:
    return root / "roads" / "roads.parquet"


def _rewrite_points(path: Path, coords: list[tuple[float, float]], **write_options: Any) -> None:
    """Rewrite the GeoParquet at ``path`` with one point row per coordinate pair.

    The attribute columns repeat the original rows. The geometry, the
    ``bbox`` covering column, and the ``geo`` bbox follow ``coords``.
    """
    table = pq.read_table(path)
    xs = [x for x, _ in coords]
    ys = [y for _, y in coords]
    rows = table.take([i % table.num_rows for i in range(len(coords))])
    geometry = pa.array([struct.pack("<BIdd", 1, 1, x, y) for x, y in coords], pa.binary())
    bbox = pa.StructArray.from_arrays(
        [pa.array(xs), pa.array(ys), pa.array(xs), pa.array(ys)],
        names=["xmin", "ymin", "xmax", "ymax"],
    )
    rows = rows.set_column(rows.schema.get_field_index("geometry"), "geometry", geometry)
    rows = rows.set_column(rows.schema.get_field_index("bbox"), "bbox", bbox)
    metadata = dict(table.schema.metadata or {})
    geo = json.loads(metadata[b"geo"])
    geo["columns"]["geometry"]["bbox"] = [min(xs), min(ys), max(xs), max(ys)]
    metadata[b"geo"] = json.dumps(geo).encode()
    pq.write_table(rows.replace_schema_metadata(metadata), path, **write_options)


def _original_coords(path: Path) -> list[tuple[float, float]]:
    boxes = pq.read_table(path, columns=["bbox"]).column("bbox").to_pylist()
    return [(b["xmin"], b["ymin"]) for b in boxes]


# ---------------------------------------------------------------------------
# files
# ---------------------------------------------------------------------------


@breaks("PTL-FIL-001")
def delete_required_file(root: Path, data: DataObject) -> None:
    """Delete README.md or AGENTS.md from one catalog or collection directory."""
    directory = _choose(data, _containers(root)).parent
    (directory / _choose(data, ("README.md", "AGENTS.md"))).unlink()


@breaks("PTL-FIL-002")
def drop_agents_link(root: Path, data: DataObject) -> None:
    """Remove the rel='agents' link from one catalog or collection."""
    _edit(_choose(data, _containers(root)), _drop_links("agents"))


@breaks("PTL-FIL-003")
def drop_readme_link(root: Path, data: DataObject) -> None:
    """Remove the rel='describedby' link from one catalog or collection."""
    _edit(_choose(data, _containers(root)), _drop_links("describedby"))


# ---------------------------------------------------------------------------
# titles
# ---------------------------------------------------------------------------


@breaks("PTL-TTL-001")
def drop_title_or_description(root: Path, data: DataObject) -> None:
    """Remove the title or the description of one catalog or collection."""
    field = _choose(data, ("title", "description"))
    _edit(_choose(data, _containers(root)), lambda node: node.pop(field))


@breaks("PTL-TTL-003")
def drop_link_title(root: Path, data: DataObject) -> None:
    """Remove the title of one child or item link."""
    titled = [
        (path, index)
        for path in _containers(root)
        for index, link in enumerate(_load(path)["links"])
        if link.get("rel") in ("child", "item") and "title" in link
    ]
    path, index = _choose(data, titled)
    _edit(path, lambda node: node["links"][index].pop("title"))


# ---------------------------------------------------------------------------
# links
# ---------------------------------------------------------------------------


def _required_rels(root: Path, path: Path) -> tuple[str, ...]:
    kind = _load(path)["type"]
    if kind == "Feature":
        return ("root", "parent", "collection")
    return ("root",) if path == root / "catalog.json" else ("root", "parent")


@breaks("PTL-LNK-001")
def drop_structural_link(root: Path, data: DataObject) -> None:
    """Remove one required root, parent, or collection link."""
    path = _choose(data, _stac(root, "Catalog", "Collection", "Feature"))
    _edit(path, _drop_links(_choose(data, _required_rels(root, path))))


@breaks("PTL-LNK-002")
def drop_containment_link(root: Path, data: DataObject) -> None:
    """Remove one child or item link, so a contained object is orphaned."""
    links = [
        (path, index)
        for path in _containers(root)
        for index, link in enumerate(_load(path)["links"])
        if link.get("rel") in ("child", "item")
    ]
    path, index = _choose(data, links)
    _edit(path, lambda node: node["links"].pop(index))


@breaks("PTL-LNK-003")
def mistype_structural_link(root: Path, data: DataObject) -> None:
    """Give one structural link a media type that is not JSON."""
    links = [
        (path, index)
        for path in _stac(root, "Catalog", "Collection", "Feature")
        for index, link in enumerate(_load(path)["links"])
        if link.get("rel") in STRUCTURAL
    ]
    path, index = _choose(data, links)
    wrong = _choose(data, ("text/html", "application/xml", "text/plain"))
    _edit(path, lambda node: node["links"][index].__setitem__("type", wrong))


@breaks("PTL-LNK-006")
def repoint_structural_link(root: Path, data: DataObject) -> None:
    """Point one item's root, parent, or collection link at the wrong object."""
    item = _choose(data, _stac(root, "Feature"))
    rel = _choose(data, ("root", "parent", "collection"))
    wrong = "../collection.json" if rel == "root" else "../../catalog.json"

    def mutate(node: dict[str, Any]) -> None:
        for link in node["links"]:
            if link["rel"] == rel:
                link["href"] = wrong

    _edit(item, mutate)


# ---------------------------------------------------------------------------
# bbox
# ---------------------------------------------------------------------------


@breaks("PTL-BBX-001")
def corrupt_bbox(root: Path, data: DataObject) -> None:
    """Swap south and north, or push a longitude out of range, on one bbox."""
    path = _choose(data, _stac(root, "Collection", "Feature"))
    swap = data.draw(st.booleans())
    longitude = data.draw(st.floats(min_value=180.5, max_value=1000.0))

    def mutate(node: dict[str, Any]) -> None:
        box = node["bbox"] if node["type"] == "Feature" else node["extent"]["spatial"]["bbox"][0]
        if swap:
            box[1], box[3] = box[3], box[1]
        else:
            box[2] = longitude

    _edit(path, mutate)


# ---------------------------------------------------------------------------
# assets
# ---------------------------------------------------------------------------


@breaks("PTL-AST-001")
def drop_asset_type_or_roles(root: Path, data: DataObject) -> None:
    """Remove the media type or the roles of one local asset."""
    path, key = _choose(data, _local_assets(root))
    field = _choose(data, ("type", "roles"))
    _edit(path, lambda node: node["assets"][key].pop(field))


@breaks("PTL-AST-003")
def drop_file_fields(root: Path, data: DataObject) -> None:
    """Remove file:size or file:checksum from one local asset."""
    path, key = _choose(data, _local_assets(root))
    field = _choose(data, ("file:size", "file:checksum"))
    _edit(path, lambda node: node["assets"][key].pop(field))


@breaks("PTL-AST-004")
def bare_checksum(root: Path, data: DataObject) -> None:
    """Strip the multihash prefix from one file:checksum, leaving a bare digest."""
    path, key = _choose(data, _local_assets(root))
    checksum = _load(path)["assets"][key]["file:checksum"]
    _set_asset(path, key, "file:checksum", checksum.removeprefix("1220"))


# ---------------------------------------------------------------------------
# conformance
# ---------------------------------------------------------------------------


@breaks("PTL-CNF-001")
def drop_schema_uri(root: Path, data: DataObject) -> None:
    """Remove the Portolan schema URI from one catalog or collection."""

    def mutate(node: dict[str, Any]) -> None:
        node["stac_extensions"] = [u for u in node["stac_extensions"] if not PORTOLAN_URI.match(u)]

    _edit(_choose(data, _containers(root)), mutate)


@breaks("PTL-CNF-002")
def misalign_schema_uri(root: Path, data: DataObject) -> None:
    """Declare a Portolan schema version on one collection that the root does not."""
    version = _choose(data, ("v0.1.0", "v0.2.0"))

    def mutate(node: dict[str, Any]) -> None:
        node["stac_extensions"] = [
            re.sub(r"/v\d+\.\d+\.\d+/", f"/{version}/", u) if PORTOLAN_URI.match(u) else u
            for u in node["stac_extensions"]
        ]

    _edit(_choose(data, _stac(root, "Collection")), mutate)


# ---------------------------------------------------------------------------
# visualization
# ---------------------------------------------------------------------------


@breaks("PTL-VIZ-001")
def drop_thumbnail(root: Path, data: DataObject) -> None:
    """Remove one collection's thumbnail asset, and the image file when drawn."""
    path = _choose(data, _stac(root, "Collection"))
    href = _load(path)["assets"]["thumbnail"]["href"]
    if data.draw(st.booleans()):
        (path.parent / href).unlink()
    _edit(path, lambda node: node["assets"].pop("thumbnail"))


@breaks("PTL-VIZ-003")
def drop_pmtiles_link(root: Path, data: DataObject) -> None:
    """Remove the rel='pmtiles' link from the collection that has a PMTiles asset."""
    del data
    _edit(root / "roads" / "collection.json", _drop_links("pmtiles"))


@breaks("PTL-VIZ-005")
def mistype_style(root: Path, data: DataObject) -> None:
    """Give the PMTiles style asset a media type that is not a MapLibre style."""
    wrong = _choose(data, ("application/json", "text/plain"))
    _set_asset(root / "roads" / "collection.json", "styles/default", "type", wrong)


# ---------------------------------------------------------------------------
# partitions
# ---------------------------------------------------------------------------


@breaks("PTL-PRT-001", base="partitioned")
def drop_partition_field(root: Path, data: DataObject) -> None:
    """Remove one partition: field, or the partition extension URI."""
    target = _choose(data, (*PARTITION_FIELDS, "stac_extensions"))

    def mutate(node: dict[str, Any]) -> None:
        if target == "stac_extensions":
            node[target] = [u for u in node[target] if "/partition/" not in u]
        else:
            node.pop(target)

    _edit(root / "buildings" / "collection.json", mutate)


# ---------------------------------------------------------------------------
# item mirror
# ---------------------------------------------------------------------------


@breaks("PTL-MIR-001")
def drop_mirror(root: Path, data: DataObject) -> None:
    """Unregister items.parquet, and delete the file when drawn."""
    collection = root / "imagery" / "collection.json"
    if data.draw(st.booleans()):
        (root / "imagery" / "items.parquet").unlink()
    _edit(collection, lambda node: node["assets"].pop("geoparquet-items"))


@breaks("PTL-MIR-002")
def misregister_mirror(root: Path, data: DataObject) -> None:
    """Give the items.parquet asset the wrong role or the wrong media type."""
    field, value = _choose(
        data, (("roles", ["data"]), ("type", "application/x-parquet"), ("roles", []))
    )
    _set_asset(root / "imagery" / "collection.json", "geoparquet-items", field, value)


# ---------------------------------------------------------------------------
# data pass
# ---------------------------------------------------------------------------


@breaks("PTL-DAT-001")
def wrong_checksum(root: Path, data: DataObject) -> None:
    """Record the multihash of other bytes as one asset's file:checksum."""
    path, key = _choose(data, _local_assets(root))
    other = data.draw(st.binary(min_size=1, max_size=32))
    _set_asset(path, key, "file:checksum", "1220" + hashlib.sha256(b"x" + other).hexdigest())


@breaks("PTL-DAT-002")
def wrong_size(root: Path, data: DataObject) -> None:
    """Record a wrong file:size on one asset."""
    path, key = _choose(data, _local_assets(root))
    size = _load(path)["assets"][key]["file:size"]
    _set_asset(path, key, "file:size", size + data.draw(st.integers(min_value=1, max_value=10**6)))


@breaks("PTL-DAT-003")
def mistype_thumbnail(root: Path, data: DataObject) -> None:
    """Declare one JPEG thumbnail as PNG, so its bytes contradict its type."""
    path = _choose(data, _stac(root, "Collection", "Feature"))
    _set_asset(path, "thumbnail", "type", "image/png")


@breaks("PTL-DAT-004")
def strip_cog(root: Path, data: DataObject) -> None:
    """Rewrite one scene as a striped GeoTIFF over 512px, which is not a COG."""
    size = data.draw(st.integers(min_value=513, max_value=700))
    _rewrite_raster(_scene(root, data), width=size, height=size, tiled=False)


@breaks("PTL-DAT-006")
def shuffle_rows(root: Path, data: DataObject) -> None:
    """Rewrite the GeoParquet with enough points in random order to be judged unordered."""
    path = _roads_parquet(root)
    original = _original_coords(path)
    xmin, ymin = min(x for x, _ in original), min(y for _, y in original)
    xmax, ymax = max(x for x, _ in original), max(y for _, y in original)
    rng = random.Random(data.draw(st.integers(min_value=0, max_value=2**32 - 1)))
    count = data.draw(st.integers(min_value=300, max_value=1000))
    coords = [(rng.uniform(xmin, xmax), rng.uniform(ymin, ymax)) for _ in range(count)]
    _rewrite_points(path, original + coords)


@breaks("PTL-DAT-007")
def strip_statistics(root: Path, data: DataObject) -> None:
    """Rewrite the GeoParquet with no column statistics in its row groups."""
    path = _roads_parquet(root)
    groups = data.draw(st.integers(min_value=1, max_value=5))
    coords = _original_coords(path)
    _rewrite_points(path, coords, write_statistics=False, row_group_size=-(-len(coords) // groups))


@breaks("PTL-DAT-008")
def oversize_row_group(root: Path, data: DataObject) -> None:
    """Rewrite the GeoParquet with one row group over the 150,000 row limit."""
    path = _roads_parquet(root)
    original = _original_coords(path)
    extra = data.draw(st.integers(min_value=1, max_value=1000))
    rows = original * (150_000 // len(original)) + original[:extra]
    _rewrite_points(path, rows, row_group_size=len(rows))


@breaks("PTL-DAT-011")
def drop_overviews(root: Path, data: DataObject) -> None:
    """Rewrite one scene as a tiled GeoTIFF larger than its tile, with no overviews."""
    size = data.draw(st.integers(min_value=300, max_value=700))
    _rewrite_raster(
        _scene(root, data), width=size, height=size, tiled=True, blockxsize=256, blockysize=256
    )


@breaks("PTL-DAT-013")
def bad_tile_size(root: Path, data: DataObject) -> None:
    """Rewrite one scene with non-square tiles, or square tiles over 512px."""
    scene = _scene(root, data)
    # A tile as large as the raster reads as a strip, so the raster is larger.
    if data.draw(st.booleans()):
        _rewrite_raster(scene, width=1024, height=1024, tiled=True, blockxsize=768, blockysize=768)
        return
    width, height = data.draw(
        st.lists(st.sampled_from((16, 32, 64)), min_size=2, max_size=2, unique=True)
    )
    _rewrite_raster(scene, width=128, height=128, tiled=True, blockxsize=width, blockysize=height)
