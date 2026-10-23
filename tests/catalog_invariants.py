"""Whole-catalog invariants for every catalog the real CLI writes in a test.

The ``run_cli`` fixture in ``tests/conftest.py`` records each catalog root a
CLI invocation creates under ``tmp_path``. At teardown it calls
:func:`check_catalog_invariants` on those roots. A hand-written fixture under
``tests/fixtures/`` never reaches the checker, so the check is opt-in by
construction.

The checker computes every expected value from the bytes on disk with
``hashlib``, ``pyarrow``, ``rasterio``, and ``json``. It imports nothing from
``portolan_cli``, because it must not trust the code it checks.

The codes fall in two groups, each with its source:

- ``P-*`` rules come from portolan-spec and the rashid ``PTL-*`` rules.
- ``S-*`` rules come from STAC 1.1.0 and its extensions.

Each code names the open issues it catches in its docstring.
``KNOWN_INVARIANT_GAPS`` lists the codes that still fire on ``main``. The
fixture drops those codes, and ``TestKnownGapsStillFire`` in
``tests/integration/test_catalog_invariants.py`` proves each one still fires.
A fix deletes its entry and its still-fires test in the same commit.

Run ``python -m tests.catalog_invariants <root>`` to check one catalog by hand.
The command exits 1 when it finds a violation.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol
from urllib.parse import parse_qsl, urlsplit

import yaml

if TYPE_CHECKING:
    from click.testing import Result

# Codes that fire on main, each keyed to the open issue that tracks it. The
# fixture drops these codes, so a test that builds a catalog through run_cli
# stays green while the bug is open.
KNOWN_INVARIANT_GAPS: dict[str, str] = {
    # Each item that add writes carries file:* fields without the file
    # extension URI.
    "S-EXT-DECLARED": "#1037",
    # Collection summaries flatten proj:bbox, proj:shape, and proj:transform.
    "P-SUMMARY-FROM-ITEMS": "#1023",
    # add keeps the source GeoJSON as a data asset beside its GeoParquet.
    "P-DATA-ROLE-TYPE": "#960",
    # add --force keeps the old extent after the data shrinks.
    "S-EXTENT-NOT-STALE": "#801",
    # An Iceberg collection publishes the local sqlite path in
    # iceberg:catalog_uri.
    "P-NO-LOCAL-PATH": "#939",
    # A raster add keys the item thumbnail a second time at the collection root.
    "P-VERSIONS-KEY": "#1053",
}

COG_MEDIA_TYPE = "image/tiff; application=geotiff; profile=cloud-optimized"
PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"

# Media types that a data asset may carry (portolan-spec formats.md).
CLOUD_NATIVE_DATA_TYPES = frozenset(
    {
        PARQUET_MEDIA_TYPE,
        "application/x-parquet",
        COG_MEDIA_TYPE,
        "application/vnd.flatgeobuf",
        "application/vnd+zarr",
        "application/vnd.zarr",
        "application/vnd.laszip+copc",
        "application/vnd.apache.iceberg",
    }
)
# File extensions that a data asset never carries, whatever its media type.
NON_DATA_SUFFIXES = frozenset({".geojson", ".csv", ".tsv", ".pmtiles", ".shp", ".gpkg", ".kml"})
# Text tables that tabular.convert: false keeps as the data itself.
TABLE_SUFFIXES = frozenset({".csv", ".tsv"})

CREDENTIAL_KEYS = frozenset(
    {
        "token",
        "api_key",
        "apikey",
        "key",
        "access_token",
        "signature",
        "sig",
        "x-amz-credential",
        "x-amz-signature",
    }
)

# Field prefix -> a fragment of the extension schema URI that declares it.
EXTENSION_PREFIXES: dict[str, str] = {
    "file:": "stac-extensions.github.io/file/",
    "table:": "stac-extensions.github.io/table/",
    "proj:": "stac-extensions.github.io/projection/",
    "raster:": "stac-extensions.github.io/raster/",
    "version:": "stac-extensions.github.io/version/",
    "iceberg:": "/iceberg/",
}

LOCAL_PATH = re.compile(r"^(/|[A-Za-z]:[\\/])")
ISO_UTC = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")
TOLERANCE = 1e-9

Bbox = tuple[float, float, float, float]

EXEMPTIONS_FILE = Path(__file__).parent / "guardrail_exemptions.txt"


def load_exemptions(kind: str) -> list[list[str]]:
    """Return the tab-separated fields after ``kind`` for each line of that kind.

    ``tests/guardrail_exemptions.txt`` holds three kinds of line: ``xfail``
    (issue, owner, where), ``invariant`` (a ``build_damaged_catalog`` reason),
    and ``config-key`` (a key the reachability test cannot observe).
    """
    rows: list[list[str]] = []
    for line in EXEMPTIONS_FILE.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        fields = line.split("\t")
        if fields[0] == kind:
            rows.append(fields[1:])
    return rows


class RunCli(Protocol):
    """The ``run_cli`` fixture: invoke the real CLI and track the catalogs it writes."""

    def __call__(
        self,
        *args: str,
        cwd: Path | None = None,
        expect: int | None = 0,
        input: str | None = None,  # noqa: A002 - mirrors CliRunner.invoke
    ) -> Result: ...


@dataclass(frozen=True)
class Violation:
    """One broken invariant: its code, the file, and what differs."""

    code: str
    path: str
    detail: str


@dataclass
class _Node:
    path: Path
    data: dict[str, Any]


@dataclass
class _Catalog:
    root: Path
    catalogs: list[_Node]
    collections: list[_Node]
    items: list[_Node]
    versions: list[_Node]

    def owner(self, item: _Node) -> _Node | None:
        """Return the collection whose directory is the nearest ancestor of ``item``."""
        best: _Node | None = None
        for collection in self.collections:
            directory = collection.path.parent
            if item.path.is_relative_to(directory) and (
                best is None or len(directory.parts) > len(best.path.parent.parts)
            ):
                best = collection
        return best

    def members(self, collection: _Node) -> list[_Node]:
        return [item for item in self.items if self.owner(item) is collection]

    def stac_nodes(self) -> list[_Node]:
        return [*self.catalogs, *self.collections, *self.items]


def _load(root: Path) -> _Catalog:
    catalog = _Catalog(root, [], [], [], [])
    for path in sorted(root.rglob("*.json")):
        relative = path.relative_to(root)
        if ".portolan" in relative.parts:
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict):
            continue
        node = _Node(path, data)
        kind = data.get("type")
        if path.name == "versions.json":
            catalog.versions.append(node)
        elif kind == "Catalog":
            catalog.catalogs.append(node)
        elif kind == "Collection":
            catalog.collections.append(node)
        elif kind == "Feature" and "stac_version" in data:
            catalog.items.append(node)
    return catalog


def _rel(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return str(path)


def _has_scheme(href: str) -> bool:
    return bool(urlsplit(href).scheme) and not re.match(r"^[A-Za-z]:[\\/]", href)


def _local(node: _Node, href: str) -> Path | None:
    """Resolve a relative href against the JSON file's directory."""
    if _has_scheme(href) or LOCAL_PATH.match(href):
        return None
    return (node.path.parent / href.split("#")[0].split("?")[0]).resolve()


def _assets(node: _Node) -> dict[str, dict[str, Any]]:
    """Return the object-valued entries of the node's ``assets``, or none."""
    raw = node.data.get("assets")
    pairs = raw.items() if isinstance(raw, dict) else ()
    return dict(pair for pair in pairs if isinstance(pair[1], dict))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [s for key, item in value.items() for s in (*_strings(key), *_strings(item))]
    if isinstance(value, list):
        return [s for item in value for s in _strings(item)]
    return []


# ---------------------------------------------------------------------------
# Bbox geometry
# ---------------------------------------------------------------------------


def _as_bbox(raw: Any) -> Bbox | None:
    """Return the 2D part of a 4- or 6-number bbox, or None for another shape."""
    if not isinstance(raw, list) or not all(isinstance(v, (int, float)) for v in raw):
        return None
    if len(raw) == 4:
        return (float(raw[0]), float(raw[1]), float(raw[2]), float(raw[3]))
    if len(raw) == 6:
        return (float(raw[0]), float(raw[1]), float(raw[3]), float(raw[4]))
    return None


def _segments(box: Bbox) -> list[Bbox]:
    """Split a bbox that crosses the antimeridian (west > east) into two."""
    west, south, east, north = box
    if west <= east:
        return [box]
    return [(west, south, 180.0, north), (-180.0, south, east, north)]


def _inside(inner: Bbox, outer: Bbox) -> bool:
    return (
        inner[0] >= outer[0] - TOLERANCE
        and inner[1] >= outer[1] - TOLERANCE
        and inner[2] <= outer[2] + TOLERANCE
        and inner[3] <= outer[3] + TOLERANCE
    )


def _contains(outer: Bbox, inner: Bbox) -> bool:
    """True when every segment of ``inner`` lies inside a segment of ``outer``."""
    outer_parts = _segments(outer)
    return all(any(_inside(part, o) for o in outer_parts) for part in _segments(inner))


def _union(boxes: list[Bbox]) -> Bbox:
    """Union bboxes. A crossing input yields one crossing union (west > east)."""
    parts = [part for box in boxes for part in _segments(box)]
    south = min(p[1] for p in parts)
    north = max(p[3] for p in parts)
    if all(box[0] <= box[2] for box in boxes):
        return (min(p[0] for p in parts), south, max(p[2] for p in parts), north)
    # Segments east of the antimeridian set the west edge, the others the east edge.
    east_side = [p for p in parts if p[2] >= 180.0 - TOLERANCE or (p[0] + p[2]) / 2 >= 0]
    west_side = [p for p in parts if p not in east_side]
    west = min(p[0] for p in east_side)
    east = max(p[2] for p in west_side) if west_side else -180.0
    return (west, south, east, north)


def _same(a: Bbox, b: Bbox) -> bool:
    return all(abs(x - y) <= TOLERANCE for x, y in zip(a, b, strict=True))


def _data_bbox(path: Path) -> Bbox | None:
    """Read the extent of one data file from its bytes, in EPSG:4326."""
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        import pyarrow.parquet as pq

        metadata = pq.read_schema(path).metadata or {}
        if b"geo" not in metadata:
            return None
        geo = json.loads(metadata[b"geo"])
        column = geo.get("columns", {}).get(geo.get("primary_column"), {})
        return _as_bbox(column.get("bbox"))
    if suffix in {".tif", ".tiff"}:
        import rasterio
        from rasterio.warp import transform_bounds

        with rasterio.open(path) as source:
            if source.crs is None:
                return None
            west, south, east, north = transform_bounds(source.crs, "EPSG:4326", *source.bounds)
        return (west, south, east, north)
    return None


# ---------------------------------------------------------------------------
# Portolan group (portolan-spec, rashid PTL-* rules)
# ---------------------------------------------------------------------------


def _check_size(cat: _Catalog, node: _Node, key: str, asset: dict[str, Any]) -> list[Violation]:
    """P-SIZE (#930, #960): ``file:size`` equals the byte count of the file (PTL-AST-003)."""
    target = _local(node, str(asset.get("href", "")))
    if "file:size" not in asset or target is None or not target.is_file():
        return []
    actual = target.stat().st_size
    if asset["file:size"] == actual:
        return []
    return [
        Violation(
            "P-SIZE",
            _rel(cat.root, node.path),
            f"asset {key!r}: file:size {asset['file:size']} != {actual} bytes on disk",
        )
    ]


def _check_checksum(cat: _Catalog, node: _Node, key: str, asset: dict[str, Any]) -> list[Violation]:
    """P-CHECKSUM (#766 regression, #930): ``file:checksum`` is the sha2-256 multihash (PTL-AST-004)."""
    target = _local(node, str(asset.get("href", "")))
    if "file:checksum" not in asset or target is None or not target.is_file():
        return []
    expected = "1220" + _sha256(target)
    if asset["file:checksum"] == expected:
        return []
    return [
        Violation(
            "P-CHECKSUM",
            _rel(cat.root, node.path),
            f"asset {key!r}: file:checksum {asset['file:checksum']!r} != {expected!r}",
        )
    ]


def _names(columns: Any) -> list[str] | None:
    if not isinstance(columns, list):
        return None
    return [str(column.get("name")) for column in columns if isinstance(column, dict)]


def _check_columns(cat: _Catalog, node: _Node) -> list[Violation]:
    """P-COLUMNS (#931, #813): ``table:columns`` names equal the Parquet schema names.

    An asset that carries its own ``table:columns`` matches its file. The
    ``table:columns`` of the item or collection matches the schema of its one
    Parquet data asset, in order. For a collection of several files it holds
    the union of their column names. ``add`` reads the files in directory
    order, which differs between platforms, so the union compares as a set.
    """
    import pyarrow.parquet as pq

    owner_level = (
        node.data.get("properties", {}) if node.data.get("type") == "Feature" else node.data
    )
    shared: list[list[str]] = []
    checks: list[tuple[str, list[str] | None, list[str]]] = []
    for key, asset in _assets(node).items():
        target = _local(node, str(asset.get("href", "")))
        if (
            "data" not in asset.get("roles", [])
            or target is None
            or target.suffix.lower() != ".parquet"
            or not target.is_file()
        ):
            continue
        actual = pq.read_schema(target).names
        if "table:columns" in asset:
            checks.append((f"asset {key!r}", _names(asset["table:columns"]), actual))
        else:
            shared.append(actual)
    declared_shared = _names(owner_level.get("table:columns"))
    if len(shared) == 1:
        checks.append(("table:columns", declared_shared, shared[0]))
    elif shared and declared_shared is not None:
        union = sorted({name for names in shared for name in names})
        checks.append(("table:columns", sorted(declared_shared), union))
    return [
        Violation(
            "P-COLUMNS",
            _rel(cat.root, node.path),
            f"{where}: table:columns {declared} != Parquet schema {actual}",
        )
        for where, declared, actual in checks
        if declared is not None and declared != actual
    ]


def _check_data_role_type(
    cat: _Catalog, node: _Node, key: str, asset: dict[str, Any]
) -> list[Violation]:
    """P-DATA-ROLE-TYPE (#960, #929): a ``data`` asset is a cloud-native format.

    One exception: with ``tabular.convert: false`` the user keeps a CSV or TSV
    as the table itself. Such a file may carry the data role when no Parquet
    asset of the same stem sits beside it.
    """
    if "data" not in asset.get("roles", []):
        return []
    href = str(asset.get("href", ""))
    stem_path = Path(urlsplit(href).path)
    suffix = stem_path.suffix.lower()
    media_type = asset.get("type")
    if suffix not in NON_DATA_SUFFIXES and media_type in CLOUD_NATIVE_DATA_TYPES:
        return []
    if suffix in TABLE_SUFFIXES:
        siblings = {Path(urlsplit(str(a.get("href", ""))).path) for a in _assets(node).values()}
        if stem_path.with_suffix(".parquet") not in siblings:
            return []
    return [
        Violation(
            "P-DATA-ROLE-TYPE",
            _rel(cat.root, node.path),
            f"asset {key!r}: role data on {href!r} with type {media_type!r}",
        )
    ]


def _check_strings(cat: _Catalog, node: _Node) -> list[Violation]:
    """P-NO-LOCAL-PATH (#939, #840) and P-NO-CREDENTIAL (#981).

    A published catalog holds no absolute local path and no URL that carries
    a key, token, or signature query parameter.
    """
    found: list[Violation] = []
    where = _rel(cat.root, node.path)
    root_text = str(cat.root.resolve())
    for value in _strings(node.data):
        if LOCAL_PATH.match(value) or root_text in value:
            found.append(Violation("P-NO-LOCAL-PATH", where, f"local path {value!r}"))
        if _has_scheme(value) and "?" in value:
            keys = {k.lower() for k, _ in parse_qsl(urlsplit(value).query, keep_blank_values=True)}
            leaked = sorted(keys & CREDENTIAL_KEYS)
            if leaked:
                found.append(Violation("P-NO-CREDENTIAL", where, f"URL {value!r} carries {leaked}"))
    return found


def _check_versions(cat: _Catalog, stac_targets: set[Path]) -> list[Violation]:
    """P-VERSIONS-MATCH (#929, #960) and P-VERSIONS-KEY (#1053) on versions.json.

    P-VERSIONS-MATCH: each asset of the current snapshot carries the sha256
    and the size of its file, and a STAC asset in the catalog has an href that
    resolves to it.

    P-VERSIONS-KEY: the key of each asset is the path of its file relative to
    the collection directory, as ``add`` writes it. A second key for one file
    fails.
    """
    found: list[Violation] = []
    for node in cat.versions:
        versions = node.data.get("versions")
        if not isinstance(versions, list) or not versions:
            continue
        current = node.data.get("current_version")
        snapshot = next(
            (v for v in versions if isinstance(v, dict) and v.get("version") == current),
            versions[-1],
        )
        where = _rel(cat.root, node.path)
        for name, entry in (snapshot.get("assets") or {}).items():
            href = str(entry.get("href", name))
            target = (cat.root / href).resolve()
            # P-VERSIONS-KEY (#1053): the key names the file relative to the
            # collection directory, so one file has one key.
            if (node.path.parent / name).resolve() != target:
                found.append(
                    Violation(
                        "P-VERSIONS-KEY", where, f"key {name!r} names a file other than {href}"
                    )
                )
            if not target.is_file():
                found.append(Violation("P-VERSIONS-MATCH", where, f"{name!r}: {href} is missing"))
                continue
            size = target.stat().st_size
            if entry.get("size_bytes") != size:
                found.append(
                    Violation(
                        "P-VERSIONS-MATCH",
                        where,
                        f"{name!r}: size_bytes {entry.get('size_bytes')} != {size}",
                    )
                )
            digest = _sha256(target)
            if entry.get("sha256") != digest:
                found.append(
                    Violation(
                        "P-VERSIONS-MATCH",
                        where,
                        f"{name!r}: sha256 {entry.get('sha256')!r} != {digest!r}",
                    )
                )
            if target not in stac_targets:
                found.append(
                    Violation(
                        "P-VERSIONS-MATCH", where, f"{name!r}: no STAC asset points to {href}"
                    )
                )
    return found


def _check_summaries(cat: _Catalog, collection: _Node) -> list[Violation]:
    """P-SUMMARY-FROM-ITEMS (#1023): each list summary value is a value some member item holds."""
    summaries = collection.data.get("summaries")
    members = cat.members(collection)
    if not isinstance(summaries, dict) or not members:
        return []
    found: list[Violation] = []
    for field, values in summaries.items():
        if not isinstance(values, list):
            continue
        held: list[Any] = []
        for item in members:
            properties = item.data.get("properties", {})
            if field in properties:
                held.append(properties[field])
            held.extend(asset[field] for asset in _assets(item).values() if field in asset)
        found.extend(
            Violation(
                "P-SUMMARY-FROM-ITEMS",
                _rel(cat.root, collection.path),
                f"summaries[{field!r}] value {value!r} is on no member item",
            )
            for value in values
            if value not in held
        )
    return found


# ---------------------------------------------------------------------------
# STAC 1.1.0 group
# ---------------------------------------------------------------------------


def _collection_bboxes(node: _Node) -> Any:
    return node.data.get("extent", {}).get("spatial", {}).get("bbox")


def _check_bbox_shape(cat: _Catalog, node: _Node) -> list[Violation]:
    """S-BBOX-COUNT (#852) and S-BBOX-RANGE (#851 regression).

    STAC 1.1.0 allows a collection one bbox, or an overall bbox plus two or
    more sub-bboxes, so a list of two is invalid. An item bbox has 4 or 6
    numbers. Every longitude lies in [-180, 180] and every latitude in
    [-90, 90] exactly. West greater than east marks an antimeridian crossing.
    """
    found: list[Violation] = []
    where = _rel(cat.root, node.path)
    if node.data.get("type") == "Collection":
        raw = _collection_bboxes(node)
        boxes = raw if isinstance(raw, list) else []
        if len(boxes) == 2 or not boxes:
            found.append(
                Violation("S-BBOX-COUNT", where, f"extent.spatial.bbox has {len(boxes)} bboxes")
            )
    else:
        raw = node.data.get("bbox")
        boxes = [raw] if raw is not None else []
        if raw is not None and (not isinstance(raw, list) or len(raw) not in (4, 6)):
            found.append(Violation("S-BBOX-COUNT", where, f"bbox {raw!r} is not 4 or 6 numbers"))
    for raw_box in boxes:
        box = _as_bbox(raw_box)
        if box is None:
            if node.data.get("type") == "Collection":
                found.append(Violation("S-BBOX-COUNT", where, f"bbox {raw_box!r} is malformed"))
            continue
        west, south, east, north = box
        if not (
            -180.0 <= west <= 180.0 and -180.0 <= east <= 180.0 and -90.0 <= south <= north <= 90.0
        ):
            found.append(Violation("S-BBOX-RANGE", where, f"bbox {raw_box!r} is out of range"))
    return found


def _check_hrefs(cat: _Catalog, node: _Node) -> list[Violation]:
    """S-HREF-RESOLVES (#840): every relative link and asset href names a file that exists."""
    found: list[Violation] = []
    hrefs = [
        ("link", link.get("rel"), link.get("href"))
        for link in node.data.get("links", [])
        if isinstance(link, dict)
    ]
    hrefs += [("asset", key, asset.get("href")) for key, asset in _assets(node).items()]
    for kind, name, href in hrefs:
        if not isinstance(href, str):
            continue
        target = _local(node, href)
        if target is None:
            continue
        # A partition asset names its files with a glob, which resolves when it
        # matches at least one file.
        if any(c in href for c in "*?["):
            if next(node.path.parent.glob(href.removeprefix("./")), None) is None:
                found.append(
                    Violation(
                        "S-HREF-RESOLVES",
                        _rel(cat.root, node.path),
                        f"{kind} {name!r} glob {href!r} matches no file",
                    )
                )
        elif not target.exists():
            found.append(
                Violation(
                    "S-HREF-RESOLVES",
                    _rel(cat.root, node.path),
                    f"{kind} {name!r} href {href!r} does not resolve",
                )
            )
    return found


def _check_extent(cat: _Catalog, collection: _Node) -> list[Violation]:
    """S-EXTENT-CONTAINS (#932, #852) and S-EXTENT-NOT-STALE (#801).

    The overall bbox contains every sub-bbox and every member item bbox.

    The overall bbox also equals the union of the extents read from the data
    files. A collection-level ``.portolan/metadata.yaml`` may set
    ``extent.spatial.bbox`` as a human override. The override must then equal
    the stored bbox and contain the data. A stored bbox wider than the data
    with no override is the #801 shrink bug.
    """
    raw = _collection_bboxes(collection)
    if not isinstance(raw, list) or not raw:
        return []
    overall = _as_bbox(raw[0])
    if overall is None:
        return []
    where = _rel(cat.root, collection.path)
    found: list[Violation] = []
    members = cat.members(collection)
    inner: list[tuple[str, Bbox]] = [
        (f"extent.spatial.bbox[{i}]", box)
        for i, sub in enumerate(raw[1:], start=1)
        if (box := _as_bbox(sub)) is not None
    ]
    inner += [
        (_rel(cat.root, item.path), box)
        for item in members
        if (box := _as_bbox(item.data.get("bbox"))) is not None
    ]
    for name, box in inner:
        if not _contains(overall, box):
            found.append(
                Violation(
                    "S-EXTENT-CONTAINS", where, f"bbox {list(overall)} misses {name} {list(box)}"
                )
            )

    data_boxes: list[Bbox] = []
    for node in [collection, *members]:
        for asset in _assets(node).values():
            if "data" not in asset.get("roles", []):
                continue
            target = _local(node, str(asset.get("href", "")))
            if target is not None and target.is_file():
                box = _data_bbox(target)
                if box is not None:
                    data_boxes.append(box)
    if not data_boxes:
        return found
    data_union = _union(data_boxes)
    override = _override_bbox(collection.path.parent)
    if override is None:
        if not _same(overall, data_union):
            found.append(
                Violation(
                    "S-EXTENT-NOT-STALE",
                    where,
                    f"bbox {list(overall)} != data union {list(data_union)}",
                )
            )
    elif not _same(overall, override) or not _contains(override, data_union):
        found.append(
            Violation(
                "S-EXTENT-NOT-STALE",
                where,
                f"bbox {list(overall)} vs override {list(override)} and data {list(data_union)}",
            )
        )
    return found


def _override_bbox(directory: Path) -> Bbox | None:
    path = directory / ".portolan" / "metadata.yaml"
    if not path.is_file():
        return None
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        return None
    boxes = (loaded.get("extent") or {}).get("spatial", {}).get("bbox")
    if isinstance(boxes, list) and boxes and isinstance(boxes[0], list):
        return _as_bbox(boxes[0])
    return _as_bbox(boxes)


def _check_temporal(cat: _Catalog, collection: _Node) -> list[Violation]:
    """S-TEMPORAL: each interval is ISO-8601 UTC with a ``Z`` and starts at or before its end."""
    intervals = collection.data.get("extent", {}).get("temporal", {}).get("interval")
    where = _rel(cat.root, collection.path)
    if not isinstance(intervals, list) or not intervals:
        return [Violation("S-TEMPORAL", where, "extent.temporal.interval is missing")]
    found: list[Violation] = []
    for interval in intervals:
        if not isinstance(interval, list) or len(interval) != 2:
            found.append(Violation("S-TEMPORAL", where, f"interval {interval!r} is malformed"))
            continue
        start, end = interval
        bad = [
            v for v in interval if v is not None and not (isinstance(v, str) and ISO_UTC.match(v))
        ]
        if bad:
            found.append(Violation("S-TEMPORAL", where, f"interval {interval!r} is not ISO-8601 Z"))
        elif start is not None and end is not None and start > end:
            found.append(
                Violation("S-TEMPORAL", where, f"interval {interval!r} ends before it starts")
            )
    return found


def _prefixed_fields(node: _Node) -> set[str]:
    data = node.data
    keys: set[str] = set(data)
    keys |= set(data.get("properties", {}) or {})
    keys |= set(data.get("summaries", {}) or {})
    for asset in _assets(node).values():
        keys |= set(asset)
    return {key for key in keys if any(key.startswith(p) for p in EXTENSION_PREFIXES)}


def _check_extensions(cat: _Catalog, node: _Node) -> list[Violation]:
    """S-EXT-DECLARED: every extension-prefixed field has its schema URI in ``stac_extensions``."""
    declared = [str(uri) for uri in node.data.get("stac_extensions", []) or []]
    found: list[Violation] = []
    for prefix in sorted(
        {p for key in _prefixed_fields(node) for p in EXTENSION_PREFIXES if key.startswith(p)}
    ):
        fragment = EXTENSION_PREFIXES[prefix]
        if not any(fragment in uri for uri in declared):
            found.append(
                Violation(
                    "S-EXT-DECLARED",
                    _rel(cat.root, node.path),
                    f"{prefix} fields with no {fragment} schema in stac_extensions",
                )
            )
    return found


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def check_catalog_invariants(root: Path) -> list[Violation]:
    """Return every invariant violation in the catalog at ``root``."""
    cat = _load(root)
    found: list[Violation] = []
    stac_targets: set[Path] = set()
    for node in cat.stac_nodes():
        found += _check_strings(cat, node)
        found += _check_hrefs(cat, node)
        found += _check_extensions(cat, node)
        found += _check_columns(cat, node)
        if node.data.get("type") != "Catalog":
            found += _check_bbox_shape(cat, node)
        for key, asset in _assets(node).items():
            target = _local(node, str(asset.get("href", "")))
            if target is not None:
                stac_targets.add(target)
            found += _check_size(cat, node, key, asset)
            found += _check_checksum(cat, node, key, asset)
            found += _check_data_role_type(cat, node, key, asset)
    for collection in cat.collections:
        found += _check_extent(cat, collection)
        found += _check_temporal(cat, collection)
        found += _check_summaries(cat, collection)
    for node in cat.versions:
        found += _check_strings(cat, node)
    found += _check_versions(cat, stac_targets)
    return found


def codes(violations: list[Violation]) -> set[str]:
    """Return the distinct codes in ``violations``."""
    return {violation.code for violation in violations}


def format_violations(root: Path, violations: list[Violation]) -> str:
    """Render violations one per line, grouped under the catalog root."""
    lines = [f"{root}: {len(violations)} catalog invariant violation(s)"]
    lines += [f"  {v.code} {v.path}: {v.detail}" for v in violations]
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    """Check each root given on the command line and return 1 on any violation."""
    failed = False
    for arg in argv:
        root = Path(arg)
        violations = check_catalog_invariants(root)
        if violations:
            failed = True
            print(format_violations(root, violations))  # noqa: T201 - command-line entry point
        else:
            print(f"{root}: no catalog invariant violations")  # noqa: T201
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
