"""The fixers `portolan check --fix` dispatches on."""

from __future__ import annotations

import importlib
import json
import logging
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from rashid_core.api import (
    STRUCTURAL_RELS,
    CatalogGraph,
    ExpectedLink,
    Node,
    expected_links,
    has_cog,
    is_absolute_href,
    links_of,
    roles_of,
)

from portolan_cli.metadata.fix import FixAction, FixReport, FixResult
from portolan_cli.validation.remediation import Bucket, remediation_of

if TYPE_CHECKING:
    from portolan_core.run_state import RunState


#: A fixer sweeps ``catalog_root``. ``dry_run`` means report but do not write.
#: The run state is the one of the command, or None outside ``check --fix``.
class Fixer(Protocol):
    """A fixer sweeps ``catalog_root``. A call without a state opens its own run."""

    def __call__(
        self, root: Path, dry_run: bool, state: RunState | None = None, /
    ) -> list[FixResult]:
        """Repair the catalog and report each change."""


@dataclass(frozen=True)
class FixerRun:
    """What one ``apply_fixers`` sweep asked for, what it achieved, and why not."""

    report: FixReport
    selected: list[str] = field(default_factory=list)
    applied: list[str] = field(default_factory=list)
    skip_reasons: dict[str, list[str]] = field(default_factory=dict)


logger = logging.getLogger(__name__)

_STYLE_MEDIA_TYPE = "application/vnd.mapbox.style+json"
_PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"
_MIRROR_ROLE = "collection-mirror"
_MIRROR_FILENAME = "items.parquet"


# --- shared plumbing ---


def _graph(root: Path) -> CatalogGraph:
    return CatalogGraph.load(root)


def _write_node(node: Node, *, dry_run: bool) -> None:
    from portolan_core.json_io import write_json_atomic

    if dry_run:
        return
    write_json_atomic(node.abs_path, node.data)


def _reload(path: Path) -> dict[str, Any] | None:
    """Re-read a STAC object a repair rewrote behind the graph's back."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _updated(node: Node, message: str) -> FixResult:
    return FixResult(
        file_path=node.abs_path,
        action=FixAction.UPDATED,
        success=True,
        message=message,
    )


def _skipped(path: Path, message: str) -> FixResult:
    return FixResult(path, FixAction.SKIPPED, True, message)


def _assets_of(node: Node) -> dict[str, dict[str, Any]]:
    assets = node.data.get("assets")
    if not isinstance(assets, dict):
        return {}
    return {key: value for key, value in assets.items() if isinstance(value, dict)}


def _local_asset_path(node: Node, asset: dict[str, Any]) -> Path | None:
    """The file an asset href points at, or None when it is not a local file."""
    href = asset.get("href")
    if not isinstance(href, str) or not href or is_absolute_href(href):
        return None
    candidate = (node.abs_path.parent / href).resolve()
    return candidate if candidate.is_file() else None


# --- links: PTL-LNK-001..006 ---


def _reuse_link(
    existing: list[dict[str, Any]], node: Node, graph: CatalogGraph, expected: ExpectedLink
) -> dict[str, Any] | None:
    """An existing link to carry forward, so titles and extras survive the repair."""
    same_rel = [link for link in existing if link.get("rel") == expected.rel]
    if not same_rel:
        return None
    if expected.rel not in ("child", "item"):
        return same_rel[0]
    for link in same_rel:
        href = link.get("href")
        if isinstance(href, str) and graph.resolve_link(node, href) is expected.target:
            return link
    return None


def _rebuilt_rels() -> frozenset[str]:
    """The rels the rebuild re-emits itself, so stale copies are dropped first."""
    return frozenset(STRUCTURAL_RELS) | {"self"}


def _rebuild_links(node: Node, graph: CatalogGraph) -> list[dict[str, Any]]:
    """The link block ``node`` should carry: structural first, everything else after."""
    existing = links_of(node)
    rebuilt: list[dict[str, Any]] = []
    for expected in expected_links(graph, node):
        rel, target = expected.rel, expected.target
        link = dict(_reuse_link(existing, node, graph, expected) or {})
        link["rel"] = rel
        link["href"] = expected.href
        link["type"] = expected.type
        if rel in ("child", "item") and not link.get("title"):
            # An item holds its title under properties, a catalog at the top.
            properties = target.data.get("properties")
            own = properties if rel == "item" and isinstance(properties, dict) else target.data
            title = own.get("title") or target.id
            if isinstance(title, str) and title:
                link["title"] = title
        rebuilt.append(link)
    rebuilt.extend(link for link in existing if link.get("rel") not in _rebuilt_rels())
    return rebuilt


def _fix_links(root: Path, dry_run: bool, _state: RunState | None = None) -> list[FixResult]:
    """Drop self links, backfill and repoint the structural block, relativize hrefs."""
    results: list[FixResult] = []
    graph = _graph(root)
    for node in graph.iter("catalog", "collection", "item"):
        rebuilt = _rebuild_links(node, graph)
        if rebuilt == links_of(node) and isinstance(node.data.get("links"), list):
            continue
        node.data["links"] = rebuilt
        _write_node(node, dry_run=dry_run)
        results.append(_updated(node, "Rebuilt the structural links from the file tree"))
    return results


# --- bbox: PTL-BBX-001 ---


def _extent_bboxes(node: Node) -> list[Any] | None:
    extent = node.data.get("extent")
    if not isinstance(extent, dict):
        return None
    spatial = extent.get("spatial")
    if not isinstance(spatial, dict):
        return None
    boxes = spatial.get("bbox")
    return boxes if isinstance(boxes, list) else None


def _looks_valid(box: Any) -> bool:
    from portolan_core.bbox import is_valid_bbox, snap_to_wgs84_range, to_2d_bbox

    if not isinstance(box, list) or len(box) not in (4, 6):
        return False
    if any(isinstance(v, bool) or not isinstance(v, (int, float)) for v in box):
        return False
    flat = to_2d_bbox(box)
    if not is_valid_bbox(flat):
        return False
    # A coordinate just past the bound passes is_valid_bbox once snapped, but the stored extent keeps the raw value.
    return snap_to_wgs84_range(flat) == flat


def _child_bboxes(node: Node, graph: CatalogGraph) -> list[list[float]]:
    """Every bbox contained by ``node``, from items directly and collections' extents."""
    from portolan_core.bbox import to_2d_bbox

    found: list[list[float]] = []
    for child in graph.children_of(node):
        if child.kind == "catalog":
            found.extend(_child_bboxes(child, graph))
            continue
        candidates: list[Any] = (
            [child.data.get("bbox")] if child.kind == "item" else (_extent_bboxes(child) or [])
        )
        found.extend(to_2d_bbox(box) for box in candidates if _looks_valid(box))
    return found


#: Asset suffix -> the extractor whose metadata object ``wgs84_bbox``
#: understands. Anything absent from this map has no readable footprint, so the
#: bbox fixer skips it rather than inventing one.
_BBOX_EXTRACTORS: dict[str, tuple[str, str]] = {
    ".parquet": ("portolan_cli.metadata.geoparquet", "extract_geoparquet_metadata"),
    ".tif": ("portolan_cli.metadata.cog", "extract_cog_metadata"),
    ".tiff": ("portolan_cli.metadata.cog", "extract_cog_metadata"),
    ".fgb": ("portolan_cli.metadata.flatgeobuf", "extract_flatgeobuf_metadata"),
    ".pmtiles": ("portolan_cli.metadata.pmtiles", "extract_pmtiles_metadata"),
}


def _wgs84_bbox_from_asset(node: Node, asset: dict[str, Any]) -> list[float] | None:
    """The asset's own footprint in WGS84, or None when it cannot be read."""
    path = _local_asset_path(node, asset)
    if path is None:
        return None
    entry = _BBOX_EXTRACTORS.get(path.suffix.lower())
    if entry is None:
        return None
    from portolan_cli.preparation import wgs84_bbox

    module_name, attribute = entry
    try:
        extract = getattr(importlib.import_module(module_name), attribute)
        return wgs84_bbox(extract(path))
    except Exception:  # any extractor failure is an honest skip
        return None


def _asset_bboxes(node: Node) -> list[list[float]]:
    """Every readable footprint among ``node``'s own local assets."""
    found = []
    for asset in _assets_of(node).values():
        box = _wgs84_bbox_from_asset(node, asset)
        if box is not None:
            found.append(box)
    return found


_BBOX_SKIP_MESSAGE = (
    "Cannot recompute the bbox: nothing readable to derive it from; "
    "re-run `portolan add` so the bbox comes from the data"
)


def _fix_item_bbox(node: Node, *, dry_run: bool) -> FixResult | None:
    """Recompute one item's bbox (and, if absent, its geometry) from its assets."""
    from portolan_cli.item import _bbox_to_polygon
    from portolan_core.bbox import compute_bbox_union

    declared = "bbox" in node.data
    if declared and _looks_valid(node.data["bbox"]):
        return None
    union = compute_bbox_union(_asset_bboxes(node), wgs84_only=True).bbox
    if union is None:
        # An item with no bbox key at all is another rule's finding; only a
        # declared-but-broken bbox is this fixer's to report.
        return _skipped(node.abs_path, _BBOX_SKIP_MESSAGE) if declared else None
    node.data["bbox"] = union
    if not isinstance(node.data.get("geometry"), dict):
        node.data["geometry"] = _bbox_to_polygon(union)
    _write_node(node, dry_run=dry_run)
    return _updated(node, "Recomputed the bbox from the item's own assets")


def _fix_container_bbox(node: Node, graph: CatalogGraph, *, dry_run: bool) -> FixResult | None:
    """Recompute a catalog/collection extent from its children, else its own assets."""
    boxes = _extent_bboxes(node)
    if boxes is None or all(_looks_valid(box) for box in boxes):
        return None
    from portolan_cli.stac import build_spatial_extent
    from portolan_core.errors import InvalidBboxError

    # Items were repaired first and graph nodes are shared, so the bboxes written above are visible here.
    try:
        extent = build_spatial_extent(_child_bboxes(node, graph) or _asset_bboxes(node))
    except InvalidBboxError:
        return _skipped(node.abs_path, _BBOX_SKIP_MESSAGE)
    node.data["extent"]["spatial"]["bbox"] = extent.bboxes
    _write_node(node, dry_run=dry_run)
    return _updated(node, "Recomputed the spatial extent from contained bboxes")


def _fix_bbox(root: Path, dry_run: bool, _state: RunState | None = None) -> list[FixResult]:
    """Recompute broken bboxes: items from their assets, containers from children."""
    graph = _graph(root)
    results = [
        result
        for node in graph.iter("item")
        if (result := _fix_item_bbox(node, dry_run=dry_run)) is not None
    ]
    results.extend(
        result
        for node in graph.iter("catalog", "collection")
        if (result := _fix_container_bbox(node, graph, dry_run=dry_run)) is not None
    )
    return results


# --- assets: PTL-AST-001 ---


#: What ``preparation._get_media_type`` returns when the extension is unknown.
#: Writing it would silence PTL-AST-001 with a value that says nothing, so an
#: asset that would receive it is reported instead.
_OCTET_STREAM = "application/octet-stream"


def _backfill_asset(asset: dict[str, Any], href: str) -> bool | None:
    """Type and role one asset. True if changed, False if already fine, None if unknown."""
    from portolan_cli.preparation import _get_asset_role, _get_media_type

    suffix = Path(href)
    needs_type = not isinstance(asset.get("type"), str) or not str(asset["type"]).strip()
    needs_roles = not roles_of(asset)
    if not needs_type and not needs_roles:
        return False
    if needs_type and _get_media_type(suffix) == _OCTET_STREAM:
        return None
    if needs_type:
        asset["type"] = _get_media_type(suffix)
    if needs_roles:
        asset["roles"] = [_get_asset_role(suffix)]
    return True


def _fix_assets(root: Path, dry_run: bool, _state: RunState | None = None) -> list[FixResult]:
    """Backfill asset media type and roles from the extension registry."""
    results: list[FixResult] = []
    for node in _graph(root).iter("collection", "item"):
        changed = False
        unknown: list[str] = []
        for key, asset in _assets_of(node).items():
            href = asset.get("href")
            if not isinstance(href, str) or not href:
                continue
            outcome = _backfill_asset(asset, href)
            if outcome is None:
                unknown.append(key)
            changed = changed or bool(outcome)
        if changed:
            _write_node(node, dry_run=dry_run)
            results.append(_updated(node, "Backfilled asset media types and roles"))
        if unknown:
            results.append(
                _skipped(
                    node.abs_path,
                    f"Cannot type {', '.join(sorted(unknown))}: the extension is not in the "
                    "extension registry; set the asset type by hand or convert the file",
                )
            )
    return results


# --- media_type: PTL-DAT-003 ---

#: The magic bytes of each format the data pass sniffs, and the media type they imply.
_MAGIC_MEDIA_TYPES: tuple[tuple[bytes, str], ...] = (
    (b"PAR1", _PARQUET_MEDIA_TYPE),
    (b"II*\x00", "image/tiff; application=geotiff"),
    (b"MM\x00*", "image/tiff; application=geotiff"),
    (b"PMTiles", "application/vnd.pmtiles"),
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"\xff\xd8\xff", "image/jpeg"),
)


def _kind(media_type: str) -> str:
    """The format a media type names, so that equivalent spellings compare equal."""
    lowered = media_type.lower()
    family = next((name for name in ("parquet", "pmtiles") if name in lowered), None)
    return family or lowered.split(";")[0].strip().replace("image/jpg", "image/jpeg")


def _sniffed_media_type(path: Path) -> str | None:
    """The media type the file's magic bytes imply, preferring the extension's spelling."""
    from portolan_cli.formats import is_cloud_optimized_geotiff
    from portolan_cli.preparation import _get_media_type

    with path.open("rb") as handle:
        head = handle.read(8)
    sniffed = next((kind for magic, kind in _MAGIC_MEDIA_TYPES if head.startswith(magic)), None)
    if sniffed is None:
        return None
    if _kind(sniffed) == "image/tiff":
        cog = "image/tiff; application=geotiff; profile=cloud-optimized"
        return cog if is_cloud_optimized_geotiff(path) else sniffed
    by_extension = _get_media_type(path)
    return by_extension if _kind(by_extension) == _kind(sniffed) else sniffed


def _fix_media_types(root: Path, dry_run: bool, _state: RunState | None = None) -> list[FixResult]:
    """Declare the media type that each local asset's bytes carry."""
    known = {_kind(media_type) for _, media_type in _MAGIC_MEDIA_TYPES}
    results: list[FixResult] = []
    for node in _graph(root).iter("collection", "item"):
        changed = False
        for asset in _assets_of(node).values():
            declared = asset.get("type")
            path = _local_asset_path(node, asset)
            if path is None or not isinstance(declared, str) or _kind(declared) not in known:
                continue
            try:
                sniffed = _sniffed_media_type(path)
            except OSError as exc:
                logger.warning("Cannot read %s to type it: %s", path.name, exc)
                continue
            if sniffed is not None and _kind(sniffed) != _kind(declared):
                asset["type"] = sniffed
                changed = True
        if changed:
            _write_node(node, dry_run=dry_run)
            results.append(_updated(node, "Typed the assets from their bytes"))
    return results


# --- checksum: PTL-AST-003/004, PTL-DAT-001/002 ---


def _fix_checksums(root: Path, dry_run: bool, _state: RunState | None = None) -> list[FixResult]:
    """Recompute file:size and the file:checksum multihash from the asset bytes."""
    from portolan_core.checksums import file_fields

    results: list[FixResult] = []
    for node in _graph(root).iter("collection", "item"):
        changed = False
        for asset in _assets_of(node).values():
            path = _local_asset_path(node, asset)
            if path is None:
                continue
            for name, value in file_fields(path).items():
                if asset.get(name) != value:
                    asset[name] = value
                    changed = True
        if changed:
            _write_node(node, dry_run=dry_run)
            results.append(_updated(node, "Recomputed file:size and file:checksum from the bytes"))
    return results


# --- styles: PTL-VIZ-005 ---


def _provides_pmtiles(node: Node) -> bool:
    for asset in _assets_of(node).values():
        href = asset.get("href")
        if asset.get("type") == "application/vnd.pmtiles" or (
            isinstance(href, str) and href.endswith(".pmtiles")
        ):
            return True
    return any(link.get("rel") == "pmtiles" for link in links_of(node))


def _fix_styles(root: Path, dry_run: bool, _state: RunState | None = None) -> list[FixResult]:
    """Type a PMTiles collection's style assets as MapLibre GL styles."""
    results: list[FixResult] = []
    for node in _graph(root).iter("collection"):
        if not _provides_pmtiles(node):
            continue
        changed = False
        for asset in _assets_of(node).values():
            if "style" in roles_of(asset) and asset.get("type") != _STYLE_MEDIA_TYPE:
                asset["type"] = _STYLE_MEDIA_TYPE
                changed = True
        if changed:
            _write_node(node, dry_run=dry_run)
            results.append(_updated(node, f"Typed the style assets {_STYLE_MEDIA_TYPE}"))
    return results


# --- thumbnail: PTL-VIZ-001 ---

#: rashid's _THUMBNAIL_TYPES. A thumbnail asset typed anything else fails
#: PTL-VIZ-001 just as surely as a missing one.
_THUMBNAIL_MEDIA_TYPES = {"image/png", "image/jpeg", "image/webp"}
_THUMBNAIL_TYPE_BY_SUFFIX = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
}


def _retype_thumbnails(node: Node, dry_run: bool) -> FixResult | None:
    """Re-derive a mistyped thumbnail's media type from the file it points at."""
    changed = False
    for asset in _assets_of(node).values():
        if "thumbnail" not in roles_of(asset):
            continue
        if asset.get("type") in _THUMBNAIL_MEDIA_TYPES:
            continue
        href = asset.get("href")
        if not isinstance(href, str):
            continue
        correct = _THUMBNAIL_TYPE_BY_SUFFIX.get(Path(href).suffix.lower())
        if correct is None:
            # Points at something that is not an image; retyping would lie.
            continue
        asset["type"] = correct
        changed = True
    if not changed:
        return None
    _write_node(node, dry_run=dry_run)
    return _updated(node, "Retyped the thumbnail asset from the file it points at")


def _fix_thumbnail(root: Path, dry_run: bool, state: RunState | None = None) -> list[FixResult]:
    """Give every geospatial collection a correctly typed thumbnail asset."""
    from portolan_cli.collection_thumbnail import (
        ensure_collection_thumbnail,
        would_generate_thumbnail,
    )

    results: list[FixResult] = []
    for node in _graph(root).iter("collection"):
        retyped = _retype_thumbnails(node, dry_run)
        if retyped is not None:
            results.append(retyped)
            continue
        if any("thumbnail" in roles_of(asset) for asset in _assets_of(node).values()):
            continue
        collection_dir = node.abs_path.parent
        if dry_run:
            # Ask the orchestrator: a tabular or undrawable collection declines, and a dry run must say so.
            if would_generate_thumbnail(collection_dir, root, state=state):
                results.append(_skipped(node.abs_path, "Would generate a collection thumbnail"))
            continue
        thumbnail = ensure_collection_thumbnail(collection_dir, root, state=state)
        if thumbnail is not None:
            results.append(_updated(node, f"Generated the collection thumbnail {thumbnail.name}"))
    return results


# --- item_mirror: PTL-MIR-001/002 ---


def _mirror_assets(node: Node) -> list[dict[str, Any]]:
    found = []
    for asset in _assets_of(node).values():
        href = asset.get("href")
        by_href = isinstance(href, str) and href.rsplit("/", 1)[-1] == _MIRROR_FILENAME
        if _MIRROR_ROLE in roles_of(asset) or by_href:
            found.append(asset)
    return found


def _register_mirror(node: Node) -> bool:
    """Give every mirror asset the role and media type the spec names."""
    changed = False
    for asset in _mirror_assets(node):
        roles = roles_of(asset)
        if _MIRROR_ROLE not in roles:
            asset["roles"] = [*roles, _MIRROR_ROLE]
            changed = True
        if asset.get("type") != _PARQUET_MEDIA_TYPE:
            asset["type"] = _PARQUET_MEDIA_TYPE
            changed = True
    return changed


def _has_cog_items(node: Node, graph: CatalogGraph) -> bool:
    """Whether ``node`` has scene items carrying COGs, exactly as rashid scopes it."""
    return any(has_cog(item) for item in graph.items_of(node))


def _write_mirror(collection_dir: Path, root: Path, state: RunState | None) -> None:
    """Write items.parquet, register it, and record its bytes in versions.json."""
    from portolan_cli.stac_parquet import (
        generate_items_parquet,
        register_mirror_asset,
        track_parquet_in_versions,
    )
    from portolan_cli.version_ops import owned_run

    with owned_run(root, state) as run:
        generate_items_parquet(collection_dir, state=run)
        register_mirror_asset(collection_dir, state=run)
        if (collection_dir / "versions.json").exists():
            track_parquet_in_versions(collection_dir, root, state=run)


def _generate_mirror(node: Node, root: Path, *, dry_run: bool, state: RunState | None) -> FixResult:
    """Write items.parquet for a scene collection and register it."""
    collection_dir = node.abs_path.parent
    if dry_run:
        # Same action as the real run: a dry run must predict what will happen,
        # not report a different verb for the same work.
        return FixResult(
            node.abs_path,
            FixAction.CREATED,
            True,
            "Would generate items.parquet and register the mirror",
        )
    try:
        _write_mirror(collection_dir, root, state)
    except (ImportError, ValueError, OSError) as exc:
        return FixResult(
            node.abs_path, FixAction.SKIPPED, True, f"Cannot generate the mirror: {exc}"
        )
    return FixResult(node.abs_path, FixAction.CREATED, True, "Generated the items.parquet mirror")


def _fix_item_mirror(root: Path, dry_run: bool, state: RunState | None = None) -> list[FixResult]:
    """Publish and register the stac-geoparquet item mirror."""
    results: list[FixResult] = []
    graph = _graph(root)
    for node in graph.iter("collection"):
        if _mirror_assets(node):
            if _register_mirror(node):
                _write_node(node, dry_run=dry_run)
                results.append(_updated(node, "Registered items.parquet as the collection mirror"))
            continue
        if _has_cog_items(node, graph):
            results.append(_generate_mirror(node, root, dry_run=dry_run, state=state))
    return results


# --- partition: PTL-PRT-001 ---


def _is_partitioned(node: Node) -> bool:
    from portolan_core.constants import PARTITION_EXTENSION_URI

    if any(key.startswith("partition:") for key in node.data):
        return True
    extensions = node.data.get("stac_extensions")
    return isinstance(extensions, list) and PARTITION_EXTENSION_URI in extensions


def _fix_partition(root: Path, dry_run: bool, _state: RunState | None = None) -> list[FixResult]:
    """Populate the partition: block from the Hive layout on disk."""
    from portolan_cli.partitioning import build_glob_pattern, detect_partitioning
    from portolan_core.constants import PARTITION_EXTENSION_URI

    results: list[FixResult] = []
    for node in _graph(root).iter("collection"):
        if not _is_partitioned(node):
            continue
        detected = detect_partitioning(node.abs_path.parent)
        if detected is None:
            results.append(
                _skipped(
                    node.abs_path,
                    "Cannot read the partition layout: no key=value directories on disk",
                )
            )
            continue
        columns = [key["name"] for key in detected["partition:keys"]]
        wanted: dict[str, Any] = {
            "partition:scheme": detected["partition:scheme"],
            "partition:keys": detected["partition:keys"],
            "partition:glob": build_glob_pattern(partition_columns=columns),
        }
        changed = False
        for name, value in wanted.items():
            if not node.data.get(name):
                node.data[name] = value
                changed = True
        extensions = node.data.setdefault("stac_extensions", [])
        if isinstance(extensions, list) and PARTITION_EXTENSION_URI not in extensions:
            extensions.append(PARTITION_EXTENSION_URI)
            changed = True
        if changed:
            _write_node(node, dry_run=dry_run)
            results.append(_updated(node, "Populated the partition: fields from the disk layout"))
    return results


# --- conformance, required files, and the existing repairers ---


def _fix_schema_uri(root: Path, dry_run: bool, _state: RunState | None = None) -> list[FixResult]:
    """Declare the versioned Portolan profile URI on catalogs and collections."""
    from portolan_cli.catalog import ensure_schema_uris
    from portolan_cli.stac import ensure_portolan_schema_uri

    results = [
        _updated(node, "Declared the Portolan schema URI")
        for node in _graph(root).iter("catalog", "collection")
        if ensure_portolan_schema_uri(dict(node.data))
    ]
    if results and not dry_run:
        ensure_schema_uris(root)
    return results


#: (rel-presence predicate, filename) pairs for the two required sibling docs.
_Gap = Callable[[Path, Path, dict[str, Any]], bool]


def _readme_gap(root: Path, stac_path: Path, data: dict[str, Any]) -> bool:
    """True when README.md is absent or its rel='describedby' link is unfit."""
    from portolan_cli.readme import README_FILENAME, readme_link_gap

    return not (stac_path.parent / README_FILENAME).exists() or readme_link_gap(
        root, stac_path, data
    )


def _agents_gap(root: Path, stac_path: Path, data: dict[str, Any]) -> bool:
    """True when AGENTS.md is absent or its rel='agents' link is unfit."""
    from portolan_core.agents_md import AGENTS_MD_FILENAME, agents_link_gap

    return not (stac_path.parent / AGENTS_MD_FILENAME).exists() or agents_link_gap(
        root, stac_path, data
    )


def _fix_markdown_requirement(
    root: Path,
    dry_run: bool,
    *,
    gap: _Gap,
    repair: Callable[[Path], Any],
    message: str,
) -> list[FixResult]:
    """Scaffold a required sibling markdown file and its link, and say so honestly."""
    gapped = [
        node
        for node in _graph(root).iter("catalog", "collection")
        if gap(root, node.abs_path, node.data)
    ]
    if not gapped or dry_run:
        return [_updated(node, message) for node in gapped]
    repair(root)
    results = []
    for node in gapped:
        data = _reload(node.abs_path)
        if data is None or gap(root, node.abs_path, data):
            results.append(
                _skipped(
                    node.abs_path, f"{message}, but the link still does not conform; fix it by hand"
                )
            )
        else:
            results.append(_updated(node, message))
    return results


def _fix_readme(root: Path, dry_run: bool, state: RunState | None = None) -> list[FixResult]:
    """Scaffold README.md and its rel='describedby' link."""
    from portolan_cli.readme import ensure_readmes

    return _fix_markdown_requirement(
        root,
        dry_run,
        gap=_readme_gap,
        repair=lambda directory: ensure_readmes(directory, state=state),
        message="Scaffolded README.md and its rel='describedby' link",
    )


def _fix_agents(root: Path, dry_run: bool, _state: RunState | None = None) -> list[FixResult]:
    """Scaffold AGENTS.md and its rel='agents' link."""
    from portolan_core.agents_md import ensure_agents_md_tree

    return _fix_markdown_requirement(
        root,
        dry_run,
        gap=_agents_gap,
        repair=ensure_agents_md_tree,
        message="Scaffolded AGENTS.md and its rel='agents' link",
    )


def _fix_required_files(
    root: Path, dry_run: bool, state: RunState | None = None
) -> list[FixResult]:
    """Every catalog and collection directory carries README.md and AGENTS.md."""
    return [*_fix_agents(root, dry_run, state), *_fix_readme(root, dry_run, state)]


def _fix_titles(root: Path, dry_run: bool, state: RunState | None = None) -> list[FixResult]:
    """Derive titles and descriptions, and backfill child/item link titles."""
    from portolan_cli.metadata.fix import repair_titles_and_links

    return repair_titles_and_links(root, dry_run=dry_run, state=state)


def _fix_pmtiles(root: Path, dry_run: bool, _state: RunState | None = None) -> list[FixResult]:
    """Register the rel='pmtiles' web-map-links link on PMTiles collections."""
    from portolan_cli.metadata.fix import repair_pmtiles_links

    return repair_pmtiles_links(root, dry_run=dry_run)


def _fix_convert(root: Path, _dry_run: bool, _state: RunState | None = None) -> list[FixResult]:
    """Inert: the geo-asset pass converts, and rewrites the files that these findings name.

    ``--metadata --fix`` must not rewrite asset bytes, so this reports the hand-off.
    """
    return [
        _skipped(
            root,
            "Conversion runs in the geo-asset pass; re-run `portolan check --fix` "
            "without --metadata",
        )
    ]


#: Fixer key -> implementation. Iteration order is execution order: structure
#: before content, so the link rebuild lands before the title backfill that
#: reads it, and conversion runs last because it rewrites the bytes every
#: earlier fixer described.
FIXERS: dict[str, Fixer] = {
    "schema_uri": _fix_schema_uri,
    "required_files": _fix_required_files,
    "agents": _fix_agents,
    "readme": _fix_readme,
    "links": _fix_links,
    "titles": _fix_titles,
    "assets": _fix_assets,
    "media_type": _fix_media_types,
    "bbox": _fix_bbox,
    "partition": _fix_partition,
    "item_mirror": _fix_item_mirror,
    "styles": _fix_styles,
    "thumbnail": _fix_thumbnail,
    "pmtiles": _fix_pmtiles,
    "checksum": _fix_checksums,
    "convert": _fix_convert,
}


#: Fixers that already run other registered fixers. The registry keeps every key
#: (a rule may name a member on its own), but selecting a composite must not also
#: select its members, or PTL-FIL-001/-002/-003 firing together would run the
#: README and AGENTS.md repairs twice over the same tree.
_COMPOSED_OF: dict[str, tuple[str, ...]] = {"required_files": ("agents", "readme")}


#: Repairs that must follow any pass that writes an item JSON.
#: Both are idempotent whole-catalog sweeps, so a second pass is safe.
POST_ITEM_WRITE_FIXERS: tuple[str, ...] = ("links", "checksum")


def _collection_dir_of(path: Path, root: Path) -> Path | None:
    """The nearest ancestor of ``path`` that holds a ``collection.json``."""
    for ancestor in path.resolve().parents:
        if (ancestor / "collection.json").exists():
            return ancestor
        if ancestor == root.resolve():
            break
    return None


def _refresh_mirrors(
    root: Path, changed: Iterable[Path], *, dry_run: bool, state: RunState | None
) -> list[FixResult]:
    """Rewrite ``items.parquet`` for every collection whose items just changed."""
    collections = {
        collection
        for path in changed
        if (collection := _collection_dir_of(path, root)) is not None
        and (collection / _MIRROR_FILENAME).exists()
    }
    results: list[FixResult] = []
    for collection in sorted(collections):
        target = collection / _MIRROR_FILENAME
        if dry_run:
            results.append(
                FixResult(target, FixAction.UPDATED, True, "Would refresh the items.parquet mirror")
            )
            continue
        try:
            _write_mirror(collection, root, state)
        except (ImportError, ValueError, OSError) as exc:
            results.append(
                FixResult(target, FixAction.SKIPPED, True, f"Cannot refresh the mirror: {exc}")
            )
            continue
        results.append(
            FixResult(target, FixAction.UPDATED, True, "Refreshed the items.parquet mirror")
        )
    return results


def repair_written_items(
    root: Path,
    changed: Iterable[Path],
    *,
    dry_run: bool,
    state: RunState | None = None,
) -> list[FixResult]:
    """Bring items a freshness pass just wrote up to the same standard as the rest."""
    changed = list(changed)
    results: list[FixResult] = []
    for key in POST_ITEM_WRITE_FIXERS:
        try:
            results.extend(FIXERS[key](root, dry_run, state))
        except Exception as exc:  # one bad fixer must not strand the rest
            results.append(
                FixResult(root, FixAction.SKIPPED, False, f"Fixer '{key}' failed: {exc}")
            )
    results.extend(_refresh_collection_fields(root, changed, dry_run=dry_run, state=state))
    results.extend(_refresh_mirrors(root, changed, dry_run=dry_run, state=state))
    return results


def _refresh_collection_fields(
    root: Path, changed: Iterable[Path], *, dry_run: bool, state: RunState | None
) -> list[FixResult]:
    """Derive the extent and summaries again for each collection whose items changed."""
    from portolan_cli.finalization import refresh_collection_fields

    collections = {
        collection for path in changed if (collection := _collection_dir_of(path, root)) is not None
    }
    results: list[FixResult] = []
    for collection in sorted(collections):
        target = collection / "collection.json"
        if dry_run:
            continue
        try:
            changed_file = refresh_collection_fields(root, collection, state=state)
        except (ValueError, OSError) as exc:
            results.append(
                FixResult(target, FixAction.SKIPPED, True, f"Cannot derive the extent: {exc}")
            )
            continue
        if changed_file:
            results.append(
                FixResult(target, FixAction.UPDATED, True, "Derived the extent from the items")
            )
    return results


def auto_fixer_keys(findings: Iterable[Any]) -> list[str]:
    """The distinct fixer keys the AUTO findings call for, in execution order."""
    wanted = set()
    for finding in findings:
        remediation = remediation_of(finding)
        if remediation.bucket is Bucket.AUTO and remediation.fixer in FIXERS:
            wanted.add(remediation.fixer)
    for composite, members in _COMPOSED_OF.items():
        if composite in wanted:
            wanted.difference_update(members)
    return [key for key in FIXERS if key in wanted]


def apply_fixers(
    root: Path,
    findings: Iterable[Any],
    *,
    dry_run: bool,
    skip: Sequence[str] | set[str] = (),
    state: RunState | None = None,
) -> FixerRun:
    """Run every fixer the AUTO findings call for, once each, over ``root``."""
    selected = [key for key in auto_fixer_keys(findings) if key not in skip]
    results: list[FixResult] = []
    applied: list[str] = []
    skip_reasons: dict[str, list[str]] = {}
    for key in selected:
        try:
            outcome = FIXERS[key](root, dry_run, state)
        except Exception as exc:  # one bad fixer must not strand the rest
            outcome = [FixResult(root, FixAction.SKIPPED, False, f"Fixer '{key}' failed: {exc}")]
        results.extend(outcome)
        if any(result.action in (FixAction.CREATED, FixAction.UPDATED) for result in outcome):
            applied.append(key)
            continue
        messages = list(dict.fromkeys(result.message for result in outcome if result.message))
        if messages:
            skip_reasons[key] = messages
    return FixerRun(
        report=FixReport(results=results),
        selected=selected,
        applied=applied,
        skip_reasons=skip_reasons,
    )
