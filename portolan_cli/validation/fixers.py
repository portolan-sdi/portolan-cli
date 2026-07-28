"""The fixers `portolan check --fix` dispatches on.

rashid reports a defect; :mod:`portolan_cli.validation.remediation` says who
resolves it; this module is the *who* for the AUTO bucket. Every AUTO row names
a ``fixer`` key, and every key has an entry in :data:`FIXERS` — a completeness
gate in ``tests/unit/validation/test_fixers.py`` enforces the correspondence in
both directions, so a rule can never claim a fixer that does not exist.

The signature is deliberately narrow::

    Fixer = Callable[[Path, bool], list[FixResult]]  # (catalog_root, dry_run)

A fixer takes the catalog root and sweeps it whole. It is not handed the
findings that triggered it, because every repairer here is already a
whole-catalog sweep: the findings decide *whether* a fixer runs, never *where*
it looks. That keeps each fixer idempotent, keeps :func:`apply_fixers` free of
per-finding bookkeeping, and means running a fixer twice costs a re-scan rather
than a double edit.

``dry_run`` is a hard contract: a fixer must report what it would change and
write nothing. The tests snapshot the tree before and after to hold it.

Results are :class:`~portolan_cli.metadata.fix.FixResult` values merged into the
existing :class:`~portolan_cli.metadata.fix.FixReport`, so ``--fix`` has one
report shape whether the repair came from a fixer or from the metadata
freshness workflow. :func:`apply_fixers` wraps that report in a
:class:`FixerRun` that also says which fixers were selected, which actually
changed something, and what the rest reported — the caller cannot otherwise
tell a fixer that did nothing from one that never ran.

A defect a fixer cannot resolve without inventing data — a bbox with no
readable asset behind it, an asset whose extension the registry does not know —
is reported ``SKIPPED`` with the reason, never guessed at.
"""

from __future__ import annotations

import importlib
import json
import posixpath
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from rashid.catalog import CatalogGraph, Node, is_absolute_href
from rashid.rules._common import STRUCTURAL_RELS, links_of, roles_of

from portolan_cli.metadata.fix import FixAction, FixReport, FixResult
from portolan_cli.validation.remediation import Bucket, remediation_for

#: A fixer sweeps ``catalog_root``; ``dry_run`` means report but do not write.
Fixer = Callable[[Path, bool], list[FixResult]]


@dataclass(frozen=True)
class FixerRun:
    """What one ``apply_fixers`` sweep asked for, what it achieved, and why not.

    ``FixReport`` alone cannot answer "did the fixer the finding named actually
    do anything?" — a fixer that ran and skipped everything looks identical to
    one that was never selected. The caller needs the difference to tell an
    operator whether a surviving finding is a bug or a repair that honestly
    could not proceed.

    Attributes:
        report: Every :class:`~portolan_cli.metadata.fix.FixResult` produced.
        selected: Fixer keys the findings called for, after composite dedup and
            minus the caller's ``skip``, in :data:`FIXERS` (execution) order.
        applied: The subset of ``selected`` that produced at least one
            ``CREATED``/``UPDATED`` result, in the same order.
        skip_reasons: For each selected-but-not-applied key that said something,
            its deduplicated messages in first-seen order.
    """

    report: FixReport
    selected: list[str] = field(default_factory=list)
    applied: list[str] = field(default_factory=list)
    skip_reasons: dict[str, list[str]] = field(default_factory=dict)


_JSON_TYPE = "application/json"
_GEOJSON_TYPE = "application/geo+json"
_STYLE_MEDIA_TYPE = "application/vnd.mapbox.style+json"
_PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"
_MIRROR_ROLE = "collection-mirror"
_MIRROR_FILENAME = "items.parquet"
_COG_MEDIA_PREFIX = "image/tiff"
_COG_MEDIA_PROFILE = "profile=cloud-optimized"


# --------------------------------------------------------------------------
# shared plumbing
# --------------------------------------------------------------------------


def _graph(root: Path) -> CatalogGraph:
    return CatalogGraph.load(root)


def _write_node(node: Node, *, dry_run: bool) -> None:
    from portolan_cli.json_io import write_json_atomic

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


# --------------------------------------------------------------------------
# links — PTL-LNK-001..006
# --------------------------------------------------------------------------


def _relative_href(from_node: Node, to_path: PurePosixPath) -> str:
    """Href for ``to_path`` written relative to ``from_node``'s own directory."""
    rel = posixpath.relpath(str(to_path), str(from_node.path.parent))
    return rel if rel.startswith("../") else f"./{rel}"


def _expected_structural_links(node: Node, graph: CatalogGraph) -> list[tuple[str, Node]]:
    """The (rel, target) pairs the spec requires on ``node``, in link order."""
    expected: list[tuple[str, Node]] = []
    root = graph.root
    parent = graph.parent_of(node)
    if root is not None:
        expected.append(("root", root))
    if parent is not None:
        expected.append(("parent", parent))
        if node.kind == "item" and parent.kind == "collection":
            expected.append(("collection", parent))
    for child in graph.children_of(node):
        expected.append(("item" if child.kind == "item" else "child", child))
    return expected


def _reuse_link(
    existing: list[dict[str, Any]], node: Node, graph: CatalogGraph, rel: str, target: Node
) -> dict[str, Any] | None:
    """An existing link to carry forward, so titles and extras survive the repair.

    Matched by where the href *resolves*, not by its spelling: ``./a/b.json`` and
    ``a/b.json`` name the same file, and a hand-written title on either should
    outlive the repair. A ``child``/``item`` link that resolves nowhere has no
    counterpart to carry forward and is simply replaced.
    """
    same_rel = [link for link in existing if link.get("rel") == rel]
    if not same_rel:
        return None
    if rel not in ("child", "item"):
        return same_rel[0]
    for link in same_rel:
        href = link.get("href")
        if isinstance(href, str) and graph.resolve_link(node, href) is target:
            return link
    return None


def _rebuild_links(node: Node, graph: CatalogGraph) -> list[dict[str, Any]]:
    """The link block ``node`` should carry: structural first, everything else after."""
    existing = links_of(node)
    rebuilt: list[dict[str, Any]] = []
    for rel, target in _expected_structural_links(node, graph):
        href = _relative_href(node, target.path)
        link = dict(_reuse_link(existing, node, graph, rel, target) or {})
        link["rel"] = rel
        link["href"] = href
        link["type"] = _GEOJSON_TYPE if rel == "item" else _JSON_TYPE
        if rel in ("child", "item") and not link.get("title"):
            title = target.data.get("title") or target.id
            if isinstance(title, str) and title:
                link["title"] = title
        rebuilt.append(link)
    rebuilt.extend(link for link in existing if link.get("rel") not in STRUCTURAL_RELS + ("self",))
    return rebuilt


def _fix_links(root: Path, dry_run: bool) -> list[FixResult]:
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


# --------------------------------------------------------------------------
# bbox — PTL-BBX-001
# --------------------------------------------------------------------------


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
    from portolan_cli.bbox import is_valid_bbox, to_2d_bbox

    if not isinstance(box, list) or len(box) not in (4, 6):
        return False
    if any(isinstance(v, bool) or not isinstance(v, (int, float)) for v in box):
        return False
    return is_valid_bbox(to_2d_bbox(box))


def _child_bboxes(node: Node, graph: CatalogGraph) -> list[list[float]]:
    """Every bbox contained by ``node``, from items directly and collections' extents."""
    from portolan_cli.bbox import to_2d_bbox

    found: list[list[float]] = []
    for child in graph.children_of(node):
        candidates: list[Any] = (
            [child.data.get("bbox")] if child.kind == "item" else (_extent_bboxes(child) or [])
        )
        found.extend(to_2d_bbox(box) for box in candidates if _looks_valid(box))
    return found


#: Asset suffix -> the extractor whose metadata object ``_extract_bbox_wgs84``
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
    """The asset's own footprint in WGS84, or None when it cannot be read.

    Honest by construction: a remote href, an unrecognized suffix, an unreadable
    file, or a CRS that cannot be reprojected all yield None, so the caller
    skips. Never falls back to a default or a whole-world box — a wrong extent
    published as fact is worse than a reported gap.
    """
    path = _local_asset_path(node, asset)
    if path is None:
        return None
    entry = _BBOX_EXTRACTORS.get(path.suffix.lower())
    if entry is None:
        return None
    from portolan_cli.preparation import _extract_bbox_wgs84

    module_name, attribute = entry
    try:
        extract = getattr(importlib.import_module(module_name), attribute)
        return _extract_bbox_wgs84(extract(path))
    except Exception:  # noqa: BLE001 - any extractor failure is an honest skip
        return None


def _asset_bboxes(node: Node) -> list[list[float]]:
    """Every readable footprint among ``node``'s own local assets."""
    found = []
    for asset in _assets_of(node).values():
        box = _wgs84_bbox_from_asset(node, asset)
        if box is not None:
            found.append(box)
    return found


def _union_of(boxes: list[list[float]]) -> list[float] | None:
    from portolan_cli.bbox import compute_bbox_union

    return compute_bbox_union(boxes, wgs84_only=True).bbox if boxes else None


_BBOX_SKIP_MESSAGE = (
    "Cannot recompute the bbox: nothing readable to derive it from; "
    "re-run `portolan add` so the bbox comes from the data"
)


def _fix_item_bbox(node: Node, *, dry_run: bool) -> FixResult | None:
    """Recompute one item's bbox (and, if absent, its geometry) from its assets."""
    from portolan_cli.item import _bbox_to_polygon

    declared = "bbox" in node.data
    if declared and _looks_valid(node.data["bbox"]):
        return None
    union = _union_of(_asset_bboxes(node))
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
    # Items were repaired first and graph nodes are shared objects, so a bbox
    # written above is already visible here without reloading the tree.
    union = _union_of(_child_bboxes(node, graph) or _asset_bboxes(node))
    if union is None:
        return _skipped(node.abs_path, _BBOX_SKIP_MESSAGE)
    node.data["extent"]["spatial"]["bbox"] = [union]
    _write_node(node, dry_run=dry_run)
    return _updated(node, "Recomputed the spatial extent from contained bboxes")


def _fix_bbox(root: Path, dry_run: bool) -> list[FixResult]:
    """Recompute broken bboxes: items from their assets, containers from children.

    Items go first so a container can union the values they just gained. Asset
    bytes are read only where a bbox is actually missing or invalid, so a clean
    catalog costs nothing beyond the JSON walk.
    """
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


# --------------------------------------------------------------------------
# assets — PTL-AST-001
# --------------------------------------------------------------------------


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


def _fix_assets(root: Path, dry_run: bool) -> list[FixResult]:
    """Backfill asset media type and roles from the extension registry (ADR-0055)."""
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


# --------------------------------------------------------------------------
# checksum — PTL-AST-003/004, PTL-DAT-001/002
# --------------------------------------------------------------------------


def _fix_checksums(root: Path, dry_run: bool) -> list[FixResult]:
    """Recompute file:size and the file:checksum multihash from the asset bytes.

    Streams through ``sync.checksums.compute_checksum`` rather than reading each
    asset whole: a COG is routinely gigabytes, and the whole point of this fixer
    is that it runs over every asset in the catalog.
    """
    from portolan_cli.sync.checksums import compute_checksum, multihash_sha256

    results: list[FixResult] = []
    for node in _graph(root).iter("collection", "item"):
        changed = False
        for asset in _assets_of(node).values():
            path = _local_asset_path(node, asset)
            if path is None:
                continue
            size = path.stat().st_size
            checksum = multihash_sha256(compute_checksum(path))
            if asset.get("file:size") != size:
                asset["file:size"] = size
                changed = True
            if asset.get("file:checksum") != checksum:
                asset["file:checksum"] = checksum
                changed = True
        if changed:
            _write_node(node, dry_run=dry_run)
            results.append(_updated(node, "Recomputed file:size and file:checksum from the bytes"))
    return results


# --------------------------------------------------------------------------
# styles — PTL-VIZ-005
# --------------------------------------------------------------------------


def _provides_pmtiles(node: Node) -> bool:
    for asset in _assets_of(node).values():
        href = asset.get("href")
        if asset.get("type") == "application/vnd.pmtiles" or (
            isinstance(href, str) and href.endswith(".pmtiles")
        ):
            return True
    return any(link.get("rel") == "pmtiles" for link in links_of(node))


def _fix_styles(root: Path, dry_run: bool) -> list[FixResult]:
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


# --------------------------------------------------------------------------
# item_mirror — PTL-MIR-001/002
# --------------------------------------------------------------------------


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
    """Whether ``node`` has scene items carrying COGs, exactly as rashid scopes it.

    A plain ``image/tiff`` is a GeoTIFF, not a COG, and PTL-MIR-001 does not
    fire on it — the media type must also carry ``profile=cloud-optimized``.
    Replicated rather than imported because rashid exposes no public predicate
    yet — https://github.com/portolan-sdi/rashid/issues/57 tracks the export.
    """
    for child in graph.children_of(node):
        if child.kind != "item":
            continue
        for asset in _assets_of(child).values():
            media_type = asset.get("type")
            if not isinstance(media_type, str):
                continue
            normalized = media_type.strip().lower()
            if normalized.startswith(_COG_MEDIA_PREFIX) and _COG_MEDIA_PROFILE in normalized:
                return True
    return False


def _generate_mirror(node: Node, *, dry_run: bool) -> FixResult:
    """Write items.parquet for a scene collection and register it."""
    from portolan_cli.stac_parquet import add_parquet_link_to_collection, generate_items_parquet

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
        generate_items_parquet(collection_dir)
        add_parquet_link_to_collection(collection_dir)
    except (ImportError, ValueError, OSError) as exc:
        return FixResult(
            node.abs_path, FixAction.SKIPPED, True, f"Cannot generate the mirror: {exc}"
        )
    return FixResult(node.abs_path, FixAction.CREATED, True, "Generated the items.parquet mirror")


def _fix_item_mirror(root: Path, dry_run: bool) -> list[FixResult]:
    """Publish and register the stac-geoparquet item mirror (ADR-0049)."""
    results: list[FixResult] = []
    graph = _graph(root)
    for node in graph.iter("collection"):
        if _mirror_assets(node):
            if _register_mirror(node):
                _write_node(node, dry_run=dry_run)
                results.append(_updated(node, "Registered items.parquet as the collection mirror"))
            continue
        if _has_cog_items(node, graph):
            results.append(_generate_mirror(node, dry_run=dry_run))
    return results


# --------------------------------------------------------------------------
# partition — PTL-PRT-001
# --------------------------------------------------------------------------


def _is_partitioned(node: Node) -> bool:
    from portolan_cli.constants import PARTITION_EXTENSION_URI

    if any(key.startswith("partition:") for key in node.data):
        return True
    extensions = node.data.get("stac_extensions")
    return isinstance(extensions, list) and PARTITION_EXTENSION_URI in extensions


def _fix_partition(root: Path, dry_run: bool) -> list[FixResult]:
    """Populate the partition: block from the Hive layout on disk (ADR-0042)."""
    from portolan_cli.constants import PARTITION_EXTENSION_URI
    from portolan_cli.partitioning import build_glob_pattern, detect_partitioning

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


# --------------------------------------------------------------------------
# conformance, required files, and the existing repairers
# --------------------------------------------------------------------------


def _fix_schema_uri(root: Path, dry_run: bool) -> list[FixResult]:
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
_Gap = Callable[[Path, dict[str, Any]], bool]


def _readme_gap(stac_path: Path, data: dict[str, Any]) -> bool:
    """True when README.md is absent or its rel='describedby' link is unfit."""
    from portolan_cli.readme import README_FILENAME, readme_link_gap

    return not (stac_path.parent / README_FILENAME).exists() or readme_link_gap(stac_path, data)


def _agents_gap(stac_path: Path, data: dict[str, Any]) -> bool:
    """True when AGENTS.md is absent or its rel='agents' link is unfit."""
    from portolan_cli.agents_md import AGENTS_MD_FILENAME, agents_link_gap

    return not (stac_path.parent / AGENTS_MD_FILENAME).exists() or agents_link_gap(stac_path, data)


def _fix_markdown_requirement(
    root: Path,
    dry_run: bool,
    *,
    gap: _Gap,
    repair: Callable[[Path], Any],
    message: str,
) -> list[FixResult]:
    """Scaffold a required sibling markdown file and its link, and say so honestly.

    ``gap`` mirrors rashid's four-case link check (wrong ``type``, absolute
    href, unresolvable href, no link at all), which is wider than what the
    scaffolders normalize. When a repair runs and the gap survives it, the
    result is SKIPPED rather than UPDATED: the finding will outlive the
    re-check, and ``--fix`` must not claim work the next check contradicts.
    """
    gapped = [
        node for node in _graph(root).iter("catalog", "collection") if gap(node.abs_path, node.data)
    ]
    if not gapped or dry_run:
        return [_updated(node, message) for node in gapped]
    repair(root)
    results = []
    for node in gapped:
        data = _reload(node.abs_path)
        if data is None or gap(node.abs_path, data):
            results.append(
                _skipped(
                    node.abs_path, f"{message}, but the link still does not conform; fix it by hand"
                )
            )
        else:
            results.append(_updated(node, message))
    return results


def _fix_readme(root: Path, dry_run: bool) -> list[FixResult]:
    """Scaffold README.md and its rel='describedby' link."""
    from portolan_cli.readme import ensure_readmes

    return _fix_markdown_requirement(
        root,
        dry_run,
        gap=_readme_gap,
        repair=ensure_readmes,
        message="Scaffolded README.md and its rel='describedby' link",
    )


def _fix_agents(root: Path, dry_run: bool) -> list[FixResult]:
    """Scaffold AGENTS.md and its rel='agents' link (ADR-0052)."""
    from portolan_cli.agents_md import ensure_agents_md_tree

    return _fix_markdown_requirement(
        root,
        dry_run,
        gap=_agents_gap,
        repair=ensure_agents_md_tree,
        message="Scaffolded AGENTS.md and its rel='agents' link",
    )


def _fix_required_files(root: Path, dry_run: bool) -> list[FixResult]:
    """Every catalog and collection directory carries README.md and AGENTS.md."""
    return [*_fix_agents(root, dry_run), *_fix_readme(root, dry_run)]


def _fix_titles(root: Path, dry_run: bool) -> list[FixResult]:
    """Derive titles and descriptions, and backfill child/item link titles (ADR-0053)."""
    from portolan_cli.metadata.fix import repair_titles_and_links

    return repair_titles_and_links(root, dry_run=dry_run)


def _fix_pmtiles(root: Path, dry_run: bool) -> list[FixResult]:
    """Register the rel='pmtiles' web-map-links link on PMTiles collections."""
    from portolan_cli.metadata.fix import repair_pmtiles_links

    return repair_pmtiles_links(root, dry_run=dry_run)


def _fix_convert(root: Path, dry_run: bool) -> list[FixResult]:
    """Registered but deliberately inert: conversion belongs to the geo-asset pass.

    PTL-DAT-003 names this key, so the registry must carry it. Running a
    conversion sweep from here was wrong twice over: ``--metadata --fix``
    silently rewrote asset bytes the operator never asked about, and the sweep
    ran without the worker and force settings the geo-asset pass is configured
    with. Reporting the honest hand-off costs the operator one flag and costs
    nobody their data.
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
    "bbox": _fix_bbox,
    "partition": _fix_partition,
    "item_mirror": _fix_item_mirror,
    "styles": _fix_styles,
    "pmtiles": _fix_pmtiles,
    "checksum": _fix_checksums,
    "convert": _fix_convert,
}


#: Fixers that already run other registered fixers. The registry keeps every key
#: (a rule may name a member on its own), but selecting a composite must not also
#: select its members, or PTL-FIL-001/-002/-003 firing together would run the
#: README and AGENTS.md repairs twice over the same tree.
_COMPOSED_OF: dict[str, tuple[str, ...]] = {"required_files": ("agents", "readme")}


def auto_fixer_keys(findings: Iterable[Any]) -> list[str]:
    """The distinct fixer keys the AUTO findings call for, in execution order.

    Args:
        findings: rashid findings, any severity.

    Returns:
        Registry keys, deduplicated (including across :data:`_COMPOSED_OF`) and
        ordered as :data:`FIXERS` is.
    """
    wanted = set()
    for finding in findings:
        remediation = remediation_for(finding.rule_id)
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
) -> FixerRun:
    """Run every fixer the AUTO findings call for, once each, over ``root``.

    Args:
        root: Catalog root to sweep.
        findings: rashid findings from the pre-fix check.
        dry_run: Report what would change and write nothing.
        skip: Fixer keys the caller already covers, so no repair runs twice.
            ``check --fix`` passes ``convert`` when its geo-asset pass is in
            scope, since that pass is the same conversion sweep.

    Returns:
        A :class:`FixerRun`: the merged report plus which fixers were selected,
        which actually changed something, and the reasons the rest gave. A fixer
        that raises becomes one failed result rather than aborting the run — one
        broken repair must not strand the others — and counts as selected but
        not applied.
    """
    selected = [key for key in auto_fixer_keys(findings) if key not in skip]
    results: list[FixResult] = []
    applied: list[str] = []
    skip_reasons: dict[str, list[str]] = {}
    for key in selected:
        try:
            outcome = FIXERS[key](root, dry_run)
        except Exception as exc:  # noqa: BLE001 - one bad fixer must not strand the rest
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
