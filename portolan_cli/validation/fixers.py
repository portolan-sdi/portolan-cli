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
freshness workflow. A defect a fixer cannot resolve without inventing data — a
bbox with nothing to recompute from — is reported ``SKIPPED`` with the reason,
never guessed at.
"""

from __future__ import annotations

import hashlib
import json
import posixpath
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

from rashid.catalog import CatalogGraph, Node, is_absolute_href
from rashid.rules._common import STRUCTURAL_RELS, links_of, roles_of

from portolan_cli.metadata.fix import FixAction, FixReport, FixResult
from portolan_cli.validation.remediation import Bucket, remediation_for

#: A fixer sweeps ``catalog_root``; ``dry_run`` means report but do not write.
Fixer = Callable[[Path, bool], list[FixResult]]

_JSON_TYPE = "application/json"
_GEOJSON_TYPE = "application/geo+json"
_STYLE_MEDIA_TYPE = "application/vnd.mapbox.style+json"
_PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"
_MIRROR_ROLE = "collection-mirror"
_MIRROR_FILENAME = "items.parquet"


# --------------------------------------------------------------------------
# shared plumbing
# --------------------------------------------------------------------------


def _graph(root: Path) -> CatalogGraph:
    return CatalogGraph.load(root)


def _write_node(node: Node, *, dry_run: bool) -> None:
    if dry_run:
        return
    node.abs_path.write_text(json.dumps(node.data, indent=2), encoding="utf-8")


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


def _fix_bbox(root: Path, dry_run: bool) -> list[FixResult]:
    """Recompute a collection's spatial extent from what it contains."""
    from portolan_cli.bbox import compute_bbox_union

    results: list[FixResult] = []
    graph = _graph(root)
    for node in graph.iter("catalog", "collection"):
        boxes = _extent_bboxes(node)
        if boxes is None or all(_looks_valid(box) for box in boxes):
            continue
        union = compute_bbox_union(_child_bboxes(node, graph)).bbox
        if union is None:
            results.append(
                _skipped(
                    node.abs_path,
                    "Cannot recompute the extent: no valid child bbox to derive it from; "
                    "re-run `portolan add` so the extent comes from the data",
                )
            )
            continue
        node.data["extent"]["spatial"]["bbox"] = [union]
        _write_node(node, dry_run=dry_run)
        results.append(_updated(node, "Recomputed the spatial extent from contained bboxes"))
    return results


# --------------------------------------------------------------------------
# assets — PTL-AST-001
# --------------------------------------------------------------------------


def _fix_assets(root: Path, dry_run: bool) -> list[FixResult]:
    """Backfill asset media type and roles from the extension registry (ADR-0055)."""
    from portolan_cli.preparation import _get_asset_role, _get_media_type

    results: list[FixResult] = []
    for node in _graph(root).iter("collection", "item"):
        changed = False
        for asset in _assets_of(node).values():
            href = asset.get("href")
            if not isinstance(href, str) or not href:
                continue
            suffix = Path(href)
            media_type = asset.get("type")
            if not isinstance(media_type, str) or not media_type.strip():
                asset["type"] = _get_media_type(suffix)
                changed = True
            if not roles_of(asset):
                asset["roles"] = [_get_asset_role(suffix)]
                changed = True
        if changed:
            _write_node(node, dry_run=dry_run)
            results.append(_updated(node, "Backfilled asset media types and roles"))
    return results


# --------------------------------------------------------------------------
# checksum — PTL-AST-003/004, PTL-DAT-001/002
# --------------------------------------------------------------------------


def _fix_checksums(root: Path, dry_run: bool) -> list[FixResult]:
    """Recompute file:size and the file:checksum multihash from the asset bytes."""
    from portolan_cli.sync.checksums import multihash_sha256

    results: list[FixResult] = []
    for node in _graph(root).iter("collection", "item"):
        changed = False
        for asset in _assets_of(node).values():
            path = _local_asset_path(node, asset)
            if path is None:
                continue
            payload = path.read_bytes()
            checksum = multihash_sha256(hashlib.sha256(payload).hexdigest())
            if asset.get("file:size") != len(payload):
                asset["file:size"] = len(payload)
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
    for child in graph.children_of(node):
        if child.kind != "item":
            continue
        for asset in _assets_of(child).values():
            media_type = asset.get("type")
            if isinstance(media_type, str) and media_type.strip().lower().startswith("image/tiff"):
                return True
    return False


def _generate_mirror(node: Node, *, dry_run: bool) -> FixResult:
    """Write items.parquet for a scene collection and register it."""
    from portolan_cli.stac_parquet import add_parquet_link_to_collection, generate_items_parquet

    collection_dir = node.abs_path.parent
    if dry_run:
        return _updated(node, "Would generate items.parquet and register the mirror")
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
        for field, value in wanted.items():
            if not node.data.get(field):
                node.data[field] = value
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


def _readme_gap(node: Node) -> bool:
    from portolan_cli.readme import README_LINK_REL

    if not (node.abs_path.parent / "README.md").exists():
        return True
    return not any(link.get("rel") == README_LINK_REL for link in links_of(node))


def _fix_readme(root: Path, dry_run: bool) -> list[FixResult]:
    """Scaffold README.md and its rel='describedby' link."""
    from portolan_cli.readme import ensure_readmes

    results = [
        _updated(node, "Scaffolded README.md and its rel='describedby' link")
        for node in _graph(root).iter("catalog", "collection")
        if _readme_gap(node)
    ]
    if results and not dry_run:
        ensure_readmes(root)
    return results


def _fix_agents(root: Path, dry_run: bool) -> list[FixResult]:
    """Scaffold AGENTS.md and its rel='agents' link (ADR-0052)."""
    from portolan_cli.metadata.fix import repair_agents_md

    return repair_agents_md(root, dry_run=dry_run)


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
    """Re-convert assets that are not the cloud-native format they claim to be."""
    from portolan_cli.convert import ConversionStatus
    from portolan_cli.scan.check import check_directory

    report = check_directory(root, fix=True, dry_run=dry_run, catalog_path=root)
    conversion = report.conversion_report
    if conversion is None:
        return []
    return [
        FixResult(
            file_path=result.source,
            action=(
                FixAction.UPDATED
                if result.status is ConversionStatus.SUCCESS
                else FixAction.SKIPPED
            ),
            success=result.status
            in (ConversionStatus.SUCCESS, ConversionStatus.SKIPPED, ConversionStatus.UNSUPPORTED),
            message=result.error or f"Conversion {result.status.value}",
        )
        for result in conversion.results
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


def auto_fixer_keys(findings: Iterable[Any]) -> list[str]:
    """The distinct fixer keys the AUTO findings call for, in execution order.

    Args:
        findings: rashid findings, any severity.

    Returns:
        Registry keys, deduplicated and ordered as :data:`FIXERS` is.
    """
    wanted = set()
    for finding in findings:
        remediation = remediation_for(finding.rule_id)
        if remediation.bucket is Bucket.AUTO and remediation.fixer in FIXERS:
            wanted.add(remediation.fixer)
    return [key for key in FIXERS if key in wanted]


def apply_fixers(
    root: Path,
    findings: Iterable[Any],
    *,
    dry_run: bool,
    skip: Sequence[str] | set[str] = (),
) -> FixReport:
    """Run every fixer the AUTO findings call for, once each, over ``root``.

    Args:
        root: Catalog root to sweep.
        findings: rashid findings from the pre-fix check.
        dry_run: Report what would change and write nothing.
        skip: Fixer keys the caller already covers, so no repair runs twice.
            ``check --fix`` passes ``convert`` when its geo-asset pass is in
            scope, since that pass is the same conversion sweep.

    Returns:
        A :class:`~portolan_cli.metadata.fix.FixReport` merging every result.
        A fixer that raises becomes one failed result rather than aborting the
        run: one broken repair must not strand the others.
    """
    results: list[FixResult] = []
    for key in auto_fixer_keys(findings):
        if key in skip:
            continue
        try:
            results.extend(FIXERS[key](root, dry_run))
        except Exception as exc:  # noqa: BLE001 - one bad fixer must not strand the rest
            results.append(
                FixResult(root, FixAction.SKIPPED, False, f"Fixer '{key}' failed: {exc}")
            )
    return FixReport(results=results)
