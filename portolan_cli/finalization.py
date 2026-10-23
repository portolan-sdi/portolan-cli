"""Batch finalization for the add pipeline: STAC-write + backend coordination.

Extracted from ``add.py`` (issue #624). This module owns the second phase of
``add``: taking the per-item :class:`PreparedItem` objects produced by
``preparation.prepare_item`` and writing the *collection-level* artifacts exactly
once per collection — ``collection.json`` (assets, summaries, extents,
extensions), ``versions.json``, and the parent ``catalog.json`` links — then
routing the version snapshot to the active backend (file or plugin).

Batching all writes by collection is the key optimization behind Issue #281:
one write per collection instead of O(n) per item. A per-item preparation
failure therefore leaves no partial version entry (see ``.claude/rules``:
"add must be atomic").

the CLI stays a thin wrapper; ``add.py`` orchestrates on top of the
routines here. This module deliberately imports nothing from ``add`` so the
dependency edge is one-directional (add -> finalization). Imports of
``catalog`` / ``backends`` / ``version_ops`` are kept **function-local** on
purpose: ``catalog`` re-exports ``add_files`` at import time, so a module-level
``finalization -> catalog`` edge would reintroduce a load-time cycle.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, Any

import pystac
from pystac.layout import AsIsLayoutStrategy

from portolan_cli.config import load_merged_metadata, load_own_metadata
from portolan_cli.hookspecs import EnrichContext
from portolan_cli.humanize import humanize_slug
from portolan_cli.json_io import write_json_atomic
from portolan_cli.metadata import extract_geoparquet_metadata
from portolan_cli.metadata.geoparquet import GeoParquetMetadata
from portolan_cli.query import ItemInfo
from portolan_cli.stac import (
    DEFAULT_LICENSE,
    PROVISIONAL_END_DATETIME,
    PROVISIONAL_START_DATETIME,
    CollectionMember,
    MergeStrategy,
    add_asset_to_collection,
    add_collection_properties_from_metadata,
    add_item_to_collection,
    add_partition_metadata_to_collection,
    add_table_extension,
    aggregate_table_metadata,
    apply_collection_fields,
    apply_collection_fields_to_dict,
    apply_human_license,
    apply_human_providers,
    apply_human_titles,
    apply_provenance,
    create_collection,
    declare_file_extension,
    derive_collection_fields,
    update_catalog_provenance,
)
from portolan_cli.stac_enrich import enrich_collection_file, enrich_item_file
from portolan_cli.stac_links import iter_links
from portolan_cli.utils import href_root, relative_href
from portolan_cli.versions import (
    Asset,
    VersionsFile,
    _increment_version,
    add_version,
    read_versions,
    versions_asset_key,
    write_versions,
)

if TYPE_CHECKING:
    from portolan_cli.plugins.formats import FormatContext
    from portolan_cli.preparation import PreparedItem

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Collection link management
# ─────────────────────────────────────────────────────────────────────────────


def _deduplicate_collection_item_links(collection: pystac.Collection) -> None:
    """De-duplicate item links in a PySTAC collection.

    PySTAC adds duplicate links when the same item is added multiple times.
    This modifies collection.links in place.
    """
    seen_item_ids: set[str] = set()
    unique_links: list[pystac.Link] = []
    for link in collection.links:
        if link.rel == "item":
            # For item links, de-duplicate by target item's ID
            target = link.target
            if isinstance(target, pystac.Item):
                item_id_key = target.id
            else:
                # If target is a string (href), use it directly
                item_id_key = str(target) if target else ""
            if item_id_key in seen_item_ids:
                continue
            seen_item_ids.add(item_id_key)
        unique_links.append(link)
    collection.links = unique_links


def relative_root_href(catalog_root: PurePath, collection_dir: PurePath) -> str:
    """The href from ``collection_dir`` to the root ``catalog.json``, in POSIX.

    Args:
        catalog_root: Directory holding the root ``catalog.json``.
        collection_dir: Directory the link will live in.

    Returns:
        A relative POSIX href, e.g. ``../catalog.json``.
    """
    return relative_href(collection_dir, PurePath(catalog_root) / "catalog.json")


def _set_structural_link(
    links: list[dict[str, Any]],
    rel: str,
    href: str,
    media_type: str = "application/json",
) -> None:
    """Point ``rel`` at ``href``, adding the link when it is absent.

    Repointing matters as much as adding. PySTAC writes a ``parent`` link of its
    own, so a generator that only fills in missing links leaves PySTAC's answer
    standing, which is how the wrong parent survived to disk (issue #711).
    """
    for link in links:
        if link.get("rel") == rel:
            # PySTAC stamps the owning object's title on the links it writes, so
            # a title that survived a repoint would now name the old target.
            # Titles on child/item links are backfilled from the target by
            # ensure_link_titles; these structural rels are not, so dropping is
            # the honest option.
            if link.get("href") != href:
                link.pop("title", None)
            link["href"] = href
            link["type"] = media_type
            return
    links.append({"rel": rel, "href": href, "type": media_type})


def _fix_collection_links(
    collection_json_path: Path,
    catalog_root: Path,
    collection_dir: Path,
) -> None:
    """Fix root/parent links and deduplicate item links in collection JSON.

    PySTAC sets root to self by default, so both structural links need
    repointing: ``root`` at the catalog root, and ``parent`` at whatever catalog
    contains this collection. Those two coincide only for a single-level id. For
    a nested id the container is the intermediate ``catalog.json`` that
    ``create_intermediate_catalogs`` writes one level up, and reusing the root
    href walked straight past it (rashid PTL-LNK-006, issue #711).

    Also deduplicates item links that can occur when add is called
    multiple times on the same collection.
    """
    if not collection_json_path.exists():
        return

    collection_data = json.loads(collection_json_path.read_text(encoding="utf-8"))
    links: list[dict[str, Any]] = collection_data.setdefault("links", [])

    _set_structural_link(links, "root", relative_root_href(catalog_root, collection_dir))
    _set_structural_link(
        links, "parent", relative_href(collection_dir, collection_dir.parent / "catalog.json")
    )

    # Deduplicate item links (can occur when add is called multiple times)
    seen_item_hrefs: set[str] = set()
    deduped_links: list[dict[str, Any]] = []
    for link in collection_data.get("links", []):
        if link.get("rel") == "item":
            href = link.get("href", "")
            if href in seen_item_hrefs:
                continue
            seen_item_hrefs.add(href)
        deduped_links.append(link)
    collection_data["links"] = deduped_links

    write_json_atomic(collection_json_path, collection_data)


def _fix_item_links(
    collection_json_path: Path,
    catalog_root: Path,
    collection_dir: Path,
) -> None:
    """Repoint the structural links of every item this collection owns.

    ``Collection.save`` serializes its items, but the collection is loaded
    standalone and never attached to a parent ``pystac.Catalog``, so PySTAC
    resolves each item's root to the collection itself and writes
    ``root -> collection.json``. The spec wants the catalog root there
    (core.md:262-264, rashid PTL-LNK-006), and until now only ``check --fix``
    repaired it — generation and repair disagreed.

    ``parent`` and ``collection`` both point at the owning collection, which is
    what PySTAC already writes; they are set here so a hand-edited or partially
    written item ends up complete.

    Args:
        collection_json_path: The saved ``collection.json``, read for item links.
        catalog_root: Directory holding the root ``catalog.json``.
        collection_dir: Directory holding ``collection.json``.
    """
    if not collection_json_path.exists():
        return

    collection_data = json.loads(collection_json_path.read_text(encoding="utf-8"))
    for _link, _href, linked_path in iter_links(collection_data, collection_dir, ("item",)):
        item_path = linked_path.resolve()
        if not item_path.exists():
            continue

        item_data = json.loads(item_path.read_text(encoding="utf-8"))
        item_dir = item_path.parent
        item_links: list[dict[str, Any]] = item_data.setdefault("links", [])
        collection_href = relative_href(item_dir, collection_dir / "collection.json")

        _set_structural_link(
            item_links, "root", relative_href(item_dir, catalog_root / "catalog.json")
        )
        _set_structural_link(item_links, "parent", collection_href)
        _set_structural_link(item_links, "collection", collection_href)
        write_json_atomic(item_path, item_data)


def _update_catalog_links(catalog_root: Path, collection_id: str) -> None:
    """Ensure the catalog tree has a child link down to this collection.

    ``update_catalog_links_for_nested`` handles both shapes: a flat id gets one
    child link on the root, a nested id gets one at every level. It edits only
    the ``links`` arrays it must, which is why the flat case routes here too.

    The flat case used to load the root with PySTAC and call
    ``save(SELF_CONTAINED)``. That walks and re-serializes every descendant, and
    ``normalize_hrefs`` lays children out by ``id`` — so an intermediate catalog,
    whose id is its POSIX path, was relocated from ``env/air/`` to
    ``env/env/air/`` and its child link followed. Adding a flat collection
    corrupted the nested half of the catalog (issue #711).

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Collection identifier (may be nested like "climate/hittekaart").
    """
    from portolan_cli.catalog import update_catalog_links_for_nested

    update_catalog_links_for_nested(catalog_root, collection_id)


def _save_collection_with_links(
    collection: pystac.Collection,
    collection_dir: Path,
    catalog_root: Path,
    collection_id: str,
) -> None:
    """Save collection and fix links.

    Args:
        collection: PySTAC collection to save.
        collection_dir: Collection directory path.
        catalog_root: Catalog root path.
        collection_id: Collection identifier.
    """
    _deduplicate_collection_item_links(collection)
    collection.set_self_href(str(collection_dir / "collection.json"))
    collection.normalize_hrefs(href_root(collection_dir), strategy=AsIsLayoutStrategy())
    collection.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)

    collection_json_path = collection_dir / "collection.json"
    _fix_collection_links(collection_json_path, catalog_root, collection_dir)
    _fix_item_links(collection_json_path, catalog_root, collection_dir)
    _update_catalog_links(catalog_root, collection_id)

    # Scaffold AGENTS.md and add the rel="agents" link (rashid PTL-FIL-002).
    # Idempotent and merge-safe: never overwrites an existing AGENTS.md, only
    # adds the link when absent, so re-running `add` preserves human edits.
    from portolan_cli.agents_md import ensure_agents_md

    ensure_agents_md(collection_json_path)


# ─────────────────────────────────────────────────────────────────────────────
# Collection members: the input of derive_collection_fields
# ─────────────────────────────────────────────────────────────────────────────

# Roles of collection-level assets that describe the collection itself, not data.
_NON_MEMBER_ROLES = frozenset({"collection-mirror", "stac-items", "source"})


def _item_member(item: pystac.Item) -> CollectionMember:
    """The bbox, times, and properties of one item."""
    props = item.properties
    provisional = (
        props.get("start_datetime") == PROVISIONAL_START_DATETIME
        and props.get("end_datetime") == PROVISIONAL_END_DATETIME
    )
    start = end = item.datetime
    if start is None and not provisional:
        start = item.common_metadata.start_datetime
        end = item.common_metadata.end_datetime
    return CollectionMember(
        bbox=list(item.bbox) if item.bbox is not None else None,
        start=start,
        end=end,
        properties=props,
    )


def _asset_member_path(collection_dir: Path, asset: pystac.Asset) -> Path | None:
    """The local file a collection-level data asset names, or None."""
    roles = set(asset.roles or [])
    if "data" not in roles or roles & _NON_MEMBER_ROLES:
        return None
    href = asset.get_absolute_href() or asset.href
    if "://" in href or "*" in href:
        return None
    path = Path(href)
    return path if path.is_absolute() else collection_dir / href.removeprefix("./")


def _read_wgs84_bbox(path: Path, catalog_root: Path | None) -> list[float] | None:
    """The WGS84 footprint of a data file, measured the way ``add`` measures it.

    None when no handler reads the file, so a table or a remote asset adds no
    footprint, and the extent is never invented.
    """
    from portolan_cli.plugins.formats import handler_for
    from portolan_cli.preparation import _extract_bbox_wgs84

    try:
        handler = handler_for(path, catalog_root)
        if handler is None:
            return None
        metadata = handler.read_metadata(path)
        if not getattr(metadata, "bbox", None):
            return None
        return _extract_bbox_wgs84(metadata, path)  # type: ignore[arg-type]
    except Exception:  # noqa: BLE001 - an unreadable file contributes no footprint
        logger.debug("Cannot read the footprint of %s", path, exc_info=True)
        return None


def _linked_items(collection: pystac.Collection) -> list[pystac.Item]:
    """Every item the collection links, skipping a link whose file is gone.

    ``rm`` deletes an item directory but leaves its link, so one dead link must
    not stop the derivation of the rest.
    """
    try:
        return list(collection.get_items(recursive=True))
    except Exception:  # noqa: BLE001 - fall back to resolving each link alone
        logger.debug("Collection '%s' links an unreadable item", collection.id, exc_info=True)
    items: list[pystac.Item] = []
    for link in collection.get_links(pystac.RelType.ITEM):
        try:
            target = link.resolve_stac_object(root=collection.get_root()).target
        except Exception:  # noqa: BLE001 - a dead link contributes no member
            logger.debug("Skipping the dead item link %s", link.href)
            continue
        if isinstance(target, pystac.Item):
            items.append(target)
    return items


def collection_members(
    collection: pystac.Collection,
    collection_dir: Path,
    *,
    catalog_root: Path | None = None,
    known_bboxes: dict[str, list[float]] | None = None,
) -> list[CollectionMember]:
    """Every item and collection-level data asset of a collection, from disk.

    An item contributes its bbox, times, and properties. A collection-level
    data asset contributes its footprint: the bbox ``add`` measured in this
    run when ``known_bboxes`` has it, else the bbox read from the file again.
    The stored extent is never an input (issue #801).

    Args:
        collection: The collection, with its items linked.
        collection_dir: Directory holding collection.json.
        catalog_root: Catalog root, for the format handler pins.
        known_bboxes: WGS84 bbox per resolved asset path, measured in this run.

    Returns:
        One member per item and per collection-level data asset.
    """
    known = known_bboxes or {}
    members = [_item_member(item) for item in _linked_items(collection)]
    for asset in collection.assets.values():
        path = _asset_member_path(collection_dir, asset)
        if path is None:
            continue
        bbox = known.get(str(path.resolve()))
        if bbox is None:
            bbox = _read_wgs84_bbox(path, catalog_root)
        members.append(CollectionMember(bbox=bbox))
    return members


def derive_and_apply_fields(
    collection: pystac.Collection,
    collection_dir: Path,
    *,
    catalog_root: Path | None = None,
    known_bboxes: dict[str, list[float]] | None = None,
) -> None:
    """Replace the derived fields of an in-memory collection from its members."""
    members = collection_members(
        collection, collection_dir, catalog_root=catalog_root, known_bboxes=known_bboxes
    )
    apply_collection_fields(
        collection, derive_collection_fields(members, load_own_metadata(collection_dir))
    )


def refresh_collection_fields(catalog_root: Path, collection_dir: Path) -> bool:
    """Derive the fields of a saved collection again, after its members changed.

    ``rm`` and ``check --fix --metadata`` change the members without passing
    through ``finalize_items``. They call this, so the extent and summaries
    follow the members that remain. The file is edited as JSON, so fields an
    enrichment hook wrote survive.

    Args:
        catalog_root: Root directory of the catalog.
        collection_dir: Directory holding collection.json.

    Returns:
        True when collection.json changed.
    """
    path = collection_dir / "collection.json"
    if not path.is_file():
        return False
    try:
        collection = pystac.Collection.from_file(str(path))
    except (pystac.STACError, pystac.STACTypeError, ValueError, OSError):
        # Not a collection pystac can read: nothing to derive, and check
        # reports the file itself.
        logger.warning("Cannot read %s; its extent is not derived", path)
        return False
    members = collection_members(collection, collection_dir, catalog_root=catalog_root)
    fields = derive_collection_fields(members, load_own_metadata(collection_dir))
    data = json.loads(path.read_text(encoding="utf-8"))
    before = json.dumps(data, sort_keys=True)
    apply_collection_fields_to_dict(data, fields)
    if json.dumps(data, sort_keys=True) == before:
        return False
    write_json_atomic(path, data)
    return True


def _batch_bboxes(items: list[PreparedItem]) -> dict[str, list[float]]:
    """The bbox ``add`` measured for each collection-level data file of the batch.

    The scan of a flat collection also lists the other files beside the one
    added, so only the files of the item's own stem take its bbox. Any other
    file is read from disk.
    """
    known: dict[str, list[float]] = {}
    for p in items:
        if not p.is_collection_level_asset:
            continue
        for file_path, _checksum, _size in p.asset_files.values():
            if file_path.stem == p.item_id:
                known[str(file_path.resolve())] = list(p.bbox)
    return known


# ─────────────────────────────────────────────────────────────────────────────
# Collection assembly helpers
# ─────────────────────────────────────────────────────────────────────────────


def _get_or_create_collection(
    catalog_root: Path,
    collection_id: str,
    initial_bbox: list[float],
) -> pystac.Collection:
    """Load existing collection or create new one.

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Collection identifier (may be nested path like "climate/hittekaart").
        initial_bbox: Initial bounding box for new collections.

    Returns:
        pystac.Collection object.
    """
    # STAC at root level, handle nested paths
    collection_path = catalog_root / Path(*collection_id.split("/")) / "collection.json"

    if collection_path.exists():
        return pystac.Collection.from_file(str(collection_path))

    # Create new collection. Issue #502: derive a human-readable title from
    # the collection id and default the description to it (no "Collection:
    # <slug>" placeholder). create_collection fills both in when omitted.
    title = humanize_slug(collection_id)
    return create_collection(
        collection_id=collection_id,
        description=title,
        title=title,
        bbox=initial_bbox,
    )


def apply_human_metadata(
    collection: pystac.Collection,
    collection_dir: Path,
    catalog_root: Path,
) -> None:
    """Write human-authored metadata onto a collection, own value over inherited.

    A metadata.yaml the collection owns is authoritative. Portolan re-applies its
    title, description, license, and providers on every add. A metadata.yaml an
    ancestor owns only fills a title or a license the collection still lacks. So a
    re-add never replaces a title or a license a maintainer set on this
    collection, and a catalog title never becomes a collection title (issue #755).

    Providers differ. A ``providers`` array in any metadata.yaml, own or
    inherited, rebuilds the array on every add, because the human file is the
    override (issue #813). An own ``contact`` counts as an own providers
    declaration too: it seeds the host provider. Either one replaces a providers
    array a maintainer hand-edited into collection.json.

    Args:
        collection: The collection to update in place.
        collection_dir: The collection's directory under the catalog root.
        catalog_root: Root directory of the catalog.
    """
    own = load_own_metadata(collection_dir)
    merged = load_merged_metadata(collection_dir, catalog_root)

    # Title and description: only the collection's own declaration applies. An
    # ancestor title names the catalog, not this collection, so it never inherits.
    apply_human_titles(collection, own)

    # License: the own declaration wins. An inherited license fills only a
    # collection that still carries the placeholder default license.
    if "license" in own or collection.license == DEFAULT_LICENSE:
        apply_human_license(collection, merged)
    else:
        # Preserve the existing license, but let the collection's own license_url
        # still resolve its link.
        apply_human_license(collection, own)

    # Providers: a providers array in any metadata.yaml, own or inherited, or an
    # own contact rebuilds the array, because the human file is the override
    # (issue #813). An inherited contact alone only seeds a collection that
    # declares no providers yet.
    if "providers" in merged or "contact" in own or not collection.providers:
        apply_human_providers(collection, merged)

    # Provenance derives from the providers now on the collection and the source
    # link the merged metadata declares.
    apply_provenance(collection, merged)


# The collection.json keys apply_human_metadata writes.
_HUMAN_METADATA_KEYS = ("title", "description", "license", "providers", "links", "updated")


def apply_human_metadata_to_dict(
    data: dict[str, Any], collection_dir: Path, catalog_root: Path
) -> None:
    """Apply :func:`apply_human_metadata` to a parsed ``collection.json`` in place.

    The tabular writer keeps collection.json as JSON. It calls this so an
    ``add --force`` applies metadata.yaml to a tabular collection the way it
    does to a spatial one (issue #813).

    Args:
        data: Parsed ``collection.json``, mutated in place.
        collection_dir: The collection's directory under the catalog root.
        catalog_root: Root directory of the catalog.
    """
    collection = pystac.Collection.from_dict(data, migrate=False)
    apply_human_metadata(collection, collection_dir, catalog_root)
    applied = collection.to_dict(include_self_link=False, transform_hrefs=False)
    for key in _HUMAN_METADATA_KEYS:
        if key in applied:
            data[key] = applied[key]
        else:
            data.pop(key, None)


def _preflight_asset_keys(
    catalog_root: Path,
    by_collection: dict[str, list[PreparedItem]],
    merge_strategy: MergeStrategy,
) -> None:
    """Refuse the batch before any write when an asset key collides (Issue #933).

    ``finalize_items`` writes one collection at a time. A collision in a later
    collection would otherwise leave the earlier collections written. This
    check merges each collection-level asset into an in-memory copy of its
    collection and discards the copy.

    Raises:
        AssetKeyCollisionError: A key holds a different file of another media
            type.
    """
    for collection_id, items in by_collection.items():
        level_items = [p for p in items if p.is_collection_level_asset and p.stac_assets]
        if not level_items:
            continue
        collection = _get_or_create_collection(catalog_root, collection_id, level_items[0].bbox)
        for p in level_items:
            for asset_key, asset in (p.stac_assets or {}).items():
                add_asset_to_collection(
                    collection, asset_key, asset.clone(), merge_strategy=merge_strategy
                )


def _add_prepared_items_to_collection(
    collection: pystac.Collection,
    items: list[PreparedItem],
    merge_strategy: MergeStrategy = MergeStrategy.SMART,
) -> None:
    """Add prepared items or collection-level assets to a collection.

    Collection-level vector assets go directly in collection.assets.
    Item-level assets get linked via add_item_to_collection.

    Args:
        collection: The pystac Collection to add to.
        items: List of PreparedItem objects.
        merge_strategy: How to merge auto-detected metadata with existing values.
    """
    for p in items:
        if p.is_collection_level_asset and p.stac_assets is not None:
            # Collection-level asset: add directly to collection.assets
            for asset_key, asset in p.stac_assets.items():
                add_asset_to_collection(collection, asset_key, asset, merge_strategy=merge_strategy)
            # Add format-specific properties (proj:code, pmtiles:*, flatgeobuf:*)
            if p.metadata is not None:
                add_collection_properties_from_metadata(
                    collection, p.metadata, asset_keys=p.stac_assets.keys()
                )
        elif p.stac_item is not None:
            # Item-level: add item link to collection
            add_item_to_collection(collection, p.stac_item)


def _collect_parquet_metadata_from_disk(
    collection_dir: Path,
    collection: pystac.Collection,
) -> list[GeoParquetMetadata]:
    """Scan collection directory and extract metadata from tracked parquet assets.

    Issue #447: Used to recompute row counts from disk instead of carrying forward
    potentially stale aggregated counts. This ensures correctness when:
    - Re-adding the same file (no double-count)
    - Files are replaced on disk with different content
    - Files are deleted and re-added

    IMPORTANT: Only counts files that are tracked as assets in collection.json.
    Untracked parquet files (temp files, work-in-progress) are ignored to prevent
    inflating row counts.

    Args:
        collection_dir: Path to the collection directory.
        collection: The collection to check tracked assets against.

    Returns:
        List of GeoParquetMetadata for tracked parquet assets found on disk.
    """
    # Build set of tracked asset hrefs (normalized without ./ prefix)
    tracked_hrefs: set[str] = set()
    for asset in collection.assets.values():
        # The item mirror is derived metadata, not data: it carries no
        # GeoParquet bbox, and folding it into table aggregation broke
        # partitioned collections whose data is tracked by a glob href
        # (#654). stac-items covers catalogs written before the role
        # upgrade, mirroring the skip in viz/pmtiles.py.
        roles = asset.roles or []
        if "collection-mirror" in roles or "stac-items" in roles:
            continue
        if asset.href:
            # Normalize to match relative_to(...).as_posix() below: drop only an
            # exact "./" prefix. lstrip("./") would also strip leading dots from
            # hidden paths (".hidden/x.parquet" -> "hidden/x.parquet") and never
            # match, silently dropping the asset from row-count aggregation.
            href = asset.href.removeprefix("./")
            tracked_hrefs.add(href)

    metadata_list: list[GeoParquetMetadata] = []

    for parquet_file in collection_dir.glob("**/*.parquet"):
        # Skip files in .portolan directory (internal state)
        if ".portolan" in parquet_file.parts:
            continue

        # Only include files that are tracked as assets
        # Use as_posix() for cross-platform consistency (Windows uses backslashes,
        # but STAC asset hrefs always use forward slashes)
        relative_path = parquet_file.relative_to(collection_dir).as_posix()
        if relative_path not in tracked_hrefs:
            logger.debug("Skipping untracked parquet file: %s", relative_path)
            continue

        try:
            meta = extract_geoparquet_metadata(parquet_file)
            metadata_list.append(meta)
        except Exception as e:
            # Log but don't fail - file might be corrupted or not a valid parquet
            logger.warning("Could not read metadata from %s: %s", parquet_file, e)

    return metadata_list


def _warn_about_stale_assets(
    collection: pystac.Collection,
    collection_dir: Path,
) -> list[str]:
    """Check for assets that reference missing files and return warnings.

    Issue #447: Emits warnings for assets pointing to files that no longer exist.
    Does NOT remove them - that's the job of `check --fix`.

    Args:
        collection: The collection to check.
        collection_dir: Path to the collection directory.

    Returns:
        List of warning messages for stale assets.
    """
    warnings: list[str] = []

    for key, asset in collection.assets.items():
        if not asset.href:
            continue

        # Resolve href relative to collection directory
        if asset.href.startswith("./"):
            asset_path = collection_dir / asset.href[2:]
        elif asset.href.startswith("/"):
            asset_path = Path(asset.href)
        else:
            asset_path = collection_dir / asset.href

        if not asset_path.exists():
            warnings.append(f"Asset '{key}' references missing file: {asset.href}")

    return warnings


def _ensure_partition_metadata(
    collection: pystac.Collection,
    collection_dir: Path,
    items: list[PreparedItem],
) -> list[str]:
    """Add partition metadata to collection from items or auto-detection.

    Issue #232: Adds partition extension if any items have partition metadata.
    Issue #443: Auto-detects pre-existing Hive partitions if no metadata was set
    from items. This handles the case where users add pre-partitioned data not
    created by Portolan. Also creates glob assets for bulk access.

    Args:
        collection: The STAC collection to update.
        collection_dir: Directory containing the collection.
        items: List of prepared items for this collection.

    Returns:
        List of warning messages (e.g., schema inconsistency warnings).
    """
    from portolan_cli.partitioning import (
        build_glob_pattern,
        detect_partitioning,
        validate_partition_schemas,
    )

    warnings: list[str] = []

    # First, check if any items have explicit partition metadata
    for p in items:
        if p.partition_metadata is not None:
            add_partition_metadata_to_collection(collection, p.partition_metadata)
            # Validate schema consistency for partitioned data
            validation = validate_partition_schemas(collection_dir)
            if not validation.is_consistent and validation.partition_count > 0:
                warnings.append(
                    f"Schema inconsistency in partitioned data: {validation.error_message}"
                )
            return warnings  # Only one partition metadata per collection

    # No explicit metadata - try auto-detection for pre-existing Hive partitions
    detected = detect_partitioning(collection_dir)
    if detected:
        add_partition_metadata_to_collection(collection, detected)
        partition_keys = detected.get("partition:keys", [])
        partition_columns = [k["name"] for k in partition_keys]
        file_count = detected.get("partition:file_count", 0)

        logger.debug("Auto-detected Hive partitions in %s: %s", collection_dir, partition_columns)

        # Create glob asset for bulk access (Issue #443)
        # Only add if not already present (avoid duplicates on re-add)
        glob_pattern = build_glob_pattern(partition_columns=partition_columns)
        glob_asset_key = "partitioned_data"

        # Check if glob asset already exists (any asset with * in href)
        existing_glob = None
        for key, asset in collection.assets.items():
            if asset.href and "*" in asset.href:
                existing_glob = key
                break

        if existing_glob is None:
            # Check if target key is occupied by a non-glob asset (avoid clobbering)
            # Per Issue #443: Don't overwrite user-defined assets at this key
            existing_at_key = collection.assets.get(glob_asset_key)
            if existing_at_key is not None and (
                not existing_at_key.href or "*" not in existing_at_key.href
            ):
                # Key is occupied by non-glob asset - use alternate key
                glob_asset_key = "partitioned_data_glob"
                logger.debug(
                    "Key 'partitioned_data' occupied by non-glob asset, using '%s' instead",
                    glob_asset_key,
                )

            glob_asset = pystac.Asset(
                href=glob_pattern,
                media_type="application/vnd.apache.parquet",
                roles=["data"],
                title="Partitioned GeoParquet",
                description=f"Glob pattern for {file_count} partitioned files",
            )
            collection.assets[glob_asset_key] = glob_asset
            logger.debug("Added glob asset with pattern: %s", glob_pattern)

        # Validate schema consistency for auto-detected partitions
        validation = validate_partition_schemas(collection_dir)
        if not validation.is_consistent and validation.partition_count > 0:
            warnings.append(f"Schema inconsistency in partitioned data: {validation.error_message}")

    return warnings


# ─────────────────────────────────────────────────────────────────────────────
# Versioning (file backend) + freshness tracking
# ─────────────────────────────────────────────────────────────────────────────


_FRESHNESS_CHECKABLE_SUFFIXES = frozenset({".parquet", ".tif", ".tiff"})


def _asset_freshness_fields(
    file_path: Path,
    *,
    is_collection_level: bool,
) -> tuple[float | None, int | None, str | None]:
    """Compute (source_mtime, feature_count, schema_fingerprint) for tracking.

    Only collection-level, freshness-checkable assets (GeoParquet / COG) get a
    baseline: they have no companion item.json, so the metadata_fresh check
    reads their freshness straight from versions.json (#512). Item-level assets
    are left to their existing item.json / ``--fix`` path.

    Best-effort: any extraction failure falls back to ``(None, None, None)`` so
    tracking never blocks an otherwise successful ``add``. The asset's own mtime
    doubles as ``source_mtime`` — mirroring ``update_versions_tracking`` — since
    the freshness fast path compares the asset file's current mtime to it.
    """
    if not is_collection_level:
        return (None, None, None)
    if file_path.suffix.lower() not in _FRESHNESS_CHECKABLE_SUFFIXES:
        return (None, None, None)

    from portolan_cli.metadata.detection import get_current_metadata

    try:
        current = get_current_metadata(file_path)
        return (
            file_path.stat().st_mtime,
            current.current_feature_count,
            current.current_schema_fingerprint,
        )
    except (ValueError, OSError):
        logger.debug("Could not extract freshness heuristics for %s", file_path, exc_info=True)
        return (None, None, None)


def _batch_update_versions(
    collection_dir: Path,
    collection_id: str,
    items: list[PreparedItem],
) -> tuple[str, int, int]:
    """Batch update versions.json for multiple items in a single read-modify-write.

    This is the key optimization for Issue #281: instead of O(n) writes
    (one per item), we do O(1) writes per collection.

    Args:
        collection_dir: Path to collection directory.
        collection_id: Collection identifier.
        items: List of PreparedItem objects to add versions for.

    Returns:
        Tuple of (current_version, asset_count, total_size_bytes) for catalog-level
        versioning updates.
    """
    versions_path = collection_dir / "versions.json"

    # Read existing versions (or create new)
    if versions_path.exists():
        versions_file = read_versions(versions_path)
    else:
        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version=None,
            versions=[],
        )

    # Compute new version string using the helper (handles prerelease versions)
    if versions_file.current_version is None:
        new_version = "1.0.0"
    else:
        new_version = _increment_version(versions_file.current_version)

    # Build assets dict from ALL items (batch)
    all_assets: dict[str, Asset] = {}
    for p in items:
        for file_path, file_checksum, file_size in p.asset_files.values():
            # The key is the path of the file under the collection, and the href
            # adds the collection id. Both follow where the file is, never the
            # item id: --item-id names the item, not a directory (issue #840).
            asset_key = versions_asset_key(file_path, collection_dir)
            href = f"{collection_id}/{asset_key}"

            stat = file_path.stat()
            # Collection-level assets have no companion item.json, so
            # the freshness check reads their baseline straight from
            # versions.json. Persist source_mtime + heuristics here so a plain
            # `add` produces a FRESH asset instead of a perpetual STALE (#512),
            # and so a touched-but-identical asset stays FRESH via the
            # heuristic fallback rather than flipping to STALE/BREAKING.
            source_mtime, feature_count, schema_fingerprint = _asset_freshness_fields(
                file_path, is_collection_level=p.is_collection_level_asset
            )
            # Use pre-computed file_size (handles directories like FileGDB correctly)
            all_assets[asset_key] = Asset(
                sha256=file_checksum,
                size_bytes=file_size,
                href=href,
                mtime=stat.st_mtime,
                source_mtime=source_mtime,
                feature_count=feature_count,
                schema_fingerprint=schema_fingerprint,
            )

    # Add single version with all assets
    updated = add_version(
        versions_file,
        version=new_version,
        assets=all_assets,
        breaking=False,
    )

    # Single write for all items
    write_versions(versions_path, updated)

    # Return info for catalog-level versioning. add_version always leaves at
    # least one version, so the latest one exists.
    latest = updated.versions[-1]
    total_size = sum(a.size_bytes for a in latest.assets.values())
    return (updated.current_version or new_version, len(latest.assets), total_size)


def _finalize_with_backend(
    catalog_root: Path,
    collection_id: str,
    collection_dir: Path,
    collection: object,
    items: list[PreparedItem],
    active_backend: str,
) -> None:
    """Run backend versioning and post-add hooks for a non-file backend.

    Handles both publish_version() and on_post_add() calls so that
    finalize_items() stays within complexity rank C.

    This is backend routing logic added by the iceberg-backend-integration
    branch.
    """
    from portolan_cli.backends import get_backend
    from portolan_cli.backends.protocol import PostAddContext, PostAddItemContext
    from portolan_cli.config import get_setting
    from portolan_cli.version_ops import publish_version

    # Publish version snapshot via the plugin backend
    assets = {
        versions_asset_key(file_path, collection_dir): str(file_path)
        for p in items
        for file_path, _checksum, _size in p.asset_files.values()
    }
    publish_version(collection_id, assets=assets, catalog_root=catalog_root)

    # NOTE: Plugin backends (e.g. Iceberg) may override table:* STAC extension
    # fields in collection.json via on_post_add, since the backend's table state
    # (actual row counts, schema excluding derived columns) is authoritative.
    backend = get_backend(active_backend, catalog_root=catalog_root)
    remote = get_setting("remote", catalog_path=catalog_root, collection=collection_id)
    first = items[0]
    # For collection-level assets, item_json_path is None; use collection_dir
    first_item_dir = first.item_json_path.parent if first.item_json_path else collection_dir
    context = PostAddContext(
        catalog_root=catalog_root,
        collection_id=collection_id,
        collection_dir=collection_dir,
        collection=collection,
        item_id=first.item_id,
        item_dir=first_item_dir,
        asset_files=first.asset_files,
        items=[
            PostAddItemContext(
                item_id=p.item_id,
                item_dir=p.item_json_path.parent if p.item_json_path else collection_dir,
                asset_files=p.asset_files,
            )
            for p in items
        ],
        remote=remote,
    )
    try:
        backend.on_post_add(context)
    except Exception:
        # Version was already published successfully. Log warning but don't fail
        # the entire add operation. The backend hook is for optional enrichment
        # (e.g., uploading STAC metadata to remote).
        logger.warning(
            "Backend on_post_add hook failed for collection '%s'. "
            "Version was published but post-add actions may be incomplete.",
            collection_id,
            exc_info=True,
        )


def _finalize_with_file_backend(
    catalog_root: Path,
    collection_id: str,
    collection_dir: Path,
    items: list[PreparedItem],
) -> None:
    """Update versions.json and catalog-level versions via the file backend.

    File backend fast path (O(1) write per collection). Updates the
    collection-level versions.json first, then mirrors the summary into the
    catalog-level versions.json. A catalog-level failure is logged
    but never fails the add: the collection version was already published.
    """
    from portolan_cli.catalog import update_catalog_versions

    current_version, asset_count, total_size = _batch_update_versions(
        collection_dir=collection_dir,
        collection_id=collection_id,
        items=items,
    )

    # Update catalog-level versions.json
    # This keeps the catalog-level view in sync with collection state.
    # Wrap in try/except to avoid failing the add if catalog update fails
    # (collection-level versions.json was already written successfully).
    try:
        update_catalog_versions(
            catalog_root=catalog_root,
            collection_id=collection_id,
            current_version=current_version,
            asset_count=asset_count,
            total_size_bytes=total_size,
        )
    except Exception:
        # Collection-level versions.json was written successfully.
        # Log warning but don't fail the add operation.
        logger.warning(
            "Failed to update catalog-level versions.json for collection '%s'. "
            "Collection version was published but catalog-level view may be stale.",
            collection_id,
            exc_info=True,
        )


def _publish_collection_version(
    catalog_root: Path,
    collection_id: str,
    collection_dir: Path,
    collection: pystac.Collection,
    items: list[PreparedItem],
) -> None:
    """Route the collection's version snapshot to the active backend.

    Plugin backends (e.g. Iceberg) publish + run post-add hooks; the default
    file backend uses the optimized batch write. Backend selection follows the
    resolved ``backend`` setting.
    """
    from portolan_cli.config import get_setting

    active_backend = get_setting("backend", catalog_path=catalog_root)

    if active_backend is not None and active_backend != "file":
        _finalize_with_backend(
            catalog_root=catalog_root,
            collection_id=collection_id,
            collection_dir=collection_dir,
            collection=collection,
            items=items,
            active_backend=active_backend,
        )
    else:
        _finalize_with_file_backend(
            catalog_root=catalog_root,
            collection_id=collection_id,
            collection_dir=collection_dir,
            items=items,
        )


def record_prepared_items(
    catalog_root: Path, collection_dir: Path, items: list[PreparedItem]
) -> None:
    """Record the asset files of prepared items in a new version, as ``add`` does.

    ``check --fix`` recreates an item outside ``finalize_items``. It calls this,
    so the version snapshot holds every file the recreated item declares
    (issue #1046).

    Args:
        catalog_root: Root directory of the catalog.
        collection_dir: Directory holding collection.json.
        items: The prepared items whose asset files the snapshot records.
    """
    collection_id = collection_dir.resolve().relative_to(catalog_root.resolve()).as_posix()
    collection = pystac.Collection.from_file(str(collection_dir / "collection.json"))
    _publish_collection_version(catalog_root, collection_id, collection_dir, collection, items)


# ─────────────────────────────────────────────────────────────────────────────
# Per-collection assembly + batch entry point
# ─────────────────────────────────────────────────────────────────────────────


def _emit_stale_asset_warnings(
    collection: pystac.Collection,
    collection_dir: Path,
) -> None:
    """Warn about assets referencing missing files (issue #447).

    Warns but does not remove; removal is handled by ``check --fix``.
    """
    stale_warnings = _warn_about_stale_assets(collection, collection_dir)
    if not stale_warnings:
        return

    from portolan_cli.output import warn as warn_output

    warn_output(
        f"{len(stale_warnings)} asset(s) reference missing files "
        "(run `portolan check --fix` to clean up)"
    )
    for warning_msg in stale_warnings:
        logger.debug(warning_msg)


def _describe_collection(
    collection: pystac.Collection,
    catalog_root: Path,
    collection_dir: Path,
    items: list[PreparedItem],
    merge_strategy: MergeStrategy,
) -> None:
    """Let each format handler in this batch add its collection fields (#947).

    The GeoParquet handler aggregates the table extension, for example.

    Args:
        collection: The collection in memory.
        catalog_root: Catalog root.
        collection_dir: Directory holding collection.json.
        items: Prepared items of this batch.
        merge_strategy: How to merge detected values into existing ones.
    """
    from portolan_cli.plugins.formats import FormatContext, handler_named

    by_handler: dict[str, list[Any]] = {}
    for prepared in items:
        if prepared.format_name and prepared.metadata is not None:
            by_handler.setdefault(prepared.format_name, []).append(prepared.metadata)
    context = FormatContext(
        catalog_root=catalog_root, collection_dir=collection_dir, merge_strategy=merge_strategy
    )
    for name, metadata in by_handler.items():
        handler = handler_named(name)
        if handler is not None:
            handler.describe_collection(collection, metadata, context)


def aggregate_table_extension(
    collection: pystac.Collection, metadata: list[Any], context: FormatContext
) -> None:
    """Aggregate table:* metadata from tracked parquet on disk (issues #304/#447).

    Runs only when the batch holds at least one NEW GeoParquet file.
    Recomputes aggregates from ALL tracked parquet files on disk instead of
    carrying forward stale counts. This prevents double-counting on re-add and
    stale counts when files are replaced. The built-in GeoParquet handler
    calls this from ``describe_collection``.

    Args:
        collection: The collection in memory.
        metadata: What the handler read from each new file.
        context: Where the collection sits, and the merge strategy.
    """
    if not any(isinstance(m, GeoParquetMetadata) for m in metadata):
        return
    all_parquet_metadata = _collect_parquet_metadata_from_disk(context.collection_dir, collection)
    if all_parquet_metadata:
        aggregated = aggregate_table_metadata(all_parquet_metadata)
        add_table_extension(collection, aggregated, merge_strategy=context.merge_strategy)


def _emit_partition_warnings(
    collection: pystac.Collection,
    collection_dir: Path,
    items: list[PreparedItem],
) -> None:
    """Add partition metadata (issues #232/#443) and surface any schema warnings."""
    partition_warnings = _ensure_partition_metadata(collection, collection_dir, items)
    if not partition_warnings:
        return

    from portolan_cli.output import warn as warn_output

    for warning_msg in partition_warnings:
        warn_output(warning_msg)


def _enrich_saved_documents(
    catalog_root: Path,
    collection_id: str,
    collection_dir: Path,
    items: list[PreparedItem],
) -> None:
    """Run the item hook on each prepared item file, then the collection hook.

    A collection-level asset has no item file, so only the collection hook
    sees it.

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Collection identifier (may be nested).
        collection_dir: Directory holding collection.json.
        items: Prepared items of this collection.
    """
    for prepared in items:
        item_path = prepared.item_json_path
        if item_path is not None and item_path.exists():
            enrich_item_file(
                item_path,
                EnrichContext(
                    catalog_root=catalog_root,
                    collection_id=collection_id,
                    document_path=item_path,
                    item_id=prepared.item_id,
                ),
            )
    collection_path = collection_dir / "collection.json"
    enrich_collection_file(
        collection_path,
        EnrichContext(
            catalog_root=catalog_root,
            collection_id=collection_id,
            document_path=collection_path,
        ),
    )


def _finalize_collection(
    catalog_root: Path,
    collection_id: str,
    items: list[PreparedItem],
    merge_strategy: MergeStrategy,
) -> list[ItemInfo]:
    """Assemble and persist a single collection from its prepared items.

    Builds collection.json (assets, summaries, extents, extensions) in memory,
    saves it once, and publishes the version snapshot via the active backend.
    See ``finalize_items`` for the batch orchestration around this.

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Collection identifier (may be nested).
        items: Prepared items belonging to this collection.
        merge_strategy: How to merge auto-detected metadata with existing values.

    Returns:
        List of ItemInfo for each finalized item in this collection.
    """
    collection_dir = catalog_root / Path(*collection_id.split("/"))

    # Get or create collection, then add all items at once
    first_item = items[0]
    collection = _get_or_create_collection(
        catalog_root=catalog_root,
        collection_id=collection_id,
        initial_bbox=first_item.bbox,
    )

    # Issue #502/#654/#684/#755: apply human title/description/license/providers
    # from metadata.yaml. The collection's own metadata.yaml wins; inherited
    # ancestor metadata only fills fields the collection still lacks.
    apply_human_metadata(collection, collection_dir, catalog_root)

    # Add items or collection-level assets to collection (in memory)
    _add_prepared_items_to_collection(collection, items, merge_strategy)

    # Issue #447: Check for stale assets (reference missing files)
    _emit_stale_asset_warnings(collection, collection_dir)

    # Add table extension if any items are GeoParquet format (Issue #304/#447)
    _describe_collection(collection, catalog_root, collection_dir, items, merge_strategy)

    # Add partition extension if any items have partition metadata (Issue #232/#443)
    _emit_partition_warnings(collection, collection_dir, items)

    # Declare the file extension the assets use (Issue #501, narrowed by #654)
    declare_file_extension(collection)

    # Extent, summaries, and the extensions the summaries use come from the
    # members, never from the stored values (issues #801, #852, #1023).
    derive_and_apply_fields(
        collection,
        collection_dir,
        catalog_root=catalog_root,
        known_bboxes=_batch_bboxes(items),
    )

    # Save collection.json ONCE for all items in this collection
    _save_collection_with_links(collection, collection_dir, catalog_root, collection_id)

    # Run the enrichment hooks on the saved files. A backend may upload the
    # STAC JSON when it publishes, so the hooks run first (#947).
    _enrich_saved_documents(catalog_root, collection_id, collection_dir, items)

    # Route version snapshot to the active backend (plugin or file)
    _publish_collection_version(catalog_root, collection_id, collection_dir, collection, items)

    # Build results
    return [
        ItemInfo(
            item_id=p.item_id,
            collection_id=p.collection_id,
            format_type=p.format_type,
            bbox=p.bbox,
            asset_paths=[str(path) for (path, _checksum, _size) in p.asset_files.values()],
        )
        for p in items
    ]


def finalize_items(
    catalog_root: Path,
    prepared: list[PreparedItem],
    merge_strategy: MergeStrategy = MergeStrategy.SMART,
) -> list[ItemInfo]:
    """Finalize prepared items by writing versions.json and collection.json.

    This function batches all writes by collection, enabling O(n) versioning
    instead of O(n²). See Issue #281.

    Args:
        catalog_root: Root directory of the catalog.
        prepared: List of PreparedItem objects from prepare_item().
        merge_strategy: How to merge auto-detected metadata with existing values.

    Returns:
        List of ItemInfo for each finalized item.

    Raises:
        AssetKeyCollisionError: A key holds a different file of another media
            type. No collection of the batch is written.
    """
    if not prepared:
        return []

    # Group by collection for efficient batch writes
    from collections import defaultdict

    by_collection: dict[str, list[PreparedItem]] = defaultdict(list)
    for p in prepared:
        by_collection[p.collection_id].append(p)

    _preflight_asset_keys(catalog_root, by_collection, merge_strategy)

    results: list[ItemInfo] = []
    for collection_id, items in by_collection.items():
        results.extend(_finalize_collection(catalog_root, collection_id, items, merge_strategy))

    from portolan_cli.catalog import (
        apply_catalog_human_titles,
        ensure_link_titles,
        ensure_schema_uris,
    )

    # Issue #815: the root catalog takes its title and description from the root
    # metadata.yaml, as every collection already does. It runs before the README
    # sweep below, which reads catalog.json.
    apply_catalog_human_titles(catalog_root)

    # Issue #502: backfill human-readable titles onto child/item links so STAC
    # Browser renders names without fetching every child. Done once per batch
    # (O(catalog), not per-collection) after all collections are written.
    ensure_link_titles(catalog_root)

    # / issue #654: every catalog and collection declares the Portolan
    # profile schema URI and carries AGENTS.md and README.md behind their links.
    # Swept once per batch so subcatalogs created along the way (
    # nesting) and pre-existing objects are covered, not just what was written.
    from portolan_cli.agents_md import ensure_agents_md_tree
    from portolan_cli.readme import ensure_readmes

    ensure_schema_uris(catalog_root)
    ensure_agents_md_tree(catalog_root)
    ensure_readmes(catalog_root)

    # Issue #684: a catalog whose every collection is a mirror carries the sync
    # time itself. Needs the whole tree, so it runs after the last collection.
    update_catalog_provenance(catalog_root)

    return results
