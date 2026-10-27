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
from dataclasses import dataclass
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, Any

import pystac
from pystac.layout import AsIsLayoutStrategy
from pystac.stac_io import DefaultStacIO

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
from portolan_cli.stac_links import in_link_order, iter_links
from portolan_cli.utils import href_root, relative_href
from portolan_cli.versions import (
    Asset,
    versions_asset_key,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from portolan_cli.plugins.formats import FormatContext
    from portolan_cli.preparation import PreparedItem
    from portolan_cli.run_state import RunState

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

    The link carries no title. PySTAC stamps the title of the object it resolves
    as the target, and on a first save it resolves the root to the collection
    itself. The same add then wrote a title on a second run and none on the
    first. ensure_link_titles backfills titles on child and item links only.
    """
    for link in links:
        if link.get("rel") == rel:
            link.pop("title", None)
            link["href"] = href
            link["type"] = media_type
            return
    links.append({"rel": rel, "href": href, "type": media_type})


def _set_collection_links(
    collection_data: dict[str, Any],
    catalog_root: Path,
    collection_dir: Path,
) -> None:
    """Fix the root and parent links and drop duplicate item links, in memory.

    PySTAC sets root to self by default, so both structural links need
    repointing: ``root`` at the catalog root, and ``parent`` at whatever catalog
    contains this collection. Those two coincide only for a single-level id. For
    a nested id the container is the intermediate ``catalog.json`` that
    ``create_intermediate_catalogs`` writes one level up, and reusing the root
    href walked straight past it (rashid PTL-LNK-006, issue #711).

    Also deduplicates item links that can occur when add is called
    multiple times on the same collection.
    """
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
    collection_data["links"] = in_link_order(deduped_links)


def _fix_collection_links(
    collection_json_path: Path,
    catalog_root: Path,
    collection_dir: Path,
) -> None:
    """Apply :func:`_set_collection_links` to a ``collection.json`` on disk."""
    if not collection_json_path.exists():
        return

    collection_data = json.loads(collection_json_path.read_text(encoding="utf-8"))
    _set_collection_links(collection_data, catalog_root, collection_dir)
    write_json_atomic(collection_json_path, collection_data)


def _fix_item_links(
    collection_json_path: Path,
    catalog_root: Path,
    collection_dir: Path,
    collection_data: dict[str, Any] | None = None,
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
        collection_json_path: The ``collection.json`` whose item links to follow.
        catalog_root: Directory holding the root ``catalog.json``.
        collection_dir: Directory holding ``collection.json``.
        collection_data: The collection in memory. None reads
            ``collection_json_path``.
    """
    if collection_data is None:
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
        item_data["links"] = in_link_order(item_links)
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


class _HoldCollectionIO(DefaultStacIO):
    """Write each item that PySTAC saves, and hold the collection dict.

    ``Collection.save`` writes the items and then the collection. The caller
    finishes the held dict and writes it once.
    """

    def __init__(self, collection_json_path: Path) -> None:
        super().__init__()
        self._target = collection_json_path.resolve()
        self.held: dict[str, Any] | None = None

    def save_json(self, dest: Any, json_dict: dict[str, Any], *args: Any, **kwargs: Any) -> None:
        href = dest.get_absolute_href() if isinstance(dest, pystac.Link) else str(dest)
        if href is not None and Path(href).resolve() == self._target:
            self.held = json_dict
            return
        super().save_json(dest, json_dict, *args, **kwargs)


def _declare_conformance_links(
    collection_data: dict[str, Any], collection_dir: Path, *, readme_link: bool
) -> None:
    """Add the profile schema URI and the AGENTS.md and README.md links in memory.

    The tree sweeps of ``finalize_items`` add them too. A collection written
    here already carries them, so the sweeps only create the two files and do
    not write ``collection.json`` again. With ``readme_link`` False, the
    README.md link stays out, because no sweep writes the README.md file.
    """
    from portolan_cli.agents_md import (
        agents_link_is_wellformed,
        build_agents_link,
        find_agents_link,
    )
    from portolan_cli.readme import _ensure_readme_link
    from portolan_cli.stac import ensure_portolan_schema_uri

    ensure_portolan_schema_uri(collection_data)
    links = collection_data.setdefault("links", [])
    link = find_agents_link(links)
    if link is None:
        links.append(build_agents_link())
    elif not agents_link_is_wellformed(link):
        link.update(build_agents_link())
    if readme_link:
        _ensure_readme_link(collection_dir, collection_data)


def _save_collection_with_links(
    collection: pystac.Collection,
    collection_dir: Path,
    catalog_root: Path,
    collection_id: str,
    finish: Callable[[dict[str, Any]], None] | None = None,
    *,
    readme_link: bool = True,
) -> None:
    """Write ``collection.json`` once, with its links and conformance links.

    PySTAC writes the items. The collection dict stays in memory until its
    links, the conformance links, and ``finish`` apply, and then one write
    puts it on disk (issue #995).

    Args:
        collection: PySTAC collection to save.
        collection_dir: Collection directory path.
        catalog_root: Catalog root path.
        collection_id: Collection identifier.
        finish: Edits the collection dict before the write, such as the
            companion assets of ``add``.
        readme_link: Add the README.md link. A caller that runs no README
            sweep, such as ``add-external``, passes False, so no link dangles.
    """
    collection_json_path = collection_dir / "collection.json"
    _deduplicate_collection_item_links(collection)
    collection.set_self_href(str(collection_json_path))
    collection.normalize_hrefs(href_root(collection_dir), strategy=AsIsLayoutStrategy())
    stac_io = _HoldCollectionIO(collection_json_path)
    collection.save(catalog_type=pystac.CatalogType.SELF_CONTAINED, stac_io=stac_io)
    data = stac_io.held
    if data is None:
        raise RuntimeError(f"PySTAC did not save {collection_json_path}")

    _set_collection_links(data, catalog_root, collection_dir)
    if finish is not None:
        finish(data)
    _declare_conformance_links(data, collection_dir, readme_link=readme_link)
    write_json_atomic(collection_json_path, data)

    _fix_item_links(collection_json_path, catalog_root, collection_dir, data)
    _update_catalog_links(catalog_root, collection_id)

    # Scaffold AGENTS.md next to the collection. The link is already in the
    # file, so this writes no JSON. It never overwrites an existing AGENTS.md,
    # so re-running `add` preserves human edits.
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


def _read_wgs84_bbox(
    path: Path, catalog_root: Path | None, state: RunState | None = None
) -> list[float] | None:
    """The WGS84 footprint of a data file, measured the way ``add`` measures it.

    None when no handler reads the file, so a table or a remote asset adds no
    footprint, and the extent is never invented. With ``state``, the run reads
    the footprint of each unchanged file once.
    """
    from portolan_cli.plugins.formats import handler_for
    from portolan_cli.preparation import _extract_bbox_wgs84

    def _load(file_path: Path) -> list[float] | None:
        handler = handler_for(file_path, catalog_root, state=state)
        if handler is None:
            return None
        metadata = handler.read_metadata(file_path)
        if not getattr(metadata, "bbox", None):
            return None
        return _extract_bbox_wgs84(metadata, file_path)  # type: ignore[arg-type]

    try:
        return state.memo("wgs84_bbox", path, _load) if state is not None else _load(path)
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
    state: RunState | None = None,
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
        state: The run state, which parses the config once per run.

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
            bbox = _read_wgs84_bbox(path, catalog_root, state)
        members.append(CollectionMember(bbox=bbox))
    return members


def derive_and_apply_fields(
    collection: pystac.Collection,
    collection_dir: Path,
    *,
    catalog_root: Path | None = None,
    known_bboxes: dict[str, list[float]] | None = None,
    state: RunState | None = None,
) -> None:
    """Replace the derived fields of an in-memory collection from its members."""
    members = collection_members(
        collection,
        collection_dir,
        catalog_root=catalog_root,
        known_bboxes=known_bboxes,
        state=state,
    )
    own = (
        state.metadata(collection_dir, inherit=False)
        if state is not None
        else load_own_metadata(collection_dir)
    )
    apply_collection_fields(collection, derive_collection_fields(members, own))


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
    state: RunState | None = None,
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
        state: The run state, which parses each metadata.yaml once per run.
    """
    if state is not None:
        own = state.metadata(collection_dir, inherit=False)
        merged = state.metadata(collection_dir)
    else:
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
    data: dict[str, Any],
    collection_dir: Path,
    catalog_root: Path,
    state: RunState | None = None,
) -> None:
    """Apply :func:`apply_human_metadata` to a parsed ``collection.json`` in place.

    The tabular writer keeps collection.json as JSON. It calls this so an
    ``add --force`` applies metadata.yaml to a tabular collection the way it
    does to a spatial one (issue #813).

    Args:
        data: Parsed ``collection.json``, mutated in place.
        collection_dir: The collection's directory under the catalog root.
        catalog_root: Root directory of the catalog.
        state: The run state, which parses each metadata.yaml once per run.
    """
    collection = pystac.Collection.from_dict(data, migrate=False)
    apply_human_metadata(collection, collection_dir, catalog_root, state)
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
    state: RunState | None = None,
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
        state: The run state. Its memo holds the footer that preparation read,
            so a file of this run opens once (issue #995).

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

    # Walk the tracked hrefs, not the directory: a glob of a large collection
    # costs a directory walk per finalize (issue #995). A glob href, an href
    # outside the collection, and the .portolan directory hold no table.
    root = collection_dir.resolve()
    for href in sorted(tracked_hrefs):
        parquet_file = collection_dir / href
        if "*" in href or parquet_file.suffix.lower() != ".parquet":
            continue
        try:
            inside = parquet_file.resolve().relative_to(root)
        except ValueError:
            continue
        if ".portolan" in inside.parts or not parquet_file.is_file():
            continue

        try:
            meta = extract_geoparquet_metadata(parquet_file, state=state)
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
    state: RunState | None = None,
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
    With ``state``, the read reuses the footer that preparation read.
    """
    if not is_collection_level:
        return (None, None, None)
    if file_path.suffix.lower() not in _FRESHNESS_CHECKABLE_SUFFIXES:
        return (None, None, None)

    from portolan_cli.metadata.detection import get_current_metadata

    try:
        current = get_current_metadata(file_path, state=state)
        return (
            file_path.stat().st_mtime,
            current.current_feature_count,
            current.current_schema_fingerprint,
        )
    except (ValueError, OSError):
        logger.debug("Could not extract freshness heuristics for %s", file_path, exc_info=True)
        return (None, None, None)


def _stage_items(
    state: RunState,
    collection_id: str,
    collection_dir: Path,
    items: list[PreparedItem],
) -> None:
    """Stage one version of the collection with the assets of every item.

    The commit at the end of the run writes it (issue #281, issue #995). Each
    record carries the checksum that preparation computed, so the commit hashes
    nothing again.

    Args:
        state: The run state that holds the staged versions.
        collection_id: Collection identifier.
        collection_dir: Path to collection directory.
        items: List of PreparedItem objects to add versions for.
    """
    assets: dict[str, Asset] = {}
    for p in items:
        for file_path, file_checksum, file_size in p.asset_files.values():
            # The key is the path of the file under the collection, and the href
            # adds the collection id. Both follow where the file is, never the
            # item id: --item-id names the item, not a directory (issue #840).
            asset_key = versions_asset_key(file_path, collection_dir)
            # Collection-level assets have no companion item.json, so
            # the freshness check reads their baseline straight from
            # versions.json. Persist source_mtime + heuristics here so a plain
            # `add` produces a FRESH asset instead of a perpetual STALE (#512),
            # and so a touched-but-identical asset stays FRESH via the
            # heuristic fallback rather than flipping to STALE/BREAKING.
            source_mtime, feature_count, schema_fingerprint = _asset_freshness_fields(
                file_path, is_collection_level=p.is_collection_level_asset, state=state
            )
            # Use pre-computed file_size (handles directories like FileGDB correctly)
            assets[asset_key] = Asset(
                sha256=file_checksum,
                size_bytes=file_size,
                href=f"{collection_id}/{asset_key}",
                mtime=file_path.stat().st_mtime,
                source_mtime=source_mtime,
                source_path=p.source_paths.get(file_path),
                feature_count=feature_count,
                schema_fingerprint=schema_fingerprint,
            )
    state.stage(collection_id, assets=assets)


def _finalize_with_backend(
    catalog_root: Path,
    collection_id: str,
    collection_dir: Path,
    collection: object,
    items: list[PreparedItem],
    active_backend: str,
    state: RunState,
    companions: tuple[Path, ...] = (),
) -> None:
    """Run backend versioning and post-add hooks for a non-file backend.

    The plugin publishes here, not at the commit, because ``on_post_add`` must
    see the published version. A plugin backend gets the asset paths and
    computes its own checksums. The companion files go in the same version.
    """
    from portolan_cli.backends import get_backend
    from portolan_cli.backends.protocol import PostAddContext, PostAddItemContext
    from portolan_cli.version_ops import publish_version

    # Publish version snapshot via the plugin backend
    assets = {
        versions_asset_key(file_path, collection_dir): str(file_path)
        for p in items
        for file_path, _checksum, _size in p.asset_files.values()
    }
    assets.update({versions_asset_key(path, collection_dir): str(path) for path in companions})
    publish_version(collection_id, assets=assets, catalog_root=catalog_root, state=state)

    # NOTE: Plugin backends (e.g. Iceberg) may override table:* STAC extension
    # fields in collection.json via on_post_add, since the backend's table state
    # (actual row counts, schema excluding derived columns) is authoritative.
    backend = get_backend(active_backend, catalog_root=catalog_root)
    remote = state.setting("remote", collection_dir=collection_dir)
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


def _publish_collection_version(
    catalog_root: Path,
    collection_id: str,
    collection_dir: Path,
    collection: pystac.Collection,
    items: list[PreparedItem],
    state: RunState,
    companions: tuple[Path, ...] = (),
) -> None:
    """Route the collection's version snapshot to the active backend.

    A plugin backend (such as Iceberg) publishes now and runs its post-add
    hook. The file backend stages the version, and ``version_ops.commit_run``
    writes it with the catalog-level index at the end of the run. The
    companion files join that one version on either backend.
    """
    active_backend = state.setting("backend")
    if active_backend is not None and active_backend != "file":
        _finalize_with_backend(
            catalog_root=catalog_root,
            collection_id=collection_id,
            collection_dir=collection_dir,
            collection=collection,
            items=items,
            active_backend=active_backend,
            state=state,
            companions=companions,
        )
    else:
        _stage_items(state, collection_id, collection_dir, items)
        if companions:
            stage_companions(state, collection_id, list(companions))


# ─────────────────────────────────────────────────────────────────────────────
# Companion assets: collection-level files without geometry (issue #383)
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CompanionAsset:
    """A collection-level file without geometry that ``add`` tracks as an asset.

    Attributes:
        path: The file in the collection directory.
        role: The asset role. None takes the role of the file extension.
    """

    path: Path
    role: str | None = None


def _companion_key(assets: dict[str, Any], path: Path) -> str:
    """The asset key of a companion: its stem, or its name when another file has the stem."""
    key = path.stem
    if key in assets and assets[key].get("href", "") != f"./{path.name}":
        return path.name
    return key


def add_companion_assets(
    collection_data: dict[str, Any],
    collection_dir: Path,
    companions: list[CompanionAsset],
    merge_strategy: MergeStrategy,
    state: RunState,
) -> None:
    """Put each companion in the collection dict, with its table fields.

    The spec asks a tabular collection to document its columns with the table
    extension, and to populate ``extent.temporal`` when the data carries a time
    dimension (issue #749). Both come from the Parquet footer.
    :func:`extract_tabular_metadata` returns None for a non-Parquet file and
    for GeoParquet, so those get no table fields. The caller applies
    metadata.yaml to the collection before this call.

    Args:
        collection_data: Parsed ``collection.json``, mutated in place.
        collection_dir: The collection directory.
        companions: The files to add.
        merge_strategy: Decides only the column descriptions (issue #931).
        state: The run state, which hashes each file once per run.
    """
    from portolan_cli.metadata.tabular import extract_tabular_metadata
    from portolan_cli.preparation import _get_asset_role, _get_media_type
    from portolan_cli.stac import document_tabular_table, set_temporal_extent
    from portolan_cli.stac_parquet import sync_file_extension
    from portolan_cli.sync.checksums import file_fields_from

    assets = collection_data.setdefault("assets", {})
    for companion in companions:
        path = companion.path
        key = _companion_key(assets, path)
        assets[key] = {
            "href": f"./{path.name}",
            "type": _get_media_type(path),
            "roles": [companion.role or _get_asset_role(path)],
            **file_fields_from(state.checksum(path), path.stat().st_size),
        }
        metadata = extract_tabular_metadata(path)
        if metadata is None:
            continue
        document_tabular_table(collection_data, key, metadata, merge_strategy)
        if metadata.temporal_interval is not None:
            set_temporal_extent(collection_data, metadata.temporal_interval)
    sync_file_extension(collection_data, assets, collection_dir / "collection.json")


def stage_companions(state: RunState, collection_id: str, paths: list[Path]) -> None:
    """Stage the version records of companion files, with the checksums of the run."""
    from portolan_cli.versions import generated_asset_record

    collection_dir = state.collection_dir(collection_id)
    records = dict(
        generated_asset_record(path, state.catalog_root, collection_dir, state.checksum(path))
        for path in paths
    )
    state.stage(collection_id, assets=records)


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
    state: RunState | None = None,
) -> None:
    """Let each format handler in this batch add its collection fields (#947).

    The GeoParquet handler aggregates the table extension, for example.

    Args:
        collection: The collection in memory.
        catalog_root: Catalog root.
        collection_dir: Directory holding collection.json.
        items: Prepared items of this batch.
        merge_strategy: How to merge detected values into existing ones.
        state: The run state, which parses the config once per run.
    """
    from portolan_cli.plugins.formats import FormatContext, handler_named

    by_handler: dict[str, list[Any]] = {}
    for prepared in items:
        if prepared.format_name and prepared.metadata is not None:
            by_handler.setdefault(prepared.format_name, []).append(prepared.metadata)
    context = FormatContext(
        catalog_root=catalog_root,
        collection_dir=collection_dir,
        merge_strategy=merge_strategy,
        state=state,
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
    from portolan_cli.run_state import RunState

    # FormatContext types its state as a config source for plugins. The run
    # passes a RunState, whose memo holds the footers.
    state = context.state if isinstance(context.state, RunState) else None
    all_parquet_metadata = _collect_parquet_metadata_from_disk(
        context.collection_dir, collection, state
    )
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
    state: RunState,
    companions: list[CompanionAsset] | None = None,
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
        state: The run state, which holds the staged version.
        companions: Collection-level files without geometry that go in the
            same write and the same version.

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
    apply_human_metadata(collection, collection_dir, catalog_root, state)

    # Add items or collection-level assets to collection (in memory)
    _add_prepared_items_to_collection(collection, items, merge_strategy)

    # Issue #447: Check for stale assets (reference missing files)
    _emit_stale_asset_warnings(collection, collection_dir)

    # Add table extension if any items are GeoParquet format (Issue #304/#447)
    _describe_collection(collection, catalog_root, collection_dir, items, merge_strategy, state)

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
        state=state,
    )

    # Save collection.json ONCE for all items in this collection
    def finish(data: dict[str, Any]) -> None:
        if companions:
            add_companion_assets(data, collection_dir, companions, merge_strategy, state)

    _save_collection_with_links(collection, collection_dir, catalog_root, collection_id, finish)

    # Run the enrichment hooks on the saved files. A backend may upload the
    # STAC JSON when it publishes, so the hooks run first (#947).
    _enrich_saved_documents(catalog_root, collection_id, collection_dir, items)

    # Route version snapshot to the active backend (plugin or file)
    _publish_collection_version(
        catalog_root,
        collection_id,
        collection_dir,
        collection,
        items,
        state,
        tuple(c.path for c in companions or ()),
    )

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
    *,
    state: RunState | None = None,
    companions: Mapping[str, list[CompanionAsset]] | None = None,
) -> list[ItemInfo]:
    """Finalize prepared items by writing versions.json and collection.json.

    This function batches all writes by collection, enabling O(n) versioning
    instead of O(n²). See Issue #281.

    Args:
        catalog_root: Root directory of the catalog.
        prepared: List of PreparedItem objects from prepare_item().
        merge_strategy: How to merge auto-detected metadata with existing values.
        state: The run state, which holds the staged versions. Without one,
            this opens a state and commits it before it returns.
        companions: Collection id to the collection-level files without
            geometry that ``add`` tracks beside the data. A collection of the
            batch writes them in its one ``collection.json`` write.

    Returns:
        List of ItemInfo for each finalized item.

    Raises:
        AssetKeyCollisionError: A key holds a different file of another media
            type. No collection of the batch is written.
    """
    if not prepared:
        return []

    from portolan_cli.version_ops import owned_run

    with owned_run(catalog_root, state) as run:
        return _finalize_batch(catalog_root, prepared, merge_strategy, run, companions or {})


def _finalize_batch(
    catalog_root: Path,
    prepared: list[PreparedItem],
    merge_strategy: MergeStrategy,
    state: RunState,
    companions: Mapping[str, list[CompanionAsset]],
) -> list[ItemInfo]:
    """Write the collections of ``prepared`` and stage their versions."""
    # Group by collection for efficient batch writes
    from collections import defaultdict

    by_collection: dict[str, list[PreparedItem]] = defaultdict(list)
    for p in prepared:
        by_collection[p.collection_id].append(p)

    _preflight_asset_keys(catalog_root, by_collection, merge_strategy)

    results: list[ItemInfo] = []
    for collection_id, items in by_collection.items():
        results.extend(
            _finalize_collection(
                catalog_root,
                collection_id,
                items,
                merge_strategy,
                state,
                companions.get(collection_id),
            )
        )

    from portolan_cli.catalog import (
        apply_catalog_human_titles,
        ensure_link_titles,
        ensure_schema_uris,
    )

    # Issue #815: the root catalog takes its title and description from the root
    # metadata.yaml, as every collection already does. It runs before the README
    # sweep below, which reads catalog.json.
    apply_catalog_human_titles(catalog_root, state=state)

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
    ensure_readmes(catalog_root, state=state)

    # Issue #684: a catalog whose every collection is a mirror carries the sync
    # time itself. Needs the whole tree, so it runs after the last collection.
    update_catalog_provenance(catalog_root)

    return results
