"""Batch finalization for the add pipeline: STAC-write + backend coordination."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, replace
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, Any

import pystac
from pystac.layout import AsIsLayoutStrategy
from pystac.stac_io import DefaultStacIO

from portolan_cli.hookspecs import EnrichContext
from portolan_cli.humanize import humanize_slug
from portolan_cli.metadata import extract_geoparquet_metadata
from portolan_cli.metadata.geoparquet import GeoParquetMetadata
from portolan_cli.report import emit
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
from portolan_cli.utils import href_root, relative_href
from portolan_core.config import load_merged_metadata, load_own_metadata
from portolan_core.json_io import write_json_atomic
from portolan_core.query import ItemInfo
from portolan_core.stac_links import contained_link_target, in_link_order, iter_links
from portolan_core.versions import (
    Asset,
    versions_asset_key,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from portolan_cli.backends.protocol import PostAddItemContext
    from portolan_cli.plugins.formats import FormatContext
    from portolan_cli.preparation import PreparedItem
    from portolan_core.run_state import RunState

logger = logging.getLogger(__name__)


def _deduplicate_collection_item_links(collection: pystac.Collection) -> None:
    """De-duplicate item links in a PySTAC collection."""
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
    """The href from ``collection_dir`` to the root ``catalog.json``, in POSIX."""
    return relative_href(collection_dir, PurePath(catalog_root) / "catalog.json")


def _set_structural_link(
    links: list[dict[str, Any]],
    rel: str,
    href: str,
    media_type: str = "application/json",
) -> None:
    """Point ``rel`` at ``href``, adding the link when it is absent."""
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
    """Fix the root and parent links and drop duplicate item links, in memory."""
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
    """Repoint the structural links of every item this collection owns."""
    if collection_data is None:
        if not collection_json_path.exists():
            return
        collection_data = json.loads(collection_json_path.read_text(encoding="utf-8"))
    for _link, href, _path in iter_links(collection_data, collection_dir, ("item",)):
        # The repair writes only inside the catalog, after symlinks resolve.
        item_path = contained_link_target(catalog_root, collection_dir, href)
        if item_path is None or not item_path.exists():
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
    """Ensure the catalog tree has a child link down to this collection."""
    from portolan_cli.catalog import update_catalog_links_for_nested

    update_catalog_links_for_nested(catalog_root, collection_id)


class _HoldCollectionIO(DefaultStacIO):
    """Write each item that PySTAC saves, and hold the collection dict."""

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
    """Add the profile schema URI and the AGENTS.md and README.md links in memory."""
    from portolan_cli.readme import _ensure_readme_link
    from portolan_cli.stac import ensure_portolan_schema_uri
    from portolan_core.agents_md import (
        agents_link_is_wellformed,
        build_agents_link,
        find_agents_link,
    )

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
    """Write ``collection.json`` once, with its links and conformance links."""
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

    from portolan_core.agents_md import ensure_agents_md

    ensure_agents_md(collection_json_path)


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
    """The WGS84 footprint of a data file, measured the way ``add`` measures it."""
    from portolan_cli.plugins.formats import handler_for
    from portolan_cli.preparation import wgs84_bbox

    def _load(file_path: Path) -> list[float] | None:
        handler = handler_for(file_path, catalog_root, state=state)
        if handler is None:
            return None
        metadata = handler.read_metadata(file_path)
        if not getattr(metadata, "bbox", None):
            return None
        return wgs84_bbox(metadata, file_path)  # type: ignore[arg-type]

    try:
        return state.memo("wgs84_bbox", path, _load) if state is not None else _load(path)
    except Exception:  # an unreadable file contributes no footprint
        logger.debug("Cannot read the footprint of %s", path, exc_info=True)
        return None


def _linked_items(collection: pystac.Collection) -> list[pystac.Item]:
    """Every item the collection links, skipping a link whose file is gone."""
    try:
        return list(collection.get_items(recursive=True))
    except Exception:  # fall back to resolving each link alone
        logger.debug("Collection '%s' links an unreadable item", collection.id, exc_info=True)
    items: list[pystac.Item] = []
    for link in collection.get_links(pystac.RelType.ITEM):
        try:
            target = link.resolve_stac_object(root=collection.get_root()).target
        except Exception:  # a dead link contributes no member
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
    """Every item and collection-level data asset of a collection, from disk."""
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


def refresh_collection_fields(
    catalog_root: Path, collection_dir: Path, *, state: RunState | None = None
) -> bool:
    """Derive the fields of a saved collection again, after its members changed."""
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
    own = (
        state.metadata(collection_dir, inherit=False)
        if state is not None
        else load_own_metadata(collection_dir)
    )
    fields = derive_collection_fields(members, own)
    data = json.loads(path.read_text(encoding="utf-8"))
    before = json.dumps(data, sort_keys=True)
    apply_collection_fields_to_dict(data, fields)
    if json.dumps(data, sort_keys=True) == before:
        return False
    write_json_atomic(path, data)
    return True


def _batch_bboxes(items: list[PreparedItem]) -> dict[str, list[float]]:
    """The bbox ``add`` measured for each collection-level data file of the batch."""
    known: dict[str, list[float]] = {}
    for p in items:
        if not p.is_collection_level_asset:
            continue
        for file_path, _checksum, _size in p.asset_files.values():
            if file_path.stem == p.item_id:
                known[str(file_path.resolve())] = list(p.bbox)
    return known


def _get_or_create_collection(
    catalog_root: Path,
    collection_id: str,
    initial_bbox: list[float],
) -> pystac.Collection:
    """Load existing collection or create new one."""
    # STAC at root level, handle nested paths
    collection_path = catalog_root / Path(*collection_id.split("/")) / "collection.json"

    if collection_path.exists():
        return pystac.Collection.from_file(str(collection_path))

    # New collection: derive a human-readable title from the id.
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
    """Write human-authored metadata onto a collection, own value over inherited."""
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
    """Apply :func:`apply_human_metadata` to a parsed ``collection.json`` in place."""
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
    """Refuse the batch before any write when an asset key collides."""
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
    """Add prepared items or collection-level assets to a collection."""
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
    """Scan collection directory and extract metadata from tracked parquet assets."""
    # Build set of tracked asset hrefs (normalized without ./ prefix)
    tracked_hrefs: set[str] = set()
    for asset in collection.assets.values():
        roles = asset.roles or []
        if "collection-mirror" in roles or "stac-items" in roles:
            continue
        if asset.href:
            href = asset.href.removeprefix("./")
            tracked_hrefs.add(href)

    metadata_list: list[GeoParquetMetadata] = []

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
    """Check for assets that reference missing files and return warnings."""
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
    """Add partition metadata to collection from items or auto-detection."""
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

        # Create glob asset for bulk access
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
            # Don't overwrite user-defined assets at this key
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


_FRESHNESS_CHECKABLE_SUFFIXES = frozenset({".parquet", ".tif", ".tiff"})


def _asset_freshness_fields(
    file_path: Path,
    *,
    is_collection_level: bool,
    state: RunState | None = None,
) -> tuple[float | None, int | None, str | None]:
    """Compute (source_mtime, feature_count, schema_fingerprint) for tracking."""
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
    """Stage one version of the collection with the assets of every item."""
    assets: dict[str, Asset] = {}
    for p in items:
        for file_path, file_checksum, file_size in p.asset_files.values():
            asset_key = versions_asset_key(file_path, collection_dir)
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
    """Run backend versioning and post-add hooks for a non-file backend."""
    from portolan_cli.backends.protocol import PostAddItemContext

    _stage_items(state, collection_id, collection_dir, items)
    if companions:
        stage_companions(state, collection_id, list(companions))

    _stage_backend_hook(
        catalog_root,
        state,
        collection_id,
        collection,
        [
            PostAddItemContext(
                item_id=p.item_id,
                item_dir=p.item_json_path.parent if p.item_json_path else collection_dir,
                asset_files=p.asset_files,
            )
            for p in items
        ],
        active_backend,
    )


def _stage_backend_hook(
    catalog_root: Path,
    state: RunState,
    collection_id: str,
    collection: object,
    items: list[PostAddItemContext],
    active_backend: str,
) -> None:
    """Attach the post-add hook to the collection's one pending publish."""
    from portolan_cli.backends import get_backend
    from portolan_cli.backends.protocol import PostAddContext

    collection_dir = state.collection_dir(collection_id)
    backend = get_backend(active_backend, catalog_root=catalog_root)
    first = items[0]
    context = PostAddContext(
        catalog_root=catalog_root,
        collection_id=collection_id,
        collection_dir=collection_dir,
        collection=collection,
        item_id=first["item_id"],
        item_dir=first["item_dir"],
        asset_files=first["asset_files"],
        items=items,
        remote=state.setting("remote", collection_dir=collection_dir),
    )

    def post_add() -> None:
        try:
            backend.on_post_add(context)
        except Exception:
            # The version is already published. Optional backend enrichment
            # must not turn a successful publish into retryable staged work.
            logger.warning(
                "Backend on_post_add hook failed for collection '%s'. "
                "Version was published but post-add actions may be incomplete.",
                collection_id,
                exc_info=True,
            )

    state.stage(collection_id, on_publish=post_add)


def stage_deferred_post_add(
    state: RunState,
    collection_id: str,
    assets: Mapping[str, Asset],
    item_ids: Mapping[Path, str],
) -> None:
    """Register the plugin hook when an add changed only deferred assets."""
    from portolan_cli.backends.protocol import PostAddItemContext

    directory = state.collection_dir(collection_id)
    backend = state.backend(directory)
    if backend == "file":
        return
    groups: dict[Path, dict[str, tuple[Path, str, int]]] = {}
    for asset in assets.values():
        path = state.catalog_root / asset.href
        groups.setdefault(path.parent, {})[path.name] = (path, asset.sha256, asset.size_bytes)
    _stage_backend_hook(
        state.catalog_root,
        state,
        collection_id,
        pystac.Collection.from_file(str(directory / "collection.json")),
        [
            PostAddItemContext(
                item_id=item_ids.get(parent, next(iter(files.values()))[0].stem),
                item_dir=parent,
                asset_files=files,
            )
            for parent, files in sorted(groups.items())
        ],
        backend,
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
    """Route the collection's version snapshot to the active backend."""
    active_backend = state.backend(collection_dir)
    if active_backend != "file":
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


@dataclass(frozen=True)
class CompanionAsset:
    """A collection-level file without geometry that ``add`` tracks as an asset."""

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
    """Put each companion in the collection dict, with its table fields."""
    from portolan_cli.metadata.tabular import extract_tabular_metadata
    from portolan_cli.preparation import _get_asset_role, _get_media_type
    from portolan_cli.stac import document_tabular_table, set_temporal_extent
    from portolan_cli.stac_parquet import sync_file_extension
    from portolan_core.checksums import file_fields_from

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
    from portolan_core.versions import generated_asset_record

    collection_dir = state.collection_dir(collection_id)
    records = dict(
        generated_asset_record(path, state.catalog_root, collection_dir, state.checksum(path))
        for path in paths
    )
    state.stage(collection_id, assets=records, minor=True)


def stage_rewritten(state: RunState, paths: Iterable[Path]) -> None:
    """Stage the new bytes of each tracked file that a pass rewrote, as ``add`` records them."""
    root = state.catalog_root.resolve()
    for path in paths:
        collection_dir = next(
            (p for p in path.resolve().parents if (p / "collection.json").is_file()), None
        )
        if collection_dir is None or not collection_dir.is_relative_to(root):
            continue
        key = versions_asset_key(path, collection_dir)
        previous = state.tracked_assets(collection_dir).get(key)
        if previous is None:
            continue
        state.invalidate(path)
        stat = path.stat()
        _, feature_count, fingerprint = _asset_freshness_fields(
            path, is_collection_level=path.parent.resolve() == collection_dir, state=state
        )
        record = replace(
            previous,
            sha256=state.checksum(path),
            size_bytes=stat.st_size,
            mtime=stat.st_mtime,
            source_mtime=None if previous.source_mtime is None else stat.st_mtime,
        )
        if fingerprint is not None:
            record = replace(record, feature_count=feature_count, schema_fingerprint=fingerprint)
        state.stage(collection_dir.relative_to(root).as_posix(), assets={key: record})


def _emit_stale_asset_warnings(
    collection: pystac.Collection,
    collection_dir: Path,
) -> None:
    """Warn about assets referencing missing files."""
    stale_warnings = _warn_about_stale_assets(collection, collection_dir)
    if not stale_warnings:
        return

    emit(
        "warn",
        f"{len(stale_warnings)} asset(s) reference missing files "
        "(run `portolan check --fix` to clean up)",
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
    """Let each format handler in this batch add its collection fields."""
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
    """Aggregate table:* metadata from tracked parquet on disk.

    Args:
        collection: The collection in memory.
        metadata: What the handler read from each new file.
        context: Where the collection sits, and the merge strategy.
    """
    if not any(isinstance(m, GeoParquetMetadata) for m in metadata):
        return
    from portolan_core.run_state import RunState

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
    """Add partition metadata and surface any schema warnings."""
    partition_warnings = _ensure_partition_metadata(collection, collection_dir, items)
    if not partition_warnings:
        return

    for warning_msg in partition_warnings:
        emit("warn", warning_msg)


def _enrich_saved_documents(
    catalog_root: Path,
    collection_id: str,
    collection_dir: Path,
    items: list[PreparedItem],
) -> None:
    """Run the item hook on each prepared item file, then the collection hook."""
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
    """Assemble and persist a single collection from its prepared items."""
    collection_dir = catalog_root / Path(*collection_id.split("/"))

    # Get or create collection, then add all items at once
    first_item = items[0]
    collection = _get_or_create_collection(
        catalog_root=catalog_root,
        collection_id=collection_id,
        initial_bbox=first_item.bbox,
    )

    apply_human_metadata(collection, collection_dir, catalog_root, state)

    # Add items or collection-level assets to collection (in memory)
    _add_prepared_items_to_collection(collection, items, merge_strategy)

    # Check for stale assets (reference missing files)
    _emit_stale_asset_warnings(collection, collection_dir)

    # Add table extension if any items are GeoParquet format
    _describe_collection(collection, catalog_root, collection_dir, items, merge_strategy, state)

    # Add partition extension if any items have partition metadata
    _emit_partition_warnings(collection, collection_dir, items)

    declare_file_extension(collection)

    # Extent, summaries, and the extensions the summaries use come from the
    # members, never from the stored values.
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
    # STAC JSON when it publishes, so the hooks run first.
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
    """Finalize prepared items by writing versions.json and collection.json."""
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

    apply_catalog_human_titles(catalog_root, state=state)

    ensure_link_titles(catalog_root)

    # Every catalog and collection declares the profile schema URI and links AGENTS.md and README.md.
    from portolan_cli.readme import ensure_readmes
    from portolan_core.agents_md import ensure_agents_md_tree

    ensure_schema_uris(catalog_root)
    ensure_agents_md_tree(catalog_root)
    ensure_readmes(catalog_root, state=state)

    # A catalog whose collections are all mirrors carries the sync time. It needs the whole tree.
    update_catalog_provenance(catalog_root)

    return results
