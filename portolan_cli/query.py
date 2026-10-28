"""Catalog query API: list items, get item info, freshness checks."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from portolan_cli.format_families import family_of_asset
from portolan_cli.run_state import RunState
from portolan_cli.stac_links import catalog_collections, owned_item_hrefs

if TYPE_CHECKING:
    from datetime import datetime
    from pathlib import Path

    from portolan_cli.versions import Asset

logger = logging.getLogger(__name__)


@dataclass
class ItemInfo:
    """Information about an item in the catalog.

    Attributes:
        item_id: STAC item identifier.
        collection_id: Parent collection identifier.
        format_type: Format family, for example "vector" or "raster". None if unknown.
        bbox: Bounding box [min_x, min_y, max_x, max_y].
        asset_paths: Paths to data assets.
        title: Optional display title.
        description: Optional description.
        datetime: Acquisition/creation datetime.
    """

    item_id: str
    collection_id: str
    format_type: str | None
    bbox: list[float]
    asset_paths: list[str] = field(default_factory=list)
    title: str | None = None
    description: str | None = None
    datetime: datetime | None = None


def _item_info(item_data: dict[str, Any], collection_id: str, fallback_id: str) -> ItemInfo:
    """Build an ItemInfo from a parsed STAC item."""
    # Determine format from assets
    format_type: str | None = None
    asset_paths: list[str] = []
    for asset in item_data.get("assets", {}).values():
        href = asset.get("href", "")
        asset_paths.append(href)
        family = family_of_asset(asset.get("type"), href)
        if family is not None:
            format_type = family

    properties = item_data.get("properties", {})
    return ItemInfo(
        item_id=item_data.get("id", fallback_id),
        collection_id=collection_id,
        format_type=format_type,
        bbox=item_data.get("bbox", [0, 0, 0, 0]),
        asset_paths=asset_paths,
        title=properties.get("title"),
        description=properties.get("description"),
    )


def _collection_items(catalog_root: Path, collection_id: str) -> list[ItemInfo]:
    """The items a collection owns, read through its links."""
    items: list[ItemInfo] = []
    collection_json = catalog_root / collection_id / "collection.json"
    for _href, item_path in owned_item_hrefs(collection_json):
        if not item_path.exists():
            continue
        item_data = json.loads(item_path.read_text(encoding="utf-8"))
        items.append(_item_info(item_data, collection_id, item_path.stem))
    return items


def list_items(
    catalog_root: Path,
    collection_id: str | None = None,
) -> list[ItemInfo]:
    """List items in a Portolan catalog.

    The collections come from the containment walk that ``check`` applies. The
    items of each collection come from its ``item`` links, through any
    catalogs that organize them (issue #944).

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Optional collection to filter by.

    Returns:
        List of ItemInfo objects.
    """
    if not (catalog_root / "catalog.json").exists():
        return []

    items: list[ItemInfo] = []
    for col_id in catalog_collections(catalog_root):
        if collection_id and col_id != collection_id:
            continue
        items.extend(_collection_items(catalog_root, col_id))
    return items


def get_item_info(
    catalog_root: Path,
    stac_id: str,
) -> ItemInfo:
    """Get information about a specific item.

    Args:
        catalog_root: Root directory of the catalog.
        stac_id: STAC identifier in format "collection/item". A nested
            collection gives "parent/collection/item".

    Returns:
        ItemInfo for the requested item.

    Raises:
        KeyError: If the item doesn't exist.
    """
    if "/" not in stac_id:
        raise KeyError(f"Item not found: {stac_id} (expected format: collection/item)")

    collection_id, item_id = stac_id.rsplit("/", 1)

    for item in _collection_items(catalog_root, collection_id):
        if item.item_id == item_id:
            return item

    raise KeyError(f"Item not found: {stac_id}")


def is_current(
    path: Path,
    versions_path: Path,
    *,
    asset_key: str | None = None,
    state: RunState | None = None,
) -> bool:
    """Check if a file is unchanged compared to versions.json.

    The run state reads ``versions.json`` once per run, so a loop over many
    candidate files costs one read (issue #995). ``RunState.unchanged`` checks
    the size and mtime first and hashes only a same-size file with a new mtime.

    Args:
        path: Path to the file to check.
        versions_path: Path to versions.json for this collection.
        asset_key: Optional explicit key to look up in versions.json.
            If not provided, looks up by filename alone (legacy behavior).
        state: The run state. Without one, the call opens its own.

    Returns:
        True if file is unchanged (already tracked at current state),
        False if new or modified.
    """
    collection_dir = versions_path.parent
    if state is None:
        state = RunState.open(collection_dir.parent)
    versions_file = state.versions(collection_dir)
    if not versions_file.versions:
        return False

    asset = _find_asset(versions_file.versions[-1].assets, path, asset_key)
    if asset is None:
        return False
    return state.unchanged(path, asset)


def _find_asset(assets: dict[str, Asset], path: Path, asset_key: str | None) -> Asset | None:
    """Find the record of ``path`` under each key shape that ``add`` writes.

    The order is the explicit key, ``{item_id}/{filename}``, the bare filename,
    the converted ``{stem}.parquet``, and ``{item_id}/{stem}.parquet``.
    """
    item_id = path.parent.name
    parquet_name = f"{path.stem}.parquet"
    keys = (
        asset_key,
        f"{item_id}/{path.name}",
        path.name,
        parquet_name,
        f"{item_id}/{parquet_name}",
    )
    return next((assets[k] for k in keys if k is not None and k in assets), None)
