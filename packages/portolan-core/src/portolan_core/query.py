"""Catalog query API: list items, get item info, freshness checks."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from portolan_core.format_families import family_of_asset
from portolan_core.run_state import RunState
from portolan_core.stac_links import catalog_collections, owned_item_hrefs

if TYPE_CHECKING:
    from datetime import datetime
    from pathlib import Path

    from portolan_core.versions import Asset

logger = logging.getLogger(__name__)


@dataclass
class ItemInfo:
    """Information about an item in the catalog."""

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
    """List items in a Portolan catalog."""
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
    """Get information about a specific item."""
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
    """Check if a file is unchanged compared to versions.json."""
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
    """Find the record of ``path`` under each key shape that ``add`` writes."""
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
