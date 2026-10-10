"""Catalog query API: list items, get item info, freshness checks."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from portolan_cli.constants import (
    MTIME_TOLERANCE_SECONDS,
)
from portolan_cli.format_types import FormatType
from portolan_cli.stac_links import catalog_collections, owned_item_hrefs
from portolan_cli.sync.checksums import compute_checksum, compute_dir_checksum
from portolan_cli.versions import (
    read_versions,
)

if TYPE_CHECKING:
    from datetime import datetime
    from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class ItemInfo:
    """Information about an item in the catalog.

    Attributes:
        item_id: STAC item identifier.
        collection_id: Parent collection identifier.
        format_type: Vector or raster format.
        bbox: Bounding box [min_x, min_y, max_x, max_y].
        asset_paths: Paths to data assets.
        title: Optional display title.
        description: Optional description.
        datetime: Acquisition/creation datetime.
    """

    item_id: str
    collection_id: str
    format_type: FormatType
    bbox: list[float]
    asset_paths: list[str] = field(default_factory=list)
    title: str | None = None
    description: str | None = None
    datetime: datetime | None = None


def _item_info(item_data: dict[str, Any], collection_id: str, fallback_id: str) -> ItemInfo:
    """Build an ItemInfo from a parsed STAC item."""
    # Determine format from assets
    format_type = FormatType.UNKNOWN
    asset_paths: list[str] = []
    for asset in item_data.get("assets", {}).values():
        href = asset.get("href", "")
        asset_paths.append(href)
        if href.endswith(".parquet"):
            format_type = FormatType.VECTOR
        elif href.endswith(".tif"):
            format_type = FormatType.RASTER

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
) -> bool:
    """Check if a file is unchanged compared to versions.json.

    Uses mtime as fast-path, falls back to sha256 if mtime changed.

    Args:
        path: Path to the file to check.
        versions_path: Path to versions.json for this collection.
        asset_key: Optional explicit key to look up in versions.json.
            If not provided, looks up by filename alone (legacy behavior).

    Returns:
        True if file is unchanged (already tracked at current state),
        False if new or modified.
    """
    if not versions_path.exists():
        return False

    versions_file = read_versions(versions_path)
    if not versions_file.versions:
        return False

    current_version = versions_file.versions[-1]

    # Look for this file in current version assets
    # Try explicit key first, then item-scoped key, then filename, then converted name
    asset = None
    filename = path.name

    if asset_key is not None:
        asset = current_version.assets.get(asset_key)

    if asset is None:
        # Try item-scoped key format: {item_id}/{filename}
        # This is how _update_versions stores multi-asset items
        item_id = path.parent.name
        item_scoped_key = f"{item_id}/{filename}"
        asset = current_version.assets.get(item_scoped_key)

    if asset is None:
        # Try bare filename (legacy format)
        asset = current_version.assets.get(filename)

    if asset is None:
        # Also check for stem.parquet (converted name)
        parquet_name = f"{path.stem}.parquet"
        asset = current_version.assets.get(parquet_name)

    if asset is None:
        # Try item-scoped with converted name
        item_id = path.parent.name
        item_scoped_parquet = f"{item_id}/{parquet_name}"
        asset = current_version.assets.get(item_scoped_parquet)

    if asset is None:
        return False

    # Get file stats once (used for both mtime and size checks)
    file_stat = path.stat()

    # For directory-format assets (e.g., FileGDB), skip the mtime fast-path and
    # size comparison — neither is reliable for directories. A directory's mtime
    # changes when its children change, but MTIME_TOLERANCE_SECONDS (2s, for
    # NFS/CIFS compatibility) would mask rapid modifications. Instead, go
    # directly to the content fingerprint (compute_dir_checksum), which hashes
    # the sorted (path, size, mtime) tuples of all files inside the directory.
    if path.is_dir():
        current_checksum = compute_dir_checksum(path)
        return current_checksum == asset.sha256

    # Fast path: mtime unchanged AND size unchanged → file is current
    # Both conditions must hold; size check catches fast overwrites within mtime tolerance
    mtime_unchanged = (
        asset.mtime is not None and abs(file_stat.st_mtime - asset.mtime) < MTIME_TOLERANCE_SECONDS
    )
    size_unchanged = asset.size_bytes is not None and file_stat.st_size == asset.size_bytes

    if mtime_unchanged and size_unchanged:
        return True

    # Medium path: size differs → definitely changed
    if asset.size_bytes is not None and file_stat.st_size != asset.size_bytes:
        return False

    # Slow path: mtime changed but size matches → check sha256
    current_checksum = compute_checksum(path)
    return current_checksum == asset.sha256
