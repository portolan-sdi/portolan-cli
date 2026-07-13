"""Catalog query API: list items, get item info, freshness checks."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from portolan_cli.constants import (
    MTIME_TOLERANCE_SECONDS,
)
from portolan_cli.formats import (
    FormatType,
)
from portolan_cli.sync.checksums import compute_checksum, compute_dir_checksum
from portolan_cli.versions import (
    read_versions,
)

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


def list_items(
    catalog_root: Path,
    collection_id: str | None = None,
) -> list[ItemInfo]:
    """List items in a Portolan catalog.

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Optional collection to filter by.

    Returns:
        List of ItemInfo objects.
    """
    # Catalog at root level (per ADR-0023)
    catalog_path = catalog_root / "catalog.json"

    if not catalog_path.exists():
        return []

    items: list[ItemInfo] = []

    # Scan root-level directories for collections (per ADR-0023)
    for col_dir in catalog_root.iterdir():
        if not col_dir.is_dir():
            continue

        # Skip .portolan and hidden directories
        if col_dir.name.startswith("."):
            continue

        col_id = col_dir.name

        # Filter by collection if specified
        if collection_id and col_id != collection_id:
            continue

        collection_path = col_dir / "collection.json"
        if not collection_path.exists():
            continue

        # Load collection to get items
        collection_data = json.loads(collection_path.read_text(encoding="utf-8"))

        for link in collection_data.get("links", []):
            if link.get("rel") != "item":
                continue

            # Parse item href to get item ID
            item_href = link.get("href", "")
            # href is like ./item-id/item-id.json
            item_id = item_href.split("/")[1] if "/" in item_href else item_href

            # Load item
            item_path = col_dir / item_href.removeprefix("./")
            if not item_path.exists():
                continue

            item_data = json.loads(item_path.read_text(encoding="utf-8"))

            # Determine format from assets
            format_type = FormatType.UNKNOWN
            asset_paths: list[str] = []
            for _asset_key, asset in item_data.get("assets", {}).items():
                href = asset.get("href", "")
                asset_paths.append(href)
                if href.endswith(".parquet"):
                    format_type = FormatType.VECTOR
                elif href.endswith(".tif"):
                    format_type = FormatType.RASTER

            items.append(
                ItemInfo(
                    item_id=item_data.get("id", item_id),
                    collection_id=col_id,
                    format_type=format_type,
                    bbox=item_data.get("bbox", [0, 0, 0, 0]),
                    asset_paths=asset_paths,
                    title=item_data.get("properties", {}).get("title"),
                    description=item_data.get("properties", {}).get("description"),
                )
            )

    return items


def get_item_info(
    catalog_root: Path,
    stac_id: str,
) -> ItemInfo:
    """Get information about a specific item.

    Args:
        catalog_root: Root directory of the catalog.
        stac_id: STAC identifier in format "collection/item".

    Returns:
        ItemInfo for the requested item.

    Raises:
        KeyError: If the item doesn't exist.
    """
    if "/" not in stac_id:
        raise KeyError(f"Item not found: {stac_id} (expected format: collection/item)")

    collection_id, item_id = stac_id.split("/", 1)

    # STAC at root level (per ADR-0023)
    item_path = catalog_root / collection_id / item_id / f"{item_id}.json"

    if not item_path.exists():
        raise KeyError(f"Item not found: {stac_id}")

    item_data = json.loads(item_path.read_text(encoding="utf-8"))

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

    return ItemInfo(
        item_id=item_data.get("id", item_id),
        collection_id=collection_id,
        format_type=format_type,
        bbox=item_data.get("bbox", [0, 0, 0, 0]),
        asset_paths=asset_paths,
        title=item_data.get("properties", {}).get("title"),
        description=item_data.get("properties", {}).get("description"),
    )


def is_current(
    path: Path,
    versions_path: Path,
    *,
    asset_key: str | None = None,
) -> bool:
    """Check if a file is unchanged compared to versions.json.

    Uses mtime as fast-path (per ADR-0017), falls back to sha256 if mtime changed.

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
