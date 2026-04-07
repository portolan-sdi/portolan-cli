"""STAC GeoParquet generation for efficient collection queries.

This module provides optional items.parquet generation for collections with
many items, enabling efficient spatial/temporal queries without N HTTP requests.

Per issue #319:
- Optional but recommended for collections exceeding threshold (default: 100 items)
- Uses stac-geoparquet library for STAC → GeoParquet conversion
- Adds items.parquet link to collection.json with rel=items, type=application/x-parquet
- Tracks items.parquet in versions.json so push detects changes

Usage:
    from portolan_cli.stac_parquet import (
        count_items,
        should_suggest_parquet,
        generate_items_parquet,
        add_parquet_link_to_collection,
    )

    # Check if parquet generation is recommended
    if should_suggest_parquet(collection_path, threshold=100):
        parquet_path = generate_items_parquet(collection_path)
        add_parquet_link_to_collection(collection_path)
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

# Constants
PARQUET_FILENAME = "items.parquet"
PARQUET_MEDIA_TYPE = "application/x-parquet"


def count_items(collection_path: Path) -> int:
    """Count items in a collection from collection.json links.

    Args:
        collection_path: Path to collection directory containing collection.json.

    Returns:
        Number of items (links with rel="item").

    Raises:
        FileNotFoundError: If collection.json doesn't exist.
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        raise FileNotFoundError(f"collection.json not found in {collection_path}")

    data = json.loads(collection_json_path.read_text())
    links = data.get("links", [])

    return sum(1 for link in links if link.get("rel") == "item")


def should_suggest_parquet(collection_path: Path, threshold: int = 100) -> bool:
    """Check if parquet generation should be suggested for a collection.

    Args:
        collection_path: Path to collection directory.
        threshold: Item count above which parquet is recommended.

    Returns:
        True if item count exceeds threshold.
    """
    return count_items(collection_path) > threshold


def has_parquet_link(collection_path: Path) -> bool:
    """Check if collection.json has items.parquet (link or asset).

    Checks for both the rel="items" link and the collection-level asset.

    Args:
        collection_path: Path to collection directory.

    Returns:
        True if items.parquet link or asset exists.
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        return False

    data = json.loads(collection_json_path.read_text())

    # Check link
    links = data.get("links", [])
    has_link = any(
        link.get("type") == PARQUET_MEDIA_TYPE and link.get("rel") == "items" for link in links
    )
    if has_link:
        return True

    # Check asset (both old key "items_parquet" and new key "geoparquet-items")
    assets = data.get("assets", {})
    has_asset = (
        "geoparquet-items" in assets
        or "items_parquet" in assets
        or any(asset.get("href") == f"./{PARQUET_FILENAME}" for asset in assets.values())
    )
    return has_asset


def _load_item_dicts(collection_path: Path) -> list[dict[str, Any]]:
    """Load all STAC item dictionaries from a collection.

    Args:
        collection_path: Path to collection directory.

    Returns:
        List of STAC item dictionaries.

    Raises:
        ValueError: If no items found.
    """
    collection_json_path = collection_path / "collection.json"
    data = json.loads(collection_json_path.read_text())
    links = data.get("links", [])

    item_links = [link for link in links if link.get("rel") == "item"]

    if not item_links:
        raise ValueError(f"No items found in collection at {collection_path}")

    items = []
    missing_hrefs = []

    for link in item_links:
        href = link.get("href", "")
        # Resolve relative paths
        if href.startswith("./"):
            item_path = collection_path / href[2:]
        elif href.startswith("../"):
            item_path = (collection_path / href).resolve()
        else:
            item_path = collection_path / href

        if item_path.exists():
            item_data = json.loads(item_path.read_text())
            items.append(item_data)
        else:
            missing_hrefs.append(href)

    # Fail fast on stale links - items.parquet must be in sync with collection.json
    if missing_hrefs:
        missing_list = ", ".join(missing_hrefs[:5])
        if len(missing_hrefs) > 5:
            missing_list += f" ... and {len(missing_hrefs) - 5} more"
        raise ValueError(
            f"collection.json at {collection_path} has stale item links. "
            f"Missing items: {missing_list}"
        )

    if not items:
        raise ValueError(f"No items found in collection at {collection_path}")

    return items


def generate_items_parquet(collection_path: Path) -> Path:
    """Generate items.parquet from STAC items in a collection.

    Uses stac-geoparquet to convert all STAC items to GeoParquet format,
    enabling efficient spatial/temporal queries.

    Args:
        collection_path: Path to collection directory containing collection.json
            and item subdirectories.

    Returns:
        Path to generated items.parquet file.

    Raises:
        ValueError: If no items found in collection.
        ImportError: If stac-geoparquet not installed.
    """
    try:
        import stac_geoparquet.arrow
    except ImportError as e:
        raise ImportError(
            "stac-geoparquet is required for items.parquet generation. "
            "Install with: pip install stac-geoparquet"
        ) from e

    # Load all item dictionaries
    items = _load_item_dicts(collection_path)

    # Convert to Arrow using stac-geoparquet
    # parse_stac_items_to_arrow returns a RecordBatchReader
    record_batch_reader = stac_geoparquet.arrow.parse_stac_items_to_arrow(items)
    table = record_batch_reader.read_all()

    # Write to parquet with GeoParquet metadata
    output_path = collection_path / PARQUET_FILENAME
    stac_geoparquet.arrow.to_parquet(table, output_path)

    return output_path


def add_parquet_link_to_collection(collection_path: Path) -> None:
    """Add items.parquet link and asset to collection.json.

    Per ADR-0031 (collection-level assets), adds:
    1. A link with rel="items" and type="application/x-parquet"
    2. A collection-level asset for the GeoParquet file

    Idempotent - won't duplicate if already present.

    Args:
        collection_path: Path to collection directory.

    Raises:
        FileNotFoundError: If collection.json doesn't exist.
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        raise FileNotFoundError(f"collection.json not found in {collection_path}")

    data = json.loads(collection_json_path.read_text())
    modified = False

    # --- Add link (rel="items") ---
    links = data.get("links", [])
    has_link = any(
        link.get("type") == PARQUET_MEDIA_TYPE and link.get("rel") == "items" for link in links
    )

    if not has_link:
        parquet_link = {
            "rel": "items",
            "href": f"./{PARQUET_FILENAME}",
            "type": PARQUET_MEDIA_TYPE,
            "title": "STAC items as GeoParquet",
        }
        links.append(parquet_link)
        data["links"] = links
        modified = True

    # --- Add collection-level asset (per ADR-0031) ---
    # Uses community convention: key="geoparquet-items", roles=["stac-items"]
    # Ref: https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip
    assets = data.get("assets", {})
    asset_key = "geoparquet-items"

    # Check if asset already exists (by key or by href)
    has_asset = asset_key in assets or any(
        asset.get("href") == f"./{PARQUET_FILENAME}" for asset in assets.values()
    )

    if not has_asset:
        assets[asset_key] = {
            "href": f"./{PARQUET_FILENAME}",
            "type": PARQUET_MEDIA_TYPE,
            "title": "STAC items as GeoParquet",
            "roles": ["stac-items"],
        }
        data["assets"] = assets
        modified = True

    # Write back only if changes were made
    if modified:
        collection_json_path.write_text(json.dumps(data, indent=2))


def remove_parquet_link_from_collection(collection_path: Path) -> bool:
    """Remove items.parquet link and asset from collection.json.

    Removes both the rel="items" link and the collection-level asset.

    Args:
        collection_path: Path to collection directory.

    Returns:
        True if link or asset was removed, False if neither existed.
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        return False

    data = json.loads(collection_json_path.read_text())
    modified = False

    # Remove link
    links = data.get("links", [])
    original_link_count = len(links)
    links = [
        link
        for link in links
        if not (link.get("type") == PARQUET_MEDIA_TYPE and link.get("rel") == "items")
    ]
    if len(links) != original_link_count:
        data["links"] = links
        modified = True

    # Remove asset (by key or by href)
    # Check both old key (items_parquet) and new key (geoparquet-items)
    assets = data.get("assets", {})
    for key_to_remove in ("geoparquet-items", "items_parquet"):
        if key_to_remove in assets:
            del assets[key_to_remove]
            data["assets"] = assets
            modified = True
            break
    else:
        # Check by href in case it was added with a different key
        for key, asset in list(assets.items()):
            if asset.get("href") == f"./{PARQUET_FILENAME}":
                del assets[key]
                data["assets"] = assets
                modified = True
                break

    if modified:
        collection_json_path.write_text(json.dumps(data, indent=2))

    return modified


def track_parquet_in_versions(collection_path: Path) -> None:
    """Track items.parquet in versions.json so push detects changes.

    Updates the collection's versions.json to include items.parquet as an asset.
    This ensures `portolan push` will upload the generated parquet file.

    Args:
        collection_path: Path to collection directory.

    Raises:
        FileNotFoundError: If items.parquet or versions.json doesn't exist.
    """
    from portolan_cli.versions import (
        Asset,
        VersionsFile,
        add_version,
        parse_version,
        read_versions,
        write_versions,
    )

    parquet_path = collection_path / PARQUET_FILENAME
    versions_path = collection_path / "versions.json"

    if not parquet_path.exists():
        raise FileNotFoundError(f"items.parquet not found at {parquet_path}")

    # If no versions.json, create a minimal one
    if not versions_path.exists():
        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version=None,
            versions=[],
        )
    else:
        versions_file = read_versions(versions_path)

    # Compute checksum and stats for parquet file
    stat = parquet_path.stat()
    sha256 = hashlib.sha256(parquet_path.read_bytes()).hexdigest()

    # Href is relative to catalog root: {collection_name}/items.parquet
    collection_name = collection_path.name
    parquet_asset = Asset(
        sha256=sha256,
        size_bytes=stat.st_size,
        href=f"{collection_name}/{PARQUET_FILENAME}",
        mtime=stat.st_mtime,
    )

    # Determine next version
    if versions_file.current_version:
        major, minor, patch = parse_version(versions_file.current_version)
        new_version = f"{major}.{minor}.{patch + 1}"
    else:
        new_version = "1.0.0"

    # Add version with parquet asset
    # NOTE: add_version creates full snapshots per ADR-0005, which means push
    # will try to re-upload all existing assets. See GitHub issue for push
    # optimization to diff assets instead of uploading entire snapshots.
    updated = add_version(
        versions_file,
        version=new_version,
        assets={PARQUET_FILENAME: parquet_asset},
        breaking=False,
        message="Generated items.parquet for STAC GeoParquet queries",
    )

    write_versions(versions_path, updated)
