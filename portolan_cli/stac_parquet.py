"""STAC GeoParquet generation for efficient collection queries.

This module provides optional items.parquet generation for collections with
many items, enabling efficient spatial/temporal queries without N HTTP requests.

Per issue #319:
- Optional but recommended for collections exceeding threshold (default: 100 items)
- Uses stac-geoparquet library for STAC → GeoParquet conversion
- Adds items.parquet link to collection.json with rel=items, type=application/x-parquet

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
    """Check if collection.json has an items.parquet link.

    Args:
        collection_path: Path to collection directory.

    Returns:
        True if items.parquet link exists.
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        return False

    data = json.loads(collection_json_path.read_text())
    links = data.get("links", [])

    return any(
        link.get("type") == PARQUET_MEDIA_TYPE and link.get("rel") == "items" for link in links
    )


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
    """Add items.parquet link to collection.json.

    Adds a link with rel="items" and type="application/x-parquet" pointing
    to items.parquet. Idempotent - won't duplicate if already present.

    Args:
        collection_path: Path to collection directory.

    Raises:
        FileNotFoundError: If collection.json doesn't exist.
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        raise FileNotFoundError(f"collection.json not found in {collection_path}")

    data = json.loads(collection_json_path.read_text())
    links = data.get("links", [])

    # Check if link already exists (idempotent)
    existing = any(
        link.get("type") == PARQUET_MEDIA_TYPE and link.get("rel") == "items" for link in links
    )

    if existing:
        return  # Already has the link

    # Add the new link
    parquet_link = {
        "rel": "items",
        "href": f"./{PARQUET_FILENAME}",
        "type": PARQUET_MEDIA_TYPE,
        "title": "STAC items as GeoParquet",
    }
    links.append(parquet_link)
    data["links"] = links

    # Write back
    collection_json_path.write_text(json.dumps(data, indent=2))


def remove_parquet_link_from_collection(collection_path: Path) -> bool:
    """Remove items.parquet link from collection.json.

    Args:
        collection_path: Path to collection directory.

    Returns:
        True if link was removed, False if it didn't exist.
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        return False

    data = json.loads(collection_json_path.read_text())
    links = data.get("links", [])

    original_count = len(links)
    links = [
        link
        for link in links
        if not (link.get("type") == PARQUET_MEDIA_TYPE and link.get("rel") == "items")
    ]

    if len(links) == original_count:
        return False  # No link was removed

    data["links"] = links
    collection_json_path.write_text(json.dumps(data, indent=2))
    return True
