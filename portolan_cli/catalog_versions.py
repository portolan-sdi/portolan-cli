"""Catalog-level versions.json management for collection tracking.

Per ADR-0005, the catalog-level versions.json tracks which collections exist
in the catalog, when each was created/modified, and summary metadata.

Structure:
    {
        "schema_version": "1.0.0",
        "catalog_id": "my-catalog",
        "created": "2024-01-01T00:00:00Z",
        "collections": {
            "my-collection": {
                "created": "2024-01-01T00:00:00Z",
                "modified": "2024-06-01T12:00:00Z",
                "current_version": "1.2.0",
                "item_count": 5
            }
        }
    }
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class CollectionEntry:
    """Entry for a single collection in catalog-level versions.json.

    Attributes:
        created: ISO 8601 timestamp when the collection was first tracked.
        current_version: Current version string of the collection.
        item_count: Number of items in the collection.
        modified: ISO 8601 timestamp of last modification (optional).
    """

    created: str
    current_version: str
    item_count: int
    modified: str | None = None


@dataclass
class CatalogVersionsFile:
    """The catalog-level versions.json structure.

    Attributes:
        schema_version: Schema version for the versions.json format.
        catalog_id: Unique identifier for the catalog.
        created: ISO 8601 timestamp when the catalog was initialized.
        collections: Mapping of collection_id to CollectionEntry.
    """

    schema_version: str
    catalog_id: str
    created: str
    collections: dict[str, CollectionEntry] = field(default_factory=dict)


def read_catalog_versions(catalog_root: Path) -> dict[str, Any]:
    """Read the catalog-level versions.json file.

    Args:
        catalog_root: Root directory of the catalog.

    Returns:
        Dictionary containing the catalog versions data.

    Raises:
        FileNotFoundError: If versions.json doesn't exist.
        ValueError: If the JSON is invalid.
    """
    versions_path = catalog_root / "versions.json"

    if not versions_path.exists():
        raise FileNotFoundError(f"versions.json not found: {versions_path}")

    try:
        data: dict[str, Any] = json.loads(versions_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in versions.json: {e}") from e

    return data


def write_catalog_versions(catalog_root: Path, catalog_versions: CatalogVersionsFile) -> None:
    """Write catalog-level versions.json atomically.

    Uses atomic write pattern (write to temp file, then rename) to prevent
    corruption from interrupted writes.

    Args:
        catalog_root: Root directory of the catalog.
        catalog_versions: The CatalogVersionsFile to serialize.
    """
    catalog_root.mkdir(parents=True, exist_ok=True)
    versions_path = catalog_root / "versions.json"

    # Serialize to dict
    data = _serialize_catalog_versions(catalog_versions)
    content = json.dumps(data, indent=2, ensure_ascii=False) + "\n"

    # Atomic write: write to temp file in same directory, then rename
    fd, tmp_path = tempfile.mkstemp(
        dir=catalog_root,
        prefix=".versions_",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        # Atomic rename (POSIX guarantees atomicity for same-filesystem renames)
        os.replace(tmp_path, versions_path)
    except Exception:
        # Clean up temp file on failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _serialize_catalog_versions(catalog_versions: CatalogVersionsFile) -> dict[str, Any]:
    """Serialize a CatalogVersionsFile to a JSON-compatible dictionary.

    Args:
        catalog_versions: The CatalogVersionsFile to serialize.

    Returns:
        Dictionary suitable for JSON serialization.
    """
    collections_dict: dict[str, dict[str, Any]] = {}
    for collection_id, entry in catalog_versions.collections.items():
        entry_dict: dict[str, Any] = {
            "created": entry.created,
            "current_version": entry.current_version,
            "item_count": entry.item_count,
        }
        if entry.modified is not None:
            entry_dict["modified"] = entry.modified
        collections_dict[collection_id] = entry_dict

    return {
        "schema_version": catalog_versions.schema_version,
        "catalog_id": catalog_versions.catalog_id,
        "created": catalog_versions.created,
        "collections": collections_dict,
    }


def _parse_catalog_versions(data: dict[str, Any]) -> CatalogVersionsFile:
    """Parse a dictionary into a CatalogVersionsFile object.

    Args:
        data: Parsed JSON dictionary.

    Returns:
        CatalogVersionsFile object.

    Raises:
        ValueError: If required fields are missing or invalid.
    """
    try:
        schema_version = data["schema_version"]
        catalog_id = data["catalog_id"]
        created = data["created"]
        collections_data = data.get("collections", {})
    except KeyError as e:
        raise ValueError(f"Invalid catalog versions.json: missing field {e}") from e

    collections: dict[str, CollectionEntry] = {}
    for collection_id, entry_data in collections_data.items():
        collections[collection_id] = CollectionEntry(
            created=entry_data["created"],
            current_version=entry_data.get("current_version", "1.0.0"),
            item_count=entry_data.get("item_count", 0),
            modified=entry_data.get("modified"),
        )

    return CatalogVersionsFile(
        schema_version=schema_version,
        catalog_id=catalog_id,
        created=created,
        collections=collections,
    )


def update_catalog_versions_collection(
    catalog_root: Path,
    collection_id: str,
    item_count: int,
    *,
    current_version: str | None = None,
) -> None:
    """Update or add a collection entry in the catalog-level versions.json.

    If the collection doesn't exist, it will be created with the current timestamp.
    If it already exists, item_count and current_version will be updated,
    and the modified timestamp will be set.

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Unique identifier for the collection.
        item_count: Number of items in the collection.
        current_version: Optional version string for the collection.
    """
    versions_path = catalog_root / "versions.json"

    # Read existing data
    if versions_path.exists():
        data = json.loads(versions_path.read_text(encoding="utf-8"))
        catalog_versions = _parse_catalog_versions(data)
    else:
        raise FileNotFoundError(f"versions.json not found: {versions_path}")

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # Check if collection already exists
    if collection_id in catalog_versions.collections:
        # Update existing entry
        existing = catalog_versions.collections[collection_id]
        catalog_versions.collections[collection_id] = CollectionEntry(
            created=existing.created,  # Preserve original created timestamp
            current_version=current_version or existing.current_version,
            item_count=item_count,
            modified=now,
        )
    else:
        # Create new entry
        catalog_versions.collections[collection_id] = CollectionEntry(
            created=now,
            current_version=current_version or "1.0.0",
            item_count=item_count,
            modified=None,  # No modified timestamp for new entries
        )

    write_catalog_versions(catalog_root, catalog_versions)


def get_collection_info(catalog_root: Path, collection_id: str) -> dict[str, Any] | None:
    """Get information about a specific collection from catalog versions.json.

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Unique identifier for the collection.

    Returns:
        Dictionary with collection info, or None if collection doesn't exist.
    """
    try:
        data = read_catalog_versions(catalog_root)
    except FileNotFoundError:
        return None

    collections: dict[str, dict[str, Any]] = data.get("collections", {})
    return collections.get(collection_id)


def remove_collection_from_catalog_versions(catalog_root: Path, collection_id: str) -> None:
    """Remove a collection entry from the catalog-level versions.json.

    If the collection doesn't exist, this is a no-op.

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Unique identifier for the collection to remove.
    """
    versions_path = catalog_root / "versions.json"

    if not versions_path.exists():
        return

    data = json.loads(versions_path.read_text(encoding="utf-8"))
    catalog_versions = _parse_catalog_versions(data)

    if collection_id in catalog_versions.collections:
        del catalog_versions.collections[collection_id]
        write_catalog_versions(catalog_root, catalog_versions)
