"""Collection ID validation and normalization."""

from __future__ import annotations

import re
import unicodedata
from typing import TYPE_CHECKING

from portolan_cli.scan.detect import is_filegdb

if TYPE_CHECKING:
    from pathlib import Path

    from portolan_core.config import ConfigSource

_RASTER_MIN_PATH_PARTS = 3

VALID_COLLECTION_ID_PATTERN: re.Pattern[str] = re.compile(
    r"^[a-z0-9][a-z0-9_-]*(?:/[a-z0-9][a-z0-9_-]*)*$"
)

# Pattern for invalid characters (anything not lowercase letter, number, hyphen, underscore, slash)
INVALID_CHAR_PATTERN: re.Pattern[str] = re.compile(r"[^a-z0-9_/-]")


class CollectionIdError(ValueError):
    """Raised when a collection ID cannot be normalized."""


def validate_collection_id(collection_id: str) -> tuple[bool, str | None]:
    """Validate a collection ID against naming conventions."""
    # Check for empty or whitespace-only
    if not collection_id or not collection_id.strip():
        return False, "Collection ID cannot be empty"

    # Check for spaces
    if " " in collection_id:
        return False, "Collection ID contains spaces - use hyphens or underscores instead"

    # Check for uppercase
    if collection_id != collection_id.lower():
        return False, "Collection ID contains uppercase letters - must be lowercase"

    # Check first character (must be letter or number, not hyphen/underscore)
    if not collection_id[0].isalnum():
        return False, "Collection ID must start with a letter or number"

    # Check for invalid characters
    invalid_match = INVALID_CHAR_PATTERN.search(collection_id)
    if invalid_match:
        char = invalid_match.group()
        return False, f"Collection ID contains invalid character: '{char}'"

    # Full pattern validation (should be redundant but ensures consistency)
    if not VALID_COLLECTION_ID_PATTERN.match(collection_id):
        return (
            False,
            "Collection ID must contain only lowercase letters, numbers, hyphens, and underscores",
        )

    return True, None


def _transliterate_to_ascii(text: str) -> str:
    """Transliterate non-ASCII characters to ASCII equivalents."""
    # Normalize to decomposed form (e -> e + combining accent)
    normalized = unicodedata.normalize("NFKD", text)
    # Encode to ASCII, ignoring non-ASCII characters
    return normalized.encode("ascii", "ignore").decode("ascii")


def normalize_collection_id(collection_id: str) -> str:
    """Normalize a collection ID to valid form."""
    # Check for empty input
    if not collection_id or not collection_id.strip():
        raise CollectionIdError("Collection ID cannot be empty")

    # Step 1: Lowercase
    result = collection_id.lower()

    # Step 2: Transliterate non-ASCII
    result = _transliterate_to_ascii(result)

    # Step 3: Replace invalid characters with hyphens (preserves slashes)
    result = INVALID_CHAR_PATTERN.sub("-", result)

    # Step 4: Collapse multiple consecutive hyphens
    result = re.sub(r"-+", "-", result)

    # Step 5: Strip leading/trailing hyphens and slashes
    result = result.strip("-/")

    # Step 6: Collapse double slashes
    result = re.sub(r"/+", "/", result)

    # Check if result is empty after normalization
    if not result:
        raise CollectionIdError(
            f"Collection ID '{collection_id}' cannot be normalized - no valid characters remain"
        )

    segments = result.split("/")
    fixed_segments = []
    for raw_segment in segments:
        # Strip hyphens from segment boundaries
        segment = raw_segment.strip("-")
        if segment:  # Skip empty segments
            # Prefix with 'n' if starts with non-alphanumeric (e.g., underscore)
            # Numbers are now valid at start, so only check isalnum()
            fixed_segments.append(segment if segment[0].isalnum() else f"n{segment}")

    if not fixed_segments:
        raise CollectionIdError(
            f"Collection ID '{collection_id}' cannot be normalized - no valid characters remain"
        )

    return "/".join(fixed_segments)


def resolve_collection_id(path: Path, catalog_root: Path) -> str:
    """Resolve collection ID from a file path."""
    # Get path relative to catalog root
    try:
        relative = path.resolve().relative_to(catalog_root.resolve())
    except ValueError as err:
        raise ValueError(f"Path {path} is outside catalog root {catalog_root}") from err

    # First component is the collection ID
    parts = relative.parts
    if not parts:
        raise ValueError(f"Cannot determine collection from path: {path}")

    # Skip the filename if path is a file
    if path.is_file() and len(parts) == 1:
        raise ValueError(f"File {path} must be in a subdirectory (collection)")

    return parts[0]


def infer_nested_collection_id(
    path: Path, catalog_root: Path, *, state: ConfigSource | None = None
) -> str:
    """Infer nested collection ID from a file or directory-based data asset."""
    from portolan_cli.scan.detect import is_hive_partition_dir

    # Get path relative to catalog root
    try:
        relative = path.resolve().relative_to(catalog_root.resolve())
    except ValueError as err:
        raise ValueError(f"Path {path} is outside catalog root {catalog_root}") from err

    # Get parent directory path (all components except filename)
    parts = relative.parts
    if not parts:
        raise ValueError(f"Cannot determine collection from path: {path}")

    is_gdb_suffix = path.is_dir() and path.name.lower().endswith(".gdb")
    is_asset = path.is_file() or is_filegdb(path) or is_gdb_suffix

    # Data asset must be in at least one subdirectory (collection)
    if is_asset and len(parts) == 1:
        raise ValueError(f"Data asset {path} must be in a subdirectory (collection)")

    from portolan_cli.plugins.formats import handler_for

    handler = handler_for(path, catalog_root, state=state)
    is_raster = handler is not None and handler.asset_level == "item"

    if is_raster:
        # Raster files need item subdirectory: collection/item/data.tif
        # Minimum depth: 3 parts (collection, item_dir, filename)
        if len(parts) < _RASTER_MIN_PATH_PARTS:
            raise ValueError(
                f"Raster file {path} must be in a subdirectory (collection/item/). "
                f"Raster data requires item-level organization."
            )
        # Return grandparent as collection (all but last 2 components)
        collection_parts = parts[:-2]
    else:
        # Vector files: parent directory = collection
        # Return parent as collection (all but last component)
        collection_parts = parts[:-1] if is_asset else parts

    collection_parts = tuple(
        part for part in collection_parts if is_hive_partition_dir(part) is None
    )

    if not collection_parts:
        raise ValueError(f"Data asset {path} must be in a subdirectory (collection)")

    return "/".join(collection_parts)
