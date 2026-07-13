"""Collection ID validation and normalization.

Per portolan-spec/structure.md and ADR-0032, collection IDs SHOULD:
- Contain only lowercase letters, numbers, hyphens, underscores, and forward slashes
- Start with a letter or number (not hyphen/underscore)
- Be unique within the catalog
- Support nested paths (e.g., climate/hittekaart, rivers/2020/q1)

This module provides:
- validate_collection_id(): Check if an ID is valid
- normalize_collection_id(): Convert invalid ID to valid form
- CollectionIdError: Raised when normalization fails
"""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path

from portolan_cli.formats import FormatType, detect_format
from portolan_cli.scan.detect import is_filegdb

# Pattern for valid collection IDs (supports path syntax per ADR-0032):
# - Start with lowercase letter or number (year-based organization like 2020/)
# - Followed by lowercase letters, numbers, hyphens, underscores, or forward slashes
# - No leading/trailing/double slashes
# - Each segment starts with letter or number (not hyphen/underscore)
VALID_COLLECTION_ID_PATTERN: re.Pattern[str] = re.compile(
    r"^[a-z0-9][a-z0-9_-]*(?:/[a-z0-9][a-z0-9_-]*)*$"
)

# Pattern for invalid characters (anything not lowercase letter, number, hyphen, underscore, slash)
INVALID_CHAR_PATTERN: re.Pattern[str] = re.compile(r"[^a-z0-9_/-]")


class CollectionIdError(ValueError):
    """Raised when a collection ID cannot be normalized."""

    pass


def validate_collection_id(collection_id: str) -> tuple[bool, str | None]:
    """Validate a collection ID against naming conventions.

    Args:
        collection_id: The collection ID to validate.

    Returns:
        Tuple of (is_valid, error_message).
        If valid, returns (True, None).
        If invalid, returns (False, "description of the problem").
    """
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
    """Transliterate non-ASCII characters to ASCII equivalents.

    Uses Unicode NFKD normalization to decompose characters, then
    encodes to ASCII, ignoring characters that can't be represented.

    Example:
        >>> _transliterate_to_ascii("données")
        'donnees'
        >>> _transliterate_to_ascii("naïve")
        'naive'
    """
    # Normalize to decomposed form (e -> e + combining accent)
    normalized = unicodedata.normalize("NFKD", text)
    # Encode to ASCII, ignoring non-ASCII characters
    return normalized.encode("ascii", "ignore").decode("ascii")


def normalize_collection_id(collection_id: str) -> str:
    """Normalize a collection ID to valid form.

    Transformations applied:
    1. Lowercase
    2. Transliterate non-ASCII to ASCII
    3. Replace invalid characters (spaces, special chars) with hyphens (preserves slashes)
    4. Collapse multiple consecutive hyphens
    5. Strip leading/trailing hyphens and slashes
    6. Collapse double slashes
    7. Clean up segment boundaries (strip hyphens)

    Note: Segments CAN start with numbers (year-based organization like 2020/).

    Args:
        collection_id: The collection ID to normalize.

    Returns:
        Normalized collection ID.

    Raises:
        CollectionIdError: If input is empty or normalizes to empty string.
    """
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

    # Step 7: Clean up segments
    # Numbers at segment start are valid (year-based organization like 2020/)
    # But hyphens/underscores at start still need fixing
    segments = result.split("/")
    fixed_segments = []
    for segment in segments:
        # Strip hyphens from segment boundaries
        segment = segment.strip("-")
        if segment:  # Skip empty segments
            # Prefix with 'n' if starts with non-alphanumeric (e.g., underscore)
            # Numbers are now valid at start, so only check isalnum()
            if segment and not segment[0].isalnum():
                segment = f"n{segment}"
            fixed_segments.append(segment)

    if not fixed_segments:
        raise CollectionIdError(
            f"Collection ID '{collection_id}' cannot be normalized - no valid characters remain"
        )

    return "/".join(fixed_segments)


def resolve_collection_id(path: Path, catalog_root: Path) -> str:
    """Resolve collection ID from a file path.

    Per ADR-0022: First path component (relative to catalog root) = collection ID.

    Args:
        path: Path to the file.
        catalog_root: Root directory of the catalog.

    Returns:
        Collection ID (first directory component relative to catalog).

    Raises:
        ValueError: If path is not inside catalog root.
    """
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


def infer_nested_collection_id(path: Path, catalog_root: Path) -> str:
    """Infer nested collection ID from a file or directory-based data asset.

    Per ADR-0031 (Collection-Level Assets for Vector Data) and ADR-0032
    (Nested Catalogs with Flat Collections), the collection depth depends
    on the format type:

    - **Vector data**: Parent directory = collection (collection-level asset)
      Example: demographics/boundaries.parquet -> collection = "demographics"

    - **Raster data**: Grandparent directory = collection, parent = item
      Example: 2025/tile1/scene.tif -> collection = "2025", item = "tile1"

    Per Issue #443, Hive partition directories (key=value format) are filtered
    out and NOT included in the collection ID:
      Example: sites/contours/gms_feature_id=abc/data.parquet -> "sites/contours"

    Directory-based formats like FileGDB (*.gdb) are treated as vector data
    (collection-level assets).

    Examples:
        # Vector (collection-level)
        climate/hittekaart/data.parquet -> "climate/hittekaart"
        demographics/boundaries.geojson -> "demographics"
        ocha/my_data.gdb -> "ocha"  (FileGDB directory)

        # Raster (item-level, needs subdirectory)
        imagery/2025/tile1/scene.tif -> "imagery/2025"
        satellite/scene-001/B04.tif -> "satellite"

        # Hive partitions (filtered out)
        sites/contours/gms_feature_id=abc/data.parquet -> "sites/contours"
        data/year=2024/month=01/file.parquet -> "data"

    Args:
        path: Path to the file or directory-based data asset (e.g., FileGDB).
        catalog_root: Root directory of the catalog.

    Returns:
        Collection ID (nested path relative to catalog root, excluding Hive partitions).

    Raises:
        ValueError: If path is not inside catalog root, at root level, or
            if raster data lacks required item subdirectory structure.
    """
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

    # A path is treated as a "data asset" (not a collection) if it's a file
    # OR a FileGDB directory. FileGDB directories (*.gdb) contain the actual
    # data - they're assets, not organizational collections (Issue #259).
    #
    # For FileGDB detection, we use:
    # 1. is_filegdb() - content inspection (internal .gdbtable files or 'gdb' marker)
    # 2. Suffix fallback - handles empty/incomplete/corrupted FileGDB directories
    is_gdb_suffix = path.is_dir() and path.name.lower().endswith(".gdb")
    is_asset = path.is_file() or is_filegdb(path) or is_gdb_suffix

    # Data asset must be in at least one subdirectory (collection)
    if is_asset and len(parts) == 1:
        raise ValueError(f"Data asset {path} must be in a subdirectory (collection)")

    # Detect format type to determine collection depth (ADR-0031)
    # - Vector: parent directory = collection (collection-level asset)
    # - Raster: grandparent = collection, parent = item (item-level asset)
    format_type = detect_format(path)
    is_raster = format_type == FormatType.RASTER

    if is_raster:
        # Raster files need item subdirectory: collection/item/data.tif
        # Minimum depth: 3 parts (collection, item_dir, filename)
        if len(parts) < 3:
            raise ValueError(
                f"Raster file {path} must be in a subdirectory (collection/item/). "
                f"Per ADR-0031, raster data requires item-level organization."
            )
        # Return grandparent as collection (all but last 2 components)
        collection_parts = parts[:-2]
    else:
        # Vector files: parent directory = collection
        # Return parent as collection (all but last component)
        collection_parts = parts[:-1] if is_asset else parts

    # Filter out Hive partition directories (key=value pattern)
    # Per Issue #443: partitions should not be part of collection ID
    collection_parts = tuple(
        part for part in collection_parts if is_hive_partition_dir(part) is None
    )

    if not collection_parts:
        raise ValueError(f"Data asset {path} must be in a subdirectory (collection)")

    return "/".join(collection_parts)
