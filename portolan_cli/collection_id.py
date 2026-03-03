"""Collection ID validation and normalization.

Per portolan-spec/structure.md, collection IDs SHOULD:
- Contain only lowercase letters, numbers, hyphens, and underscores
- Start with a letter
- Be unique within the catalog

This module provides:
- validate_collection_id(): Check if an ID is valid
- normalize_collection_id(): Convert invalid ID to valid form
- CollectionIdError: Raised when normalization fails
"""

from __future__ import annotations

import re
import unicodedata

# Pattern for valid collection IDs:
# - Start with lowercase letter
# - Followed by lowercase letters, numbers, hyphens, or underscores
VALID_COLLECTION_ID_PATTERN: re.Pattern[str] = re.compile(r"^[a-z][a-z0-9_-]*$")

# Pattern for invalid characters (anything not lowercase letter, number, hyphen, underscore)
INVALID_CHAR_PATTERN: re.Pattern[str] = re.compile(r"[^a-z0-9_-]")


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

    # Check first character
    if not collection_id[0].isalpha():
        return False, "Collection ID must start with a letter"

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
        >>> _transliterate_to_ascii("donnees")
        'donnees'
        >>> _transliterate_to_ascii("naive")
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
    3. Replace invalid characters (spaces, special chars) with hyphens
    4. Collapse multiple consecutive hyphens
    5. Strip leading/trailing hyphens
    6. Prefix with 'n' if starts with a number

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

    # Step 3: Replace invalid characters with hyphens
    result = INVALID_CHAR_PATTERN.sub("-", result)

    # Step 4: Collapse multiple consecutive hyphens
    result = re.sub(r"-+", "-", result)

    # Step 5: Strip leading/trailing hyphens
    result = result.strip("-")

    # Check if result is empty after normalization
    if not result:
        raise CollectionIdError(
            f"Collection ID '{collection_id}' cannot be normalized - no valid characters remain"
        )

    # Step 6: Prefix with 'n' if starts with a number
    if result[0].isdigit():
        result = f"n{result}"

    return result
