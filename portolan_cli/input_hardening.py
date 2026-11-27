"""Input validation and hardening against agent hallucinations.

The rules are strict on purpose: do not relax a validator to accept an edge case.
"""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse

# ASCII control characters sit below the space character.
_FIRST_PRINTABLE = 0x20


class InputValidationError(ValueError):
    """Raised when input validation fails; the JSON error envelope reports this class name."""


def validate_safe_path(path: Path, base_dir: Path | None = None) -> Path:
    """Validate a file path for safety against path traversal attacks."""
    if base_dir is None:
        base_dir = Path.cwd()

    # Resolve to absolute path (follows symlinks, resolves ..)
    try:
        base_resolved = base_dir.resolve()
        # If path is relative, join with base_dir before resolving
        # If path is absolute, resolve() will use it as-is
        resolved = (base_resolved / path).resolve() if not path.is_absolute() else path.resolve()
    except (OSError, RuntimeError) as e:
        raise InputValidationError(f"Cannot resolve path {path}: {e}") from e

    # Check if resolved path is under base directory
    try:
        resolved.relative_to(base_resolved)
    except ValueError as e:
        raise InputValidationError(
            f"Path traversal detected: {path} resolves outside {base_dir}"
        ) from e

    return resolved


def validate_collection_id(collection_id: str) -> str:
    """Validate a collection ID for safety and STAC compliance."""
    if not collection_id:
        raise InputValidationError("Collection ID cannot be empty")

    # Reject control characters (< ASCII 0x20, including newlines/tabs)
    if any(ord(c) < _FIRST_PRINTABLE for c in collection_id):
        raise InputValidationError("Control characters not allowed in collection ID")

    # Reject query parameters and fragments (common agent hallucination)
    if "?" in collection_id:
        raise InputValidationError(
            "Query parameters not allowed in collection ID (did agent hallucinate URL syntax?)"
        )
    if "#" in collection_id:
        raise InputValidationError("URL fragments not allowed in collection ID")

    # Reject pre-encoded strings (agents double-encode %2e%2e → %252e%252e)
    if "%" in collection_id:
        raise InputValidationError(
            "URL-encoded characters not allowed in collection ID (provide raw ID, not encoded)"
        )

    # Reject backslashes (forward slashes allowed for nested catalogs)
    if "\\" in collection_id:
        raise InputValidationError("Backslashes not allowed in collection ID")

    if not re.match(r"^[a-z0-9][a-z0-9_-]*(?:/[a-z0-9][a-z0-9_-]*)*$", collection_id):
        raise InputValidationError(
            f"Collection ID '{collection_id}' invalid: use only lowercase letters, numbers, "
            "hyphens, underscores, forward slashes (STAC best practice + )"
        )

    return collection_id


def validate_item_id(item_id: str) -> str:
    """Validate a STAC item ID for safety and compliance."""
    if not item_id:
        raise InputValidationError("Item ID cannot be empty")

    # Same basic checks as collection ID
    if any(ord(c) < _FIRST_PRINTABLE for c in item_id):
        raise InputValidationError("Control characters not allowed in item ID")

    if "?" in item_id or "#" in item_id:
        raise InputValidationError("Query parameters/fragments not allowed in item ID")

    if "%" in item_id:
        raise InputValidationError("URL-encoded characters not allowed in item ID")

    # Item IDs can have more variation (version numbers, dates)
    # But still reject obvious path traversals
    if ".." in item_id or "/" in item_id or "\\" in item_id:
        raise InputValidationError("Path separators/traversals not allowed in item ID")

    return item_id


_CATALOG_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")

#: Upper bound on a catalog ID. The ID reaches catalog.json, versions.json, and
#: every href a consumer resolves, so it stays inside the 255-byte name limit
#: that common filesystems and object stores impose.
MAX_CATALOG_ID_LENGTH = 255


def validate_catalog_id(catalog_id: str) -> str:
    """Validate a STAC catalog ID for safety and compliance."""
    if not catalog_id:
        raise InputValidationError("Catalog ID cannot be empty")

    if len(catalog_id) > MAX_CATALOG_ID_LENGTH:
        raise InputValidationError(
            f"Catalog ID is too long: {len(catalog_id)} characters, limit {MAX_CATALOG_ID_LENGTH}"
        )

    if not _CATALOG_ID_PATTERN.match(catalog_id):
        raise InputValidationError(
            f"Invalid catalog ID {catalog_id!r}: use only letters, digits, hyphens, and underscores"
        )

    return catalog_id


def validate_remote_url(url: str) -> str:
    """Validate a remote storage URL for push/pull/sync operations."""
    if not url:
        raise InputValidationError("Remote URL cannot be empty")

    try:
        parsed = urlparse(url)
    except ValueError as e:
        raise InputValidationError(f"Malformed URL: {url}") from e

    # Validate scheme
    allowed_schemes = {"s3", "gs", "az", "http", "https"}
    if parsed.scheme not in allowed_schemes:
        raise InputValidationError(
            f"Unsupported URL scheme '{parsed.scheme}'. "
            f"Allowed: {', '.join(sorted(allowed_schemes))}"
        )

    # Validate that host/bucket/container is present
    if not parsed.netloc:
        raise InputValidationError(
            f"URL missing host/bucket/container: {url}. "
            "URLs must include a destination (e.g., s3://bucket/path, https://host/path)"
        )

    # Reject path traversals in URL path
    if ".." in parsed.path:
        raise InputValidationError(f"Path traversal in URL not allowed: {url}")

    # Reject control characters in any URL component
    for component in [parsed.netloc, parsed.path, parsed.query, parsed.fragment]:
        if component and any(ord(c) < _FIRST_PRINTABLE for c in component):
            raise InputValidationError(f"Control characters not allowed in URL: {url}")

    return url


def validate_config_key(key: str) -> str:
    """Validate a configuration key for portolan config set."""
    if not key:
        raise InputValidationError("Config key cannot be empty")

    # Reject control characters
    if any(ord(c) < _FIRST_PRINTABLE for c in key):
        raise InputValidationError("Control characters not allowed in config key")

    # Config keys should be simple identifiers
    if not re.match(r"^[a-z][a-z0-9_]*$", key):
        raise InputValidationError(
            f"Config key '{key}' should match pattern: lowercase alphanumeric with underscores"
        )

    return key


def validate_config_value(value: str, key: str) -> str:
    """Validate a configuration value for portolan config set."""
    if not value:
        raise InputValidationError(f"Config value for '{key}' cannot be empty")

    # Reject control characters (except for multiline values)
    if any(ord(c) < _FIRST_PRINTABLE and c != "\n" for c in value):
        raise InputValidationError(f"Control characters not allowed in config value for '{key}'")

    # Key-specific validation
    if key == "remote":
        validate_remote_url(value)

    return value
