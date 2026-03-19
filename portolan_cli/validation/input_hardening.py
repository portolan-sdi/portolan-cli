"""Input validation and hardening against agent hallucinations.

This module provides validation functions to protect against common agent
hallucination patterns identified in agent-native CLI research:

- Path traversals (../../.ssh)
- Embedded query parameters in IDs (fileId?fields=name)
- Pre-encoded strings that get double-encoded (%2e%2e)
- Control characters in string inputs
- Malicious URL constructions

Reference: https://jpoehnelt.dev/blog/agent-native-cli/

All user-facing inputs should be validated through these functions before
use in file operations, API calls, or database queries.
"""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse


class InputValidationError(ValueError):
    """Raised when input validation fails.

    This is a ValueError subclass to maintain compatibility with existing
    error handling while providing semantic clarity.
    """


def validate_safe_path(path: Path, base_dir: Path | None = None) -> Path:
    """Validate a file path for safety against path traversal attacks.

    Agents may hallucinate path traversals like ../../.ssh by confusing
    path segment context. This function canonicalizes the path and ensures
    it doesn't escape the base directory.

    Args:
        path: The path to validate (may be relative or absolute).
        base_dir: Base directory to restrict access to. If None, uses CWD.

    Returns:
        The resolved absolute path if valid.

    Raises:
        InputValidationError: If path traversal is detected.

    Examples:
        >>> validate_safe_path(Path("data/file.parquet"), Path.cwd())
        PosixPath('/current/dir/data/file.parquet')

        >>> validate_safe_path(Path("../../.ssh/id_rsa"), Path.cwd())
        InputValidationError: Path traversal detected
    """
    if base_dir is None:
        base_dir = Path.cwd()

    # Resolve to absolute path (follows symlinks, resolves ..)
    try:
        base_resolved = base_dir.resolve()
        # If path is relative, join with base_dir before resolving
        # If path is absolute, resolve() will use it as-is
        if not path.is_absolute():
            resolved = (base_resolved / path).resolve()
        else:
            resolved = path.resolve()
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
    """Validate a collection ID for safety and STAC compliance.

    Agents may hallucinate:
    - Control characters (< ASCII 0x20)
    - Query parameters (?fields=name)
    - URL fragments (#section)
    - Pre-encoded strings (%2e%2e)

    STAC collection IDs should be URL-safe strings without special characters.

    Args:
        collection_id: The collection ID to validate.

    Returns:
        The validated collection ID.

    Raises:
        InputValidationError: If validation fails.

    Examples:
        >>> validate_collection_id("census-2020")
        'census-2020'

        >>> validate_collection_id("census?fields=name")
        InputValidationError: Query parameters not allowed
    """
    if not collection_id:
        raise InputValidationError("Collection ID cannot be empty")

    # Reject control characters (< ASCII 0x20, including newlines/tabs)
    if any(ord(c) < 0x20 for c in collection_id):
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

    # Reject backslashes (forward slashes allowed for nested catalogs per ADR-0032)
    if "\\" in collection_id:
        raise InputValidationError("Backslashes not allowed in collection ID")

    # STAC recommendation: lowercase with hyphens (ADR-0032 allows nested paths)
    # Pattern: segments separated by /, each segment is alphanumeric with hyphens/underscores
    # Segments CAN start with numbers (e.g., "rivers/2020/q1" for year-based organization)
    if not re.match(r"^[a-z0-9][a-z0-9_-]*(?:/[a-z0-9][a-z0-9_-]*)*$", collection_id):
        raise InputValidationError(
            f"Collection ID '{collection_id}' invalid: use only lowercase letters, numbers, "
            "hyphens, underscores, forward slashes (STAC best practice + ADR-0032)"
        )

    return collection_id


def validate_item_id(item_id: str) -> str:
    """Validate a STAC item ID for safety and compliance.

    Similar to collection ID validation but allows slightly more flexibility
    (STAC items may include version numbers, dates, etc.).

    Args:
        item_id: The item ID to validate.

    Returns:
        The validated item ID.

    Raises:
        InputValidationError: If validation fails.
    """
    if not item_id:
        raise InputValidationError("Item ID cannot be empty")

    # Same basic checks as collection ID
    if any(ord(c) < 0x20 for c in item_id):
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


def validate_remote_url(url: str) -> str:
    """Validate a remote storage URL for push/pull/sync operations.

    Validates S3, GCS, Azure Blob Storage URLs and rejects:
    - Path traversals in bucket paths
    - Unsupported schemes
    - Malformed URLs

    Args:
        url: The remote URL to validate (e.g., s3://bucket/path).

    Returns:
        The validated URL.

    Raises:
        InputValidationError: If validation fails.

    Examples:
        >>> validate_remote_url("s3://my-bucket/catalog")
        's3://my-bucket/catalog'

        >>> validate_remote_url("s3://bucket/../etc/passwd")
        InputValidationError: Path traversal in URL
    """
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
        if component and any(ord(c) < 0x20 for c in component):
            raise InputValidationError(f"Control characters not allowed in URL: {url}")

    return url


def validate_config_key(key: str) -> str:
    """Validate a configuration key for portolan config set.

    Args:
        key: The config key to validate.

    Returns:
        The validated key.

    Raises:
        InputValidationError: If validation fails.
    """
    if not key:
        raise InputValidationError("Config key cannot be empty")

    # Reject control characters
    if any(ord(c) < 0x20 for c in key):
        raise InputValidationError("Control characters not allowed in config key")

    # Config keys should be simple identifiers
    if not re.match(r"^[a-z][a-z0-9_]*$", key):
        raise InputValidationError(
            f"Config key '{key}' should match pattern: lowercase alphanumeric with underscores"
        )

    return key


def validate_config_value(value: str, key: str) -> str:
    """Validate a configuration value for portolan config set.

    Args:
        value: The config value to validate.
        key: The config key (used for context-specific validation).

    Returns:
        The validated value.

    Raises:
        InputValidationError: If validation fails.
    """
    if not value:
        raise InputValidationError(f"Config value for '{key}' cannot be empty")

    # Reject control characters (except for multiline values)
    if any(ord(c) < 0x20 and c != "\n" for c in value):
        raise InputValidationError(f"Control characters not allowed in config value for '{key}'")

    # Key-specific validation
    if key == "remote":
        validate_remote_url(value)

    return value
