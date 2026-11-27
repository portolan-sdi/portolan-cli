"""Filtering for extraction operations."""

from __future__ import annotations

from fnmatch import fnmatch, fnmatchcase


def filter_services(
    services: list[str],
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    *,
    case_sensitive: bool = True,
) -> list[str]:
    """Filter services by include/exclude glob patterns."""
    result = services

    # Apply include filter (if provided and non-empty)
    if include:
        result = [s for s in result if _matches_any_glob(s, include, case_sensitive)]

    # Apply exclude filter (if provided and non-empty)
    if exclude:
        result = [s for s in result if not _matches_any_glob(s, exclude, case_sensitive)]

    return result


def _matches_any_glob(
    value: str,
    patterns: list[str],
    case_sensitive: bool,
) -> bool:
    """Check if value matches any of the glob patterns."""
    for pattern in patterns:
        if case_sensitive:
            if fnmatchcase(value, pattern):
                return True
        elif fnmatch(value.lower(), pattern.lower()):
            return True
    return False


def is_glob(pattern: str) -> bool:
    """Return True when ``pattern`` holds a glob character, so it names no single layer."""
    return any(char in pattern for char in "*?[")


def layer_matches(layer_id: int | str, name: str, pattern: str) -> bool:
    """Return True when ``pattern`` selects the layer."""
    pattern = pattern.strip()
    if pattern.isdigit():
        return str(layer_id) == pattern
    lowered = pattern.lower()
    return any(fnmatch(candidate.lower(), lowered) for candidate in (name, name.rsplit(":", 1)[-1]))
