"""Filtering for extraction operations.

Services and layers are filtered with glob patterns (fnmatch): ``*``
matches any text and ``?`` one character. ``source.filter_refs`` applies
``layer_matches`` to the layers of a run. ``filter_services`` filters the
services of an ArcGIS services root.
"""

from __future__ import annotations

from fnmatch import fnmatch, fnmatchcase


def filter_services(
    services: list[str],
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    *,
    case_sensitive: bool = True,
) -> list[str]:
    """Filter services by include/exclude glob patterns.

    The filtering order is:
    1. If include patterns are provided, only services matching at least one
       include pattern are kept
    2. If exclude patterns are provided, any services matching at least one
       exclude pattern are removed

    This means include patterns narrow the set first, then exclude patterns
    remove from that narrowed set.

    Args:
        services: List of service names to filter
        include: Glob patterns to include (if None or empty, include all)
        exclude: Glob patterns to exclude (if None or empty, exclude none)
        case_sensitive: Whether pattern matching is case-sensitive (default True)

    Returns:
        Filtered list of service names (preserving original order)

    Examples:
        >>> filter_services(["Census_2020", "Census_2010", "Transport"], include=["Census*"])
        ['Census_2020', 'Census_2010']

        >>> filter_services(["Census_2020", "Legacy_Data", "Transport"], exclude=["Legacy*"])
        ['Census_2020', 'Transport']
    """
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
        else:
            if fnmatch(value.lower(), pattern.lower()):
                return True
    return False


def is_glob(pattern: str) -> bool:
    """Return True when ``pattern`` holds a glob character, so it names no single layer."""
    return any(char in pattern for char in "*?[")


def layer_matches(layer_id: int | str, name: str, pattern: str) -> bool:
    """Return True when ``pattern`` selects the layer.

    An all-digit pattern matches the layer id that the source uses, not the
    position in a list (issue #1003). Any other pattern is a glob that
    matches, without case, the full name or its leaf after the last ``:``.
    So ``terenuri`` matches the WFS layer ``cadastru_data:terenuri`` (issue
    #768).

    Args:
        layer_id: The layer id at the source.
        name: The qualified layer name, such as ``workspace:layer``.
        pattern: One ``--layers`` or ``--exclude-layers`` pattern.

    Returns:
        True when the pattern matches.
    """
    pattern = pattern.strip()
    if pattern.isdigit():
        return str(layer_id) == pattern
    lowered = pattern.lower()
    return any(fnmatch(candidate.lower(), lowered) for candidate in (name, name.rsplit(":", 1)[-1]))
