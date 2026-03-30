"""Filtering for ArcGIS extraction.

This module provides filtering logic for the `portolan extract arcgis` command.
Both services and layers can be filtered using glob patterns (fnmatch).

Per CLI design (context/shared/plans/extract-arcgis-design.md):
- `--services` accepts comma-separated glob patterns to include
- `--exclude-services` accepts comma-separated glob patterns to exclude
- `--layers` accepts comma-separated glob patterns OR IDs to include
- `--exclude-layers` accepts comma-separated glob patterns OR IDs to exclude
- `--filter` applies the same glob pattern to both services AND layers
- Patterns use fnmatch semantics: `*` matches any, `?` matches single char
"""

from __future__ import annotations

from fnmatch import fnmatch


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

        >>> filter_services(
        ...     ["Census_2020", "Census_Legacy", "Transport"],
        ...     include=["Census*"],
        ...     exclude=["*Legacy"],
        ... )
        ['Census_2020']
    """
    result = services

    # Apply include filter (if provided and non-empty)
    if include:
        result = [s for s in result if _matches_any_glob(s, include, case_sensitive)]

    # Apply exclude filter (if provided and non-empty)
    if exclude:
        result = [s for s in result if not _matches_any_glob(s, exclude, case_sensitive)]

    return result


def filter_layers(
    layers: list[dict[str, int | str]],
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    *,
    case_sensitive: bool = False,
) -> list[dict[str, int | str]]:
    """Filter layers by ID or glob pattern on name.

    Supports both exact ID matching and glob patterns on names:
    - Numeric strings ("0", "1") match layer IDs exactly
    - Glob patterns ("sdn_*", "*_2024*") match layer names
    - Plain strings match layer names (case-insensitive by default)

    Args:
        layers: List of layer dicts with {"id": int, "name": str}.
        include: List of layer IDs or glob patterns to include. If None or
            empty, all layers are included.
        exclude: List of layer IDs or glob patterns to exclude. Applied after
            include filtering.
        case_sensitive: Whether name matching is case-sensitive (default False)

    Returns:
        Filtered list of layers, preserving original order.

    Examples:
        >>> layers = [
        ...     {"id": 0, "name": "sdn_admin_boundaries"},
        ...     {"id": 1, "name": "sdn_health_facilities"},
        ...     {"id": 2, "name": "ukr_admin_boundaries"},
        ... ]

        # Filter by glob pattern
        >>> filter_layers(layers, include=["sdn_*"])
        [{'id': 0, 'name': 'sdn_admin_boundaries'}, {'id': 1, 'name': 'sdn_health_facilities'}]

        # Filter by ID
        >>> filter_layers(layers, include=["0", "1"])
        [{'id': 0, 'name': 'sdn_admin_boundaries'}, {'id': 1, 'name': 'sdn_health_facilities'}]

        # Exclude pattern
        >>> filter_layers(layers, exclude=["*_admin_*"])
        [{'id': 1, 'name': 'sdn_health_facilities'}]

        # Combined
        >>> filter_layers(layers, include=["sdn_*"], exclude=["*health*"])
        [{'id': 0, 'name': 'sdn_admin_boundaries'}]
    """
    result = layers

    # Apply include filter
    if include:
        result = [layer for layer in result if _layer_matches(layer, include, case_sensitive)]

    # Apply exclude filter
    if exclude:
        result = [layer for layer in result if not _layer_matches(layer, exclude, case_sensitive)]

    return result


def apply_unified_filter(
    services: list[str] | None,
    layers: list[dict[str, int | str]] | None,
    filter_pattern: list[str] | None,
    exclude_pattern: list[str] | None = None,
) -> tuple[list[str] | None, list[dict[str, int | str]] | None]:
    """Apply unified filter pattern to both services and layers.

    This is a convenience function for the `--filter` CLI option that applies
    the same glob pattern to both services and layers.

    Args:
        services: List of service names (None if not applicable)
        layers: List of layer dicts (None if not applicable)
        filter_pattern: Glob patterns to include (applied to both)
        exclude_pattern: Glob patterns to exclude (applied to both)

    Returns:
        Tuple of (filtered_services, filtered_layers)

    Examples:
        >>> services = ["sdn_Census", "ukr_Census", "sdn_Health"]
        >>> layers = [
        ...     {"id": 0, "name": "sdn_boundaries"},
        ...     {"id": 1, "name": "ukr_boundaries"},
        ... ]
        >>> apply_unified_filter(services, layers, ["sdn_*"], None)
        (['sdn_Census', 'sdn_Health'], [{'id': 0, 'name': 'sdn_boundaries'}])
    """
    filtered_services = None
    filtered_layers = None

    if services is not None:
        filtered_services = filter_services(
            services,
            include=filter_pattern,
            exclude=exclude_pattern,
            case_sensitive=False,  # Unified filter is case-insensitive
        )

    if layers is not None:
        filtered_layers = filter_layers(
            layers,
            include=filter_pattern,
            exclude=exclude_pattern,
            case_sensitive=False,
        )

    return filtered_services, filtered_layers


def _matches_any_glob(
    value: str,
    patterns: list[str],
    case_sensitive: bool,
) -> bool:
    """Check if value matches any of the glob patterns.

    Args:
        value: String to check
        patterns: List of glob patterns
        case_sensitive: Whether matching is case-sensitive

    Returns:
        True if value matches at least one pattern
    """
    for pattern in patterns:
        if case_sensitive:
            if fnmatch(value, pattern):
                return True
        else:
            # Case-insensitive: lowercase both
            if fnmatch(value.lower(), pattern.lower()):
                return True
    return False


def _layer_matches(
    layer: dict[str, int | str],
    patterns: list[str],
    case_sensitive: bool,
) -> bool:
    """Check if a layer matches any pattern (by ID or name glob).

    Matches against:
    - Layer ID (if pattern is a numeric string)
    - Layer name (glob pattern, case-insensitive by default)
    """
    layer_id = layer.get("id")
    layer_name = str(layer.get("name", ""))

    for pattern in patterns:
        pattern_stripped = pattern.strip()

        # Check if pattern is a numeric ID
        if pattern_stripped.isdigit():
            if layer_id is not None and str(layer_id) == pattern_stripped:
                return True
        else:
            # Glob pattern on name
            if case_sensitive:
                if fnmatch(layer_name, pattern_stripped):
                    return True
            else:
                if fnmatch(layer_name.lower(), pattern_stripped.lower()):
                    return True

    return False
