"""Conversion configuration for controlling format handling behavior.

This module provides configuration for overriding default format conversion
behavior:
- Force-convert cloud-native formats (e.g., FlatGeobuf -> GeoParquet)
- Preserve convertible formats (e.g., keep Shapefiles as-is)
- Path-based overrides with glob patterns

Config is stored in .portolan/config.yaml under the 'conversion' key:

    conversion:
      extensions:
        convert: [fgb]       # Force convert these cloud-native formats
        preserve: [gpkg]     # Keep these convertible formats as-is
      paths:
        preserve:            # Glob patterns for files to preserve
          - "legacy/**"
          - "regulatory/*.shp"

See:
- GitHub Issue #75: FlatGeobuf cloud-native status
- GitHub Issue #103: Config for non-cloud-native file handling
- ADR-0014: Accept non-cloud-native formats
"""

from __future__ import annotations

from dataclasses import dataclass, field
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

from portolan_cli.config import load_config


@dataclass(frozen=True)
class ConversionOverrides:
    """Configuration overrides for format conversion behavior.

    Attributes:
        extensions_convert: Extensions to force-convert even if cloud-native.
            Normalized to lowercase with leading dot (e.g., {".fgb"}).
        extensions_preserve: Extensions to preserve even if convertible.
            Normalized to lowercase with leading dot (e.g., {".shp"}).
        paths_preserve: Glob patterns for files to preserve regardless of format.
            Patterns are matched against relative paths from catalog root.
    """

    extensions_convert: frozenset[str] = field(default_factory=frozenset)
    extensions_preserve: frozenset[str] = field(default_factory=frozenset)
    paths_preserve: tuple[str, ...] = field(default_factory=tuple)

    def should_force_convert(self, path: Path) -> bool:
        """Check if a file should be force-converted based on extension.

        Args:
            path: Path to the file to check.

        Returns:
            True if the file's extension is in extensions_convert.
        """
        return path.suffix.lower() in self.extensions_convert

    def should_preserve(self, path: Path, *, root: Path | None = None) -> bool:
        """Check if a file should be preserved (not converted).

        Checks both extension-based and path-based preserve rules.
        Path patterns take precedence over extension rules.

        Args:
            path: Path to the file to check.
            root: Catalog root for resolving relative paths in glob patterns.
                Required if paths_preserve contains patterns.

        Returns:
            True if the file should be preserved based on extension or path pattern.
        """
        # Check path patterns first (higher precedence)
        if self.paths_preserve and root is not None:
            try:
                relative = path.relative_to(root)
                # Use POSIX-style paths for consistent matching across platforms
                relative_str = relative.as_posix()
                for pattern in self.paths_preserve:
                    if fnmatch(relative_str, pattern):
                        return True
            except ValueError:
                # Path not relative to root, skip path matching
                pass

        # Check extension-based preserve
        return path.suffix.lower() in self.extensions_preserve


def _normalize_extension(ext: str) -> str:
    """Normalize an extension to lowercase with leading dot.

    Args:
        ext: Extension string, with or without leading dot.

    Returns:
        Lowercase extension with leading dot (e.g., ".fgb").
    """
    ext = ext.lower().strip()
    if not ext.startswith("."):
        ext = f".{ext}"
    return ext


def _get_dict(data: dict[str, Any], key: str) -> dict[str, Any]:
    """Safely get a dict value, returning empty dict if not a dict."""
    value = data.get(key, {})
    return value if isinstance(value, dict) else {}


def _get_list(data: dict[str, Any], key: str) -> list[Any]:
    """Safely get a list value, returning empty list if not a list."""
    value = data.get(key, [])
    return value if isinstance(value, list) else []


def _parse_extensions(items: list[Any]) -> frozenset[str]:
    """Parse and normalize extension list, filtering non-strings."""
    return frozenset(_normalize_extension(e) for e in items if isinstance(e, str) and e)


def _parse_paths(items: list[Any]) -> tuple[str, ...]:
    """Parse path list, filtering non-strings."""
    return tuple(p for p in items if isinstance(p, str))


def get_conversion_overrides(catalog_path: Path) -> ConversionOverrides:
    """Load conversion overrides from catalog config.

    Reads the 'conversion' section from .portolan/config.yaml and returns
    a ConversionOverrides instance with normalized values.

    Args:
        catalog_path: Root path of the catalog.

    Returns:
        ConversionOverrides instance. Returns empty overrides if no config exists.
    """
    config = load_config(catalog_path)

    conversion = _get_dict(config, "conversion")
    if not conversion:
        return ConversionOverrides()

    extensions = _get_dict(conversion, "extensions")
    paths = _get_dict(conversion, "paths")

    return ConversionOverrides(
        extensions_convert=_parse_extensions(_get_list(extensions, "convert")),
        extensions_preserve=_parse_extensions(_get_list(extensions, "preserve")),
        paths_preserve=_parse_paths(_get_list(paths, "preserve")),
    )


# =============================================================================
# COG Settings (Issue #279)
# =============================================================================


@dataclass(frozen=True)
class CogSettings:
    """Configuration for Cloud-Optimized GeoTIFF conversion.

    Defaults match ADR-0019: COG Optimization Defaults.

    Attributes:
        compression: Compression algorithm (DEFLATE, JPEG, LZW, ZSTD, etc.).
        quality: JPEG quality (1-100). Only applies when compression is JPEG.
        tile_size: Internal tile size in pixels (default 512).
        predictor: Compression predictor (1=none, 2=horizontal, 3=floating point).
        resampling: Overview resampling method (nearest, bilinear, cubic, etc.).
    """

    compression: str = "DEFLATE"
    quality: int | None = None
    tile_size: int = 512
    predictor: int = 2
    resampling: str = "nearest"


def get_cog_settings(catalog_path: Path) -> CogSettings:
    """Load COG conversion settings from catalog config.

    Reads the 'conversion.cog' section from .portolan/config.yaml and returns
    a CogSettings instance with values from config merged with defaults.

    Args:
        catalog_path: Root path of the catalog.

    Returns:
        CogSettings instance. Returns defaults if no config exists.
    """
    config = load_config(catalog_path)

    conversion = _get_dict(config, "conversion")
    if not conversion:
        return CogSettings()

    cog = _get_dict(conversion, "cog")
    if not cog:
        return CogSettings()

    # Parse individual settings with type validation
    compression = cog.get("compression")
    if isinstance(compression, str):
        compression = compression.upper()
    else:
        compression = "DEFLATE"

    quality = cog.get("quality")
    if not isinstance(quality, int):
        quality = None

    tile_size = cog.get("tile_size")
    if not isinstance(tile_size, int):
        tile_size = 512

    predictor = cog.get("predictor")
    if not isinstance(predictor, int):
        predictor = 2

    resampling = cog.get("resampling")
    if not isinstance(resampling, str):
        resampling = "nearest"

    return CogSettings(
        compression=compression,
        quality=quality,
        tile_size=tile_size,
        predictor=predictor,
        resampling=resampling,
    )
