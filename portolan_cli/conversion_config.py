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
                relative_str = str(relative)
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

    conversion = config.get("conversion", {})
    if not isinstance(conversion, dict):
        return ConversionOverrides()

    extensions = conversion.get("extensions", {})
    if not isinstance(extensions, dict):
        extensions = {}

    paths = conversion.get("paths", {})
    if not isinstance(paths, dict):
        paths = {}

    # Parse extensions.convert
    convert_list = extensions.get("convert", [])
    if not isinstance(convert_list, list):
        convert_list = []
    extensions_convert = frozenset(_normalize_extension(e) for e in convert_list if e)

    # Parse extensions.preserve
    preserve_list = extensions.get("preserve", [])
    if not isinstance(preserve_list, list):
        preserve_list = []
    extensions_preserve = frozenset(_normalize_extension(e) for e in preserve_list if e)

    # Parse paths.preserve
    paths_preserve_list = paths.get("preserve", [])
    if not isinstance(paths_preserve_list, list):
        paths_preserve_list = []
    paths_preserve = tuple(p for p in paths_preserve_list if isinstance(p, str))

    return ConversionOverrides(
        extensions_convert=extensions_convert,
        extensions_preserve=extensions_preserve,
        paths_preserve=paths_preserve,
    )
