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

import logging
from dataclasses import dataclass, field
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

from portolan_cli.config import load_config

logger = logging.getLogger(__name__)


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

# Valid compression algorithms supported by rio-cogeo
# See: rio_cogeo.profiles.cog_profiles
VALID_COG_COMPRESSIONS: frozenset[str] = frozenset(
    {
        "DEFLATE",
        "LZW",
        "ZSTD",
        "JPEG",
        "WEBP",
        "LERC",
        "LERC_DEFLATE",
        "LERC_ZSTD",
        "PACKBITS",
        "LZMA",
        "RAW",  # No compression
    }
)

# Lossy compression methods (predictor doesn't apply)
LOSSY_COMPRESSIONS: frozenset[str] = frozenset({"JPEG", "WEBP"})

# Compression methods that support quality setting
QUALITY_COMPRESSIONS: frozenset[str] = frozenset({"JPEG", "WEBP"})

# Valid resampling methods for overview generation
# See: rio_cogeo.cogeo.cog_translate overview_resampling parameter
VALID_RESAMPLING_METHODS: frozenset[str] = frozenset(
    {
        "nearest",
        "bilinear",
        "cubic",
        "cubic_spline",
        "lanczos",
        "average",
        "mode",
        "gauss",
        "rms",
    }
)


@dataclass(frozen=True)
class CogSettings:
    """Configuration for Cloud-Optimized GeoTIFF conversion.

    Defaults match ADR-0019: COG Optimization Defaults.

    Attributes:
        compression: Compression algorithm (DEFLATE, JPEG, LZW, ZSTD, etc.).
        quality: Quality setting (1-100). Applies to JPEG and WEBP compression.
        tile_size: Internal tile size in pixels (default 512).
        predictor: Compression predictor (1=none, 2=horizontal, 3=floating point).
        resampling: Overview resampling method (nearest, bilinear, cubic, etc.).
    """

    compression: str = "DEFLATE"
    quality: int | None = None
    tile_size: int = 512
    predictor: int = 2
    resampling: str = "nearest"
    generate_thumbnail: bool = True
    thumbnail_max_size: int = 512


def validate_cog_settings(settings: CogSettings) -> list[str]:
    """Validate COG settings and return warnings for any issues.

    Does not raise exceptions — returns a list of warning messages. This allows
    conversion to proceed with potentially suboptimal settings while informing
    the user of issues.

    Args:
        settings: CogSettings instance to validate.

    Returns:
        List of warning messages. Empty list if all settings are valid.
    """
    warnings: list[str] = []

    # Validate compression
    if settings.compression not in VALID_COG_COMPRESSIONS:
        warnings.append(
            f"Unknown compression '{settings.compression}'. "
            f"Valid values: {', '.join(sorted(VALID_COG_COMPRESSIONS))}. "
            "Conversion may fail."
        )

    # Validate resampling
    if settings.resampling not in VALID_RESAMPLING_METHODS:
        warnings.append(
            f"Unknown resampling method '{settings.resampling}'. "
            f"Valid values: {', '.join(sorted(VALID_RESAMPLING_METHODS))}. "
            "Conversion may fail."
        )

    # Validate quality bounds
    if settings.quality is not None:
        if not 1 <= settings.quality <= 100:
            warnings.append(
                f"Quality {settings.quality} is out of range. "
                "Valid range: 1-100. Using clamped value."
            )
        # Warn if quality is set for non-lossy compression
        if settings.compression not in QUALITY_COMPRESSIONS:
            warnings.append(
                f"Quality setting ({settings.quality}) is ignored for "
                f"'{settings.compression}' compression. "
                f"Quality only applies to: {', '.join(sorted(QUALITY_COMPRESSIONS))}."
            )

    # Validate tile_size
    if settings.tile_size < 64:
        warnings.append(
            f"tile_size {settings.tile_size} is very small. "
            "Minimum recommended: 64. This may cause performance issues."
        )
    elif settings.tile_size > 4096:
        warnings.append(
            f"tile_size {settings.tile_size} is very large. "
            "Maximum recommended: 4096. This may cause memory issues."
        )
    # Warn if not a power of 2 (common convention, not strict requirement)
    elif settings.tile_size & (settings.tile_size - 1) != 0:
        warnings.append(
            f"tile_size {settings.tile_size} is not a power of 2. "
            "While valid, power-of-2 sizes (256, 512, 1024) are conventional."
        )

    # Validate predictor
    if settings.predictor not in (1, 2, 3):
        warnings.append(
            f"Predictor {settings.predictor} is invalid. "
            "Valid values: 1 (none), 2 (horizontal), 3 (floating point). "
            "Using predictor=2."
        )

    # Warn about predictor with lossy compression
    if settings.compression in LOSSY_COMPRESSIONS and settings.predictor != 1:
        warnings.append(
            f"Predictor={settings.predictor} is ignored for lossy compression "
            f"'{settings.compression}'. Consider setting predictor=1 to avoid confusion."
        )

    # Validate thumbnail_max_size (Issue #372)
    if settings.thumbnail_max_size <= 0:
        warnings.append(
            f"thumbnail_max_size {settings.thumbnail_max_size} is invalid. "
            "Must be > 0. Using default 512."
        )
    elif settings.thumbnail_max_size > 4096:
        warnings.append(
            f"thumbnail_max_size {settings.thumbnail_max_size} is very large. "
            "Recommended: <= 4096. Defeats the purpose of a thumbnail."
        )

    return warnings


def get_cog_settings(catalog_path: Path) -> CogSettings:
    """Load COG conversion settings from catalog config.

    Reads the 'conversion.cog' section from .portolan/config.yaml and returns
    a CogSettings instance with values from config merged with defaults.

    Validates settings and logs warnings for any issues. Invalid values are
    either corrected (e.g., quality clamped to 1-100) or passed through to
    let rio-cogeo handle the error with its own message.

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
    elif quality is not None:
        # Clamp quality to valid range
        quality = max(1, min(100, quality))

    tile_size = cog.get("tile_size")
    if not isinstance(tile_size, int):
        tile_size = 512

    predictor = cog.get("predictor")
    if not isinstance(predictor, int):
        predictor = 2

    resampling = cog.get("resampling")
    if not isinstance(resampling, str):
        resampling = "nearest"
    else:
        # Normalize resampling to lowercase
        resampling = resampling.lower()

    generate_thumbnail = cog.get("generate_thumbnail")
    if not isinstance(generate_thumbnail, bool):
        generate_thumbnail = True

    thumbnail_max_size = cog.get("thumbnail_max_size")
    if not isinstance(thumbnail_max_size, int) or thumbnail_max_size <= 0:
        thumbnail_max_size = 512

    settings = CogSettings(
        compression=compression,
        quality=quality,
        tile_size=tile_size,
        predictor=predictor,
        resampling=resampling,
        generate_thumbnail=generate_thumbnail,
        thumbnail_max_size=thumbnail_max_size,
    )

    # Validate and log warnings
    warnings = validate_cog_settings(settings)
    for warning in warnings:
        logger.warning("COG config: %s", warning)

    return settings
