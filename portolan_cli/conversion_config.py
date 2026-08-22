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
- Accept non-cloud-native formats
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, replace
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

# Sentinel meaning "derive this from the source raster" (Issue #690).
AUTO = "auto"

# Fallback pair used when a raster cannot be inspected. These are the values
# Portolan hardcoded before #690: safe everywhere, optimal nowhere.
FALLBACK_PREDICTOR = 2
FALLBACK_RESAMPLING = "nearest"

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

    Defaults are the built-in COG settings.

    Attributes:
        compression: Compression algorithm (DEFLATE, JPEG, LZW, ZSTD, etc.).
        quality: Quality setting (1-100). Applies to JPEG and WEBP compression.
        tile_size: Internal tile size in pixels (default 512).
        predictor: Compression predictor (1=none, 2=horizontal, 3=floating point),
            or "auto" to derive it from the source raster's dtype.
        resampling: Overview resampling method (nearest, bilinear, cubic, etc.),
            or "auto" to derive it from the source raster's dtype.
    """

    compression: str = "DEFLATE"
    quality: int | None = None
    tile_size: int = 512
    predictor: int | str = AUTO
    resampling: str = AUTO
    generate_thumbnail: bool = True
    thumbnail_max_size: int = 512
    thumbnail_quality: int = 75


def derive_cog_defaults(source: Path) -> tuple[int, str]:
    """Pick a predictor and an overview resampling method for one raster.

    The spec's conversion defaults tie both settings to what the pixels mean
    (see specs/best-practices/conversion-defaults.md in portolan-spec):

    - Floating-point rasters (elevation, model output) compress better with the
      floating-point predictor, and their overviews should be averaged.
    - Integer rasters may carry class codes, so their overviews stay nearest:
      averaging two class codes invents a class that does not exist.
    - Multi-band byte imagery gains nothing from horizontal differencing, so it
      gets no predictor at all.

    Continuous versus categorical has no perfect signal. Dtype is the one signal
    that is cheap and rarely wrong, and both results stay overridable through
    ``conversion.cog`` in ``.portolan/config.yaml``.

    Args:
        source: Raster to inspect.

    Returns:
        A ``(predictor, resampling)`` pair. Unreadable rasters yield the
        conservative fallback, ``(2, "nearest")``.
    """
    import rasterio

    try:
        with rasterio.open(source) as src:
            dtype = str(src.dtypes[0])
            band_count = int(src.count)
    except Exception as exc:  # noqa: BLE001 - any read failure means "use fallback"
        logger.debug("Could not inspect %s for COG defaults (%s); using fallback", source, exc)
        return FALLBACK_PREDICTOR, FALLBACK_RESAMPLING

    if dtype.startswith("float") or dtype.startswith("complex"):
        return 3, "average"
    if dtype == "uint8" and band_count >= 3:
        return 1, FALLBACK_RESAMPLING
    return FALLBACK_PREDICTOR, FALLBACK_RESAMPLING


def resolve_cog_settings(settings: CogSettings, source: Path) -> CogSettings:
    """Replace any "auto" field in ``settings`` with a value read off ``source``.

    Configured values pass through untouched. Call this immediately before
    handing a profile to rio-cogeo, never at config load time: derivation needs
    the raster, and one catalog holds many.

    Args:
        settings: Settings as loaded from config.
        source: Raster about to be converted.

    Returns:
        Settings with concrete ``predictor`` and ``resampling`` values.
    """
    if settings.predictor != AUTO and settings.resampling != AUTO:
        return settings

    derived_predictor, derived_resampling = derive_cog_defaults(source)

    return replace(
        settings,
        predictor=derived_predictor if settings.predictor == AUTO else settings.predictor,
        resampling=derived_resampling if settings.resampling == AUTO else settings.resampling,
    )


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
    if settings.resampling != AUTO and settings.resampling not in VALID_RESAMPLING_METHODS:
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
    if settings.predictor != AUTO and settings.predictor not in (1, 2, 3):
        warnings.append(
            f"Predictor {settings.predictor} is invalid. "
            "Valid values: 1 (none), 2 (horizontal), 3 (floating point), "
            "or 'auto' to derive from the source raster. Using predictor=2."
        )

    # Warn about predictor with lossy compression
    if (
        settings.compression in LOSSY_COMPRESSIONS
        and settings.predictor != AUTO
        and settings.predictor != 1
    ):
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

    # Validate thumbnail_quality (Issue #372)
    if not 1 <= settings.thumbnail_quality <= 100:
        warnings.append(
            f"thumbnail_quality {settings.thumbnail_quality} is out of range. "
            "Valid range: 1-100. Using clamped value."
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

    # predictor and resampling both accept "auto", meaning "derive from the
    # source raster at conversion time" (Issue #690). Anything unparsable
    # falls back to "auto" rather than to a hardcoded value.
    raw_predictor = cog.get("predictor", AUTO)
    predictor: int | str
    if isinstance(raw_predictor, bool) or not isinstance(raw_predictor, int):
        predictor = AUTO
    else:
        predictor = raw_predictor

    raw_resampling = cog.get("resampling", AUTO)
    resampling = raw_resampling.lower() if isinstance(raw_resampling, str) else AUTO

    generate_thumbnail = cog.get("generate_thumbnail")
    if not isinstance(generate_thumbnail, bool):
        generate_thumbnail = True

    thumbnail_max_size = cog.get("thumbnail_max_size")
    if not isinstance(thumbnail_max_size, int) or thumbnail_max_size <= 0:
        thumbnail_max_size = 512

    thumbnail_quality = cog.get("thumbnail_quality")
    if not isinstance(thumbnail_quality, int) or not 1 <= thumbnail_quality <= 100:
        thumbnail_quality = 75

    settings = CogSettings(
        compression=compression,
        quality=quality,
        tile_size=tile_size,
        predictor=predictor,
        resampling=resampling,
        generate_thumbnail=generate_thumbnail,
        thumbnail_max_size=thumbnail_max_size,
        thumbnail_quality=thumbnail_quality,
    )

    # Validate and log warnings
    warnings = validate_cog_settings(settings)
    for warning in warnings:
        logger.warning("COG config: %s", warning)

    return settings


# =============================================================================
# Vector Settings (Issue #340)
# =============================================================================

# Valid spatial index types supported by geoparquet-io
VALID_SPATIAL_INDEXES: frozenset[str] = frozenset({"h3", "quadkey", "s2", "a5", "kdtree", "none"})

# Valid sort methods
VALID_SORT_METHODS: frozenset[str] = frozenset({"hilbert", "quadkey", "none"})

# Defaults that make generated GeoParquet conform to the Portolan profile
# (issue #805). Named so the parse fallbacks below cannot drift from the
# dataclass defaults.
DEFAULT_SORT = "hilbert"
DEFAULT_ADD_BBOX = True

# Default resolutions per index type (geoparquet-io defaults)
DEFAULT_RESOLUTIONS: dict[str, int] = {
    "h3": 9,
    "quadkey": 13,
    "s2": 13,
    "a5": 15,
    "kdtree": 9,  # iterations for kdtree
}


@dataclass(frozen=True)
class VectorSettings:
    """Configuration for vector (GeoParquet) conversion.

    Controls spatial optimization at conversion time via geoparquet-io's
    fluent Table API. Both file and Iceberg backends receive the same
    spatially-enriched output.

    Attributes:
        spatial_index: Spatial index column to add (h3, quadkey, s2, a5, kdtree, none).
        resolution: Index resolution. "auto" uses geoparquet-io defaults which
            include row-count-based tuning. Explicit int overrides.
        sort: Row ordering method (hilbert, quadkey, none). Defaults to
            ``hilbert``.
        add_bbox: Whether to add a bbox struct column. Defaults to ``True``.
        partition: Whether to produce hive-partitioned output. Only affects
            file backend; Iceberg uses native partitioning on the spatial column.

    ``sort`` and ``add_bbox`` default to the values the Portolan GeoParquet
    profile requires (issue #805). rashid reads row order through the bbox
    covering column, so ``add_bbox`` is what makes PTL-DAT-006 and PTL-DAT-007
    evaluable and ``sort`` is what makes PTL-DAT-006 pass. Set ``sort: none``
    or ``add_bbox: false`` in ``.portolan/config.yaml`` to turn either off.
    """

    spatial_index: str = "none"
    resolution: int | str = "auto"
    sort: str = DEFAULT_SORT
    add_bbox: bool = DEFAULT_ADD_BBOX
    partition: bool = False


def validate_vector_settings(settings: VectorSettings) -> list[str]:
    """Validate vector settings and return warnings for any issues.

    Does not raise exceptions — returns a list of warning messages.

    Args:
        settings: VectorSettings instance to validate.

    Returns:
        List of warning messages. Empty list if all settings are valid.
    """
    warnings: list[str] = []

    # Validate spatial_index
    if settings.spatial_index not in VALID_SPATIAL_INDEXES:
        warnings.append(
            f"Unknown spatial_index '{settings.spatial_index}'. "
            f"Valid values: {', '.join(sorted(VALID_SPATIAL_INDEXES))}. "
            "Falling back to 'none'."
        )

    # Validate sort
    if settings.sort not in VALID_SORT_METHODS:
        warnings.append(
            f"Unknown sort method '{settings.sort}'. "
            f"Valid values: {', '.join(sorted(VALID_SORT_METHODS))}. "
            f"Falling back to '{DEFAULT_SORT}'."
        )

    # Validate resolution if explicit
    if settings.resolution != "auto":
        if not isinstance(settings.resolution, int):
            warnings.append(
                f"Resolution '{settings.resolution}' is not valid. "
                "Must be 'auto' or an integer. Using 'auto'."
            )
        elif settings.resolution < 0:
            warnings.append(
                f"Resolution {settings.resolution} is negative. Using default for index type."
            )

    # Warn if partition=True but spatial_index=none
    if settings.partition and settings.spatial_index == "none":
        warnings.append(
            "partition=True requires a spatial_index. "
            "Set spatial_index to h3, quadkey, s2, a5, or kdtree."
        )

    return warnings


def get_vector_settings(catalog_path: Path) -> VectorSettings:
    """Load vector conversion settings from catalog config.

    Reads the 'conversion.vector' section from .portolan/config.yaml and returns
    a VectorSettings instance with values from config merged with defaults.

    Args:
        catalog_path: Root path of the catalog.

    Returns:
        VectorSettings instance. Returns defaults if no config exists.
    """
    config = load_config(catalog_path)

    conversion = _get_dict(config, "conversion")
    if not conversion:
        return VectorSettings()

    vector = _get_dict(conversion, "vector")
    if not vector:
        return VectorSettings()

    # Parse spatial_index
    spatial_index = vector.get("spatial_index")
    if not isinstance(spatial_index, str):
        spatial_index = "none"
    else:
        spatial_index = spatial_index.lower()

    # Parse resolution
    resolution: int | str = vector.get("resolution", "auto")
    if isinstance(resolution, str):
        resolution = resolution.lower()
        if resolution != "auto":
            # Try to parse as int
            try:
                resolution = int(resolution)
            except ValueError:
                resolution = "auto"
    elif not isinstance(resolution, int):
        resolution = "auto"

    # Parse sort. A missing or non-string value keeps the default rather than
    # disabling the optimization (issue #805).
    sort = vector.get("sort")
    if not isinstance(sort, str):
        sort = DEFAULT_SORT
    else:
        sort = sort.lower()

    # Parse add_bbox
    add_bbox = vector.get("add_bbox")
    if not isinstance(add_bbox, bool):
        add_bbox = DEFAULT_ADD_BBOX

    # Parse partition
    partition = vector.get("partition")
    if not isinstance(partition, bool):
        partition = False

    # Normalize invalid values before creating settings. A misspelled value
    # changes what the writer produces, so the operator sees these, not only
    # the log (issue #805).
    from portolan_cli.output import warn

    if spatial_index not in VALID_SPATIAL_INDEXES:
        message = f"Vector config: unknown spatial_index '{spatial_index}', using 'none'"
        logger.warning(message)
        warn(message)
        spatial_index = "none"

    if sort not in VALID_SORT_METHODS:
        message = f"Vector config: unknown sort '{sort}', using '{DEFAULT_SORT}'"
        logger.warning(message)
        warn(message)
        sort = DEFAULT_SORT

    if resolution != "auto" and (not isinstance(resolution, int) or resolution < 0):
        logger.warning("Vector config: Invalid resolution '%s', using 'auto'", resolution)
        resolution = "auto"

    if partition and spatial_index == "none":
        logger.warning("Vector config: partition=True requires spatial_index, disabling partition")
        partition = False

    return VectorSettings(
        spatial_index=spatial_index,
        resolution=resolution,
        sort=sort,
        add_bbox=add_bbox,
        partition=partition,
    )
