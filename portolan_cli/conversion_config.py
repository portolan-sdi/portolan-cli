"""Conversion configuration for controlling format handling behavior."""

from __future__ import annotations

import logging
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from fnmatch import fnmatch
from os import PathLike
from typing import TYPE_CHECKING, Any

from portolan_core.config import ConfigSource, load_catalog_config
from portolan_core.trace import count

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

_RGB_BAND_COUNT = 3
_MAX_QUALITY = 100
_MIN_TILE_SIZE = 64
_MAX_TILE_SIZE = 4096
_MAX_THUMBNAIL_SIZE = 4096

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConversionOverrides:
    """Configuration overrides for format conversion behavior."""

    extensions_convert: frozenset[str] = field(default_factory=frozenset)
    extensions_preserve: frozenset[str] = field(default_factory=frozenset)
    paths_preserve: tuple[str, ...] = field(default_factory=tuple)

    def should_force_convert(self, path: Path) -> bool:
        """Check if a file should be force-converted based on extension."""
        return path.suffix.lower() in self.extensions_convert

    def should_preserve(self, path: Path, *, root: Path | None = None) -> bool:
        """Check if a file should be preserved (not converted)."""
        # Check path patterns first (higher precedence)
        if self.paths_preserve and root is not None:
            try:
                # Normalize the directory, keeping a symlinked asset's own
                # name: preserve globs name catalog entries, not their targets.
                relative = (path.parent.resolve() / path.name).relative_to(root.resolve())
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
    """Normalize an extension to lowercase with leading dot."""
    ext = ext.lower().strip()
    if not ext.startswith("."):
        ext = f".{ext}"
    return ext


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


def get_conversion_overrides(
    catalog_path: Path, *, state: ConfigSource | None = None
) -> ConversionOverrides:
    """Load conversion overrides from catalog config."""
    config = load_catalog_config(catalog_path, state=state)
    extensions = config.section("conversion", "extensions")
    paths = config.section("conversion", "paths")

    return ConversionOverrides(
        extensions_convert=_parse_extensions(_get_list(extensions, "convert")),
        extensions_preserve=_parse_extensions(_get_list(extensions, "preserve")),
        paths_preserve=_parse_paths(_get_list(paths, "preserve")),
    )


def get_handler_pins(catalog_path: Path, *, state: ConfigSource | None = None) -> dict[str, str]:
    """Load ``conversion.handlers``: the format handler to use per extension."""
    handlers = load_catalog_config(catalog_path, state=state).section("conversion", "handlers")
    pins: dict[str, str] = {}
    for extension, name in handlers.items():
        ext = str(extension).strip().lower()
        pins[ext if ext.startswith(".") else f".{ext}"] = str(name).strip()
    return pins


# The compression algorithms that rio-cogeo accepts. The module
# rio_cogeo.profiles holds the profile table that these names index.
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

# The compressions that take a predictor. GDAL ignores PREDICTOR for any other
# codec and logs a warning for each file.
PREDICTOR_COMPRESSIONS: frozenset[str] = frozenset({"DEFLATE", "LZW", "LZMA", "ZSTD"})

# The GTiff COMPRESS value for each name that GDAL spells differently. RAW is
# the rio-cogeo profile name for no compression, and GDAL calls it NONE.
GDAL_COMPRESS_NAMES: dict[str, str] = {"RAW": "NONE"}

# Compression methods that support quality setting
QUALITY_COMPRESSIONS: frozenset[str] = frozenset({"JPEG", "WEBP"})

# Sentinel meaning "derive this from the source raster".
AUTO = "auto"

# Fallback pair for a raster that cannot be inspected: safe everywhere, optimal nowhere.
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
    """Configuration for Cloud-Optimized GeoTIFF conversion."""

    compression: str = "DEFLATE"
    quality: int | None = None
    tile_size: int = 512
    predictor: int | str = AUTO
    resampling: str = AUTO
    generate_thumbnail: bool = True
    thumbnail_max_size: int = 512
    thumbnail_quality: int = 75


def derive_cog_defaults(source: Path | Any) -> tuple[int, str]:
    """Pick a predictor and an overview resampling method for one raster."""
    import rasterio

    try:
        if isinstance(source, PathLike):
            count("raster.opens")
            with rasterio.open(source) as src:
                dtype = str(src.dtypes[0])
                band_count = int(src.count)
        else:
            dtype, band_count = str(source.dtypes[0]), int(source.count)
    except Exception as exc:  # any read failure means "use fallback"
        logger.debug("Could not inspect %s for COG defaults (%s); using fallback", source, exc)
        return FALLBACK_PREDICTOR, FALLBACK_RESAMPLING

    if dtype.startswith(("float", "complex")):
        return 3, "average"
    if dtype == "uint8" and band_count >= _RGB_BAND_COUNT:
        return 1, FALLBACK_RESAMPLING
    return FALLBACK_PREDICTOR, FALLBACK_RESAMPLING


def resolve_cog_settings(settings: CogSettings, source: Path | Any) -> CogSettings:
    """Replace any "auto" field in ``settings`` with a value read off ``source``."""
    if AUTO not in (settings.predictor, settings.resampling):
        return settings

    derived_predictor, derived_resampling = derive_cog_defaults(source)

    return replace(
        settings,
        predictor=derived_predictor if settings.predictor == AUTO else settings.predictor,
        resampling=derived_resampling if settings.resampling == AUTO else settings.resampling,
    )


def validate_cog_settings(settings: CogSettings) -> list[str]:
    """Validate COG settings and return warnings for any issues."""
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
        if not 1 <= settings.quality <= _MAX_QUALITY:
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
    if settings.tile_size < _MIN_TILE_SIZE:
        warnings.append(
            f"tile_size {settings.tile_size} is very small. "
            "Minimum recommended: 64. This may cause performance issues."
        )
    elif settings.tile_size > _MAX_TILE_SIZE:
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

    # Warn about a predictor that the compression does not use
    if settings.compression not in PREDICTOR_COMPRESSIONS and settings.predictor not in (AUTO, 1):
        warnings.append(
            f"Predictor={settings.predictor} is ignored for compression "
            f"'{settings.compression}'. "
            f"A predictor applies only to: {', '.join(sorted(PREDICTOR_COMPRESSIONS))}."
        )

    # Validate thumbnail_max_size
    if settings.thumbnail_max_size <= 0:
        warnings.append(
            f"thumbnail_max_size {settings.thumbnail_max_size} is invalid. "
            "Must be > 0. Using default 512."
        )
    elif settings.thumbnail_max_size > _MAX_THUMBNAIL_SIZE:
        warnings.append(
            f"thumbnail_max_size {settings.thumbnail_max_size} is very large. "
            "Recommended: <= 4096. Defeats the purpose of a thumbnail."
        )

    # Validate thumbnail_quality
    if not 1 <= settings.thumbnail_quality <= _MAX_QUALITY:
        warnings.append(
            f"thumbnail_quality {settings.thumbnail_quality} is out of range. "
            "Valid range: 1-100. Using clamped value."
        )

    return warnings


#: The ``partitioning.*`` keys that ``partitioning`` reads for one directory.
PARTITION_KEYS: tuple[str, ...] = (
    "partitioning.enabled",
    "partitioning.threshold_gb",
    "partitioning.strategy",
    "partitioning.target_rows",
)


@dataclass(frozen=True)
class PartitionSettings:
    """The resolved ``partitioning.*`` settings of one directory."""

    directory: Path
    values: dict[str, Any]


def resolve_partition_settings(directory: Path, state: ConfigSource) -> PartitionSettings:
    """Resolve the ``partitioning.*`` settings of ``directory`` through the run state."""
    return PartitionSettings(
        directory, {key: state.setting(key, collection_dir=directory) for key in PARTITION_KEYS}
    )


_SETTINGS_OVERRIDE: ContextVar[
    tuple[CogSettings | None, VectorSettings | None, PartitionSettings | None]
] = ContextVar("portolan_conversion_settings_override", default=(None, None, None))


@contextmanager
def settings_override(
    cog: CogSettings | None = None,
    vector: VectorSettings | None = None,
    partition: PartitionSettings | None = None,
) -> Iterator[None]:
    """Make the settings loaders return these values."""
    token = _SETTINGS_OVERRIDE.set((cog, vector, partition))
    try:
        yield
    finally:
        _SETTINGS_OVERRIDE.reset(token)


def partition_override(directory: Path) -> PartitionSettings | None:
    """The partition settings of ``settings_override`` when they are for ``directory``."""
    partition = _SETTINGS_OVERRIDE.get()[2]
    return partition if partition is not None and partition.directory == directory else None


def get_cog_settings(
    catalog_path: Path | None, *, state: ConfigSource | None = None
) -> CogSettings:
    """Load COG conversion settings from catalog config."""
    override = _SETTINGS_OVERRIDE.get()[0]
    if override is not None:
        return override
    if catalog_path is None:
        return CogSettings()
    cog = load_catalog_config(catalog_path, state=state).section("conversion", "cog")
    if not cog:
        return CogSettings()

    # Parse individual settings with type validation
    compression = cog.get("compression")
    compression = compression.upper() if isinstance(compression, str) else "DEFLATE"

    quality = cog.get("quality")
    if not isinstance(quality, int):
        quality = None
    elif quality is not None:
        # Clamp quality to valid range
        quality = max(1, min(100, quality))

    tile_size = cog.get("tile_size")
    if not isinstance(tile_size, int):
        tile_size = 512

    # "auto" derives predictor and resampling from the source; unparsable values fall back to it.
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
    if not isinstance(thumbnail_quality, int) or not 1 <= thumbnail_quality <= _MAX_QUALITY:
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


# Valid spatial index types supported by geoparquet-io
VALID_SPATIAL_INDEXES: frozenset[str] = frozenset({"h3", "quadkey", "s2", "a5", "kdtree", "none"})

# Valid sort methods
VALID_SORT_METHODS: frozenset[str] = frozenset({"hilbert", "quadkey", "none"})

# Defaults that make GeoParquet conform to the Portolan profile; named so parse fallbacks match.
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
    """Configuration for vector (GeoParquet) conversion."""

    spatial_index: str = "none"
    resolution: int | str = "auto"
    sort: str = DEFAULT_SORT
    add_bbox: bool = DEFAULT_ADD_BBOX
    partition: bool = False


def validate_vector_settings(settings: VectorSettings) -> list[str]:
    """Validate vector settings and return warnings for any issues."""
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


# Warnings already shown, keyed by catalog and message, so a batch warns once, not per file.
_CONFIG_WARNINGS_SHOWN: set[tuple[str, str]] = set()


def reset_config_warning_cache() -> None:
    """Forget which config warnings were already shown."""
    _CONFIG_WARNINGS_SHOWN.clear()


def _warn_once(catalog_path: Path, message: str) -> None:
    """Log a config problem as a warning once, and at debug level after that."""
    key = (str(catalog_path), message)
    if key in _CONFIG_WARNINGS_SHOWN:
        logger.debug(message)
        return
    _CONFIG_WARNINGS_SHOWN.add(key)
    logger.warning(message)


def get_vector_settings(
    catalog_path: Path | None, *, state: ConfigSource | None = None
) -> VectorSettings:
    """Load vector conversion settings from catalog config."""
    override = _SETTINGS_OVERRIDE.get()[1]
    if override is not None:
        return override
    if catalog_path is None:
        return VectorSettings()
    vector = load_catalog_config(catalog_path, state=state).section("conversion", "vector")
    if not vector:
        return VectorSettings()

    # Parse spatial_index
    spatial_index = vector.get("spatial_index")
    spatial_index = "none" if not isinstance(spatial_index, str) else spatial_index.lower()

    # Parse resolution. YAML can hold any type here, such as a float.
    raw_resolution: Any = vector.get("resolution", "auto")
    resolution: int | str = "auto"
    if isinstance(raw_resolution, str):
        with suppress(ValueError):
            resolution = int(raw_resolution)
    elif isinstance(raw_resolution, int):
        resolution = raw_resolution

    # Parse sort. A missing or non-string value keeps the default.
    sort = vector.get("sort")
    sort = DEFAULT_SORT if not isinstance(sort, str) else sort.lower()

    # Parse add_bbox
    add_bbox = vector.get("add_bbox")
    if not isinstance(add_bbox, bool):
        add_bbox = DEFAULT_ADD_BBOX

    # Parse partition
    partition = vector.get("partition")
    if not isinstance(partition, bool):
        partition = False

    # Normalize invalid values; the operator sees these, not only the log.
    if spatial_index not in VALID_SPATIAL_INDEXES:
        _warn_once(
            catalog_path,
            f"Vector config: unknown spatial_index '{spatial_index}', using 'none'",
        )
        spatial_index = "none"

    if sort not in VALID_SORT_METHODS:
        _warn_once(
            catalog_path,
            f"Vector config: unknown sort '{sort}', using '{DEFAULT_SORT}'",
        )
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
