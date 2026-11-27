"""File classification logic for portolan scan."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from portolan_cli.formats import is_geoparquet
from portolan_core import extension_registry as _reg

if TYPE_CHECKING:
    from pathlib import Path

# Extension mappings, derived from extension_registry. Edit rows there, not these frozensets.

# Primary geospatial formats. .parquet needs a metadata peek, through the same
# formats.is_geoparquet that the format detector uses.
GEO_ASSET_EXTENSIONS: frozenset[str] = _reg.extensions_where(scan_category="geo_asset")

# Shapefile / raster sidecars (KNOWN_SIDECAR). Compound forms like .aux.xml are
# caught by .xml here, since Path.suffix returns only the last extension.
SIDECAR_EXTENSIONS: frozenset[str] = _reg.extensions_where(scan_category="known_sidecar")

# Tabular data (TABULAR_DATA). .parquet is here because plain Parquet is tabular;
# GeoParquet is detected by is_geoparquet()'s metadata peek.
TABULAR_EXTENSIONS: frozenset[str] = _reg.extensions_where(is_tabular=True)

# Documentation (DOCUMENTATION).
DOC_EXTENSIONS: frozenset[str] = _reg.extensions_where(scan_category="documentation")

# Visualization. .pmtiles is not here: it is a primary cloud-native format (GEO_ASSET).
VIZ_EXTENSIONS: frozenset[str] = _reg.extensions_where(scan_category="visualization")

# Thumbnail/image formats (THUMBNAIL) — checked with size.
IMAGE_EXTENSIONS: frozenset[str] = _reg.extensions_where(scan_category="thumbnail")

# Junk file extensions (JUNK).
JUNK_EXTENSIONS: frozenset[str] = _reg.extensions_where(scan_category="junk")

# Junk directory names (JUNK).
JUNK_DIRS: frozenset[str] = _reg.JUNK_DIRS

# STAC/catalog metadata filenames (STAC_METADATA).
STAC_FILENAMES: frozenset[str] = _reg.STAC_FILENAMES

# Style filenames (STYLE).
STYLE_FILENAMES: frozenset[str] = _reg.STYLE_FILENAMES

# Max size for thumbnail classification (1MB).
THUMBNAIL_MAX_SIZE: int = _reg.THUMBNAIL_MAX_SIZE


class FileCategory(Enum):
    """Classification categories for scanned files."""

    GEO_ASSET = "geo_asset"
    KNOWN_SIDECAR = "known_sidecar"
    TABULAR_DATA = "tabular_data"
    STAC_METADATA = "stac_metadata"
    DOCUMENTATION = "documentation"
    VISUALIZATION = "visualization"
    THUMBNAIL = "thumbnail"
    STYLE = "style"
    JUNK = "junk"
    UNKNOWN = "unknown"


class SkipReasonType(Enum):
    """Categories of skip reasons."""

    NOT_GEOSPATIAL = "not_geospatial"
    SIDECAR_FILE = "sidecar_file"
    VISUALIZATION_ONLY = "visualization"
    METADATA_FILE = "metadata_file"
    JUNK_FILE = "junk_file"
    INVALID_FORMAT = "invalid_format"
    SPECIAL_DIRECTORY = "special_directory"
    UNKNOWN_FORMAT = "unknown_format"


@dataclass(frozen=True)
class SkippedFile:
    """A file that was skipped during scan with reason."""

    path: Path
    relative_path: str
    category: FileCategory
    reason_type: SkipReasonType
    reason_message: str

    def to_dict(self) -> dict[str, str]:
        """Convert to dictionary for JSON serialization."""
        return {
            "path": str(self.path),
            "relative_path": self.relative_path,
            "category": self.category.value,
            "reason_type": self.reason_type.value,
            "reason": self.reason_message,
        }


def _is_in_junk_dir(path: Path) -> bool:
    """Check if path is inside a junk directory."""
    return any(part.lower() in JUNK_DIRS for part in path.parts)


def _classify_parquet(path: Path) -> tuple[FileCategory, SkipReasonType | None, str | None]:
    """Tell GeoParquet (with 'geo' metadata) from plain Parquet (tabular data)."""
    if is_geoparquet(path):
        return (FileCategory.GEO_ASSET, None, None)
    return (
        FileCategory.TABULAR_DATA,
        SkipReasonType.NOT_GEOSPATIAL,
        "PARQUET is tabular data without GeoParquet metadata",
    )


def _classify_by_extension(
    ext: str, path: Path
) -> tuple[FileCategory, SkipReasonType | None, str | None] | None:
    """Classify a file by its extension alone."""
    if ext in GEO_ASSET_EXTENSIONS:
        return (FileCategory.GEO_ASSET, None, None)

    # Special handling for .parquet: peek at metadata to distinguish
    # GeoParquet (has 'geo' metadata) from plain Parquet (tabular data)
    if ext == ".parquet":
        return _classify_parquet(path)

    if ext in SIDECAR_EXTENSIONS:
        return (
            FileCategory.KNOWN_SIDECAR,
            SkipReasonType.SIDECAR_FILE,
            f"{ext} is a sidecar file for a primary asset",
        )

    if ext in TABULAR_EXTENSIONS:
        return (
            FileCategory.TABULAR_DATA,
            SkipReasonType.NOT_GEOSPATIAL,
            f"{ext[1:].upper()} is tabular data, not a geospatial format",
        )

    if ext in DOC_EXTENSIONS:
        return (
            FileCategory.DOCUMENTATION,
            SkipReasonType.NOT_GEOSPATIAL,
            f"{ext[1:].upper()} is documentation, not a geospatial format",
        )

    if ext in VIZ_EXTENSIONS:
        return (
            FileCategory.VISUALIZATION,
            SkipReasonType.VISUALIZATION_ONLY,
            f"{ext[1:]} files are visualization-only, not primary geospatial data",
        )

    if ext in JUNK_EXTENSIONS:
        return (
            FileCategory.JUNK,
            SkipReasonType.JUNK_FILE,
            f"{ext[1:]} files are not geospatial data",
        )

    return None


def _classify_by_name(
    path: Path, name: str, ext: str
) -> tuple[FileCategory, SkipReasonType | None, str | None] | None:
    """Classify a file by its directory and its name."""
    # Check junk directories first
    if _is_in_junk_dir(path):
        return (
            FileCategory.JUNK,
            SkipReasonType.JUNK_FILE,
            f"File is inside a junk directory ({path.parent.name})",
        )

    # Check specific filenames (STAC, style)
    if name in STAC_FILENAMES:
        return (
            FileCategory.STAC_METADATA,
            SkipReasonType.METADATA_FILE,
            f"{name} is STAC catalog metadata",
        )

    # A STAC item file is named after its parent directory: tile_0_0/tile_0_0.json.
    if ext == ".json" and path.stem.lower() == path.parent.name.lower():
        return (
            FileCategory.STAC_METADATA,
            SkipReasonType.METADATA_FILE,
            f"{path.name} is a STAC item metadata file",
        )

    if name in STYLE_FILENAMES:
        return (
            FileCategory.STYLE,
            SkipReasonType.METADATA_FILE,
            f"{name} is a map style definition",
        )

    # Check if file is inside a styles/ directory
    if path.parent.name == "styles" and ext == ".json":
        return (
            FileCategory.STYLE,
            SkipReasonType.METADATA_FILE,
            f"{path.name} is a map style definition in styles/ directory",
        )

    return None


def _classify_image(
    path: Path, size_bytes: int | None
) -> tuple[FileCategory, SkipReasonType | None, str | None]:
    """Classify an image file as a thumbnail or as an unknown raster by its size."""
    # Get size if not provided
    if size_bytes is None:
        try:
            size_bytes = path.stat().st_size
        except OSError:
            size_bytes = 0

    if size_bytes < THUMBNAIL_MAX_SIZE:
        return (
            FileCategory.THUMBNAIL,
            SkipReasonType.NOT_GEOSPATIAL,
            f"Small image file ({size_bytes} bytes) - likely a thumbnail",
        )
    # Large image - might be a raster, but not our known formats
    return (
        FileCategory.UNKNOWN,
        SkipReasonType.UNKNOWN_FORMAT,
        "Large image file - unknown if geospatial raster",
    )


def classify_file(
    path: Path,
    size_bytes: int | None = None,
) -> tuple[FileCategory, SkipReasonType | None, str | None]:
    """Classify a file into one of 10 categories."""
    ext = path.suffix.lower()
    result = _classify_by_name(path, path.name.lower(), ext)
    if result is not None:
        return result

    # Check by extension (simple extension-based classification)
    result = _classify_by_extension(ext, path)
    if result is not None:
        return result

    # Check images for thumbnail classification
    if ext in IMAGE_EXTENSIONS:
        return _classify_image(path, size_bytes)

    # Unknown extension
    return (
        FileCategory.UNKNOWN,
        SkipReasonType.UNKNOWN_FORMAT,
        f"Unknown file extension: {ext or '(no extension)'}",
    )


def get_skip_reason(
    category: FileCategory,
    path: Path,
) -> tuple[SkipReasonType, str]:
    """Get skip reason for a non-geo-asset file."""
    if category == FileCategory.GEO_ASSET:
        msg = "GEO_ASSET files are not skipped"
        raise ValueError(msg)

    # Map categories to skip reasons and messages
    reason_map: dict[FileCategory, tuple[SkipReasonType, str]] = {
        FileCategory.KNOWN_SIDECAR: (
            SkipReasonType.SIDECAR_FILE,
            f"{path.suffix} is a sidecar file belonging to a primary asset",
        ),
        FileCategory.TABULAR_DATA: (
            SkipReasonType.NOT_GEOSPATIAL,
            f"{path.suffix[1:].upper() if path.suffix else 'File'} is tabular data, not geospatial",
        ),
        FileCategory.STAC_METADATA: (
            SkipReasonType.METADATA_FILE,
            f"{path.name} is STAC catalog metadata",
        ),
        FileCategory.DOCUMENTATION: (
            SkipReasonType.NOT_GEOSPATIAL,
            f"{path.suffix[1:].upper() if path.suffix else 'File'} is documentation",
        ),
        FileCategory.VISUALIZATION: (
            SkipReasonType.VISUALIZATION_ONLY,
            f"{path.suffix[1:] if path.suffix else 'File'} is a visualization format, not primary data",
        ),
        FileCategory.THUMBNAIL: (
            SkipReasonType.NOT_GEOSPATIAL,
            "Image file is a thumbnail preview",
        ),
        FileCategory.STYLE: (
            SkipReasonType.METADATA_FILE,
            f"{path.name} is a map style definition",
        ),
        FileCategory.JUNK: (
            SkipReasonType.JUNK_FILE,
            f"{path.name} is a system/build file, not geospatial data",
        ),
        FileCategory.UNKNOWN: (
            SkipReasonType.UNKNOWN_FORMAT,
            f"Unknown file format: {path.suffix or '(no extension)'}",
        ),
    }

    return reason_map.get(
        category,
        (SkipReasonType.UNKNOWN_FORMAT, f"Unknown category: {category.value}"),
    )
