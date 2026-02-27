"""File classification logic for portolan scan.

This module provides the 10-category file classification system:
- GEO_ASSET: Primary geospatial data (GeoParquet, GeoJSON, Shapefile, etc.)
- KNOWN_SIDECAR: Shapefile sidecars, aux.xml, ovr files
- TABULAR_DATA: Non-geo parquet, CSV, TSV, XLSX
- STAC_METADATA: catalog.json, collection.json, STAC items
- DOCUMENTATION: .md, .txt, README files
- VISUALIZATION: .pmtiles, .mbtiles
- THUMBNAIL: Small .png, .jpg, .jpeg, .webp (< 1MB)
- STYLE: style.json, MapLibre style definitions
- JUNK: .exe, __pycache__, .git, IDE directories
- UNKNOWN: Unclassified files requiring user review

Functions:
    classify_file: Classify a single file into one of 10 categories.
    get_skip_reason: Get human-readable skip reason for a file category.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path

# =============================================================================
# Extension Mappings
# =============================================================================

# Primary geospatial formats (GEO_ASSET)
GEO_ASSET_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".geojson",
        ".shp",
        ".gpkg",
        ".fgb",
        ".parquet",
        ".tif",
        ".tiff",
        ".jp2",
    }
)

# Shapefile sidecars (KNOWN_SIDECAR)
# Note: .aux.xml removed - Path.suffix returns ".xml" not ".aux.xml",
# so .xml already catches these files
SIDECAR_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".dbf",
        ".shx",
        ".prj",
        ".cpg",
        ".sbn",
        ".sbx",
        ".ovr",
        ".xml",
    }
)

# Tabular data formats (TABULAR_DATA)
TABULAR_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".csv",
        ".tsv",
        ".xlsx",
        ".xls",
    }
)

# Documentation formats (DOCUMENTATION)
DOC_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".md",
        ".txt",
        ".rst",
        ".html",
        ".htm",
    }
)

# Visualization formats (VISUALIZATION)
VIZ_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".pmtiles",
        ".mbtiles",
    }
)

# Thumbnail/image formats (THUMBNAIL) - checked with size
IMAGE_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".png",
        ".jpg",
        ".jpeg",
        ".webp",
        ".gif",
    }
)

# Junk file extensions (JUNK)
JUNK_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".exe",
        ".dll",
        ".so",
        ".dylib",
        ".pyc",
        ".pyo",
        ".class",
        ".o",
        ".obj",
    }
)

# Junk directory names (JUNK)
JUNK_DIRS: frozenset[str] = frozenset(
    {
        "__pycache__",
        ".git",
        ".svn",
        ".hg",
        ".idea",
        ".vscode",
        "node_modules",
        ".tox",
        ".pytest_cache",
    }
)

# STAC/catalog metadata filenames (STAC_METADATA)
STAC_FILENAMES: frozenset[str] = frozenset(
    {
        "catalog.json",
        "collection.json",
        "versions.json",  # Portolan version history
    }
)

# Style filenames (STYLE)
STYLE_FILENAMES: frozenset[str] = frozenset(
    {
        "style.json",
    }
)

# Max size for thumbnail classification (1MB)
THUMBNAIL_MAX_SIZE: int = 1024 * 1024


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
    """Check if path is inside a junk directory.

    Uses case-insensitive matching to handle Windows/macOS filesystems
    where __PYCACHE__ or .GIT are valid directory names.
    """
    for part in path.parts:
        if part.lower() in JUNK_DIRS:
            return True
    return False


def classify_file(
    path: Path,
    size_bytes: int | None = None,
) -> tuple[FileCategory, SkipReasonType | None, str | None]:
    """Classify a file into one of 10 categories.

    Args:
        path: Path to the file.
        size_bytes: File size in bytes (optional, will stat if not provided).

    Returns:
        Tuple of (category, skip_reason_type, skip_reason_message).
        If category is GEO_ASSET, skip_reason_type and message are None.
    """
    name = path.name.lower()
    ext = path.suffix.lower()

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

    if name in STYLE_FILENAMES:
        return (
            FileCategory.STYLE,
            SkipReasonType.METADATA_FILE,
            f"{name} is a map style definition",
        )

    # Check by extension
    if ext in GEO_ASSET_EXTENSIONS:
        return (FileCategory.GEO_ASSET, None, None)

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

    # Check images for thumbnail classification
    if ext in IMAGE_EXTENSIONS:
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
        else:
            # Large image - might be a raster, but not our known formats
            return (
                FileCategory.UNKNOWN,
                SkipReasonType.UNKNOWN_FORMAT,
                "Large image file - unknown if geospatial raster",
            )

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
    """Get skip reason for a non-geo-asset file.

    Args:
        category: The file's classification category.
        path: Path to the file (for context in message).

    Returns:
        Tuple of (SkipReasonType, human-readable message).

    Raises:
        ValueError: If category is GEO_ASSET (which isn't skipped).
    """
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
