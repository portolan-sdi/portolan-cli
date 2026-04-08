"""Directory scanning and issue detection for geospatial files.

This module provides read-only analysis of directory structures to identify
geospatial files ready for import and detect structural issues that could
cause import failures.

The scan command follows the ruff model: separate scanning/validation from
import (like ruff check vs ruff format). See ADR-0016 for rationale.

Example:
    >>> from portolan_cli.scan import scan_directory, ScanOptions
    >>> result = scan_directory(Path("/data/geospatial"))
    >>> print(f"Found {len(result.ready)} files ready to import")
    >>> if result.has_errors:
    ...     print(f"Found {result.error_count} errors")

Phase 1 MVP features:
- File discovery with extension filtering
- Recursive scanning with depth controls
- Hidden file and symlink handling
- Issue detection (14 types with 3 severity levels)
- Human-readable and JSON output
"""

from __future__ import annotations

import os
import re
from collections import defaultdict
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    # Type alias for progress callback
    ProgressCallback = Callable[[], None]

# Import new types from scan modules
from portolan_cli.collection_id import (
    CollectionIdError,
    normalize_collection_id,
    validate_collection_id,
)
from portolan_cli.constants import PARQUET_EXTENSION, WINDOWS_RESERVED_NAMES

# Import format detection from formats.py (ADR-0010: delegate to upstream)
from portolan_cli.formats import (
    CloudNativeStatus,
    FormatInfo,
    FormatType,
    _detect_json_type,
    get_cloud_native_status,
    is_geoparquet,
    is_valid_parquet,
)
from portolan_cli.scan_classify import (
    STAC_FILENAMES,
    FileCategory,
    SkippedFile,
    SkipReasonType,
    classify_file,
)
from portolan_cli.scan_detect import (
    FILEGDB_LOCK_PATTERNS,
    DualFormatPair,
    SpecialFormat,
    is_filegdb,
)
from portolan_cli.scan_fix import ProposedFix
from portolan_cli.scan_infer import CollectionSuggestion

# =============================================================================
# Constants
# =============================================================================

# Recognized geospatial file extensions (primary assets)
# Note: .parquet is NOT here because we need to check metadata to distinguish
# GeoParquet (geospatial) from regular Parquet (tabular data).
RECOGNIZED_VECTOR_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".geojson",
        ".shp",
        ".gpkg",
        ".fgb",
        ".pmtiles",  # PMTiles: cloud-native vector tiles (issue #198)
    }
)

RECOGNIZED_RASTER_EXTENSIONS: frozenset[str] = frozenset({".tif", ".tiff", ".jp2"})

RECOGNIZED_EXTENSIONS: frozenset[str] = RECOGNIZED_VECTOR_EXTENSIONS | RECOGNIZED_RASTER_EXTENSIONS

# Note: PARQUET_EXTENSION imported from portolan_cli.constants

# Overview/derivative formats (not primary assets)
# Note: .pmtiles was removed from here — PMTiles is a primary cloud-native
# format, not a derivative. See issue #198 and formats.py CLOUD_NATIVE_EXTENSIONS.
OVERVIEW_EXTENSIONS: frozenset[str] = frozenset()

# Shapefile sidecar extensions
SHAPEFILE_REQUIRED_SIDECARS: frozenset[str] = frozenset({".dbf", ".shx"})
SHAPEFILE_OPTIONAL_SIDECARS: frozenset[str] = frozenset({".prj", ".cpg", ".sbn", ".sbx"})
SHAPEFILE_ALL_SIDECARS: frozenset[str] = SHAPEFILE_REQUIRED_SIDECARS | SHAPEFILE_OPTIONAL_SIDECARS

# Path length threshold for warnings
LONG_PATH_THRESHOLD: int = 200

# Pattern for invalid filename characters
# Matches: spaces, parentheses, brackets, control chars (0x00-0x1F, 0x7F), and non-ASCII
INVALID_CHAR_PATTERN: re.Pattern[str] = re.compile(r"[\s()\[\]\x00-\x1f\x7f]|[^\x00-\x7f]")

# WINDOWS_RESERVED_NAMES imported from portolan_cli.constants


# =============================================================================
# Enums
# =============================================================================


class Severity(Enum):
    """Issue severity levels."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class IssueType(Enum):
    """Categories of scan issues."""

    # Existing issue types
    INCOMPLETE_SHAPEFILE = "incomplete_shapefile"
    ZERO_BYTE_FILE = "zero_byte_file"
    SYMLINK_LOOP = "symlink_loop"
    BROKEN_SYMLINK = "broken_symlink"
    PERMISSION_DENIED = "permission_denied"
    INVALID_CHARACTERS = "invalid_characters"
    MULTIPLE_PRIMARIES = "multiple_primaries"
    LONG_PATH = "long_path"
    DUPLICATE_BASENAME = "duplicate_basename"
    MIXED_FORMATS = "mixed_formats"

    # NEW: Special format detection
    FILEGDB_DETECTED = "filegdb_detected"
    HIVE_PARTITION_DETECTED = "hive_partition"
    EXISTING_CATALOG = "existing_catalog"
    DUAL_FORMAT = "dual_format"

    # NEW: Cross-platform compatibility
    WINDOWS_RESERVED_NAME = "windows_reserved_name"
    PATH_TOO_LONG = "path_too_long"

    # NEW: Structure issues
    MIXED_FLAT_MULTIITEM = "mixed_flat_multiitem"
    ORPHAN_SIDECAR = "orphan_sidecar"
    MULTIPLE_GEO_PRIMARIES = "multiple_geo_primaries"

    # NEW: Collection ID validation
    INVALID_COLLECTION_ID = "invalid_collection_id"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass(frozen=True)
class ScanOptions:
    """Configuration options for scan operation."""

    # Existing options
    recursive: bool = True
    max_depth: int | None = None
    include_hidden: bool = False
    follow_symlinks: bool = False

    # NEW: Output control
    show_all: bool = False  # Don't truncate output
    verbose: bool = False  # Show detailed skip reasons

    # NEW: Special format handling
    allow_existing_catalogs: bool = False  # Proceed even if STAC catalog found

    # NEW: Fix modes
    fix: bool = False  # Apply safe fixes
    unsafe_fix: bool = False  # Apply unsafe fixes (requires fix=True)
    dry_run: bool = False  # Preview fixes without applying

    # NEW: Collection inference
    suggest_collections: bool = False  # Suggest collection groupings

    # NEW: Strict mode (Phase 4)
    strict: bool = False  # Treat warnings as errors

    def __post_init__(self) -> None:
        """Validate options."""
        if self.unsafe_fix and not self.fix:
            msg = "--unsafe-fix requires --fix"
            raise ValueError(msg)


@dataclass(frozen=True)
class ScannedFile:
    """A geospatial file ready for import.

    Attributes:
        path: Absolute path to the file or directory.
        relative_path: Path relative to scan root, using forward slashes.
        extension: File extension (e.g., ".parquet", ".gdb").
        format_type: Whether this is VECTOR or RASTER data.
        size_bytes: Total size in bytes.
        inferred_collection_id: Nested collection path derived from directory structure.
            For nested catalogs (ADR-0032), this is the parent directory path relative
            to the scan root. Example: "climate/hittekaart" for a file at
            climate/hittekaart/data.parquet. Empty string for files at scan root.
        format_status: Cloud-native status classification (CLOUD_NATIVE, CONVERTIBLE, UNSUPPORTED).
        format_display_name: Human-readable format name (e.g., "GeoParquet", "GeoTIFF (not COG)").
        metadata: Format-specific metadata (e.g., gdbtable_count for FileGDB).
    """

    path: Path
    relative_path: str
    extension: str
    format_type: FormatType
    size_bytes: int
    inferred_collection_id: str = ""
    format_status: CloudNativeStatus = CloudNativeStatus.CLOUD_NATIVE
    format_display_name: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def basename(self) -> str:
        """Filename without directory."""
        return self.path.name


@dataclass(frozen=True)
class ScanIssue:
    """A detected problem with a file or directory."""

    path: Path
    relative_path: str
    issue_type: IssueType
    severity: Severity
    message: str
    suggestion: str | None = None


@dataclass
class ScanResult:
    """Aggregate result of directory scan."""

    root: Path
    ready: list[ScannedFile]
    issues: list[ScanIssue]
    # CHANGED: Now accepts either Path (legacy) or SkippedFile (new)
    skipped: list[Path | SkippedFile]
    directories_scanned: int

    # NEW: Special formats detected (FileGDB, Hive partitions, etc.)
    special_formats: list[SpecialFormat] = field(default_factory=list)

    # NEW: Collection suggestions (when suggest_collections=True)
    collection_suggestions: list[CollectionSuggestion] = field(default_factory=list)

    # NEW: Dual-format pairs detected
    dual_format_pairs: list[DualFormatPair] = field(default_factory=list)

    # NEW: Fix mode results
    proposed_fixes: list[ProposedFix] = field(default_factory=list)
    applied_fixes: list[ProposedFix] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        """True if any issue has ERROR severity."""
        return any(i.severity == Severity.ERROR for i in self.issues)

    @property
    def error_count(self) -> int:
        """Count of ERROR severity issues."""
        return sum(1 for i in self.issues if i.severity == Severity.ERROR)

    @property
    def warning_count(self) -> int:
        """Count of WARNING severity issues."""
        return sum(1 for i in self.issues if i.severity == Severity.WARNING)

    @property
    def info_count(self) -> int:
        """Count of INFO severity issues."""
        return sum(1 for i in self.issues if i.severity == Severity.INFO)

    @property
    def classification_summary(self) -> dict[str, int]:
        """Count of files by category.

        Includes ready files as GEO_ASSET and skipped files by their category.
        """
        from collections import Counter

        counts: Counter[str] = Counter()
        # All ready files are geo_assets
        counts["geo_asset"] = len(self.ready)
        # Count skipped by category
        for item in self.skipped:
            if isinstance(item, SkippedFile):
                counts[item.category.value] += 1
            else:
                # Legacy Path - count as unknown
                counts["unknown"] += 1
        return dict(counts)

    def to_dict(self) -> dict[str, object]:
        """Convert to JSON-serializable dictionary."""
        base: dict[str, object] = {
            "root": str(self.root),
            "summary": {
                "directories_scanned": self.directories_scanned,
                "ready_count": len(self.ready),
                "issue_count": len(self.issues),
                "skipped_count": len(self.skipped),
                "special_formats_count": len(self.special_formats),
                "suggested_collections_count": len(self.collection_suggestions),
            },
            "classification": self.classification_summary,
            "ready": [
                {
                    "path": str(f.path),
                    "relative_path": f.relative_path,
                    "extension": f.extension,
                    "format_type": f.format_type.value,
                    "size_bytes": f.size_bytes,
                    "inferred_collection_id": f.inferred_collection_id or None,
                    "format_status": f.format_status.value if f.format_status else None,
                    "format_display_name": f.format_display_name or None,
                    "metadata": f.metadata,
                }
                for f in self.ready
            ],
            "issues": [
                {
                    "path": str(i.path),
                    "relative_path": i.relative_path,
                    "type": i.issue_type.value,
                    "severity": i.severity.value,
                    "message": i.message,
                    "suggestion": i.suggestion,
                }
                for i in self.issues
            ],
            "skipped": [
                item.to_dict()
                if isinstance(item, SkippedFile)
                else {
                    "path": str(item),
                    "relative_path": str(item.relative_to(self.root))
                    if self._is_relative_to(item, self.root)
                    else str(item),
                    "reason": "unsupported_format",
                }
                for item in self.skipped
            ],
            "special_formats": [sf.to_dict() for sf in self.special_formats],
            "collection_suggestions": [cs.to_dict() for cs in self.collection_suggestions],
            "dual_format_pairs": [dfp.to_dict() for dfp in self.dual_format_pairs],
        }
        if self.proposed_fixes:
            base["proposed_fixes"] = [pf.to_dict() for pf in self.proposed_fixes]
        if self.applied_fixes:
            base["applied_fixes"] = [af.to_dict() for af in self.applied_fixes]
        return base

    @staticmethod
    def _is_relative_to(path: Path, base: Path) -> bool:
        """Check if path is relative to base (Python 3.9+ compatible)."""
        try:
            path.relative_to(base)
            return True
        except ValueError:
            return False


# =============================================================================
# Internal Helper Classes
# =============================================================================


@dataclass
class _ScanContext:
    """Internal context for tracking scan state."""

    root: Path
    options: ScanOptions
    visited_inodes: set[tuple[int, int]] = field(default_factory=set)
    ready: list[ScannedFile] = field(default_factory=list)
    issues: list[ScanIssue] = field(default_factory=list)
    skipped: list[Path | SkippedFile] = field(default_factory=list)
    directories_scanned: int = 0
    # For detecting duplicates and multiple primaries
    basenames: dict[str, list[Path]] = field(default_factory=lambda: defaultdict(list))
    primaries_by_dir: dict[Path, list[Path]] = field(default_factory=lambda: defaultdict(list))
    formats_by_dir: dict[Path, set[FormatType]] = field(default_factory=lambda: defaultdict(set))
    # Track shapefile sidecars
    shapefile_sidecars: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    # Track special formats (FileGDB, etc.)
    special_formats: list[SpecialFormat] = field(default_factory=list)
    # Progress callback (called for each directory scanned)
    progress_callback: Callable[[], None] | None = None


# =============================================================================
# Helper Functions
# =============================================================================


def _is_recognized_extension(ext: str) -> bool:
    """Check if extension is a recognized geospatial format."""
    return ext.lower() in RECOGNIZED_EXTENSIONS


def _is_sidecar_extension(ext: str) -> bool:
    """Check if extension is a shapefile sidecar."""
    return ext.lower() in SHAPEFILE_ALL_SIDECARS


def _get_format_type(ext: str) -> FormatType:
    """Get the format type (vector/raster) for an extension."""
    ext_lower = ext.lower()
    if ext_lower in RECOGNIZED_VECTOR_EXTENSIONS:
        return FormatType.VECTOR
    if ext_lower in RECOGNIZED_RASTER_EXTENSIONS:
        return FormatType.RASTER
    # Default to vector for unknown (shouldn't happen with proper filtering)
    return FormatType.VECTOR


def _is_hidden(name: str) -> bool:
    """Check if a filename is hidden (starts with dot)."""
    return name.startswith(".")


def _has_invalid_characters(name: str) -> bool:
    """Check if filename contains invalid characters."""
    return bool(INVALID_CHAR_PATTERN.search(name))


def _get_relative_path(path: Path, root: Path) -> str:
    """Get path relative to root as forward-slash string.

    Returns paths with forward slashes regardless of OS for STAC compatibility.
    STAC uses URL-style paths which always use forward slashes.
    """
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def _infer_collection_id_from_relative_path(relative_path: str) -> str:
    """Infer nested collection ID from a relative path (ADR-0032).

    The collection ID is the directory portion of the relative path,
    representing the leaf collection in a nested catalog structure.

    Examples:
        "data.parquet" -> ""
        "collection/data.parquet" -> "collection"
        "climate/hittekaart/data.parquet" -> "climate/hittekaart"
        "env/air/quality/pm25.parquet" -> "env/air/quality"

    Args:
        relative_path: Path relative to scan root, using forward slashes.

    Returns:
        Collection ID (parent directory path). Empty string for root-level files.
    """
    # Find the last slash - everything before it is the collection ID
    last_slash_idx = relative_path.rfind("/")
    if last_slash_idx == -1:
        # File is at root level (no directory component)
        return ""
    return relative_path[:last_slash_idx]


def _get_format_info(path: Path, ext: str) -> FormatInfo:
    """Get cloud-native status and format info for a file.

    This is a lightweight wrapper around get_cloud_native_status() that
    handles errors gracefully for scan context.

    Args:
        path: Path to the file.
        ext: File extension (lowercase).

    Returns:
        FormatInfo with status, display_name, target_format, and error_message.
    """
    try:
        return get_cloud_native_status(path)
    except (FileNotFoundError, IsADirectoryError):
        # Return a fallback for edge cases
        from portolan_cli.formats import FormatInfo as FI

        return FI(
            status=CloudNativeStatus.CLOUD_NATIVE,
            display_name=ext.upper().lstrip(".") if ext else "Unknown",
            target_format=None,
            error_message=None,
        )


def _make_skipped_file(
    ctx: _ScanContext,
    path: Path,
    size_bytes: int | None = None,
) -> SkippedFile:
    """Create a SkippedFile with proper classification.

    Args:
        ctx: Scan context (provides root for relative path).
        path: Path to the file being skipped.
        size_bytes: File size in bytes (for thumbnail classification).

    Returns:
        SkippedFile with category and reason from classify_file.
    """
    category, reason_type, reason_message = classify_file(path, size_bytes)
    return SkippedFile(
        path=path,
        relative_path=_get_relative_path(path, ctx.root),
        category=category,
        reason_type=reason_type or SkipReasonType.UNKNOWN_FORMAT,
        reason_message=reason_message or f"Unrecognized extension: {path.suffix}",
    )


# NOTE: is_geoparquet() is now imported from portolan_cli.formats (ADR-0010)


def _is_overview_extension(ext: str) -> bool:
    """Check if extension is an overview/derivative format."""
    return ext.lower() in OVERVIEW_EXTENSIONS


# =============================================================================
# Issue Detection Functions
# =============================================================================


def _check_incomplete_shapefile(ctx: _ScanContext, shp_path: Path) -> None:
    """Check if a shapefile has all required sidecars."""
    stem = shp_path.stem
    parent = shp_path.parent
    sidecars = ctx.shapefile_sidecars.get(f"{parent}/{stem}", set())

    missing = SHAPEFILE_REQUIRED_SIDECARS - sidecars
    if missing:
        missing_str = ", ".join(sorted(missing))
        ctx.issues.append(
            ScanIssue(
                path=shp_path,
                relative_path=_get_relative_path(shp_path, ctx.root),
                issue_type=IssueType.INCOMPLETE_SHAPEFILE,
                severity=Severity.ERROR,
                message=f"Shapefile missing required sidecars: {missing_str}",
                suggestion="Add missing sidecar files or remove orphaned .shp",
            )
        )


def _check_zero_byte_file(ctx: _ScanContext, path: Path, size: int) -> bool:
    """Check if file is zero bytes. Returns True if zero-byte (should skip)."""
    if size == 0:
        ctx.issues.append(
            ScanIssue(
                path=path,
                relative_path=_get_relative_path(path, ctx.root),
                issue_type=IssueType.ZERO_BYTE_FILE,
                severity=Severity.ERROR,
                message="File is empty (zero bytes)",
                suggestion="Remove or replace with valid file",
            )
        )
        return True
    return False


def _check_invalid_characters(ctx: _ScanContext, path: Path) -> None:
    """Check for invalid characters in filename."""
    name = path.name
    if _has_invalid_characters(name):
        # Generate suggested name
        suggested = re.sub(r"[\s()\[\]]", "_", name)
        suggested = suggested.encode("ascii", "replace").decode("ascii")
        ctx.issues.append(
            ScanIssue(
                path=path,
                relative_path=_get_relative_path(path, ctx.root),
                issue_type=IssueType.INVALID_CHARACTERS,
                severity=Severity.WARNING,
                message="Filename contains problematic characters (spaces, parentheses, or non-ASCII)",
                suggestion=f"Rename to {suggested}",
            )
        )


def _check_long_path(ctx: _ScanContext, path: Path) -> None:
    """Check if path exceeds length threshold."""
    path_str = str(path)
    if len(path_str) > LONG_PATH_THRESHOLD:
        ctx.issues.append(
            ScanIssue(
                path=path,
                relative_path=_get_relative_path(path, ctx.root),
                issue_type=IssueType.LONG_PATH,
                severity=Severity.WARNING,
                message=f"Path exceeds {LONG_PATH_THRESHOLD} characters ({len(path_str)} chars)",
                suggestion="Consider shortening directory names or moving file",
            )
        )


def _check_symlink_loop(ctx: _ScanContext, path: Path, is_directory: bool) -> bool:
    """Check for symlink loop using inode tracking. Returns True if loop detected.

    Only tracks inodes for directories to prevent infinite recursion.
    File symlinks pointing to the same target are NOT considered loops.

    Args:
        ctx: Scan context with visited_inodes set.
        path: Path to check (must be a symlink).
        is_directory: True if the symlink target is a directory.

    Returns:
        True if a directory symlink loop was detected, False otherwise.
    """
    # Only track directory inodes - file symlinks to same target are valid
    if not is_directory:
        return False

    try:
        stat_info = path.stat()
        inode_key = (stat_info.st_dev, stat_info.st_ino)
        if inode_key in ctx.visited_inodes:
            ctx.issues.append(
                ScanIssue(
                    path=path,
                    relative_path=_get_relative_path(path, ctx.root),
                    issue_type=IssueType.SYMLINK_LOOP,
                    severity=Severity.ERROR,
                    message="Symlink loop detected",
                    suggestion="Remove circular symlink",
                )
            )
            return True
        ctx.visited_inodes.add(inode_key)
    except OSError:
        pass
    return False


def _check_broken_symlink(
    ctx: _ScanContext, path: Path, is_symlink: bool, is_file: bool, is_dir: bool
) -> bool:
    """Check if a symlink is broken (target doesn't exist). Returns True if broken.

    A broken symlink is one where:
    - is_symlink is True (it's a symlink)
    - is_file is False (target isn't a file or doesn't exist)
    - is_dir is False (target isn't a directory)

    Args:
        ctx: Scan context for recording issues.
        path: Path to check.
        is_symlink: True if entry is a symlink.
        is_file: Result of is_file(follow_symlinks=True).
        is_dir: Result of is_dir(follow_symlinks=True).

    Returns:
        True if broken symlink was detected and reported, False otherwise.
    """
    if is_symlink and not is_file and not is_dir:
        # Read the symlink target for the message
        try:
            target = path.readlink()
            target_str = str(target)
        except OSError:
            target_str = "unknown"

        ctx.issues.append(
            ScanIssue(
                path=path,
                relative_path=_get_relative_path(path, ctx.root),
                issue_type=IssueType.BROKEN_SYMLINK,
                severity=Severity.WARNING,
                message=f"Broken symlink: target '{target_str}' does not exist",
                suggestion="Remove symlink or create the target file",
            )
        )
        return True
    return False


def _check_orphan_sidecars(ctx: _ScanContext) -> None:
    """Check for sidecar files without a corresponding primary (.shp) file.

    Orphan sidecars are .dbf, .shx, .prj, etc. files that don't have a
    matching .shp file in the same directory with the same stem.
    """
    # Get all stems that have a .shp file
    shp_stems: set[str] = set()
    for scanned in ctx.ready:
        if scanned.extension == ".shp":
            key = f"{scanned.path.parent}/{scanned.path.stem}"
            shp_stems.add(key)

    # Check each sidecar group for orphans
    for key, sidecars in ctx.shapefile_sidecars.items():
        if key not in shp_stems and sidecars:
            # This is an orphan - sidecars exist but no .shp
            # Extract parent dir and stem from key
            parts = key.rsplit("/", 1)
            if len(parts) == 2:
                parent_str, stem = parts
                parent = Path(parent_str)
            else:
                continue

            # Find one of the sidecar files to use as the issue path
            sidecar_exts = sorted(sidecars)
            example_sidecar = parent / f"{stem}{sidecar_exts[0]}"

            ctx.issues.append(
                ScanIssue(
                    path=example_sidecar,
                    relative_path=_get_relative_path(example_sidecar, ctx.root),
                    issue_type=IssueType.ORPHAN_SIDECAR,
                    severity=Severity.WARNING,
                    message=f"Sidecar files without primary .shp: {', '.join(sidecar_exts)}",
                    suggestion=f"Add {stem}.shp or remove orphan sidecars",
                )
            )


def _check_mixed_structure(ctx: _ScanContext) -> None:
    """Check for mixed flat/multi-item directory structure.

    This detects directories that have both files directly AND files in
    subdirectories, which indicates an unclear catalog structure.
    Is this directory a single item with multiple files, or is each
    subdirectory a separate item?

    This check applies to ALL directories with geo-assets, not just root.

    Algorithm: O(n × depth) where depth is tree depth (typically ~10).
    For each directory with files, walk UP the parent chain checking if
    any ancestor also has files. This replaces the previous O(n²) approach
    that compared all pairs of directories.

    See: https://github.com/portolan-sdi/portolan-cli/issues/314
    """
    # Build set of directories with files for O(1) lookup
    dirs_with_files: set[Path] = {d for d, files in ctx.primaries_by_dir.items() if files}

    # Track directories we've already flagged to avoid duplicates
    flagged_dirs: set[Path] = set()

    # For each directory with files, check if any ANCESTOR also has files
    for dir_path in dirs_with_files:
        # Walk up the parent chain
        parent = dir_path.parent
        while parent != ctx.root.parent and parent != dir_path:
            if parent in dirs_with_files and parent not in flagged_dirs:
                # Parent has files AND we (descendant) have files = mixed structure
                flagged_dirs.add(parent)
                ctx.issues.append(
                    ScanIssue(
                        path=parent,
                        relative_path=_get_relative_path(parent, ctx.root),
                        issue_type=IssueType.MIXED_FLAT_MULTIITEM,
                        severity=Severity.WARNING,
                        message="Directory has both data files AND subdirectories with data",
                        suggestion="Organize as either flat (all files here) or hierarchical (files only in subdirectories)",
                    )
                )
            parent = parent.parent


def _check_multiple_geo_primaries(ctx: _ScanContext) -> None:
    """Check for multiple GeoParquet files in the same directory.

    Multiple GeoParquet files in the same directory creates structural
    ambiguity about which is the primary geo-asset for catalog purposes.
    This is distinct from MULTIPLE_PRIMARIES (generic) because it specifically
    targets the GeoParquet case and enables targeted guidance.

    Key rule: One GeoParquet + multiple plain Parquet = VALID (companions OK)

    Only GeoParquet files (those with geo metadata) are counted as geo-primaries.
    Plain Parquet files (lookup tables, metadata) are companions, not primaries.
    """
    for dir_path, primaries in ctx.primaries_by_dir.items():
        # Filter to only .parquet files
        parquet_files = [p for p in primaries if p.suffix.lower() == ".parquet"]

        if len(parquet_files) < 2:
            continue  # At most 1 parquet file, no issue

        # Count how many are actual GeoParquet (have geo metadata)
        geo_parquets = [p for p in parquet_files if is_geoparquet(p)]

        if len(geo_parquets) > 1:
            names = ", ".join(p.name for p in geo_parquets)
            ctx.issues.append(
                ScanIssue(
                    path=dir_path,
                    relative_path=_get_relative_path(dir_path, ctx.root),
                    issue_type=IssueType.MULTIPLE_GEO_PRIMARIES,
                    severity=Severity.WARNING,
                    message=f"Multiple primary geo-assets in directory: {names}",
                    suggestion="Move to separate subdirectories or reorganize as partitioned data",
                )
            )


def _check_collection_ids(ctx: _ScanContext) -> None:
    """Check directory names for valid collection ID format.

    Per portolan-spec/structure.md, collection IDs SHOULD:
    - Contain only lowercase letters, numbers, hyphens, and underscores
    - Start with a letter
    """
    # Get unique directories that contain geospatial files
    checked_dirs: set[Path] = set()

    for dir_path in ctx.primaries_by_dir:
        # Only check immediate subdirectories of root (potential collections)
        if dir_path == ctx.root:
            continue

        # Get the first-level directory (collection level)
        try:
            relative = dir_path.relative_to(ctx.root)
            collection_dir_name = relative.parts[0]
            collection_dir = ctx.root / collection_dir_name
        except (ValueError, IndexError):
            continue

        # Skip if already checked
        if collection_dir in checked_dirs:
            continue
        checked_dirs.add(collection_dir)

        # Validate the collection ID (directory name)
        is_valid, error_msg = validate_collection_id(collection_dir_name)
        if not is_valid:
            # Generate suggested normalized name
            try:
                normalized = normalize_collection_id(collection_dir_name)
                suggestion = f"Rename to '{normalized}' or use --fix to auto-rename"
            except CollectionIdError:
                suggestion = "Rename directory to use only lowercase letters, numbers, hyphens, and underscores"

            ctx.issues.append(
                ScanIssue(
                    path=collection_dir,
                    relative_path=collection_dir_name,
                    issue_type=IssueType.INVALID_COLLECTION_ID,
                    severity=Severity.WARNING,
                    message=f"Invalid collection ID: {error_msg}",
                    suggestion=suggestion,
                )
            )


def _finalize_multi_asset_checks(ctx: _ScanContext) -> None:
    """Run checks that need all files collected first."""
    # Check for multiple primaries per directory
    for dir_path, primaries in ctx.primaries_by_dir.items():
        if len(primaries) > 1:
            names = ", ".join(p.name for p in primaries)
            ctx.issues.append(
                ScanIssue(
                    path=dir_path,
                    relative_path=_get_relative_path(dir_path, ctx.root),
                    issue_type=IssueType.MULTIPLE_PRIMARIES,
                    severity=Severity.WARNING,
                    message=f"Directory has {len(primaries)} primary assets: {names}",
                    suggestion="Split into separate directories, or track each asset as a separate item",
                )
            )

    # Check for mixed formats per directory
    for dir_path, formats in ctx.formats_by_dir.items():
        if len(formats) > 1:
            ctx.issues.append(
                ScanIssue(
                    path=dir_path,
                    relative_path=_get_relative_path(dir_path, ctx.root),
                    issue_type=IssueType.MIXED_FORMATS,
                    severity=Severity.INFO,
                    message="Directory contains both raster and vector formats",
                    suggestion="Consider separating by format type",
                )
            )

    # Check for duplicate basenames WITHIN the same directory only.
    # Files with same name in sibling directories (e.g., 2010/radios.parquet and
    # 2022/radios.parquet) are intentional organization, not duplicates.
    for _basename, paths in ctx.basenames.items():
        if len(paths) > 1:
            # Group by parent directory
            by_dir: dict[Path, list[Path]] = defaultdict(list)
            for p in paths:
                by_dir[p.parent].append(p)
            # Only warn about directories with multiple files of same basename
            for _dir_path, same_dir_paths in by_dir.items():
                if len(same_dir_paths) > 1:
                    names = ", ".join(p.name for p in same_dir_paths)
                    ctx.issues.append(
                        ScanIssue(
                            path=same_dir_paths[0],
                            relative_path=_get_relative_path(same_dir_paths[0], ctx.root),
                            issue_type=IssueType.DUPLICATE_BASENAME,
                            severity=Severity.INFO,
                            message=f"Duplicate basenames in same directory: {names}",
                            suggestion="Rename files to have unique names",
                        )
                    )


def is_windows_reserved_name(name: str) -> bool:
    """Check if a filename is a Windows reserved device name.

    Args:
        name: Filename (stem only, without extension) to check.

    Returns:
        True if the name is a Windows reserved name.
    """
    return name.lower() in WINDOWS_RESERVED_NAMES


def _check_windows_reserved(ctx: _ScanContext, path: Path) -> None:
    """Check if path contains Windows reserved names.

    This is a cross-platform compatibility check. Files with Windows
    reserved names (CON, PRN, AUX, NUL, COMx, LPTx) cannot be created
    or accessed on Windows systems.
    """
    # Check the file/directory name itself
    stem = path.stem.lower()
    if stem in WINDOWS_RESERVED_NAMES:
        ctx.issues.append(
            ScanIssue(
                path=path,
                relative_path=_get_relative_path(path, ctx.root),
                issue_type=IssueType.WINDOWS_RESERVED_NAME,
                severity=Severity.WARNING,
                message=f"'{path.stem}' is a Windows reserved name",
                suggestion="Rename to avoid cross-platform issues",
            )
        )
        return

    # Check if any parent directory has a reserved name
    for parent in path.parents:
        if parent == ctx.root:
            break
        if parent.name.lower() in WINDOWS_RESERVED_NAMES:
            ctx.issues.append(
                ScanIssue(
                    path=path,
                    relative_path=_get_relative_path(path, ctx.root),
                    issue_type=IssueType.WINDOWS_RESERVED_NAME,
                    severity=Severity.WARNING,
                    message=f"File is in directory '{parent.name}' which is a Windows reserved name",
                    suggestion="Rename parent directory to avoid cross-platform issues",
                )
            )
            break


# =============================================================================
# Core Scan Functions
# =============================================================================


def _get_dir_size(path: Path) -> int:
    """Calculate total size of all files in a directory (non-recursive).

    Args:
        path: Path to directory.

    Returns:
        Total size in bytes of all files in the directory.
    """
    total = 0
    try:
        for entry in os.scandir(path):
            if entry.is_file(follow_symlinks=False):
                try:
                    total += entry.stat(follow_symlinks=False).st_size
                except OSError:
                    pass
    except OSError:
        pass
    return total


def _gather_filegdb_metadata(path: Path) -> dict[str, Any]:
    """Gather FileGDB-specific metadata.

    Args:
        path: Path to FileGDB directory.

    Returns:
        Dict with keys:
        - gdbtable_count: Number of .gdbtable files
        - lock_files_present: True if lock files detected (ArcGIS may have it open)
    """
    gdbtable_count = 0
    lock_files_present = False

    try:
        for entry in os.scandir(path):
            name_lower = entry.name.lower()
            if name_lower.endswith(".gdbtable"):
                gdbtable_count += 1
            # Check for lock files (ArcGIS patterns)
            for lock_pattern in FILEGDB_LOCK_PATTERNS:
                if lock_pattern in name_lower:
                    lock_files_present = True
                    break
    except OSError:
        pass

    return {
        "gdbtable_count": gdbtable_count,
        "lock_files_present": lock_files_present,
    }


def _discover_files(
    ctx: _ScanContext,
) -> Iterator[tuple[Path, int]]:
    """Discover files using os.walk with depth control.

    Yields (file_path, file_size) tuples for all files found.
    Uses os.walk for performance as recommended in design doc.
    """
    root = ctx.root
    options = ctx.options

    # Calculate effective max depth
    effective_max_depth: int | None
    if not options.recursive:
        effective_max_depth = 0
    else:
        effective_max_depth = options.max_depth

    def _walk_with_depth(start: Path, current_depth: int = 0) -> Iterator[tuple[Path, int]]:
        """Walk directory with depth tracking."""
        # Check depth limit BEFORE incrementing counter
        if effective_max_depth is not None and current_depth > effective_max_depth:
            return

        ctx.directories_scanned += 1
        # Call progress callback if provided
        if ctx.progress_callback is not None:
            ctx.progress_callback()

        try:
            entries = list(os.scandir(start))
        except PermissionError:
            ctx.issues.append(
                ScanIssue(
                    path=start,
                    relative_path=_get_relative_path(start, root),
                    issue_type=IssueType.PERMISSION_DENIED,
                    severity=Severity.ERROR,
                    message="Permission denied",
                    suggestion="Check directory permissions or run with appropriate access",
                )
            )
            return

        dirs_to_process = []

        for entry in entries:
            name = entry.name

            # Skip hidden if not included
            if not options.include_hidden and _is_hidden(name):
                continue

            path = Path(entry.path)

            try:
                is_symlink = entry.is_symlink()
                is_dir = entry.is_dir(follow_symlinks=options.follow_symlinks)
                is_file = entry.is_file(follow_symlinks=options.follow_symlinks)
            except OSError:
                continue

            # Handle symlinks
            if is_symlink and not options.follow_symlinks:
                continue

            # Check for symlink loops when following (only for directories)
            if is_symlink and options.follow_symlinks:
                if _check_symlink_loop(ctx, path, is_directory=is_dir):
                    continue

                # Check for broken symlinks (target doesn't exist)
                if _check_broken_symlink(ctx, path, is_symlink, is_file, is_dir):
                    continue

            if is_dir:
                # Check if directory is a FileGDB - treat as single asset, don't recurse
                if is_filegdb(path):
                    # Yield FileGDB directory as a single file with total size
                    size = _get_dir_size(path)
                    yield (path, size)
                else:
                    # Queue regular directory for later processing
                    dirs_to_process.append(path)
            elif is_file:
                try:
                    size = entry.stat(follow_symlinks=options.follow_symlinks).st_size
                    yield (path, size)
                except OSError as e:
                    # Emit warning for stat failures (e.g., race conditions, permission issues)
                    ctx.issues.append(
                        ScanIssue(
                            path=path,
                            relative_path=_get_relative_path(path, root),
                            issue_type=IssueType.PERMISSION_DENIED,
                            severity=Severity.WARNING,
                            message=f"Cannot read file: {e}",
                            suggestion="Check file permissions or if file still exists",
                        )
                    )
                    continue

        # Process subdirectories (if recursive)
        if options.recursive:
            for subdir in dirs_to_process:
                yield from _walk_with_depth(subdir, current_depth + 1)

    yield from _walk_with_depth(root)


def _process_file(ctx: _ScanContext, path: Path, size: int) -> None:
    """Process a single discovered file or FileGDB directory."""
    # Check for FileGDB directory FIRST - these are yielded by _discover_files
    # as directories to be treated as single assets
    # Issue #154: FileGDBs should be added to ready list (not special_formats)
    # so they can be processed by `portolan add`
    if path.is_dir() and is_filegdb(path):
        # Gather FileGDB-specific metadata
        metadata = _gather_filegdb_metadata(path)

        # Compute relative path and inferred collection ID
        relative_path = _get_relative_path(path, ctx.root)
        inferred_collection_id = _infer_collection_id_from_relative_path(relative_path)

        # Create ScannedFile for FileGDB directory
        # FileGDB is convertible to GeoParquet
        scanned = ScannedFile(
            path=path,
            relative_path=relative_path,
            extension=".gdb",
            format_type=FormatType.VECTOR,
            size_bytes=size,
            inferred_collection_id=inferred_collection_id,
            format_status=CloudNativeStatus.CONVERTIBLE,
            format_display_name="FileGDB",
            metadata=metadata,
        )
        ctx.ready.append(scanned)

        # Track for duplicate/multi-asset detection
        ctx.basenames[path.name.lower()].append(path)
        ctx.primaries_by_dir[path.parent].append(path)
        ctx.formats_by_dir[path.parent].add(FormatType.VECTOR)
        return

    ext = path.suffix.lower()
    name = path.name
    parent = path.parent

    # Check for zero-byte file first
    if _check_zero_byte_file(ctx, path, size):
        return

    # Check for long path
    _check_long_path(ctx, path)

    # Check for invalid characters
    _check_invalid_characters(ctx, path)

    # Check for Windows reserved names (cross-platform compatibility)
    _check_windows_reserved(ctx, path)

    # Handle shapefile sidecars
    if _is_sidecar_extension(ext):
        # Track sidecar for later shapefile completeness check
        stem = path.stem
        key = f"{parent}/{stem}"
        ctx.shapefile_sidecars[key].add(ext)
        ctx.skipped.append(_make_skipped_file(ctx, path, size))
        return

    # Handle overview/derivative formats - skip, not primary assets
    # (OVERVIEW_EXTENSIONS is currently empty; retained for future extension points)
    if _is_overview_extension(ext):
        ctx.skipped.append(_make_skipped_file(ctx, path, size))
        return

    # Handle .parquet specially - must check if it's GeoParquet
    if ext == PARQUET_EXTENSION:
        # First check if it's a valid Parquet file at all (not corrupted)
        if not is_valid_parquet(path):
            # File has .parquet extension but is corrupted or not a valid Parquet
            ctx.skipped.append(
                SkippedFile(
                    path=path,
                    relative_path=_get_relative_path(path, ctx.root),
                    category=FileCategory.UNKNOWN,
                    reason_type=SkipReasonType.INVALID_FORMAT,
                    reason_message="File has .parquet extension but is not a valid Parquet file (corrupted or wrong format)",
                )
            )
            return
        if not is_geoparquet(path):
            # Regular Parquet (tabular data), not a geospatial asset
            # Create SkippedFile directly since classify_file can't detect non-geo parquet
            ctx.skipped.append(
                SkippedFile(
                    path=path,
                    relative_path=_get_relative_path(path, ctx.root),
                    category=FileCategory.TABULAR_DATA,
                    reason_type=SkipReasonType.NOT_GEOSPATIAL,
                    reason_message="Parquet file without GeoParquet metadata (tabular data)",
                )
            )
            return
        # It's GeoParquet - treat as vector format
        format_type = FormatType.VECTOR
    elif ext == ".json":
        # Handle .json files - need content inspection for GeoJSON detection
        # Issue #256: GeoJSON files are often saved with .json extension
        #
        # But first, check for STAC metadata files (catalog.json, collection.json, etc.)
        # These should be skipped as metadata, not inspected for GeoJSON content.
        if name in STAC_FILENAMES:
            # STAC metadata file - skip with proper classification
            ctx.skipped.append(
                SkippedFile(
                    path=path,
                    relative_path=_get_relative_path(path, ctx.root),
                    category=FileCategory.STAC_METADATA,
                    reason_type=SkipReasonType.METADATA_FILE,
                    reason_message=f"{name} is STAC catalog metadata",
                )
            )
            return
        # Check for STAC item files: JSON files named after their parent directory
        # Pattern: item_dir/item_dir.json (e.g., tile_0_0/tile_0_0.json)
        # This is the standard Portolan item structure per ADR-0031
        if path.stem.lower() == path.parent.name.lower():
            ctx.skipped.append(
                SkippedFile(
                    path=path,
                    relative_path=_get_relative_path(path, ctx.root),
                    category=FileCategory.STAC_METADATA,
                    reason_type=SkipReasonType.METADATA_FILE,
                    reason_message=f"{path.name} is a STAC item metadata file",
                )
            )
            return
        if _detect_json_type(path) != FormatType.VECTOR:
            # Plain JSON, not GeoJSON - skip with informative message
            # We override classify_file here because we have specific knowledge:
            # we inspected the content and determined it's not GeoJSON.
            ctx.skipped.append(
                SkippedFile(
                    path=path,
                    relative_path=_get_relative_path(path, ctx.root),
                    category=FileCategory.UNKNOWN,
                    reason_type=SkipReasonType.NOT_GEOSPATIAL,
                    reason_message="JSON file does not contain GeoJSON content",
                )
            )
            return
        # It's GeoJSON in a .json file - treat as vector format
        format_type = FormatType.VECTOR
    elif _is_recognized_extension(ext):
        # Get format type for other recognized extensions
        format_type = _get_format_type(ext)
    else:
        # Unrecognized extension
        ctx.skipped.append(_make_skipped_file(ctx, path, size))
        return

    # Compute relative path and inferred collection ID
    relative_path = _get_relative_path(path, ctx.root)
    inferred_collection_id = _infer_collection_id_from_relative_path(relative_path)

    # Get format info for cloud-native status and display name
    format_info = _get_format_info(path, ext)

    # Create scanned file
    scanned = ScannedFile(
        path=path,
        relative_path=relative_path,
        extension=ext,
        format_type=format_type,
        size_bytes=size,
        inferred_collection_id=inferred_collection_id,
        format_status=format_info.status,
        format_display_name=format_info.display_name,
    )
    ctx.ready.append(scanned)

    # Track for duplicate/multi-asset detection
    ctx.basenames[name.lower()].append(path)
    ctx.primaries_by_dir[parent].append(path)
    ctx.formats_by_dir[parent].add(format_type)

    # Track shapefile primary for completeness check
    if ext == ".shp":
        stem = path.stem
        key = f"{parent}/{stem}"
        # Ensure key exists for later checking
        if key not in ctx.shapefile_sidecars:
            ctx.shapefile_sidecars[key] = set()


def scan_directory(
    path: Path,
    options: ScanOptions | None = None,
    progress_callback: Callable[[], None] | None = None,
) -> ScanResult:
    """Scan a directory for geospatial files and issues.

    This is the primary entry point for the scan module.

    Args:
        path: Directory path to scan.
        options: Scan configuration options. Defaults to ScanOptions().
        progress_callback: Optional callback called for each directory scanned.
            Used for progress reporting. Called with no arguments.

    Returns:
        ScanResult containing ready files, issues, and skipped files.

    Raises:
        FileNotFoundError: If path does not exist.
        NotADirectoryError: If path is not a directory.

    Example:
        >>> from portolan_cli.scan import scan_directory, ScanOptions
        >>> result = scan_directory(Path("/data/geospatial"))
        >>> print(f"Found {len(result.ready)} files ready to import")
        >>> if result.has_errors:
        ...     print(f"Found {result.error_count} errors")
    """
    # Validate path
    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {path}")
    if not path.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {path}")

    # Use default options if not provided
    if options is None:
        options = ScanOptions()

    # Create scan context
    ctx = _ScanContext(root=path, options=options, progress_callback=progress_callback)

    # Discover and process files
    for file_path, file_size in _discover_files(ctx):
        _process_file(ctx, file_path, file_size)

    # Check incomplete shapefiles
    for scanned in ctx.ready:
        if scanned.extension == ".shp":
            _check_incomplete_shapefile(ctx, scanned.path)

    # Check for orphan sidecars (sidecars without a .shp file)
    _check_orphan_sidecars(ctx)

    # Check for invalid collection IDs
    _check_collection_ids(ctx)

    # Check for mixed flat/multi-item structure
    _check_mixed_structure(ctx)

    # Check for multiple GeoParquet files in same directory
    _check_multiple_geo_primaries(ctx)

    # Run multi-asset checks
    _finalize_multi_asset_checks(ctx)

    return ScanResult(
        root=path,
        ready=ctx.ready,
        issues=ctx.issues,
        skipped=ctx.skipped,
        directories_scanned=ctx.directories_scanned,
        special_formats=ctx.special_formats,
    )
