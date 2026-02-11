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
- Issue detection (8 types with 3 severity levels)
- Human-readable and JSON output
"""

from __future__ import annotations

import os
import re
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

# =============================================================================
# Constants
# =============================================================================

# Recognized geospatial file extensions (primary assets)
RECOGNIZED_VECTOR_EXTENSIONS: frozenset[str] = frozenset(
    {".parquet", ".geojson", ".shp", ".gpkg", ".fgb"}
)

RECOGNIZED_RASTER_EXTENSIONS: frozenset[str] = frozenset({".tif", ".tiff", ".jp2"})

RECOGNIZED_EXTENSIONS: frozenset[str] = RECOGNIZED_VECTOR_EXTENSIONS | RECOGNIZED_RASTER_EXTENSIONS

# Shapefile sidecar extensions
SHAPEFILE_REQUIRED_SIDECARS: frozenset[str] = frozenset({".dbf", ".shx"})
SHAPEFILE_OPTIONAL_SIDECARS: frozenset[str] = frozenset({".prj", ".cpg", ".sbn", ".sbx"})
SHAPEFILE_ALL_SIDECARS: frozenset[str] = SHAPEFILE_REQUIRED_SIDECARS | SHAPEFILE_OPTIONAL_SIDECARS

# Path length threshold for warnings
LONG_PATH_THRESHOLD: int = 200

# Pattern for invalid filename characters
# Matches: spaces, parentheses, brackets, and non-ASCII characters
INVALID_CHAR_PATTERN: re.Pattern[str] = re.compile(r"[\s()\[\]]|[^\x00-\x7F]")


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

    INCOMPLETE_SHAPEFILE = "incomplete_shapefile"
    ZERO_BYTE_FILE = "zero_byte_file"
    SYMLINK_LOOP = "symlink_loop"
    INVALID_CHARACTERS = "invalid_characters"
    MULTIPLE_PRIMARIES = "multiple_primaries"
    LONG_PATH = "long_path"
    DUPLICATE_BASENAME = "duplicate_basename"
    MIXED_FORMATS = "mixed_formats"


class FormatType(Enum):
    """Type of geospatial format."""

    VECTOR = "vector"
    RASTER = "raster"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass(frozen=True)
class ScanOptions:
    """Configuration options for scan operation."""

    recursive: bool = True
    max_depth: int | None = None
    include_hidden: bool = False
    follow_symlinks: bool = False


@dataclass(frozen=True)
class ScannedFile:
    """A geospatial file ready for import."""

    path: Path
    relative_path: str
    extension: str
    format_type: FormatType
    size_bytes: int

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
    skipped: list[Path]
    directories_scanned: int

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

    def to_dict(self) -> dict[str, object]:
        """Convert to JSON-serializable dictionary."""
        return {
            "root": str(self.root),
            "summary": {
                "directories_scanned": self.directories_scanned,
                "ready_count": len(self.ready),
                "issue_count": len(self.issues),
                "skipped_count": len(self.skipped),
            },
            "ready": [
                {
                    "path": str(f.path),
                    "relative_path": f.relative_path,
                    "extension": f.extension,
                    "format_type": f.format_type.value,
                    "size_bytes": f.size_bytes,
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
                {
                    "path": str(p),
                    "relative_path": str(p.relative_to(self.root))
                    if self._is_relative_to(p, self.root)
                    else str(p),
                    "reason": "unsupported_format",
                }
                for p in self.skipped
            ],
        }

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
    skipped: list[Path] = field(default_factory=list)
    directories_scanned: int = 0
    # For detecting duplicates and multiple primaries
    basenames: dict[str, list[Path]] = field(default_factory=lambda: defaultdict(list))
    primaries_by_dir: dict[Path, list[Path]] = field(default_factory=lambda: defaultdict(list))
    formats_by_dir: dict[Path, set[FormatType]] = field(default_factory=lambda: defaultdict(set))
    # Track shapefile sidecars
    shapefile_sidecars: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))


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
    """Get path relative to root as string."""
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


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


def _check_symlink_loop(ctx: _ScanContext, path: Path) -> bool:
    """Check for symlink loop using inode tracking. Returns True if loop detected."""
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
                    suggestion="Split into separate directories or use --bundle flag during import",
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

    # Check for duplicate basenames
    for _basename, paths in ctx.basenames.items():
        if len(paths) > 1:
            # Check for case-insensitive duplicates
            lower_names = defaultdict(list)
            for p in paths:
                lower_names[p.name.lower()].append(p)

            for _lower_name, matching_paths in lower_names.items():
                if len(matching_paths) > 1:
                    locations = ", ".join(
                        _get_relative_path(p.parent, ctx.root) for p in matching_paths
                    )
                    ctx.issues.append(
                        ScanIssue(
                            path=matching_paths[0],
                            relative_path=_get_relative_path(matching_paths[0], ctx.root),
                            issue_type=IssueType.DUPLICATE_BASENAME,
                            severity=Severity.INFO,
                            message=f"Duplicate basename '{matching_paths[0].name}' found in: {locations}",
                            suggestion="Rename files to have unique names",
                        )
                    )


# =============================================================================
# Core Scan Functions
# =============================================================================


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

        try:
            entries = list(os.scandir(start))
        except PermissionError:
            ctx.issues.append(
                ScanIssue(
                    path=start,
                    relative_path=_get_relative_path(start, root),
                    issue_type=IssueType.SYMLINK_LOOP,  # Reuse for permission issues
                    severity=Severity.ERROR,
                    message="Permission denied",
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

            # Check for symlink loops when following
            if is_symlink and options.follow_symlinks:
                if _check_symlink_loop(ctx, path):
                    continue

            if is_dir:
                # Queue directory for later processing
                dirs_to_process.append(path)
            elif is_file:
                try:
                    size = entry.stat(follow_symlinks=options.follow_symlinks).st_size
                    yield (path, size)
                except OSError:
                    continue

        # Process subdirectories (if recursive)
        if options.recursive:
            for subdir in dirs_to_process:
                yield from _walk_with_depth(subdir, current_depth + 1)

    yield from _walk_with_depth(root)


def _process_file(ctx: _ScanContext, path: Path, size: int) -> None:
    """Process a single discovered file."""
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

    # Handle shapefile sidecars
    if _is_sidecar_extension(ext):
        # Track sidecar for later shapefile completeness check
        stem = path.stem
        key = f"{parent}/{stem}"
        ctx.shapefile_sidecars[key].add(ext)
        ctx.skipped.append(path)
        return

    # Check if recognized extension
    if not _is_recognized_extension(ext):
        ctx.skipped.append(path)
        return

    # Get format type
    format_type = _get_format_type(ext)

    # Create scanned file
    scanned = ScannedFile(
        path=path,
        relative_path=_get_relative_path(path, ctx.root),
        extension=ext,
        format_type=format_type,
        size_bytes=size,
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
) -> ScanResult:
    """Scan a directory for geospatial files and issues.

    This is the primary entry point for the scan module.

    Args:
        path: Directory path to scan.
        options: Scan configuration options. Defaults to ScanOptions().

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
    ctx = _ScanContext(root=path, options=options)

    # Discover and process files
    for file_path, file_size in _discover_files(ctx):
        _process_file(ctx, file_path, file_size)

    # Check incomplete shapefiles
    for scanned in ctx.ready:
        if scanned.extension == ".shp":
            _check_incomplete_shapefile(ctx, scanned.path)

    # Run multi-asset checks
    _finalize_multi_asset_checks(ctx)

    return ScanResult(
        root=path,
        ready=ctx.ready,
        issues=ctx.issues,
        skipped=ctx.skipped,
        directories_scanned=ctx.directories_scanned,
    )
