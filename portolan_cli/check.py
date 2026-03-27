"""Check command logic for validating and fixing geospatial files.

This module provides the check command functionality:
- Identifying files that need conversion to cloud-native formats
- Converting files with --fix flag
- Dry-run mode for previewing changes
- Removing legacy files after successful conversion (--remove-legacy)

Per ADR-0007, this module contains the logic; CLI commands are thin wrappers.

See Also:
    - GitHub Issue #209: Add --remove-legacy flag to check --fix
"""

from __future__ import annotations

import logging
import shutil
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from portolan_cli.constants import GEOSPATIAL_EXTENSIONS, PARQUET_EXTENSION, SIDECAR_PATTERNS
from portolan_cli.conversion_config import ConversionOverrides, get_conversion_overrides
from portolan_cli.convert import (
    ConversionReport,
    ConversionResult,
    ConversionStatus,
    convert_directory,
)
from portolan_cli.formats import (
    CloudNativeStatus,
    get_cloud_native_status,
    get_effective_status,
    is_geoparquet,
)
from portolan_cli.scan_detect import is_filegdb

logger = logging.getLogger(__name__)

# Extensions to check for cloud-native status
CHECK_EXTENSIONS: frozenset[str] = GEOSPATIAL_EXTENSIONS | frozenset({PARQUET_EXTENSION})


@dataclass
class FileStatus:
    """Status of a single file for check command.

    Attributes:
        path: Absolute path to the file.
        relative_path: Path relative to the check root.
        status: Cloud-native status (CLOUD_NATIVE, CONVERTIBLE, UNSUPPORTED).
        display_name: Human-readable format name.
        target_format: Target format if convertible, else None.
    """

    path: Path
    relative_path: str
    status: CloudNativeStatus
    display_name: str
    target_format: str | None


@dataclass
class LegacyRemovalReport:
    """Report from removing legacy files after conversion.

    Attributes:
        removed: List of paths that were successfully removed.
        errors: Dict mapping paths to error messages for failed removals.

    See Also:
        GitHub Issue #209: Add --remove-legacy flag to check --fix
    """

    removed: list[Path] = field(default_factory=list)
    errors: dict[Path, str] = field(default_factory=dict)

    @property
    def success_count(self) -> int:
        """Number of files successfully removed."""
        return len(self.removed)

    @property
    def error_count(self) -> int:
        """Number of files that failed to be removed."""
        return len(self.errors)

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dictionary."""
        return {
            "removed": [str(p) for p in self.removed],
            "errors": {str(p): msg for p, msg in self.errors.items()},
            "summary": {
                "removed_count": self.success_count,
                "error_count": self.error_count,
            },
        }


@dataclass
class CheckReport:
    """Report from checking a directory for cloud-native status.

    Attributes:
        root: Directory that was checked.
        files: List of FileStatus for each file found.
        conversion_report: Results from --fix conversion (None if not run).
        legacy_removal_report: Results from --remove-legacy (None if not run).
    """

    root: Path
    files: list[FileStatus]
    conversion_report: ConversionReport | None = None
    legacy_removal_report: LegacyRemovalReport | None = None

    @property
    def cloud_native_count(self) -> int:
        """Number of files already cloud-native."""
        return sum(1 for f in self.files if f.status == CloudNativeStatus.CLOUD_NATIVE)

    @property
    def convertible_count(self) -> int:
        """Number of files that can be converted."""
        return sum(1 for f in self.files if f.status == CloudNativeStatus.CONVERTIBLE)

    @property
    def unsupported_count(self) -> int:
        """Number of unsupported files."""
        return sum(1 for f in self.files if f.status == CloudNativeStatus.UNSUPPORTED)

    @property
    def total(self) -> int:
        """Total number of files checked."""
        return len(self.files)

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dictionary."""
        result: dict[str, Any] = {
            "root": str(self.root),
            "summary": {
                "total": self.total,
                "cloud_native": self.cloud_native_count,
                "convertible": self.convertible_count,
                "unsupported": self.unsupported_count,
            },
            "files": [
                {
                    "path": str(f.path),
                    "relative_path": f.relative_path,
                    "status": f.status.value,
                    "format": f.display_name,
                    "target_format": f.target_format,
                }
                for f in self.files
            ],
        }
        if self.conversion_report is not None:
            result["conversion"] = self.conversion_report.to_dict()
        if self.legacy_removal_report is not None:
            result["legacy_removed"] = self.legacy_removal_report.to_dict()
        return result


# =============================================================================
# Legacy File Removal Functions (Issue #209)
# =============================================================================


def get_legacy_files_to_remove(report: ConversionReport) -> list[Path]:
    """Identify legacy source files that can be safely removed after conversion.

    Only returns source files from successful conversions where the output
    file exists. This ensures we never delete source files unless the
    conversion actually produced a valid output.

    Args:
        report: ConversionReport from a completed conversion run.

    Returns:
        List of source file paths that can be safely removed.

    Note:
        This function does NOT return sidecar files - those are handled by
        remove_legacy_files() which uses get_sidecars() for each source.

    See Also:
        GitHub Issue #209: Add --remove-legacy flag to check --fix
    """
    files_to_remove: list[Path] = []

    for result in report.results:
        # Only include successful conversions
        if result.status != ConversionStatus.SUCCESS:
            continue

        # Safety: verify output actually exists
        if result.output is None or not result.output.exists():
            logger.warning("Skipping legacy removal for %s: output file missing", result.source)
            continue

        files_to_remove.append(result.source)

    return files_to_remove


def get_sidecars_for_file(path: Path) -> list[Path]:
    """Get sidecar files for a given primary file.

    Uses SIDECAR_PATTERNS to find associated files (e.g., .dbf/.shx for shapefiles).

    Args:
        path: Path to the primary file.

    Returns:
        List of existing sidecar file paths.
    """
    suffix_lower = path.suffix.lower()
    patterns = SIDECAR_PATTERNS.get(suffix_lower, [])

    sidecars: list[Path] = []
    stem = path.stem
    parent = path.parent

    for ext in patterns:
        sidecar_path = parent / f"{stem}{ext}"
        if sidecar_path.exists():
            sidecars.append(sidecar_path)

    return sidecars


def remove_legacy_files(files: list[Path]) -> tuple[list[Path], dict[Path, str]]:
    """Remove legacy source files and their sidecars.

    Handles:
    - Single files (GeoJSON, etc.)
    - Shapefiles with sidecars (.dbf, .shx, .prj, .cpg, etc.)
    - FileGDB directories (.gdb)

    Args:
        files: List of primary file paths to remove.

    Returns:
        Tuple of:
        - List of successfully removed primary files
        - Dict mapping failed paths to error messages

    Note:
        This function is idempotent - missing files are silently skipped.
        Sidecar removal failures are logged but don't prevent primary removal.

    See Also:
        GitHub Issue #209: Add --remove-legacy flag to check --fix
    """
    removed: list[Path] = []
    errors: dict[Path, str] = {}

    for file_path in files:
        try:
            # Check if file exists (idempotent - skip if already gone)
            if not file_path.exists():
                logger.debug("File already removed, skipping: %s", file_path)
                continue

            # Handle FileGDB directories
            if file_path.is_dir() and is_filegdb(file_path):
                shutil.rmtree(file_path)
                logger.info("Removed FileGDB directory: %s", file_path)
                removed.append(file_path)
                continue

            # Get and remove sidecars first
            sidecars = get_sidecars_for_file(file_path)
            for sidecar in sidecars:
                try:
                    sidecar.unlink(missing_ok=True)
                    logger.debug("Removed sidecar: %s", sidecar)
                except OSError as e:
                    # Log but continue - don't fail primary removal for sidecar issues
                    logger.warning("Failed to remove sidecar %s: %s", sidecar, e)

            # Remove primary file
            file_path.unlink()
            logger.info("Removed legacy file: %s", file_path)
            removed.append(file_path)

        except PermissionError as e:
            error_msg = f"Permission denied: {e}"
            logger.error("Failed to remove %s: %s", file_path, error_msg)
            errors[file_path] = error_msg
        except OSError as e:
            error_msg = f"OS error: {e}"
            logger.error("Failed to remove %s: %s", file_path, error_msg)
            errors[file_path] = error_msg

    return removed, errors


def check_directory(
    path: Path,
    *,
    fix: bool = False,
    dry_run: bool = False,
    remove_legacy: bool = False,
    on_progress: Callable[[ConversionResult], None] | None = None,
    catalog_path: Path | None = None,
) -> CheckReport:
    """Check a directory for cloud-native status and optionally fix.

    Scans the directory for geospatial files and reports their cloud-native
    status. With --fix, converts CONVERTIBLE files to cloud-native formats.

    Respects conversion config from .portolan/config.yaml if catalog_path is
    provided. This allows:
    - Force-converting cloud-native formats (e.g., FlatGeobuf -> GeoParquet)
    - Preserving convertible formats (e.g., keeping Shapefiles as-is)
    - Path-based overrides (e.g., preserving everything in archive/)

    Args:
        path: Directory to check.
        fix: If True, convert convertible files to cloud-native formats.
        dry_run: If True, preview what would be converted without changes.
        remove_legacy: If True, delete source files after successful conversion.
            Requires fix=True. Handles sidecars (.dbf, .shx, etc.) and
            FileGDB directories (.gdb). Only removes files converted in THIS run.
        on_progress: Optional callback for conversion progress (--fix mode).
        catalog_path: Optional catalog root for loading conversion config.
            If provided, loads conversion overrides from .portolan/config.yaml.

    Returns:
        CheckReport with file statuses, conversion results, and removal results.

    Raises:
        FileNotFoundError: If the directory does not exist.
        NotADirectoryError: If the path is not a directory.
        ValueError: If remove_legacy=True but fix=False.

    See Also:
        - GitHub Issue #75: FlatGeobuf cloud-native status
        - GitHub Issue #103: Config for non-cloud-native file handling
        - GitHub Issue #209: Add --remove-legacy flag to check --fix
    """
    # Validate parameter combinations
    if remove_legacy and not fix:
        raise ValueError("remove_legacy requires fix=True")

    if not path.exists():
        raise FileNotFoundError(f"Directory not found: {path}")

    if not path.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {path}")

    # Load conversion overrides from config (if catalog_path provided)
    overrides: ConversionOverrides | None = None
    if catalog_path is not None:
        overrides = get_conversion_overrides(catalog_path)

    # Scan for geospatial files
    files = _scan_for_files(path)

    # Get cloud-native status for each file (with overrides applied)
    file_statuses = []
    for file_path in files:
        relative = _get_relative_path(file_path, path)
        if overrides is not None:
            status_info = get_effective_status(file_path, overrides=overrides, root=catalog_path)
        else:
            status_info = get_cloud_native_status(file_path)
        file_statuses.append(
            FileStatus(
                path=file_path,
                relative_path=relative,
                status=status_info.status,
                display_name=status_info.display_name,
                target_format=status_info.target_format,
            )
        )

    report = CheckReport(root=path, files=file_statuses)

    # Handle fix mode
    if fix and not dry_run:
        # Only convert files that are CONVERTIBLE (not cloud-native, not unsupported)
        convertible_files = [
            f.path for f in file_statuses if f.status == CloudNativeStatus.CONVERTIBLE
        ]
        conversion_report = convert_directory(
            path,
            on_progress=on_progress,
            file_paths=convertible_files,
            catalog_path=catalog_path,
        )
        report.conversion_report = conversion_report

        # Handle legacy file removal (only after actual conversions, not dry run)
        if remove_legacy and conversion_report is not None:
            files_to_remove = get_legacy_files_to_remove(conversion_report)
            if files_to_remove:
                removed, errors = remove_legacy_files(files_to_remove)
                report.legacy_removal_report = LegacyRemovalReport(
                    removed=removed,
                    errors=errors,
                )

    elif fix and dry_run:
        # Preview mode - create results showing what would be converted
        preview_results = [
            ConversionResult(
                source=f.path,
                output=f.path.parent / f"{f.path.stem}.parquet"
                if f.target_format == "GeoParquet"
                else f.path.parent / f"{f.path.stem}.tif",
                format_from=f.display_name,
                format_to=f.target_format,
                status=ConversionStatus.SUCCESS,  # Predicted outcome
                error=None,
                duration_ms=0,
            )
            for f in file_statuses
            if f.status == CloudNativeStatus.CONVERTIBLE
        ]
        report.conversion_report = ConversionReport(results=preview_results)
        # Note: remove_legacy is ignored in dry_run mode (no actual removal)

    return report


def _scan_for_files(path: Path) -> list[Path]:
    """Scan directory for geospatial files.

    Args:
        path: Directory to scan.

    Returns:
        List of paths to geospatial files, sorted.
    """
    files: list[Path] = []
    for item in path.rglob("*"):
        if not item.is_file():
            continue
        ext = item.suffix.lower()
        if ext in CHECK_EXTENSIONS:
            # For parquet, check if it's GeoParquet
            if ext == PARQUET_EXTENSION:
                if not is_geoparquet(item):
                    continue
            files.append(item)
    files.sort()
    return files


def _get_relative_path(file_path: Path, root: Path) -> str:
    """Get path relative to root as forward-slash string.

    Returns paths with forward slashes regardless of OS for STAC compatibility.
    STAC uses URL-style paths which always use forward slashes.
    """
    try:
        return file_path.relative_to(root).as_posix()
    except ValueError:
        return file_path.as_posix()
