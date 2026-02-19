"""Check command logic for validating and fixing geospatial files.

This module provides the check command functionality:
- Identifying files that need conversion to cloud-native formats
- Converting files with --fix flag
- Dry-run mode for previewing changes

Per ADR-0007, this module contains the logic; CLI commands are thin wrappers.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from portolan_cli.convert import (
    GEOSPATIAL_EXTENSIONS,
    ConversionReport,
    ConversionResult,
    convert_directory,
)
from portolan_cli.formats import CloudNativeStatus, get_cloud_native_status
from portolan_cli.scan import PARQUET_EXTENSION, is_geoparquet

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
class CheckReport:
    """Report from checking a directory for cloud-native status.

    Attributes:
        root: Directory that was checked.
        files: List of FileStatus for each file found.
        conversion_report: Results from --fix conversion (None if not run).
    """

    root: Path
    files: list[FileStatus]
    conversion_report: ConversionReport | None = None

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
        return result


def check_directory(
    path: Path,
    *,
    fix: bool = False,
    dry_run: bool = False,
    on_progress: Callable[[ConversionResult], None] | None = None,
) -> CheckReport:
    """Check a directory for cloud-native status and optionally fix.

    Scans the directory for geospatial files and reports their cloud-native
    status. With --fix, converts CONVERTIBLE files to cloud-native formats.

    Args:
        path: Directory to check.
        fix: If True, convert convertible files to cloud-native formats.
        dry_run: If True, preview what would be converted without changes.
        on_progress: Optional callback for conversion progress (--fix mode).

    Returns:
        CheckReport with file statuses and conversion results (if fix=True).

    Raises:
        FileNotFoundError: If the directory does not exist.
        NotADirectoryError: If the path is not a directory.
    """
    if not path.exists():
        raise FileNotFoundError(f"Directory not found: {path}")

    if not path.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {path}")

    # Scan for geospatial files
    files = _scan_for_files(path)

    # Get cloud-native status for each file
    file_statuses = []
    for file_path in files:
        relative = _get_relative_path(file_path, path)
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
        # Pass the already-discovered file list to avoid re-scanning
        # This ensures ConversionReport aligns with file_statuses
        conversion_report = convert_directory(
            path,
            on_progress=on_progress,
            file_paths=[f.path for f in file_statuses],
        )
        report.conversion_report = conversion_report
    elif fix and dry_run:
        # Preview mode - create empty conversion report
        # The caller should show what would be converted based on file_statuses
        report.conversion_report = ConversionReport(results=[])

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
    """Get path relative to root as string."""
    try:
        return str(file_path.relative_to(root))
    except ValueError:
        return str(file_path)
