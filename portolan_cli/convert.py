"""Conversion API for transforming files to cloud-native formats.

This module provides the core conversion functionality for Portolan CLI:
- ConversionStatus: Enum of possible conversion outcomes
- ConversionResult: Result of a single file conversion
- ConversionReport: Aggregate results from batch conversion
- convert_file(): Convert a single file to cloud-native format
- convert_directory(): Convert all files in a directory

Per ADR-0007, this module contains the logic; CLI commands are thin wrappers.
Per ADR-0010, actual conversion is delegated to geoparquet-io and rio-cogeo.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from portolan_cli.errors import (
    ConversionFailedError,
    UnsupportedFormatError,
)
from portolan_cli.formats import (
    CloudNativeStatus,
    FormatType,
    detect_format,
    get_cloud_native_status,
)

logger = logging.getLogger(__name__)


class ConversionStatus(Enum):
    """Possible outcomes of a file conversion operation.

    Attributes:
        SUCCESS: File was converted successfully and validated.
        SKIPPED: File was already cloud-native, no conversion needed.
        FAILED: Conversion threw an exception (original file preserved).
        INVALID: Conversion completed but validation failed (output kept for inspection).
    """

    SUCCESS = "success"
    SKIPPED = "skipped"
    FAILED = "failed"
    INVALID = "invalid"


@dataclass
class ConversionResult:
    """Result of a single file conversion operation.

    Attributes:
        source: Path to the source file that was converted.
        output: Path to the output file (None if conversion failed or skipped).
        format_from: Display name of the source format (e.g., "SHP", "GeoJSON").
        format_to: Display name of the target format (e.g., "GeoParquet", "COG").
            None if skipped.
        status: Outcome of the conversion operation.
        error: Error message if conversion failed or validation failed.
            None on success or skip.
        duration_ms: Time taken for the conversion in milliseconds.
    """

    source: Path
    output: Path | None
    format_from: str
    format_to: str | None
    status: ConversionStatus
    error: str | None
    duration_ms: int

    def to_dict(self) -> dict[str, Any]:
        """Convert to a JSON-serializable dictionary.

        Returns:
            Dictionary with all fields, paths converted to strings,
            and status converted to its string value.
        """
        return {
            "source": str(self.source),
            "output": str(self.output) if self.output else None,
            "format_from": self.format_from,
            "format_to": self.format_to,
            "status": self.status.value,
            "error": self.error,
            "duration_ms": self.duration_ms,
        }


@dataclass
class ConversionReport:
    """Aggregate results from batch conversion operations.

    Attributes:
        results: List of ConversionResult from processing each file.
    """

    results: list[ConversionResult]

    @property
    def succeeded(self) -> int:
        """Count of files successfully converted."""
        return sum(1 for r in self.results if r.status == ConversionStatus.SUCCESS)

    @property
    def failed(self) -> int:
        """Count of files that failed conversion."""
        return sum(1 for r in self.results if r.status == ConversionStatus.FAILED)

    @property
    def skipped(self) -> int:
        """Count of files skipped (already cloud-native)."""
        return sum(1 for r in self.results if r.status == ConversionStatus.SKIPPED)

    @property
    def invalid(self) -> int:
        """Count of files that converted but failed validation."""
        return sum(1 for r in self.results if r.status == ConversionStatus.INVALID)

    @property
    def total(self) -> int:
        """Total number of files processed."""
        return len(self.results)

    def to_dict(self) -> dict[str, Any]:
        """Convert to a JSON-serializable dictionary.

        Returns:
            Dictionary with summary counts and full results array.
        """
        return {
            "summary": {
                "succeeded": self.succeeded,
                "failed": self.failed,
                "skipped": self.skipped,
                "invalid": self.invalid,
                "total": self.total,
            },
            "results": [r.to_dict() for r in self.results],
        }


def convert_file(
    source: Path,
    output_dir: Path | None = None,
) -> ConversionResult:
    """Convert a single file to cloud-native format.

    Converts vector files to GeoParquet and raster files to COG.
    Files that are already cloud-native are skipped.

    Args:
        source: Path to the source file to convert.
        output_dir: Directory for the output file. If None, uses the same
            directory as the source file.

    Returns:
        ConversionResult with conversion outcome, timing, and paths.

    Raises:
        FileNotFoundError: If the source file does not exist.
    """
    start_time = time.perf_counter()

    if not source.exists():
        raise FileNotFoundError(f"Source file not found: {source}")

    # Get format info
    format_info = get_cloud_native_status(source)

    # Skip if already cloud-native
    if format_info.status == CloudNativeStatus.CLOUD_NATIVE:
        duration_ms = int((time.perf_counter() - start_time) * 1000)
        return ConversionResult(
            source=source,
            output=None,
            format_from=format_info.display_name,
            format_to=None,
            status=ConversionStatus.SKIPPED,
            error=None,
            duration_ms=duration_ms,
        )

    # Handle unsupported formats
    if format_info.status == CloudNativeStatus.UNSUPPORTED:
        duration_ms = int((time.perf_counter() - start_time) * 1000)
        # Use structured error for logging context
        format_error = UnsupportedFormatError(str(source), format_info.display_name)
        logger.warning(
            "Unsupported format: %s [%s]",
            source,
            format_error.code,
        )
        return ConversionResult(
            source=source,
            output=None,
            format_from=format_info.display_name,
            format_to=None,
            status=ConversionStatus.FAILED,
            error=format_info.error_message
            or f"Format {format_info.display_name} is not supported",
            duration_ms=duration_ms,
        )

    # Determine output directory and format type
    out_dir = output_dir if output_dir else source.parent
    format_type = detect_format(source)

    # Convert based on format type
    try:
        if format_type == FormatType.VECTOR:
            output_path = _convert_vector(source, out_dir)
            target_format = "GeoParquet"
        elif format_type == FormatType.RASTER:
            output_path = _convert_raster(source, out_dir)
            target_format = "COG"
        else:
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            return ConversionResult(
                source=source,
                output=None,
                format_from=format_info.display_name,
                format_to=None,
                status=ConversionStatus.FAILED,
                error=f"Unable to determine format type for {source.suffix}",
                duration_ms=duration_ms,
            )

        duration_ms = int((time.perf_counter() - start_time) * 1000)
        return ConversionResult(
            source=source,
            output=output_path,
            format_from=format_info.display_name,
            format_to=target_format,
            status=ConversionStatus.SUCCESS,
            error=None,
            duration_ms=duration_ms,
        )

    except Exception as e:
        duration_ms = int((time.perf_counter() - start_time) * 1000)
        # Use structured error for logging context
        conversion_error = ConversionFailedError(str(source), e)
        logger.exception(
            "Conversion failed for %s [%s]: %s",
            source,
            conversion_error.code,
            e,
        )
        return ConversionResult(
            source=source,
            output=None,
            format_from=format_info.display_name,
            format_to=format_info.target_format,
            status=ConversionStatus.FAILED,
            error=str(e),
            duration_ms=duration_ms,
        )


def _convert_vector(source: Path, output_dir: Path) -> Path:
    """Convert a vector file to GeoParquet.

    Args:
        source: Source vector file.
        output_dir: Directory for output file.

    Returns:
        Path to the output GeoParquet file.
    """
    import geoparquet_io as gpio  # type: ignore[import-untyped]

    output_path = output_dir / f"{source.stem}.parquet"

    # Use geoparquet-io fluent API for conversion
    gpio.convert(str(source)).write(str(output_path))

    return output_path


def _convert_raster(source: Path, output_dir: Path) -> Path:
    """Convert a raster file to COG.

    Uses COG defaults from the convert command design:
    - DEFLATE compression
    - Predictor=2 (horizontal differencing)
    - 512x512 tiles
    - Nearest resampling

    Args:
        source: Source raster file.
        output_dir: Directory for output file.

    Returns:
        Path to the output COG file.
    """
    import tempfile

    from rio_cogeo.cogeo import cog_translate
    from rio_cogeo.profiles import cog_profiles

    output_path = output_dir / f"{source.stem}.tif"

    # Use DEFLATE profile with our opinionated defaults
    profile = cog_profiles.get("deflate")  # type: ignore[no-untyped-call]

    # Set predictor=2 for horizontal differencing compression
    profile["predictor"] = 2

    # Write to temp file first to avoid corrupting source if output_path == source
    # Use same directory as output for atomic rename across filesystems
    temp_fd, temp_path_str = tempfile.mkstemp(
        suffix=".tif", prefix=".portolan_cog_", dir=output_dir
    )
    temp_path = Path(temp_path_str)

    try:
        # Close the file descriptor - rio-cogeo will open it
        import os

        os.close(temp_fd)

        cog_translate(
            str(source),
            str(temp_path),
            profile,
            quiet=True,
            overview_resampling="nearest",
        )

        # Atomic replace
        temp_path.replace(output_path)
    except Exception:
        # Clean up temp file on error
        if temp_path.exists():
            temp_path.unlink()
        raise

    return output_path


# Extensions we recognize as geospatial for batch conversion
GEOSPATIAL_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".geojson",
        ".parquet",
        ".shp",
        ".gpkg",
        ".fgb",
        ".csv",
        ".tif",
        ".tiff",
        ".jp2",
        ".pmtiles",
    }
)


def convert_directory(
    path: Path,
    output_dir: Path | None = None,
    on_progress: Callable[[ConversionResult], None] | None = None,
    recursive: bool = True,
    file_paths: list[Path] | None = None,
) -> ConversionReport:
    """Convert all geospatial files in a directory to cloud-native formats.

    Iterates through the directory, converts each geospatial file to its
    cloud-native equivalent (GeoParquet or COG), and returns an aggregate report.

    Args:
        path: Directory containing files to convert.
        output_dir: Directory for output files. If None, outputs are placed
            in the same directory as each source file.
        on_progress: Optional callback invoked after each file is processed.
            Receives the ConversionResult for streaming progress updates.
        recursive: If True (default), process subdirectories recursively.
        file_paths: Optional list of specific files to convert. If provided,
            skips directory scanning and converts only these files. Useful
            when the caller has already scanned and filtered the files.

    Returns:
        ConversionReport with results for all processed files.

    Raises:
        FileNotFoundError: If the directory does not exist.
        NotADirectoryError: If the path is a file, not a directory.
    """
    if not path.exists():
        raise FileNotFoundError(f"Directory not found: {path}")

    if not path.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {path}")

    # Use provided file list or scan for geospatial files
    if file_paths is not None:
        files = sorted(file_paths)
    else:
        files = []
        if recursive:
            for item in path.rglob("*"):
                if item.is_file() and item.suffix.lower() in GEOSPATIAL_EXTENSIONS:
                    files.append(item)
        else:
            for item in path.iterdir():
                if item.is_file() and item.suffix.lower() in GEOSPATIAL_EXTENSIONS:
                    files.append(item)
        files.sort()

    # Process each file
    results: list[ConversionResult] = []
    for file_path in files:
        # Determine output directory for this file
        file_output_dir = output_dir if output_dir else file_path.parent

        result = convert_file(file_path, output_dir=file_output_dir)
        results.append(result)

        # Invoke callback if provided
        if on_progress is not None:
            on_progress(result)

    return ConversionReport(results=results)
