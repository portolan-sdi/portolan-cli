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
from typing import TYPE_CHECKING, Any

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
    FormatType,
    detect_format,
    get_cloud_native_status,
    get_effective_status,
    is_geoparquet,
)
from portolan_cli.scan_detect import is_filegdb

if TYPE_CHECKING:
    from portolan_cli.metadata.fix import FixReport

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
    force: bool = False,
    workers: int | None = None,
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
        force: If True, also re-optimize already-cloud-native RASTERS (valid COGs)
            by re-applying current COG settings, e.g. to add missing overviews.
            Requires fix=True. Raster-scoped: valid GeoParquet is left untouched
            (issue #530).
        workers: Parallel worker processes for conversion. None/1 runs serially;
            >1 uses a process pool. Threaded to convert_directory.
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

    if force and not fix:
        raise ValueError("force requires fix=True")

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
        # Convert CONVERTIBLE files, plus (when --force) already-cloud-native
        # RASTERS so valid-but-unoptimized COGs get re-encoded with current
        # settings (issue #530). Valid vectors are never force-re-processed.
        files_to_convert = [
            f.path for f in file_statuses if f.status == CloudNativeStatus.CONVERTIBLE
        ]
        if force:
            files_to_convert.extend(_forced_raster_paths(file_statuses))
        conversion_report = convert_directory(
            path,
            on_progress=on_progress,
            file_paths=files_to_convert,
            catalog_path=catalog_path,
            workers=workers,
            force=force,
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
        # Preview mode - show what would be converted (no changes on disk).
        report.conversion_report = ConversionReport(
            results=_build_preview_results(file_statuses, force=force)
        )
        # Note: remove_legacy is ignored in dry_run mode (no actual removal)

    return report


def _forced_raster_paths(file_statuses: list[FileStatus]) -> list[Path]:
    """Paths of already-cloud-native RASTERS eligible for --force re-optimization.

    These are valid COGs that `--fix` would normally skip. Vectors are excluded
    so `--force` only re-encodes rasters (issue #530).
    """
    return [
        f.path
        for f in file_statuses
        if f.status == CloudNativeStatus.CLOUD_NATIVE and detect_format(f.path) == FormatType.RASTER
    ]


def _build_preview_results(
    file_statuses: list[FileStatus], *, force: bool
) -> list[ConversionResult]:
    """Build dry-run preview ConversionResults for --fix (and --force)."""

    def predicted_output(f: FileStatus) -> Path:
        if f.target_format == "GeoParquet":
            return f.path.parent / f"{f.path.stem}.parquet"
        return f.path.parent / f"{f.path.stem}.tif"

    previews = [
        ConversionResult(
            source=f.path,
            output=predicted_output(f),
            format_from=f.display_name,
            format_to=f.target_format,
            status=ConversionStatus.SUCCESS,  # Predicted outcome
            error=None,
            duration_ms=0,
        )
        for f in file_statuses
        if f.status == CloudNativeStatus.CONVERTIBLE
    ]
    if force:
        # Forced rasters re-encode in place: COG -> COG, same path.
        forced = set(_forced_raster_paths(file_statuses))
        previews.extend(
            ConversionResult(
                source=f.path,
                output=f.path.parent / f"{f.path.stem}.tif",
                format_from=f.display_name,
                format_to="COG",
                status=ConversionStatus.SUCCESS,  # Predicted outcome
                error=None,
                duration_ms=0,
            )
            for f in file_statuses
            if f.path in forced
        )
    return previews


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


# --- Check / fix workflow orchestration (ADR-0007: logic lives here) ---


def resolve_catalog_root_for_check(path: Path) -> Path | None:
    """Walk up from ``path`` to find the directory containing ``catalog.json``.

    The metadata scanner only needs ``catalog.json`` to function, so this
    deliberately does not require the ``.portolan/config.yaml`` sentinel that
    ``find_catalog_root`` insists on. Returns None if no ``catalog.json`` is
    found within the search depth.
    """
    from portolan_cli.constants import MAX_CATALOG_SEARCH_DEPTH

    candidate = path.resolve() if path.exists() else path
    for _ in range(MAX_CATALOG_SEARCH_DEPTH):
        if (candidate / "catalog.json").exists():
            return candidate
        if candidate.parent == candidate:
            break
        candidate = candidate.parent
    return None


def build_check_rules(path: Path, *, strict: bool) -> tuple[Any, ...]:
    """Build the validation rule set, honoring config severity overrides.

    Loads ``.portolan/config.yaml`` (when present) for ``stac_lint.severity.*``
    overrides and always routes through ``_build_rules`` so the ``strict`` flag
    and config are respected.

    Args:
        path: Directory being checked (catalog root or a subdirectory).
        strict: Whether ``--strict`` was passed (escalates warnings to errors).

    Returns:
        The ordered list of validation rule instances.
    """
    from portolan_cli.config import load_config
    from portolan_cli.validation.runner import _build_rules

    config = load_config(path) if (path / ".portolan" / "config.yaml").exists() else None
    return _build_rules(strict=strict, config=config)


@dataclass
class FixWorkflowOutcome:
    """Result of running the ``check --fix`` workflow.

    Attributes:
        metadata_fix_report: Metadata fix results (None when metadata not in scope
            or skipped because no catalog root was found in mixed mode).
        format_fix_report: Geo-asset conversion results (None when not in scope).
        has_failures: True if any metadata fix failed.
        fatal_error: Non-catalog message when a metadata-only fix found no
            ``catalog.json``; the caller surfaces it and exits non-zero.
    """

    metadata_fix_report: FixReport | None = None
    format_fix_report: Any = None
    has_failures: bool = False
    fatal_error: str | None = None


def run_fix_workflow(
    *,
    path: Path,
    run_metadata: bool,
    run_geo_assets: bool,
    dry_run: bool,
    remove_legacy: bool,
    force: bool = False,
    workers: int | None = None,
    on_progress: Callable[[ConversionResult], None] | None = None,
) -> FixWorkflowOutcome:
    """Run the metadata and/or geo-asset fix workflow and return the reports.

    This owns the fix orchestration (ADR-0007): resolving the catalog root,
    scanning metadata, applying fixes plus the title/tabular/PMTiles repairs, and
    converting geo-assets. It performs no output rendering or process exit — the
    caller renders the returned :class:`FixWorkflowOutcome` and decides exit codes.

    Args:
        path: Directory to check/fix.
        run_metadata: Whether to fix metadata issues.
        run_geo_assets: Whether to fix geo-asset format issues.
        dry_run: Preview changes without applying them.
        remove_legacy: Remove source files after successful conversion.
        force: Re-optimize already-valid COGs (raster-scoped, issue #530).
        workers: Parallel worker processes for conversion.
        on_progress: Optional per-conversion progress callback.

    Returns:
        A :class:`FixWorkflowOutcome` with the fix reports and status flags.
    """
    outcome = FixWorkflowOutcome()

    # Fix metadata if in scope
    if run_metadata:
        from portolan_cli.metadata import fix_metadata
        from portolan_cli.metadata.fix import (
            repair_agents_md,
            repair_pmtiles_links,
            repair_tabular_flags,
            repair_titles_and_links,
        )
        from portolan_cli.metadata.scan import scan_catalog_metadata

        # Resolve to the catalog root before scanning. Without this the scanner
        # returns an empty report whenever `path` points at a subdirectory below
        # the root, causing --fix to silently no-op. The scanner's only
        # structural requirement is catalog.json, so walk parents looking for it
        # (tests and existing catalogs may not have a .portolan sentinel, so
        # find_catalog_root is too strict here).
        catalog_root = resolve_catalog_root_for_check(path)
        if catalog_root is None:
            # Metadata-only mode: user explicitly asked, fail loudly so the
            # silent-no-op trap is closed. Mixed mode (--fix without flags):
            # stay backwards-compatible — skip metadata so the geo-assets pass
            # can still operate on the directory.
            if not run_geo_assets:
                outcome.fatal_error = (
                    f"fatal: not a portolan catalog (or any parent of {path}): "
                    "no catalog.json found, cannot run metadata fix"
                )
                return outcome
        else:
            metadata_check_report = scan_catalog_metadata(catalog_root)
            metadata_fix_report = fix_metadata(catalog_root, metadata_check_report, dry_run=dry_run)

            # Issue #502: populate human-readable titles/descriptions and
            # backfill child/item link titles as part of the metadata fix.
            metadata_fix_report.results.extend(
                repair_titles_and_links(catalog_root, dry_run=dry_run)
            )

            # Issue #481: backfill portolan:geospatial: false on tabular
            # collections (RULE-0090) as part of the metadata fix.
            metadata_fix_report.results.extend(repair_tabular_flags(catalog_root, dry_run=dry_run))

            # Issue #569: backfill the rel="pmtiles" web-map-links link on
            # collections with a PMTiles asset but no link (RULE-0061).
            metadata_fix_report.results.extend(repair_pmtiles_links(catalog_root, dry_run=dry_run))

            # ADR-0052: scaffold AGENTS.md and backfill the rel="agents" link on
            # catalogs and collections that lack them (RULE-0080/0081).
            metadata_fix_report.results.extend(repair_agents_md(catalog_root, dry_run=dry_run))

            outcome.metadata_fix_report = metadata_fix_report
            if metadata_fix_report.failure_count > 0:
                outcome.has_failures = True

    # Fix geo-assets if in scope
    if run_geo_assets:
        outcome.format_fix_report = check_directory(
            path,
            fix=True,
            dry_run=dry_run,
            remove_legacy=remove_legacy,
            force=force,
            workers=workers,
            on_progress=on_progress,
            catalog_path=path,
        )

    return outcome
