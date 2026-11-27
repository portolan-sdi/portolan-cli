"""Check command logic for validating and fixing geospatial files."""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

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
from portolan_cli.scan.detect import is_filegdb
from portolan_core.constants import GEOSPATIAL_EXTENSIONS, PARQUET_EXTENSION, SIDECAR_PATTERNS

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_cli.metadata.fix import FixReport
    from portolan_core.run_state import RunState

logger = logging.getLogger(__name__)

# Extensions to check for cloud-native status
CHECK_EXTENSIONS: frozenset[str] = GEOSPATIAL_EXTENSIONS | frozenset({PARQUET_EXTENSION})


@dataclass
class FileStatus:
    """Status of a single file for check command."""

    path: Path
    relative_path: str
    status: CloudNativeStatus
    display_name: str
    target_format: str | None


@dataclass
class LegacyRemovalReport:
    """Report from removing legacy files after conversion."""

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
    """Report from checking a directory for cloud-native status."""

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


# Legacy File Removal Functions


def get_legacy_files_to_remove(report: ConversionReport) -> list[Path]:
    """Identify legacy source files that can be safely removed after conversion."""
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
    """Get sidecar files for a given primary file."""
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
    """Remove legacy source files and their sidecars."""
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
    reoptimize: frozenset[Path] = frozenset(),
    workers: int | None = None,
    on_progress: Callable[[ConversionResult], None] | None = None,
    catalog_path: Path | None = None,
    state: RunState | None = None,
) -> CheckReport:
    """Check a directory for cloud-native status and optionally fix."""
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
        overrides = get_conversion_overrides(catalog_path, state=state)

    # Scan for geospatial files
    files = _scan_for_files(path)

    # Get cloud-native status for each file (with overrides applied)
    file_statuses = []
    for file_path in files:
        relative = _get_relative_path(file_path, path)
        if overrides is not None:
            status_info = get_effective_status(
                file_path, overrides=overrides, root=catalog_path, state=state
            )
        else:
            status_info = get_cloud_native_status(file_path, catalog_path)
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

    forced = _forced_paths(
        file_statuses, reoptimize, force=force, catalog_path=catalog_path, state=state
    )
    # Handle fix mode
    if fix and not dry_run:
        files_to_convert = [
            f.path for f in file_statuses if f.status == CloudNativeStatus.CONVERTIBLE
        ]
        conversion_report = convert_directory(
            path,
            on_progress=on_progress,
            file_paths=[*files_to_convert, *forced],
            catalog_path=catalog_path,
            workers=workers,
            force=force or bool(forced),
            state=state,
        )
        report.conversion_report = conversion_report
        _stage_converted(conversion_report, catalog_path or path, state)

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
            results=_build_preview_results(
                file_statuses, forced, catalog_path=catalog_path, state=state
            )
        )
        # Note: remove_legacy is ignored in dry_run mode (no actual removal)

    return report


def _stage_converted(report: ConversionReport, root: Path, state: RunState | None) -> None:
    """Record the new bytes of every tracked file a conversion wrote, so ``push`` uploads them."""
    from portolan_cli.finalization import stage_rewritten
    from portolan_cli.version_ops import commit_run
    from portolan_core.run_state import RunState

    outputs = [
        r.output for r in report.results if r.status is ConversionStatus.SUCCESS and r.output
    ]
    run = state or RunState.open(root)
    stage_rewritten(run, outputs)
    # A run that stages nothing must not commit: that commit rewrites the catalog index.
    if state is None and run.staged():
        commit_run(run)


def _forced_paths(
    file_statuses: list[FileStatus],
    reoptimize: frozenset[Path],
    *,
    force: bool,
    catalog_path: Path | None = None,
    state: RunState | None = None,
) -> list[Path]:
    """Cloud-native files to rewrite: rasters under ``force``, and every file in ``reoptimize``.

    A file qualifies only when its format handler has a ``reoptimize`` method. A symlink never does.
    """
    from portolan_cli.plugins.formats import handler_for
    from portolan_cli.report import emit

    if not force and not reoptimize:
        return []
    named = {p.resolve() for p in reoptimize}
    forced = []
    for f in file_statuses:
        if f.status != CloudNativeStatus.CLOUD_NATIVE:
            continue
        handler = handler_for(f.path, catalog_path, state=state)
        if handler is None or not hasattr(handler, "reoptimize"):
            continue
        if f.path.resolve() not in named and not (force and handler.family == "raster"):
            continue
        if f.path.is_symlink():
            # The rewrite swaps a new file in, so it would replace the link with a copy.
            emit("warn", f"{f.path.name}: a symlink, not rewritten in place")
            continue
        forced.append(f.path)
    return forced


def _build_preview_results(
    file_statuses: list[FileStatus],
    forced: list[Path],
    *,
    catalog_path: Path | None = None,
    state: RunState | None = None,
) -> list[ConversionResult]:
    """Build dry-run preview ConversionResults for --fix: conversions, then rewrites in place."""
    from portolan_cli.plugins.formats import handler_for

    def preview(f: FileStatus, format_to: str | None) -> ConversionResult:
        # The handler names its output. A plugin can write any file name.
        handler = handler_for(f.path, catalog_path, state=state)
        output = (
            handler.output_path(f.path, f.path.parent)
            if handler is not None
            else f.path.parent / f"{f.path.stem}.tif"
        )
        return ConversionResult(
            source=f.path,
            output=output,
            format_from=f.display_name,
            format_to=format_to or (handler.display_name if handler is not None else None),
            status=ConversionStatus.SUCCESS,  # Predicted outcome
            error=None,
            duration_ms=0,
        )

    rewritten = set(forced)
    return [
        *(
            preview(f, f.target_format)
            for f in file_statuses
            if f.status == CloudNativeStatus.CONVERTIBLE
        ),
        *(preview(f, None) for f in file_statuses if f.path in rewritten),
    ]


def _scan_for_files(path: Path) -> list[Path]:
    """Scan directory for geospatial files."""
    from portolan_cli.plugins.formats import plugin_extensions

    # A plugin format handler adds its extensions.
    extensions = CHECK_EXTENSIONS | plugin_extensions()
    files: list[Path] = []
    for item in path.rglob("*"):
        if not item.is_file():
            continue
        ext = item.suffix.lower()
        if ext in extensions:
            # For parquet, check if it's GeoParquet
            if ext == PARQUET_EXTENSION and not is_geoparquet(item):
                continue
            files.append(item)
    files.sort()
    return files


def _get_relative_path(file_path: Path, root: Path) -> str:
    """Get path relative to root as forward-slash string."""
    try:
        return file_path.relative_to(root).as_posix()
    except ValueError:
        return file_path.as_posix()


# --- Check / fix workflow orchestration (logic lives here) ---


def resolve_catalog_root_for_check(path: Path) -> Path | None:
    """Walk up from ``path`` to find the directory containing ``catalog.json``."""
    from portolan_core.constants import MAX_CATALOG_SEARCH_DEPTH

    candidate = path.resolve() if path.exists() else path
    for _ in range(MAX_CATALOG_SEARCH_DEPTH):
        if (candidate / "catalog.json").exists():
            return candidate
        if candidate.parent == candidate:
            break
        candidate = candidate.parent
    return None


@dataclass
class FixWorkflowOutcome:
    """Result of running the ``check --fix`` workflow."""

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
    reoptimize: frozenset[Path] = frozenset(),
    workers: int | None = None,
    on_progress: Callable[[ConversionResult], None] | None = None,
    state: RunState | None = None,
) -> FixWorkflowOutcome:
    """Run the metadata and/or geo-asset fix workflow and return the reports."""
    outcome = FixWorkflowOutcome()

    # Fix metadata if in scope
    if run_metadata:
        from portolan_cli.metadata import fix_metadata
        from portolan_cli.metadata.scan import scan_catalog_metadata

        # Walk up to catalog.json, so a subdirectory path does not make --fix a no-op.
        # find_catalog_root is too strict: a catalog may have no .portolan sentinel.
        catalog_root = resolve_catalog_root_for_check(path)
        if catalog_root is None:
            # Metadata-only mode fails loudly. Mixed mode skips metadata so the
            # geo-assets pass can still run on the directory.
            if not run_geo_assets:
                outcome.fatal_error = (
                    f"fatal: not a portolan catalog (or any parent of {path}): "
                    "no catalog.json found, cannot run metadata fix"
                )
                return outcome
        else:
            # No rule reports these two, so the registry never runs them: item
            # freshness and the sweep of fields the spec does not define.
            from portolan_cli.metadata.fix import strip_removed_fields

            metadata_check_report = scan_catalog_metadata(catalog_root, state=state)
            metadata_fix_report = fix_metadata(
                catalog_root, metadata_check_report, dry_run=dry_run, state=state
            )
            metadata_fix_report.results.extend(strip_removed_fields(catalog_root, dry_run=dry_run))

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
            reoptimize=reoptimize,
            workers=workers,
            on_progress=on_progress,
            catalog_path=resolve_catalog_root_for_check(path) or path,
            state=state,
        )

    return outcome
