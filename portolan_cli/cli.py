"""Portolan CLI - Command-line interface for managing cloud-native geospatial data.

The CLI is a thin wrapper around the Python API (see catalog.py).
All business logic lives in the library; the CLI handles user interaction.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import click

from portolan_cli.check import check_directory
from portolan_cli.dataset import (
    add_dataset,
    get_dataset_info,
    list_datasets,
    remove_dataset,
)
from portolan_cli.json_output import ErrorDetail, error_envelope, success_envelope
from portolan_cli.output import detail, error, info, success, warn
from portolan_cli.scan import (
    ScanIssue,
    ScanOptions,
    ScanResult,
    scan_directory,
)
from portolan_cli.scan import (
    Severity as ScanSeverity,
)
from portolan_cli.scan_fix import ProposedFix, apply_safe_fixes
from portolan_cli.scan_infer import infer_collections
from portolan_cli.scan_output import (
    format_collection_suggestion,
    generate_next_steps,
    get_category_display_name,
    get_fixability,
    group_skipped_files,
    render_tree_view,
)
from portolan_cli.validation import Severity
from portolan_cli.validation import check as validate_catalog


def should_output_json(ctx: click.Context, json_flag: bool = False) -> bool:
    """Determine if JSON output should be used.

    Checks both the global --format option and per-command --json flags.
    Global --format=json takes precedence, but per-command flags also work
    for backward compatibility.

    Args:
        ctx: Click context containing the format preference.
        json_flag: Per-command --json flag value.

    Returns:
        True if JSON output should be used, False for text output.
    """
    # Get format from context (set by global --format option)
    obj = ctx.find_root().obj or {}
    global_format = obj.get("format", "text")

    # Global format takes precedence, but per-command --json also works
    return global_format == "json" or json_flag


def output_json_envelope(envelope: Any) -> None:
    """Output a JSON envelope to stdout.

    Args:
        envelope: OutputEnvelope instance to output.
    """
    click.echo(envelope.to_json())


@click.group()
@click.version_option()
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["json", "text"]),
    default="text",
    help="Output format (json for machine parsing, text for humans).",
)
@click.pass_context
def cli(ctx: click.Context, output_format: str) -> None:
    """Portolan - Publish and manage cloud-native geospatial data catalogs."""
    # Store format in context for subcommands
    ctx.ensure_object(dict)
    ctx.obj["format"] = output_format


@cli.command()
@click.argument("path", type=click.Path(path_type=Path), default=".")
@click.option(
    "--auto",
    "auto_mode",  # Rename to avoid vulture unused variable warning
    is_flag=True,
    default=False,
    help="Skip interactive prompts and use auto-extracted/default values.",
)
@click.option(
    "--title",
    "-t",
    type=str,
    default=None,
    help="Human-readable title for the catalog.",
)
@click.option(
    "--description",
    "-d",
    type=str,
    default=None,
    help="Description of the catalog.",
)
@click.pass_context
def init(
    ctx: click.Context, path: Path, auto_mode: bool, title: str | None, description: str | None
) -> None:
    """Initialize a new Portolan catalog.

    Creates a catalog.json at the root level and a .portolan directory with
    management files (config.json, state.json, versions.json).

    Auto-extracts the catalog ID from the directory name.

    PATH is the directory where the catalog should be created (default: current directory).

    Use --auto to skip all prompts and use default values. Use --title and
    --description to set catalog metadata directly.

    \b
    Examples:
        portolan init                       # Initialize in current directory
        portolan init --auto                # Skip prompts, use defaults
        portolan init --title "My Catalog"  # Set title
        portolan init /path/to/data --auto  # Initialize in specific directory
    """
    import json

    from portolan_cli.catalog import init_catalog
    from portolan_cli.errors import CatalogAlreadyExistsError, UnmanagedStacCatalogError

    use_json = should_output_json(ctx)

    # Interactive prompting (unless --auto or JSON mode)
    if not auto_mode and not use_json:
        if title is None:
            title_input = click.prompt(
                "Catalog title (optional, press Enter to skip)",
                default="",
                show_default=False,
            )
            if title_input:
                title = title_input

        if description is None:
            description = click.prompt(
                "Catalog description",
                default="A Portolan-managed STAC catalog",
            )

    try:
        catalog_file, warnings = init_catalog(
            path,
            title=title,
            description=description,
        )

        # Read back catalog ID for display
        catalog_data = json.loads(catalog_file.read_text())
        catalog_id = catalog_data.get("id", "unknown")

        if use_json:
            envelope = success_envelope(
                "init",
                {
                    "path": str(path.resolve()),
                    "catalog_file": "catalog.json",
                    "catalog_id": catalog_id,
                    "warnings": warnings,
                },
            )
            output_json_envelope(envelope)
        else:
            success(f"Initialized Portolan catalog in {path.resolve()}")
            info(f"Catalog ID: {catalog_id}")
            for w in warnings:
                warn(w)

    except CatalogAlreadyExistsError as err:
        if use_json:
            envelope = error_envelope(
                "init",
                [ErrorDetail(type="CatalogAlreadyExistsError", message=str(err), code=err.code)],
            )
            output_json_envelope(envelope)
        else:
            error(f"Already a Portolan catalog at {path.resolve()}")
        raise SystemExit(1) from err
    except UnmanagedStacCatalogError as err:
        if use_json:
            envelope = error_envelope(
                "init",
                [ErrorDetail(type="UnmanagedStacCatalogError", message=str(err), code=err.code)],
            )
            output_json_envelope(envelope)
        else:
            error(f"Existing STAC catalog found at {path.resolve()}")
            info("Use 'portolan adopt' to bring it under Portolan management (not yet implemented)")
        raise SystemExit(1) from err


def _output_check_json(report: Any, *, mode: str = "all") -> None:
    """Output check results as JSON envelope.

    Args:
        report: ValidationReport from metadata validation.
        mode: Check mode ("metadata", "format", or "all").
    """
    data = report.to_dict()
    data["mode"] = mode
    data["summary"] = {
        "total": len(report.results),
        "passed": sum(1 for r in report.results if r.passed),
        "errors": len(report.errors),
        "warnings": len(report.warnings),
    }

    if report.passed:
        envelope = success_envelope("check", data)
    else:
        errors = [ErrorDetail(type="ValidationError", message=r.message) for r in report.errors]
        envelope = error_envelope("check", errors, data=data)

    output_json_envelope(envelope)


def _print_validation_result(result: Any) -> None:
    """Print a single validation result with appropriate formatting."""
    msg = f"{result.rule_name}: {result.message}"
    if result.passed:
        success(msg)
    elif result.severity == Severity.ERROR:
        error(msg)
    elif result.severity == Severity.WARNING:
        warn(msg)
    else:
        info(msg)

    if not result.passed and result.fix_hint:
        detail(f"  Hint: {result.fix_hint}")


def _print_check_summary(report: Any) -> None:
    """Print check summary message."""
    if report.passed:
        success("All validation checks passed")
        return

    error_count = len(report.errors)
    warning_count = len(report.warnings)
    parts = []
    if error_count:
        parts.append(f"{error_count} error{'s' if error_count != 1 else ''}")
    if warning_count:
        parts.append(f"{warning_count} warning{'s' if warning_count != 1 else ''}")
    error(f"Validation failed: {', '.join(parts)}")


def _print_format_check_results(report: Any, *, verbose: bool = False) -> None:
    """Print format check results (not conversion, just status check).

    Args:
        report: CheckReport with file statuses.
        verbose: If True, show all files including cloud-native.
    """
    from portolan_cli.formats import CloudNativeStatus

    if report.total == 0:
        info("No geospatial files found")
        return

    cloud_native = [f for f in report.files if f.status == CloudNativeStatus.CLOUD_NATIVE]
    convertible = [f for f in report.files if f.status == CloudNativeStatus.CONVERTIBLE]
    unsupported = [f for f in report.files if f.status == CloudNativeStatus.UNSUPPORTED]

    # Summary
    if cloud_native:
        success(f"{len(cloud_native)} file(s) already cloud-native")
    if convertible:
        warn(f"{len(convertible)} file(s) need conversion")
    if unsupported:
        detail(f"{len(unsupported)} file(s) unsupported")

    # Details if verbose
    if verbose:
        for f in cloud_native:
            success(f"  {f.relative_path} ({f.display_name})")
        for f in convertible:
            warn(f"  {f.relative_path} ({f.display_name}) → {f.target_format}")
        for f in unsupported:
            detail(f"  {f.relative_path} ({f.display_name})")


def _output_combined_check_json(
    metadata_report: Any | None,
    format_report: Any | None,
    *,
    mode: str = "all",
) -> None:
    """Output combined check results as JSON envelope.

    Args:
        metadata_report: Optional ValidationReport from metadata validation.
        format_report: Optional CheckReport from format checking.
        mode: Check mode ("metadata", "format", or "all").
    """
    data: dict[str, Any] = {"mode": mode}
    errors: list[ErrorDetail] = []

    if metadata_report is not None:
        data["metadata"] = metadata_report.to_dict()
        data["metadata"]["summary"] = {
            "total": len(metadata_report.results),
            "passed": sum(1 for r in metadata_report.results if r.passed),
            "errors": len(metadata_report.errors),
            "warnings": len(metadata_report.warnings),
        }
        if metadata_report.errors:
            errors.extend(
                [
                    ErrorDetail(type="ValidationError", message=r.message)
                    for r in metadata_report.errors
                ]
            )

    if format_report is not None:
        data["format"] = format_report.to_dict()

    # Determine overall success
    has_errors = bool(errors)

    if has_errors:
        envelope = error_envelope("check", errors, data=data)
    else:
        envelope = success_envelope("check", data)

    output_json_envelope(envelope)


@cli.command()
@click.argument("path", type=click.Path(path_type=Path), default=".")
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON")
@click.option("--verbose", "-v", is_flag=True, help="Show all validation rules, not just failures")
@click.option(
    "--fix",
    is_flag=True,
    help="Convert non-cloud-native files to cloud-native formats (GeoParquet, COG)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Preview what would be converted (use with --fix)",
)
@click.option(
    "--metadata",
    is_flag=True,
    help="Only validate STAC metadata (links, schema, required fields)",
)
@click.option(
    "--geo-assets",
    "geo_assets",
    is_flag=True,
    help="Only check geospatial assets (cloud-native status, convertibility)",
)
@click.pass_context
def check(
    ctx: click.Context,
    path: Path,
    json_output: bool,
    verbose: bool,
    fix: bool,
    dry_run: bool,
    metadata: bool,
    geo_assets: bool,
) -> None:
    """Validate a Portolan catalog or check files for cloud-native status.

    Runs validation rules against the catalog and reports any issues.
    With --fix, converts non-cloud-native files to GeoParquet (vectors) or COG (rasters).

    PATH is the directory to check (default: current directory).

    Use --metadata or --geo-assets to run only specific validations:
    - --metadata: Validate STAC catalog structure and metadata
    - --geo-assets: Check geospatial assets for cloud-native compliance

    Examples:

        portolan check                        # Validate all (metadata + geo-assets)

        portolan check --metadata             # Validate metadata only

        portolan check --geo-assets           # Check geo-assets only

        portolan check /data --fix            # Convert files to cloud-native

        portolan check /data --geo-assets --fix  # Convert only (no metadata validation)

        portolan check /data --fix --dry-run  # Preview conversions
    """
    use_json = should_output_json(ctx, json_output)

    # Validate path exists
    if not path.exists():
        _handle_path_not_found(path, use_json)

    # Warn if --dry-run is used without --fix
    if dry_run and not fix:
        warn("--dry-run has no effect without --fix")

    # Determine which checks to run
    run_metadata, run_format, mode = _determine_check_mode(metadata, geo_assets, fix)

    # Execute the appropriate check workflow
    _execute_check_workflow(
        path=path,
        run_metadata=run_metadata,
        run_format=run_format,
        mode=mode,
        fix=fix,
        dry_run=dry_run,
        use_json=use_json,
        verbose=verbose,
    )


def _handle_path_not_found(path: Path, use_json: bool) -> None:
    """Handle path not found error and exit."""
    if use_json:
        envelope = error_envelope(
            "check",
            [ErrorDetail(type="PathNotFoundError", message=f"Path does not exist: {path}")],
        )
        output_json_envelope(envelope)
    else:
        error(f"Path does not exist: {path}")
    raise SystemExit(1)


def _determine_check_mode(metadata: bool, geo_assets: bool, fix: bool) -> tuple[bool, bool, str]:
    """Determine which checks to run and the mode string.

    Returns:
        Tuple of (run_metadata, run_format, mode_string).
    """
    explicit_flags = metadata or geo_assets

    if explicit_flags:
        run_metadata = metadata
        run_format = geo_assets
    else:
        # Backward compatible: metadata without fix, format with fix
        run_metadata = not fix
        run_format = fix

    # Determine mode string
    if run_metadata and not run_format:
        mode = "metadata"
    elif run_format and not run_metadata:
        mode = "geo-assets"
    else:
        mode = "all"

    return run_metadata, run_format, mode


def _execute_check_workflow(
    *,
    path: Path,
    run_metadata: bool,
    run_format: bool,
    mode: str,
    fix: bool,
    dry_run: bool,
    use_json: bool,
    verbose: bool,
) -> None:
    """Execute the check workflow based on flags."""
    metadata_report = None

    # Run metadata validation if requested
    if run_metadata:
        metadata_report = validate_catalog(path)
        if not run_format:
            _output_metadata_only(metadata_report, mode, use_json, verbose)
            return

    # Run format check if requested
    if run_format:
        if fix:
            _run_check_fix(
                path=path,
                dry_run=dry_run,
                use_json=use_json,
                verbose=verbose,
                mode=mode,
                metadata_report=metadata_report,
            )
        elif not run_metadata:
            _output_format_only(path, mode, use_json, verbose)
        else:
            _output_combined(path, metadata_report, mode, use_json, verbose)


def _output_metadata_only(report: Any, mode: str, use_json: bool, verbose: bool) -> None:
    """Output metadata-only check results."""
    if use_json:
        _output_check_json(report, mode=mode)
    else:
        for result in report.results:
            if verbose or not result.passed:
                _print_validation_result(result)
        _print_check_summary(report)
    if report.errors:
        raise SystemExit(1)


def _output_format_only(path: Path, mode: str, use_json: bool, verbose: bool) -> None:
    """Output format-only check results."""
    format_report = check_directory(path, fix=False, dry_run=False)
    if use_json:
        data = format_report.to_dict()
        data["mode"] = mode
        envelope = success_envelope("check", data)
        output_json_envelope(envelope)
    else:
        _print_format_check_results(format_report, verbose=verbose)


def _output_combined(
    path: Path, metadata_report: Any, mode: str, use_json: bool, verbose: bool
) -> None:
    """Output combined metadata + format check results."""
    format_report = check_directory(path, fix=False, dry_run=False)
    has_errors = False

    if use_json:
        _output_combined_check_json(metadata_report, format_report, mode=mode)
    else:
        if metadata_report:
            info("Metadata validation:")
            for result in metadata_report.results:
                if verbose or not result.passed:
                    _print_validation_result(result)
            _print_check_summary(metadata_report)
            has_errors = bool(metadata_report.errors)
        if format_report:
            info("\nFormat check:")
            _print_format_check_results(format_report, verbose=verbose)

    if has_errors:
        raise SystemExit(1)


# ─────────────────────────────────────────────────────────────────────────────
# Check --fix helpers
# ─────────────────────────────────────────────────────────────────────────────


def _run_check_fix(
    *,
    path: Path,
    dry_run: bool,
    use_json: bool,
    verbose: bool = False,
    mode: str = "all",
    metadata_report: Any | None = None,
) -> None:
    """Run check --fix workflow to convert files to cloud-native formats.

    Args:
        path: Directory to check and optionally fix.
        dry_run: If True, preview without making changes.
        use_json: If True, output JSON envelope.
        verbose: If True, show detailed output for each file.
        mode: Check mode ("metadata", "format", or "all").
        metadata_report: Optional ValidationReport from metadata validation.
    """
    report = check_directory(path, fix=True, dry_run=dry_run)
    has_metadata_errors = metadata_report is not None and bool(metadata_report.errors)
    has_conversion_errors = (
        report.conversion_report is not None and report.conversion_report.failed > 0
    )

    if use_json:
        _output_fix_json(report, metadata_report, mode)
    else:
        _output_fix_human(report, metadata_report, dry_run, verbose)

    if has_conversion_errors or has_metadata_errors:
        raise SystemExit(1)


def _output_fix_json(report: Any, metadata_report: Any | None, mode: str) -> None:
    """Output JSON for check --fix workflow."""
    from portolan_cli.convert import ConversionStatus

    data = report.to_dict()
    data["mode"] = mode
    has_metadata_errors = metadata_report is not None and bool(metadata_report.errors)
    has_conversion_errors = (
        report.conversion_report is not None and report.conversion_report.failed > 0
    )

    if metadata_report is not None:
        data["metadata"] = metadata_report.to_dict()

    errors: list[ErrorDetail] = []
    if has_conversion_errors and report.conversion_report is not None:
        errors.extend(
            ErrorDetail(type="ConversionFailed", message=r.error or "Unknown error")
            for r in report.conversion_report.results
            if r.status == ConversionStatus.FAILED
        )
    if has_metadata_errors and metadata_report is not None:
        errors.extend(
            ErrorDetail(type="ValidationError", message=r.message) for r in metadata_report.errors
        )

    if errors:
        envelope = error_envelope("check", errors, data=data)
    else:
        envelope = success_envelope("check", data)
    output_json_envelope(envelope)


def _output_fix_human(
    report: Any, metadata_report: Any | None, dry_run: bool, verbose: bool
) -> None:
    """Output human-readable results for check --fix workflow."""
    if metadata_report is not None:
        info("Metadata validation:")
        for result in metadata_report.results:
            if verbose or not result.passed:
                _print_validation_result(result)
        _print_check_summary(metadata_report)
        info("")  # Blank line separator

    info("Format conversion:")
    if dry_run:
        _print_check_fix_preview(report)
    else:
        _print_check_fix_results(report, verbose=verbose)


def _print_check_fix_preview(report: Any) -> None:
    """Print preview of what would be converted."""
    from portolan_cli.formats import CloudNativeStatus

    convertible = [f for f in report.files if f.status == CloudNativeStatus.CONVERTIBLE]

    if not convertible:
        info("No files need conversion")
        return

    info(f"Dry run: {len(convertible)} file(s) would be converted")
    for f in convertible:
        detail(f"  {f.relative_path} ({f.display_name}) -> {f.target_format}")


def _print_check_fix_results(report: Any, *, verbose: bool = False) -> None:
    """Print conversion results.

    Args:
        report: CheckReport with conversion results.
        verbose: If True, show details for all files including skipped.
    """
    from portolan_cli.convert import ConversionStatus

    conv = report.conversion_report
    if conv is None:
        return

    if conv.total == 0:
        info("No files to convert")
        return

    # Summary
    if conv.succeeded > 0:
        success(f"Converted {conv.succeeded} file(s)")
    if conv.skipped > 0:
        detail(f"  {conv.skipped} file(s) skipped (already cloud-native)")
    if conv.failed > 0:
        error(f"  {conv.failed} file(s) failed")
    if conv.invalid > 0:
        warn(f"  {conv.invalid} file(s) invalid after conversion")

    # Show details for failures (always) and successes/skipped (if verbose)
    for r in conv.results:
        if r.status == ConversionStatus.FAILED:
            error(f"  {r.source.name}: {r.error}")
        elif r.status == ConversionStatus.SUCCESS:
            detail(f"  {r.source.name} -> {r.output.name if r.output else 'N/A'}")
        elif verbose and r.status == ConversionStatus.SKIPPED:
            detail(f"  {r.source.name} (skipped - already cloud-native)")


# ─────────────────────────────────────────────────────────────────────────────
# Scan command
# ─────────────────────────────────────────────────────────────────────────────


def _handle_fix_mode(
    result: ScanResult,
    *,
    dry_run: bool,
    use_json: bool,
) -> tuple[list[ProposedFix], list[ProposedFix]]:
    """Handle --fix mode for scan command.

    Args:
        result: Scan result with issues to fix.
        dry_run: If True, preview fixes without applying.
        use_json: If True, suppress human output.

    Returns:
        Tuple of (proposed_fixes, applied_fixes).
    """
    # Dry-run mode: compute and show what would be done
    if dry_run:
        proposed, _ = apply_safe_fixes(result.issues, dry_run=True)
        if not use_json:
            if not proposed:
                info("No issues to fix")
            else:
                info(f"Dry run: {len(proposed)} fix(es) would be applied")
                for fix in proposed:
                    detail(f"  {fix.preview}")
        return proposed, []

    # Apply fixes
    proposed, applied = apply_safe_fixes(result.issues, dry_run=False)

    if not use_json:
        if not proposed:
            info("No issues to fix")
        else:
            # Show successful fixes
            if applied:
                success(f"Applied {len(applied)} fix(es)")
                for fix in applied:
                    detail(f"  {fix.preview}")

            # Show any that failed to apply (collisions)
            failed = [p for p in proposed if p not in applied]
            if failed:
                warn(f"{len(failed)} fix(es) could not be applied (collision):")
                for fix in failed:
                    detail(f"  {fix.preview}")

    return proposed, applied


@cli.command()
@click.argument("path", type=click.Path(path_type=Path))
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON")
@click.option(
    "--no-recursive",
    is_flag=True,
    help="Scan only the target directory (no subdirectories)",
)
@click.option(
    "--max-depth",
    type=int,
    default=None,
    help="Maximum recursion depth (0 = target directory only)",
)
@click.option(
    "--include-hidden",
    is_flag=True,
    help="Include hidden files (starting with .)",
)
@click.option(
    "--follow-symlinks",
    is_flag=True,
    help="Follow symbolic links (may cause loops)",
)
@click.option(
    "--all",
    "show_all",
    is_flag=True,
    help="Show all issues without truncation (default: show first 10 per severity)",
)
@click.option(
    "--tree",
    "show_tree",
    is_flag=True,
    help="Show directory tree view with file status markers",
)
@click.option(
    "--suggest-collections",
    "suggest_collections",
    is_flag=True,
    help="Suggest collection groupings based on filename patterns",
)
@click.option(
    "--manual",
    "manual_only",
    is_flag=True,
    help="Show only issues requiring manual resolution",
)
@click.option(
    "--fix",
    is_flag=True,
    help="Apply safe fixes (rename files with invalid characters, Windows reserved names, or long paths)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Preview fixes without applying them (use with --fix)",
)
@click.pass_context
def scan(
    ctx: click.Context,
    path: Path,
    json_output: bool,
    no_recursive: bool,
    max_depth: int | None,
    include_hidden: bool,
    follow_symlinks: bool,
    show_all: bool,
    show_tree: bool,
    suggest_collections: bool,
    manual_only: bool,
    fix: bool,
    dry_run: bool,
) -> None:
    """Scan a directory for geospatial files and potential issues.

    Discovers files by extension, validates shapefile completeness,
    and reports issues that may cause problems during import.

    PATH is the directory to scan.

    \b
    Fix Mode:
        Use --fix to auto-rename files with:
        - Invalid characters (spaces, parentheses, non-ASCII)
        - Windows reserved names (CON, PRN, AUX, etc.)
        - Long paths (> 200 characters)

        Use --dry-run to preview changes without applying.

    Examples:

        portolan scan /data/geospatial

        portolan scan . --json

        portolan scan /large/tree --max-depth=2

        portolan scan /data --no-recursive

        portolan scan /data --fix --dry-run

        portolan scan /data --fix
    """
    use_json = should_output_json(ctx, json_output)

    # Validate path exists and is a directory (handle in code for JSON envelope support)
    if not path.exists():
        if use_json:
            envelope = error_envelope(
                "scan",
                [
                    ErrorDetail(
                        type="PathNotFoundError", message=f"Directory does not exist: {path}"
                    )
                ],
            )
            output_json_envelope(envelope)
        else:
            error(f"Directory does not exist: {path}")
        raise SystemExit(1)

    if not path.is_dir():
        if use_json:
            envelope = error_envelope(
                "scan",
                [
                    ErrorDetail(
                        type="NotADirectoryError", message=f"Path is not a directory: {path}"
                    )
                ],
            )
            output_json_envelope(envelope)
        else:
            error(f"Path is not a directory: {path}")
        raise SystemExit(1)

    # Build options from CLI flags
    options = ScanOptions(
        recursive=not no_recursive,
        max_depth=max_depth,
        include_hidden=include_hidden,
        follow_symlinks=follow_symlinks,
        show_all=show_all,
        suggest_collections=suggest_collections,
    )

    try:
        result = scan_directory(path, options)
    except FileNotFoundError as err:
        if use_json:
            envelope = error_envelope(
                "scan",
                [ErrorDetail(type="FileNotFoundError", message=str(err))],
            )
            output_json_envelope(envelope)
        else:
            error(str(err))
        raise SystemExit(1) from err
    except NotADirectoryError as err:
        if use_json:
            envelope = error_envelope(
                "scan",
                [ErrorDetail(type="NotADirectoryError", message=str(err))],
            )
            output_json_envelope(envelope)
        else:
            error(str(err))
        raise SystemExit(1) from err

    # Run collection inference if requested
    if suggest_collections and result.ready:
        result.collection_suggestions = infer_collections(result.ready)

    # Warn if --dry-run is used without --fix (no effect)
    if dry_run and not fix:
        warn("--dry-run has no effect without --fix")

    # Handle fix mode
    if fix:
        proposed, applied = _handle_fix_mode(
            result,
            dry_run=dry_run,
            use_json=use_json,
        )
        result.proposed_fixes = proposed
        result.applied_fixes = applied

    if use_json:
        # JSON output with envelope
        data = result.to_dict()
        data["summary"] = {
            "ready_count": len(result.ready),
            "error_count": result.error_count,
            "warning_count": result.warning_count,
            "skipped_count": len(result.skipped),
        }

        if result.has_errors:
            # Still return data, but mark as not successful
            errors = [
                ErrorDetail(type=issue.issue_type.value, message=issue.message)
                for issue in result.issues
                if issue.severity == ScanSeverity.ERROR
            ]
            envelope = error_envelope("scan", errors, data=data)
        else:
            envelope = success_envelope("scan", data)

        output_json_envelope(envelope)
    elif manual_only:
        # Show only issues requiring manual resolution
        from portolan_cli.scan_output import format_scan_output

        output = format_scan_output(result, manual_only=True)
        click.echo(output)
    else:
        # Human-readable output per FR-018
        _print_scan_summary_enhanced(result, show_all=show_all, show_tree=show_tree)

    # Scan is informational — always exit 0 on success


def _print_scan_header(result: ScanResult) -> None:
    """Print scan header with file counts."""
    ready_count = len(result.ready)
    if ready_count == 0:
        info(f"Scanned {result.directories_scanned} directories")
        warn("No geo-assets found")
    else:
        success(f"{ready_count} geo-asset{'s' if ready_count != 1 else ''} found")


def _print_format_breakdown(result: ScanResult) -> None:
    """Print breakdown of files by format."""
    if not result.ready:
        return
    formats: dict[str, int] = {}
    for f in result.ready:
        formats[f.extension] = formats.get(f.extension, 0) + 1
    for ext, count in sorted(formats.items()):
        detail(f"  {count} {ext} file{'s' if count != 1 else ''}")


# Default maximum issues to show per severity before truncation
DEFAULT_ISSUE_LIMIT = 10


def _print_issue_group(
    issues: list[ScanIssue],
    severity: ScanSeverity,
    header_fn: Callable[[str], None],
    count: int,
    label: str,
    *,
    show_all: bool = False,
    limit: int = DEFAULT_ISSUE_LIMIT,
) -> None:
    """Print a group of issues with the same severity.

    Args:
        issues: List of all issues.
        severity: Severity level to filter for.
        header_fn: Function to print header (error/warn/info).
        count: Total count of issues with this severity.
        label: Label for the issue type (e.g., "error", "warning").
        show_all: If True, show all issues without truncation.
        limit: Maximum issues to show per severity (default: 10).
    """
    if count == 0:
        return
    header_fn(f"{count} {label}{'s' if count != 1 else ''}")

    # Filter issues by severity
    severity_issues = [i for i in issues if i.severity == severity]

    # Apply truncation if needed
    displayed = severity_issues if show_all else severity_issues[:limit]
    truncated_count = len(severity_issues) - len(displayed)

    for issue in displayed:
        header_fn(f"  {issue.relative_path}: {issue.message}")
        if issue.suggestion is not None:
            detail(f"    Hint: {issue.suggestion}")

    # Show truncation message if issues were hidden
    if truncated_count > 0:
        detail(f"  ... and {truncated_count} more (use --all to see all)")


def _print_issues_by_severity(result: ScanResult, *, show_all: bool = False) -> None:
    """Print issues grouped by severity.

    Args:
        result: The scan result containing issues.
        show_all: If True, show all issues without truncation.
    """
    if not result.issues:
        return

    _print_issue_group(
        result.issues, ScanSeverity.ERROR, error, result.error_count, "error", show_all=show_all
    )
    _print_issue_group(
        result.issues,
        ScanSeverity.WARNING,
        warn,
        result.warning_count,
        "warning",
        show_all=show_all,
    )
    _print_issue_group(
        result.issues, ScanSeverity.INFO, info, result.info_count, "info message", show_all=show_all
    )


def _print_scan_summary(result: ScanResult, *, show_all: bool = False) -> None:
    """Print human-readable scan summary (legacy).

    Args:
        result: The scan result to print.
        show_all: If True, show all issues without truncation.
    """
    _print_scan_header(result)
    _print_format_breakdown(result)
    _print_issues_by_severity(result, show_all=show_all)

    if result.skipped:
        detail(f"{len(result.skipped)} files skipped (unrecognized format)")


def _print_scan_summary_enhanced(
    result: ScanResult,
    *,
    show_all: bool = False,
    show_tree: bool = False,
) -> None:
    """Print enhanced human-readable scan summary.

    Includes:
    - Summary header
    - Format breakdown
    - Tree view (if --tree)
    - Issues with fixability labels
    - Skipped files by category
    - Collection suggestions
    - Actionable next steps

    Args:
        result: The scan result to print.
        show_all: If True, show all issues without truncation.
        show_tree: If True, show directory tree view.
    """
    # Header
    _print_scan_header(result)
    _print_format_breakdown(result)

    # Tree view (if requested)
    if show_tree:
        click.echo()
        tree_output = render_tree_view(result, show_missing=True)
        click.echo(tree_output)

    # Issues with fixability labels
    _print_issues_with_fixability(result, show_all=show_all)

    # Skipped files by category
    _print_skipped_by_category(result)

    # Collection suggestions
    _print_collection_suggestions(result)

    # Next steps
    _print_next_steps(result)


def _print_issues_with_fixability(result: ScanResult, *, show_all: bool = False) -> None:
    """Print issues grouped by severity with fixability labels.

    Args:
        result: The scan result containing issues.
        show_all: If True, show all issues without truncation.
    """
    if not result.issues:
        return

    limit = None if show_all else DEFAULT_ISSUE_LIMIT

    # Group by severity
    for severity, header_fn, label in [
        (ScanSeverity.ERROR, error, "error"),
        (ScanSeverity.WARNING, warn, "warning"),
        (ScanSeverity.INFO, info, "info message"),
    ]:
        severity_issues = [i for i in result.issues if i.severity == severity]
        if not severity_issues:
            continue

        count = len(severity_issues)
        header_fn(f"{count} {label}{'s' if count != 1 else ''}")

        # Apply truncation if needed
        displayed = severity_issues if limit is None else severity_issues[:limit]
        truncated_count = len(severity_issues) - len(displayed)

        for issue in displayed:
            fix_label = get_fixability(issue.issue_type).label
            header_fn(f"  {fix_label} {issue.relative_path}: {issue.message}")
            if issue.suggestion is not None:
                detail(f"    Hint: {issue.suggestion}")

        if truncated_count > 0:
            detail(f"  ... and {truncated_count} more (use --all to see all)")


def _print_skipped_by_category(result: ScanResult) -> None:
    """Print skipped files grouped by category.

    Args:
        result: The scan result containing skipped files.
    """
    if not result.skipped:
        return

    grouped = group_skipped_files(result.skipped)
    if not grouped:
        # Fallback for legacy Path objects
        detail(f"{len(result.skipped)} files skipped (unrecognized format)")
        return

    # Check if any files are truly unknown
    from portolan_cli.scan_classify import FileCategory

    unknown_count = len(grouped.get(FileCategory.UNKNOWN, []))
    recognized_count = len(result.skipped) - unknown_count

    # If all files are recognized (no unknowns), show a concise summary
    if unknown_count == 0:
        # Build a compact list: "5 catalog files, 4 tabular, 2 thumbnails, ..."
        parts = []
        for category, files in sorted(grouped.items(), key=lambda x: len(x[1]), reverse=True):
            display_name = get_category_display_name(category)
            parts.append(f"{len(files)} {display_name}")
        detail(f"  {', '.join(parts)}")
    else:
        # Some unknown files - show the detailed breakdown
        click.echo()
        if unknown_count > 0:
            warn(f"{unknown_count} files with unrecognized format")
        detail(f"Other files ({recognized_count} recognized):")
        for category, files in sorted(grouped.items(), key=lambda x: len(x[1]), reverse=True):
            display_name = get_category_display_name(category)
            detail(f"  {len(files)} {display_name}")


def _print_collection_suggestions(result: ScanResult) -> None:
    """Print collection suggestions if available.

    Args:
        result: The scan result with collection suggestions.
    """
    if not result.collection_suggestions:
        return

    click.echo()
    info("Suggested collections:")
    for suggestion in result.collection_suggestions:
        click.echo(format_collection_suggestion(suggestion))


def _print_next_steps(result: ScanResult) -> None:
    """Print actionable next steps.

    Args:
        result: The scan result to analyze for next steps.
    """
    steps = generate_next_steps(result)
    if not steps:
        return

    click.echo()
    info("Next steps:")
    for step in steps:
        detail(f"  \u2192 {step}")


# ─────────────────────────────────────────────────────────────────────────────
# Dataset commands
# ─────────────────────────────────────────────────────────────────────────────


@cli.group()
@click.pass_context
def dataset(ctx: click.Context) -> None:
    """Manage datasets in the catalog."""
    # Ensure context is passed through to subcommands
    ctx.ensure_object(dict)


@dataset.command("add")
@click.argument("path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--collection",
    "-c",
    required=True,
    help="Collection to add the dataset to.",
)
@click.option("--title", "-t", help="Display title for the dataset.")
@click.option("--description", "-d", help="Description of the dataset.")
@click.option("--id", "item_id", help="Item ID (defaults to filename).")
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=".",
    help="Path to catalog root (default: current directory).",
)
@click.pass_context
def dataset_add(
    ctx: click.Context,
    path: Path,
    collection: str,
    title: str | None,
    description: str | None,
    item_id: str | None,
    catalog_path: Path,
) -> None:
    """Add a dataset to the catalog.

    PATH is the file or directory to add.

    Examples:

        portolan dataset add data.geojson --collection demographics

        portolan dataset add raster.tif --collection imagery --title "Satellite Image"
    """
    use_json = should_output_json(ctx)

    try:
        result = add_dataset(
            path=path,
            catalog_root=catalog_path,
            collection_id=collection,
            title=title,
            description=description,
            item_id=item_id,
        )

        if use_json:
            envelope = success_envelope(
                "dataset_add",
                {
                    "item_id": result.item_id,
                    "collection_id": result.collection_id,
                    "format": result.format_type.value,
                    "bbox": result.bbox,
                    "title": result.title,
                },
            )
            output_json_envelope(envelope)
        else:
            success(f"Added {result.item_id} to collection {result.collection_id}")
            if result.title:
                detail(f"  Title: {result.title}")
            detail(f"  Format: {result.format_type.value}")
            detail(f"  Bbox: {result.bbox}")
    except ValueError as err:
        if use_json:
            envelope = error_envelope(
                "dataset_add",
                [ErrorDetail(type="ValueError", message=str(err))],
            )
            output_json_envelope(envelope)
        else:
            error(str(err))
        raise SystemExit(1) from err
    except FileNotFoundError as err:
        if use_json:
            envelope = error_envelope(
                "dataset_add",
                [ErrorDetail(type="FileNotFoundError", message=str(err))],
            )
            output_json_envelope(envelope)
        else:
            error(str(err))
        raise SystemExit(1) from err


@dataset.command("list")
@click.option(
    "--collection",
    "-c",
    help="Filter by collection ID.",
)
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=".",
    help="Path to catalog root (default: current directory).",
)
@click.pass_context
def dataset_list(ctx: click.Context, collection: str | None, catalog_path: Path) -> None:
    """List datasets in the catalog.

    Examples:

        portolan dataset list

        portolan dataset list --collection demographics
    """
    use_json = should_output_json(ctx)

    datasets = list_datasets(catalog_path, collection_id=collection)

    if use_json:
        envelope = success_envelope(
            "dataset_list",
            {
                "datasets": [
                    {
                        "item_id": ds.item_id,
                        "collection_id": ds.collection_id,
                        "format": ds.format_type.value,
                        "title": ds.title,
                    }
                    for ds in datasets
                ],
                "count": len(datasets),
            },
        )
        output_json_envelope(envelope)
    else:
        if not datasets:
            info("No datasets found")
            return

        for ds in datasets:
            info(f"{ds.collection_id}/{ds.item_id}")
            if ds.title:
                detail(f"  Title: {ds.title}")
            detail(f"  Format: {ds.format_type.value}")


@dataset.command("info")
@click.argument("dataset_id")
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=".",
    help="Path to catalog root (default: current directory).",
)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.pass_context
def dataset_info(
    ctx: click.Context, dataset_id: str, catalog_path: Path, json_output: bool
) -> None:
    """Show information about a dataset.

    DATASET_ID is in the format 'collection/item'.

    Examples:

        portolan dataset info demographics/census

        portolan dataset info imagery/satellite-2024 --json
    """
    use_json = should_output_json(ctx, json_output)

    try:
        ds = get_dataset_info(catalog_path, dataset_id)

        if use_json:
            envelope = success_envelope(
                "dataset_info",
                {
                    "item_id": ds.item_id,
                    "collection_id": ds.collection_id,
                    "format": ds.format_type.value,
                    "bbox": ds.bbox,
                    "assets": ds.asset_paths,
                    "title": ds.title,
                    "description": ds.description,
                },
            )
            output_json_envelope(envelope)
        else:
            info(f"Dataset: {ds.collection_id}/{ds.item_id}")
            if ds.title:
                detail(f"  Title: {ds.title}")
            if ds.description:
                detail(f"  Description: {ds.description}")
            detail(f"  Format: {ds.format_type.value}")
            detail(f"  Bbox: {ds.bbox}")
            if ds.asset_paths:
                detail(f"  Assets: {', '.join(ds.asset_paths)}")
    except KeyError as err:
        if use_json:
            envelope = error_envelope(
                "dataset_info",
                [ErrorDetail(type="KeyError", message=str(err))],
            )
            output_json_envelope(envelope)
        else:
            error(str(err))
        raise SystemExit(1) from err


@dataset.command("remove")
@click.argument("dataset_id")
@click.option(
    "--collection",
    is_flag=True,
    help="Remove entire collection (not just item).",
)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    help="Skip confirmation prompt.",
)
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=".",
    help="Path to catalog root (default: current directory).",
)
@click.pass_context
def dataset_remove(
    ctx: click.Context,
    dataset_id: str,
    collection: bool,
    yes: bool,
    catalog_path: Path,
) -> None:
    """Remove a dataset from the catalog.

    DATASET_ID is in the format 'collection/item' or just 'collection' with --collection.

    Examples:

        portolan dataset remove demographics/census

        portolan dataset remove demographics --collection
    """
    use_json = should_output_json(ctx)

    # Confirm unless --yes (skip confirmation in JSON mode for automation)
    if not yes and not use_json:
        if collection:
            msg = f"Remove entire collection '{dataset_id}'?"
        else:
            msg = f"Remove dataset '{dataset_id}'?"
        if not click.confirm(msg):
            info("Cancelled")
            return

    try:
        remove_dataset(catalog_path, dataset_id, remove_collection=collection)

        if use_json:
            envelope = success_envelope(
                "dataset_remove",
                {
                    "removed": dataset_id,
                    "type": "collection" if collection else "item",
                },
            )
            output_json_envelope(envelope)
        else:
            if collection:
                success(f"Removed collection {dataset_id}")
            else:
                success(f"Removed dataset {dataset_id}")
    except KeyError as err:
        if use_json:
            envelope = error_envelope(
                "dataset_remove",
                [ErrorDetail(type="KeyError", message=str(err))],
            )
            output_json_envelope(envelope)
        else:
            error(str(err))
        raise SystemExit(1) from err


# ─────────────────────────────────────────────────────────────────────────────
# Push command
# ─────────────────────────────────────────────────────────────────────────────


@cli.command()
@click.argument("destination")
@click.option(
    "--collection",
    "-c",
    required=True,
    help="Collection to push (required).",
)
@click.option(
    "--force",
    is_flag=True,
    help="Overwrite remote even if it has diverged.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be pushed without uploading.",
)
@click.option(
    "--profile",
    help="AWS profile name (for S3 destinations).",
)
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=".",
    help="Path to catalog root (default: current directory).",
)
@click.pass_context
def push(
    ctx: click.Context,
    destination: str,
    collection: str,
    force: bool,
    dry_run: bool,
    profile: str | None,
    catalog_path: Path,
) -> None:
    """Push local catalog changes to cloud object storage.

    Syncs a collection's versions to a remote destination (S3, GCS, Azure).
    Uses optimistic locking to detect concurrent modifications.

    DESTINATION is the object store URL (e.g., s3://mybucket/my-catalog).

    \b
    Examples:
        portolan push s3://mybucket/catalog --collection demographics
        portolan push gs://mybucket/catalog -c imagery --dry-run
        portolan push s3://mybucket/catalog -c data --force --profile prod
    """
    from portolan_cli.push import PushConflictError
    from portolan_cli.push import push as push_fn

    use_json = should_output_json(ctx)

    try:
        result = push_fn(
            catalog_root=catalog_path,
            collection=collection,
            destination=destination,
            force=force,
            dry_run=dry_run,
            profile=profile,
        )

        if use_json:
            envelope = success_envelope(
                "push",
                {
                    "files_uploaded": result.files_uploaded,
                    "versions_pushed": result.versions_pushed,
                    "conflicts": result.conflicts,
                    "errors": result.errors,
                },
            )
            output_json_envelope(envelope)
        else:
            if result.success:
                if result.versions_pushed > 0:
                    success(
                        f"Pushed {result.versions_pushed} version(s), {result.files_uploaded} file(s)"
                    )
                else:
                    info("Nothing to push - local and remote are in sync")
            else:
                for err_msg in result.errors:
                    error(err_msg)
                raise SystemExit(1)

    except PushConflictError as err:
        if use_json:
            envelope = error_envelope(
                "push",
                [ErrorDetail(type="PushConflictError", message=str(err))],
            )
            output_json_envelope(envelope)
        else:
            error(f"Push conflict: {err}")
            info("Use --force to overwrite, or pull remote changes first")
        raise SystemExit(1) from err

    except FileNotFoundError as err:
        if use_json:
            envelope = error_envelope(
                "push",
                [ErrorDetail(type="FileNotFoundError", message=str(err))],
            )
            output_json_envelope(envelope)
        else:
            error(str(err))
        raise SystemExit(1) from err

    except ValueError as err:
        if use_json:
            envelope = error_envelope(
                "push",
                [ErrorDetail(type="ValueError", message=str(err))],
            )
            output_json_envelope(envelope)
        else:
            error(str(err))
        raise SystemExit(1) from err


# ─────────────────────────────────────────────────────────────────────────────
# Pull command
# ─────────────────────────────────────────────────────────────────────────────


@cli.command()
@click.argument("remote_url")
@click.option(
    "--collection",
    "-c",
    required=True,
    help="Collection to pull.",
)
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=".",
    help="Path to local catalog root (default: current directory).",
)
@click.option(
    "--force",
    is_flag=True,
    help="Discard uncommitted local changes and overwrite with remote.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be downloaded without actually downloading.",
)
@click.option(
    "--profile",
    type=str,
    default=None,
    help="AWS profile name (for S3).",
)
@click.pass_context
def pull_command(
    ctx: click.Context,
    remote_url: str,
    collection: str,
    catalog_path: Path,
    force: bool,
    dry_run: bool,
    profile: str | None,
) -> None:
    """Pull updates from a remote catalog.

    Fetches changes from a remote catalog and downloads updated files.
    Similar to `git pull`, this checks for uncommitted local changes before
    overwriting.

    REMOTE_URL is the remote catalog URL (e.g., s3://bucket/catalog).

    \b
    Examples:
        portolan pull s3://mybucket/my-catalog --collection demographics
        portolan pull s3://mybucket/catalog -c imagery --dry-run
        portolan pull s3://bucket/catalog -c data --force
        portolan pull s3://bucket/catalog -c data --profile myprofile
    """
    from portolan_cli.pull import pull as pull_fn

    use_json = should_output_json(ctx)

    result = pull_fn(
        remote_url=remote_url,
        local_root=catalog_path,
        collection=collection,
        force=force,
        dry_run=dry_run,
        profile=profile,
    )

    if use_json:
        data = {
            "files_downloaded": result.files_downloaded,
            "files_skipped": result.files_skipped,
            "local_version": result.local_version,
            "remote_version": result.remote_version,
            "up_to_date": result.up_to_date,
        }

        if result.success:
            envelope = success_envelope("pull", data)
        else:
            errors = []
            if result.uncommitted_changes:
                errors.append(
                    ErrorDetail(
                        type="UncommittedChangesError",
                        message=f"Uncommitted changes: {', '.join(result.uncommitted_changes)}",
                    )
                )
            else:
                errors.append(ErrorDetail(type="PullError", message="Pull failed"))
            envelope = error_envelope("pull", errors, data=data)

        output_json_envelope(envelope)
    else:
        # Human-readable output
        if result.up_to_date:
            info("Already up to date")
        elif result.success:
            if dry_run:
                info(f"[DRY RUN] Would pull {result.files_downloaded} file(s)")
            else:
                success(f"Pulled {result.files_downloaded} file(s)")
                detail(f"  Local: {result.local_version} -> {result.remote_version}")
        else:
            if result.uncommitted_changes:
                error("Pull blocked by uncommitted changes:")
                for filename in result.uncommitted_changes:
                    detail(f"  {filename}")
                warn("Use --force to discard local changes")
            else:
                error("Pull failed")

    if not result.success:
        raise SystemExit(1)


# ─────────────────────────────────────────────────────────────────────────────
# Sync command
# ─────────────────────────────────────────────────────────────────────────────


@cli.command()
@click.argument("destination")
@click.option(
    "--collection",
    "-c",
    required=True,
    help="Collection to sync (required).",
)
@click.option(
    "--force",
    is_flag=True,
    help="Overwrite conflicts on both pull and push.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would happen without making changes.",
)
@click.option(
    "--fix",
    is_flag=True,
    help="Convert non-cloud-native formats during check.",
)
@click.option(
    "--profile",
    help="AWS profile name (for S3 destinations).",
)
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=".",
    help="Path to catalog root (default: current directory).",
)
@click.pass_context
def sync(
    ctx: click.Context,
    destination: str,
    collection: str,
    force: bool,
    dry_run: bool,
    fix: bool,
    profile: str | None,
    catalog_path: Path,
) -> None:
    """Sync local catalog with remote storage (pull + push).

    Orchestrates a full sync workflow: Pull -> Init -> Scan -> Check -> Push.
    This is the recommended way to keep a local catalog in sync with remote.

    DESTINATION is the object store URL (e.g., s3://mybucket/my-catalog).

    \b
    Examples:
        portolan sync s3://mybucket/catalog --collection demographics
        portolan sync s3://mybucket/catalog -c imagery --dry-run
        portolan sync s3://mybucket/catalog -c data --fix --force
        portolan sync s3://mybucket/catalog -c data --profile prod
    """
    from portolan_cli.sync import sync as sync_fn

    use_json = should_output_json(ctx)

    result = sync_fn(
        catalog_root=catalog_path,
        collection=collection,
        destination=destination,
        force=force,
        dry_run=dry_run,
        fix=fix,
        profile=profile,
    )

    if use_json:
        data: dict[str, Any] = {
            "init_performed": result.init_performed,
            "errors": result.errors,
        }

        # Include pull results if available
        if result.pull_result is not None:
            data["pull"] = {
                "files_downloaded": result.pull_result.files_downloaded,
                "files_skipped": result.pull_result.files_skipped,
                "up_to_date": result.pull_result.up_to_date,
                "local_version": result.pull_result.local_version,
                "remote_version": result.pull_result.remote_version,
            }

        # Include push results if available
        if result.push_result is not None:
            data["push"] = {
                "files_uploaded": result.push_result.files_uploaded,
                "versions_pushed": result.push_result.versions_pushed,
                "conflicts": result.push_result.conflicts,
            }

        if result.success:
            envelope = success_envelope("sync", data)
        else:
            errors = [ErrorDetail(type="SyncError", message=err_msg) for err_msg in result.errors]
            envelope = error_envelope("sync", errors, data=data)

        output_json_envelope(envelope)
    else:
        # Human-readable output is already printed by sync()
        # Just handle the final status
        if result.success:
            if dry_run:
                info("[DRY RUN] Sync completed successfully")
            else:
                success("Sync completed successfully")
        else:
            # Errors already logged by sync() - just emit final status
            error("Sync failed")

    if not result.success:
        raise SystemExit(1)


# ─────────────────────────────────────────────────────────────────────────────
# Clone command
# ─────────────────────────────────────────────────────────────────────────────


@cli.command()
@click.argument("remote_url")
@click.argument(
    "local_path",
    type=click.Path(path_type=Path),
)
@click.option(
    "--collection",
    "-c",
    required=True,
    help="Collection to clone (required).",
)
@click.option(
    "--profile",
    help="AWS profile name (for S3 sources).",
)
@click.pass_context
def clone(
    ctx: click.Context,
    remote_url: str,
    local_path: Path,
    collection: str,
    profile: str | None,
) -> None:
    """Clone a remote catalog to a local directory.

    This is essentially "pull to an empty directory" with guardrails.
    Creates the target directory and pulls the specified collection.

    REMOTE_URL is the object store URL (e.g., s3://mybucket/my-catalog).
    LOCAL_PATH is the directory to clone into (will be created).

    \b
    Examples:
        portolan clone s3://mybucket/catalog ./local --collection demographics
        portolan clone s3://mybucket/catalog ./data -c imagery --profile prod
    """
    from portolan_cli.sync import clone as clone_fn

    use_json = should_output_json(ctx)

    result = clone_fn(
        remote_url=remote_url,
        local_path=local_path,
        collection=collection,
        profile=profile,
    )

    if use_json:
        data: dict[str, Any] = {
            "local_path": str(result.local_path),
        }

        if result.pull_result is not None:
            data["pull"] = {
                "files_downloaded": result.pull_result.files_downloaded,
                "remote_version": result.pull_result.remote_version,
            }

        if result.success:
            envelope = success_envelope("clone", data)
        else:
            errors = [
                ErrorDetail(type="CloneError", message=err, code="CLONE_FAILED")
                for err in result.errors
            ]
            envelope = error_envelope("clone", errors)

        click.echo(json.dumps(envelope, indent=2))
    else:
        if result.success:
            success(f"Clone completed: {result.local_path}")
        else:
            if result.errors:
                for err_msg in result.errors:
                    error(err_msg)
            error("Clone failed")

    if not result.success:
        raise SystemExit(1)
