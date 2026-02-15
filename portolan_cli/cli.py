"""Portolan CLI - Command-line interface for managing cloud-native geospatial data.

The CLI is a thin wrapper around the Python API (see catalog.py).
All business logic lives in the library; the CLI handles user interaction.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import click

from portolan_cli.catalog import CatalogExistsError
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
    is_flag=True,
    default=False,
    help="Skip interactive prompts and use auto-extracted/default values.",
)
@click.pass_context
def init(ctx: click.Context, path: Path, auto: bool) -> None:
    """Initialize a new Portolan catalog.

    Creates a .portolan directory with a STAC catalog.json file.
    Auto-extracts the catalog ID from the directory name and generates timestamps.

    PATH is the directory where the catalog should be created (default: current directory).

    Use --auto to skip all prompts and use default values. Warnings will be emitted
    for missing best-practice fields (title, description).
    """
    from portolan_cli.catalog import create_catalog, write_catalog_json
    from portolan_cli.errors import CatalogAlreadyExistsError

    use_json = should_output_json(ctx)

    try:
        # Use the new CatalogModel-based creation
        result = create_catalog(path, auto=auto, return_warnings=True)
        catalog, warnings = result
        write_catalog_json(catalog, path)

        if use_json:
            envelope = success_envelope(
                "init",
                {
                    "path": str(path.resolve()),
                    "catalog_file": ".portolan/catalog.json",
                    "catalog_id": catalog.id,
                    "warnings": warnings,
                },
            )
            output_json_envelope(envelope)
        else:
            success(f"Initialized Portolan catalog in {path.resolve()}")
            info(f"Catalog ID: {catalog.id}")
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
            error(f"Catalog already exists at {path.resolve()}")
        raise SystemExit(1) from err
    except CatalogExistsError as err:
        # Legacy error handling for backward compatibility
        if use_json:
            envelope = error_envelope(
                "init",
                [ErrorDetail(type="CatalogExistsError", message=str(err))],
            )
            output_json_envelope(envelope)
        else:
            error(f"Catalog already exists at {path.resolve()}")
        raise SystemExit(1) from err


def _output_check_json(report: Any) -> None:
    """Output check results as JSON envelope."""
    data = report.to_dict()
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


@cli.command()
@click.argument("path", type=click.Path(path_type=Path), default=".")
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON")
@click.option("--verbose", "-v", is_flag=True, help="Show all validation rules, not just failures")
@click.pass_context
def check(ctx: click.Context, path: Path, json_output: bool, verbose: bool) -> None:
    """Validate a Portolan catalog.

    Runs validation rules against the catalog and reports any issues.

    PATH is the directory containing the .portolan catalog (default: current directory).
    """
    use_json = should_output_json(ctx, json_output)

    # Validate path exists (handle in code for JSON envelope support)
    if not path.exists():
        if use_json:
            envelope = error_envelope(
                "check",
                [ErrorDetail(type="PathNotFoundError", message=f"Path does not exist: {path}")],
            )
            output_json_envelope(envelope)
        else:
            error(f"Path does not exist: {path}")
        raise SystemExit(1)

    report = validate_catalog(path)

    if use_json:
        _output_check_json(report)
    else:
        # Human-readable output
        for result in report.results:
            if verbose or not result.passed:
                _print_validation_result(result)
        _print_check_summary(report)

    # Exit code: 1 if any errors (not warnings)
    if report.errors:
        raise SystemExit(1)


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
