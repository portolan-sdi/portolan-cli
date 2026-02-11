"""Portolan CLI - Command-line interface for managing cloud-native geospatial data.

The CLI is a thin wrapper around the Python API (see catalog.py).
All business logic lives in the library; the CLI handles user interaction.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import click

from portolan_cli.catalog import Catalog, CatalogExistsError
from portolan_cli.dataset import (
    add_dataset,
    get_dataset_info,
    list_datasets,
    remove_dataset,
)
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
from portolan_cli.validation import Severity
from portolan_cli.validation import check as validate_catalog


@click.group()
@click.version_option()
def cli() -> None:
    """Portolan - Publish and manage cloud-native geospatial data catalogs."""
    pass


@cli.command()
@click.argument("path", type=click.Path(path_type=Path), default=".")
def init(path: Path) -> None:
    """Initialize a new Portolan catalog.

    Creates a .portolan directory with a STAC catalog.json file.

    PATH is the directory where the catalog should be created (default: current directory).
    """
    try:
        Catalog.init(path)
        success(f"Initialized Portolan catalog in {path.resolve()}")
    except CatalogExistsError as err:
        error(f"Catalog already exists at {path.resolve()}")
        raise SystemExit(1) from err


@cli.command()
@click.argument("path", type=click.Path(path_type=Path, exists=True), default=".")
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON")
@click.option("--verbose", "-v", is_flag=True, help="Show all validation rules, not just failures")
def check(path: Path, json_output: bool, verbose: bool) -> None:
    """Validate a Portolan catalog.

    Runs validation rules against the catalog and reports any issues.

    PATH is the directory containing the .portolan catalog (default: current directory).
    """
    import json

    report = validate_catalog(path)

    if json_output:
        click.echo(json.dumps(report.to_dict(), indent=2))
    else:
        # Human-readable output
        if verbose:
            # Show all rules
            for result in report.results:
                if result.passed:
                    success(f"{result.rule_name}: {result.message}")
                elif result.severity == Severity.ERROR:
                    error(f"{result.rule_name}: {result.message}")
                    if result.fix_hint:
                        detail(f"  Hint: {result.fix_hint}")
                elif result.severity == Severity.WARNING:
                    warn(f"{result.rule_name}: {result.message}")
                    if result.fix_hint:
                        detail(f"  Hint: {result.fix_hint}")
                else:
                    info(f"{result.rule_name}: {result.message}")
        else:
            # Show only failures
            for result in report.results:
                if not result.passed:
                    if result.severity == Severity.ERROR:
                        error(f"{result.rule_name}: {result.message}")
                    elif result.severity == Severity.WARNING:
                        warn(f"{result.rule_name}: {result.message}")
                    else:
                        info(f"{result.rule_name}: {result.message}")
                    if result.fix_hint:
                        detail(f"  Hint: {result.fix_hint}")

        # Summary
        if report.passed:
            success("All validation checks passed")
        else:
            error_count = len(report.errors)
            warning_count = len(report.warnings)
            parts = []
            if error_count:
                parts.append(f"{error_count} error{'s' if error_count != 1 else ''}")
            if warning_count:
                parts.append(f"{warning_count} warning{'s' if warning_count != 1 else ''}")
            error(f"Validation failed: {', '.join(parts)}")

    # Exit code: 1 if any errors (not warnings)
    if report.errors:
        raise SystemExit(1)


# ─────────────────────────────────────────────────────────────────────────────
# Scan command
# ─────────────────────────────────────────────────────────────────────────────


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
def scan(
    path: Path,
    json_output: bool,
    no_recursive: bool,
    max_depth: int | None,
    include_hidden: bool,
    follow_symlinks: bool,
) -> None:
    """Scan a directory for geospatial files and potential issues.

    Discovers files by extension, validates shapefile completeness,
    and reports issues that may cause problems during import.

    PATH is the directory to scan.

    Examples:

        portolan scan /data/geospatial

        portolan scan . --json

        portolan scan /large/tree --max-depth=2

        portolan scan /data --no-recursive
    """
    import json as json_module

    # Build options from CLI flags
    options = ScanOptions(
        recursive=not no_recursive,
        max_depth=max_depth,
        include_hidden=include_hidden,
        follow_symlinks=follow_symlinks,
    )

    try:
        result = scan_directory(path, options)
    except FileNotFoundError as err:
        error(str(err))
        raise SystemExit(1) from err
    except NotADirectoryError as err:
        error(str(err))
        raise SystemExit(1) from err

    if json_output:
        # JSON output per FR-019
        click.echo(json_module.dumps(result.to_dict(), indent=2))
    else:
        # Human-readable output per FR-018
        _print_scan_summary(result)

    # Exit code per FR-020: 0 if no errors, 1 if errors exist
    if result.has_errors:
        raise SystemExit(1)


def _print_scan_header(result: ScanResult) -> None:
    """Print scan header with file counts."""
    ready_count = len(result.ready)
    if ready_count == 0:
        info(f"Scanned {result.directories_scanned} directories")
        warn("0 files ready to import")
    else:
        success(f"{ready_count} files ready to import")


def _print_format_breakdown(result: ScanResult) -> None:
    """Print breakdown of files by format."""
    if not result.ready:
        return
    formats: dict[str, int] = {}
    for f in result.ready:
        formats[f.extension] = formats.get(f.extension, 0) + 1
    for ext, count in sorted(formats.items()):
        detail(f"  {count} {ext} file{'s' if count != 1 else ''}")


def _print_issue_group(
    issues: list[ScanIssue],
    severity: ScanSeverity,
    header_fn: Callable[[str], None],
    count: int,
    label: str,
) -> None:
    """Print a group of issues with the same severity."""
    if count == 0:
        return
    header_fn(f"{count} {label}{'s' if count != 1 else ''}")
    for issue in issues:
        if issue.severity == severity:
            header_fn(f"  {issue.relative_path}: {issue.message}")
            if issue.suggestion:
                detail(f"    Hint: {issue.suggestion}")


def _print_issues_by_severity(result: ScanResult) -> None:
    """Print issues grouped by severity."""
    if not result.issues:
        return

    _print_issue_group(result.issues, ScanSeverity.ERROR, error, result.error_count, "error")
    _print_issue_group(result.issues, ScanSeverity.WARNING, warn, result.warning_count, "warning")
    _print_issue_group(result.issues, ScanSeverity.INFO, info, result.info_count, "info message")


def _print_scan_summary(result: ScanResult) -> None:
    """Print human-readable scan summary."""
    _print_scan_header(result)
    _print_format_breakdown(result)
    _print_issues_by_severity(result)

    if result.skipped:
        detail(f"{len(result.skipped)} files skipped (unrecognized format)")


# ─────────────────────────────────────────────────────────────────────────────
# Dataset commands
# ─────────────────────────────────────────────────────────────────────────────


@cli.group()
def dataset() -> None:
    """Manage datasets in the catalog."""
    pass


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
def dataset_add(
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
    try:
        result = add_dataset(
            path=path,
            catalog_root=catalog_path,
            collection_id=collection,
            title=title,
            description=description,
            item_id=item_id,
        )
        success(f"Added {result.item_id} to collection {result.collection_id}")
        if result.title:
            detail(f"  Title: {result.title}")
        detail(f"  Format: {result.format_type.value}")
        detail(f"  Bbox: {result.bbox}")
    except ValueError as err:
        error(str(err))
        raise SystemExit(1) from err
    except FileNotFoundError as err:
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
def dataset_list(collection: str | None, catalog_path: Path) -> None:
    """List datasets in the catalog.

    Examples:

        portolan dataset list

        portolan dataset list --collection demographics
    """
    datasets = list_datasets(catalog_path, collection_id=collection)

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
def dataset_info(dataset_id: str, catalog_path: Path, json_output: bool) -> None:
    """Show information about a dataset.

    DATASET_ID is in the format 'collection/item'.

    Examples:

        portolan dataset info demographics/census

        portolan dataset info imagery/satellite-2024 --json
    """
    import json as json_module

    try:
        ds = get_dataset_info(catalog_path, dataset_id)
    except KeyError as err:
        error(str(err))
        raise SystemExit(1) from err

    if json_output:
        data = {
            "item_id": ds.item_id,
            "collection_id": ds.collection_id,
            "format": ds.format_type.value,
            "bbox": ds.bbox,
            "assets": ds.asset_paths,
            "title": ds.title,
            "description": ds.description,
        }
        click.echo(json_module.dumps(data, indent=2))
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
def dataset_remove(
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
    # Confirm unless --yes
    if not yes:
        if collection:
            msg = f"Remove entire collection '{dataset_id}'?"
        else:
            msg = f"Remove dataset '{dataset_id}'?"
        if not click.confirm(msg):
            info("Cancelled")
            return

    try:
        remove_dataset(catalog_path, dataset_id, remove_collection=collection)
        if collection:
            success(f"Removed collection {dataset_id}")
        else:
            success(f"Removed dataset {dataset_id}")
    except KeyError as err:
        error(str(err))
        raise SystemExit(1) from err
