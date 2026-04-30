"""Portolan CLI - Command-line interface for managing cloud-native geospatial data.

The CLI is a thin wrapper around the Python API (see catalog.py).
All business logic lives in the library; the CLI handles user interaction.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn

if TYPE_CHECKING:
    from portolan_cli.backends.protocol import VersioningBackend
    from portolan_cli.extract.arcgis.report import ExtractionReport
    from portolan_cli.pull import PullResult

import click

from portolan_cli.add_progress import AddProgressReporter, count_files
from portolan_cli.catalog import find_catalog_root
from portolan_cli.catalog_list import (
    AssetStatus,
    CatalogListResult,
    list_catalog_contents,
)
from portolan_cli.check import check_directory
from portolan_cli.convert import ConversionResult
from portolan_cli.dataset import (
    AddFailure,
    DatasetInfo,
    add_files,
    get_sidecars,
    remove_files,
    resolve_collection_id,
)
from portolan_cli.json_output import ErrorDetail, error_envelope, success_envelope
from portolan_cli.metadata import fix_metadata
from portolan_cli.metadata.fix import FixReport
from portolan_cli.output import detail, error, success, warn
from portolan_cli.output import info as info_output
from portolan_cli.scan import (
    IssueType,
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
    format_fix_commands_json,
    generate_next_steps,
    get_category_display_name,
    get_fixability,
    group_skipped_files,
    render_tree_view,
)
from portolan_cli.scan_progress import ScanProgressReporter, count_directories
from portolan_cli.temporal import FLEXIBLE_DATETIME
from portolan_cli.validation import (
    Severity,
)
from portolan_cli.validation import check as validate_catalog


def format_size(size_bytes: int) -> str:
    """Format a file size in human-readable format.

    Args:
        size_bytes: Size in bytes.

    Returns:
        Human-readable size string (e.g., "4.2MB", "100B").
    """
    if size_bytes < 1024:
        return f"{size_bytes}B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f}KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f}MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f}GB"


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


def load_dotenv_and_warn_sensitive(catalog_path: Path) -> None:
    """Load .env from catalog and warn if config.yaml has sensitive settings.

    This should be called after the actual catalog path is resolved (not in
    the global CLI callback) to ensure the correct .env is loaded when
    --catalog/--portolan-dir overrides the default.

    Args:
        catalog_path: Resolved catalog root path.
    """
    from portolan_cli.config import check_sensitive_settings_in_config, load_dotenv_from_catalog

    load_dotenv_from_catalog(catalog_path)

    sensitive_in_config = check_sensitive_settings_in_config(catalog_path)
    if sensitive_in_config:
        settings_str = ", ".join(sensitive_in_config)
        warn(
            f"config.yaml contains sensitive settings ({settings_str}) that will be "
            f"pushed to remote. Move these to .env file or use PORTOLAN_* env vars."
        )


def require_catalog_root(
    use_json: bool = False,
    command_name: str = "command",
) -> Path:
    """Find and validate catalog root, or exit with git-style error.

    Git-style behavior: walks up from cwd to find .portolan/config.yaml.
    Commands that need to operate on the entire catalog use this to find
    the root regardless of which subdirectory they're run from.

    Args:
        use_json: If True, output error as JSON envelope.
        command_name: Name of the command for error messages.

    Returns:
        Path to catalog root.

    Raises:
        SystemExit: If not inside a catalog.
    """
    catalog_root = find_catalog_root()
    if catalog_root is None:
        msg = "fatal: not a portolan catalog (or any parent up to mount point)"
        if use_json:
            envelope = error_envelope(
                command_name,
                [ErrorDetail(type="NotACatalogError", message=msg)],
            )
            output_json_envelope(envelope)
        else:
            error(msg)
        raise SystemExit(1)
    return catalog_root


def _collection_path(catalog_path: Path | None, collection: str | None) -> Path | None:
    """Compute collection folder path for hierarchical config (ADR-0039)."""
    return catalog_path / collection if catalog_path and collection else None


def resolve_remote(
    destination: str | None,
    catalog_path: Path | None,
    collection: str | None = None,
) -> str | None:
    """Resolve remote destination with precedence: CLI > env var > config.

    Args:
        destination: CLI-provided destination value (None if not specified).
        catalog_path: Path to catalog root for config lookup.
        collection: Optional collection name for collection-level config.

    Returns:
        Resolved destination URL or None if not configured.
    """
    from portolan_cli.config import get_setting

    return get_setting(
        "remote",
        cli_value=destination,
        catalog_path=catalog_path,
        collection=collection,
        collection_path=_collection_path(catalog_path, collection),
    )


def resolve_aws_profile(
    profile: str | None,
    catalog_path: Path | None,
    collection: str | None = None,
) -> str:
    """Resolve AWS profile with precedence: CLI > env var > config > default.

    Args:
        profile: CLI-provided profile value (None if not specified).
        catalog_path: Path to catalog root for config lookup.
        collection: Optional collection name for collection-level config.

    Returns:
        Resolved profile name (defaults to "default" if nothing configured).
    """
    from portolan_cli.config import get_setting

    resolved = get_setting(
        "aws_profile",
        cli_value=profile,
        catalog_path=catalog_path,
        collection=collection,
        collection_path=_collection_path(catalog_path, collection),
    )
    return resolved if resolved is not None else "default"


def resolve_aws_region(
    region: str | None,
    catalog_path: Path | None,
    collection: str | None = None,
) -> str | None:
    """Resolve AWS region with precedence: CLI > env var > config.

    Args:
        region: CLI-provided region value (None if not specified).
        catalog_path: Path to catalog root for config lookup.
        collection: Optional collection name for collection-level config.

    Returns:
        Resolved region name or None if not configured.
    """
    from portolan_cli.config import get_setting

    return get_setting(
        "region",
        cli_value=region,
        catalog_path=catalog_path,
        collection=collection,
        collection_path=_collection_path(catalog_path, collection),
    )


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

    # Note: .env loading moved to individual commands after catalog path resolution
    # to ensure correct .env is loaded when --catalog/--portolan-dir is used.
    # See load_dotenv_and_warn_sensitive() helper.
    pass


@cli.command()
@click.argument("path", type=click.Path(path_type=Path), default=".")
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
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
@click.option(
    "--backend",
    type=str,
    default="file",
    help="Versioning backend to use (e.g., 'file', 'iceberg').",
)
@click.pass_context
def init(
    ctx: click.Context,
    path: Path,
    json_output: bool,
    auto_mode: bool,
    title: str | None,
    description: str | None,
    backend: str,
) -> None:
    """Initialize a new Portolan catalog.

    Creates a catalog.json at the root level and a .portolan directory with
    management files (config.yaml). Also creates versions.json at the root.

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
        portolan init --backend iceberg     # Use Iceberg backend
    """

    from portolan_cli.catalog import init_catalog
    from portolan_cli.errors import CatalogAlreadyExistsError, UnmanagedStacCatalogError

    use_json = should_output_json(ctx, json_output)

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
            backend=backend,
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
            info_output(f"Catalog ID: {catalog_id}")
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
            info_output(
                "Use 'portolan adopt' to bring it under Portolan management (not yet implemented)"
            )
        raise SystemExit(1) from err


# =============================================================================
# List command (top-level, ADR-0022)
# =============================================================================


def _get_format_display_name(format_type: Any) -> str:
    """Get human-readable format name.

    Args:
        format_type: FormatType enum value.

    Returns:
        Human-readable format name (e.g., "GeoParquet", "COG").
    """
    from portolan_cli.formats import FormatType

    if format_type == FormatType.VECTOR:
        return "GeoParquet"
    elif format_type == FormatType.RASTER:
        return "COG"
    else:
        return "Unknown"


def _get_asset_format_display_name(asset_href: str) -> str:
    """Get human-readable format name for an individual asset based on extension.

    Uses FORMAT_DISPLAY_NAMES from formats.py for known geospatial extensions,
    falls back to the uppercase extension for unknown types.

    Args:
        asset_href: Asset href (e.g., "./data.parquet", "./thumb.png").

    Returns:
        Human-readable format name (e.g., "GeoParquet", "PMTiles", "PNG").
    """
    from portolan_cli.formats import FORMAT_DISPLAY_NAMES

    ext = Path(asset_href).suffix.lower()
    if ext in FORMAT_DISPLAY_NAMES:
        return FORMAT_DISPLAY_NAMES[ext]
    if ext:
        return ext.upper().lstrip(".")
    return "Unknown"


def _get_asset_file_size(
    catalog_path: Path, collection_id: str, item_id: str, asset_href: str
) -> int | None:
    """Get the file size for an asset.

    Args:
        catalog_path: Path to catalog root.
        collection_id: Collection ID.
        item_id: Item ID (subdirectory under collection).
        asset_href: Asset href (relative to item directory).

    Returns:
        File size in bytes, or None if file not found.
    """
    # Asset hrefs are relative to the item directory: collection/item/
    # The href looks like "./data.parquet" or just "data.parquet"
    clean_href = asset_href.removeprefix("./")

    # Full path: catalog_root / collection / item / asset
    asset_path = catalog_path / collection_id / item_id / clean_href

    if asset_path.exists():
        return asset_path.stat().st_size

    return None


def _apply_list_status_filter(
    result: CatalogListResult,
    tracked_only: bool,
    untracked_only: bool,
) -> None:
    """Apply status filters to catalog list result in-place.

    Args:
        result: CatalogListResult to filter (modified in-place).
        tracked_only: If True, keep only tracked assets.
        untracked_only: If True, keep only untracked assets.
    """
    if not (tracked_only or untracked_only):
        return

    for col in result.collections:
        for item in col.items:
            if tracked_only:
                item.assets = [a for a in item.assets if a.status == AssetStatus.TRACKED]
            elif untracked_only:
                item.assets = [a for a in item.assets if a.status == AssetStatus.UNTRACKED]
        # Remove empty items after filtering
        col.items = [item for item in col.items if item.assets]
    # Remove empty collections after filtering
    result.collections = [col for col in result.collections if col.items]


def _list_tree_output_with_status(result: CatalogListResult) -> None:
    """Output items in hierarchical tree view with status indicators.

    Shows ALL files per item, grouped under their parent item directory
    with status counts. Per issue #210, everything in a catalog is tracked.

    Format:
        censo-2010/
            data/ (3 tracked, 2 untracked)
              + census-data.parquet (GeoParquet, 4.5MB)
              + metadata.parquet (GeoParquet, 1.2MB)
              + README.md (2KB)
              + style.json (1KB)

    Status indicators:
        + = tracked (in versions.json, unchanged)
        + = untracked (on disk, not in versions.json)
        ~ = modified (in versions.json, checksum changed)
        ! = deleted (in versions.json, missing from disk)

    Args:
        result: CatalogListResult with all collections and items.
    """
    # Status indicator symbols and colors
    status_symbols = {
        AssetStatus.TRACKED: "\u2713",  # Checkmark
        AssetStatus.UNTRACKED: "+",
        AssetStatus.MODIFIED: "~",
        AssetStatus.DELETED: "!",
    }

    for col in result.collections:
        # Print collection header
        info_output(f"{col.collection_id}/")

        for item in col.items:
            # Build status summary
            parts = []
            if item.tracked_count > 0:
                parts.append(f"{item.tracked_count} tracked")
            if item.untracked_count > 0:
                parts.append(f"{item.untracked_count} untracked")
            if item.modified_count > 0:
                parts.append(f"{item.modified_count} modified")
            if item.deleted_count > 0:
                parts.append(f"{item.deleted_count} deleted")

            status_summary = ", ".join(parts) if parts else "empty"
            detail(f"  {item.item_id}/ ({status_summary})")

            # List each asset with status indicator
            for asset in item.assets:
                symbol = status_symbols.get(asset.status, "?")
                format_name = asset.format_name or "Unknown"

                # Build size string
                size_str = ""
                if asset.size_bytes is not None:
                    size_str = f", {format_size(asset.size_bytes)}"

                # Add status label for non-tracked
                status_label = ""
                if asset.status == AssetStatus.MODIFIED:
                    status_label = ", modified"
                elif asset.status == AssetStatus.DELETED:
                    status_label = ", deleted"

                detail(f"    {symbol} {asset.path} ({format_name}{size_str}{status_label})")


@cli.command("list")
@click.option(
    "--collection",
    "-c",
    help="Filter by collection ID.",
)
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to catalog root (default: auto-detect by walking up from cwd).",
)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.option(
    "--tracked-only",
    is_flag=True,
    help="Show only tracked files (hide untracked).",
)
@click.option(
    "--untracked-only",
    is_flag=True,
    help="Show only untracked files.",
)
@click.pass_context
def list_cmd(
    ctx: click.Context,
    collection: str | None,
    catalog_path: Path | None,
    json_output: bool,
    tracked_only: bool,
    untracked_only: bool,
) -> None:
    """List all files in the catalog with tracking status.

    Git-style behavior: automatically finds the catalog root by walking up
    from the current directory. Works from any subdirectory within a catalog.
    Use --catalog to override and specify an explicit path.

    Shows all files organized by collection in a hierarchical tree view.
    Each file shows its tracking status, format type, and file size.

    \b
    Status indicators:
        + = tracked (in versions.json, unchanged)
        + = untracked (on disk, not in versions.json)
        ~ = modified (in versions.json, checksum changed)
        ! = deleted (in versions.json, missing from disk)

    \b
    Example output:
        censo-2010/
            data/ (3 tracked, 2 untracked)
              + census-data.parquet (GeoParquet, 4.5MB)
              + metadata.parquet (GeoParquet, 1.2MB)
              + README.md (2KB)
              + style.json (1KB)

    \b
    Examples:
        portolan list                           # List all files with status
        portolan list --collection demographics # Filter by collection
        portolan list --tracked-only            # Show only tracked files
        portolan list --untracked-only          # Show only untracked files
        portolan list --json                    # JSON output
    """
    use_json = should_output_json(ctx, json_output)

    # Git-style: find catalog root from anywhere within the catalog
    # Use explicit --catalog if provided, otherwise auto-detect
    if catalog_path is None:
        catalog_path = require_catalog_root(use_json, "list")

    # Get all catalog contents with status
    result = list_catalog_contents(catalog_path, collection_id=collection)

    # Apply status filters (modifies result in-place)
    _apply_list_status_filter(result, tracked_only, untracked_only)

    if use_json:
        # Build JSON output with status information
        collections_data = []
        for col in result.collections:
            items_data = []
            for item in col.items:
                assets_data = [
                    {
                        "path": a.path,
                        "status": a.status.value,
                        "format": a.format_name,
                        "size": a.size_bytes,
                    }
                    for a in item.assets
                ]
                items_data.append(
                    {
                        "id": item.item_id,
                        "tracked": item.tracked_count,
                        "untracked": item.untracked_count,
                        "modified": item.modified_count,
                        "deleted": item.deleted_count,
                        "assets": assets_data,
                    }
                )
            collections_data.append(
                {
                    "id": col.collection_id,
                    "is_initialized": col.is_initialized,
                    "items": items_data,
                }
            )

        envelope = success_envelope(
            "list",
            {
                "collections": collections_data,
                "summary": {
                    "total_tracked": result.total_tracked,
                    "total_untracked": result.total_untracked,
                    "total_modified": result.total_modified,
                    "total_deleted": result.total_deleted,
                },
            },
        )
        output_json_envelope(envelope)
    else:
        if result.is_empty():
            info_output("No tracked items")
            info_output("")
            detail("To get started:")
            detail("  portolan scan .      Discover files in this directory")
            detail("  portolan add <path>  Track a specific file or directory")
            return

        _list_tree_output_with_status(result)


# =============================================================================
# Info command (top-level, ADR-0022)
# =============================================================================


@cli.command("info")
@click.argument("target", type=click.Path(path_type=Path), required=False)
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=".",
    help="Path to catalog root (default: current directory).",
)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.pass_context
def info_cmd(
    ctx: click.Context,
    target: Path | None,
    catalog_path: Path,
    json_output: bool,
) -> None:
    """Show information about a file, collection, or catalog.

    TARGET can be:
    - A file path (e.g., demographics/census.parquet) - shows file metadata
    - A collection directory (e.g., demographics/) - shows collection metadata
    - Omitted - shows catalog-level metadata

    Per ADR-0022, the output format for files is:
        Format: GeoParquet
        CRS: EPSG:4326
        Bbox: [-122.5, 37.7, -122.3, 37.9]
        Features: 4,231
        Version: v1.2.0

    \b
    Examples:
        portolan info demographics/census.parquet  # File info
        portolan info demographics/                # Collection info
        portolan info                              # Catalog info
        portolan info demographics/census.parquet --json  # JSON output
    """
    from portolan_cli.inspect import (
        inspect_catalog,
        inspect_collection,
        inspect_file,
    )

    use_json = should_output_json(ctx, json_output)

    try:
        if target is None:
            # Catalog-level info
            catalog_result = inspect_catalog(catalog_path)
            _output_catalog_info(catalog_result, use_json=use_json)
        elif target.is_file():
            # File-level info
            file_result = inspect_file(target, catalog_root=catalog_path)
            _output_file_info(file_result, use_json=use_json)
        elif target.is_dir():
            # Check what type of directory this is based on its contents.
            # STAC structure is self-describing: catalog.json vs collection.json.
            # Per ADR-0032 Pattern 2, a directory CAN have both (e.g., a collection
            # with sub-catalogs organizing items). We prefer catalog.json since it
            # represents the organizational structure.
            if (target / "catalog.json").exists():
                # It's a catalog (root or subcatalog)
                catalog_result = inspect_catalog(target)
                _output_catalog_info(catalog_result, use_json=use_json)
            elif (target / "collection.json").exists():
                # It's a collection
                collection_result = inspect_collection(target)
                _output_collection_info(collection_result, use_json=use_json)
            else:
                # Directory exists but isn't a catalog or collection
                raise ValueError(f"Directory is not a catalog or collection: {target}")
        else:
            # Path doesn't exist
            raise FileNotFoundError(f"Path not found: {target}")

    except FileNotFoundError as err:
        if use_json:
            envelope = error_envelope(
                "info",
                [ErrorDetail(type="FileNotFoundError", message=str(err))],
            )
            output_json_envelope(envelope)
        else:
            error(str(err))
        raise SystemExit(1) from err
    except ValueError as err:
        if use_json:
            envelope = error_envelope(
                "info",
                [ErrorDetail(type="ValueError", message=str(err))],
            )
            output_json_envelope(envelope)
        else:
            error(str(err))
        raise SystemExit(1) from err


def _output_file_info(result: Any, *, use_json: bool) -> None:
    """Output file info in human or JSON format."""
    if use_json:
        envelope = success_envelope("info", result.to_dict())
        output_json_envelope(envelope)
    else:
        for line in result.format_human():
            info_output(line)


def _output_collection_info(result: Any, *, use_json: bool) -> None:
    """Output collection info in human or JSON format."""
    if use_json:
        envelope = success_envelope("info", result.to_dict())
        output_json_envelope(envelope)
    else:
        for line in result.format_human():
            info_output(line)


def _output_catalog_info(result: Any, *, use_json: bool) -> None:
    """Output catalog info in human or JSON format."""
    if use_json:
        envelope = success_envelope("info", result.to_dict())
        output_json_envelope(envelope)
    else:
        for line in result.format_human():
            info_output(line)


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
        info_output(msg)

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
        info_output("No geospatial files found")
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
    help="Fix issues: convert geo-assets to cloud-native, update stale metadata",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Preview what would be fixed (use with --fix)",
)
@click.option(
    "--remove-legacy",
    is_flag=True,
    help="Remove source files after successful conversion (use with --fix)",
)
@click.option(
    "--metadata",
    is_flag=True,
    help="Only check/fix STAC metadata (links, schema, staleness)",
)
@click.option(
    "--geo-assets",
    "geo_assets",
    is_flag=True,
    help="Only check/fix geospatial assets (cloud-native status, convertibility)",
)
@click.pass_context
def check(
    ctx: click.Context,
    path: Path,
    json_output: bool,
    verbose: bool,
    fix: bool,
    dry_run: bool,
    remove_legacy: bool,
    metadata: bool,
    geo_assets: bool,
) -> None:
    """Validate a Portolan catalog or check files for cloud-native status.

    Runs validation rules against the catalog and reports any issues.
    With --fix, applies fixes based on selected scope.

    PATH is the directory to check (default: current directory).

    Use --metadata or --geo-assets to limit scope:
    - --metadata: Only check/fix STAC metadata (staleness, missing items)
    - --geo-assets: Only check/fix geospatial assets (cloud-native status)
    - Neither: Check/fix both (default)

    Examples:

        portolan check                        # Validate all (metadata + geo-assets)

        portolan check --metadata             # Validate metadata only

        portolan check --geo-assets           # Check geo-assets only

        portolan check --fix                  # Fix both metadata and geo-assets

        portolan check --metadata --fix       # Fix only metadata (create/update items)

        portolan check --geo-assets --fix     # Fix only geo-assets (convert files)

        portolan check --fix --dry-run        # Preview all fixes
    """
    use_json = should_output_json(ctx, json_output)

    # Validate path exists
    if not path.exists():
        _handle_path_not_found(path, use_json)

    # Warn if --dry-run is used without --fix
    if dry_run and not fix:
        warn("--dry-run has no effect without --fix")

    # Warn if --remove-legacy is used without --fix
    if remove_legacy and not fix:
        warn("--remove-legacy requires --fix")

    # Determine which checks to run based on scope flags
    run_metadata, run_geo_assets, mode = _determine_check_mode(metadata, geo_assets)

    # Execute the appropriate check workflow
    _execute_check_workflow(
        path=path,
        run_metadata=run_metadata,
        run_geo_assets=run_geo_assets,
        mode=mode,
        fix=fix,
        dry_run=dry_run,
        remove_legacy=remove_legacy,
        use_json=use_json,
        verbose=verbose,
    )


def _output_fix_json(
    *,
    mode: str,
    metadata_fix_report: FixReport | None,
    format_fix_report: Any,
    has_failures: bool,
) -> None:
    """Output combined fix results as JSON.

    Args:
        mode: Check mode string.
        metadata_fix_report: Results from metadata fix (if run).
        format_fix_report: Results from geo-asset fix (if run).
        has_failures: Whether any fix operation failed.
    """
    data: dict[str, Any] = {"mode": mode}

    if metadata_fix_report is not None:
        if not isinstance(metadata_fix_report, FixReport):
            raise TypeError(f"Expected FixReport, got {type(metadata_fix_report).__name__}")
        data["metadata_fix"] = metadata_fix_report.to_dict()

    if format_fix_report is not None:
        # Use "conversion" key for backward compatibility with existing tests
        data["conversion"] = format_fix_report.to_dict()

    # Use error_envelope if there were failures
    if has_failures:
        envelope = error_envelope(
            "check",
            [ErrorDetail(type="FixError", message="Some fixes failed")],
            data=data,
        )
    else:
        envelope = success_envelope("check", data)
    output_json_envelope(envelope)


def _output_fix_human(
    *,
    mode: str,
    metadata_fix_report: FixReport | None,
    format_fix_report: Any,
    verbose: bool,
    dry_run: bool,
) -> None:
    """Output combined fix results in human-readable format.

    Args:
        mode: Check mode string.
        metadata_fix_report: Results from metadata fix (if run).
        format_fix_report: Results from geo-asset fix (if run).
        verbose: Show detailed output.
        dry_run: Whether this was a dry run.
    """
    # Output metadata fix results
    if metadata_fix_report is not None:
        if not isinstance(metadata_fix_report, FixReport):
            raise TypeError(f"Expected FixReport, got {type(metadata_fix_report).__name__}")

        if metadata_fix_report.total_count > 0:
            action = "create/update" if dry_run else "Created/updated"
            success(
                f"{action} {metadata_fix_report.total_count} metadata "
                f"item{'s' if metadata_fix_report.total_count != 1 else ''}"
            )
        if metadata_fix_report.skipped_count > 0:
            info_output(f"Skipped {metadata_fix_report.skipped_count} items (already fresh)")
        if metadata_fix_report.failure_count > 0:
            error(f"Failed to fix {metadata_fix_report.failure_count} metadata items")

        # Show details if verbose or failures
        if verbose or metadata_fix_report.failure_count > 0:
            for result in metadata_fix_report.results:
                status_char = "✓" if result.success else "✗"
                msg = f"{status_char} {result.file_path}: {result.action.value} ({result.message})"
                if result.success:
                    detail(msg)
                else:
                    error(msg)

    # Output format fix results (conversion)
    if format_fix_report is not None:
        if dry_run:
            _print_check_fix_preview(format_fix_report)
        else:
            _print_check_fix_results(format_fix_report, verbose=verbose)


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


def _determine_check_mode(metadata: bool, geo_assets: bool) -> tuple[bool, bool, str]:
    """Determine which checks to run and the mode string.

    The scope flags (--metadata, --geo-assets) determine WHAT to check/fix:
    - Neither flag: check/fix both (default)
    - --metadata: check/fix metadata only
    - --geo-assets: check/fix geo-assets only
    - Both flags: check/fix both (explicit)

    The --fix flag separately controls WHETHER to apply fixes (orthogonal).

    Returns:
        Tuple of (run_metadata, run_geo_assets, mode_string).
    """
    explicit_flags = metadata or geo_assets

    if explicit_flags:
        run_metadata = metadata
        run_geo_assets = geo_assets
    else:
        # No explicit flags: run both
        run_metadata = True
        run_geo_assets = True

    # Determine mode string
    if run_metadata and not run_geo_assets:
        mode = "metadata"
    elif run_geo_assets and not run_metadata:
        mode = "geo-assets"
    else:
        mode = "all"

    return run_metadata, run_geo_assets, mode


def _execute_check_workflow(
    *,
    path: Path,
    run_metadata: bool,
    run_geo_assets: bool,
    mode: str,
    fix: bool,
    dry_run: bool,
    remove_legacy: bool,
    use_json: bool,
    verbose: bool,
) -> None:
    """Execute the check workflow based on flags.

    The workflow varies based on scope (--metadata, --geo-assets) and --fix:
    - Without --fix: run validation and report issues
    - With --fix: run validation AND apply fixes for the selected scope
    """
    # Handle fix workflows (may exit early)
    if fix:
        _run_fix_workflow(
            path=path,
            run_metadata=run_metadata,
            run_geo_assets=run_geo_assets,
            mode=mode,
            dry_run=dry_run,
            remove_legacy=remove_legacy,
            use_json=use_json,
            verbose=verbose,
        )
        return

    # Check-only workflows (no --fix)
    if run_metadata and not run_geo_assets:
        # Metadata only
        metadata_report = validate_catalog(path)
        _output_metadata_only(metadata_report, mode, use_json, verbose)
    elif run_geo_assets and not run_metadata:
        # Geo-assets only
        _output_format_only(path, mode, use_json, verbose)
    else:
        # Both (combined)
        metadata_report = validate_catalog(path)
        _output_combined(path, metadata_report, mode, use_json, verbose)


def _run_fix_workflow(
    *,
    path: Path,
    run_metadata: bool,
    run_geo_assets: bool,
    mode: str,
    dry_run: bool,
    remove_legacy: bool,
    use_json: bool,
    verbose: bool,
) -> None:
    """Execute the fix workflow for selected scope.

    Args:
        path: Directory to check/fix.
        run_metadata: Whether to fix metadata issues.
        run_geo_assets: Whether to fix geo-asset format issues.
        mode: Mode string for JSON output.
        dry_run: Preview changes without applying them.
        remove_legacy: Remove source files after successful conversion.
        use_json: Output JSON envelope.
        verbose: Show detailed output.
    """
    metadata_fix_report: FixReport | None = None
    format_fix_report = None
    has_failures = False

    # Fix metadata if in scope
    if run_metadata:
        from portolan_cli.metadata.scan import scan_catalog_metadata

        metadata_check_report = scan_catalog_metadata(path)
        metadata_fix_report = fix_metadata(path, metadata_check_report, dry_run=dry_run)
        if metadata_fix_report.failure_count > 0:
            has_failures = True

    # Fix geo-assets if in scope
    if run_geo_assets:
        # Progress callback for conversion (skip for JSON mode, per ADR-0040: per-file only in verbose)
        def show_conversion_progress(result: ConversionResult) -> None:
            if not use_json and verbose and result.source:
                info_output(f"Converting: {result.source.name}")

        format_fix_report = check_directory(
            path,
            fix=True,
            dry_run=dry_run,
            remove_legacy=remove_legacy,
            on_progress=show_conversion_progress,
            catalog_path=path,
        )

    # Output results
    if use_json:
        _output_fix_json(
            mode=mode,
            metadata_fix_report=metadata_fix_report,
            format_fix_report=format_fix_report,
            has_failures=has_failures,
        )
    else:
        _output_fix_human(
            mode=mode,
            metadata_fix_report=metadata_fix_report,
            format_fix_report=format_fix_report,
            verbose=verbose,
            dry_run=dry_run,
        )

    # Exit with error if any failures
    if has_failures:
        raise SystemExit(1)
    # Also exit with error if format conversion had failures
    if (
        format_fix_report
        and format_fix_report.conversion_report
        and format_fix_report.conversion_report.failed > 0
    ):
        raise SystemExit(1)


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
    format_report = check_directory(path, fix=False, dry_run=False, catalog_path=path)
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
    """Output combined metadata + format check results.

    Args:
        path: Directory that was checked.
        metadata_report: ValidationReport from metadata validation.
        mode: Check mode string for JSON output.
        use_json: Whether to output JSON envelope.
        verbose: Whether to show detailed output.

    Raises:
        SystemExit: If metadata validation has errors.
    """
    format_report = check_directory(path, fix=False, dry_run=False, catalog_path=path)
    has_metadata_errors = metadata_report is not None and bool(metadata_report.errors)

    if use_json:
        _output_combined_check_json(metadata_report, format_report, mode=mode)
    else:
        if metadata_report:
            info_output("Metadata validation:")
            for result in metadata_report.results:
                if verbose or not result.passed:
                    _print_validation_result(result)
            _print_check_summary(metadata_report)
        if format_report:
            info_output("\nFormat check:")
            _print_format_check_results(format_report, verbose=verbose)

    # Exit with error if metadata validation failed
    if has_metadata_errors:
        raise SystemExit(1)


def _print_check_fix_preview(report: Any) -> None:
    """Print preview of what would be converted."""
    from portolan_cli.formats import CloudNativeStatus

    convertible = [f for f in report.files if f.status == CloudNativeStatus.CONVERTIBLE]

    if not convertible:
        info_output("No files need conversion")
        return

    info_output(f"Dry run: {len(convertible)} file(s) would be converted")
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
        info_output("No files to convert")
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

    # Print legacy removal results if present
    removal = report.legacy_removal_report
    if removal is not None:
        if removal.success_count > 0:
            success(f"Removed {removal.success_count} legacy file(s)")
            if verbose:
                for removed_path in removal.removed:
                    detail(f"  {removed_path.name}")
        if removal.error_count > 0:
            error(f"Failed to remove {removal.error_count} file(s)")
            for failed_path, err_msg in removal.errors.items():
                error(f"  {failed_path.name}: {err_msg}")


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
                info_output("No issues to fix")
            else:
                info_output(f"Dry run: {len(proposed)} fix(es) would be applied")
                for fix in proposed:
                    detail(f"  {fix.preview}")
        return proposed, []

    # Apply fixes
    proposed, applied = apply_safe_fixes(result.issues, dry_run=False)

    if not use_json:
        if not proposed:
            info_output("No issues to fix")
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
@click.argument("path", type=click.Path(path_type=Path), default=".")
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
@click.option(
    "--strict",
    is_flag=True,
    help="Treat warnings as errors (exit 1 on any warning or error)",
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
    strict: bool,
) -> None:
    """Scan a directory for geospatial files and potential issues.

    Discovers files by extension, validates shapefile completeness,
    and reports issues that may cause problems during import.

    PATH is the directory to scan (default: current directory).

    \b
    Fix Mode:
        Use --fix to auto-rename files with:
        - Invalid characters (spaces, parentheses, non-ASCII)
        - Windows reserved names (CON, PRN, AUX, etc.)
        - Long paths (> 200 characters)

        Use --dry-run to preview changes without applying.

    Examples:

        portolan scan                         # Scan current directory

        portolan scan --json                  # JSON output in current directory

        portolan scan /data/geospatial

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
        strict=strict,
    )

    # Pre-count directories only when progress will be displayed
    # ScanProgressReporter displays progress when: not json_mode AND stderr is TTY
    should_show_progress = not use_json and sys.stderr.isatty()
    total_dirs = 0
    if should_show_progress:
        # Use same follow_symlinks setting as scan for accurate progress
        total_dirs = count_directories(
            path,
            include_hidden=include_hidden,
            max_depth=max_depth,
            recursive=not no_recursive,
            follow_symlinks=follow_symlinks,
        )

    # Create progress reporter (suppressed in JSON mode or non-TTY)
    progress_reporter = ScanProgressReporter(
        total_directories=total_dirs,
        json_mode=use_json,
    )

    try:
        with progress_reporter:
            result = scan_directory(path, options, progress_callback=progress_reporter.advance)
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

    # Determine if we should fail based on strict mode
    # Strict mode: errors OR warnings → failure
    # Normal mode: errors only → failure (but scan is informational, so still exit 0)
    has_strict_failure = strict and (result.has_errors or result.warning_count > 0)

    # Output results in appropriate format
    _output_scan_results(
        result,
        use_json=use_json,
        manual_only=manual_only,
        show_all=show_all,
        show_tree=show_tree,
        strict=strict,
        has_strict_failure=has_strict_failure,
        elapsed_seconds=progress_reporter.elapsed_seconds,
    )

    # Handle strict mode exit code
    if has_strict_failure:
        _handle_strict_mode_exit(result, use_json=use_json)

    # Normal mode: scan is informational — exit 0 even with errors


def _handle_strict_mode_exit(result: ScanResult, *, use_json: bool) -> NoReturn:
    """Handle strict mode exit with appropriate messaging.

    Args:
        result: The scan result.
        use_json: If True, skip human-readable message (already in JSON envelope).

    Raises:
        SystemExit: Always raises with exit code 1.
    """
    if not use_json:
        # Add strict mode message for human output
        warn_count = result.warning_count
        err_count = result.error_count
        if warn_count > 0 and err_count == 0:
            error(f"Strict mode: {warn_count} warning(s) treated as error(s)")
        elif warn_count > 0 and err_count > 0:
            error(
                f"Strict mode: {err_count} error(s) and {warn_count} warning(s) treated as errors"
            )
    raise SystemExit(1)


def _output_scan_results(
    result: ScanResult,
    *,
    use_json: bool,
    manual_only: bool,
    show_all: bool,
    show_tree: bool,
    strict: bool,
    has_strict_failure: bool,
    elapsed_seconds: float = 0.0,
) -> None:
    """Output scan results in the appropriate format.

    Args:
        result: The scan result.
        use_json: If True, output JSON envelope.
        manual_only: If True, show only manual-resolution issues.
        show_all: If True, show all issues without truncation.
        show_tree: If True, show directory tree view.
        strict: If True, treat warnings as errors in JSON output.
        has_strict_failure: If True, the scan has failed in strict mode.
        elapsed_seconds: Time elapsed during scan (for progress reporting).
    """
    if use_json:
        _output_scan_json(result, strict=strict, has_strict_failure=has_strict_failure)
    elif manual_only:
        from portolan_cli.scan_output import format_scan_output

        output = format_scan_output(result, manual_only=True)
        click.echo(output)
    else:
        _print_scan_summary_enhanced(
            result,
            show_all=show_all,
            show_tree=show_tree,
            elapsed_seconds=elapsed_seconds,
        )


def _output_scan_json(result: ScanResult, *, strict: bool, has_strict_failure: bool) -> None:
    """Output scan results as JSON envelope.

    Args:
        result: The scan result.
        strict: If True, include warnings as errors.
        has_strict_failure: If True, mark as not successful.
    """
    data = result.to_dict()
    data["summary"] = {
        "ready_count": len(result.ready),
        "error_count": result.error_count,
        "warning_count": result.warning_count,
        "skipped_count": len(result.skipped),
    }
    # Add fix_commands for agent consumption
    data["fix_commands"] = format_fix_commands_json(result)

    if has_strict_failure or result.has_errors:
        # Still return data, but mark as not successful
        # In strict mode, include warnings as errors too
        errors = [
            ErrorDetail(type=issue.issue_type.value, message=issue.message)
            for issue in result.issues
            if issue.severity == ScanSeverity.ERROR
            or (strict and issue.severity == ScanSeverity.WARNING)
        ]
        envelope = error_envelope("scan", errors, data=data)
    else:
        envelope = success_envelope("scan", data)

    output_json_envelope(envelope)


def _print_scan_header(result: ScanResult, *, elapsed_seconds: float = 0.0) -> None:
    """Print scan header with file counts and timing.

    Args:
        result: The scan result.
        elapsed_seconds: Time elapsed during scan.
    """
    ready_count = len(result.ready)
    dirs_scanned = result.directories_scanned

    # Format timing string
    time_str = f" in {elapsed_seconds:.1f}s" if elapsed_seconds > 0 else ""

    # Show directories scanned with timing
    detail(f"Scanned {dirs_scanned} director{'y' if dirs_scanned == 1 else 'ies'}{time_str}")

    if ready_count == 0:
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

# Maximum example paths to show per batched issue group
BATCH_EXAMPLES_LIMIT = 3


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
        result.issues,
        ScanSeverity.INFO,
        info_output,
        result.info_count,
        "info message",
        show_all=show_all,
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
    elapsed_seconds: float = 0.0,
) -> None:
    """Print enhanced human-readable scan summary.

    Includes:
    - Summary header with timing
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
        elapsed_seconds: Time elapsed during scan.
    """
    # Header with timing info
    _print_scan_header(result, elapsed_seconds=elapsed_seconds)
    _print_format_breakdown(result)

    # Tree view (if requested)
    if show_tree:
        click.echo()
        tree_output = render_tree_view(result, show_missing=True)
        click.echo(tree_output)

    # Issues with fixability labels
    _print_issues_with_fixability(result, show_all=show_all)

    # Skipped files by category
    _print_skipped_by_category(result, show_all=show_all)

    # Collection suggestions
    _print_collection_suggestions(result)

    # Next steps
    _print_next_steps(result)


def _print_issues_with_fixability(result: ScanResult, *, show_all: bool = False) -> None:
    """Print issues grouped by severity and IssueType with fixability labels.

    Issues that share the same severity AND IssueType are batched together so
    that noisy repetitions (e.g. 265 uppercase-named directories) collapse into
    a single summary line with up to 3 example paths.  The full list is shown
    when ``show_all=True``.

    Args:
        result: The scan result containing issues.
        show_all: If True, show all example paths instead of truncating at 3.
    """
    if not result.issues:
        return

    # Iterate severity levels in display order so errors appear first.
    for severity, header_fn, label in [
        (ScanSeverity.ERROR, error, "error"),
        (ScanSeverity.WARNING, warn, "warning"),
        (ScanSeverity.INFO, info_output, "info message"),
    ]:
        severity_issues = [i for i in result.issues if i.severity == severity]
        if not severity_issues:
            continue

        total = len(severity_issues)
        header_fn(f"{total} {label}{'s' if total != 1 else ''}")

        # Group by (IssueType, message) so issues with the same problem description
        # batch together, while distinct messages get separate groups.
        # Preserves insertion order (Python 3.7+).
        GroupKey = tuple[IssueType, str]
        groups: dict[GroupKey, list[ScanIssue]] = {}
        for issue in severity_issues:
            key: GroupKey = (issue.issue_type, issue.message)
            groups.setdefault(key, []).append(issue)

        for (issue_type, _message), group in groups.items():
            fix_label = get_fixability(issue_type).label
            count = len(group)

            if count == 1:
                # Single issue: print normally (no batching overhead).
                issue = group[0]
                header_fn(f"  {fix_label} {issue.relative_path}: {issue.message}")
                if issue.suggestion is not None:
                    detail(f"    Hint: {issue.suggestion}")
            else:
                # Multiple issues with same type+message+suggestion: show count + examples.
                # Use the shared message directly since all issues in this group have it.
                shared_message = group[0].message
                header_fn(f"  {fix_label} {count} files: {shared_message}")

                # Decide how many examples to display.
                examples = group if show_all else group[:BATCH_EXAMPLES_LIMIT]
                remaining = count - len(examples)

                paths = ", ".join(i.relative_path for i in examples)
                if remaining > 0:
                    detail(f"    Examples: {paths} (and {remaining} more, use --all to see all)")
                else:
                    detail(f"    Examples: {paths}")

                # Show a representative suggestion (may vary per file, show first).
                first_suggestion = next(
                    (i.suggestion for i in group if i.suggestion is not None), None
                )
                if first_suggestion is not None:
                    detail(f"    Hint: {first_suggestion}")


def _print_skipped_by_category(result: ScanResult, *, show_all: bool = False) -> None:
    """Print skipped files grouped by category.

    Args:
        result: The scan result containing skipped files.
        show_all: If True, show all unrecognized files. If False, truncate after 10.
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

    unknown_files = grouped.get(FileCategory.UNKNOWN, [])
    unknown_count = len(unknown_files)
    # Calculate recognized count from grouped files only (excludes legacy Path objects)
    total_categorized = sum(len(files) for files in grouped.values())
    recognized_count = total_categorized - unknown_count

    # If all files are recognized (no unknowns), show a concise summary
    if unknown_count == 0:
        # Build a compact list: "5 catalog files, 4 tabular, 2 thumbnails, ..."
        parts = []
        for category, files in sorted(grouped.items(), key=lambda x: len(x[1]), reverse=True):
            display_name = get_category_display_name(category)
            parts.append(f"{len(files)} {display_name}")
        detail(f"  {', '.join(parts)}")
    else:
        # Some unknown files - show the detailed breakdown with file listing
        click.echo()
        if unknown_count > 0:
            warn(f"{unknown_count} files with unrecognized format:")
            # List the specific unrecognized files (sorted for deterministic output)
            sorted_unknown_files = sorted(unknown_files, key=lambda f: f.relative_path)
            max_files = unknown_count if show_all else min(10, unknown_count)
            for skipped_file in sorted_unknown_files[:max_files]:
                detail(f"  - {skipped_file.relative_path}")

            # Show truncation message if needed
            if unknown_count > max_files and not show_all:
                detail(f"  ... and {unknown_count - max_files} more (use --all to see all)")

        if recognized_count > 0:
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
    info_output("Suggested collections:")
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
    info_output("Next steps:")
    for step in steps:
        detail(f"  \u2192 {step}")


# ─────────────────────────────────────────────────────────────────────────────
# Top-level add/rm commands (per ADR-0022)
# ─────────────────────────────────────────────────────────────────────────────


def _handle_cmd_error(cmd: str, err_type: str, message: str, use_json: bool) -> None:
    """Handle error output for add/rm commands (standardized)."""
    if use_json:
        envelope = error_envelope(cmd, [ErrorDetail(type=err_type, message=message)])
        output_json_envelope(envelope)
    else:
        error(message)


def _output_add_json(
    added: list[DatasetInfo],
    skipped: list[Path],
    failures: list[AddFailure],
) -> None:
    """Output add command results as JSON."""
    data = {
        "added": [
            {
                "item_id": ds.item_id,
                "collection_id": ds.collection_id,
                "format_type": ds.format_type.value,
                "bbox": ds.bbox,
            }
            for ds in added
        ],
        "skipped": [str(p) for p in skipped],
        "failures": [{"path": str(f.path), "error": f.error} for f in failures],
    }
    if failures:
        envelope = error_envelope(
            "add",
            [ErrorDetail(type="AddError", message=f"{str(f.path)}: {f.error}") for f in failures],
            data=data,
        )
    else:
        envelope = success_envelope("add", data)
    output_json_envelope(envelope)


def _format_sidecar_note(ds: DatasetInfo) -> str:
    """Format sidecar count note for dataset output."""
    if not ds.asset_paths:
        return ""
    sidecars = get_sidecars(Path(ds.asset_paths[0]))
    return f" (+ {len(sidecars)} sidecars)" if sidecars else ""


def _output_added_single_collection(added: list[DatasetInfo], *, verbose: bool = False) -> None:
    """Output added datasets for a single collection.

    Per ADR-0040: Only show per-file output when verbose=True.
    Default is summary only (progress bar + final count).
    """
    if verbose:
        coll = added[0].collection_id
        count = len(added)
        info_output(f"Adding {count} file{'s' if count != 1 else ''} to {coll}")
        for ds in added:
            success(f"  + {ds.item_id}{_format_sidecar_note(ds)}")


def _output_added_multi_collection(added: list[DatasetInfo], *, verbose: bool = False) -> None:
    """Output added datasets grouped by collection.

    Per ADR-0040: Only show per-file output when verbose=True.
    Default is summary only (progress bar + final count).
    """
    if verbose:
        collections: dict[str, list[DatasetInfo]] = {}
        for ds in added:
            collections.setdefault(ds.collection_id, []).append(ds)

        total = len(added)
        info_output(
            f"Adding {total} file{'s' if total != 1 else ''} to {len(collections)} collections"
        )
        for coll, datasets in sorted(collections.items()):
            info_output(f"  {coll}:")
            for ds in datasets:
                success(f"    + {ds.item_id}{_format_sidecar_note(ds)}")


def _print_add_failures_batched(failures: list[AddFailure]) -> None:
    """Print add failures grouped by error message.

    Groups failures with identical error messages to reduce noise.
    Single-occurrence errors are shown inline; repeated errors are batched
    with example paths (up to BATCH_EXAMPLES_LIMIT) and a remainder count.

    Follows the same batching pattern used by ``_print_issues_with_fixability``
    for scan output (PR #194).

    Args:
        failures: List of AddFailure objects to display.
    """
    if not failures:
        return

    fail_count = len(failures)
    error(f"{fail_count} item{'s' if fail_count != 1 else ''} failed:")

    # Group by error message
    groups: dict[str, list[AddFailure]] = {}
    for f in failures:
        groups.setdefault(f.error, []).append(f)

    for error_msg, group in groups.items():
        count = len(group)
        if count == 1:
            error(f"  - {group[0].path}: {error_msg}")
        else:
            error(f"  {error_msg} ({count} files):")
            examples = group[:BATCH_EXAMPLES_LIMIT]
            remaining = count - len(examples)
            paths = ", ".join(str(f.path) for f in examples)
            if remaining > 0:
                detail(f"    Examples: {paths} (and {remaining} more)")
            else:
                detail(f"    Examples: {paths}")


def _output_add_unchanged(skipped: list[Path], verbose: bool) -> None:
    """Output message when all files are already tracked (unchanged)."""
    skip_count = len(skipped)
    success(f"All {skip_count} file{'s' if skip_count != 1 else ''} already tracked (unchanged)")
    if verbose:
        for p in skipped:
            detail(f"  {p.name}")


def _output_add_summary(added: list[DatasetInfo]) -> None:
    """Output final success summary after adding files (ADR-0040)."""
    total_added = len(added)
    unique_items = len({ds.item_id for ds in added})
    unique_collections = len({ds.collection_id for ds in added})

    # Build summary with file count, item count (if different), and collection count
    parts = []

    if unique_items == total_added:
        parts.append(f"{total_added} file{'s' if total_added != 1 else ''}")
    else:
        parts.append(
            f"{total_added} file{'s' if total_added != 1 else ''} "
            f"({unique_items} item{'s' if unique_items != 1 else ''})"
        )

    # Always show collection count for clarity
    parts.append(f"to {unique_collections} collection{'s' if unique_collections != 1 else ''}")

    success(f"Added {' '.join(parts)}")


def _output_add_human(
    added: list[DatasetInfo],
    skipped: list[Path],
    failures: list[AddFailure],
    verbose: bool,
) -> None:
    """Output add command results as human-readable text."""
    # Handle edge cases with early returns
    if not added and not failures and not skipped:
        info_output("No geospatial files found to add")
        return
    if not added and not failures:
        _output_add_unchanged(skipped, verbose)
        return

    # Output successful adds (per-file details only in verbose mode per ADR-0040)
    if added:
        unique_collections = {ds.collection_id for ds in added}
        if len(unique_collections) == 1:
            _output_added_single_collection(added, verbose=verbose)
        else:
            _output_added_multi_collection(added, verbose=verbose)

    # Show skipped files in verbose mode
    if verbose and skipped:
        for p in skipped:
            detail(f"Skipping {p.name} (unchanged)")

    # Final success summary (always show if we added something, even with failures)
    # Per ADR-0040: Summary is always shown, failures are shown separately
    if added:
        _output_add_summary(added)

    # Output failures batched by error message (Issue #199)
    if failures:
        _print_add_failures_batched(failures)


def _output_add_results(
    added: list[DatasetInfo],
    skipped: list[Path],
    failures: list[AddFailure],
    verbose: bool,
    use_json: bool,
) -> None:
    """Output results for add command.

    Per Issue #175: Shows both successes and failures, enabling users to see
    all issues at once rather than stopping on the first error.
    """
    if use_json:
        _output_add_json(added, skipped, failures)
    else:
        _output_add_human(added, skipped, failures, verbose)


def _resolve_catalog_root_for_add(
    catalog_path: Path | None,
    use_json: bool,
) -> Path:
    """Resolve and validate catalog root for add command.

    Args:
        catalog_path: Explicit catalog path from --portolan-dir, or None for auto-detect.
        use_json: Whether to output errors as JSON.

    Returns:
        Resolved catalog root path.

    Raises:
        SystemExit: If not inside a catalog or catalog is invalid.
    """
    if catalog_path is not None:
        catalog_root = catalog_path.resolve()
        # Validate catalog exists (when explicitly specified)
        # Per ADR-0029, use .portolan/config.yaml as the single sentinel
        if not (catalog_root / ".portolan" / "config.yaml").exists():
            _handle_cmd_error("add", "NotACatalogError", f"Not a catalog: {catalog_root}", use_json)
            if not use_json:
                detail("Run 'portolan init' to create a catalog")
            raise SystemExit(1)
        return catalog_root

    # Auto-detect catalog root (git-style)
    detected_root = find_catalog_root()
    if detected_root is None:
        _handle_cmd_error(
            "add",
            "NotACatalogError",
            "Not inside a Portolan catalog (no .portolan/config.yaml found)",
            use_json,
        )
        if not use_json:
            detail("Run 'portolan init' to create a catalog, or cd into one")
        raise SystemExit(1)
    return detected_root


def _validate_item_id_usage(
    item_id: str | None,
    paths: tuple[Path, ...],
    use_json: bool,
) -> None:
    """Validate --item-id is only used with a single file.

    Args:
        item_id: Item ID override, or None if not specified.
        paths: Paths from CLI arguments.
        use_json: Whether to output errors as JSON.

    Raises:
        SystemExit: If --item-id is used with multiple paths or a directory.
    """
    if item_id is None:
        return

    if len(paths) != 1:
        _handle_cmd_error(
            "add",
            "ValueError",
            "--item-id can only be used with a single file, not multiple paths",
            use_json,
        )
        raise SystemExit(1)

    # Exactly 1 path: check if it's a directory
    if paths[0].resolve().is_dir():
        _handle_cmd_error(
            "add",
            "ValueError",
            "--item-id can only be used with a single file, not a directory",
            use_json,
        )
        raise SystemExit(1)


def _handle_parquet_after_add(
    catalog_root: Path,
    affected_collections: set[str],
    generate_parquet: bool,
    verbose: bool,
    *,
    show_hints: bool = True,
) -> None:
    """Handle stac-geoparquet generation/hints after add command.

    If generate_parquet is True or parquet.enabled config is set, generates
    items.parquet for affected collections. Otherwise, shows hints for
    collections exceeding the threshold.

    Args:
        catalog_root: Path to catalog root.
        affected_collections: Set of collection IDs that were modified.
        generate_parquet: Whether --stac-geoparquet flag was passed.
        verbose: Whether to show verbose output.
        show_hints: Whether to show hints (disabled in JSON output mode).
    """
    if not affected_collections:
        return

    from portolan_cli.config import get_setting
    from portolan_cli.stac_parquet import (
        add_parquet_link_to_collection,
        count_items,
        generate_items_parquet,
        should_suggest_parquet,
        track_parquet_in_versions,
    )

    for coll_id in affected_collections:
        coll_path = catalog_root / coll_id
        if not (coll_path / "collection.json").exists():
            continue

        # Get settings per-collection with hierarchical lookup (ADR-0039)
        parquet_enabled_raw = get_setting(
            "parquet.enabled",
            catalog_path=catalog_root,
            collection=coll_id,
            collection_path=coll_path,
        )
        threshold_raw = get_setting(
            "parquet.threshold",
            catalog_path=catalog_root,
            collection=coll_id,
            collection_path=coll_path,
        )

        # Type coercion: parquet.enabled can be string from env var
        parquet_enabled = _coerce_bool(parquet_enabled_raw, default=False)
        threshold = _coerce_int(threshold_raw, default=100)

        # Count items first to apply threshold gate
        item_count = count_items(coll_path)

        # Generate when:
        # - Explicit --stac-geoparquet flag (always generate), OR
        # - Auto-generation enabled AND item count exceeds threshold
        should_generate = generate_parquet or (parquet_enabled and item_count > threshold)

        if should_generate and item_count > 0:
            try:
                generate_items_parquet(coll_path)
                add_parquet_link_to_collection(coll_path)
                track_parquet_in_versions(coll_path)
                if verbose:
                    info_output(f"Generated items.parquet for '{coll_id}'")
            except Exception as e:
                # Explicit --stac-geoparquet should fail the command
                if generate_parquet:
                    raise
                # Auto-generation failures just warn
                warn(f"Failed to generate parquet for '{coll_id}': {e}")
        elif show_hints and should_suggest_parquet(coll_path, threshold=threshold):
            info_output(
                f"Hint: Collection '{coll_id}' has {item_count} items (>{threshold}). "
                f"Consider running: portolan stac-geoparquet -c {coll_id}"
            )


@dataclass
class PMTilesSettings:
    """Settings for PMTiles generation."""

    enabled: bool = False
    min_zoom: int | None = None
    max_zoom: int | None = None
    layer: str | None = None
    bbox: str | None = None
    where: str | None = None
    include_cols: str | None = None
    precision: int = 6
    attribution: str | None = None
    src_crs: str | None = None


def _get_pmtiles_settings(catalog_root: Path, coll_id: str, coll_path: Path) -> PMTilesSettings:
    """Get PMTiles settings for a collection."""
    from portolan_cli.config import get_setting

    def get(key: str) -> Any:
        return get_setting(
            f"pmtiles.{key}",
            catalog_path=catalog_root,
            collection=coll_id,
            collection_path=coll_path,
        )

    return PMTilesSettings(
        enabled=_coerce_bool(get("enabled"), default=False),
        min_zoom=None if get("min_zoom") is None else _coerce_int(get("min_zoom"), default=0),
        max_zoom=None if get("max_zoom") is None else _coerce_int(get("max_zoom"), default=14),
        layer=get("layer"),
        bbox=get("bbox"),
        where=get("where"),
        include_cols=get("include_cols"),
        precision=_coerce_int(get("precision"), default=6),
        attribution=get("attribution"),
        src_crs=get("src_crs"),
    )


def _handle_pmtiles_after_add(
    catalog_root: Path,
    affected_collections: set[str],
    generate_pmtiles: bool,
    force: bool,
    verbose: bool,
    *,
    use_json: bool = False,
) -> None:
    """Handle PMTiles generation after add command."""
    if not affected_collections:
        return

    from portolan_cli.pmtiles import (
        PMTilesNotAvailableError,
        TippecanoeNotFoundError,
        generate_pmtiles_for_collection,
    )

    for coll_id in affected_collections:
        coll_path = catalog_root / coll_id
        if not (coll_path / "collection.json").exists():
            continue

        settings = _get_pmtiles_settings(catalog_root, coll_id, coll_path)
        if not (generate_pmtiles or settings.enabled):
            continue

        try:
            result = generate_pmtiles_for_collection(
                coll_path,
                catalog_root,
                force=force,
                min_zoom=settings.min_zoom,
                max_zoom=settings.max_zoom,
                layer=settings.layer,
                bbox=settings.bbox,
                where=settings.where,
                include_cols=settings.include_cols,
                precision=settings.precision,
                attribution=settings.attribution,
                src_crs=settings.src_crs,
            )
            _report_pmtiles_result(result, verbose, generate_pmtiles, use_json=use_json)
        except PMTilesNotAvailableError as e:
            _handle_pmtiles_unavailable(
                e, generate_pmtiles, settings.enabled, verbose, coll_id, use_json=use_json
            )
        except TippecanoeNotFoundError as e:
            _handle_tippecanoe_missing(e, generate_pmtiles, coll_id, use_json=use_json)


def _report_pmtiles_result(
    result: Any, verbose: bool, explicit_flag: bool, *, use_json: bool = False
) -> None:
    """Report PMTiles generation results."""
    if not use_json:
        for p in result.generated:
            success(f"Generated PMTiles: {p.name}")
        if result.skipped and verbose:
            for p in result.skipped:
                info_output(f"Skipped PMTiles (up-to-date): {p.name}")
    for path, error_msg in result.failed:
        if explicit_flag:
            if not use_json:
                error(f"PMTiles generation failed: {error_msg}")
            raise SystemExit(1)
        if not use_json:
            warn(f"Failed to generate PMTiles for '{path}': {error_msg}")


def _handle_pmtiles_unavailable(
    e: Exception,
    explicit_flag: bool,
    config_enabled: bool,
    verbose: bool,
    coll_id: str,
    *,
    use_json: bool = False,
) -> None:
    """Handle PMTilesNotAvailableError."""
    if explicit_flag:
        if not use_json:
            error(str(e))
        raise SystemExit(1) from e
    if use_json:
        return
    # Warn if pmtiles.enabled in config (user expectation), or info if just verbose
    if config_enabled:
        warn(f"PMTiles enabled for '{coll_id}' but gpio-pmtiles not installed")
    elif verbose:
        info_output(f"Skipping PMTiles for '{coll_id}': gpio-pmtiles not installed")


def _handle_tippecanoe_missing(
    e: Exception, explicit_flag: bool, coll_id: str, *, use_json: bool = False
) -> None:
    """Handle TippecanoeNotFoundError."""
    if explicit_flag:
        if not use_json:
            error(str(e))
        raise SystemExit(1) from e
    if not use_json:
        warn(f"Skipping PMTiles for '{coll_id}': tippecanoe not installed")


def _coerce_bool(value: Any, *, default: bool = False) -> bool:
    """Coerce a value to boolean.

    Handles string values like "true", "1", "yes" from env vars.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "on")
    return bool(value)


def _coerce_int(value: Any, *, default: int) -> int:
    """Coerce a value to integer with safe fallback."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


@cli.command("add")
@click.argument(
    "paths",
    type=click.Path(exists=True, path_type=Path),
    nargs=-1,
    required=True,
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Show detailed output including skipped unchanged files.",
)
@click.option(
    "--item-id",
    default=None,
    help="Override automatic item ID derivation. Must be a single path segment.",
)
@click.option(
    "--portolan-dir",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to Portolan catalog root (default: auto-detect by walking up from cwd).",
)
@click.option(
    "--datetime",
    "item_datetime",
    type=FLEXIBLE_DATETIME,
    default=None,
    help=(
        "Acquisition/creation datetime (ISO 8601, YYYY-MM-DD, or 'YYYY-MM-DD HH:MM:SS'). "
        "Applied to ALL items in this command. For different datetimes per item, "
        "run separate add commands. If omitted, items are marked as provisional "
        "(portolan check will flag them)."
    ),
)
@click.option(
    "--workers",
    type=int,
    default=1,
    help=(
        "Number of parallel workers for metadata extraction. "
        "Default is 1 (sequential). Use higher values for large catalogs."
    ),
)
@click.option(
    "--stac-geoparquet",
    "generate_parquet",
    is_flag=True,
    help="Generate items.parquet for affected collections after add.",
)
@click.option(
    "--pmtiles",
    "generate_pmtiles",
    is_flag=True,
    help="Generate PMTiles from GeoParquet assets (requires tippecanoe).",
)
@click.option(
    "--force-pmtiles",
    "force_pmtiles",
    is_flag=True,
    help="Regenerate PMTiles even if they exist and are up-to-date.",
)
@click.pass_context
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
def add_cmd(
    ctx: click.Context,
    json_output: bool,
    paths: tuple[Path, ...],
    verbose: bool,
    item_id: str | None,
    catalog_path: Path | None,
    item_datetime: datetime | None,
    workers: int,
    generate_parquet: bool,
    generate_pmtiles: bool,
    force_pmtiles: bool,
) -> None:
    """Track files in the catalog.

    Accepts multiple paths like git add. Each path is processed independently
    with automatic collection inference based on directory structure.

    Works like git: run from anywhere inside a catalog and it auto-detects
    the catalog root. Use --portolan-dir to override.

    \b
    Item ID derivation:
        By default, the item ID is derived from the parent directory name.
        For example, adding 'census/2020/data.parquet' creates an item
        named '2020'. Use --item-id to override this automatic derivation.
        All other files in the item directory are tracked as companion
        assets (per ADR-0028).

    \b
    Datetime handling (per ADR-0035):
        --datetime applies to ALL items added in this command. For items
        with different acquisition dates, run separate add commands:

            portolan add census/2020/ --datetime 2020-04-01
            portolan add census/2023/ --datetime 2023-04-01

        If --datetime is omitted, items have null temporal extent and are
        marked as provisional. Run 'portolan check' to find items needing dates.

    \b
    Examples:
        portolan add demographics/census.parquet
        portolan add file1.geojson file2.geojson   # Add multiple files
        portolan add imagery/                      # Add all files in directory
        portolan add .                             # Add all files in catalog
        portolan add data.geojson --item-id my-id  # Override item ID (single file only)
        portolan add sat.tif --datetime 2024-06-15 # Explicit acquisition date

    Smart behavior:
    - Unchanged files are silently skipped (use --verbose to see them)
    - Changed files are re-extracted with new metadata
    - Sidecar files (.dbf, .shx, .prj for shapefiles) are auto-detected
    - All files in the item directory are tracked, not just geo files (ADR-0028)
    """
    use_json = should_output_json(ctx, json_output)

    # Resolve and validate catalog root (git-style auto-detection)
    catalog_root = _resolve_catalog_root_for_add(catalog_path, use_json)

    # Validate --item-id usage (only valid with single file, not directory)
    _validate_item_id_usage(item_id, paths, use_json)

    # Resolve all CLI paths upfront and deduplicate by resolved path.
    # Using dict.fromkeys preserves order while deduplicating.
    resolved_paths: list[Path] = list(dict.fromkeys(p.resolve() for p in paths))

    # For single-file adds, validate that the file is in a collection subdirectory.
    # This catches the error early with a clear message, matching pre-batch behavior.
    if len(resolved_paths) == 1 and not resolved_paths[0].is_dir():
        single_path = resolved_paths[0]
        try:
            resolve_collection_id(single_path, catalog_root)
        except ValueError as err:
            _handle_cmd_error("add", "PathError", str(err), use_json)
            raise SystemExit(1) from err

    # Call add_files once with all resolved paths.
    # We pass collection_id=None so that add_files infers the collection per-file
    # from directory structure. This handles:
    # - `portolan add .` (catalog-root add, multiple collections)
    # - `portolan add file1 file2` (files from different collections)
    # - `portolan add file1 file2` (files from same collection)
    # Per ADR-0028, add_files deduplicates paths internally.
    #
    # NOTE: We intentionally do NOT do per-path collection inference in the CLI.
    # add_files already does this correctly when collection_id=None, and batching
    # avoids duplicate item writes when the same item directory appears via
    # multiple CLI arguments (e.g. `portolan add . foo/data.parquet`).

    # Pre-count files for progress bar (ADR-0040: unified progress output)
    # Only count when progress will be displayed:
    # - Not JSON mode (agents get structured output)
    # - Not verbose mode (verbose gets per-file output instead)
    # - TTY available (progress bars need terminal)
    should_show_progress = not use_json and not verbose and sys.stderr.isatty()
    total_files = count_files(resolved_paths) if should_show_progress else 0

    # Create progress reporter (suppressed unless should_show_progress)
    progress_reporter = AddProgressReporter(
        total_files=total_files, json_mode=not should_show_progress
    )

    # Progress callback: verbose mode prints per-file, otherwise advances progress bar
    def on_file_progress(file_path: Path) -> None:
        if verbose and not use_json:
            info_output(f"Adding: {file_path.name}")
        if should_show_progress:
            progress_reporter.advance()

    # item_datetime is parsed by Click via FLEXIBLE_DATETIME type (ADR-0035)
    try:
        with progress_reporter:
            all_added, all_skipped, all_failures = add_files(
                paths=resolved_paths,
                catalog_root=catalog_root,
                collection_id=None,
                item_id=item_id,
                item_datetime=item_datetime,
                verbose=verbose,
                on_progress=on_file_progress,
                workers=workers,
                json_mode=use_json,
            )
    except (ValueError, FileNotFoundError) as err:
        err_type = type(err).__name__
        # Include failed path context in error message when there's only one path
        path_context = f"{resolved_paths[0]}: " if len(resolved_paths) == 1 else ""
        _handle_cmd_error("add", err_type, f"{path_context}{err}", use_json)
        raise SystemExit(1) from err

    # Compute affected collections before any post-processing
    # Include both added AND skipped assets so --pmtiles works on already-tracked files
    # Note: all_added contains DatasetInfo objects, all_skipped contains Path objects
    affected: set[str] = set()
    for a in all_added:
        if hasattr(a, "collection_id") and a.collection_id:
            affected.add(a.collection_id)
    for p in all_skipped:
        # Extract collection_id from path relative to catalog_root
        try:
            rel = p.relative_to(catalog_root)
            # Collection ID is the first path component
            if rel.parts:
                affected.add(rel.parts[0])
        except ValueError:
            pass  # Path not relative to catalog_root

    # Handle stac-geoparquet generation BEFORE output (so JSON reflects final state)
    # Always run parquet generation if --stac-geoparquet flag was passed, regardless of output mode
    # Only show hints in non-JSON mode
    _handle_parquet_after_add(
        catalog_root, affected, generate_parquet, verbose, show_hints=not use_json
    )

    # Handle PMTiles generation BEFORE output (so JSON reflects final state)
    _handle_pmtiles_after_add(
        catalog_root, affected, generate_pmtiles, force_pmtiles, verbose, use_json=use_json
    )

    # Output combined results (after all processing complete)
    _output_add_results(all_added, all_skipped, all_failures, verbose, use_json)

    # Exit with non-zero code if any failures occurred
    if all_failures:
        raise SystemExit(1)


@cli.command("rm")
@click.argument("path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--keep",
    is_flag=True,
    help="Untrack file but preserve it on disk.",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Force deletion without safety check. Required for destructive rm.",
)
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    help="Show what would be removed without actually removing.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Show detailed output including skipped files.",
)
@click.option(
    "--portolan-dir",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to Portolan catalog root (default: auto-detect by walking up from cwd).",
)
@click.pass_context
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
def rm_cmd(
    ctx: click.Context,
    json_output: bool,
    path: Path,
    keep: bool,
    force: bool,
    dry_run: bool,
    verbose: bool,
    catalog_path: Path | None,
) -> None:
    """Remove files from tracking.

    By default, removes the file from disk AND untracks it from the catalog.
    Requires --force for destructive operations (deleting files).

    Works like git: run from anywhere inside a catalog and it auto-detects
    the catalog root. Use --portolan-dir to override.

    \b
    Safety flags:
    - --keep: Untrack file but preserve it on disk (safe, no --force needed)
    - --force: Required for destructive rm (when not using --keep)
    - --dry-run: Preview what would be removed without actually removing

    \b
    Examples:
        portolan rm --keep imagery/old_data.tif     # Safe: untrack only
        portolan rm --dry-run vectors/              # Preview what would be removed
        portolan rm -f demographics/census.parquet  # Force delete and untrack
        portolan rm -f vectors/                     # Force remove entire directory
    """
    use_json = should_output_json(ctx, json_output)

    # Require --force for destructive operations (unless --keep or --dry-run)
    if not keep and not dry_run and not force:
        _handle_cmd_error(
            "rm",
            "SafetyError",
            "Destructive rm requires --force flag. Use --keep to preserve files, or --dry-run to preview.",
            use_json,
        )
        if not use_json:
            detail("Examples:")
            detail("  portolan rm --keep myfile.parquet  # Untrack but keep file")
            detail("  portolan rm --dry-run myfile.parquet  # Preview removal")
            detail("  portolan rm -f myfile.parquet  # Force delete")
        raise SystemExit(1)

    # Auto-detect catalog root (git-style)
    catalog_root: Path
    if catalog_path is not None:
        catalog_root = catalog_path.resolve()
    else:
        detected_root = find_catalog_root()
        if detected_root is None:
            _handle_cmd_error(
                "rm",
                "NotACatalogError",
                "Not inside a Portolan catalog (no .portolan/config.yaml found)",
                use_json,
            )
            if not use_json:
                detail("Run 'portolan init' to create a catalog, or cd into one")
            raise SystemExit(1)
        catalog_root = detected_root

    try:
        target_path = path.resolve()

        removed, skipped = remove_files(
            paths=[target_path],
            catalog_root=catalog_root,
            keep=keep,
            dry_run=dry_run,
        )

        if use_json:
            data = {
                "removed": [str(p) for p in removed],
                "skipped": [str(p) for p in skipped],
                "kept_on_disk": keep,
                "dry_run": dry_run,
            }
            envelope = success_envelope("rm", data)
            output_json_envelope(envelope)
        else:
            if dry_run:
                info_output("Dry run - no files were actually removed:")
            for p in removed:
                if dry_run:
                    detail(f"  Would remove: {p.name}")
                elif keep:
                    success(f"Untracked {p.name} (file preserved)")
                else:
                    success(f"Removed {p.name}")

            # Show skipped files in verbose mode
            if verbose and skipped:
                for p in skipped:
                    warn(f"Skipped {p.name} (not in catalog or outside catalog)")

    except ValueError as err:
        _handle_cmd_error("rm", "ValueError", str(err), use_json)
        raise SystemExit(1) from err
    except FileNotFoundError as err:
        _handle_cmd_error("rm", "FileNotFoundError", str(err), use_json)
        raise SystemExit(1) from err


# ─────────────────────────────────────────────────────────────────────────────
# Push command
# ─────────────────────────────────────────────────────────────────────────────


def _check_backend_push_support(
    catalog_path: Path,
    collection: str | None,
    use_json: bool,
) -> None:
    """Exit with error if the active backend does not support file-based push."""
    from portolan_cli.version_ops import check_backend_supports_push

    result = check_backend_supports_push(catalog_path, collection)
    if result is None:
        return

    if use_json:
        envelope = error_envelope(
            "push", [ErrorDetail(type="NotImplementedError", message=result.message)]
        )
        output_json_envelope(envelope)
    else:
        error(result.message)
    raise SystemExit(1)


def _resolve_push_settings(
    destination: str | None,
    profile: str | None,
    catalog_path: Path,
    collection: str | None,
    use_json: bool,
    command: str,
) -> tuple[str | None, str, str | None]:
    """Resolve remote/profile/region for push/sync commands.

    Args:
        destination: CLI destination argument
        profile: CLI profile argument
        catalog_path: Resolved catalog path
        collection: Optional collection name
        use_json: Whether to output JSON
        command: Command name for error messages

    Returns:
        Tuple of (resolved_destination, resolved_profile, resolved_region)

    Raises:
        SystemExit: If config.yaml contains stale sensitive settings
    """
    try:
        resolved_destination = resolve_remote(destination, catalog_path, collection)
        resolved_profile = resolve_aws_profile(profile, catalog_path, collection)
        resolved_region = resolve_aws_region(None, catalog_path, collection)
        return resolved_destination, resolved_profile, resolved_region
    except ValueError as e:
        if use_json:
            envelope = error_envelope(command, [ErrorDetail(type="ConfigError", message=str(e))])
            output_json_envelope(envelope)
        else:
            error(str(e))
        raise SystemExit(1) from None


def _prepare_push_concurrency(
    concurrency: int,
    chunk_concurrency: int,
    max_connections: int | None,
    workers: int | None,
    collection: str | None,
    use_json: bool,
) -> tuple[int, int]:
    """Compute effective concurrency and warn if too high (Issue #344).

    Returns:
        Tuple of (effective_file_concurrency, effective_chunk_concurrency).
    """
    from portolan_cli.async_utils import (
        MAX_SAFE_CONNECTIONS,
        adjust_concurrency_for_max_connections,
        calculate_connection_footprint,
    )

    # Apply max_connections cap
    effective_file = concurrency
    effective_chunk = chunk_concurrency
    if max_connections is not None:
        effective_file, effective_chunk = adjust_concurrency_for_max_connections(
            concurrency, chunk_concurrency, max_connections
        )

    # Warn if footprint exceeds safe threshold
    eff_workers = workers if workers is not None else (4 if collection is None else 1)
    footprint = calculate_connection_footprint(effective_file, effective_chunk, eff_workers)
    if footprint > MAX_SAFE_CONNECTIONS and not use_json:
        worker_part = f"{eff_workers} workers × " if eff_workers > 1 else ""
        warn(
            f"High connection count: {footprint} concurrent connections "
            f"({worker_part}{effective_file} files × {effective_chunk} chunks). "
            "This may overwhelm home networks. Consider using --max-connections to limit."
        )

    return effective_file, effective_chunk


@cli.command()
@click.argument("destination", required=False, default=None)
@click.option(
    "--collection",
    "-c",
    required=False,
    default=None,
    help="Collection to push. If not specified, pushes all collections.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Overwrite remote even if it has diverged.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be pushed without uploading. Note: skips remote state check (no network I/O), so conflicts won't be detected.",
)
@click.option(
    "--profile",
    default=None,
    help="AWS profile name (for S3 destinations). Uses config or 'default' if not specified.",
)
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to catalog root (default: auto-detect by walking up from cwd).",
)
@click.option(
    "--workers",
    "-w",
    type=click.IntRange(min=1),
    default=None,
    help="Parallel workers for catalog-wide push (default: auto-detect based on CPU count; "
    "use 1 for sequential). Ignored when --collection is specified.",
)
@click.option(
    "--concurrency",
    type=click.IntRange(min=1, max=500),
    default=8,
    help="Maximum concurrent file uploads within each collection (default: 8). "
    "Per-worker connections = concurrency × chunk-concurrency; "
    "catalog-wide total = workers × concurrency × chunk-concurrency.",
)
@click.option(
    "--chunk-concurrency",
    type=click.IntRange(min=1, max=50),
    default=4,
    help="Maximum concurrent chunks per file upload (default: 4). "
    "Per-worker connections = concurrency × chunk-concurrency; "
    "catalog-wide total = workers × concurrency × chunk-concurrency. "
    "Lower values are safer for home networks.",
)
@click.option(
    "--max-connections",
    type=click.IntRange(min=1),
    default=None,
    help="Maximum total concurrent HTTP connections. If set, auto-adjusts "
    "concurrency and chunk-concurrency to stay within limit. "
    "Recommended for flaky or metered connections.",
)
@click.option(
    "--adaptive/--no-adaptive",
    default=True,
    help="Enable adaptive concurrency (default: on). Starts with low concurrency, "
    "ramps up on success, backs off on errors. Safer for home networks.",
)
@click.pass_context
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Show per-file upload details with size and speed.",
)
def push(
    ctx: click.Context,
    json_output: bool,
    verbose: bool,
    destination: str | None,
    collection: str | None,
    force: bool,
    dry_run: bool,
    profile: str | None,
    catalog_path: Path | None,
    workers: int | None,
    concurrency: int,
    chunk_concurrency: int,
    max_connections: int | None,
    adaptive: bool,
) -> None:
    """Push local catalog changes to cloud object storage.

    Git-style behavior: automatically finds the catalog root by walking up
    from the current directory. Works from any subdirectory within a catalog.
    Use --catalog to override and specify an explicit path.

    Syncs collection(s) to a remote destination (S3, GCS, Azure).
    Uses optimistic locking to detect concurrent modifications.

    DESTINATION is the object store URL (e.g., s3://mybucket/my-catalog).
    If not provided, uses 'remote' from PORTOLAN_REMOTE env var or .env file.

    If --collection is specified, pushes that collection only.
    If --collection is omitted, pushes all collections in the catalog.

    \b
    Examples:
        # Push a single collection
        portolan push s3://mybucket/catalog --collection demographics
        portolan push gs://mybucket/catalog -c imagery --dry-run

        # Push all collections
        portolan push s3://mybucket/catalog
        portolan push --dry-run  # Uses configured remote
    """
    import asyncio

    from portolan_cli.push import PushConflictError, push_all_collections, push_async

    use_json = should_output_json(ctx, json_output)

    # Git-style: find catalog root from anywhere within the catalog
    # Use explicit --catalog if provided, otherwise auto-detect
    if catalog_path is None:
        catalog_path = require_catalog_root(use_json, "push")

    # Load .env for credentials (must happen before any config reads)
    load_dotenv_and_warn_sensitive(catalog_path)

    # Check if active backend supports push
    _check_backend_push_support(catalog_path, collection, use_json)

    # Resolve remote/profile/region (raises SystemExit on stale config)
    resolved_destination, resolved_profile, resolved_region = _resolve_push_settings(
        destination, profile, catalog_path, collection, use_json, "push"
    )

    if resolved_destination is None:
        if use_json:
            envelope = error_envelope(
                "push",
                [
                    ErrorDetail(
                        type="UsageError",
                        message="No destination provided and no 'remote' configured. "
                        "Provide a DESTINATION argument or set PORTOLAN_REMOTE env var (or add to .env)",
                    )
                ],
            )
            output_json_envelope(envelope)
        else:
            error(
                "No destination provided and no 'remote' configured. "
                "Provide a DESTINATION argument or set PORTOLAN_REMOTE env var (or add to .env)"
            )
        raise SystemExit(1)

    # Apply max_connections cap and warn about high connection count (Issue #344)
    effective_file_conc, effective_chunk_conc = _prepare_push_concurrency(
        concurrency, chunk_concurrency, max_connections, workers, collection, use_json
    )

    # If no collection specified, push all collections
    if collection is None:
        try:
            all_result = push_all_collections(
                catalog_root=catalog_path,
                destination=resolved_destination,
                force=force,
                dry_run=dry_run,
                profile=resolved_profile,
                region=resolved_region,
                workers=workers,
                file_concurrency=effective_file_conc,
                chunk_concurrency=effective_chunk_conc,
                max_connections=max_connections,
                adaptive=adaptive,
                verbose=verbose,
                json_mode=use_json,
            )

            if use_json:
                envelope = success_envelope(
                    "push",
                    {
                        "total_collections": all_result.total_collections,
                        "successful_collections": all_result.successful_collections,
                        "failed_collections": all_result.failed_collections,
                        "total_files_uploaded": all_result.total_files_uploaded,
                        "total_versions_pushed": all_result.total_versions_pushed,
                        "collection_errors": all_result.collection_errors,
                    },
                )
                output_json_envelope(envelope)
            # Terminal output is handled by push_all_collections()

            if not all_result.success:
                raise SystemExit(1)

            return

        except Exception as err:
            if use_json:
                envelope = error_envelope(
                    "push",
                    [ErrorDetail(type=type(err).__name__, message=str(err))],
                )
                output_json_envelope(envelope)
            else:
                error(str(err))
            raise SystemExit(1) from err

    try:
        # Use async push for single-collection push (concurrent uploads)
        # max_connections already applied above via effective_*_conc
        result = asyncio.run(
            push_async(
                catalog_root=catalog_path,
                collection=collection,
                destination=resolved_destination,
                force=force,
                dry_run=dry_run,
                profile=resolved_profile,
                region=resolved_region,
                concurrency=effective_file_conc,
                chunk_concurrency=effective_chunk_conc,
                adaptive=adaptive,
                json_mode=use_json,
            )
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
                elif dry_run:
                    # Dry-run mode: push.py already printed what would be done
                    info_output("[DRY RUN] Complete - no files were uploaded")
                else:
                    info_output("Nothing to push - local and remote are in sync")
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
            info_output("Use --force to overwrite, or pull remote changes first")
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


def _output_pull_human(result: PullResult, *, dry_run: bool) -> None:
    """Output human-readable pull result (reduces complexity in main command)."""
    if result.up_to_date:
        info_output("Already up to date")
    elif result.success:
        if dry_run:
            info_output(f"[DRY RUN] Would pull {result.files_downloaded} file(s)")
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


def _try_backend_pull(
    catalog_path: Path,
    remote_url: str,
    collection: str | None,
    dry_run: bool,
    use_json: bool,
) -> bool:
    """Attempt a pull via the active non-file backend.

    Returns True if the backend handled the pull (caller should return).
    Returns False if the backend has no pull() method and the file-based pull
    should proceed as normal.
    """
    from portolan_cli.version_ops import try_backend_pull

    result = try_backend_pull(catalog_path, remote_url, collection, dry_run)
    if not result.handled:
        return False

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
            envelope = error_envelope(
                "pull",
                [ErrorDetail(type="PullError", message="Pull failed")],
                data=data,
            )
        output_json_envelope(envelope)
    else:
        # Create a minimal PullResult-like object for _output_pull_human
        from types import SimpleNamespace
        from typing import cast

        pull_result = cast(
            "PullResult",
            SimpleNamespace(
                success=result.success,
                files_downloaded=result.files_downloaded,
                files_skipped=result.files_skipped,
                local_version=result.local_version,
                remote_version=result.remote_version,
                up_to_date=result.up_to_date,
                uncommitted_changes=[],
            ),
        )
        _output_pull_human(pull_result, dry_run=dry_run)

    if not result.success:
        raise SystemExit(1)
    return True


@cli.command()
@click.argument("remote_url")
@click.option(
    "--collection",
    "-c",
    default=None,
    help="Collection to pull. If not specified, pulls all collections.",
)
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to catalog root (default: auto-detect by walking up from cwd).",
)
@click.option(
    "--force",
    is_flag=True,
    help="Discard uncommitted local changes and overwrite with remote.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be downloaded without actually downloading. Note: skips remote state check (no network I/O), so remote changes won't be detected.",
)
@click.option(
    "--restore",
    is_flag=True,
    help="Re-download files that are missing locally even if version metadata matches. Use to recover accidentally deleted files. Note: slower than normal pull (checks file existence).",
)
@click.option(
    "--profile",
    type=str,
    default=None,
    help="AWS profile name (for S3). Uses config or 'default' if not specified.",
)
@click.option(
    "--workers",
    "-w",
    type=click.IntRange(min=1),
    default=None,
    help=(
        "Parallel workers for catalog-wide pull (default: auto-detect based on "
        "CPU count; use 1 for sequential). Ignored when --collection is specified."
    ),
)
@click.option(
    "--concurrency",
    type=click.IntRange(min=1),
    default=50,
    help=(
        "Maximum concurrent file downloads within each collection (default: 50). "
        "Higher values speed up downloads but use more connections."
    ),
)
@click.pass_context
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
def pull_command(
    ctx: click.Context,
    json_output: bool,
    remote_url: str,
    collection: str | None,
    catalog_path: Path | None,
    force: bool,
    dry_run: bool,
    restore: bool,
    profile: str | None,
    workers: int | None,
    concurrency: int,
) -> None:
    """Pull updates from a remote catalog.

    Git-style behavior: automatically finds the catalog root by walking up
    from the current directory. Works from any subdirectory within a catalog.
    Use --catalog to override and specify an explicit path.

    Fetches changes from a remote catalog and downloads updated files.
    Similar to `git pull`, this checks for uncommitted local changes before
    overwriting.

    REMOTE_URL is the remote catalog URL (e.g., s3://bucket/catalog).

    If --collection is specified, pulls that collection only. If --collection
    is omitted, pulls all collections in the catalog.

    \b
    Examples:
        # Pull a single collection
        portolan pull s3://mybucket/my-catalog --collection demographics
        portolan pull s3://mybucket/catalog -c imagery --dry-run

        # Pull all collections
        portolan pull s3://mybucket/catalog
        portolan pull s3://mybucket/catalog --workers 4
    """
    from portolan_cli.pull import pull as pull_fn
    from portolan_cli.pull import pull_all_collections

    use_json = should_output_json(ctx, json_output)

    # Git-style: find catalog root from anywhere within the catalog
    # Use explicit --catalog if provided, otherwise auto-detect
    if catalog_path is None:
        catalog_path = require_catalog_root(use_json, "pull")

    # Load .env for credentials (must happen after catalog_path resolution)
    load_dotenv_and_warn_sensitive(catalog_path)

    # Resolve profile - wrap in try/except for stale sensitive config
    try:
        resolved_profile = resolve_aws_profile(profile, catalog_path, collection)
    except ValueError as e:
        if use_json:
            envelope = error_envelope("pull", [ErrorDetail(type="ConfigError", message=str(e))])
            output_json_envelope(envelope)
        else:
            error(str(e))
        raise SystemExit(1) from None

    # Route to backend-specific pull if using non-file backend
    if _try_backend_pull(catalog_path, remote_url, collection, dry_run, use_json):
        return

    # Catalog-wide pull (no --collection specified)
    if collection is None:
        try:
            all_result = pull_all_collections(
                remote_url=remote_url,
                local_root=catalog_path,
                force=force,
                dry_run=dry_run,
                restore=restore,
                profile=resolved_profile,
                workers=workers,
                file_concurrency=concurrency,
            )

            if use_json:
                data = {
                    "total_collections": all_result.total_collections,
                    "successful_collections": all_result.successful_collections,
                    "failed_collections": all_result.failed_collections,
                    "total_files_downloaded": all_result.total_files_downloaded,
                    "collection_errors": all_result.collection_errors,
                }

                if all_result.success:
                    envelope = success_envelope("pull", data)
                else:
                    errors = [
                        ErrorDetail(
                            type="PullError",
                            message=f"{coll}: {', '.join(errs)}",
                        )
                        for coll, errs in all_result.collection_errors.items()
                    ]
                    envelope = error_envelope("pull", errors, data=data)

                output_json_envelope(envelope)

            if not all_result.success:
                raise SystemExit(1)

            return

        except Exception as err:
            if use_json:
                envelope = error_envelope(
                    "pull",
                    [ErrorDetail(type=type(err).__name__, message=str(err))],
                )
                output_json_envelope(envelope)
            else:
                error(str(err))
            raise SystemExit(1) from err

    # Single collection pull
    try:
        single_result = pull_fn(
            remote_url=remote_url,
            local_root=catalog_path,
            collection=collection,
            force=force,
            dry_run=dry_run,
            restore=restore,
            profile=resolved_profile,
            concurrency=concurrency,
        )

        if use_json:
            data = {
                "files_downloaded": single_result.files_downloaded,
                "files_skipped": single_result.files_skipped,
                "files_restored": single_result.files_restored,
                "local_version": single_result.local_version,
                "remote_version": single_result.remote_version,
                "up_to_date": single_result.up_to_date,
            }

            if single_result.success:
                envelope = success_envelope("pull", data)
            else:
                errors = []
                if single_result.uncommitted_changes:
                    errors.append(
                        ErrorDetail(
                            type="UncommittedChangesError",
                            message=f"Uncommitted changes: {', '.join(single_result.uncommitted_changes)}",
                        )
                    )
                else:
                    errors.append(ErrorDetail(type="PullError", message="Pull failed"))
                envelope = error_envelope("pull", errors, data=data)

            output_json_envelope(envelope)
        else:
            _output_pull_human(single_result, dry_run=dry_run)

        if not single_result.success:
            raise SystemExit(1)

    except FileNotFoundError as err:
        if use_json:
            envelope = error_envelope(
                "pull",
                [ErrorDetail(type="FileNotFoundError", message=str(err))],
            )
            output_json_envelope(envelope)
        else:
            error(str(err))
        raise SystemExit(1) from err

    except ValueError as err:
        if use_json:
            envelope = error_envelope(
                "pull",
                [ErrorDetail(type="ValueError", message=str(err))],
            )
            output_json_envelope(envelope)
        else:
            error(str(err))
        raise SystemExit(1) from err


# ─────────────────────────────────────────────────────────────────────────────
# Sync command
# ─────────────────────────────────────────────────────────────────────────────


@cli.command()
@click.argument("destination", required=False, default=None)
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
    default=None,
    help="AWS profile name (for S3 destinations). Uses config or 'default' if not specified.",
)
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to catalog root (default: auto-detect by walking up from cwd).",
)
@click.pass_context
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
def sync(
    ctx: click.Context,
    json_output: bool,
    destination: str | None,
    collection: str,
    force: bool,
    dry_run: bool,
    fix: bool,
    profile: str | None,
    catalog_path: Path | None,
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
        portolan sync --collection demographics  # Uses configured remote
    """
    from portolan_cli.sync import sync as sync_fn

    use_json = should_output_json(ctx, json_output)

    # Git-style: find catalog root from anywhere within the catalog
    # Use explicit --catalog if provided, otherwise auto-detect
    if catalog_path is None:
        catalog_path = require_catalog_root(use_json, "sync")

    # Load .env for credentials (must happen after catalog_path resolution)
    load_dotenv_and_warn_sensitive(catalog_path)

    # Resolve remote/profile/region (raises SystemExit on stale config)
    resolved_destination, resolved_profile, resolved_region = _resolve_push_settings(
        destination, profile, catalog_path, collection, use_json, "sync"
    )

    if resolved_destination is None:
        if use_json:
            envelope = error_envelope(
                "sync",
                [
                    ErrorDetail(
                        type="UsageError",
                        message="No destination provided and no 'remote' configured. "
                        "Provide a DESTINATION argument or set PORTOLAN_REMOTE env var (or add to .env)",
                    )
                ],
            )
            output_json_envelope(envelope)
        else:
            error(
                "No destination provided and no 'remote' configured. "
                "Provide a DESTINATION argument or set PORTOLAN_REMOTE env var (or add to .env)"
            )
        raise SystemExit(1)

    result = sync_fn(
        catalog_root=catalog_path,
        collection=collection,
        destination=resolved_destination,
        force=force,
        dry_run=dry_run,
        fix=fix,
        profile=resolved_profile,
        region=resolved_region,
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
                info_output("[DRY RUN] Sync completed successfully")
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
    required=False,
    default=None,
)
@click.option(
    "--collection",
    "-c",
    required=False,
    default=None,
    help="Collection to clone. If not specified, clones all collections.",
)
@click.option(
    "--profile",
    default=None,
    help="AWS profile name (for S3 sources). Uses env var or 'default' if not specified.",
)
@click.pass_context
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
def clone(
    ctx: click.Context,
    json_output: bool,
    remote_url: str,
    local_path: Path | None,
    collection: str | None,
    profile: str | None,
) -> None:
    """Clone a remote catalog to a local directory.

    This is essentially "pull to an empty directory" with guardrails.
    Creates the target directory and pulls collections from remote storage.

    REMOTE_URL is the object store URL (e.g., s3://mybucket/my-catalog).

    LOCAL_PATH is optional - if not provided, it will be inferred from the
    catalog name in the URL (git clone style).

    --collection is optional - if not provided, all collections in the remote
    catalog will be cloned.

    \b
    Examples:
        # Infer directory from URL, clone all collections
        portolan clone s3://mybucket/my-catalog

        # Clone to current directory (must be empty)
        portolan clone s3://mybucket/my-catalog .

        # Clone specific collection
        portolan clone s3://mybucket/catalog -c demographics

        # Clone all collections to specific directory
        portolan clone s3://mybucket/catalog ./local-copy

        # Clone specific collection with profile
        portolan clone s3://mybucket/catalog ./data -c imagery --profile prod
    """
    from portolan_cli.sync import clone as clone_fn
    from portolan_cli.sync import infer_local_path_from_url

    use_json = should_output_json(ctx, json_output)

    # Resolve profile: CLI arg > env var > default (no local catalog yet)
    resolved_profile = resolve_aws_profile(profile, catalog_path=None)

    # Infer local_path from URL if not provided
    if local_path is None:
        try:
            local_path = infer_local_path_from_url(remote_url)
            if not use_json:
                info_output(f"Inferred local path: {local_path}")
        except ValueError as e:
            if use_json:
                envelope = error_envelope(
                    "clone",
                    [ErrorDetail(type="CloneError", message=str(e), code="INVALID_URL")],
                )
                output_json_envelope(envelope)
            else:
                error(str(e))
            raise SystemExit(1) from None

    result = clone_fn(
        remote_url=remote_url,
        local_path=local_path,
        collection=collection,
        profile=resolved_profile,
    )

    if use_json:
        data: dict[str, Any] = {
            "local_path": str(result.local_path),
            "collections_cloned": result.collections_cloned,
            "total_files_downloaded": result.total_files_downloaded,
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

        output_json_envelope(envelope)
    else:
        if result.success:
            if result.collections_cloned:
                success(
                    f"Clone completed: {result.local_path} "
                    f"({len(result.collections_cloned)} collection(s), "
                    f"{result.total_files_downloaded} file(s))"
                )
            else:
                success(f"Clone completed: {result.local_path}")
        else:
            if result.errors:
                for err_msg in result.errors:
                    error(err_msg)
            error("Clone failed")

    if not result.success:
        raise SystemExit(1)


# ─────────────────────────────────────────────────────────────────────────────
# Config Commands
# ─────────────────────────────────────────────────────────────────────────────


@cli.group()
def config() -> None:
    """Manage catalog configuration.

    Configuration is stored in .portolan/config.yaml and follows this precedence:

    \b
    1. CLI argument (highest)
    2. Environment variable (PORTOLAN_<KEY>) or .env file
    3. Collection-level config
    4. Catalog-level config
    5. Built-in default (lowest)

    Note: Sensitive settings (remote, profile, region) must use env vars or .env.

    \b
    Examples:
        portolan config set backend iceberg
        portolan config get remote
        portolan config list
    """


@config.command("set")
@click.argument("key")
@click.argument("value")
@click.option(
    "--collection",
    "-c",
    type=str,
    default=None,
    help="Set config for a specific collection instead of catalog-level.",
)
@click.pass_context
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
def config_set(
    ctx: click.Context, json_output: bool, key: str, value: str, collection: str | None
) -> None:
    """Set a configuration value.

    KEY is the setting name (e.g., backend, statistics.enabled).
    VALUE is the value to set.

    Note: Sensitive settings (remote, profile, region) cannot be stored in
    config.yaml. Use environment variables or .env files instead.

    \b
    Examples:
        portolan config set backend iceberg
        portolan config set statistics.enabled true
        portolan config set pmtiles.enabled false --collection demographics
    """
    from portolan_cli.config import set_setting

    use_json = should_output_json(ctx, json_output)

    # Find catalog root
    catalog_path = find_catalog_root()
    if catalog_path is None:
        if use_json:
            envelope = error_envelope(
                "config set",
                [ErrorDetail(type="CatalogNotFoundError", message="Not in a Portolan catalog")],
            )
            output_json_envelope(envelope)
        else:
            error("Not in a Portolan catalog")
            info_output("Run 'portolan init' to create one")
        raise SystemExit(1)

    try:
        set_setting(catalog_path, key, value, collection=collection)

        if use_json:
            data = {"key": key, "value": value}
            if collection:
                data["collection"] = collection
            envelope = success_envelope("config set", data)
            output_json_envelope(envelope)
        else:
            if collection:
                success(f"Set {key}={value} for collection '{collection}'")
            else:
                success(f"Set {key}={value}")

    except Exception as e:
        if use_json:
            envelope = error_envelope(
                "config set",
                [ErrorDetail(type=type(e).__name__, message=str(e))],
            )
            output_json_envelope(envelope)
        else:
            error(f"Failed to set config: {e}")
        raise SystemExit(1) from e


@config.command("get")
@click.argument("key")
@click.option(
    "--collection",
    "-c",
    type=str,
    default=None,
    help="Get config for a specific collection.",
)
@click.pass_context
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
def config_get(ctx: click.Context, json_output: bool, key: str, collection: str | None) -> None:
    """Get a configuration value.

    Shows the resolved value and its source (env, catalog, collection, or not set).

    KEY is the setting name (e.g., remote, aws_profile).

    \b
    Examples:
        portolan config get remote
        portolan config get aws_profile --collection restricted
    """
    from portolan_cli.config import get_setting, get_setting_source

    use_json = should_output_json(ctx, json_output)

    # Find catalog root
    catalog_path = find_catalog_root()
    if catalog_path is None:
        if use_json:
            envelope = error_envelope(
                "config get",
                [ErrorDetail(type="CatalogNotFoundError", message="Not in a Portolan catalog")],
            )
            output_json_envelope(envelope)
        else:
            error("Not in a Portolan catalog")
            info_output("Run 'portolan init' to create one")
        raise SystemExit(1)

    # Load .env for credential precedence
    load_dotenv_and_warn_sensitive(catalog_path)

    try:
        value = get_setting(key, catalog_path=catalog_path, collection=collection)
        source = get_setting_source(key, catalog_path, collection)
    except ValueError as e:
        if use_json:
            envelope = error_envelope(
                "config get", [ErrorDetail(type="ConfigError", message=str(e))]
            )
            output_json_envelope(envelope)
        else:
            error(str(e))
        raise SystemExit(1) from None

    if use_json:
        data = {
            "key": key,
            "value": value,
            "source": source,
        }
        if collection:
            data["collection"] = collection
        envelope = success_envelope("config get", data)
        output_json_envelope(envelope)
    else:
        if value is not None:
            source_label = f"(from {source})"
            success(f"{key}={value} {source_label}")
        else:
            info_output(f"{key} is not set")
            detail(f"  Set via: portolan config set {key} <value>")
            detail(f"  Or set:  PORTOLAN_{key.upper()}=<value>")


@config.command("list")
@click.option(
    "--collection",
    "-c",
    type=str,
    default=None,
    help="Show config for a specific collection.",
)
@click.pass_context
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
def config_list(ctx: click.Context, json_output: bool, collection: str | None) -> None:
    """List all configuration settings.

    Shows all settings with their values and sources.

    \b
    Examples:
        portolan config list
        portolan config list --collection demographics
    """
    from portolan_cli.config import list_settings

    use_json = should_output_json(ctx, json_output)

    # Find catalog root
    catalog_path = find_catalog_root()
    if catalog_path is None:
        if use_json:
            envelope = error_envelope(
                "config list",
                [ErrorDetail(type="CatalogNotFoundError", message="Not in a Portolan catalog")],
            )
            output_json_envelope(envelope)
        else:
            error("Not in a Portolan catalog")
            info_output("Run 'portolan init' to create one")
        raise SystemExit(1)

    # Load .env for credential precedence
    load_dotenv_and_warn_sensitive(catalog_path)

    settings = list_settings(catalog_path, collection=collection)

    if use_json:
        data: dict[str, Any] = {"settings": settings}
        if collection:
            data["collection"] = collection
        envelope = success_envelope("config list", data)
        output_json_envelope(envelope)
    else:
        if not settings:
            info_output("No configuration settings found")
            detail("  Set values with: portolan config set <key> <value>")
        else:
            if collection:
                info_output(f"Configuration for collection '{collection}':")
            else:
                info_output("Configuration:")
            for key, setting_info in settings.items():
                value = setting_info["value"]
                source = setting_info["source"]
                success(f"  {key}={value} (from {source})")


@config.command("unset")
@click.argument("key")
@click.option(
    "--collection",
    "-c",
    type=str,
    default=None,
    help="Unset config for a specific collection.",
)
@click.pass_context
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
def config_unset(ctx: click.Context, json_output: bool, key: str, collection: str | None) -> None:
    """Remove a configuration value.

    Removes the setting from the config file. Does not affect environment variables.

    KEY is the setting name to remove.

    \b
    Examples:
        portolan config unset remote
        portolan config unset aws_profile --collection restricted
    """
    from portolan_cli.config import unset_setting

    use_json = should_output_json(ctx, json_output)

    # Find catalog root
    catalog_path = find_catalog_root()
    if catalog_path is None:
        if use_json:
            envelope = error_envelope(
                "config unset",
                [ErrorDetail(type="CatalogNotFoundError", message="Not in a Portolan catalog")],
            )
            output_json_envelope(envelope)
        else:
            error("Not in a Portolan catalog")
            info_output("Run 'portolan init' to create one")
        raise SystemExit(1)

    removed = unset_setting(catalog_path, key, collection=collection)

    if use_json:
        data = {"key": key, "removed": removed}
        if collection:
            data["collection"] = collection
        envelope = success_envelope("config unset", data)
        output_json_envelope(envelope)
    else:
        if removed:
            if collection:
                success(f"Removed {key} from collection '{collection}' config")
            else:
                success(f"Removed {key} from config")
        else:
            if collection:
                info_output(f"{key} was not set in collection '{collection}' config")
            else:
                info_output(f"{key} was not set in config")


# ─────────────────────────────────────────────────────────────────────────────
# Clean Command
# ─────────────────────────────────────────────────────────────────────────────


def _print_clean_preview(
    catalog_path: Path,
    files_to_remove: list[Path],
    dirs_to_remove: list[Path],
    data_files: int,
) -> None:
    """Print clean preview in text mode (dry-run)."""
    if files_to_remove or dirs_to_remove:
        info_output("Would remove:", dry_run=True)
        for dir_path in dirs_to_remove:
            detail(f"  {dir_path.relative_to(catalog_path)}/", dry_run=True)
        for file_path in files_to_remove:
            detail(f"  {file_path.relative_to(catalog_path)}", dry_run=True)
        click.echo()
        dir_label = "directory" if len(dirs_to_remove) == 1 else "directories"
        info_output(
            f"{len(files_to_remove)} files, {len(dirs_to_remove)} {dir_label} would be removed.",
            dry_run=True,
        )
        info_output(f"Data files preserved: {data_files}", dry_run=True)
    else:
        info_output("Nothing to clean - no metadata files found", dry_run=True)


@cli.command()
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Preview what would be removed without actually deleting.",
)
@click.pass_context
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
def clean(ctx: click.Context, json_output: bool, dry_run: bool) -> None:
    """Remove all Portolan metadata while preserving data files.

    Removes catalog.json, collection.json, item.json (STAC metadata),
    versions.json, and the .portolan/ directory. Preserves all data files
    (.parquet, .tif, .gpkg, .geojson, etc.).

    Use --dry-run to preview what would be removed without deleting anything.

    \b
    Examples:
        portolan clean           # Remove all metadata
        portolan clean --dry-run # Preview what would be removed
    """
    from portolan_cli.clean import clean_catalog

    use_json = should_output_json(ctx, json_output)

    # Find catalog root
    catalog_path = find_catalog_root()
    if catalog_path is None:
        if use_json:
            envelope = error_envelope(
                "clean",
                [
                    ErrorDetail(
                        type="CatalogNotFoundError",
                        message="Not inside a Portolan catalog. Run 'portolan init' first.",
                    )
                ],
            )
            output_json_envelope(envelope)
        else:
            error("Not inside a Portolan catalog")
            info_output("Run 'portolan init' to create one")
        raise SystemExit(1)

    try:
        if dry_run:
            # Preview mode - use clean_catalog with dry_run=True
            files_to_remove, dirs_to_remove, data_files = clean_catalog(catalog_path, dry_run=True)

            if use_json:
                data: dict[str, Any] = {
                    "would_remove_files": [
                        str(f.relative_to(catalog_path)) for f in files_to_remove
                    ],
                    "would_remove_directories": [
                        str(d.relative_to(catalog_path)) for d in dirs_to_remove
                    ],
                    "data_files_preserved": data_files,
                    "dry_run": True,
                }
                envelope = success_envelope("clean", data)
                output_json_envelope(envelope)
            else:
                _print_clean_preview(catalog_path, files_to_remove, dirs_to_remove, data_files)
        else:
            # Actually clean
            files_removed, dirs_removed, data_files = clean_catalog(catalog_path)

            if use_json:
                data = {
                    "files_removed": [str(f.relative_to(catalog_path)) for f in files_removed],
                    "directories_removed": [str(d.relative_to(catalog_path)) for d in dirs_removed],
                    "data_files_preserved": data_files,
                }
                envelope = success_envelope("clean", data)
                output_json_envelope(envelope)
            else:
                if files_removed or dirs_removed:
                    success(f"Removed {len(files_removed)} files, {len(dirs_removed)} directories")
                    detail(f"  Data files preserved: {data_files}")
                else:
                    info_output("Nothing to clean - no metadata files found")

    except OSError as e:
        if use_json:
            envelope = error_envelope(
                "clean",
                [ErrorDetail(type="OSError", message=str(e))],
            )
            output_json_envelope(envelope)
        else:
            error(f"Failed to clean catalog: {e}")
        raise SystemExit(1) from e


# ─────────────────────────────────────────────────────────────────────────────
# Metadata Commands (ADR-0038)
# ─────────────────────────────────────────────────────────────────────────────


@cli.group()
def metadata() -> None:
    """Manage catalog metadata for README generation.

    metadata.yaml files supplement STAC with human-enrichable fields like
    titles, descriptions, contact info, and citations. These files can exist
    at any level in the catalog hierarchy (catalog, subcatalog, collection).

    \b
    Examples:
        portolan metadata init                # Create template at catalog root
        portolan metadata init demographics   # Create template for collection
        portolan metadata validate            # Validate metadata.yaml
    """


@metadata.command("init")
@click.argument("path", required=False, type=click.Path())
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Overwrite existing metadata.yaml file.",
)
@click.option(
    "-r",
    "--recursive",
    is_flag=True,
    default=False,
    help="Create templates at all STAC levels (catalogs, subcatalogs, collections). "
    "Skips items (item.json directories) and preserves existing files unless --force is used.",
)
@click.pass_context
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
def metadata_init(
    ctx: click.Context,
    json_output: bool,
    path: str | None,
    force: bool,
    recursive: bool,
) -> None:
    """Generate a metadata.yaml template.

    Creates a .portolan/metadata.yaml file with all required and optional
    fields, including helpful comments explaining each field.

    If PATH is provided, creates the template at that directory. Otherwise,
    creates it at the catalog root.

    \b
    Examples:
        portolan metadata init                # Template at catalog root
        portolan metadata init demographics   # Template for collection
        portolan metadata init --force        # Overwrite existing
        portolan metadata init --recursive    # All levels in catalog
        portolan metadata init climate -r     # All levels under climate/
    """
    from portolan_cli.metadata_yaml import generate_metadata_template

    use_json = should_output_json(ctx, json_output)

    # Find catalog root
    catalog_path = find_catalog_root()
    if catalog_path is None:
        if use_json:
            envelope = error_envelope(
                "metadata init",
                [
                    ErrorDetail(
                        type="CatalogNotFoundError",
                        message="Not inside a Portolan catalog. Run 'portolan init' first.",
                    )
                ],
            )
            output_json_envelope(envelope)
        else:
            error("Not inside a Portolan catalog")
            info_output("Run 'portolan init' to create one")
        raise SystemExit(1)

    # Handle recursive mode
    if recursive:
        _metadata_init_recursive(catalog_path, path, use_json, force)
        return

    # Determine target directory
    if path:
        target_dir = catalog_path / path
    else:
        target_dir = catalog_path

    # Create .portolan directory if needed
    portolan_dir = target_dir / ".portolan"
    portolan_dir.mkdir(parents=True, exist_ok=True)

    # Check for existing file
    metadata_file = portolan_dir / "metadata.yaml"
    file_existed = metadata_file.exists()
    if file_existed and not force:
        if use_json:
            envelope = error_envelope(
                "metadata init",
                [
                    ErrorDetail(
                        type="FileExistsError",
                        message=f"metadata.yaml already exists at {metadata_file}. Use --force to overwrite.",
                    )
                ],
            )
            output_json_envelope(envelope)
        else:
            warn(f"metadata.yaml already exists at {metadata_file}")
            info_output("Use --force to overwrite")
        raise SystemExit(1)

    # Generate and write template
    template = generate_metadata_template()
    metadata_file.write_text(template)

    if use_json:
        relative_path = str(metadata_file.relative_to(catalog_path))
        envelope = success_envelope(
            "metadata init",
            {"path": relative_path, "overwritten": force and file_existed},
        )
        output_json_envelope(envelope)
    else:
        success(f"Created {metadata_file.relative_to(catalog_path)}")
        info_output("Edit the file to add your catalog's metadata")


@metadata.command("validate")
@click.argument("path", required=False, type=click.Path())
@click.pass_context
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
def metadata_validate(
    ctx: click.Context,
    json_output: bool,
    path: str | None,
) -> None:
    """Validate metadata.yaml against schema.

    Checks for:
    - Required fields: title, description, contact (name + email), license
    - Format validation: email, SPDX license identifier, DOI

    Uses hierarchical resolution: child metadata.yaml files inherit from
    parent levels and override specific fields.

    \b
    Examples:
        portolan metadata validate              # Validate at catalog root
        portolan metadata validate demographics # Validate for collection
    """
    from portolan_cli.errors import ConfigInvalidStructureError
    from portolan_cli.metadata_yaml import load_and_validate_metadata

    use_json = should_output_json(ctx, json_output)

    # Find catalog root
    catalog_path = find_catalog_root()
    if catalog_path is None:
        if use_json:
            envelope = error_envelope(
                "metadata validate",
                [
                    ErrorDetail(
                        type="CatalogNotFoundError",
                        message="Not inside a Portolan catalog. Run 'portolan init' first.",
                    )
                ],
            )
            output_json_envelope(envelope)
        else:
            error("Not inside a Portolan catalog")
            info_output("Run 'portolan init' to create one")
        raise SystemExit(1)

    # Determine target directory
    if path:
        target_dir = catalog_path / path
    else:
        target_dir = catalog_path

    # Load and validate
    try:
        _metadata, errors = load_and_validate_metadata(target_dir, catalog_path)
    except ConfigInvalidStructureError as err:
        if use_json:
            envelope = error_envelope(
                "metadata validate",
                [ErrorDetail(type="InvalidYAMLError", message=str(err))],
            )
            output_json_envelope(envelope)
        else:
            error(f"Invalid YAML in metadata.yaml: {err}")
        raise SystemExit(1) from err

    if use_json:
        if errors:
            envelope = error_envelope(
                "metadata validate",
                [ErrorDetail(type="ValidationError", message=e) for e in errors],
                data={"valid": False, "errors": errors, "path": str(path or ".")},
            )
            output_json_envelope(envelope)
            raise SystemExit(1)
        else:
            envelope = success_envelope(
                "metadata validate",
                {"valid": True, "errors": [], "path": str(path or ".")},
            )
            output_json_envelope(envelope)
    else:
        if errors:
            error(f"Validation failed with {len(errors)} error(s):")
            for validation_err in errors:
                detail(f"  - {validation_err}")
            raise SystemExit(1)
        else:
            success("Metadata is valid")


# ─────────────────────────────────────────────────────────────────────────────
# README Command (ADR-0038)
# ─────────────────────────────────────────────────────────────────────────────


def _generate_readme_content(
    target_dir: Path,
    catalog_path: Path,
    use_json: bool,
) -> tuple[str, bool]:
    """Generate README content for a target directory.

    Args:
        target_dir: Directory to generate README for.
        catalog_path: Root catalog path.
        use_json: Whether to output errors as JSON.

    Returns:
        Tuple of (readme_content, is_catalog_root).

    Raises:
        SystemExit: On YAML parse errors.
    """
    from portolan_cli.config import load_merged_metadata
    from portolan_cli.errors import ConfigInvalidStructureError
    from portolan_cli.readme import generate_catalog_readme, generate_readme

    # Check if at catalog root (has catalog.json, not collection.json)
    is_catalog_root = (target_dir / "catalog.json").exists() and not (
        target_dir / "collection.json"
    ).exists()

    if is_catalog_root:
        return generate_catalog_readme(target_dir), True

    # Load STAC (collection.json or catalog.json)
    stac: dict[str, Any] = {}
    for stac_file in ["collection.json", "catalog.json"]:
        stac_path = target_dir / stac_file
        if stac_path.exists():
            stac = json.loads(stac_path.read_text())
            break

    # Load merged metadata
    try:
        metadata_dict = load_merged_metadata(target_dir, catalog_path)
    except ConfigInvalidStructureError as err:
        if use_json:
            envelope = error_envelope(
                "readme",
                [ErrorDetail(type="InvalidYAMLError", message=str(err))],
            )
            output_json_envelope(envelope)
        else:
            error(f"Invalid YAML in metadata.yaml: {err}")
        raise SystemExit(1) from err

    return generate_readme(stac=stac, metadata=metadata_dict), False


def _recursive_init_error(use_json: bool, error_type: str, message: str) -> None:
    """Output error for recursive init and exit."""
    if use_json:
        envelope = error_envelope(
            "metadata init",
            [ErrorDetail(type=error_type, message=message)],
        )
        output_json_envelope(envelope)
    else:
        error(message)
    raise SystemExit(1)


def _validate_recursive_start_path(
    catalog_path: Path, start_path: str | None, use_json: bool
) -> Path:
    """Validate and return the starting directory for recursive init."""
    if not start_path:
        return catalog_path

    base_dir = catalog_path / start_path
    if not base_dir.exists():
        _recursive_init_error(
            use_json, "PathNotFoundError", f"Path '{start_path}' does not exist in catalog."
        )
    if not base_dir.is_dir():
        _recursive_init_error(
            use_json, "NotADirectoryError", f"Path '{start_path}' is not a directory."
        )
    return base_dir


def _should_process_directory(dirpath: Path, catalog_path: Path) -> bool:
    """Check if directory should be processed for metadata creation."""
    if not dirpath.is_dir() or dirpath.is_symlink():
        return False
    # Skip hidden directories
    if any(part.startswith(".") for part in dirpath.relative_to(catalog_path).parts):
        return False
    # Skip items (directories with item.json)
    if (dirpath / "item.json").exists():
        return False
    # Only process STAC entities (catalogs/collections)
    return (dirpath / "catalog.json").exists() or (dirpath / "collection.json").exists()


def _metadata_init_recursive(
    catalog_path: Path,
    start_path: str | None,
    use_json: bool,
    force: bool,
) -> None:
    """Create metadata.yaml templates at all STAC levels recursively.

    Walks the catalog tree and creates .portolan/metadata.yaml at each level
    that contains a catalog.json or collection.json. Skips items (item.json)
    and directories that already have metadata.yaml (unless force=True).

    Args:
        catalog_path: Path to catalog root.
        start_path: Optional subdirectory to start from (relative to catalog root).
        use_json: Output JSON format.
        force: Overwrite existing metadata.yaml files.

    Raises:
        SystemExit: If start_path doesn't exist or permission errors occur.
    """
    from portolan_cli.metadata_yaml import generate_metadata_template

    base_dir = _validate_recursive_start_path(catalog_path, start_path, use_json)

    created_paths: list[str] = []
    skipped_paths: list[str] = []
    permission_errors: list[str] = []

    def _create_metadata_at(dirpath: Path) -> bool:
        """Create metadata.yaml at directory. Returns True if created."""
        portolan_dir = dirpath / ".portolan"
        metadata_file = portolan_dir / "metadata.yaml"
        if metadata_file.exists() and not force:
            return False
        portolan_dir.mkdir(parents=True, exist_ok=True)
        metadata_file.write_text(generate_metadata_template())
        return True

    def _process_dir(dirpath: Path, rel_path: str) -> None:
        """Process a directory: create metadata or record skip/error."""
        try:
            if _create_metadata_at(dirpath):
                created_paths.append(rel_path)
            else:
                skipped_paths.append(rel_path)
        except PermissionError:
            permission_errors.append(rel_path)

    # Process base directory first (if it's a STAC entity or is catalog root)
    is_catalog_root = base_dir == catalog_path
    is_stac = (base_dir / "catalog.json").exists() or (base_dir / "collection.json").exists()
    if is_catalog_root or is_stac:
        rel_path = (
            ".portolan/metadata.yaml"
            if is_catalog_root
            else str(base_dir.relative_to(catalog_path) / ".portolan/metadata.yaml")
        )
        _process_dir(base_dir, rel_path)

    # Walk tree for subdirectories with permission error handling
    try:
        dir_iterator = sorted(base_dir.rglob("*"))
    except PermissionError as e:
        _recursive_init_error(use_json, "PermissionError", f"Permission denied during scan: {e}")

    for dirpath in dir_iterator:
        if not _should_process_directory(dirpath, catalog_path):
            continue
        rel_path = str(dirpath.relative_to(catalog_path) / ".portolan/metadata.yaml")
        _process_dir(dirpath, rel_path)

    # Output results
    if use_json:
        envelope = success_envelope(
            "metadata init",
            {
                "mode": "recursive",
                "created": created_paths,
                "skipped": skipped_paths,
                "permission_errors": permission_errors,
                "count": len(created_paths),
            },
        )
        output_json_envelope(envelope)
    else:
        if created_paths:
            success(f"Created {len(created_paths)} metadata.yaml template(s)")
            for p in created_paths:
                detail(f"  {p}")
        if skipped_paths:
            info_output(f"Skipped {len(skipped_paths)} existing file(s)")
        if permission_errors:
            warn(f"Permission denied for {len(permission_errors)} location(s)")
            for p in permission_errors:
                detail(f"  {p}")
        if not created_paths and not skipped_paths and not permission_errors:
            warn("No catalogs or collections found")


def _process_readme_entry(
    readme_path: Path,
    content: str,
    rel_path: str,
    check: bool,
    generated: list[str],
    stale: list[str],
) -> None:
    """Process a single README: check freshness or write.

    Args:
        readme_path: Absolute path to README.md.
        content: Generated content to write/check.
        rel_path: Relative path for reporting.
        check: If True, check freshness only.
        generated: List to append fresh/generated paths.
        stale: List to append stale paths.
    """
    if check:
        is_fresh = readme_path.exists() and readme_path.read_text() == content
        (generated if is_fresh else stale).append(rel_path)
    else:
        readme_path.write_text(content)
        generated.append(rel_path)


def _readme_recursive(
    catalog_path: Path,
    use_json: bool,
    check: bool,
    stdout: bool,
) -> None:
    """Generate READMEs for catalog and all collections recursively.

    Helper for --recursive flag. Walks the entire catalog tree and generates:
    1. Catalog/subcatalog READMEs (directories with catalog.json)
    2. Collection READMEs (directories with collection.json)

    Args:
        catalog_path: Path to catalog root.
        use_json: Output JSON format.
        check: CI mode - check freshness only.
        stdout: Print to stdout (not supported in recursive mode).
    """
    from portolan_cli.readme import (
        generate_catalog_readme,
        generate_readme_for_collection,
    )

    if stdout:
        error("--stdout is not supported with --recursive")
        raise SystemExit(1)

    generated_paths: list[str] = []
    stale_paths: list[str] = []

    # Walk entire tree to find all catalogs and collections
    for dirpath in sorted(catalog_path.rglob("*")):
        if not dirpath.is_dir():
            continue
        if any(part.startswith(".") for part in dirpath.relative_to(catalog_path).parts):
            continue

        rel_dir = dirpath.relative_to(catalog_path)
        readme_path = dirpath / "README.md"
        rel_path = str(rel_dir / "README.md")

        if (dirpath / "collection.json").exists():
            content = generate_readme_for_collection(dirpath, catalog_path)
            _process_readme_entry(
                readme_path, content, rel_path, check, generated_paths, stale_paths
            )
        elif (dirpath / "catalog.json").exists():
            content = generate_catalog_readme(dirpath)
            _process_readme_entry(
                readme_path, content, rel_path, check, generated_paths, stale_paths
            )

    # Generate root catalog README
    root_content = generate_catalog_readme(catalog_path)
    _process_readme_entry(
        catalog_path / "README.md", root_content, "README.md", check, generated_paths, stale_paths
    )
    # Move root to front of list
    if "README.md" in generated_paths:
        generated_paths.remove("README.md")
        generated_paths.insert(0, "README.md")
    if "README.md" in stale_paths:
        stale_paths.remove("README.md")
        stale_paths.insert(0, "README.md")

    # Output results
    if use_json:
        if check:
            envelope = success_envelope(
                "readme",
                {
                    "fresh": len(stale_paths) == 0,
                    "checked": generated_paths + stale_paths,
                    "stale": stale_paths,
                },
            )
        else:
            envelope = success_envelope(
                "readme",
                {"generated": generated_paths, "count": len(generated_paths)},
            )
        output_json_envelope(envelope)
        if check and stale_paths:
            raise SystemExit(1)
    else:
        if check:
            if stale_paths:
                error(f"{len(stale_paths)} README(s) are stale:")
                for p in stale_paths:
                    detail(f"  {p}")
                info_output("Run 'portolan readme --recursive' to regenerate")
                raise SystemExit(1)
            else:
                success(f"All {len(generated_paths)} README(s) are up-to-date")
        else:
            success(f"Generated {len(generated_paths)} README(s)")
            for p in generated_paths:
                detail(f"  {p}")


@cli.command()
@click.argument("path", required=False, type=click.Path())
@click.option(
    "--stdout",
    is_flag=True,
    default=False,
    help="Print README to stdout instead of writing file.",
)
@click.option(
    "--check",
    is_flag=True,
    default=False,
    help="Check if README is up-to-date (for CI). Exits 1 if stale.",
)
@click.option(
    "--recursive",
    "-r",
    is_flag=True,
    default=False,
    help="Generate READMEs for catalog and all collections.",
)
@click.pass_context
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
def readme(
    ctx: click.Context,
    json_output: bool,
    path: str | None,
    stdout: bool,
    check: bool,
    recursive: bool,
) -> None:
    """Generate README.md from STAC metadata and metadata.yaml.

    The README is a pure output - always generated from STAC (machine-extracted
    metadata) plus .portolan/metadata.yaml (human enrichment). Never hand-edit
    the README; edit metadata.yaml instead and regenerate.

    Use --check in CI to verify the README is up-to-date:

    \b
    Examples:
        portolan readme                    # Generate at catalog root
        portolan readme demographics       # Generate for collection
        portolan readme --stdout           # Print without writing
        portolan readme --check            # CI mode: exit 1 if stale
        portolan readme --recursive        # Generate for catalog and all collections
    """

    use_json = should_output_json(ctx, json_output)

    # Find catalog root
    catalog_path = find_catalog_root()
    if catalog_path is None:
        if use_json:
            envelope = error_envelope(
                "readme",
                [
                    ErrorDetail(
                        type="CatalogNotFoundError",
                        message="Not inside a Portolan catalog. Run 'portolan init' first.",
                    )
                ],
            )
            output_json_envelope(envelope)
        else:
            error("Not inside a Portolan catalog")
            info_output("Run 'portolan init' to create one")
        raise SystemExit(1)

    # Handle recursive mode
    if recursive:
        _readme_recursive(catalog_path, use_json, check, stdout)
        return

    # Determine target directory
    if path:
        target_dir = catalog_path / path
    else:
        target_dir = catalog_path

    # Generate README content
    readme_content, is_catalog_root = _generate_readme_content(target_dir, catalog_path, use_json)

    # Determine output path
    readme_path = target_dir / "README.md"

    if check:
        # Compare existing README against freshly generated content
        is_fresh = readme_path.exists() and readme_path.read_text() == readme_content

        if use_json:
            envelope = success_envelope(
                "readme",
                {"fresh": is_fresh, "path": str(readme_path.relative_to(catalog_path))},
            )
            output_json_envelope(envelope)
            if not is_fresh:
                raise SystemExit(1)
        else:
            if is_fresh:
                success("README is up-to-date")
            else:
                error("README is stale or missing")
                info_output("Run 'portolan readme' to regenerate")
                raise SystemExit(1)
    elif stdout:
        # Print to stdout
        click.echo(readme_content)
    else:
        # Write to file
        readme_path.write_text(readme_content)
        if use_json:
            envelope = success_envelope(
                "readme",
                {"path": str(readme_path.relative_to(catalog_path)), "generated": True},
            )
            output_json_envelope(envelope)
        else:
            success(f"Generated {readme_path.relative_to(catalog_path)}")


# =============================================================================
# Extract Commands
# =============================================================================


def _parse_filter_patterns(pattern_str: str | None) -> list[str] | None:
    """Parse comma-separated filter patterns into a list."""
    if not pattern_str:
        return None
    return [p.strip() for p in pattern_str.split(",") if p.strip()]


def _build_layer_filters(
    layers: str | None,
    exclude_layers: str | None,
    unified_filter: str | None,
) -> tuple[list[str] | None, list[str] | None]:
    """Build include/exclude filter lists from CLI options."""
    layer_include = _parse_filter_patterns(unified_filter)
    explicit_include = _parse_filter_patterns(layers)

    if explicit_include:
        if layer_include:
            layer_include.extend(explicit_include)
        else:
            layer_include = explicit_include

    layer_exclude = _parse_filter_patterns(exclude_layers)
    return layer_include, layer_exclude


def _build_service_filters(
    services: str | None,
    exclude_services: str | None,
    unified_filter: str | None,
    is_services_root: bool,
) -> tuple[list[str] | None, list[str] | None]:
    """Build service include/exclude filter lists from CLI options."""
    service_include = _parse_filter_patterns(services)
    service_exclude = _parse_filter_patterns(exclude_services)

    # Apply unified filter to services when at services root
    if unified_filter and is_services_root:
        unified_patterns = _parse_filter_patterns(unified_filter)
        if unified_patterns:
            if service_include:
                service_include.extend(unified_patterns)
            else:
                service_include = unified_patterns

    return service_include, service_exclude


def _handle_list_services_mode(
    url: str,
    service_filter: list[str] | None,
    timeout: float,
    use_json: bool,
) -> None:
    """Handle --list-services mode: list available services and exit."""
    from portolan_cli.extract.arcgis.orchestrator import list_services as list_services_func

    try:
        result = list_services_func(url, service_filter=service_filter, timeout=timeout)
    except Exception as e:
        _output_extract_error(use_json, type(e).__name__, str(e), url)
        raise SystemExit(1) from None

    if use_json:
        click.echo(json.dumps(result.to_dict(), indent=2))
    else:
        click.echo(f"Services at {url}:")
        click.echo()
        for svc in result.services:
            click.echo(f"  • {svc.name} ({svc.service_type})")
        click.echo()
        click.echo(f"Total: {len(result.services)} services")
        if result.folders:
            click.echo(f"Folders: {', '.join(result.folders)}")


def _validate_collection_name_cli(
    collection_name: str | None,
    json_output: bool,
    url: str,
) -> None:
    """Validate collection_name at CLI layer (fail fast, per ADR-0023).

    Args:
        collection_name: User-provided collection name (may be None).
        json_output: If True, output errors as JSON.
        url: Source URL for error context.

    Raises:
        SystemExit: If collection_name is invalid.
    """
    if collection_name is None:
        return
    # Reject path separators and parent references
    if "/" in collection_name or "\\" in collection_name or ".." in collection_name:
        _output_extract_error(
            json_output,
            "InvalidCollectionNameError",
            f"Invalid collection name: '{collection_name}'. "
            "Collection name must be a single directory name without path separators or '..'.",
            url,
        )
        raise SystemExit(1)
    # Reject empty or dot-only names
    if not collection_name or collection_name in (".", ".."):
        _output_extract_error(
            json_output,
            "InvalidCollectionNameError",
            f"Invalid collection name: '{collection_name}'. Collection name cannot be empty.",
            url,
        )
        raise SystemExit(1)


def _handle_imageserver_extraction(
    ctx: click.Context,
    url: str,
    output_dir: Path,
    tile_size: int,
    bbox: str | None,
    bbox_crs: str | None,
    compression: str | None,
    max_concurrent: int,
    timeout: float,
    retries: int,
    resume: bool,
    dry_run: bool,
    json_output: bool,
    auto: bool,
    collection_name: str | None,
) -> None:
    """Handle ImageServer URL extraction (raster data)."""
    from portolan_cli.conversion_config import CogSettings, get_cog_settings
    from portolan_cli.extract.arcgis.imageserver.orchestrator import (
        ImageServerCLIOptions,
        run_imageserver_extraction_sync,
    )
    from portolan_cli.output import error, info

    # Parse bbox if provided
    bbox_tuple: tuple[float, float, float, float] | None = None
    if bbox:
        try:
            parts = [float(x.strip()) for x in bbox.split(",")]
            if len(parts) != 4:
                raise ValueError("bbox must have exactly 4 values")
            bbox_tuple = (parts[0], parts[1], parts[2], parts[3])
        except (ValueError, TypeError) as e:
            _output_extract_error(
                json_output,
                "InvalidBboxError",
                f"Invalid bbox format: {bbox}. Expected minx,miny,maxx,maxy. Error: {e}",
                url,
            )
            raise SystemExit(1) from None

    # Load COG settings from config, override with CLI args
    try:
        cog_settings = get_cog_settings(output_dir)
    except Exception:
        cog_settings = CogSettings()

    if compression:
        cog_settings = CogSettings(
            compression=compression.upper(),
            quality=cog_settings.quality,
            tile_size=cog_settings.tile_size,
            predictor=cog_settings.predictor,
            resampling=cog_settings.resampling,
        )

    # Validate collection_name at CLI layer (fail fast, per ADR-0023 flat catalog rule)
    _validate_collection_name_cli(collection_name, json_output, url)

    # Confirmation prompt
    if not auto and not dry_run and not json_output:
        click.echo(f"Extract from: {url}")
        click.echo(f"Output to: {output_dir}")
        click.echo(f"Tile size: {tile_size}px")
        click.echo(f"Compression: {cog_settings.compression}")
        if bbox_tuple:
            click.echo(f"Bbox filter: {bbox_tuple}")
        if not click.confirm("Continue?", default=True):
            raise SystemExit(0)

    # Build options
    options = ImageServerCLIOptions(
        tile_size=tile_size,
        max_concurrent=max_concurrent,
        dry_run=dry_run,
        resume=resume,
        raw=False,  # ImageServer always creates STAC structure
        bbox=bbox_tuple,
        bbox_crs=bbox_crs,
        timeout=timeout,
        compression=cog_settings.compression,
        use_json=json_output,
        collection_name=collection_name,
    )

    # Run extraction
    if not json_output:
        info(f"Extracting ImageServer: {url}")
        if dry_run:
            info("[DRY RUN MODE]")

    exit_code, report = run_imageserver_extraction_sync(url, output_dir, options)

    # Output result
    if json_output:
        data: dict[str, Any] = {
            "source_url": url,
            "output_dir": str(output_dir),
            "service_type": "ImageServer",
            "dry_run": dry_run,
        }

        # Include report data if available
        if report:
            data["summary"] = report.summary.to_dict()
            data["metadata_extracted"] = report.metadata_extracted.to_dict()
            data["tiles"] = [t.to_dict() for t in report.tiles]

        if exit_code == 0:
            envelope = success_envelope("extract-arcgis", data)
        else:
            err = ErrorDetail(type="ExtractionError", message="ImageServer extraction failed")
            envelope = error_envelope("extract-arcgis", [err], data=data)
        output_json_envelope(envelope)
    elif exit_code != 0:
        error("ImageServer extraction failed")

    raise SystemExit(exit_code)


def _output_extract_error(
    use_json: bool,
    error_type: str,
    message: str,
    url: str,
    command: str = "extract-arcgis",
) -> None:
    """Output extraction error in JSON or text format."""
    from portolan_cli.output import error

    if use_json:
        err = ErrorDetail(type=error_type, message=message)
        envelope = error_envelope(command, [err], data={"url": url})
        output_json_envelope(envelope)
    else:
        error(message)


def _output_extract_result(
    report: ExtractionReport,
    output_dir: Path,
    use_json: bool,
    dry_run: bool,
    command: str = "extract-arcgis",
) -> None:
    """Output extraction results in JSON or text format."""
    from portolan_cli.output import error, success, warn

    if use_json:
        data = {
            "source_url": report.source_url,
            "output_dir": str(output_dir),
            "summary": {
                "total_layers": report.summary.total_layers,
                "succeeded": report.summary.succeeded,
                "failed": report.summary.failed,
                "skipped": report.summary.skipped,
                "total_features": report.summary.total_features,
                "total_size_bytes": report.summary.total_size_bytes,
            },
            "layers": [
                {
                    "id": layer.id,
                    "name": layer.name,
                    "status": layer.status,
                    "features": layer.features,
                    "output_path": layer.output_path,
                    "error": layer.error,
                }
                for layer in report.layers
            ],
        }

        if report.summary.failed > 0:
            # Emit error envelope for partial failures
            failed_layers = [
                {"id": layer.id, "name": layer.name, "error": layer.error}
                for layer in report.layers
                if layer.status == "failed"
            ]
            errors = [
                ErrorDetail(
                    type="ExtractionFailed",
                    message=f"Layer '{fl['name']}' (ID: {fl['id']}): {fl['error']}",
                )
                for fl in failed_layers
            ]
            envelope = error_envelope(command, errors, data=data)
            output_json_envelope(envelope)
            raise SystemExit(1)

        envelope = success_envelope(command, data)
        output_json_envelope(envelope)
        return

    if dry_run:
        info_output(f"\nDry run - would extract {report.summary.total_layers} layers:")
        for layer in report.layers:
            click.echo(f"  • {layer.name} (ID: {layer.id})")
        return

    click.echo()
    if report.summary.failed > 0:
        warn(
            f"Extracted {report.summary.succeeded}/{report.summary.total_layers} "
            f"layers ({report.summary.failed} failed)"
        )
        for layer in report.layers:
            if layer.status == "failed":
                error(f"  ✗ {layer.name}: {layer.error}")
        info_output(f"Output: {output_dir}")
        info_output(f"Report: {output_dir}/.portolan/extraction-report.json")
        raise SystemExit(1)

    success(f"Extracted {report.summary.succeeded}/{report.summary.total_layers} layers")
    info_output(f"Output: {output_dir}")
    info_output(f"Report: {output_dir}/.portolan/extraction-report.json")


@cli.group()
def extract() -> None:
    """Extract data from external sources into Portolan catalogs.

    Convert data from ArcGIS services, APIs, or other sources into
    well-structured Portolan catalogs with STAC metadata.

    \b
    Examples:
        portolan extract arcgis https://services.arcgis.com/.../FeatureServer ./output
        portolan extract arcgis URL --layers "Census*" --dry-run
        portolan extract arcgis URL --filter "sdn_*" --resume
    """


@extract.command("arcgis")
@click.argument("url")
@click.argument("output_dir", type=click.Path(path_type=Path), required=False)
@click.option(
    "--layers",
    type=str,
    default=None,
    help="Include layers matching glob patterns (comma-separated). Example: 'Census*,Transport*'",
)
@click.option(
    "--exclude-layers",
    type=str,
    default=None,
    help="Exclude layers matching glob patterns (comma-separated). Example: 'Legacy*,Test*'",
)
@click.option(
    "--filter",
    "unified_filter",
    type=str,
    default=None,
    help="Apply glob filter to both services and layers. Example: 'sdn_*', '*_2024*'",
)
@click.option(
    "--services",
    type=str,
    default=None,
    help="Include services matching glob patterns (comma-separated). For services root URLs only.",
)
@click.option(
    "--exclude-services",
    type=str,
    default=None,
    help="Exclude services matching glob patterns (comma-separated). For services root URLs only.",
)
@click.option(
    "--list-services",
    is_flag=True,
    help="List available services without extracting (for services root URLs).",
)
@click.option(
    "--workers",
    type=click.IntRange(min=1),
    default=3,
    help="Parallel page requests per layer (default: 3).",
)
@click.option(
    "--retries",
    type=click.IntRange(min=1),
    default=3,
    help="Retry attempts per failed layer (default: 3).",
)
@click.option(
    "--timeout",
    type=click.FloatRange(min=0.0, min_open=True),
    default=60.0,
    help="Per-request timeout in seconds (default: 60).",
)
@click.option(
    "--resume",
    is_flag=True,
    help="Resume from existing extraction-report.json (skip succeeded layers).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="List layers without extracting.",
)
@click.option(
    "--json",
    "json_output",
    is_flag=True,
    help="Output extraction report as JSON.",
)
@click.option(
    "--auto",
    is_flag=True,
    help="Skip confirmation prompts.",
)
@click.option(
    "--raw",
    is_flag=True,
    help="Skip auto-init: create only extraction files, no STAC catalog.",
)
# ImageServer-specific options
@click.option(
    "--tile-size",
    type=click.IntRange(min=256, max=8192),
    default=4096,
    help="[ImageServer] Tile size in pixels (default: 4096).",
)
@click.option(
    "--bbox",
    type=str,
    default=None,
    help="[ImageServer] Bounding box filter: minx,miny,maxx,maxy. WGS84 coords auto-converted to service CRS.",
)
@click.option(
    "--bbox-crs",
    type=str,
    default=None,
    help="[ImageServer] Explicit CRS of --bbox (e.g., EPSG:4326, EPSG:3857). Skips auto-detection.",
)
@click.option(
    "--compression",
    type=click.Choice(["DEFLATE", "JPEG", "LZW", "ZSTD"], case_sensitive=False),
    default=None,
    help="[ImageServer] COG compression (default: from config or DEFLATE).",
)
@click.option(
    "--max-concurrent",
    type=click.IntRange(min=1, max=16),
    default=4,
    help="[ImageServer] Maximum concurrent tile downloads (default: 4).",
)
@click.option(
    "--collection-name",
    type=str,
    default=None,
    help="[ImageServer] Name for the collection (default: 'tiles').",
)
@click.pass_context
def extract_arcgis_cmd(
    ctx: click.Context,
    url: str,
    output_dir: Path | None,
    layers: str | None,
    exclude_layers: str | None,
    unified_filter: str | None,
    services: str | None,
    exclude_services: str | None,
    list_services: bool,
    workers: int,
    retries: int,
    timeout: float,
    resume: bool,
    dry_run: bool,
    json_output: bool,
    auto: bool,
    raw: bool,
    tile_size: int,
    bbox: str | None,
    bbox_crs: str | None,
    compression: str | None,
    max_concurrent: int,
    collection_name: str | None,
) -> None:
    """Extract data from ArcGIS FeatureServer/MapServer/ImageServer.

    Downloads layers from an ArcGIS REST service and creates a Portolan
    catalog with GeoParquet files (vector) or COG files (raster) and STAC metadata.

    URL is the ArcGIS service URL (FeatureServer, MapServer, ImageServer, or services root).
    OUTPUT_DIR is the directory to write extracted data (default: inferred from service name).

    \b
    URL Types:
        FeatureServer/MapServer: Extract vector layers to GeoParquet
        ImageServer: Extract raster tiles to COG
        rest/services: Extract from all services (creates nested catalog)

    \b
    Glob Patterns:
        Patterns use fnmatch syntax: * matches any, ? matches single char.
        Common patterns:
        - Country prefix: 'sdn_*', 'ukr_*'
        - Year suffix: '*_2024', '*_2025'
        - Folder path: 'Hosted/cod_ab_*'
        - Dataset family: 'cod_ab_ukr*'

    \b
    Examples:
        # Extract all layers from a FeatureServer
        portolan extract arcgis https://services.arcgis.com/.../FeatureServer ./output

        # Extract specific layers by name
        portolan extract arcgis URL --layers "Census*,Transport*"

        # List available services from a services root
        portolan extract arcgis https://services.arcgis.com/.../rest/services --list-services

        # Extract from services root (filter services)
        portolan extract arcgis https://.../rest/services ./output --services "Census*"

        # Dry run to see what would be extracted
        portolan extract arcgis URL --dry-run

        # Extract raw files only (no STAC catalog auto-init)
        portolan extract arcgis URL --raw

        # JSON output for agent consumption
        portolan extract arcgis URL --json
    """
    from portolan_cli.extract.arcgis.orchestrator import (
        ExtractionOptions,
        ExtractionProgress,
        extract_arcgis_catalog,
    )
    from portolan_cli.extract.arcgis.url_parser import ArcGISURLType, parse_arcgis_url
    from portolan_cli.output import detail, info, warn

    use_json = should_output_json(ctx, json_output)

    # Parse URL to get service name for default output dir
    try:
        parsed = parse_arcgis_url(url)
    except ValueError as e:
        _output_extract_error(use_json, "InvalidURLError", str(e), url)
        raise SystemExit(1) from None

    # Handle --list-services mode
    if list_services:
        if parsed.url_type != ArcGISURLType.SERVICES_ROOT:
            _output_extract_error(
                use_json,
                "InvalidURLError",
                "--list-services requires a services root URL (ending with /rest/services)",
                url,
            )
            raise SystemExit(1)
        service_filter = _parse_filter_patterns(services)
        _handle_list_services_mode(url, service_filter, timeout, use_json)
        return

    # Default output directory from service name (or "services_extract" for services root)
    if output_dir is None:
        if parsed.url_type == ArcGISURLType.SERVICES_ROOT:
            output_dir = Path("services_extract")
        else:
            service_name = parsed.service_name or "arcgis_extract"
            output_dir = Path(service_name.replace("/", "_").lower())

    # Handle ImageServer URLs (raster extraction)
    if parsed.url_type == ArcGISURLType.IMAGE_SERVER:
        _handle_imageserver_extraction(
            ctx=ctx,
            url=url,
            output_dir=output_dir,
            tile_size=tile_size,
            bbox=bbox,
            bbox_crs=bbox_crs,
            compression=compression,
            max_concurrent=max_concurrent,
            timeout=timeout,
            retries=retries,
            resume=resume,
            dry_run=dry_run,
            json_output=use_json,
            auto=auto,
            collection_name=collection_name,
        )
        return

    # Build filter lists
    layer_include, layer_exclude = _build_layer_filters(layers, exclude_layers, unified_filter)
    is_services_root = parsed.url_type == ArcGISURLType.SERVICES_ROOT
    service_include, service_exclude = _build_service_filters(
        services, exclude_services, unified_filter, is_services_root
    )

    # Build options
    options = ExtractionOptions(
        workers=workers,
        retries=retries,
        timeout=timeout,
        resume=resume,
        dry_run=dry_run,
        raw=raw,
    )

    # Progress callback for text output
    def on_progress(progress: ExtractionProgress) -> None:
        if progress.status == "starting":
            info(f"[{progress.layer_index + 1}/{progress.total_layers}] {progress.layer_name}")
        elif progress.status == "success":
            detail("  ✓ Success")
        elif progress.status == "failed":
            warn("  ✗ Failed")
        elif progress.status == "skipped":
            detail("  ↪ Skipped (already extracted)")

    # Confirmation prompt for large extractions
    if not auto and not dry_run and not use_json:
        click.echo(f"Extract from: {url}")
        click.echo(f"Output to: {output_dir}")
        if layer_include:
            click.echo(f"Layer filter: {', '.join(layer_include)}")
        if layer_exclude:
            click.echo(f"Exclude: {', '.join(layer_exclude)}")
        if not click.confirm("Continue?", default=True):
            raise SystemExit(0)

    try:
        report = extract_arcgis_catalog(
            url=url,
            output_dir=output_dir,
            layer_filter=layer_include,
            layer_exclude=layer_exclude,
            service_filter=service_include,
            service_exclude=service_exclude,
            options=options,
            on_progress=None if use_json else on_progress,
        )
    except NotImplementedError as e:
        _output_extract_error(use_json, "NotImplementedError", str(e), url)
        raise SystemExit(1) from None
    except Exception as e:
        _output_extract_error(use_json, type(e).__name__, f"Extraction failed: {e}", url)
        raise SystemExit(1) from None

    _output_extract_result(report, output_dir, use_json, dry_run)


@extract.command("wfs")
@click.argument("url")
@click.argument("output_dir", type=click.Path(path_type=Path), required=False)
@click.option(
    "--layers",
    type=str,
    default=None,
    help="Include layers matching glob patterns (comma-separated). Example: 'buildings*,roads*'",
)
@click.option(
    "--exclude-layers",
    type=str,
    default=None,
    help="Exclude layers matching glob patterns (comma-separated). Example: 'test_*'",
)
@click.option(
    "--wfs-version",
    type=click.Choice(["1.0.0", "1.1.0", "2.0.0", "auto"]),
    default="auto",
    help="WFS version (default: auto-detect).",
)
@click.option(
    "--output-crs",
    type=str,
    default=None,
    help="Target CRS for output (e.g., 'EPSG:4326'). Default keeps source CRS.",
)
@click.option(
    "--bbox",
    type=str,
    default=None,
    help="Bounding box filter: minx,miny,maxx,maxy in output CRS.",
)
@click.option(
    "--limit",
    type=click.IntRange(min=1),
    default=None,
    help="Maximum features per layer.",
)
@click.option(
    "--workers",
    type=click.IntRange(min=1),
    default=1,
    help="Parallel workers for layer extraction (default: 1). "
    "Each layer is extracted independently.",
)
@click.option(
    "--retries",
    type=click.IntRange(min=1),
    default=3,
    help="Retry attempts per failed layer (default: 3).",
)
@click.option(
    "--timeout",
    type=click.FloatRange(min=0.0, min_open=True),
    default=300.0,
    help="Per-layer timeout in seconds (default: 300). "
    "Note: large layers use gpio's internal 10-minute HTTP timeout.",
)
@click.option(
    "--page-size",
    type=click.IntRange(min=100),
    default=10000,
    help="Features per page for large layer pagination (default: 10000).",
)
@click.option(
    "--resume",
    is_flag=True,
    help="Resume from existing extraction-report.json (skip succeeded layers).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="List layers without extracting.",
)
@click.option(
    "--json",
    "json_output",
    is_flag=True,
    help="Output extraction report as JSON.",
)
@click.option(
    "--auto",
    is_flag=True,
    help="Skip confirmation prompts.",
)
@click.option(
    "--raw",
    is_flag=True,
    help="Skip auto-init: create only extraction files, no STAC catalog.",
)
@click.pass_context
def extract_wfs_cmd(
    ctx: click.Context,
    url: str,
    output_dir: Path | None,
    layers: str | None,
    exclude_layers: str | None,
    wfs_version: str,
    output_crs: str | None,
    bbox: str | None,
    limit: int | None,
    workers: int,
    retries: int,
    timeout: float,
    page_size: int,
    resume: bool,
    dry_run: bool,
    json_output: bool,
    auto: bool,
    raw: bool,
) -> None:
    """Extract data from WFS (Web Feature Service) endpoints.

    Downloads layers from a WFS service and creates a Portolan catalog
    with GeoParquet files and STAC metadata.

    URL is the WFS service endpoint URL.
    OUTPUT_DIR is the directory to write extracted data (default: 'wfs_extract').

    \b
    WFS Versions:
        1.0.0: Basic WFS (GML 2.x output)
        1.1.0: Common version (GML 3.x, coordinate axis handling)
        2.0.0: Modern WFS (paging, stored queries)
        auto: Let the client auto-detect (default)

    \b
    Examples:
        # Extract all layers from a WFS service
        portolan extract wfs https://example.com/wfs ./output

        # Extract specific layers by typename
        portolan extract wfs URL --layers "buildings*,roads*"

        # Extract with bounding box filter
        portolan extract wfs URL --bbox "-122.5,37.5,-122.0,38.0"

        # Dry run to see available layers
        portolan extract wfs URL --dry-run

        # Extract with specific WFS version
        portolan extract wfs URL --wfs-version 2.0.0

        # Extract 4 layers in parallel with 5-minute timeout per layer
        portolan extract wfs URL --workers 4 --timeout 300
    """
    from portolan_cli.extract.wfs.orchestrator import (
        ExtractionOptions,
        ExtractionProgress,
        extract_wfs_catalog,
    )
    from portolan_cli.output import detail, info, warn

    use_json = should_output_json(ctx, json_output)

    # Default output directory
    if output_dir is None:
        output_dir = Path("wfs_extract")

    # Parse bbox if provided
    bbox_tuple: tuple[float, float, float, float] | None = None
    if bbox:
        try:
            parts = [float(x.strip()) for x in bbox.split(",")]
            if len(parts) != 4:
                _output_extract_error(
                    use_json,
                    "InvalidBBoxError",
                    "bbox must have 4 values: minx,miny,maxx,maxy",
                    url,
                    command="extract-wfs",
                )
                raise SystemExit(1)
            bbox_tuple = (parts[0], parts[1], parts[2], parts[3])
        except ValueError as e:
            _output_extract_error(use_json, "InvalidBBoxError", str(e), url, command="extract-wfs")
            raise SystemExit(1) from None

    # Build filter lists
    layer_include = _parse_filter_patterns(layers)
    layer_exclude = _parse_filter_patterns(exclude_layers)

    # Build options
    options = ExtractionOptions(
        workers=workers,
        retries=retries,
        timeout=timeout,
        resume=resume,
        dry_run=dry_run,
        raw=raw,
        wfs_version=wfs_version,
        output_crs=output_crs,
        bbox=bbox_tuple,
        limit=limit,
        page_size=page_size,
    )

    # Progress callback for text output
    def on_progress(progress: ExtractionProgress) -> None:
        if progress.status == "starting":
            info(f"[{progress.layer_index + 1}/{progress.total_layers}] {progress.layer_name}")
        elif progress.status == "success":
            detail("  ✓ Success")
        elif progress.status == "failed":
            warn("  ✗ Failed")
        elif progress.status == "skipped":
            detail("  ↪ Skipped (already extracted)")

    # Confirmation prompt
    if not auto and not dry_run and not use_json:
        info(f"Extract from: {url}")
        info(f"Output to: {output_dir}")
        if layer_include:
            detail(f"Layer filter: {', '.join(layer_include)}")
        if layer_exclude:
            detail(f"Exclude: {', '.join(layer_exclude)}")
        if not click.confirm("Continue?", default=True):
            raise SystemExit(0)

    try:
        report = extract_wfs_catalog(
            url=url,
            output_dir=output_dir,
            layer_filter=layer_include,
            layer_exclude=layer_exclude,
            options=options,
            on_progress=None if use_json else on_progress,
        )
    except Exception as e:
        _output_extract_error(
            use_json, type(e).__name__, f"Extraction failed: {e}", url, command="extract-wfs"
        )
        raise SystemExit(1) from None

    _output_extract_result(report, output_dir, use_json, dry_run, command="extract-wfs")


# =============================================================================
# Version management commands (iceberg backend only)
# =============================================================================


def _require_iceberg_backend(
    catalog_path: Path, use_json: bool, command_name: str
) -> VersioningBackend:
    """Load the iceberg backend or exit with an error."""
    from portolan_cli.version_ops import BackendRequiredError, require_iceberg_backend

    try:
        return require_iceberg_backend(catalog_path, command_name)
    except BackendRequiredError as e:
        if use_json:
            envelope = error_envelope(
                f"version {command_name}",
                [ErrorDetail(type="BackendError", message=str(e))],
            )
            output_json_envelope(envelope)
        else:
            error(str(e))
        raise SystemExit(1) from None


@cli.group()
def version() -> None:
    """Iceberg version management commands.

    \b
    Subcommands:
        current   Show current version of a collection
        list      List all versions of a collection
        rollback  Rollback to a previous version
        prune     Remove old versions
    """


@version.command()
@click.argument("collection")
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to catalog root (default: auto-detect).",
)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.pass_context
def current(
    ctx: click.Context,
    collection: str,
    catalog_path: Path | None,
    json_output: bool,
) -> None:
    """Show the current version of a collection.

    \b
    Examples:
        portolan version current boundaries
        portolan version current boundaries --json
    """
    use_json = should_output_json(ctx, json_output)

    if catalog_path is None:
        catalog_path = require_catalog_root(use_json, "version current")

    backend = _require_iceberg_backend(catalog_path, use_json, "current")

    try:
        ver = backend.get_current_version(collection)
    except (FileNotFoundError, Exception) as e:
        if use_json:
            envelope = error_envelope(
                "version current",
                [ErrorDetail(type=type(e).__name__, message=str(e))],
            )
            output_json_envelope(envelope)
        else:
            error(str(e))
        raise SystemExit(1) from None

    if use_json:
        envelope = success_envelope(
            "version current",
            {
                "collection": collection,
                "version": ver.version,
                "created": ver.created.isoformat(),
                "breaking": ver.breaking,
                "message": ver.message,
                "assets": len(ver.assets),
                "changes": ver.changes,
            },
        )
        output_json_envelope(envelope)
    else:
        breaking_flag = " [BREAKING]" if ver.breaking else ""
        timestamp = ver.created.strftime("%Y-%m-%d %H:%M:%S")
        msg = f" — {ver.message}" if ver.message else ""
        info_output(f"{collection}: {ver.version}  {timestamp}{breaking_flag}{msg}")
        if ver.assets:
            detail(f"  {len(ver.assets)} asset(s)")


@version.command("list")
@click.argument("collection")
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to catalog root (default: auto-detect).",
)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.pass_context
def version_list_cmd(
    ctx: click.Context,
    collection: str,
    catalog_path: Path | None,
    json_output: bool,
) -> None:
    """List all versions of a collection.

    \b
    Examples:
        portolan version list boundaries
        portolan version list boundaries --json
    """
    use_json = should_output_json(ctx, json_output)

    if catalog_path is None:
        catalog_path = require_catalog_root(use_json, "version list")

    backend = _require_iceberg_backend(catalog_path, use_json, "list")

    try:
        versions = backend.list_versions(collection)
    except (FileNotFoundError, Exception) as e:
        if use_json:
            envelope = error_envelope(
                "version list",
                [ErrorDetail(type=type(e).__name__, message=str(e))],
            )
            output_json_envelope(envelope)
        else:
            error(str(e))
        raise SystemExit(1) from None

    if use_json:
        versions_data = [
            {
                "version": v.version,
                "created": v.created.isoformat(),
                "breaking": v.breaking,
                "message": v.message,
                "assets": len(v.assets),
                "changes": v.changes,
            }
            for v in versions
        ]
        envelope = success_envelope(
            "version list",
            {"collection": collection, "versions": versions_data},
        )
        output_json_envelope(envelope)
    else:
        if not versions:
            info_output(f"No versions found for collection '{collection}'")
            return

        info_output(f"Versions for '{collection}' ({len(versions)} total):\n")
        for v in versions:
            breaking_flag = " [BREAKING]" if v.breaking else ""
            timestamp = v.created.strftime("%Y-%m-%d %H:%M:%S")
            msg = f" — {v.message}" if v.message else ""
            info_output(f"  {v.version}  {timestamp}{breaking_flag}{msg}")
            if v.changes:
                for change in v.changes:
                    detail(f"    {change}")


@version.command()
@click.argument("collection")
@click.argument("target_version")
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to catalog root (default: auto-detect).",
)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.pass_context
def rollback(
    ctx: click.Context,
    collection: str,
    target_version: str,
    catalog_path: Path | None,
    json_output: bool,
) -> None:
    """Rollback a collection to a previous version.

    Uses Iceberg's native snapshot management to set the current snapshot
    pointer back to TARGET_VERSION. No data is copied — this is instant.

    \b
    Examples:
        portolan version rollback boundaries 1.0.0
        portolan version rollback boundaries 2.0.0 --json
    """
    use_json = should_output_json(ctx, json_output)

    if catalog_path is None:
        catalog_path = require_catalog_root(use_json, "version rollback")

    backend = _require_iceberg_backend(catalog_path, use_json, "rollback")

    try:
        restored = backend.rollback(collection, target_version)
    except (FileNotFoundError, ValueError, Exception) as e:
        if use_json:
            envelope = error_envelope(
                "version rollback",
                [ErrorDetail(type=type(e).__name__, message=str(e))],
            )
            output_json_envelope(envelope)
        else:
            error(str(e))
        raise SystemExit(1) from None

    if use_json:
        envelope = success_envelope(
            "version rollback",
            {
                "collection": collection,
                "restored_version": restored.version,
                "created": restored.created.isoformat(),
            },
        )
        output_json_envelope(envelope)
    else:
        success(f"Rolled back '{collection}' to version {restored.version}")


@version.command()
@click.argument("collection")
@click.option(
    "--keep",
    "-k",
    type=click.IntRange(min=1),
    default=5,
    show_default=True,
    help="Number of recent versions to keep.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be pruned without deleting.",
)
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to catalog root (default: auto-detect).",
)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.pass_context
def prune(
    ctx: click.Context,
    collection: str,
    keep: int,
    dry_run: bool,
    catalog_path: Path | None,
    json_output: bool,
) -> None:
    """Remove old versions, keeping the N most recent.

    \b
    Examples:
        portolan version prune boundaries                # Keep 5 most recent
        portolan version prune boundaries --keep 3       # Keep 3 most recent
        portolan version prune boundaries --dry-run      # Preview without deleting
    """
    use_json = should_output_json(ctx, json_output)

    if catalog_path is None:
        catalog_path = require_catalog_root(use_json, "version prune")

    backend = _require_iceberg_backend(catalog_path, use_json, "prune")

    try:
        pruned = backend.prune(collection, keep=keep, dry_run=dry_run)
    except (FileNotFoundError, Exception) as e:
        if use_json:
            envelope = error_envelope(
                "version prune",
                [ErrorDetail(type=type(e).__name__, message=str(e))],
            )
            output_json_envelope(envelope)
        else:
            error(str(e))
        raise SystemExit(1) from None

    if use_json:
        pruned_data = [
            {
                "version": v.version,
                "created": v.created.isoformat(),
            }
            for v in pruned
        ]
        envelope = success_envelope(
            "version prune",
            {
                "collection": collection,
                "pruned": pruned_data,
                "kept": keep,
                "dry_run": dry_run,
            },
        )
        output_json_envelope(envelope)
    else:
        prefix = "[DRY RUN] " if dry_run else ""
        if not pruned:
            info_output(f"{prefix}Nothing to prune (≤{keep} versions exist)")
        else:
            action = "Would prune" if dry_run else "Pruned"
            info_output(f"{prefix}{action} {len(pruned)} version(s), keeping {keep}:")
            for v in pruned:
                timestamp = v.created.strftime("%Y-%m-%d %H:%M:%S")
                detail(f"  {v.version}  {timestamp}")


# =============================================================================
# STAC GeoParquet Command
# =============================================================================


def _discover_collections_with_items(catalog_root: Path) -> list[str]:
    """Find all collections that have STAC items (collection.json with item links).

    Args:
        catalog_root: Path to the catalog root directory.

    Returns:
        Sorted list of collection IDs relative to catalog_root.
    """
    import json

    collections: list[str] = []

    # Find all collection.json files
    for collection_file in catalog_root.rglob("collection.json"):
        # Skip files in .portolan directory
        if ".portolan" in collection_file.parts:
            continue

        # Check if collection has any items
        try:
            data = json.loads(collection_file.read_text())
            links = data.get("links", [])
            has_items = any(link.get("rel") == "item" for link in links)

            if has_items:
                # Get relative path as collection ID
                rel_path = collection_file.parent.relative_to(catalog_root)
                collections.append(str(rel_path))
        except (json.JSONDecodeError, OSError):
            continue

    return sorted(collections)


@dataclass
class _ParquetResult:
    """Result of processing one collection for stac-geoparquet."""

    collection: str
    item_count: int
    parquet_path: str
    dry_run: bool = False
    error: ErrorDetail | None = None


def _process_collection_for_parquet(
    coll_id: str,
    catalog_path: Path,
    dry_run: bool,
    is_bulk: bool,
    use_json: bool,
) -> _ParquetResult | None:
    """Process a single collection for stac-geoparquet generation.

    Args:
        coll_id: Collection ID to process.
        catalog_path: Path to catalog root.
        dry_run: If True, don't actually generate files.
        is_bulk: If True, suppress per-collection output.
        use_json: If True, suppress non-JSON output.

    Returns:
        _ParquetResult on success/dry-run, None on skip (empty collection in bulk mode),
        or _ParquetResult with error field set on failure.
    """
    from portolan_cli.stac_parquet import (
        add_parquet_link_to_collection,
        count_items,
        generate_items_parquet,
        track_parquet_in_versions,
    )

    collection_path = catalog_path / coll_id

    if not (collection_path / "collection.json").exists():
        err = ErrorDetail(
            type="CollectionNotFoundError",
            message=f"Collection '{coll_id}' not found at {collection_path}",
        )
        if not use_json and not is_bulk:
            error(f"Collection '{coll_id}' not found")
        return _ParquetResult(coll_id, 0, "", error=err)

    # Count items
    try:
        item_count = count_items(collection_path)
    except Exception as e:
        err = ErrorDetail(type=type(e).__name__, message=f"{coll_id}: {e}")
        if not use_json and not is_bulk:
            error(f"Failed to count items in '{coll_id}': {e}")
        return _ParquetResult(coll_id, 0, "", error=err)

    if item_count == 0:
        # For bulk processing, skip silently; for explicit, return error
        if is_bulk:
            return None
        err = ErrorDetail(
            type="EmptyCollectionError",
            message=f"No items found in collection '{coll_id}'",
        )
        if not use_json:
            error(f"No items found in collection '{coll_id}'")
        return _ParquetResult(coll_id, 0, "", error=err)

    parquet_path = collection_path / "items.parquet"

    if dry_run:
        if not use_json and not is_bulk:
            info_output(f"[DRY RUN] Would generate items.parquet for '{coll_id}'")
            detail(f"    Items: {item_count}")
            detail(f"    Output: {parquet_path}")
        return _ParquetResult(coll_id, item_count, str(parquet_path), dry_run=True)

    # Generate parquet
    try:
        parquet_path = generate_items_parquet(collection_path)
        add_parquet_link_to_collection(collection_path)
        track_parquet_in_versions(collection_path)
        if not use_json and not is_bulk:
            success(f"Generated items.parquet for '{coll_id}'")
            detail(f"    Items: {item_count}")
            detail(f"    Output: {parquet_path}")
        return _ParquetResult(coll_id, item_count, str(parquet_path))
    except Exception as e:
        err = ErrorDetail(type=type(e).__name__, message=f"{coll_id}: {e}")
        if not use_json and not is_bulk:
            error(f"Failed to generate parquet for '{coll_id}': {e}")
        return _ParquetResult(coll_id, 0, "", error=err)


def _output_parquet_results(
    results: list[dict[str, Any]],
    errors_list: list[ErrorDetail],
    total_items: int,
    is_bulk: bool,
    dry_run: bool,
    use_json: bool,
) -> None:
    """Output results of stac-geoparquet command."""
    had_errors = bool(errors_list)

    # Summary output for bulk operations
    if not use_json and is_bulk and results:
        if dry_run:
            success(
                f"[DRY RUN] Would generate items.parquet for {len(results)} "
                f"collections ({total_items:,} total items)"
            )
        else:
            success(
                f"Generated items.parquet for {len(results)} collections "
                f"({total_items:,} total items)"
            )
        if errors_list:
            warn(f"  {len(errors_list)} collection(s) failed")

    # JSON output
    if use_json:
        if had_errors and not results:
            envelope = error_envelope("stac-geoparquet", errors_list)
        else:
            envelope = success_envelope(
                "stac-geoparquet",
                {
                    "collections_processed": len(results),
                    "results": results,
                    "errors": [{"type": e.type, "message": e.message} for e in errors_list],
                },
            )
        output_json_envelope(envelope)

    if had_errors:
        raise SystemExit(1)


@cli.command("stac-geoparquet")
@click.option(
    "--collection",
    "-c",
    required=False,
    default=None,
    help="Collection ID to generate parquet for. If omitted, generates for all collections.",
)
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to catalog root (default: auto-detect).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be generated without creating files.",
)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.pass_context
def stac_geoparquet(
    ctx: click.Context,
    collection: str | None,
    catalog_path: Path | None,
    dry_run: bool,
    json_output: bool,
) -> None:
    """Generate items.parquet for efficient STAC queries.

    Creates a GeoParquet file containing all items in a collection,
    enabling fast spatial/temporal queries without N HTTP requests.

    This is optional but recommended for collections with >100 items.
    The parquet file is added as a link in collection.json.

    If --collection is omitted, generates for ALL collections in the catalog.

    \b
    Examples:
        portolan stac-geoparquet                    # Generate for ALL collections
        portolan stac-geoparquet -c landsat         # Generate for landsat collection
        portolan stac-geoparquet -c imagery --dry-run  # Preview without creating
        portolan stac-geoparquet --json             # JSON output for all collections
    """
    use_json = should_output_json(ctx, json_output)

    # Find catalog root
    if catalog_path is None:
        catalog_path = find_catalog_root()
        if catalog_path is None:
            if use_json:
                envelope = error_envelope(
                    "stac-geoparquet",
                    [
                        ErrorDetail(
                            type="CatalogNotFoundError",
                            message="Not inside a Portolan catalog. Run 'portolan init' first.",
                        )
                    ],
                )
                output_json_envelope(envelope)
            else:
                error("Not inside a Portolan catalog")
                info_output("Run 'portolan init' to create one")
            raise SystemExit(1)

    # Determine which collections to process
    if collection is not None:
        collections_to_process = [collection]
    else:
        # Discover all collections with items
        collections_to_process = _discover_collections_with_items(catalog_path)
        if not collections_to_process:
            if use_json:
                envelope = error_envelope(
                    "stac-geoparquet",
                    [
                        ErrorDetail(
                            type="NoCollectionsError",
                            message="No collections with items found in catalog",
                        )
                    ],
                )
                output_json_envelope(envelope)
            else:
                error("No collections with items found in catalog")
            raise SystemExit(1)

    # Process each collection
    results: list[dict[str, Any]] = []
    errors_list: list[ErrorDetail] = []
    total_items = 0
    is_bulk = collection is None  # Catalog-level operation

    for coll_id in collections_to_process:
        result = _process_collection_for_parquet(coll_id, catalog_path, dry_run, is_bulk, use_json)
        if result is None:
            continue  # Skipped (empty collection in bulk mode)
        if result.error:
            errors_list.append(result.error)
        else:
            results.append(
                {
                    "collection": result.collection,
                    "item_count": result.item_count,
                    "parquet_path": result.parquet_path,
                    **({"dry_run": True} if result.dry_run else {}),
                }
            )
            total_items += result.item_count

    _output_parquet_results(results, errors_list, total_items, is_bulk, dry_run, use_json)


# ─────────────────────────────────────────────────────────────────────────────
# Skills Commands
# ─────────────────────────────────────────────────────────────────────────────


@cli.group()
def skills() -> None:
    """List and view AI skills for Portolan workflows.

    Skills are markdown files that help AI agents assist users with specific
    workflows like uploading to Source Cooperative.

    \b
    Examples:
        portolan skills list              # List available skills
        portolan skills show sourcecoop   # View Source Co-op upload skill
    """


@skills.command("list")
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.pass_context
def skills_list_cmd(ctx: click.Context, json_output: bool) -> None:
    """List all available skills.

    \b
    Examples:
        portolan skills list
        portolan skills list --json
    """
    from portolan_cli.skills import list_skills

    use_json = should_output_json(ctx, json_output)
    skill_names = list_skills()

    if use_json:
        envelope = success_envelope("skills list", {"skills": skill_names})
        output_json_envelope(envelope)
    else:
        if not skill_names:
            info_output("No skills available")
        else:
            info_output(f"Available skills ({len(skill_names)}):")
            for name in skill_names:
                detail(f"  {name}")
            info_output("\nUse 'portolan skills show <name>' to view a skill")


@skills.command("show")
@click.argument("name")
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.pass_context
def skills_show_cmd(ctx: click.Context, name: str, json_output: bool) -> None:
    """View the content of a skill.

    NAME is the skill name (e.g., 'sourcecoop').

    \b
    Examples:
        portolan skills show sourcecoop
        portolan skills show sourcecoop --json
    """
    from portolan_cli.skills import get_skill, list_skills

    use_json = should_output_json(ctx, json_output)
    content = get_skill(name)

    if content is None:
        available = list_skills()
        if use_json:
            envelope = error_envelope(
                "skills show",
                [
                    ErrorDetail(
                        type="SkillNotFoundError",
                        message=f"Skill '{name}' not found. Available: {', '.join(available)}",
                    )
                ],
            )
            output_json_envelope(envelope)
        else:
            error(f"Skill '{name}' not found")
            if available:
                info_output(f"Available skills: {', '.join(available)}")
        raise SystemExit(1)

    if use_json:
        envelope = success_envelope("skills show", {"name": name, "content": content})
        output_json_envelope(envelope)
    else:
        # Print raw content for AI agents to ingest
        click.echo(content)
