"""Portolan CLI - Command-line interface for managing cloud-native geospatial data.

The CLI is a thin wrapper around the Python API (see catalog.py).
All business logic lives in the library; the CLI handles user interaction.
"""

from __future__ import annotations

import os
import sys
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn, TextIO

if TYPE_CHECKING:
    from datetime import datetime

    from portolan_cli.add import AddFailure
    from portolan_cli.convert import ConversionResult
    from portolan_cli.metadata.fix import FixReport
    from portolan_cli.sync.pull import PullResult
    from portolan_core.query import ItemInfo
    from portolan_core.run_state import RunState
    from portolan_core.status import CollectionStatus

import click
from rashid_core.api import Severity as RashidSeverity

from portolan_cli.add_progress import AddProgressReporter, count_files
from portolan_cli.catalog import find_catalog_root
from portolan_cli.cli_types import FLEXIBLE_DATETIME
from portolan_cli.collection_id import resolve_collection_id
from portolan_cli.discovery import get_sidecars
from portolan_cli.emit import emit_error, emit_success
from portolan_cli.input_hardening import InputValidationError
from portolan_cli.json_output import ErrorDetail, error_envelope, success_envelope
from portolan_cli.logging_setup import configure_logging
from portolan_cli.output import detail, error, set_writer, stdout_to_stderr, success, warn
from portolan_cli.output import info as info_output
from portolan_cli.plugins.commands import ExtractGroup, PluginGroup
from portolan_cli.plugins.manager import PluginWarning
from portolan_cli.scan.core import (
    IssueType,
    ScanIssue,
    ScanResult,
)
from portolan_cli.scan.core import (
    Severity as ScanSeverity,
)
from portolan_cli.scan.output import (
    format_collection_suggestion,
    generate_next_steps,
    get_category_display_name,
    get_fixability,
    group_skipped_files,
    render_tree_view,
)
from portolan_cli.scan.progress import ScanProgressReporter, count_directories
from portolan_cli.utils import format_size as format_size
from portolan_core import trace
from portolan_core.bbox import BBOX_2D_LEN
from portolan_core.catalog_list import (
    AssetInfo,
    AssetStatus,
    CatalogListResult,
    list_catalog_contents,
)
from portolan_core.errors import (
    AssetKeyCollisionError,
    ConfigError,
    InvalidLiveUrlError,
    MissingLicenseError,
    PortolanError,
    UnknownFormatHandlerError,
)
from portolan_core.licensing import (
    CLI_LICENSE_REMEDIATION,
    METADATA_LICENSE_REMEDIATION,
    OTHER_LICENSE,
    license_gap,
)

_MAX_LARGE_FILES_SHOWN = 5


def click_writer(
    prefix: str,
    message: str,
    *,
    fg: str | None,
    dim: bool,
    file: TextIO | None,
    nl: bool,
) -> None:
    """Write an ``output`` message through ``click.echo``.

    ``click.echo`` strips color from a pipe, follows the CliRunner ``color``
    setting, and wraps a Windows console. The library writer does not.
    """
    click.echo(
        f"{click.style(prefix, fg=fg, dim=dim)} {click.style(message, fg=fg, dim=dim)}",
        file=file,
        nl=nl,
    )


set_writer(click_writer)


def show_warning(
    message: Warning | str,
    category: type[Warning],
    filename: str,
    lineno: int,
    file: TextIO | None = None,
    line: str | None = None,
) -> None:
    """Print a ``PluginWarning`` as an output warning. Pass other warnings on.

    A plugin warning names the plugin and the problem. The source path and
    line that Python adds point into Portolan, which does not help the user.
    """
    if issubclass(category, PluginWarning):
        warn(str(message))
        return
    # The same output as Python's default `showwarning`.
    stream = file if file is not None else sys.stderr
    stream.write(warnings.formatwarning(message, category, filename, lineno, line))


warnings.showwarning = show_warning


def should_output_json(ctx: click.Context, json_flag: bool = False) -> bool:
    """Determine if JSON output should be used, and keep stdout for the envelope.

    Checks both the global --format option and per-command --json flags.
    Global --format=json takes precedence, but per-command flags also work
    for backward compatibility.

    In JSON mode this also routes the stdout-bound ``output`` messages to
    stderr until the root context closes, so the library messages of the
    run cannot break the JSON document on stdout (#1005). A second call in
    the same run changes nothing.

    Args:
        ctx: Click context containing the format preference.
        json_flag: Per-command --json flag value.

    Returns:
        True if JSON output should be used, False for text output.
    """
    root = ctx.find_root()
    obj = root.obj or {}
    use_json = obj.get("format", "text") == "json" or json_flag
    if use_json and not root.meta.get(_STDOUT_ROUTED):
        root.meta[_STDOUT_ROUTED] = True
        root.with_resource(stdout_to_stderr())
    return use_json


_STDOUT_ROUTED = "portolan.stdout_to_stderr"


def _fail_on_add_error(
    err: Exception,
    resolved_paths: list[Path],
    *,
    use_json: bool,
) -> NoReturn:
    """Report a failed add and exit 1, naming the path when there was only one.

    Args:
        err: The error raised by the library layer.
        resolved_paths: Paths the add was asked to process.
        use_json: Whether output goes out as a JSON envelope.

    Raises:
        SystemExit: Always, with status 1.
    """
    path_context = f"{resolved_paths[0]}: " if len(resolved_paths) == 1 else ""
    code = err.code if isinstance(err, PortolanError) else None
    emit_error("add", type(err).__name__, f"{path_context}{err}", use_json=use_json, code=code)
    raise SystemExit(1) from err


def _fail_on_missing_license(
    command: str,
    err: MissingLicenseError,
    *,
    use_json: bool,
) -> NoReturn:
    """Report an unlicensed collection and exit 1, naming the file to edit.

    Args:
        command: Command name, for the error envelope.
        err: The error raised by the library layer.
        use_json: Whether output goes out as a JSON envelope.

    Raises:
        SystemExit: Always, with status 1.
    """
    emit_error(command, type(err).__name__, str(err), use_json=use_json, code=err.code)
    if not use_json:
        info_output(METADATA_LICENSE_REMEDIATION)
    raise SystemExit(1) from err


def _validated_cli_license(
    command: str,
    license_id: str | None,
    license_url: str | None,
    *,
    use_json: bool,
) -> str:
    """Validate a --license / --license-url pair, or exit 1 naming both flags.

    Shared by ``init`` and ``add-external``, the two commands that take the license
    as a flag rather than reading it out of metadata.yaml. Validating here means a
    bad pair fails before either command touches the filesystem (issue #686).

    Args:
        command: Command name, for the error envelope.
        license_id: Value of --license, or None when it was not passed.
        license_url: Value of --license-url, or None.
        use_json: Whether output goes out as a JSON envelope.

    Returns:
        The stripped license identifier.

    Raises:
        SystemExit: Always exits 1 when the pair cannot produce a conformant license.
    """
    declared = license_id.strip() if license_id else ""
    has_link = bool(license_url and license_url.strip())

    gap = license_gap(declared, has_license_link=has_link)
    if gap is None:
        return declared

    err = MissingLicenseError(f"'portolan {command}'", gap)
    emit_error(command, type(err).__name__, str(err), use_json=use_json, code=err.code)
    if not use_json:
        info_output(CLI_LICENSE_REMEDIATION)
    raise SystemExit(1) from err


def output_json_envelope(envelope: Any) -> None:
    """Output a JSON envelope to stdout.

    Args:
        envelope: OutputEnvelope instance to output.
    """
    click.echo(envelope.to_json())


def load_dotenv_from_catalog(catalog_path: Path | None = None) -> bool:
    """Load environment variables from .env file in catalog root.

    It lives in the CLI layer because python-dotenv is a portolan-cli
    dependency. portolan-core does not install it (#1133).

    Searches for .env file in catalog root directory and loads it using
    python-dotenv. Does NOT override existing environment variables.

    This enables storing sensitive settings (remote, profile, region) in
    a .env file that won't be pushed to remote storage.

    Args:
        catalog_path: Path to catalog root. If None, attempts to find it.

    Returns:
        True if a .env file was found and loaded, False otherwise.
    """
    from dotenv import load_dotenv

    if catalog_path is None:
        return False

    env_file = catalog_path / ".env"
    if not env_file.is_file():
        return False

    load_dotenv(env_file, override=False)
    return True


def load_dotenv_and_warn_sensitive(catalog_path: Path, *, state: RunState | None = None) -> None:
    """Load .env from catalog and warn if config.yaml has sensitive settings.

    This should be called after the actual catalog path is resolved (not in
    the global CLI callback) to ensure the correct .env is loaded when
    --catalog/--portolan-dir overrides the default.

    Args:
        catalog_path: Resolved catalog root path.
        state: The run state, which parses config.yaml once per run.
    """
    from portolan_core.config import check_sensitive_settings_in_config

    load_dotenv_from_catalog(catalog_path)

    sensitive_in_config = check_sensitive_settings_in_config(catalog_path, state=state)
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
        emit_error(command_name, "NotACatalogError", msg, use_json=use_json)
        raise SystemExit(1)
    return catalog_root


def _collection_path(catalog_path: Path | None, collection: str | None) -> Path | None:
    """Compute collection folder path for hierarchical config."""
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
    from portolan_core.config import get_setting

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
) -> str | None:
    """Resolve the AWS profile: CLI, then env var, then config.

    A Portolan setting wins, because a user writes it for this tool. None means
    that nothing here names a profile. The sync layer then reads `AWS_PROFILE`,
    and falls back to the `default` profile, as boto3 and the AWS CLI do.

    Args:
        profile: CLI-provided profile value (None if not specified).
        catalog_path: Path to catalog root for config lookup.
        collection: Optional collection name for collection-level config.

    Returns:
        The profile name, or None when nothing here names one.
    """
    from portolan_core.config import get_setting

    return get_setting(
        "aws_profile",
        cli_value=profile,
        catalog_path=catalog_path,
        collection=collection,
        collection_path=_collection_path(catalog_path, collection),
    )


@click.group(cls=PluginGroup)
@click.version_option()
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["json", "text"]),
    default="text",
    help="Output format (json for machine parsing, text for humans).",
)
@click.option(
    "-v",
    "--verbose",
    "verbosity",
    count=True,
    help=(
        "Log level for stderr: -v for INFO, -vv for DEBUG. Put it before the "
        "command (portolan -v check). The --verbose flag of a command such as "
        "check controls how much that command prints, not the log level."
    ),
)
@click.option(
    "-q",
    "--quiet",
    is_flag=True,
    default=False,
    help="Log only errors to stderr.",
)
@click.option(
    "--timings",
    is_flag=True,
    default=False,
    help=(
        "Record stage timings and counters. JSON output gets a meta block. "
        "Text output prints a table to stderr. Same as PORTOLAN_TIMINGS=1."
    ),
)
@click.pass_context
def cli(ctx: click.Context, output_format: str, verbosity: int, quiet: bool, timings: bool) -> None:
    """Portolan - Publish and manage cloud-native geospatial data catalogs."""
    # Store format in context for subcommands
    ctx.ensure_object(dict)
    ctx.obj["format"] = output_format
    # A command that never asks should_output_json still gets a clean stdout.
    should_output_json(ctx)

    configure_logging(verbosity=verbosity, quiet=quiet)
    # Tracing state is process-wide. A second run in the same process (a
    # notebook, an agent harness, a test) must not report the spans of the
    # first run, or get a meta block it did not ask for.
    trace.reset()
    trace.enable_from_env(timings_flag=timings)
    if trace.is_enabled():
        # The close callbacks run last in, first out. The root span closes
        # first, so the trace file and the table hold its final duration.
        ctx.call_on_close(lambda: _finish_trace(output_format))
        ctx.with_resource(trace.span(f"portolan.{ctx.invoked_subcommand}"))

    # Note: .env loading moved to individual commands after catalog path resolution
    # to ensure correct .env is loaded when --catalog/--portolan-dir is used.
    # See load_dotenv_and_warn_sensitive() helper.


def _finish_trace(output_format: str) -> None:
    """Write the trace file and, in text mode, the timing table to stderr.

    Click runs this when the root context closes, after a success and after
    ``SystemExit``. The JSON envelope prints before this point, so its root
    span reports the duration up to the print.
    """
    trace_file = os.environ.get(trace.TRACE_FILE_ENV)
    if trace_file:
        trace.write_chrome_trace(Path(trace_file))
    if output_format == "text":
        click.echo(trace.format_table(), err=True)


def _prompt_init_metadata(
    title: str | None,
    description: str | None,
    license_id: str | None,
    license_url: str | None,
) -> tuple[str | None, str | None, str | None, str | None]:
    """Ask the user for the catalog metadata the flags did not supply.

    A value that came from a flag is kept. The function runs only in
    interactive mode, so --auto and --json never reach it.

    Args:
        title: Catalog title, or None to prompt.
        description: Catalog description, or None to prompt.
        license_id: SPDX identifier, or None to prompt.
        license_url: URL of the license text, or None to prompt when the
            identifier is "other".

    Returns:
        Tuple of (title, description, license_id, license_url).
    """
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

    if license_id is None:
        license_id = click.prompt("License (SPDX identifier, or 'other')")
    if license_id.strip() == OTHER_LICENSE and license_url is None:
        license_url = click.prompt("URL of the license text")

    return title, description, license_id, license_url


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
    "--id",
    "catalog_id",
    type=str,
    default=None,
    help="Catalog id. Defaults to the sanitized directory name.",
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
@click.option(
    "--license",
    "license_id",
    type=str,
    default=None,
    help="SPDX license identifier, or 'other' with --license-url. Required.",
)
@click.option(
    "--license-url",
    type=str,
    default=None,
    help="URL of the license text. Required when --license is 'other'.",
)
@click.option(
    "--logo",
    type=str,
    default=None,
    help="Local image published as the catalog logo (PNG, WebP, JPEG, GIF, AVIF, APNG, SVG).",
)
@click.option(
    "--logo-title",
    type=str,
    default=None,
    help="Accessible label for the logo link. Defaults to the catalog title.",
)
@click.pass_context
def init(
    ctx: click.Context,
    path: Path,
    json_output: bool,
    auto_mode: bool,
    catalog_id: str | None,
    title: str | None,
    description: str | None,
    backend: str,
    license_id: str | None,
    license_url: str | None,
    logo: str | None,
    logo_title: str | None,
) -> None:
    r"""Initialize a new Portolan catalog.

    Creates a catalog.json at the root level and a .portolan directory with
    management files (config.yaml, metadata.yaml). Also creates versions.json at
    the root.

    Derives the catalog ID from the directory name. Pass --id to set it
    yourself. The ID is the catalog's public STAC identity, so a directory
    named for a step in your workflow makes a poor one.

    PATH is the directory where the catalog should be created (default: current directory).

    A license is required. Every collection inherits it from the catalog's
    metadata.yaml, and 'portolan add' refuses to write a collection without one.
    Pass an SPDX identifier, or 'other' with --license-url when the license has no
    identifier. Without --auto or --json you are prompted for it.

    Use --auto to skip all prompts and use default values. Use --id, --title,
    and --description to set catalog metadata directly.

    A logo is optional. Pass --logo with a local image to copy it into _assets/
    and publish it as a rel="icon" link, or add one later with 'portolan logo'.

    \b
    Examples:
        portolan init                            # Prompts for the license
        portolan init --auto --license CC-BY-4.0 # Skip prompts
        portolan init --title "My Catalog" --license MIT
        portolan init --id phl-housing --license CC-BY-4.0
        portolan init --license other --license-url https://x.org/terms
        portolan init --backend iceberg --license CC0-1.0
        portolan init --auto --license CC-BY-4.0 --logo brand.png
    """
    from portolan_cli.catalog import CatalogState, detect_state, init_catalog
    from portolan_core.errors import (
        CatalogAlreadyExistsError,
        LogoError,
        UnmanagedStacCatalogError,
    )

    use_json = should_output_json(ctx, json_output)

    # Interactive prompting (unless --auto or JSON mode)
    if not auto_mode and not use_json:
        title, description, license_id, license_url = _prompt_init_metadata(
            title, description, license_id, license_url
        )

    # Only gate the license on a directory that can actually become a catalog. An
    # existing catalog is the more fundamental problem and --license cannot fix it,
    # so let init_catalog report that instead of sending the user after a flag.
    if detect_state(path) is CatalogState.FRESH:
        license_id = _validated_cli_license("init", license_id, license_url, use_json=use_json)

    try:
        result = init_catalog(
            path,
            catalog_id=catalog_id,
            title=title,
            description=description,
            backend=backend,
            license_id=license_id,
            license_url=license_url,
            logo=logo,
            logo_title=logo_title,
        )

        data = result.to_dict()
        if not emit_success("init", data, use_json=use_json):
            success(f"Initialized Portolan catalog in {data['path']}")
            info_output(f"Catalog ID: {data['catalog_id']}")
            if data["logo"]:
                info_output(f"Logo: {data['logo']}")
            for w in result.warnings:
                warn(w)

    except InputValidationError as err:
        # A bad --id fails before init_catalog writes anything, so the directory
        # is untouched and the caller can retry with a valid id (issue #821).
        emit_error("init", "InputValidationError", str(err), use_json=use_json)
        raise SystemExit(1) from err
    except LogoError as err:
        emit_error("init", type(err).__name__, str(err), use_json=use_json, code=err.code)
        raise SystemExit(1) from err
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
# List command (top-level)
# =============================================================================


def _get_format_display_name(format_type: str | None) -> str:
    """Get human-readable format name.

    Args:
        format_type: Format family, for example "vector" or "raster".

    Returns:
        Human-readable format name (e.g., "GeoParquet", "COG"). A family
        from a plugin format shows as written.
    """
    from portolan_core.format_families import RASTER, VECTOR

    if format_type == VECTOR:
        return "GeoParquet"
    if format_type == RASTER:
        return "COG"
    return format_type or "Unknown"


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

        # Files directly in the collection directory come before the items
        for asset in col.assets:
            detail(f"  {_list_asset_line(asset, status_symbols)}")

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
                detail(f"    {_list_asset_line(asset, status_symbols)}")


def _list_asset_line(asset: AssetInfo, status_symbols: dict[AssetStatus, str]) -> str:
    """Format one ``list`` asset line: symbol, path, format, size, and status."""
    symbol = status_symbols.get(asset.status, "?")
    format_name = asset.format_name or "Unknown"

    size_str = ""
    if asset.size_bytes is not None:
        size_str = f", {format_size(asset.size_bytes)}"

    # Add status label for non-tracked
    status_label = ""
    if asset.status == AssetStatus.MODIFIED:
        status_label = ", modified"
    elif asset.status == AssetStatus.DELETED:
        status_label = ", deleted"

    return f"{symbol} {asset.path} ({format_name}{size_str}{status_label})"


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
    r"""List all files in the catalog with tracking status.

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

    result = list_catalog_contents(
        catalog_path,
        collection_id=collection,
        tracked_only=tracked_only,
        untracked_only=untracked_only,
    )

    if not emit_success("list", result.to_dict(), use_json=use_json):
        if result.is_empty():
            info_output("No tracked items")
            info_output("")
            detail("To get started:")
            detail(" portolan scan. Discover files in this directory")
            detail("  portolan add <path>  Track a specific file or directory")
            return

        _list_tree_output_with_status(result)


# =============================================================================
# Status command (Issue #389 - git-like version management)
# =============================================================================


@cli.command("status")
@click.option(
    "--collection",
    "-c",
    help="Show status for a specific collection only.",
)
@click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to catalog root (default: auto-detect).",
)
@click.option(
    "--offline",
    is_flag=True,
    help="Skip remote version check (show local state only).",
)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.pass_context
def status_cmd(
    ctx: click.Context,
    collection: str | None,
    catalog_path: Path | None,
    offline: bool,
    json_output: bool,
) -> None:
    r"""Show local vs remote version state for collections.

    Git-style status showing version sync state, modified files, and
    untracked files for each collection in the catalog.

    \b
    Status information:
        Local version   Current version in local versions.json
        Remote version  Current version on remote (unless --offline)
        Sync state      in_sync, ahead, behind, or unknown
        Modified        Files changed since last version
        Untracked       Files on disk not in versions.json
        Deleted         Files in versions.json but missing from disk

    \b
    Examples:
        portolan status                    # Status for all collections
        portolan status -c demographics    # Status for one collection
        portolan status --offline          # Skip remote check
        portolan status --json             # JSON output for agents
    """
    use_json = should_output_json(ctx, json_output)

    if catalog_path is None:
        catalog_path = require_catalog_root(use_json, "status")

    # Load remote URL from config (if not offline)
    remote_url: str | None = None
    if not offline:
        remote_url = resolve_remote(None, catalog_path, collection)

    from functools import partial

    from portolan_cli.sync.pull import fetch_remote_versions
    from portolan_core.status import get_catalog_status

    result = get_catalog_status(
        catalog_path,
        collection,
        offline=offline,
        remote_url=remote_url,
        # The download lines print as they did before the move to the library.
        fetch_remote_versions=partial(fetch_remote_versions, report=_report),
    )

    if not emit_success("status", result.to_dict(), use_json=use_json):
        if not result.collections:
            info_output("No collections found")
        else:
            _output_status_human(result.collections)


def _output_status_human(statuses: list[CollectionStatus]) -> None:
    """Format status output for human consumption."""
    for status in statuses:
        info_output(f"Collection: {status.collection}")

        # Version info
        local_str = status.local_version or "(not initialized)"
        info_output(f"  Local version: {local_str}")

        if status.remote_version is not None:
            sync_indicator = ""
            if status.sync_state == "behind":
                sync_indicator = "  ⚠ behind remote"
            elif status.sync_state == "ahead":
                sync_indicator = "  ↑ ahead of remote"
            info_output(f"  Remote version: {status.remote_version}{sync_indicator}")
        elif status.sync_state == "unknown":
            detail("  Remote version: (offline or not configured)")

        # Modified files
        if status.modified_files:
            info_output("")
            info_output("  Modified files:")
            for f in status.modified_files:
                warn(f"    {f} (checksum changed)")

        # Deleted files
        if status.deleted_files:
            info_output("")
            info_output("  Deleted files:")
            for f in status.deleted_files:
                error(f"    {f} (missing from disk)")

        # Untracked files
        if status.untracked_files:
            info_output("")
            info_output("  Untracked files:")
            for f in status.untracked_files:
                detail(f"    {f}")

        # Clean state message
        if (
            not status.modified_files
            and not status.deleted_files
            and not status.untracked_files
            and status.local_version
        ):
            success("  No local changes")

        info_output("")


# =============================================================================
# Info command (top-level)
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
    r"""Show information about a file, collection, or catalog.

    TARGET can be:
    - A file path (e.g., demographics/census.parquet) - shows file metadata
    - A collection directory (e.g., demographics/) - shows collection metadata
    - Omitted - shows catalog-level metadata

    The output format for files is:
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
    from portolan_cli.inspect import inspect_target

    use_json = should_output_json(ctx, json_output)

    try:
        result = inspect_target(target, catalog_root=catalog_path)
    except FileNotFoundError as err:
        emit_error("info", "FileNotFoundError", str(err), use_json=use_json)
        raise SystemExit(1) from err
    except ValueError as err:
        emit_error("info", "ValueError", str(err), use_json=use_json)
        raise SystemExit(1) from err

    if not emit_success("info", result.to_dict(), use_json=use_json):
        for line in result.format_human():
            info_output(line)


def _print_finding(finding: Any, requirement: str) -> None:
    """Print one rashid finding: rule id, path, message, then how to resolve it."""
    render = {
        RashidSeverity.ERROR: error,
        RashidSeverity.WARNING: warn,
    }.get(finding.severity, info_output)
    render(f"{finding.rule_id} {finding.path}: {finding.message}")
    if finding.fix_hint:
        detail(f"  Fix: {finding.fix_hint}")
    elif requirement:
        detail(f"  Requires: {requirement}")


def _print_findings(report: Any, *, verbose: bool) -> None:
    """Print findings grouped by severity, worst first."""
    from portolan_cli.validation import remediation_for

    groups = (
        ("Errors", report.errors),
        ("Warnings", report.warnings),
        ("Suggestions", report.infos if verbose else []),
    )
    for heading, findings in groups:
        if not findings:
            continue
        info_output(f"{heading} ({len(findings)}):")
        for finding in findings:
            _print_finding(finding, remediation_for(finding.rule_id).requirement)


def _print_check_summary(report: Any, *, strict: bool) -> None:
    """Print the one-line verdict for a rashid report."""
    error_count = len(report.errors)
    warning_count = len(report.warnings)

    if not error_count and not (strict and warning_count):
        success(f"Catalog conforms ({report.files_checked} file(s) checked)")
        return

    parts = []
    if error_count:
        parts.append(f"{error_count} error{'s' if error_count != 1 else ''}")
    if warning_count:
        parts.append(f"{warning_count} warning{'s' if warning_count != 1 else ''}")
    error(f"Catalog does not conform: {', '.join(parts)}")


def _print_remediation_split(payload: dict[str, Any]) -> None:
    """Tell the user which findings `--fix` handles and which need them."""
    counts = payload.get("counts_by_remediation")
    if not counts or not any(counts.values()):
        return
    if counts["auto"]:
        detail(f"  {counts['auto']} fixable by `portolan check --fix`")
    if counts["instruct"]:
        detail(f"  {counts['instruct']} need a decision (see Requires: lines)")
    if counts["external"]:
        detail(f"  {counts['external']} are hosting-server settings")


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


def _output_check_json(payload: dict[str, Any], *, failed: bool) -> None:
    """Wrap the validation payload in the standard output envelope.

    The envelope's error list names the failing rule ids so an agent reading
    only the envelope still learns what broke; the full findings, with fix hints
    and remediation buckets, sit in ``data``.
    """
    if not failed:
        output_json_envelope(success_envelope("check", payload))
        return

    findings = payload.get("findings", [])
    errors = [
        ErrorDetail(type=finding["rule_id"], message=finding["message"])
        for finding in findings
        if finding["severity"] == "error"
    ] or [ErrorDetail(type="ValidationError", message="Catalog does not conform")]
    output_json_envelope(error_envelope("check", errors, data=payload))


@cli.command()
@click.argument("path", type=click.Path(path_type=Path), default=".")
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON")
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help=(
        "Show all validation rules, not just failures. "
        "For log messages, use the root flag: portolan -v check."
    ),
)
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
    "--force",
    is_flag=True,
    help="Re-optimize already-valid COGs by re-applying current COG settings, "
    "e.g. to add missing overviews (use with --fix; rasters only)",
)
@click.option(
    "--workers",
    "-w",
    type=click.IntRange(min=1),
    default=None,
    help="Parallel worker processes for conversion (default: auto-detect from "
    "CPU count; use 1 for sequential). Applies to --fix.",
)
@click.option(
    "--metadata",
    is_flag=True,
    help=(
        "Only check/fix STAC metadata (links, schema, staleness). With --fix this is "
        "the metadata refresh. It exits 1 while an error remains, and the JSON success "
        "field is false only when a fix fails, an auto-fixable error remains, or a file "
        "does not parse or breaks the STAC schema. --strict also exits 1 on a warning."
    ),
)
@click.option(
    "--geo-assets",
    "geo_assets",
    is_flag=True,
    help="Only check/fix geospatial assets (cloud-native status, convertibility)",
)
@click.option(
    "--strict",
    is_flag=True,
    help="Fail on warnings as well as errors",
)
@click.option(
    "--live",
    is_flag=True,
    help="Probe the published host over HTTP for Range support, CORS headers, and link targets",
)
@click.option(
    "--url",
    "public_url",
    default=None,
    help="Base URL the catalog is published under (overrides publish.public_url)",
)
@click.option(
    "--no-data",
    "no_data",
    is_flag=True,
    help="Skip the data pass; validate metadata without reading asset bytes",
)
@click.option(
    "--data-scope",
    "data_scope",
    type=click.Choice(["all", "local"]),
    default="all",
    help="Which assets the data pass may read: all (default), or local to read only "
    "the assets inside the catalog tree, leaving assets hosted elsewhere unread",
)
@click.option(
    "--no-structural",
    "no_structural",
    is_flag=True,
    help="Skip the STAC 1.1.0 structural pass",
)
@click.option(
    "--schema",
    is_flag=True,
    help="Also validate against the Portolan profile JSON Schema (restates hand-rule "
    "findings; useful on catalogs produced by other tooling)",
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
    force: bool,
    workers: int | None,
    metadata: bool,
    geo_assets: bool,
    strict: bool,
    live: bool,
    public_url: str | None,
    no_data: bool,
    data_scope: str,
    no_structural: bool,
    schema: bool,
) -> None:
    """Validate a Portolan catalog against the Portolan spec.

    Validation runs on rashid, which reports PTL-* rule ids citing the spec
    requirements they enforce. By default it checks metadata, STAC 1.1.0
    structure, and the asset bytes themselves. The metadata and structural
    passes are offline. The data pass reads asset bytes, including remote https
    assets over HTTP range requests. Use --data-scope local to read only the
    assets inside the catalog tree. Use --no-data to skip the pass. With --fix,
    applies the mechanical repairs and re-checks.

    The --live pass probes the published host for HTTP Range support and CORS
    headers. It also requests every link target under the base URL, and reports
    a linked file the host does not serve. It skips an upstream host only when it knows the base URL, from
    --url or publish.public_url. Without a base URL it probes every absolute
    href as declared, which can report CORS failures against a host you do not
    control.

    PATH is the directory to check (default: current directory).

    Use --metadata or --geo-assets to limit scope:
    - --metadata: Only validate the catalog (its own metadata, structure, data)
    - --geo-assets: Only check source files on disk for cloud-native status
    - Neither: Both (default)

    Examples:
        portolan check                        # Validate all (metadata + geo-assets)

        portolan check --metadata             # Validate the catalog only

        portolan check --geo-assets           # Check source files only

        portolan check --data-scope local     # Read only assets inside the catalog tree

        portolan check --no-data              # Skip reading asset bytes (faster)

        portolan check --live                 # Also probe the published host

        portolan check --fix                  # Fix what can be fixed, then re-check

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

    # Warn if --force is used without --fix
    if force and not fix:
        warn("--force requires --fix")

    # Warn if --data-scope is set while --no-data disables the pass it scopes
    if data_scope != "all" and no_data:
        warn("--data-scope has no effect with --no-data")

    from portolan_cli.validation.workflow import run_check_workflow
    from portolan_core.errors import NotACatalogError

    def show_conversion_progress(result: ConversionResult) -> None:
        info_output(f"Converting: {result.source.name}")

    def show_fix_report(report: FixReport | None, conversion: Any) -> None:
        _output_fix_human(
            metadata_fix_report=report,
            format_fix_report=conversion,
            verbose=verbose,
            dry_run=dry_run,
        )

    from portolan_cli.extract.common.cli_helpers import print_message
    from portolan_cli.report import reporting

    try:
        # The conversion steps report their notices through this scope.
        with reporting(print_message):
            result = run_check_workflow(
                path,
                fix=fix,
                dry_run=dry_run,
                remove_legacy=remove_legacy,
                force=force,
                workers=workers,
                metadata=metadata,
                geo_assets=geo_assets,
                strict=strict,
                live=live,
                public_url=public_url,
                data=not no_data,
                data_scope=data_scope,
                structural=not no_structural,
                schema=schema,
                # Per-file progress only in verbose human mode.
                on_progress=show_conversion_progress if verbose and not use_json else None,
                # Each fix report prints as its pass ends, so a later failure cannot hide it.
                on_fix_report=None if use_json else show_fix_report,
            )
    except InvalidLiveUrlError as err:
        emit_error("check", type(err).__name__, str(err), use_json=use_json, code=err.code)
        raise SystemExit(2) from err
    except NotACatalogError as err:
        # The envelope keeps its shape from before the error class existed: no code.
        emit_error("check", "NotACatalogError", err.message, use_json=use_json)
        raise SystemExit(1) from err
    _output_check_result(result, use_json=use_json, verbose=verbose, strict=strict, dry_run=dry_run)


def _output_check_result(
    result: Any, *, use_json: bool, verbose: bool, strict: bool, dry_run: bool
) -> None:
    """Render the check result, then exit 1 when it failed.

    The exit code follows ``passed``: a blocking finding fails the command,
    whatever ``--fix`` repaired. ``result.failed`` sets the envelope's ``success``.
    """
    payload = result.to_dict()
    fixed = "fix" in payload
    if use_json:
        _output_check_json(payload, failed=result.failed)
    else:
        post_fix = fixed and not dry_run
        _render_check(result.outcome, payload, verbose=verbose, strict=strict, post_fix=post_fix)
        if fixed:
            _print_fix_split(payload, dry_run=dry_run)

    if result.failed or not payload["passed"]:
        raise SystemExit(1)


def _output_fix_human(
    *,
    metadata_fix_report: FixReport | None,
    format_fix_report: Any,
    verbose: bool,
    dry_run: bool,
) -> None:
    """Output combined fix results in human-readable format.

    Args:
        metadata_fix_report: Results from metadata fix (if run).
        format_fix_report: Results from geo-asset fix (if run).
        verbose: Show detailed output.
        dry_run: Whether this was a dry run.
    """
    from portolan_cli.metadata.fix import FixReport

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
    emit_error("check", "PathNotFoundError", f"Path does not exist: {path}", use_json=use_json)
    raise SystemExit(1)


def _print_fix_skips(skipped: dict[str, list[str]]) -> None:
    """List every fixer that ran and changed nothing, with its reason."""
    if not skipped:
        return
    info_output(f"Skipped ({len(skipped)}):")
    for fixer_key, reasons in skipped.items():
        detail(f"  {fixer_key}: {reasons[0] if reasons else 'nothing to change'}")


def _print_fix_split(payload: dict[str, Any], *, dry_run: bool = False) -> None:
    """Render the post-fix verdict: what --fix handled, what still needs a person.

    The "Action required" list is the agent's stopping condition — every entry
    carries the imperative requirement for the defect, and a survivor of an AUTO
    fixer says so, so nobody re-runs --fix hoping for a different answer.

    "Applied" names only fixers that changed something, and every fixer that ran
    without changing anything is listed with its reason at default verbosity: a
    skip an operator cannot see reads as a silent success.

    Under ``dry_run`` the report stops after the selection. No re-check ran, so
    there is no survivor set to annotate and no repair count to give. What a
    preview can honestly report is which fixers the findings selected and how
    many AUTO defects they were selected against. A dry run keeps the findings
    list above, which already carries the requirement for each one.
    """
    fix_data: dict[str, Any] = payload.get("fix", {})
    applied = fix_data.get("applied", [])
    skipped: dict[str, list[str]] = fix_data.get("skipped", {})

    if dry_run:
        success(f"Would fix automatically ({fix_data.get('auto_count', 0)})")
        if applied:
            detail(f"  Would apply: {', '.join(applied)}")
        _print_fix_skips(skipped)
        return

    survivors = {
        (item["rule_id"], item["path"], item.get("json_pointer"), item.get("index", 0))
        for item in fix_data.get("survivors") or []
    }
    findings = payload.get("findings", [])

    success(f"Fixed automatically ({fix_data.get('fixed_count', 0)})")
    if applied:
        detail(f"  Applied: {', '.join(applied)}")
    _print_fix_skips(skipped)

    if not findings:
        return
    info_output(f"Action required ({len(findings)}):")
    seen: dict[tuple[str, str | None, str | None], int] = {}
    for finding in findings:
        requirement = (
            finding.get("requirement") or finding.get("fix_hint") or finding.get("message", "")
        )
        line = f"{finding['rule_id']} {finding['path']}: {requirement}"
        key = (finding["rule_id"], finding["path"], finding.get("json_pointer"))
        index = seen.get(key, 0)
        seen[key] = index + 1
        if (*key, index) in survivors:
            line += " [the automatic fix did not resolve this]"
        detail(f"  {line}")


def _render_check(
    outcome: Any,
    payload: dict[str, Any],
    *,
    verbose: bool,
    strict: bool,
    post_fix: bool = False,
) -> None:
    """Render the human-readable check result.

    After ``--fix`` the per-finding list and the remediation split are dropped:
    :func:`_print_fix_split` says the same things better, naming what was fixed
    and giving every remaining finding its requirement. Printing both listed each
    survivor twice. ``--verbose`` keeps the raw findings for the detail.
    """
    if outcome.legacy_note is not None:
        warn(outcome.legacy_note)
    if outcome.workflow_notice is not None:
        warn(outcome.workflow_notice.message)

    if outcome.report is not None:
        if verbose or not post_fix:
            _print_findings(outcome.report, verbose=verbose)
        _print_check_summary(outcome.report, strict=strict)
        if not post_fix:
            _print_remediation_split(payload)

    if outcome.format_report is not None:
        info_output("Source files:")
        _print_format_check_results(outcome.format_report, verbose=verbose)

    if outcome.live_hint is not None:
        info_output(outcome.live_hint.message)


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


def _print_legacy_removal_results(removal: Any, *, verbose: bool = False) -> None:
    """Print the outcome of the legacy-file removal step.

    Args:
        removal: LegacyRemovalReport, or None when the step did not run.
        verbose: If True, name every removed file.
    """
    if removal is None:
        return
    if removal.success_count > 0:
        success(f"Removed {removal.success_count} legacy file(s)")
        if verbose:
            for removed_path in removal.removed:
                detail(f"  {removed_path.name}")
    if removal.error_count > 0:
        error(f"Failed to remove {removal.error_count} file(s)")
        for failed_path, err_msg in removal.errors.items():
            error(f"  {failed_path.name}: {err_msg}")


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

    _print_legacy_removal_results(report.legacy_removal_report, verbose=verbose)


# ─────────────────────────────────────────────────────────────────────────────
# Scan command
# ─────────────────────────────────────────────────────────────────────────────


def _print_fix_report(result: ScanResult, *, dry_run: bool) -> None:
    """Print the fixes that scan --fix proposed and applied.

    Args:
        result: Scan result with the proposed and applied fixes.
        dry_run: If True, the fixes are a preview and none were applied.
    """
    proposed = result.proposed_fixes
    if not proposed:
        info_output("No issues to fix")
        return

    if dry_run:
        info_output(f"Dry run: {len(proposed)} fix(es) would be applied")
        for fix in proposed:
            detail(f"  {fix.preview}")
        return

    applied = result.applied_fixes
    if applied:
        success(f"Applied {len(applied)} fix(es)")
        for fix in applied:
            detail(f"  {fix.preview}")

    # A fix that did not apply hit a name collision.
    failed = [p for p in proposed if p not in applied]
    if failed:
        warn(f"{len(failed)} fix(es) could not be applied (collision):")
        for fix in failed:
            detail(f"  {fix.preview}")


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
    r"""Scan a directory for geospatial files and potential issues.

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

    from portolan_cli.scan.workflow import run_scan

    # Pre-count directories only when progress will be displayed
    # ScanProgressReporter displays progress when: not json_mode AND stderr is TTY
    # count_directories returns 0 for a bad path, and run_scan then reports it.
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
            outcome = run_scan(
                path,
                recursive=not no_recursive,
                max_depth=max_depth,
                include_hidden=include_hidden,
                follow_symlinks=follow_symlinks,
                show_all=show_all,
                suggest_collections=suggest_collections,
                strict=strict,
                fix=fix,
                dry_run=dry_run,
                progress_callback=progress_reporter.advance,
            )
    except (FileNotFoundError, NotADirectoryError) as err:
        # The class name is the error type: PathNotFoundError for a missing path.
        emit_error("scan", type(err).__name__, str(err), use_json=use_json)
        raise SystemExit(1) from err

    # Warn if --dry-run is used without --fix (no effect)
    if dry_run and not fix:
        warn("--dry-run has no effect without --fix")

    if fix and not use_json:
        _print_fix_report(outcome.result, dry_run=dry_run)

    # Output results in appropriate format
    if use_json:
        _output_scan_json(outcome.to_dict(), outcome.failing_issues)
    else:
        _output_scan_results(
            outcome.result,
            manual_only=manual_only,
            show_all=show_all,
            show_tree=show_tree,
            elapsed_seconds=progress_reporter.elapsed_seconds,
        )

    # Strict mode: errors OR warnings fail the command.
    if outcome.strict_failure:
        _handle_strict_mode_exit(outcome.result, use_json=use_json)

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
    manual_only: bool,
    show_all: bool,
    show_tree: bool,
    elapsed_seconds: float = 0.0,
) -> None:
    """Print human-readable scan results.

    Args:
        result: The scan result.
        manual_only: If True, show only manual-resolution issues.
        show_all: If True, show all issues without truncation.
        show_tree: If True, show directory tree view.
        elapsed_seconds: Time elapsed during scan (for progress reporting).
    """
    if manual_only:
        from portolan_cli.scan.output import format_scan_output

        output = format_scan_output(result, manual_only=True)
        click.echo(output)
    else:
        _print_scan_summary_enhanced(
            result,
            show_all=show_all,
            show_tree=show_tree,
            elapsed_seconds=elapsed_seconds,
        )


def _output_scan_json(data: dict[str, Any], failing_issues: list[ScanIssue]) -> None:
    """Output scan results as JSON envelope.

    A scan with failing issues still returns its data, in an error envelope.

    Args:
        data: The envelope data, from ScanOutcome.to_dict.
        failing_issues: The errors, plus the warnings in strict mode.
    """
    if not failing_issues:
        emit_success("scan", data, use_json=True)
        return
    errors = [
        ErrorDetail(type=issue.issue_type.value, message=issue.message) for issue in failing_issues
    ]
    output_json_envelope(error_envelope("scan", errors, data=data))


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


# Maximum example paths to show per batched issue group
BATCH_EXAMPLES_LIMIT = 3


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


# Scan issues group by their type and message. Issues that share both batch
# into one line.
_GroupKey = tuple[IssueType, str]


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
        groups: dict[_GroupKey, list[ScanIssue]] = {}
        for issue in severity_issues:
            key: _GroupKey = (issue.issue_type, issue.message)
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
    from portolan_cli.scan.classify import FileCategory

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
# Top-level add/rm commands
# ─────────────────────────────────────────────────────────────────────────────


if TYPE_CHECKING:
    from collections.abc import Iterator

    from portolan_cli.add import AddOutcome
    from portolan_cli.extract.common.progress import MessageCallback
    from portolan_cli.stac_parquet import MirrorReport
    from portolan_cli.viz.pmtiles import PMTilesOutcome, PMTilesResult


def _output_add_json(outcome: AddOutcome) -> None:
    """Output add command results as JSON."""
    data = outcome.to_dict()
    if outcome.failures:
        envelope = error_envelope(
            "add",
            [
                ErrorDetail(type="AddError", message=f"{f.path!s}: {f.error}")
                for f in outcome.failures
            ],
            data=data,
        )
    else:
        envelope = success_envelope("add", data)
    output_json_envelope(envelope)


def _format_sidecar_note(ds: ItemInfo) -> str:
    """Format sidecar count note for add output."""
    if not ds.asset_paths:
        return ""
    sidecars = get_sidecars(Path(ds.asset_paths[0]))
    return f" (+ {len(sidecars)} sidecars)" if sidecars else ""


def _output_added_single_collection(added: list[ItemInfo], *, verbose: bool = False) -> None:
    """Output added items for a single collection.

    Only show per-file output when verbose=True.
    Default is summary only (progress bar + final count).
    """
    if verbose:
        coll = added[0].collection_id
        count = len(added)
        info_output(f"Adding {count} file{'s' if count != 1 else ''} to {coll}")
        for ds in added:
            success(f"  + {ds.item_id}{_format_sidecar_note(ds)}")


def _output_added_multi_collection(added: list[ItemInfo], *, verbose: bool = False) -> None:
    """Output added items grouped by collection.

    Only show per-file output when verbose=True.
    Default is summary only (progress bar + final count).
    """
    if verbose:
        collections: dict[str, list[ItemInfo]] = {}
        for ds in added:
            collections.setdefault(ds.collection_id, []).append(ds)

        total = len(added)
        info_output(
            f"Adding {total} file{'s' if total != 1 else ''} to {len(collections)} collections"
        )
        for coll, items in sorted(collections.items()):
            info_output(f"  {coll}:")
            for ds in items:
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


def _output_add_unchanged(skipped: list[tuple[Path, str]], verbose: bool) -> None:
    """Say that the add changed nothing, and why each file was skipped."""
    from portolan_cli.add import SKIPS_THAT_TRACK

    skip_count = len(skipped)
    noun = "file" if skip_count == 1 else "files"
    if all(reason in SKIPS_THAT_TRACK for _, reason in skipped):
        success(f"All {skip_count} {noun} already tracked (unchanged)")
    else:
        info_output(f"Nothing added: {skip_count} {noun} skipped")
    if verbose:
        for p, reason in skipped:
            detail(f"  {p.name}: {reason}")


def _output_add_summary(added: list[ItemInfo]) -> None:
    """Output final success summary after adding files."""
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
    added: list[ItemInfo],
    skipped: list[tuple[Path, str]],
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

    # Output successful adds (per-file details only in verbose mode)
    if added:
        unique_collections = {ds.collection_id for ds in added}
        if len(unique_collections) == 1:
            _output_added_single_collection(added, verbose=verbose)
        else:
            _output_added_multi_collection(added, verbose=verbose)

    # Show skipped files in verbose mode
    if verbose and skipped:
        for p, reason in skipped:
            detail(f"Skipping {p.name} ({reason})")

    # Final success summary (always show if we added something, even with failures)
    # Summary is always shown, failures are shown separately
    if added:
        _output_add_summary(added)

    # Output failures batched by error message (Issue #199)
    if failures:
        _print_add_failures_batched(failures)


def _add_report(*, verbose: bool) -> MessageCallback:
    """Return the callback that prints each library line of ``add`` as before.

    A detail line is per-file detail, so it shows with --verbose only.
    """
    printers = {"success": success, "info": info_output, "warn": warn, "error": error}

    def report(level: str, message: str) -> None:
        if level == "detail":
            if verbose:
                detail(message)
        else:
            printers[level](message)

    return report


def _report_add_step(step: MirrorReport | PMTilesOutcome, *, verbose: bool, use_json: bool) -> None:
    """Print one finished side step of ``add``: the mirrors, or one collection's PMTiles."""
    from portolan_cli.stac_parquet import MirrorReport

    if isinstance(step, MirrorReport):
        if verbose:
            for coll_id in step.generated:
                info_output(f"Generated items.parquet for '{coll_id}'")
        for coll_id, exc in step.failed.items():
            warn(f"Failed to generate parquet for '{coll_id}': {exc}")
    elif not use_json:
        _report_pmtiles(step, verbose=verbose)


def _report_pmtiles(outcome: PMTilesOutcome, *, verbose: bool) -> None:
    """Print the PMTiles of one collection, and the explicit request that failed."""
    from portolan_cli.viz.pmtiles import PMTilesNotAvailableError

    if outcome.result is not None:
        _report_pmtiles_result(outcome.result, verbose=verbose, requested=outcome.requested)
    elif outcome.requested:
        pass
    elif not isinstance(outcome.missing, PMTilesNotAvailableError):
        warn(f"Skipping PMTiles for '{outcome.collection_id}': tippecanoe not installed")
    elif outcome.config_enabled:
        warn(
            f"PMTiles enabled for '{outcome.collection_id}' "
            "but geoparquet-io has no PMTiles support"
        )
    elif verbose:
        info_output(
            f"Skipping PMTiles for '{outcome.collection_id}': geoparquet-io has no PMTiles support"
        )
    if outcome.failure is not None:
        error(outcome.failure)


def _report_pmtiles_result(result: PMTilesResult, *, verbose: bool, requested: bool) -> None:
    """Print the PMTiles of one collection. A failure of a request prints as the failure."""
    for p in result.generated:
        if p in result.older:
            success(f"Rebuilt PMTiles: {p.name} was older than {result.sources[p].name}")
        else:
            success(f"Generated PMTiles: {p.name}")
    for p in result.skipped:
        if p in result.older:
            warn(
                f"Kept PMTiles: {p.name} is older than {result.sources[p].name}. "
                "Pass --pmtiles to rebuild it."
            )
        elif verbose:
            info_output(f"Skipped PMTiles: {p.name} is not older than {result.sources[p].name}")
    if not requested:
        for path, error_msg in result.failed:
            warn(f"Failed to generate PMTiles for '{path}': {error_msg}")


def _output_add_results(outcome: AddOutcome, verbose: bool, use_json: bool) -> None:
    """Output results for add command.

    Per Issue #175: Shows both successes and failures, enabling users to see
    all issues at once rather than stopping on the first error.
    """
    if use_json:
        _output_add_json(outcome)
    else:
        _output_add_human(outcome.added, outcome.skipped, outcome.failures, verbose)


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
        # Use .portolan/config.yaml as the single sentinel
        if not (catalog_root / ".portolan" / "config.yaml").exists():
            emit_error(
                "add", "NotACatalogError", f"Not a catalog: {catalog_root}", use_json=use_json
            )
            if not use_json:
                detail("Run 'portolan init' to create a catalog")
            raise SystemExit(1)
        return catalog_root

    # Auto-detect catalog root (git-style)
    detected_root = find_catalog_root()
    if detected_root is None:
        emit_error(
            "add",
            "NotACatalogError",
            "Not inside a Portolan catalog (no .portolan/config.yaml found)",
            use_json=use_json,
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
        emit_error(
            "add",
            "ValueError",
            "--item-id can only be used with a single file, not multiple paths",
            use_json=use_json,
        )
        raise SystemExit(1)

    # Exactly 1 path: check if it's a directory
    if paths[0].resolve().is_dir():
        emit_error(
            "add",
            "ValueError",
            "--item-id can only be used with a single file, not a directory",
            use_json=use_json,
        )
        raise SystemExit(1)


def _check_partition_prompt(
    resolved_paths: list[Path],
    catalog_root: Path,
    state: RunState | None = None,
) -> bool:
    """Check if user wants to skip partitioning for large files.

    Pre-scans files against partition threshold and prompts user in interactive
    mode. Returns True if partitioning should be skipped.

    Args:
        resolved_paths: List of resolved paths to check.
        catalog_root: Path to catalog root for config lookup.
        state: The run state, which parses the config once per run.

    Returns:
        True if user declined partitioning, False otherwise.
    """
    from portolan_cli.partitioning import find_large_partition_candidates

    # Prompting only makes sense interactively; skip the config read + scan
    # entirely when not attached to a terminal.
    if not sys.stderr.isatty():
        return False

    decision = find_large_partition_candidates(resolved_paths, catalog_root, state=state)
    large_files = decision.large_files
    if not large_files:
        return False

    warn(f"Found {len(large_files)} file(s) exceeding {decision.threshold_gb} GB threshold:")
    for lf, size in large_files[:5]:  # Show first 5
        detail(f"  {lf.name} ({size:.2f} GB)")
    if len(large_files) > _MAX_LARGE_FILES_SHOWN:
        detail(f"  ... and {len(large_files) - 5} more")

    if not click.confirm("Partition large files into spatial chunks?", default=True):
        info_output("Skipping partitioning (files will be added as-is)")
        return True

    return False


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
        "run separate add commands. If omitted, items carry a null datetime and the "
        "sentinel start/end range (portolan check will flag them)."
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
    help="Regenerate PMTiles even if they exist and are up-to-date. Implies --pmtiles.",
)
@click.option(
    "--thumbnails/--no-thumbnails",
    "generate_thumbnails",
    default=None,
    help=(
        "Generate a thumbnail asset for each affected collection. "
        "Defaults to the catalog's thumbnails.enabled setting. "
        "Opting out leaves PTL-VIZ-001 firing, so pair --no-thumbnails with "
        "check.disabled in config.yaml if the catalog never ships thumbnails."
    ),
)
@click.option(
    "--force-thumbnails",
    "force_thumbnails",
    is_flag=True,
    help=(
        "Redraw the thumbnail even if one is registered. Use after a collection "
        "grows, since the extent it draws is otherwise fixed at first render. "
        "A thumbnail.png or preview.png you supplied still wins."
    ),
)
@click.option(
    "--force",
    is_flag=True,
    help="Re-process all files, ignoring change detection.",
)
@click.option(
    "--reconvert",
    is_flag=True,
    help="Re-convert from source files (requires --force).",
)
@click.option(
    "--merge-strategy",
    "merge_strategy",
    type=click.Choice(["smart", "keep", "overwrite"], case_sensitive=False),
    default="smart",
    help=(
        "How to merge the human-authored fields with the detected values. "
        "Human fields are the asset title and description, custom asset fields, "
        "and column descriptions. Fields read from the data always follow the data. "
        "'smart' (default): keep a human value and fill a missing one from the data. "
        "'keep': keep a human value and never fill it from the data. "
        "'overwrite': drop the human values."
    ),
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
    generate_thumbnails: bool | None,
    force_thumbnails: bool,
    force: bool,
    reconvert: bool,
    merge_strategy: str,
) -> None:
    r"""Track files in the catalog.

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
        assets.

    \b
    Datetime handling:
        --datetime applies to ALL items added in this command. For items
        with different acquisition dates, run separate add commands:

            portolan add census/2020/ --datetime 2020-04-01
            portolan add census/2023/ --datetime 2023-04-01

        If --datetime is omitted, items have a null temporal extent and carry
        the sentinel start/end range. Run 'portolan check' to find items
        needing dates.

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
    - All files in the item directory are tracked, not just geo files

    \b
    Large file partitioning:
        GeoParquet files exceeding 2GB are automatically partitioned into
        spatial chunks using KD-tree partitioning. In interactive mode,
        you'll be prompted before partitioning. Configure via:

            partitioning.enabled: true/false (default: true)
            partitioning.prompt: true/false (default: true)
            partitioning.threshold_gb: size in GB (default: 2.0)
    """
    import contextlib

    from portolan_cli.add import run_add

    use_json = should_output_json(ctx, json_output)

    # Validate --reconvert requires --force
    if reconvert and not force:
        emit_error("add", "UsageError", "--reconvert requires --force", use_json=use_json)
        raise SystemExit(1)

    # Resolve and validate catalog root (git-style auto-detection)
    catalog_root = _resolve_catalog_root_for_add(catalog_path, use_json)

    from portolan_core.run_state import RunState

    # One state for the whole command: each YAML file parses once (issue #994).
    # run_add commits it.
    state = RunState.open(catalog_root)

    # Load .env so PORTOLAN_REMOTE (and other sensitive settings) reach the
    # versioning backend's post-add hooks (e.g. iceberg STAC upload).
    load_dotenv_and_warn_sensitive(catalog_root, state=state)

    # Validate --item-id usage (only valid with single file, not directory)
    _validate_item_id_usage(item_id, paths, use_json)

    # Resolve all CLI paths upfront and deduplicate by resolved path.
    # Using dict.fromkeys preserves order while deduplicating.
    resolved_paths: list[Path] = list(dict.fromkeys(p.resolve() for p in paths))

    # For single-file adds, validate that the file is in a collection subdirectory.
    # This catches the error early with a clear message, matching pre-batch behavior.
    if len(resolved_paths) == 1 and not resolved_paths[0].is_dir():
        try:
            resolve_collection_id(resolved_paths[0], catalog_root)
        except ValueError as err:
            emit_error("add", "PathError", str(err), use_json=use_json)
            raise SystemExit(1) from err

    # Pre-count files for progress bar (unified progress output)
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

    # Check if user wants to skip partitioning for large files (Phase 5: auto-partition UX)
    skip_partitioning = (
        _check_partition_prompt(resolved_paths, catalog_root, state) if not use_json else False
    )

    @contextlib.contextmanager
    def adding() -> Iterator[None]:
        # Only an error from adding the files becomes an error report. An error
        # from a side step propagates, as it always did.
        try:
            with progress_reporter:
                yield
        except MissingLicenseError as err:
            # Issue #686: a collection with no license fails PTL-LIC-001/002 the moment
            # check reads it, so add refuses the write and names what to supply.
            _fail_on_missing_license("add", err, use_json=use_json)
        except (
            ValueError,
            FileNotFoundError,
            AssetKeyCollisionError,
            ConfigError,
            UnknownFormatHandlerError,
        ) as err:
            # Issue #933: AssetKeyCollisionError is add refusing to replace one
            # file's asset with another file. A ConfigError is a config.yaml key
            # that add reads, such as a malformed ignored_files (#1044). An
            # UnknownFormatHandlerError is a conversion.handlers pin to a handler
            # that no plugin provides. Its message names the known handlers.
            _fail_on_add_error(err, resolved_paths, use_json=use_json)
        trace.current_span().set(elapsed_seconds=progress_reporter.elapsed_seconds)

    outcome = run_add(
        resolved_paths,
        catalog_root,
        item_id=item_id,
        item_datetime=item_datetime,
        workers=workers,
        generate_parquet=generate_parquet,
        generate_pmtiles=generate_pmtiles,
        force_pmtiles=force_pmtiles,
        generate_thumbnails=generate_thumbnails,
        force_thumbnails=force_thumbnails,
        force=force,
        reconvert=reconvert,
        merge_strategy=merge_strategy,
        skip_partitioning=skip_partitioning,
        on_progress=on_file_progress,
        adding=adding(),
        on_message=_add_report(verbose=verbose),
        on_step=lambda step: _report_add_step(step, verbose=verbose, use_json=use_json),
        state=state,
    )
    # An explicit PMTiles request that failed exits 1 with no envelope, as before.
    if outcome.pmtiles.failure is not None:
        raise SystemExit(1)

    # Output combined results (after all processing complete)
    _output_add_results(outcome, verbose, use_json)

    # Exit with non-zero code if any failures occurred
    if outcome.failures:
        raise SystemExit(1)


@cli.command("add-external")
@click.argument("url")
@click.option(
    "--collection",
    "collection_id",
    default=None,
    help="Collection ID for the external collection (default: derived from the URL).",
)
@click.option("--title", default=None, help="Human-readable title for the collection/asset.")
@click.option("--description", default=None, help="Collection description.")
@click.option(
    "--media-type",
    default=None,
    help="Asset media type (default: inferred from the URL extension).",
)
@click.option(
    "--license",
    "license_id",
    default=None,
    help="SPDX license identifier, or 'other' with --license-url. Required.",
)
@click.option(
    "--license-url",
    default=None,
    help="URL of the license text. Required when --license is 'other'.",
)
@click.option(
    "--via",
    "via_url",
    default=None,
    help="Provenance URL for the rel:via link (default: the data URL itself).",
)
@click.option(
    "--bbox",
    default=None,
    help="WGS84 bounding box as 'min_x,min_y,max_x,max_y' (default: global extent).",
)
@click.option(
    "--portolan-dir",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to Portolan catalog root (default: auto-detect by walking up from cwd).",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Overwrite existing collection if it exists.",
)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.pass_context
def add_external_cmd(
    ctx: click.Context,
    json_output: bool,
    url: str,
    collection_id: str | None,
    title: str | None,
    description: str | None,
    media_type: str | None,
    license_id: str | None,
    license_url: str | None,
    via_url: str | None,
    bbox: str | None,
    catalog_path: Path | None,
    force: bool,
) -> None:
    r"""Register remote data as a collection WITHOUT downloading or converting it.

    The external counterpart to 'portolan add'. Some valuable data sources are
    already published cloud-natively at a remote location and should be
    *referenced in place* rather than copied. This creates a collection.json
    whose collection-level 'data' asset href is the remote URL (kept as-is,
    carrying the 'external' role) plus a rel:via provenance link.

    The remote asset is never fetched, and 'portolan check' will not try to
    convert it (the metadata scanner skips scheme-qualified hrefs).

    A license is required. Pass an SPDX identifier, or 'other' with --license-url
    when the license has no identifier. Referencing someone else's data in place
    does not make its license unknowable, and a collection without one is an error
    under 'portolan check'.

    \b
    Examples:
        # Overture Maps places — planet-scale GeoParquet on Overture's S3
        portolan add-external \\
            "s3://overturemaps-us-west-2/release/2024-09-18.0/theme=places/type=place/*" \\
            --collection overture-places \\
            --title "Overture Maps — Places" \\
            --license ODbL-1.0 \\
            --via "https://docs.overturemaps.org/guides/places/"

        portolan add-external "https://example.org/data/buildings.parquet" \\
            --license other --license-url "https://example.org/terms"
    """
    from portolan_cli.external import add_external

    use_json = should_output_json(ctx, json_output)
    catalog_root = _resolve_catalog_root_for_add(catalog_path, use_json)

    parsed_bbox: list[float] | None = None
    if bbox is not None:
        try:
            parsed_bbox = [float(v) for v in bbox.split(",")]
        except ValueError as err:
            emit_error("add-external", "UsageError", f"Invalid --bbox: {err}", use_json=use_json)
            raise SystemExit(1) from err
        if len(parsed_bbox) != BBOX_2D_LEN:
            emit_error(
                "add-external",
                "UsageError",
                "--bbox must have 4 comma-separated values: min_x,min_y,max_x,max_y",
                use_json=use_json,
            )
            raise SystemExit(1)

    resolved_license = _validated_cli_license(
        "add-external", license_id, license_url, use_json=use_json
    )

    try:
        result = add_external(
            catalog_root=catalog_root,
            url=url,
            collection_id=collection_id,
            title=title,
            description=description,
            media_type=media_type,
            license=resolved_license,
            license_url=license_url,
            via_url=via_url,
            bbox=parsed_bbox,
            force=force,
        )
    except (ValueError, FileNotFoundError, FileExistsError, InputValidationError) as err:
        emit_error("add-external", type(err).__name__, str(err), use_json=use_json)
        raise SystemExit(1) from err

    if not emit_success("add-external", result.to_dict(), use_json=use_json):
        success(f"Registered external collection '{result.collection_id}'")
        info_output(f"  href: {result.href}")
        info_output(f"  type: {result.media_type} (external, not managed)")
        if result.via_url:
            info_output(f"  via:  {result.via_url}")


def _resolve_rm_catalog_root(
    catalog_path: Path | None,
    use_json: bool,
) -> Path:
    """Resolve the catalog root for rm, or exit with an error."""
    if catalog_path is not None:
        return catalog_path.resolve()

    detected_root = find_catalog_root()
    if detected_root is None:
        emit_error(
            "rm",
            "NotACatalogError",
            "Not inside a Portolan catalog (no .portolan/config.yaml found)",
            use_json=use_json,
        )
        if not use_json:
            detail("Run 'portolan init' to create a catalog, or cd into one")
        raise SystemExit(1)
    return detected_root


def _render_rm_result(
    path: Path,
    removed: list[Path],
    skipped: list[Path],
    keep: bool,
    dry_run: bool,
    verbose: bool,
) -> None:
    """Print the styled rm result to the terminal."""
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

    # Nothing matched a tracked asset or a file on disk. Report the
    # no-op so a typo does not look like a success (issue #827).
    if not removed:
        if path.exists():
            warn(f"Nothing to remove: {path.name} is not tracked")
        else:
            warn(f"Nothing to remove: {path.name} is not tracked and not on disk")


@cli.command("rm")
@click.argument("path", type=click.Path(exists=False, path_type=Path))
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
    r"""Remove files from tracking.

    By default, removes the file from disk AND untracks it from the catalog.
    Requires --force for destructive operations (deleting files).

    Works like git: run from anywhere inside a catalog and it auto-detects
    the catalog root. Use --portolan-dir to override.

    A file that is already gone from disk is still untracked, so you can drop a
    tracked asset that you deleted by hand. The removal is recorded as a new
    version.

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
    from portolan_cli.remove import SafetyError, remove_path, require_force

    use_json = should_output_json(ctx, json_output)

    # Refuse a destructive rm before the catalog root lookup, as before.
    try:
        require_force(keep=keep, dry_run=dry_run, force=force)
    except SafetyError as err:
        emit_error("rm", "SafetyError", str(err), use_json=use_json)
        if not use_json:
            detail("Examples:")
            detail("  portolan rm --keep myfile.parquet  # Untrack but keep file")
            detail("  portolan rm --dry-run myfile.parquet  # Preview removal")
            detail("  portolan rm -f myfile.parquet  # Force delete")
        raise SystemExit(1) from err

    # Auto-detect catalog root (git-style)
    catalog_root = _resolve_rm_catalog_root(catalog_path, use_json)

    from portolan_cli.extract.common.cli_helpers import print_message
    from portolan_cli.report import reporting

    try:
        # The catalog index update reports its warnings through this scope.
        with reporting(print_message):
            result = remove_path(
                path, catalog_root=catalog_root, keep=keep, dry_run=dry_run, force=force
            )
        if not emit_success("rm", result.to_dict(), use_json=use_json):
            _render_rm_result(path, result.removed, result.skipped, keep, dry_run, verbose)

    except ValueError as err:
        emit_error("rm", "ValueError", str(err), use_json=use_json)
        raise SystemExit(1) from err
    except FileNotFoundError as err:
        emit_error("rm", "FileNotFoundError", str(err), use_json=use_json)
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

    emit_error("push", "NotImplementedError", result.message, use_json=use_json)
    raise SystemExit(1)


def _resolve_push_settings(
    destination: str | None,
    profile: str | None,
    catalog_path: Path,
    collection: str | None,
    use_json: bool,
    command: str,
) -> tuple[str | None, str | None, str | None]:
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
    from portolan_core.config import resolve_push_settings

    try:
        return resolve_push_settings(destination, profile, catalog_path, collection)
    except ValueError as e:
        emit_error(command, "ConfigError", str(e), use_json=use_json)
        raise SystemExit(1) from None


def _warn_push_footprint(
    concurrency: int,
    chunk_concurrency: int,
    max_connections: int | None,
    workers: int | None,
    collection: str | None,
) -> None:
    """Warn when the connection footprint of a push is high (Issue #344)."""
    from portolan_cli.async_utils import MAX_SAFE_CONNECTIONS, calculate_connection_footprint
    from portolan_cli.sync.push import effective_push_concurrency

    effective_file, effective_chunk = effective_push_concurrency(
        concurrency, chunk_concurrency, max_connections
    )
    eff_workers = workers if workers is not None else (4 if collection is None else 1)
    footprint = calculate_connection_footprint(effective_file, effective_chunk, eff_workers)
    if footprint > MAX_SAFE_CONNECTIONS:
        worker_part = f"{eff_workers} workers × " if eff_workers > 1 else ""
        warn(
            f"High connection count: {footprint} concurrent connections "
            f"({worker_part}{effective_file} files × {effective_chunk} chunks). "
            "This may overwhelm home networks. Consider using --max-connections to limit."
        )


def _report_push_error(err: Exception, *, collection: str | None, use_json: bool) -> bool:
    """Report an error that a push raised.

    A catalog-wide push reports every error by its class name. A push of one
    collection reports its known errors, and it names any other ValueError
    by the base class.

    Returns:
        False when the caller must re-raise the error.
    """
    from portolan_cli.sync.push import PushConflictError
    from portolan_core.errors import ProfileCredentialsError, PushVerifyError

    if isinstance(err, PushConflictError):
        if use_json:
            envelope = error_envelope(
                "push",
                [ErrorDetail(type="PushConflictError", message=str(err))],
            )
            output_json_envelope(envelope)
        else:
            error(f"Push conflict: {err}")
            info_output("Use --force to overwrite, or pull remote changes first")
        return True
    known = (ProfileCredentialsError, PushVerifyError, FileNotFoundError, NotImplementedError)
    if collection is not None and not isinstance(err, known):
        if not isinstance(err, ValueError):
            return False
        emit_error("push", "ValueError", str(err), use_json=use_json)
        return True
    code = getattr(err, "code", None)
    emit_error("push", type(err).__name__, str(err), use_json=use_json, code=code)
    return True


def _report_push_all_result(all_result: Any, *, use_json: bool) -> None:
    """Report the outcome of a push that covered every collection.

    The report lines of the run already wrote the terminal output, so this adds
    the JSON envelope and the exit code.

    Args:
        all_result: PushAllResult from push_all_collections.
        use_json: If True, emit a JSON envelope.

    Raises:
        SystemExit: If any collection failed.
    """
    if use_json:
        data = all_result.to_dict()
        # A failed push returns success=false, with one error per message.
        errors: list[ErrorDetail] = []
        for name, messages in all_result.collection_errors.items():
            exc = all_result.collection_exceptions.get(name)
            errors.extend(
                ErrorDetail(
                    type=type(exc).__name__ if exc is not None else "PushError",
                    message=f"{name}: {message}",
                    code=getattr(exc, "code", None),
                )
                for message in messages
            )
        envelope = (
            success_envelope("push", data)
            if all_result.success
            else error_envelope(
                "push", errors or [ErrorDetail("PushError", "Push failed")], data=data
            )
        )
        output_json_envelope(envelope)

    if not all_result.success:
        raise SystemExit(1)


def _report_push_result(result: Any, *, dry_run: bool, use_json: bool) -> None:
    """Report the outcome of a single-collection push.

    Args:
        result: PushResult from run_push.
        dry_run: If True, the run wrote nothing.
        use_json: If True, emit a JSON envelope.

    Raises:
        SystemExit: If the push failed.
    """
    if not result.success:
        # Route a returned failure through the error path, so JSON mode reports
        # success=false and exits non-zero. It never emits a success envelope.
        err_msgs = result.errors or ["Push failed"]
        if use_json:
            envelope = error_envelope(
                "push",
                [ErrorDetail(type="PushError", message=msg) for msg in err_msgs],
                data=result.to_dict(),
            )
            output_json_envelope(envelope)
        else:
            for err_msg in err_msgs:
                error(err_msg)
        raise SystemExit(1)

    if emit_success("push", result.to_dict(), use_json=use_json):
        return

    if result.versions_pushed > 0:
        success(f"Pushed {result.versions_pushed} version(s), {result.files_uploaded} file(s)")
    elif dry_run:
        # push.py already printed what a real run would do.
        info_output("[DRY RUN] Complete - no files were uploaded")
    elif result.files_uploaded == 0:
        info_output("Nothing to push - local and remote are in sync")
    # A metadata-only sync uploads files without a version bump. push_async
    # already reported "Synced N metadata file(s)" for it (#816).


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
@click.option(
    "--verify",
    is_flag=True,
    help="Send a HEAD for each asset that push skips as unchanged. Upload the asset "
    "again when the object is missing or its size differs from the manifest. "
    "Works with no new version. A size check cannot find a wrong object of the "
    "same size. Without this flag, push sends no request for a skipped asset.",
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
    verify: bool,
) -> None:
    r"""Push local catalog changes to cloud object storage.

    Git-style behavior: automatically finds the catalog root by walking up
    from the current directory. Works from any subdirectory within a catalog.
    Use --catalog to override and specify an explicit path.

    Syncs collection(s) to a remote destination (S3, GCS, Azure).
    Uses optimistic locking to detect concurrent modifications.

    DESTINATION is the object store URL (e.g., s3://mybucket/my-catalog).
    If not provided, uses 'remote' from PORTOLAN_REMOTE env var or .env file.

    If --collection is specified, pushes that collection only.
    If --collection is omitted, pushes all collections in the catalog.

    Push skips an asset when the remote manifest records the same sha256.
    Push does not read the object for that decision. Use --verify to check
    each skipped asset in the store. Push then sends one HEAD per skipped
    asset. It uploads the asset again when the object is missing or its size
    differs from the manifest. This works with no new version, and it adds
    no version. With no new version and no --force, push uploads only those
    assets and keeps the remote versions.json as is. The check compares
    sizes only. It cannot find a wrong object of the same size.

    On Source Cooperative, use the repository as the bucket and put the
    account in the endpoint. With the account as the bucket, push cannot
    delete its backup objects, and the push fails.

    \b
    Examples:
        # Push to Source Cooperative
        export AWS_ENDPOINT_URL_S3=https://data.source.coop/my-account
        portolan push s3://my-repository/catalog --profile source-coop

    \b
        # Push a single collection
        portolan push s3://mybucket/catalog --collection demographics
        portolan push gs://mybucket/catalog -c imagery --dry-run

    \b
        # Push all collections
        portolan push s3://mybucket/catalog
        portolan push --dry-run  # Uses configured remote

    \b
        # Repair objects that disagree with the manifest
        portolan push s3://mybucket/catalog -c demographics --verify
    """
    from portolan_cli.sync.push import PushAllResult, run_push
    from portolan_core.errors import ProfileCredentialsError

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
        emit_error(
            "push",
            "UsageError",
            "No destination provided and no 'remote' configured. Provide a DESTINATION argument or set PORTOLAN_REMOTE env var (or add to .env)",
            use_json=use_json,
        )
        raise SystemExit(1)

    # Warn when no credential source answers. The push still runs, because a
    # plaintext endpoint accepts an unsigned upload. A dry run reports it too,
    # because a dry run exists to show what the real push meets.
    # `--json` carries a machine-readable envelope, so the hint stays out of it.
    if not use_json:
        from portolan_cli.sync.upload import check_credentials

        # A profile whose credential source fails stops the push. A warning
        # would let the upload meet the same failure later.
        try:
            credentials_ok, credential_hint = check_credentials(
                resolved_destination, resolved_profile
            )
        except ProfileCredentialsError as err:
            emit_error("push", type(err).__name__, str(err), use_json=use_json, code=err.code)
            raise SystemExit(1) from err
        if not credentials_ok:
            warn(credential_hint)

    if not use_json:
        _warn_push_footprint(concurrency, chunk_concurrency, max_connections, workers, collection)

    try:
        result = run_push(
            catalog_path,
            resolved_destination,
            collection=collection,
            force=force,
            dry_run=dry_run,
            profile=resolved_profile,
            region=resolved_region,
            workers=workers,
            concurrency=concurrency,
            chunk_concurrency=chunk_concurrency,
            max_connections=max_connections,
            adaptive=adaptive,
            verify=verify,
            # As before #1133, only a catalog-wide push prints the per-file lines of -v.
            verbose=verbose and collection is None,
            report=_report,
            # As before #1133, the bar draws on a terminal unless the output is JSON.
            show_progress=not use_json,
        )
    except Exception as err:
        if not _report_push_error(err, collection=collection, use_json=use_json):
            raise
        raise SystemExit(1) from err

    if isinstance(result, PushAllResult):
        _report_push_all_result(result, use_json=use_json)
    else:
        _report_push_result(result, dry_run=dry_run, use_json=use_json)


# ─────────────────────────────────────────────────────────────────────────────
# Pull command
# ─────────────────────────────────────────────────────────────────────────────


def _report(level: str, message: str) -> None:
    """Print one progress line of the sync library through output.py."""
    from portolan_cli import output

    getattr(output, level)(message)


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
    elif result.uncommitted_changes:
        error("Pull blocked by uncommitted changes:")
        for filename in result.uncommitted_changes:
            detail(f"  {filename}")
        warn("Use --force to discard local changes")
    else:
        error("Pull failed")


def _single_pull_errors(result: PullResult) -> list[ErrorDetail]:
    """Return the envelope errors of a failed pull of one collection."""
    if result.uncommitted_changes:
        return [
            ErrorDetail(
                type="UncommittedChangesError",
                message=f"Uncommitted changes: {', '.join(result.uncommitted_changes)}",
            )
        ]
    return [ErrorDetail(type="PullError", message="Pull failed")]


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
    r"""Pull updates from a remote catalog.

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
    from portolan_cli.sync.pull import BackendPull, PullAllResult, pull_catalog

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
        emit_error("pull", "ConfigError", str(e), use_json=use_json)
        raise SystemExit(1) from None

    # A catalog-wide pull reports any error. A one-collection pull reports
    # these two and lets the others propagate.
    caught: tuple[type[Exception], ...] = (
        (Exception,) if collection is None else (FileNotFoundError, ValueError)
    )
    try:
        result = pull_catalog(
            remote_url,
            catalog_path,
            collection,
            force=force,
            dry_run=dry_run,
            restore=restore,
            profile=resolved_profile,
            workers=workers,
            concurrency=concurrency,
            report=_report,
            show_progress=True,  # As before #1133: the bar draws on a terminal in every output mode.
        )
    except caught as err:
        error_type = type(err).__name__
        if collection is not None:
            error_type = "FileNotFoundError" if isinstance(err, FileNotFoundError) else "ValueError"
        emit_error("pull", error_type, str(err), use_json=use_json)
        raise SystemExit(1) from err

    outcome = result.result if isinstance(result, BackendPull) else result
    if outcome.success:
        emit_success("pull", result.to_dict(), use_json=use_json)
    elif use_json:
        if isinstance(outcome, PullAllResult):
            errors = [
                ErrorDetail(type="PullError", message=f"{coll}: {', '.join(errs)}")
                for coll, errs in outcome.collection_errors.items()
            ]
        else:
            errors = _single_pull_errors(outcome)
        output_json_envelope(error_envelope("pull", errors, data=result.to_dict()))

    if not use_json and not isinstance(outcome, PullAllResult):
        _output_pull_human(outcome, dry_run=dry_run)

    if not outcome.success:
        raise SystemExit(1)


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
    r"""Sync local catalog with remote storage (pull + push).

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
    from portolan_cli.sync.core import sync as sync_fn
    from portolan_core.errors import ProfileCredentialsError

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
        emit_error(
            "sync",
            "UsageError",
            "No destination provided and no 'remote' configured. Provide a DESTINATION argument or set PORTOLAN_REMOTE env var (or add to .env)",
            use_json=use_json,
        )
        raise SystemExit(1)

    try:
        result = sync_fn(
            catalog_root=catalog_path,
            collection=collection,
            destination=resolved_destination,
            force=force,
            dry_run=dry_run,
            fix=fix,
            profile=resolved_profile,
            region=resolved_region,
            report=_report,
            show_progress=True,  # As before #1133: the bar draws on a terminal in every output mode.
        )
    except ProfileCredentialsError as err:
        emit_error("sync", type(err).__name__, str(err), use_json=use_json, code=err.code)
        raise SystemExit(1) from err

    if use_json:
        if result.success:
            envelope = success_envelope("sync", result.to_dict())
        else:
            errors = [ErrorDetail(type="SyncError", message=err_msg) for err_msg in result.errors]
            envelope = error_envelope("sync", errors, data=result.to_dict())

        output_json_envelope(envelope)
    # sync() reported each step through _report.
    # Just handle the final status
    elif result.success:
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
    r"""Clone a remote catalog to a local directory.

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
        portolan clone s3://mybucket/my-catalog.

        # Clone specific collection
        portolan clone s3://mybucket/catalog -c demographics

        # Clone all collections to specific directory
        portolan clone s3://mybucket/catalog ./local-copy

        # Clone specific collection with profile
        portolan clone s3://mybucket/catalog ./data -c imagery --profile prod
    """
    from portolan_cli.sync.transport import clone as clone_fn
    from portolan_cli.sync.transport import infer_local_path_from_url

    use_json = should_output_json(ctx, json_output)

    # A clone has no catalog root yet, so load a caller-supplied .env file.
    from dotenv import load_dotenv

    load_dotenv(override=False)

    # Resolve profile: CLI arg > env var > default (no local catalog yet)
    resolved_profile = resolve_aws_profile(profile, catalog_path=None)

    # Infer local_path from URL if not provided
    if local_path is None:
        try:
            local_path = infer_local_path_from_url(remote_url)
            if not use_json:
                info_output(f"Inferred local path: {local_path}")
        except ValueError as e:
            emit_error("clone", "CloneError", str(e), use_json=use_json, code="INVALID_URL")
            raise SystemExit(1) from None

    result = clone_fn(
        remote_url=remote_url,
        local_path=local_path,
        collection=collection,
        profile=resolved_profile,
        report=_report,
        show_progress=True,  # As before #1133: the bar draws on a terminal in every output mode.
    )

    if use_json:
        if result.success:
            envelope = success_envelope("clone", result.to_dict())
        else:
            errors = [
                ErrorDetail(type="CloneError", message=err, code="CLONE_FAILED")
                for err in result.errors
            ]
            envelope = error_envelope("clone", errors)

        output_json_envelope(envelope)
    elif result.success:
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
    r"""Manage catalog configuration.

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


# A negative number such as -1 is a VALUE. Click reads an unknown token that
# starts with "-" as an option and fails, so the command passes it through.
@config.command("set", context_settings={"ignore_unknown_options": True})
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
    r"""Set a configuration value.

    KEY is the setting name (e.g., backend, statistics.enabled). A dotted KEY
    names a nested value: conversion.vector.sort writes sort under the
    conversion and vector blocks. An unknown KEY fails and lists the known keys.
    VALUE is the value to set. It takes the type of the setting, so "false" is
    stored as a boolean.

    Note: Sensitive settings (remote, profile, region) cannot be stored in
    config.yaml. Use environment variables or .env files instead.

    \b
    Examples:
        portolan config set backend iceberg
        portolan config set statistics.enabled true
        portolan config set conversion.vector.sort none
        portolan config set pmtiles.enabled false --collection demographics
    """
    from portolan_core.config import store_setting

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
        result = store_setting(catalog_path, key, value, collection)
    except (ConfigError, OSError, ValueError) as e:
        if use_json:
            code = e.code if isinstance(e, ConfigError) else None
            envelope = error_envelope(
                "config set",
                [ErrorDetail(type=type(e).__name__, message=str(e), code=code)],
            )
            output_json_envelope(envelope)
        else:
            error(f"Failed to set config: {e}")
        raise SystemExit(1) from e

    if not emit_success("config set", result.to_dict(), use_json=use_json):
        if collection:
            success(f"Set {key}={value} for collection '{collection}'")
        else:
            success(f"Set {key}={value}")


def _config_catalog_root(command: str, *, use_json: bool) -> Path:
    """Return the catalog root of a config read command, or exit 1 outside a catalog."""
    catalog_path = find_catalog_root()
    if catalog_path is None:
        if use_json:
            envelope = error_envelope(
                command,
                [ErrorDetail(type="CatalogNotFoundError", message="Not in a Portolan catalog")],
            )
            output_json_envelope(envelope)
        else:
            error("Not in a Portolan catalog")
            info_output("Run 'portolan init' to create one")
        raise SystemExit(1)
    return catalog_path


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
    r"""Get a configuration value.

    Shows the resolved value and its source (env, catalog, collection, or not set).

    KEY is the setting name (e.g., remote, aws_profile).

    \b
    Examples:
        portolan config get remote
        portolan config get aws_profile --collection restricted
    """
    from portolan_core.config import _get_env_var_name, read_setting
    from portolan_core.run_state import RunState

    use_json = should_output_json(ctx, json_output)

    catalog_path = _config_catalog_root("config get", use_json=use_json)

    try:
        # Load .env for credential precedence
        state = RunState.open(catalog_path)
        load_dotenv_and_warn_sensitive(catalog_path, state=state)
        result = read_setting(key, catalog_path, collection, state=state)
    except ConfigError as e:
        emit_error("config get", type(e).__name__, str(e), use_json=use_json, code=e.code)
        raise SystemExit(1) from None
    except ValueError as e:
        # A sensitive key in config.yaml. The envelope reports it as a
        # ConfigError with the plain message, as before the move.
        emit_error("config get", "ConfigError", str(e), use_json=use_json)
        raise SystemExit(1) from None

    if not emit_success("config get", result.to_dict(), use_json=use_json):
        if result.value is not None:
            success(f"{key}={result.value} (from {result.source})")
        else:
            info_output(f"{key} is not set")
            detail(f"  Set via: portolan config set {key} <value>")
            detail(f"  Or set:  {_get_env_var_name(key)}=<value>")


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
    r"""List all configuration settings.

    Shows all settings with their values and sources.

    \b
    Examples:
        portolan config list
        portolan config list --collection demographics
    """
    from portolan_core.config import read_settings
    from portolan_core.run_state import RunState

    use_json = should_output_json(ctx, json_output)

    catalog_path = _config_catalog_root("config list", use_json=use_json)

    try:
        # Load .env for credential precedence
        state = RunState.open(catalog_path)
        load_dotenv_and_warn_sensitive(catalog_path, state=state)
        result = read_settings(catalog_path, collection, state=state)
    except ConfigError as e:
        emit_error("config list", type(e).__name__, str(e), use_json=use_json, code=e.code)
        raise SystemExit(1) from None

    settings = result.settings
    if not emit_success("config list", result.to_dict(), use_json=use_json):
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
    r"""Remove a configuration value.

    Removes the setting from the config file. Does not affect environment variables.

    KEY is the setting name to remove.

    \b
    Examples:
        portolan config unset remote
        portolan config unset aws_profile --collection restricted
    """
    from portolan_core.config import clear_setting

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

    try:
        result = clear_setting(catalog_path, key, collection)
    except ConfigError as e:
        emit_error("config unset", type(e).__name__, str(e), use_json=use_json, code=e.code)
        raise SystemExit(1) from None

    if not emit_success("config unset", result.to_dict(), use_json=use_json):
        if result.removed:
            if collection:
                success(f"Removed {key} from collection '{collection}' config")
            else:
                success(f"Removed {key} from config")
        elif collection:
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
    r"""Remove all Portolan metadata while preserving data files.

    Removes catalog.json, collection.json, item.json (STAC metadata),
    versions.json, and the .portolan/ directory. Preserves all data files
    (.parquet, .tif, .gpkg, .geojson, etc.).

    Use --dry-run to preview what would be removed without deleting anything.

    \b
    Examples:
        portolan clean           # Remove all metadata
        portolan clean --dry-run # Preview what would be removed
    """
    from portolan_cli.clean import clean_metadata

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
        result = clean_metadata(catalog_path, dry_run=dry_run)
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

    if emit_success("clean", result.to_dict(), use_json=use_json):
        return
    if dry_run:
        _print_clean_preview(
            catalog_path, result.files, result.directories, result.data_files_preserved
        )
    elif result.files or result.directories:
        success(f"Removed {len(result.files)} files, {len(result.directories)} directories")
        detail(f"  Data files preserved: {result.data_files_preserved}")
    else:
        info_output("Nothing to clean - no metadata files found")


# ─────────────────────────────────────────────────────────────────────────────
# Metadata Commands
# ─────────────────────────────────────────────────────────────────────────────


@cli.group()
def metadata() -> None:
    r"""Manage catalog metadata for README generation.

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
    "--no-recursive",
    is_flag=True,
    default=False,
    help="Only create template at the specified path (skip subdirectories).",
)
@click.pass_context
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
def metadata_init(
    ctx: click.Context,
    json_output: bool,
    path: str | None,
    force: bool,
    no_recursive: bool,
) -> None:
    r"""Generate a metadata.yaml template.

    Creates .portolan/metadata.yaml files at all STAC levels (catalogs,
    subcatalogs, collections) by default. Skips items (item.json directories)
    and preserves existing files unless --force is used.

    If PATH is provided, starts from that directory. Otherwise, starts at the
    catalog root.

    \b
    Examples:
        portolan metadata init                    # All levels in catalog
        portolan metadata init climate            # All levels under climate/
        portolan metadata init --force            # Overwrite existing
        portolan metadata init --no-recursive     # Only at catalog root
        portolan metadata init demographics --no-recursive  # Only for collection
    """
    from portolan_cli.metadata.yaml_tree import init_metadata_templates
    from portolan_core.errors import PathNotFoundError

    use_json = should_output_json(ctx, json_output)
    catalog_path = _metadata_catalog_root("metadata init", use_json)
    try:
        result = init_metadata_templates(
            catalog_path, path, force=force, recursive=not no_recursive
        )
    except FileExistsError as err:
        if use_json:
            emit_error("metadata init", type(err).__name__, str(err), use_json=True)
        else:
            warn(str(err))
        raise SystemExit(1) from err
    except (InputValidationError, PathNotFoundError, NotADirectoryError, PermissionError) as err:
        _fail_walk("metadata init", err, use_json)

    if not emit_success("metadata init", result.to_dict(), use_json=use_json):
        _print_metadata_init(result)


@metadata.command("validate")
@click.argument("path", required=False, type=click.Path())
@click.option(
    "--no-recursive",
    is_flag=True,
    default=False,
    help="Only validate at the specified path (skip subdirectories).",
)
@click.pass_context
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
def metadata_validate(
    ctx: click.Context,
    json_output: bool,
    path: str | None,
    no_recursive: bool,
) -> None:
    r"""Validate metadata.yaml against schema.

    Validates all .portolan/metadata.yaml files in the catalog tree by default.
    Checks for:
    - Required fields: contact (name + email), license
    - Format validation: email, SPDX license identifier, DOI

    Uses hierarchical resolution: child metadata.yaml files inherit from
    parent levels and override specific fields.

    \b
    Examples:
        portolan metadata validate                    # Validate all levels
        portolan metadata validate climate            # Validate under climate/
        portolan metadata validate --no-recursive     # Only at catalog root
        portolan metadata validate demographics --no-recursive  # Only for collection
    """
    from portolan_cli.metadata.yaml_tree import validate_metadata_files
    from portolan_core.errors import ConfigInvalidStructureError, PathNotFoundError

    use_json = should_output_json(ctx, json_output)
    catalog_path = _metadata_catalog_root("metadata validate", use_json)
    try:
        result = validate_metadata_files(catalog_path, path, recursive=not no_recursive)
    except ConfigInvalidStructureError as err:
        _fail_invalid_yaml("metadata validate", err, use_json)
    except (InputValidationError, PathNotFoundError, NotADirectoryError, PermissionError) as err:
        _fail_walk("metadata validate", err, use_json)

    if use_json:
        _emit_metadata_validate(result)
    else:
        _print_metadata_validate(result)
    if not result.valid:
        raise SystemExit(1)


# ─────────────────────────────────────────────────────────────────────────────
# README Command
# ─────────────────────────────────────────────────────────────────────────────


if TYPE_CHECKING:
    from portolan_cli.metadata.yaml_tree import MetadataInitResult, MetadataValidateResult
    from portolan_cli.readme import ReadmeRunResult
    from portolan_core.errors import ConfigInvalidStructureError


def _metadata_catalog_root(command: str, use_json: bool) -> Path:
    """Find the catalog root for a metadata or readme command, or exit 1."""
    catalog_path = find_catalog_root()
    if catalog_path is None:
        if use_json:
            envelope = error_envelope(
                command,
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
    return catalog_path


def _fail_walk(command: str, err: Exception, use_json: bool) -> NoReturn:
    """Report an error that stopped a catalog walk, and exit 1."""
    emit_error(command, type(err).__name__, str(err), use_json=use_json)
    raise SystemExit(1) from err


def _fail_invalid_yaml(command: str, err: ConfigInvalidStructureError, use_json: bool) -> NoReturn:
    """Report a metadata.yaml that does not parse, and exit 1."""
    if use_json:
        output_json_envelope(
            error_envelope(command, [ErrorDetail(type="InvalidYAMLError", message=str(err))])
        )
    else:
        error(f"Invalid YAML in metadata.yaml: {err}")
    raise SystemExit(1) from err


def _print_metadata_init(result: MetadataInitResult) -> None:
    """Print what a metadata init run created, skipped, and could not write."""
    if not result.recursive:
        success(f"Created {result.created[0]}")
        info_output("Edit the file to add your catalog's metadata")
        return
    if result.created:
        success(f"Created {len(result.created)} metadata.yaml template(s)")
        for path in result.created:
            detail(f"  {path}")
    if result.skipped:
        info_output(f"Skipped {len(result.skipped)} existing file(s)")
    if result.permission_errors:
        warn(f"Permission denied for {len(result.permission_errors)} location(s)")
        for path in result.permission_errors:
            detail(f"  {path}")
    if not result.created and not result.skipped and not result.permission_errors:
        warn("No catalogs or collections found")


def _emit_metadata_validate(result: MetadataValidateResult) -> None:
    """Emit the JSON envelope of a metadata validate run."""
    if result.valid:
        output_json_envelope(success_envelope("metadata validate", result.to_dict()))
        return
    prefix = "{path}: " if result.recursive else ""
    errors = [
        ErrorDetail(type="ValidationError", message=prefix.format(path=v.path) + e)
        for v in result.validations
        for e in v.errors
    ]
    output_json_envelope(error_envelope("metadata validate", errors, data=result.to_dict()))


def _print_metadata_validate(result: MetadataValidateResult) -> None:
    """Print the outcome of a metadata validate run."""
    if not result.recursive:
        errors = result.validations[0].errors
        if errors:
            error(f"Validation failed with {len(errors)} error(s):")
            for validation_err in errors:
                detail(f"  - {validation_err}")
        else:
            success("Metadata is valid")
        return
    total = len(result.validations)
    invalid = [v for v in result.validations if not v.valid]
    if not invalid:
        if total == 0:
            warn("No metadata.yaml files found to validate")
        else:
            success(f"Validated {total} metadata.yaml file(s) - all valid")
        return
    error(f"Validation failed: {len(invalid)}/{total} file(s) invalid")
    for v in invalid:
        detail(f"  {v.path}:")
        for e in v.errors:
            detail(f"    - {e}")


def _print_readme(result: ReadmeRunResult) -> None:
    """Print what a readme run wrote or found stale."""
    if not result.recursive:
        if not result.check:
            success(f"Generated {result.fresh[0]}")
        elif result.fresh:
            success("README is up-to-date")
        else:
            error("README is stale or missing")
            info_output("Run 'portolan readme' to regenerate")
        return
    if not result.check:
        success(f"Generated {len(result.fresh)} README(s)")
        for path in result.fresh:
            detail(f"  {path}")
        return
    if result.stale:
        error(f"{len(result.stale)} README(s) are stale:")
        for path in result.stale:
            detail(f"  {path}")
        info_output("Run 'portolan readme' to regenerate")
        return
    success(f"All {len(result.fresh)} README(s) are up-to-date")


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
    "--no-recursive",
    is_flag=True,
    default=False,
    help="Only generate README at the specified path (skip subdirectories).",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Show detailed output. For log messages, use the root flag: portolan -v readme.",
)
@click.pass_context
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
def readme(
    ctx: click.Context,
    json_output: bool,
    path: str | None,
    stdout: bool,
    check: bool,
    no_recursive: bool,
    verbose: bool,
) -> None:
    r"""Generate README.md from STAC metadata and metadata.yaml.

    Generates READMEs for the catalog and all collections by default. The README
    is a pure output - always generated from STAC (machine-extracted metadata)
    plus .portolan/metadata.yaml (human enrichment). Never hand-edit the README;
    edit metadata.yaml instead and regenerate.

    Use --check in CI to verify READMEs are up-to-date:

    \b
    Examples:
        portolan readme                        # Generate for catalog and all collections
        portolan readme climate                # Generate under climate/
        portolan readme --check                # CI mode: exit 1 if any stale
        portolan readme --no-recursive         # Only at catalog root
        portolan readme demographics --no-recursive  # Only for collection
        portolan readme --stdout --no-recursive      # Print single README
    """
    from portolan_cli.readme import UnsupportedOptionError, generate_readmes
    from portolan_core.errors import ConfigInvalidStructureError, PathNotFoundError

    use_json = should_output_json(ctx, json_output)
    catalog_path = _metadata_catalog_root("readme", use_json)
    try:
        result = generate_readmes(
            catalog_path,
            path,
            check=check,
            stdout=stdout,
            recursive=not no_recursive,
            on_progress=info_output if verbose and not use_json else None,
        )
    except ConfigInvalidStructureError as err:
        _fail_invalid_yaml("readme", err, use_json)
    except (
        UnsupportedOptionError,
        InputValidationError,
        PathNotFoundError,
        NotADirectoryError,
    ) as err:
        _fail_walk("readme", err, use_json)

    if result.content is not None:
        click.echo(result.content)
    elif not emit_success("readme", result.to_dict(), use_json=use_json):
        _print_readme(result)
    if result.stale:
        raise SystemExit(1)


@cli.command("logo")
@click.argument("source", type=str)
@click.option(
    "--title",
    type=str,
    default=None,
    help="Accessible label for the logo. Defaults to the catalog title.",
)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.pass_context
def logo_cmd(
    ctx: click.Context,
    source: str,
    title: str | None,
    json_output: bool,
) -> None:
    r"""Publish SOURCE as the catalog logo.

    Copies the image to _assets/ beside the root catalog.json and writes a
    rel="icon" link pointing at it. A registry lists many catalogs side by side,
    and the logo is what makes yours recognizable.

    SOURCE must be a local file in one of the seven permitted formats: APNG,
    AVIF, GIF, JPEG, PNG, SVG, or WebP. A URL is rejected — download the image
    first. SVG conforms but STAC Browser will not render it.

    Re-run to replace the logo: the previous link and image are removed, so the
    catalog always carries exactly one.

    \b
    Examples:
      portolan logo brand.png                     # Title from the catalog
      portolan logo brand.png --title "Acme GIS"  # Explicit title
    """
    from portolan_cli.logo import set_catalog_logo
    from portolan_core.errors import LogoError

    use_json = should_output_json(ctx, json_output)
    catalog_root = require_catalog_root(use_json, "logo")

    try:
        result = set_catalog_logo(catalog_root, source, title=title)
    except LogoError as err:
        emit_error("logo", type(err).__name__, str(err), use_json=use_json, code=err.code)
        raise SystemExit(1) from err
    except OSError as err:
        emit_error("logo", "OSError", f"Cannot write catalog logo: {err}", use_json=use_json)
        raise SystemExit(1) from err

    data = result.to_dict()
    if not emit_success("logo", data, use_json=use_json):
        success(f"Logo published at {data['path']}")
        info_output(f'Linked as rel="icon" ({result.media_type}), titled "{result.title}"')
        for message in result.warnings:
            warn(message)


@cli.group(cls=ExtractGroup)
def extract() -> None:
    r"""Extract data from external sources into Portolan catalogs.

    Convert data from ArcGIS services, APIs, or other sources into
    well-structured Portolan catalogs with STAC metadata.

    \b
    Examples:
        portolan extract arcgis https://services.arcgis.com/.../FeatureServer ./output
        portolan extract arcgis URL --layers "Census*" --dry-run
        portolan extract arcgis URL --filter "sdn_*" --resume
    """


@cli.command()
@click.argument(
    "input_file",
    type=click.Path(exists=True, path_type=Path),
)
@click.argument(
    "output_dir",
    type=click.Path(path_type=Path),
    required=False,
)
@click.option(
    "--strategy",
    type=click.Choice(["kdtree"]),
    default="kdtree",
    help="Spatial partitioning strategy. Default: kdtree (data-driven, auto-balancing).",
)
@click.option(
    "--target-rows",
    type=int,
    default=120_000,
    help="Target rows per partition. Default: 120,000.",
)
@click.option(
    "--preview",
    is_flag=True,
    help="Analyze and preview partition strategy without creating files.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Show detailed output.",
)
@click.pass_context
def partition(
    ctx: click.Context,
    input_file: Path,
    output_dir: Path | None,
    strategy: str,
    target_rows: int,
    preview: bool,
    verbose: bool,
) -> None:
    r"""Partition a large GeoParquet file for better query performance.

    Splits a GeoParquet file into spatially-organized partitions using
    geoparquet-io. Per OGC best practices, files over 2GB should be partitioned.

    \b
    Output structure (Hive-style):
        output_dir/
        ├── kdtree_cell=001/
        │   └── data.parquet
        ├── kdtree_cell=002/
        │   └── data.parquet
        └── ...

    \b
    Examples:
        # Preview partition strategy
        portolan partition buildings.parquet --preview

        # Partition with default settings (kdtree, 120k rows/partition)
        portolan partition buildings.parquet output/

        # Custom target rows
        portolan partition buildings.parquet output/ --target-rows 50000
    """
    from portolan_cli.partitioning import PartitionPreview, partition_file
    from portolan_core.errors import PartitionError

    use_json = should_output_json(ctx)

    # Check if file is GeoParquet
    if input_file.suffix.lower() != ".parquet":
        emit_error("partition", "FormatError", "Input must be a .parquet file", use_json=use_json)
        raise SystemExit(1)

    # Require output_dir for actual partitioning
    if output_dir is None and not preview:
        emit_error(
            "partition",
            "UsageError",
            "OUTPUT_DIR required (use --preview for analysis only)",
            use_json=use_json,
        )
        raise SystemExit(1)

    if not use_json and not preview:
        info_output(f"Partitioning {input_file.name} with {strategy} strategy...")

    try:
        result = partition_file(
            input_file,
            output_dir,
            strategy=strategy,
            target_rows=target_rows,
            preview=preview,
            verbose=verbose,
        )
    except PartitionError as e:
        if use_json:
            # The envelope has never carried a code for this error, so it stays without one.
            emit_error("partition", "PartitionError", e.message, use_json=True)
        else:
            error(f"Partitioning failed: {e.message}")
        raise SystemExit(1) from None

    if emit_success("partition", result.to_dict(), use_json=use_json):
        return
    if isinstance(result, PartitionPreview):
        info_output(f"File: {result.file}")
        info_output(f"Size: {result.size_gb:.2f} GB")
        if result.recommended:
            success("Partitioning recommended (> 2GB threshold)")
        else:
            info_output("File is under 2GB threshold - partitioning optional")
        info_output(f"Strategy: {strategy}")
        info_output(f"Target rows per partition: {target_rows:,}")
        return
    success(f"Created {len(result.files)} partitions in {output_dir}")
    if verbose:
        for pf in result.files:
            detail(f"  {pf.parent.name}/{pf.name}")


@cli.group()
def version() -> None:
    r"""Version management commands.

    Works with any versioning backend (file, iceberg). Backend is auto-detected
    from catalog configuration.

    \b
    Subcommands:
        current   Show current version of a collection
        list      List all versions of a collection
        rollback  Rollback to a previous version (iceberg only)
        prune     Remove old versions (iceberg only)
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
    r"""Show the current version of a collection.

    Works with any versioning backend (auto-detected from config).

    \b
    Examples:
        portolan version current boundaries
        portolan version current boundaries --json
    """
    from portolan_cli.version_ops import describe_current_version

    use_json = should_output_json(ctx, json_output)

    if catalog_path is None:
        catalog_path = require_catalog_root(use_json, "version current")

    try:
        result = describe_current_version(collection, catalog_root=catalog_path)
    # A plugin backend can raise any exception, and the envelope reports each one.
    except Exception as e:
        emit_error("version current", type(e).__name__, str(e), use_json=use_json)
        raise SystemExit(1) from None

    if not emit_success("version current", result.to_dict(), use_json=use_json):
        ver = result.version
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
    r"""List all versions of a collection.

    Works with any versioning backend (auto-detected from config).

    \b
    Examples:
        portolan version list boundaries
        portolan version list boundaries --json
    """
    from portolan_cli.version_ops import describe_version_history

    use_json = should_output_json(ctx, json_output)

    if catalog_path is None:
        catalog_path = require_catalog_root(use_json, "version list")

    try:
        result = describe_version_history(collection, catalog_root=catalog_path)
    # A plugin backend can raise any exception, and the envelope reports each one.
    except Exception as e:
        emit_error("version list", type(e).__name__, str(e), use_json=use_json)
        raise SystemExit(1) from None

    if not emit_success("version list", result.to_dict(), use_json=use_json):
        versions = result.versions
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


def _bump_show_changes(
    collection: str,
    new_version: str,
    current_version: str | None,
    modified: list[str],
    deleted: list[str],
) -> bool:
    """Show changes and get confirmation for version bump."""
    info_output(f"Creating version {new_version} for '{collection}'")
    info_output(f"  Current version: {current_version}")
    info_output("")

    if modified:
        info_output("Modified files:")
        for f in modified:
            warn(f"  {f}")

    if deleted:
        info_output("Deleted files:")
        for f in deleted:
            error(f"  {f}")

    info_output("")
    return click.confirm("Create this version?")


def _bump_error(use_json: bool, error_type: str, message: str) -> None:
    """Output a version bump error and exit."""
    emit_error("version bump", error_type, message, use_json=use_json)
    raise SystemExit(1)


@version.command()
@click.argument("collection")
@click.argument("new_version")
@click.option(
    "--notes",
    "-m",
    help="Version notes/message describing the change.",
)
@click.option(
    "--breaking",
    is_flag=True,
    help="Mark this version as having breaking changes.",
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
    default=None,
    help="Path to catalog root (default: auto-detect).",
)
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.pass_context
def bump(
    ctx: click.Context,
    collection: str,
    new_version: str,
    notes: str | None,
    breaking: bool,
    yes: bool,
    catalog_path: Path | None,
    json_output: bool,
) -> None:
    r"""Create a new version from current file state.

    Detects modified files by comparing checksums, computes new checksums,
    and creates a new version entry in versions.json.

    NEW_VERSION must be an explicit semver string (e.g., "1.2.0").

    \b
    Examples:
        portolan version bump demographics 1.4.0 -m "Updated source data"
        portolan version bump demographics 2.0.0 --breaking -m "Schema change"
        portolan version bump demographics 1.4.0 -y  # Skip confirmation
    """
    from portolan_cli.version_ops import bump_collection_version, semver_error

    use_json = should_output_json(ctx, json_output)

    # A bad version string fails before the catalog lookup, as it always has.
    invalid = semver_error(new_version)
    if invalid is not None:
        _bump_error(use_json, "ValidationError", invalid)

    if catalog_path is None:
        catalog_path = require_catalog_root(use_json, "version bump")

    def confirm(plan: Any) -> bool:
        return _bump_show_changes(
            collection, new_version, plan.previous_version, plan.modified, plan.deleted
        )

    result = bump_collection_version(
        collection,
        new_version,
        catalog_root=catalog_path,
        notes=notes,
        breaking=breaking,
        confirm=None if use_json or yes else confirm,
    )
    if result.error is not None:
        _bump_error(use_json, *result.error)
    if result.published is None and result.changed:
        info_output("Aborted")
        return

    if not emit_success("version bump", result.to_dict(), use_json=use_json):
        if result.published is None:
            info_output(f"No changes detected in '{collection}'")
            return
        success(f"Created version {result.published.version}")
        if result.published.message:
            detail(f"  {result.published.message}")


# =============================================================================
# STAC GeoParquet Command
# =============================================================================


def _report_parquet_collection(outcome: Any) -> None:
    """Print the outcome of one collection of stac-geoparquet."""
    if outcome.error is not None:
        error(outcome.error_summary)
        return
    if outcome.dry_run:
        info_output(f"[DRY RUN] Would generate items.parquet for '{outcome.collection}'")
    else:
        success(f"Generated items.parquet for '{outcome.collection}'")
    detail(f"    Items: {outcome.item_count}")
    detail(f"    Output: {outcome.parquet_path}")


def _output_parquet_results(result: Any, is_bulk: bool, dry_run: bool, use_json: bool) -> None:
    """Output results of stac-geoparquet command."""
    results = result.results
    errors_list = result.errors
    had_errors = bool(errors_list)
    total_items = sum(r.item_count for r in results)

    # Per-collection output for a single collection
    if not use_json and not is_bulk:
        for outcome in result.outcomes:
            _report_parquet_collection(outcome)

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
            details = [ErrorDetail(type=t, message=m) for t, m in errors_list]
            output_json_envelope(error_envelope("stac-geoparquet", details))
        else:
            emit_success("stac-geoparquet", result.to_dict(), use_json=True)

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
    r"""Generate items.parquet for efficient STAC queries.

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
    from portolan_cli.stac_parquet import generate_stac_geoparquet

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

    from portolan_cli.extract.common.cli_helpers import print_message
    from portolan_cli.report import reporting

    # The catalog index update reports its warnings through this scope.
    with reporting(print_message):
        result = generate_stac_geoparquet(catalog_path, collection, dry_run=dry_run)
    if result.error is not None:
        emit_error("stac-geoparquet", *result.error, use_json=use_json)
        raise SystemExit(1)

    _output_parquet_results(result, collection is None, dry_run, use_json)


# ─────────────────────────────────────────────────────────────────────────────
# Skills Commands
# ─────────────────────────────────────────────────────────────────────────────


@cli.group()
def skills() -> None:
    """List and view AI skills for Portolan workflows.

    Skills have moved to: https://github.com/portolan-sdi/portolan-skills
    """


@skills.command("list")
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.pass_context
def skills_list_cmd(ctx: click.Context, json_output: bool) -> None:
    """List available skills."""
    from portolan_cli.skills import list_skill_entries

    use_json = should_output_json(ctx, json_output)

    result = list_skill_entries()
    if not emit_success("skills list", result.to_dict(), use_json=use_json):
        click.echo(result.instructions)


@skills.command("show")
@click.argument("name")
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.pass_context
def skills_show_cmd(ctx: click.Context, name: str, json_output: bool) -> None:
    """View a skill."""
    from portolan_cli.skills import show_skill

    use_json = should_output_json(ctx, json_output)

    result = show_skill(name)
    if not emit_success("skills show", result.to_dict(), use_json=use_json):
        click.echo(result.instructions)
