"""Click helpers that the extract commands share (#947).

This module is part of the CLI layer.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn

import click

from portolan_cli.cli import output_json_envelope
from portolan_cli.json_output import ErrorDetail, error_envelope, success_envelope
from portolan_cli.licensing import CLI_LICENSE_REMEDIATION
from portolan_cli.output import info as info_output
from portolan_cli.validation import InputValidationError, validate_catalog_id

if TYPE_CHECKING:
    from collections.abc import Callable

    from portolan_cli.errors import MissingLicenseError
    from portolan_cli.extract.common.progress import ExtractionProgress
    from portolan_cli.extract.common.report import ExtractionReport


def parse_filter_patterns(pattern_str: str | None) -> list[str] | None:
    """Parse comma-separated filter patterns into a list."""
    if not pattern_str:
        return None
    return [p.strip() for p in pattern_str.split(",") if p.strip()]


def build_layer_filters(
    layers: str | None,
    exclude_layers: str | None,
    unified_filter: str | None,
) -> tuple[list[str] | None, list[str] | None]:
    """Build include/exclude filter lists from CLI options."""
    layer_include = parse_filter_patterns(unified_filter)
    explicit_include = parse_filter_patterns(layers)

    if explicit_include:
        if layer_include:
            layer_include.extend(explicit_include)
        else:
            layer_include = explicit_include

    layer_exclude = parse_filter_patterns(exclude_layers)
    return layer_include, layer_exclude


def build_service_filters(
    services: str | None,
    exclude_services: str | None,
    unified_filter: str | None,
    is_services_root: bool,
) -> tuple[list[str] | None, list[str] | None]:
    """Build service include/exclude filter lists from CLI options."""
    service_include = parse_filter_patterns(services)
    service_exclude = parse_filter_patterns(exclude_services)

    # Apply unified filter to services when at services root
    if unified_filter and is_services_root:
        unified_patterns = parse_filter_patterns(unified_filter)
        if unified_patterns:
            if service_include:
                service_include.extend(unified_patterns)
            else:
                service_include = unified_patterns

    return service_include, service_exclude


def validate_collection_name_cli(
    collection_name: str | None,
    json_output: bool,
    url: str,
) -> None:
    """Validate collection_name at CLI layer (fail fast).

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
        output_extract_error(
            json_output,
            "InvalidCollectionNameError",
            f"Invalid collection name: '{collection_name}'. "
            "Collection name must be a single directory name without path separators or '..'.",
            url,
        )
        raise SystemExit(1)
    # Reject empty or dot-only names
    if not collection_name or collection_name in (".", ".."):
        output_extract_error(
            json_output,
            "InvalidCollectionNameError",
            f"Invalid collection name: '{collection_name}'. Collection name cannot be empty.",
            url,
        )
        raise SystemExit(1)


def output_extract_error(
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


def make_extract_progress_callback(
    *,
    skipped_message: str = "  ↪ Skipped (already extracted)",
) -> Callable[[ExtractionProgress], None]:
    """Build the text-mode progress callback that every extract command shares."""
    from portolan_cli.output import detail, info, warn

    def on_progress(progress: ExtractionProgress) -> None:
        if progress.status == "starting":
            info(f"[{progress.layer_index + 1}/{progress.total_layers}] {progress.layer_name}")
        elif progress.status == "success":
            detail("  ✓ Success")
        elif progress.status == "failed":
            # Issue #504: show the error inline instead of "Failed" alone.
            warn(f"  ✗ Failed: {progress.error}" if progress.error else "  ✗ Failed")
        elif progress.status == "skipped":
            detail(skipped_message)

    return on_progress


def parse_extract_bbox(
    bbox: str | None,
    *,
    url: str,
    use_json: bool,
    command: str,
) -> tuple[float, float, float, float] | None:
    """Parse the --bbox option. Stop the command when the value is not valid."""
    if not bbox:
        return None
    try:
        parts = [float(x.strip()) for x in bbox.split(",")]
    except ValueError as e:
        output_extract_error(use_json, "InvalidBBoxError", str(e), url, command=command)
        raise SystemExit(1) from None
    if len(parts) != 4:
        output_extract_error(
            use_json,
            "InvalidBBoxError",
            "bbox must have 4 values: minx,miny,maxx,maxy",
            url,
            command=command,
        )
        raise SystemExit(1)
    return (parts[0], parts[1], parts[2], parts[3])


def confirm_extraction(
    *,
    url: str,
    output_dir: Path,
    include: list[str] | None,
    exclude: list[str] | None,
    filter_label: str,
    line: Callable[[str], None],
    sub_line: Callable[[str], None],
    auto: bool,
    dry_run: bool,
    use_json: bool,
) -> None:
    """Show the extraction plan and ask the user to confirm it.

    The prompt runs only in interactive text mode. `line` and `sub_line` select
    the output channel, because the ArcGIS command writes with `click.echo` and
    the other commands write with `output.py`.
    """
    if auto or dry_run or use_json:
        return
    line(f"Extract from: {url}")
    line(f"Output to: {output_dir}")
    if include:
        sub_line(f"{filter_label}: {', '.join(include)}")
    if exclude:
        sub_line(f"Exclude: {', '.join(exclude)}")
    if not click.confirm("Continue?", default=True):
        raise SystemExit(0)


def check_extract_catalog_id(
    catalog_id: str | None,
    *,
    raw: bool,
    url: str,
    command: str,
    use_json: bool,
) -> None:
    """Check ``--id`` before the extraction starts.

    ``init_catalog`` validates the id, but extract calls it after the whole
    download. A rejected id would cost the harvest and leave data on disk with
    no catalog.json. This check fails on the first line instead. ``--raw``
    writes no catalog at all, so the flag does nothing and says so (issue #821).

    Args:
        catalog_id: The id the caller supplied, or None.
        raw: True when ``--raw`` suppresses catalog creation.
        url: The service URL, for the JSON error envelope.
        command: The command name for the JSON error envelope.
        use_json: True when the command emits JSON.

    Raises:
        SystemExit: If the id is not a valid STAC identifier.
    """
    if catalog_id is None:
        return

    from portolan_cli.output import warn

    try:
        validate_catalog_id(catalog_id)
    except InputValidationError as err:
        output_extract_error(use_json, "InputValidationError", str(err), url, command=command)
        raise SystemExit(1) from None

    if raw:
        warn(f"Ignored --id '{catalog_id}'. --raw writes no catalog.")


def output_extract_result(
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
                "empty": report.summary.empty,
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

        coverage = report.folder_coverage
        if coverage is not None:
            data["folder_coverage"] = coverage.to_dict()

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
    # Build status summary parts for display
    status_parts: list[str] = []
    if report.summary.failed > 0:
        status_parts.append(f"{report.summary.failed} failed")
    if report.summary.empty > 0:
        status_parts.append(f"{report.summary.empty} empty")

    if report.summary.failed > 0:
        # Partial failure: warn and list failed layers
        status_suffix = f" ({', '.join(status_parts)})" if status_parts else ""
        warn(
            f"Extracted {report.summary.succeeded}/{report.summary.total_layers} "
            f"layers{status_suffix}"
        )
        for layer in report.layers:
            if layer.status == "failed":
                error(f"  ✗ {layer.name}: {layer.error}")
        info_output(f"Output: {output_dir}")
        info_output(f"Report: {output_dir}/.portolan/extraction-report.json")
        raise SystemExit(1)

    # Success case (may still have empty layers)
    status_suffix = f" ({', '.join(status_parts)})" if status_parts else ""
    success(
        f"Extracted {report.summary.succeeded}/{report.summary.total_layers} layers{status_suffix}"
    )
    info_output(f"Output: {output_dir}")
    info_output(f"Report: {output_dir}/.portolan/extraction-report.json")

    coverage = report.folder_coverage
    if coverage is not None:
        info_output(
            f"Folders traversed: {len(coverage.folders_visited)}, "
            f"skipped: {len(coverage.folders_skipped)}, "
            f"services found: {coverage.services_found}"
        )
        for folder, reason in coverage.folders_skipped:
            warn(f"Skipped folder {folder}: {reason}")


def _apply(command: Callable[..., Any], decorators: list[Any]) -> Callable[..., Any]:
    """Apply decorators so that Click lists them in list order."""
    for decorator in reversed(decorators):
        command = decorator(command)
    return command


def source_arguments(command: Callable[..., Any]) -> Callable[..., Any]:
    """Add the ``URL`` and ``OUTPUT_DIR`` arguments of an extract command."""
    return _apply(
        command,
        [
            click.argument("url"),
            click.argument("output_dir", type=click.Path(path_type=Path), required=False),
        ],
    )


def run_options(unit: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Add ``--resume``, ``--dry-run``, ``--json``, ``--auto``, and ``--raw``.

    Args:
        unit: What the source extracts, ``layers`` or ``tables``, for the help text.

    Returns:
        A decorator for the command.
    """
    decorators = [
        click.option(
            "--resume",
            is_flag=True,
            help=f"Resume from existing extraction-report.json (skip succeeded {unit}).",
        ),
        click.option("--dry-run", is_flag=True, help=f"List {unit} without extracting."),
        click.option(
            "--json", "json_output", is_flag=True, help="Output extraction report as JSON."
        ),
        click.option("--auto", is_flag=True, help="Skip confirmation prompts."),
        click.option(
            "--raw",
            is_flag=True,
            help="Skip auto-init: create only extraction files, no STAC catalog.",
        ),
    ]
    return lambda command: _apply(command, decorators)


def catalog_options(command: Callable[..., Any]) -> Callable[..., Any]:
    """Add ``--id``, ``--license``, and ``--license-url`` to an extract command.

    Click lists options in decorator order. This decorator applies the three
    in that order, so it can stand where the three decorators stood.
    """
    options = [
        click.option(
            "--id",
            "catalog_id",
            type=str,
            default=None,
            help="Catalog id. Defaults to the sanitized directory name.",
        ),
        click.option(
            "--license",
            "license_id",
            type=str,
            default=None,
            help="SPDX license identifier, or 'other' with --license-url. Required unless the "
            "source publishes a license URL of its own.",
        ),
        click.option(
            "--license-url",
            type=str,
            default=None,
            help="URL of the license text. Required when --license is 'other'.",
        ),
    ]
    return _apply(command, options)


def fail_missing_license(
    err: MissingLicenseError,
    *,
    use_json: bool,
    url: str,
    command: str = "extract-arcgis",
) -> NoReturn:
    """Report a harvest with no usable license and stop the command.

    Extract raises it before the download starts, so the user re-runs the
    command rather than the harvest (issue #686).
    """
    output_extract_error(use_json, type(err).__name__, str(err), url, command=command)
    if not use_json:
        info_output(CLI_LICENSE_REMEDIATION)
    raise SystemExit(1) from None
