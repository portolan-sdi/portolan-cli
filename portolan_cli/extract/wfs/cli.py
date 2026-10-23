"""The ``portolan extract wfs`` command, part of the CLI layer.

The command parses the options, and ``orchestrator.extract_wfs_catalog``
harvests the layers of an OGC Web Feature Service.
"""

from __future__ import annotations

from pathlib import Path

import click

from portolan_cli.cli import should_output_json
from portolan_cli.errors import MissingLicenseError
from portolan_cli.extract.common import cli_helpers


@click.command("wfs")
@cli_helpers.source_arguments
@click.option(
    "--layers",
    type=str,
    default=None,
    help="Include layers by name or glob (comma-separated). A pattern matches the full "
    "workspace:layer name or the part after the ':'. Example: 'roads,ws:build*'",
)
@click.option(
    "--exclude-layers",
    type=str,
    default=None,
    help="Exclude layers by name or glob (comma-separated), matched as for --layers. "
    "Example: 'test_*'",
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
    default=100000,
    help="Features per page for large layer pagination (default: 100000).",
)
@click.option(
    "--auto-tile/--no-auto-tile",
    default=True,
    help="Subdivide the bbox and retry when a server caps maxFeatures, so "
    "capped layers extract completely (default: enabled).",
)
@cli_helpers.run_options("layers")
@cli_helpers.catalog_options
@click.pass_context
def extract_wfs_cmd(
    ctx: click.Context,
    catalog_id: str | None,
    license_id: str | None,
    license_url: str | None,
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
    auto_tile: bool,
    resume: bool,
    dry_run: bool,
    json_output: bool,
    auto: bool,
    raw: bool,
) -> None:
    r"""Extract data from WFS (Web Feature Service) endpoints.

    Downloads layers from a WFS service and creates a Portolan catalog
    with GeoParquet files and STAC metadata.

    URL is the WFS service endpoint URL.
    OUTPUT_DIR is the directory to write extracted data (default: 'wfs_extract').

    The catalog id comes from the output directory name. Pass --id to set it
    yourself, because that id is the catalog's public STAC identity.

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
        extract_wfs_catalog,
    )
    from portolan_cli.output import detail, info

    use_json = should_output_json(ctx, json_output)

    cli_helpers.check_extract_catalog_id(
        catalog_id, raw=raw, url=url, command="extract-wfs", use_json=use_json
    )

    # Default output directory
    if output_dir is None:
        output_dir = Path("wfs_extract")

    bbox_tuple = cli_helpers.parse_extract_bbox(
        bbox, url=url, use_json=use_json, command="extract-wfs"
    )

    # Build filter lists
    layer_include = cli_helpers.parse_filter_patterns(layers)
    layer_exclude = cli_helpers.parse_filter_patterns(exclude_layers)

    # Build options
    options = ExtractionOptions(
        catalog_id=catalog_id,
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
        auto_tile=auto_tile,
        license=license_id,
        license_url=license_url,
    )

    on_progress = cli_helpers.make_extract_progress_callback()

    cli_helpers.confirm_extraction(
        url=url,
        output_dir=output_dir,
        include=layer_include,
        exclude=layer_exclude,
        filter_label="Layer filter",
        line=info,
        sub_line=detail,
        auto=auto,
        dry_run=dry_run,
        use_json=use_json,
    )

    try:
        report = extract_wfs_catalog(
            url=url,
            output_dir=output_dir,
            layer_filter=layer_include,
            layer_exclude=layer_exclude,
            options=options,
            on_progress=None if use_json else on_progress,
        )
    except MissingLicenseError as e:
        cli_helpers.fail_missing_license(e, use_json=use_json, url=url, command="extract-wfs")
    except Exception as e:
        cli_helpers.output_extract_error(
            use_json, type(e).__name__, f"Extraction failed: {e}", url, command="extract-wfs"
        )
        raise SystemExit(1) from None

    cli_helpers.output_extract_result(report, output_dir, use_json, dry_run, command="extract-wfs")
