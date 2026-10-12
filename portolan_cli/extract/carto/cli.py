"""The ``portolan extract carto`` command, part of the CLI layer.

The command parses the options, and ``orchestrator.extract_carto_catalog``
harvests the tables of a Carto SQL API account.
"""

from __future__ import annotations

from pathlib import Path

import click

from portolan_cli.cli import should_output_json
from portolan_cli.errors import MissingLicenseError
from portolan_cli.extract.common import cli_helpers


@click.command("carto")
@cli_helpers.source_arguments
@click.option(
    "--tables",
    type=str,
    default=None,
    help="Include tables matching glob patterns (comma-separated). Example: 'vacant_*,zoning*'",
)
@click.option(
    "--exclude-tables",
    type=str,
    default=None,
    help="Exclude tables matching glob patterns (comma-separated). Example: 'test_*'",
)
@click.option(
    "--where",
    type=str,
    default=None,
    help="SQL WHERE clause applied to every table (e.g. \"updated_at > '2026-01-01'\").",
)
@click.option(
    "--bbox",
    type=str,
    default=None,
    help="Bounding box filter: minx,miny,maxx,maxy (WGS84).",
)
@click.option(
    "--limit",
    type=click.IntRange(min=1),
    default=None,
    help="Maximum rows per table.",
)
@click.option(
    "--include-cols",
    type=str,
    default=None,
    help="Comma-separated columns to include (default: all).",
)
@click.option(
    "--exclude-cols",
    type=str,
    default=None,
    help="Comma-separated columns to exclude.",
)
@click.option(
    "--api-key",
    type=str,
    default=None,
    help="Carto API key for private accounts (or set the CARTO_API_KEY env var).",
)
@click.option(
    "--workers",
    type=click.IntRange(min=1),
    default=1,
    help="Parallel workers for table extraction (default: 1).",
)
@click.option(
    "--retries",
    type=click.IntRange(min=1),
    default=3,
    help="Retry attempts per failed table (default: 3).",
)
@click.option(
    "--timeout",
    type=click.FloatRange(min=0.0, min_open=True),
    default=120.0,
    help="Per-table request timeout in seconds (default: 120).",
)
@cli_helpers.run_options("tables")
@cli_helpers.catalog_options
@click.pass_context
def extract_carto_cmd(
    ctx: click.Context,
    catalog_id: str | None,
    license_id: str | None,
    license_url: str | None,
    url: str,
    output_dir: Path | None,
    tables: str | None,
    exclude_tables: str | None,
    where: str | None,
    bbox: str | None,
    limit: int | None,
    include_cols: str | None,
    exclude_cols: str | None,
    api_key: str | None,
    workers: int,
    retries: int,
    timeout: float,
    resume: bool,
    dry_run: bool,
    json_output: bool,
    auto: bool,
    raw: bool,
) -> None:
    r"""Extract tables from a Carto SQL API account.

    Downloads tables from a Carto account and creates a Portolan catalog with
    STAC metadata. Tables are discovered via CDB_UserTables(). Spatial tables
    become GeoParquet; non-spatial tables become plain Parquet in tabular
    collections.

    URL is the Carto SQL API endpoint or account domain
    (e.g. https://phl.carto.com or https://phl.carto.com/api/v2/sql).
    OUTPUT_DIR is the directory to write extracted data (default: 'carto_extract').

    The catalog id comes from the output directory name. Pass --id to set it
    yourself, because that id is the catalog's public STAC identity.

    \b
    Examples:
        # Extract all tables from an account
        portolan extract carto https://phl.carto.com ./output

        # Extract specific tables by glob
        portolan extract carto URL --tables "vacant_*"

        # Incremental refresh via a WHERE clause
        portolan extract carto URL --tables my_table --where "updated_at > '2026-01-01'"

        # Dry run to see available tables
        portolan extract carto URL --dry-run
    """
    from portolan_cli.extract.carto.orchestrator import (
        ExtractionOptions,
        extract_carto_catalog,
    )
    from portolan_cli.output import detail, info

    use_json = should_output_json(ctx, json_output)

    cli_helpers.check_extract_catalog_id(
        catalog_id, raw=raw, url=url, command="extract-carto", use_json=use_json
    )

    if output_dir is None:
        output_dir = Path("carto_extract")

    bbox_tuple = cli_helpers.parse_extract_bbox(
        bbox, url=url, use_json=use_json, command="extract-carto"
    )

    table_include = cli_helpers.parse_filter_patterns(tables)
    table_exclude = cli_helpers.parse_filter_patterns(exclude_tables)

    options = ExtractionOptions(
        catalog_id=catalog_id,
        workers=workers,
        retries=retries,
        timeout=timeout,
        resume=resume,
        dry_run=dry_run,
        raw=raw,
        where=where,
        bbox=bbox_tuple,
        limit=limit,
        include_cols=include_cols,
        exclude_cols=exclude_cols,
        api_key=api_key,
        license=license_id,
        license_url=license_url,
    )

    on_progress = cli_helpers.make_extract_progress_callback(skipped_message="  ↪ Skipped")

    cli_helpers.confirm_extraction(
        url=url,
        output_dir=output_dir,
        include=table_include,
        exclude=table_exclude,
        filter_label="Table filter",
        line=info,
        sub_line=detail,
        auto=auto,
        dry_run=dry_run,
        use_json=use_json,
    )

    try:
        report = extract_carto_catalog(
            url=url,
            output_dir=output_dir,
            layer_filter=table_include,
            layer_exclude=table_exclude,
            options=options,
            on_progress=None if use_json else on_progress,
        )
    except MissingLicenseError as e:
        cli_helpers.fail_missing_license(e, use_json=use_json, url=url, command="extract-carto")
    except Exception as e:
        cli_helpers.output_extract_error(
            use_json, type(e).__name__, f"Extraction failed: {e}", url, command="extract-carto"
        )
        raise SystemExit(1) from None

    cli_helpers.output_extract_result(
        report, output_dir, use_json, dry_run, command="extract-carto"
    )


# =============================================================================
# Version management commands (iceberg backend only)
# =============================================================================
