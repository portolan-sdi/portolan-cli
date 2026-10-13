"""The ``portolan extract arcgis`` command, part of the CLI layer.

The command parses the options. ``orchestrator.extract_arcgis_catalog``
harvests FeatureServer and MapServer layers, and ``imageserver/`` harvests
ImageServer rasters.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click

from portolan_cli.cli import output_json_envelope, should_output_json
from portolan_cli.errors import MissingLicenseError
from portolan_cli.extract.common import cli_helpers
from portolan_cli.json_output import ErrorDetail, error_envelope, success_envelope
from portolan_cli.output import warn

if TYPE_CHECKING:
    from portolan_cli.extract.arcgis.url_parser import ParsedArcGISURL


def _handle_list_services_mode(
    url: str,
    service_filter: list[str] | None,
    timeout: float,
    use_json: bool,
    *,
    token: str | None = None,
    recurse: bool = True,
) -> None:
    """Handle --list-services mode: list available services and exit."""
    from portolan_cli.extract.arcgis.orchestrator import list_services as list_services_func

    try:
        result = list_services_func(
            url, service_filter=service_filter, token=token, recurse=recurse, timeout=timeout
        )
    except Exception as e:
        cli_helpers.output_extract_error(use_json, type(e).__name__, str(e), url)
        raise SystemExit(1) from None

    if use_json:
        click.echo(json.dumps(result.to_dict(), indent=2))
        return

    click.echo(f"Services at {url}:")
    click.echo()
    for svc in result.services:
        click.echo(f"  • {svc.name} ({svc.service_type})")
    click.echo()
    click.echo(f"Total: {len(result.services)} services")
    if result.coverage is not None:
        cov = result.coverage
        click.echo(
            f"Folders traversed: {len(cov.folders_visited)}, skipped: {len(cov.folders_skipped)}"
        )
        for folder, reason in cov.folders_skipped:
            warn(f"Skipped folder {folder}: {reason}")
    elif result.folders:
        click.echo(f"Folders: {', '.join(result.folders)}")


def _handle_imageserver_extraction(
    url: str,
    output_dir: Path,
    catalog_id: str | None,
    tile_size: int,
    coarse_scan: bool,
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
    license_id: str | None = None,
    license_url: str | None = None,
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
            cli_helpers.output_extract_error(
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

    # Validate collection_name at CLI layer (fail fast flat catalog rule)
    cli_helpers.validate_collection_name_cli(collection_name, json_output, url)

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
        catalog_id=catalog_id,
        tile_size=tile_size,
        coarse_scan=coarse_scan,
        max_concurrent=max_concurrent,
        max_retries=retries,
        dry_run=dry_run,
        resume=resume,
        raw=False,  # ImageServer always creates STAC structure
        bbox=bbox_tuple,
        bbox_crs=bbox_crs,
        timeout=timeout,
        compression=cog_settings.compression,
        use_json=json_output,
        collection_name=collection_name,
        license=license_id,
        license_url=license_url,
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


def _resolve_arcgis_token(
    *,
    url: str,
    token: str | None,
    username: str | None,
    password: str | None,
    timeout: float,
    auto: bool,
    use_json: bool,
) -> str | None:
    """Resolve an ArcGIS token from flags, env, or username/password.

    Precedence: explicit token > ARCGIS_TOKEN > username/password (minted).
    The password is never required on argv (it leaks via shell history and
    process listings): it comes from ARCGIS_PASSWORD, or an interactive prompt.
    Outputs an error envelope and raises SystemExit on auth failure.
    """
    from portolan_cli.extract.arcgis.auth import ArcGISCredentials, resolve_token

    resolved_token = token or os.environ.get("ARCGIS_TOKEN")
    resolved_password = password or os.environ.get("ARCGIS_PASSWORD")
    if username and not resolved_password and not auto and not use_json:
        resolved_password = click.prompt("ArcGIS password", hide_input=True)

    # Explicit username with no resolvable password must fail fast, not silently
    # run unauthenticated (resolve_token returns None for incomplete credentials).
    if username and not resolved_password and not resolved_token:
        cli_helpers.output_extract_error(
            use_json,
            "ArcGISAuthError",
            "ArcGIS --username requires a password (set ARCGIS_PASSWORD, "
            "or omit --auto/--json to be prompted)",
            url,
        )
        raise SystemExit(1)

    creds = ArcGISCredentials(
        token=resolved_token,
        username=username,
        password=resolved_password,
    )
    if creds.is_empty:
        return None
    try:
        return resolve_token(creds, url, timeout=timeout)
    except Exception as e:  # ArcGISAuthError and transport errors
        cli_helpers.output_extract_error(use_json, type(e).__name__, str(e), url)
        raise SystemExit(1) from None


def _default_arcgis_output_dir(parsed: ParsedArcGISURL) -> Path:
    """Infer the default output directory from a parsed ArcGIS URL."""
    from portolan_cli.extract.arcgis.url_parser import ArcGISURLType

    if parsed.url_type == ArcGISURLType.SERVICES_ROOT:
        return Path("services_extract")
    if parsed.url_type == ArcGISURLType.SERVICES_FOLDER:
        return Path((parsed.folder or "services_extract").replace("/", "_").lower())
    service_name = parsed.service_name or "arcgis_extract"
    return Path(service_name.replace("/", "_").lower())


@click.command("arcgis")
@cli_helpers.source_arguments
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
    "--token",
    default=None,
    help="ArcGIS token (or set ARCGIS_TOKEN). For secured services/folders.",
)
@click.option(
    "--username",
    default=None,
    help="ArcGIS username (mints a token via generateToken).",
)
@click.option(
    "--password",
    default=None,
    help="ArcGIS password (used with --username, or set ARCGIS_PASSWORD). "
    "Prompted interactively when omitted.",
)
@click.option(
    "--no-recurse",
    "no_recurse",
    is_flag=True,
    help="Do not traverse folders for services-root URLs (default: recurse).",
)
@click.option(
    "--output-crs",
    type=str,
    default="native",
    help="CRS for extracted GeoParquet. 'native' keeps the service's source CRS "
    "(default). Pass an EPSG code (e.g. EPSG:4326) to reproject.",
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
    help="Retry attempts per failed layer or tile (default: 3).",
)
@click.option(
    "--timeout",
    type=click.FloatRange(min=0.0, min_open=True),
    default=60.0,
    help="Per-request timeout in seconds for discovery, feature, and tile requests (default: 60).",
)
@cli_helpers.run_options("layers")
# ImageServer-specific options
@click.option(
    "--tile-size",
    type=click.IntRange(min=256, max=8192),
    default=4096,
    help="[ImageServer] Tile size in pixels (default: 4096).",
)
@click.option(
    "--coarse-scan/--no-coarse-scan",
    default=False,
    help=(
        "[ImageServer] For a cache-only service, ask a coarse cache level which "
        "blocks hold data before reading them (default: off). It makes a sparse "
        "service much faster, but it can skip a thin feature that the coarse "
        "level drops."
    ),
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
@cli_helpers.catalog_options
@click.pass_context
def extract_arcgis_cmd(
    ctx: click.Context,
    catalog_id: str | None,
    license_id: str | None,
    license_url: str | None,
    url: str,
    output_dir: Path | None,
    layers: str | None,
    exclude_layers: str | None,
    unified_filter: str | None,
    services: str | None,
    exclude_services: str | None,
    list_services: bool,
    token: str | None,
    username: str | None,
    password: str | None,
    no_recurse: bool,
    output_crs: str,
    workers: int,
    retries: int,
    timeout: float,
    resume: bool,
    dry_run: bool,
    json_output: bool,
    auto: bool,
    raw: bool,
    tile_size: int,
    coarse_scan: bool,
    bbox: str | None,
    bbox_crs: str | None,
    compression: str | None,
    max_concurrent: int,
    collection_name: str | None,
) -> None:
    r"""Extract data from ArcGIS FeatureServer/MapServer/ImageServer.

    Downloads layers from an ArcGIS REST service and creates a Portolan
    catalog with GeoParquet files (vector) or COG files (raster) and STAC metadata.

    URL is the ArcGIS service URL (FeatureServer, MapServer, ImageServer, or services root).
    OUTPUT_DIR is the directory to write extracted data (default: inferred from service name).

    The catalog id comes from the output directory name. Pass --id to set it
    yourself, because that id is the catalog's public STAC identity.

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
        - Data family: 'cod_ab_ukr*'

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

        # Reproject the output to WGS84 instead of keeping the source CRS
        portolan extract arcgis URL --output-crs EPSG:4326

        # Extract raw files only (no STAC catalog auto-init)
        portolan extract arcgis URL --raw

        # JSON output for agent consumption
        portolan extract arcgis URL --json
    """
    from portolan_cli.extract.arcgis.orchestrator import (
        ExtractionOptions,
        extract_arcgis_catalog,
    )
    from portolan_cli.extract.arcgis.url_parser import ArcGISURLType, parse_arcgis_url

    use_json = should_output_json(ctx, json_output)

    # Parse URL to get service name for default output dir
    try:
        parsed = parse_arcgis_url(url)
    except ValueError as e:
        cli_helpers.output_extract_error(use_json, "InvalidURLError", str(e), url)
        raise SystemExit(1) from None

    # The ImageServer path always writes a catalog, so --raw does not suppress
    # one there and --id still applies.
    cli_helpers.check_extract_catalog_id(
        catalog_id,
        raw=raw and parsed.url_type != ArcGISURLType.IMAGE_SERVER,
        url=url,
        command="extract-arcgis",
        use_json=use_json,
    )

    resolved_token = _resolve_arcgis_token(
        url=url,
        token=token,
        username=username,
        password=password,
        timeout=timeout,
        auto=auto,
        use_json=use_json,
    )

    # Handle --list-services mode
    if list_services:
        if parsed.url_type not in (
            ArcGISURLType.SERVICES_ROOT,
            ArcGISURLType.SERVICES_FOLDER,
        ):
            cli_helpers.output_extract_error(
                use_json,
                "InvalidURLError",
                "--list-services requires a services root or folder URL",
                url,
            )
            raise SystemExit(1)
        service_filter = cli_helpers.parse_filter_patterns(services)
        _handle_list_services_mode(
            url, service_filter, timeout, use_json, token=resolved_token, recurse=not no_recurse
        )
        return

    # Default output directory from service name (or "services_extract" for services root)
    if output_dir is None:
        output_dir = _default_arcgis_output_dir(parsed)

    # Handle ImageServer URLs (raster extraction)
    if parsed.url_type == ArcGISURLType.IMAGE_SERVER:
        if resolved_token is not None:
            # Raster extraction does not thread the token yet (issue #311); reject
            # rather than silently running unauthenticated against a secured service.
            cli_helpers.output_extract_error(
                use_json,
                "ArcGISAuthError",
                "ArcGIS authentication is not yet supported for ImageServer URLs (issue #311)",
                url,
            )
            raise SystemExit(1)
        _handle_imageserver_extraction(
            url=url,
            output_dir=output_dir,
            catalog_id=catalog_id,
            tile_size=tile_size,
            coarse_scan=coarse_scan,
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
            license_id=license_id,
            license_url=license_url,
        )
        return

    # Build filter lists
    layer_include, layer_exclude = cli_helpers.build_layer_filters(
        layers, exclude_layers, unified_filter
    )
    is_services_root = parsed.url_type == ArcGISURLType.SERVICES_ROOT
    service_include, service_exclude = cli_helpers.build_service_filters(
        services, exclude_services, unified_filter, is_services_root
    )

    # Build options
    options = ExtractionOptions(
        catalog_id=catalog_id,
        workers=workers,
        retries=retries,
        timeout=timeout,
        resume=resume,
        dry_run=dry_run,
        raw=raw,
        token=resolved_token,
        recurse=not no_recurse,
        license=license_id,
        license_url=license_url,
        output_crs=output_crs,
    )

    on_progress = cli_helpers.make_extract_progress_callback()

    cli_helpers.confirm_extraction(
        url=url,
        output_dir=output_dir,
        include=layer_include,
        exclude=layer_exclude,
        filter_label="Layer filter",
        line=click.echo,
        sub_line=click.echo,
        auto=auto,
        dry_run=dry_run,
        use_json=use_json,
    )

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
    except MissingLicenseError as e:
        cli_helpers.fail_missing_license(e, use_json=use_json, url=url)
    except NotImplementedError as e:
        cli_helpers.output_extract_error(use_json, "NotImplementedError", str(e), url)
        raise SystemExit(1) from None
    except Exception as e:
        cli_helpers.output_extract_error(use_json, type(e).__name__, f"Extraction failed: {e}", url)
        raise SystemExit(1) from None

    cli_helpers.output_extract_result(report, output_dir, use_json, dry_run)
