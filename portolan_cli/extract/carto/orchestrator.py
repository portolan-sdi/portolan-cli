"""Carto extraction orchestrator.

Ties the Carto extraction components together:
- Discovery (CDB_UserTables) → filtering → extraction (gpio) → report → auto-init.

The orchestrator is the main entry point for ``portolan extract carto``.
Each table is extracted via geoparquet-io's ``convert_carto_to_geoparquet``;
discovery and the surrounding catalog machinery live here.

Typical usage:
    from portolan_cli.extract.carto.orchestrator import extract_carto_catalog

    result = extract_carto_catalog(
        url="https://phl.carto.com/api/v2/sql",
        output_dir=Path("./output"),
        layer_filter=["vacant_*"],
    )
"""

from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlencode

from portolan_cli.extract.carto.discovery import (
    CartoDiscoveryError,
    CartoDiscoveryResult,
    CartoTableInfo,
    discover_carto_tables,
    quote_table_identifier,
    table_has_geometry,
    tables_from_names,
)
from portolan_cli.extract.common.filters import filter_layers
from portolan_cli.extract.common.report import (
    ExtractionReport,
    ExtractionSummary,
    LayerResult,
    MetadataExtracted,
    save_report,
)
from portolan_cli.extract.common.resume import ResumeState, get_resume_state, should_process_layer
from portolan_cli.extract.common.retry import RetryConfig, retry_with_backoff

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

_GLOB_CHARS = ("*", "?", "[")

_NON_SPATIAL_NOTE = (
    "Non-spatial table (no geometry column); will be extracted as plain Parquet "
    "into a tabular collection (portolan:geospatial: false, ADR-0047)."
)


@dataclass
class ExtractionOptions:
    """Options for Carto extraction.

    Attributes:
        workers: Number of parallel workers for table extraction.
        retries: Number of retry attempts per failed table.
        timeout: Per-table request timeout in seconds (passed to gpio).
        resume: Whether to resume from an existing extraction report.
        raw: If True, skip auto-init (only create extraction files, no STAC).
        dry_run: If True, list tables without extracting.
        where: SQL WHERE clause applied to every extracted table.
        bbox: Bounding box filter (minx, miny, maxx, maxy).
        limit: Maximum rows per table (None for unlimited).
        include_cols: Comma-separated columns to include.
        exclude_cols: Comma-separated columns to exclude.
        api_key: Carto API key (or set via the CARTO_API_KEY env var).
    """

    workers: int = 1
    retries: int = 3
    timeout: float = 120.0
    resume: bool = False
    raw: bool = False
    dry_run: bool = False
    where: str | None = None
    bbox: tuple[float, float, float, float] | None = None
    limit: int | None = None
    include_cols: str | None = None
    exclude_cols: str | None = None
    api_key: str | None = None


@dataclass
class ExtractionProgress:
    """Progress callback data for extraction.

    Attributes:
        layer_index: Current table index (0-based).
        total_layers: Total number of tables to process.
        layer_name: Name of the current table.
        status: One of "starting", "extracting", "success", "failed", "skipped".
        error: Error message when status is "failed".
    """

    layer_index: int
    total_layers: int
    layer_name: str
    status: str
    error: str | None = None


def _slug_for_table(name: str) -> str:
    """Convert a Carto table name to a filesystem-safe slug.

    Carto table names are already SQL identifiers (unique within an account),
    so no collision disambiguation is needed.
    """
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug or "unnamed"


def _build_table_query_url(sql_api_url: str, table_name: str) -> str:
    """Build a SQL API URL that selects a table (used as a provenance via-link).

    The query is never executed by Portolan — it is stored as a STAC ``via`` link
    href — but the table name is still quoted as a SQL identifier so the link is a
    valid, copy-pasteable Carto query.
    """
    query = f"SELECT * FROM {quote_table_identifier(table_name)}"  # nosec B608
    return f"{sql_api_url}?{urlencode({'q': query})}"


def _emit_progress(
    on_progress: Callable[[ExtractionProgress], None] | None,
    layer_index: int,
    total_layers: int,
    layer_name: str,
    status: str,
    error: str | None = None,
) -> None:
    """Emit a progress event if a callback is provided."""
    if on_progress:
        on_progress(
            ExtractionProgress(
                layer_index=layer_index,
                total_layers=total_layers,
                layer_name=layer_name,
                status=status,
                error=error,
            )
        )


def _filter_discovered_tables(
    tables: list[CartoTableInfo],
    layer_filter: list[str] | None,
    layer_exclude: list[str] | None,
) -> list[CartoTableInfo]:
    """Apply include/exclude glob filters to discovered tables."""
    if not layer_filter and not layer_exclude:
        return tables

    filtered = filter_layers(
        [t.to_filter_dict() for t in tables], include=layer_filter, exclude=layer_exclude
    )
    kept_ids = {d["id"] for d in filtered}
    return [t for t in tables if t.id in kept_ids]


def _resolve_geometry(
    sql_api_url: str,
    tables: list[CartoTableInfo],
    options: ExtractionOptions,
) -> list[CartoTableInfo]:
    """Probe each table for a geometry column (run after filtering).

    Probing only the post-filter set keeps discovery to a single request plus
    one cheap ``LIMIT 0`` probe per *wanted* table, instead of probing the
    whole account.
    """
    for table in tables:
        table.has_geometry = table_has_geometry(
            sql_api_url, table.name, api_key=options.api_key, timeout=options.timeout
        )
    return tables


def _discover_tables(
    url: str,
    layer_filter: list[str] | None,
    options: ExtractionOptions,
) -> CartoDiscoveryResult:
    """Enumerate account tables, falling back to explicit names if enumeration fails.

    If ``CDB_UserTables()`` is unavailable (single public table, or an API key
    without catalog access) but the user named literal (non-glob) tables in
    ``layer_filter``, treat those names as the table set.
    """
    try:
        return discover_carto_tables(url, api_key=options.api_key, timeout=options.timeout)
    except CartoDiscoveryError:
        literal = [p for p in (layer_filter or []) if not any(c in p for c in _GLOB_CHARS)]
        if not literal:
            raise
        logger.warning(
            "Carto table enumeration failed; falling back to explicit tables: %s", literal
        )
        return tables_from_names(url, literal, api_key=options.api_key, timeout=options.timeout)


def _extract_single_table(
    sql_api_url: str,
    table: CartoTableInfo,
    output_path: Path,
    options: ExtractionOptions,
) -> tuple[int, int, float]:
    """Extract one Carto table via geoparquet-io.

    Spatial tables become optimized GeoParquet; non-spatial tables become plain
    Parquet (no ``geo`` metadata key) via gpio's ``geometry=False`` path, so they
    can be routed into Portolan's tabular pipeline (ADR-0047). ``bbox`` is dropped
    for non-spatial tables because a bounding-box filter requires a geometry.

    Returns:
        Tuple of (feature_count, file_size_bytes, duration_seconds).
    """
    from geoparquet_io.core.carto import convert_carto_to_geoparquet  # type: ignore[import-untyped]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    start_time = time.monotonic()

    convert_carto_to_geoparquet(
        url=sql_api_url,
        table_name=table.name,
        output_file=str(output_path),
        where=options.where,
        bbox=options.bbox if table.has_geometry else None,
        limit=options.limit,
        include_cols=options.include_cols,
        exclude_cols=options.exclude_cols,
        api_key=options.api_key,
        timeout=options.timeout,
        overwrite=True,
        geometry=table.has_geometry,
    )

    duration = time.monotonic() - start_time

    if output_path.exists():
        import pyarrow.parquet as pq

        metadata = pq.read_metadata(output_path)
        feature_count = metadata.num_rows
        file_size = output_path.stat().st_size
    else:
        feature_count = 0
        file_size = 0

    return feature_count, file_size, duration


def _extract_table_task(
    sql_api_url: str,
    table: CartoTableInfo,
    output_dir: Path,
    options: ExtractionOptions,
) -> LayerResult:
    """Extract a single table with retry; return a LayerResult."""
    slug = _slug_for_table(table.name)
    output_path = output_dir / slug / f"{slug}.parquet"

    retry_config = RetryConfig(max_attempts=options.retries)
    result = retry_with_backoff(
        _extract_single_table,
        retry_config,
        sql_api_url,
        table,
        output_path,
        options,
        on_retry=lambda attempt, err: logger.debug(
            "Retry %d for table %s: %s", attempt, table.name, err
        ),
    )

    if result.success:
        features, size_bytes, duration = result.value  # type: ignore[misc]
        return LayerResult(
            id=table.id,
            name=table.name,
            status="success",
            features=features,
            size_bytes=size_bytes,
            duration_seconds=duration,
            output_path=str(output_path.relative_to(output_dir)),
            warnings=[],
            error=None,
            attempts=result.attempts,
        )

    error_msg = str(result.error) if result.error else "Unknown error"
    return LayerResult(
        id=table.id,
        name=table.name,
        status="failed",
        features=None,
        size_bytes=None,
        duration_seconds=None,
        output_path=None,
        warnings=[],
        error=error_msg,
        attempts=result.attempts,
    )


def _extract_tables(
    sql_api_url: str,
    tables_to_extract: list[CartoTableInfo],
    output_dir: Path,
    options: ExtractionOptions,
    total: int,
    on_progress: Callable[[ExtractionProgress], None] | None,
) -> list[LayerResult]:
    """Extract tables sequentially, or in parallel when workers > 1.

    Per-table timeout is enforced inside gpio (the ``timeout`` argument), so no
    external deadline tracking is needed.
    """
    results: list[LayerResult] = []

    if options.workers > 1 and len(tables_to_extract) > 1:
        max_workers = min(options.workers, len(tables_to_extract))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_table = {
                executor.submit(_extract_table_task, sql_api_url, table, output_dir, options): table
                for table in tables_to_extract
            }
            for future in as_completed(future_to_table):
                table = future_to_table[future]
                result = future.result()
                results.append(result)
                err = result.error if result.status == "failed" else None
                _emit_progress(on_progress, table.id, total, table.name, result.status, error=err)
        return results

    for table in tables_to_extract:
        _emit_progress(on_progress, table.id, total, table.name, "starting")
        _emit_progress(on_progress, table.id, total, table.name, "extracting")
        result = _extract_table_task(sql_api_url, table, output_dir, options)
        results.append(result)
        err = result.error if result.status == "failed" else None
        _emit_progress(on_progress, table.id, total, table.name, result.status, error=err)

    return results


def _skipped_result(table: CartoTableInfo, reason: str) -> LayerResult:
    """Build a skipped LayerResult (resume: already extracted)."""
    return LayerResult(
        id=table.id,
        name=table.name,
        status="skipped",
        features=0,
        size_bytes=0,
        duration_seconds=0.0,
        output_path=None,
        warnings=[],
        error=reason,
        attempts=0,
    )


def _get_package_version(package_name: str) -> str:
    """Get the installed version of a package, or 'unknown'."""
    try:
        from importlib.metadata import version

        return version(package_name)
    except Exception:
        return "unknown"


def _build_metadata(source_url: str, account_name: str | None) -> MetadataExtracted:
    """Build the harvested-metadata block for the report."""
    attribution = f"Carto account: {account_name}" if account_name else None
    return MetadataExtracted(
        source_url=source_url,
        description=None,
        attribution=attribution,
        keywords=None,
        contact_name=None,
        processing_notes=None,
        known_issues=None,
        license_info_raw=None,
    )


def _build_summary(layer_results: list[LayerResult]) -> ExtractionSummary:
    """Compute the aggregate summary from table results."""
    return ExtractionSummary(
        total_layers=len(layer_results),
        succeeded=sum(1 for r in layer_results if r.status == "success"),
        failed=sum(1 for r in layer_results if r.status == "failed"),
        skipped=sum(1 for r in layer_results if r.status == "skipped"),
        empty=sum(1 for r in layer_results if r.status == "empty"),
        total_features=sum(r.features or 0 for r in layer_results),
        total_size_bytes=sum(r.size_bytes or 0 for r in layer_results),
        total_duration_seconds=sum(r.duration_seconds or 0.0 for r in layer_results),
    )


def _build_report(
    source_url: str,
    account_name: str | None,
    layer_results: list[LayerResult],
) -> ExtractionReport:
    """Build an ExtractionReport from table results."""
    return ExtractionReport(
        extraction_date=datetime.now(timezone.utc).isoformat(),
        source_url=source_url,
        portolan_version=_get_package_version("portolan-cli"),
        gpio_version=_get_package_version("geoparquet-io"),
        metadata_extracted=_build_metadata(source_url, account_name),
        layers=layer_results,
        summary=_build_summary(layer_results),
    )


def _build_dry_run_report(
    source_url: str,
    tables: list[CartoTableInfo],
    account_name: str | None = None,
) -> ExtractionReport:
    """Build a report for dry-run mode (tables marked 'pending')."""
    results = [
        LayerResult(
            id=t.id,
            name=t.name,
            status="pending",
            features=0,
            size_bytes=0,
            duration_seconds=0.0,
            output_path="",
            warnings=[] if t.has_geometry else [_NON_SPATIAL_NOTE],
            error=None,
            attempts=0,
        )
        for t in tables
    ]
    return _build_report(source_url, account_name, results)


def extract_carto_catalog(
    url: str,
    output_dir: Path,
    *,
    layer_filter: list[str] | None = None,
    layer_exclude: list[str] | None = None,
    options: ExtractionOptions | None = None,
    on_progress: Callable[[ExtractionProgress], None] | None = None,
) -> ExtractionReport:
    """Extract tables from a Carto account into a Portolan catalog.

    Args:
        url: Carto SQL API URL or account domain.
        output_dir: Directory to write extracted data.
        layer_filter: Glob patterns / names of tables to include (None = all).
        layer_exclude: Glob patterns / names of tables to exclude.
        options: Extraction options (defaults to ExtractionOptions()).
        on_progress: Callback for progress updates.

    Returns:
        ExtractionReport with results for all tables.

    Raises:
        CartoDiscoveryError: If the account cannot be enumerated and no explicit
            tables were provided as a fallback.
    """
    if options is None:
        options = ExtractionOptions()

    discovery = _discover_tables(url, layer_filter, options)
    sql_api_url = discovery.service_url
    tables = _filter_discovered_tables(discovery.tables, layer_filter, layer_exclude)
    tables = _resolve_geometry(sql_api_url, tables, options)

    if options.dry_run:
        return _build_dry_run_report(sql_api_url, tables, discovery.account_name)

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / ".portolan").mkdir(exist_ok=True)

    report_path = output_dir / ".portolan" / "extraction-report.json"
    resume_state: ResumeState | None = None
    if options.resume and report_path.exists():
        from portolan_cli.extract.common.report import load_report

        resume_state = get_resume_state(load_report(report_path))

    tables_to_extract: list[CartoTableInfo] = []
    pre_results: list[LayerResult] = []
    total = len(tables)

    for table in tables:
        if resume_state and not should_process_layer(table.id, resume_state, layer_name=table.name):
            _emit_progress(on_progress, table.id, total, table.name, "skipped")
            pre_results.append(_skipped_result(table, reason="Already extracted"))
            continue
        tables_to_extract.append(table)

    extracted_results = _extract_tables(
        sql_api_url, tables_to_extract, output_dir, options, total, on_progress
    )

    all_results = pre_results + extracted_results
    all_results.sort(key=lambda r: r.id)

    report = _build_report(sql_api_url, discovery.account_name, all_results)
    save_report(report, report_path)

    if not options.raw:
        _auto_init_catalog(output_dir, report, discovery)

    return report


def _auto_init_catalog(
    output_dir: Path,
    report: ExtractionReport,
    discovery: CartoDiscoveryResult,
) -> None:
    """Initialize a Portolan catalog and add extracted tables.

    Creates catalog.json/config.yaml/collection.json, adds provenance via-links,
    and seeds metadata.yaml at catalog and collection level.
    """
    from portolan_cli.catalog import add_files, init_catalog
    from portolan_cli.config import set_setting
    from portolan_cli.formats import is_geoparquet

    parquet_files = [
        output_dir / r.output_path for r in report.layers if r.status == "success" and r.output_path
    ]
    if not parquet_files:
        return

    title = discovery.account_name
    init_catalog(output_dir, title=title, description=None)

    # Non-geo (tabular) outputs carry no `geo` metadata key; add_files only
    # accepts them as standalone collection-level assets when tabular support is
    # enabled (ADR-0047). Enable it before add_files when any output is non-geo.
    if any(not is_geoparquet(path) for path in parquet_files):
        set_setting(output_dir, "tabular.enabled", True)

    add_files(paths=parquet_files, catalog_root=output_dir)

    _add_via_links_to_collections(output_dir, report)
    _seed_metadata_from_extraction(output_dir, report, discovery.account_name)
    _seed_collection_metadata_carto(output_dir, report)


def _add_via_links_to_collections(output_dir: Path, report: ExtractionReport) -> None:
    """Add a `via` provenance link (a Carto SQL query URL) to each collection."""
    from portolan_cli.stac import add_via_link

    for layer in report.layers:
        if layer.status != "success" or not layer.output_path:
            continue
        collection_path = output_dir / Path(layer.output_path).parent / "collection.json"
        if not collection_path.exists():
            continue
        add_via_link(
            collection_path,
            _build_table_query_url(report.source_url, layer.name),
            title=f"Source Carto table: {layer.name}",
        )


def _seed_metadata_from_extraction(
    output_dir: Path, report: ExtractionReport, account_name: str | None
) -> None:
    """Seed catalog-level metadata.yaml from Carto account metadata."""
    from portolan_cli.extract.carto.metadata import extract_carto_metadata
    from portolan_cli.metadata_seeding import seed_metadata_yaml
    from portolan_cli.output import info

    extracted = extract_carto_metadata(report.source_url, account_name).to_extracted()
    metadata_path = output_dir / ".portolan" / "metadata.yaml"
    if seed_metadata_yaml(extracted, metadata_path):
        info(f"Seeded metadata.yaml from {extracted.source_type}")


def _seed_collection_metadata_carto(output_dir: Path, report: ExtractionReport) -> None:
    """Seed per-collection metadata.yaml with table-specific provenance."""
    from portolan_cli.extract.common.metadata_seeding import seed_collection_metadata

    for layer in report.layers:
        if layer.status != "success" or not layer.output_path:
            continue
        collection_dir = output_dir / Path(layer.output_path).parent
        seed_collection_metadata(
            collection_dir,
            source_type="carto",
            source_url=_build_table_query_url(report.source_url, layer.name),
            layer_name=layer.name,
        )
