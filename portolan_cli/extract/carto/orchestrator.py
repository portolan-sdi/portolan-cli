"""Carto extraction through the shared LayerSource driver.

``CartoLayerSource`` implements ``LayerSource`` for a Carto SQL API account,
and ``extract_carto_catalog`` runs it with ``run_layer_extraction``.

- ``discover`` lists the account's tables. When the account cannot be
  listed, it falls back to the literal table names in the layer filter. It
  then probes only the kept tables for a geometry column.
- ``fetch`` calls geoparquet-io's ``convert_carto_to_geoparquet``. A table with
  no geometry becomes plain Parquet in a tabular collection.

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
from dataclasses import dataclass, replace
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
from portolan_cli.extract.common.report import MetadataExtracted
from portolan_cli.extract.common.source import (
    Discovery,
    LayerMetadata,
    LayerRef,
    LayerSourceOptions,
    SourceLink,
    filter_refs,
    run_layer_extraction,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_cli.extract.common.progress import ExtractionProgress
    from portolan_cli.extract.common.report import ExtractionReport

logger = logging.getLogger(__name__)

_GLOB_CHARS = ("*", "?", "[")

_NON_SPATIAL_NOTE = (
    "Non-spatial table (no geometry column); will be extracted as plain Parquet "
    "into a tabular collection."
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
        license: SPDX identifier for the harvested data, or "other" with
            license_url. Carto exposes no license metadata, so this is the only
            source (issue #686).
        license_url: URL of the license text.
        catalog_id: Catalog id for the created catalog. None derives it from
            the output directory name, which is the behavior before issue #821.
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
    license: str | None = None
    license_url: str | None = None
    catalog_id: str | None = None


def _build_table_query_url(sql_api_url: str, table_name: str) -> str:
    """Build a SQL API URL that selects a table (used as a provenance via-link).

    The query is never executed by Portolan — it is stored as a STAC ``via`` link
    href — but the table name is still quoted as a SQL identifier so the link is a
    valid, copy-pasteable Carto query.
    """
    query = f"SELECT * FROM {quote_table_identifier(table_name)}"  # noqa: S608
    return f"{sql_api_url}?{urlencode({'q': query})}"


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


def _build_metadata(source_url: str, account_name: str | None) -> MetadataExtracted:
    """Build the harvested-metadata block for the report."""
    attribution = f"Carto account: {account_name}" if account_name else None
    return MetadataExtracted(
        source_url=source_url,
        description=None,
        attribution=attribution,
        keywords=None,
        producer=None,
        processing_notes=None,
        known_issues=None,
        license_info_raw=None,
    )


class CartoLayerSource:
    """A Carto SQL API account as a ``LayerSource``. Each table is a layer."""

    name = "carto"
    summary = "Carto SQL API tables."

    def __init__(self, options: ExtractionOptions | None = None) -> None:
        self.options = options or ExtractionOptions()

    def discover(self, source: str, options: LayerSourceOptions) -> Discovery:
        """List and filter the tables, then probe the kept ones for geometry.

        Probing only the kept tables costs one ``LIMIT 0`` request per wanted
        table, not one per table in the account.
        """
        found = _discover_tables(source, options.layer_filter, self.options)
        sql_api_url = found.service_url
        refs = filter_refs(
            [LayerRef(id=table.id, name=table.name, data=table) for table in found.tables],
            options.layer_filter,
            options.layer_exclude,
        )
        layers = []
        for ref in refs:
            table: CartoTableInfo = ref.data
            table.has_geometry = table_has_geometry(
                sql_api_url, table.name, api_key=self.options.api_key, timeout=self.options.timeout
            )
            notes = () if table.has_geometry else (_NON_SPATIAL_NOTE,)
            layers.append(replace(ref, notes=notes))
        return self.discovery_from(found, layers)

    @staticmethod
    def discovery_from(found: CartoDiscoveryResult, layers: list[LayerRef]) -> Discovery:
        """Build the driver's ``Discovery`` for a Carto account.

        Carto publishes no license metadata, so ``--license`` is the only
        source (issue #686). The account name titles the catalog.
        """
        from portolan_cli.extract.carto.metadata import extract_carto_metadata

        sql_api_url = found.service_url
        return Discovery(
            source_url=sql_api_url,
            layers=layers,
            metadata=_build_metadata(sql_api_url, found.account_name),
            catalog_seed=extract_carto_metadata(sql_api_url, found.account_name).to_extracted(),
            catalog_title=found.account_name,
            filtered=True,
        )

    def fetch(self, source: str, layer: LayerRef, dest: Path) -> int:
        """Write the table with geoparquet-io's ``convert_carto_to_geoparquet``.

        A spatial table becomes optimized GeoParquet. A non-spatial table
        becomes plain Parquet through gpio's ``geometry=False`` path, with no
        bbox filter, because a bounding-box filter needs a geometry.
        """
        from geoparquet_io.core import carto as gpio_carto  # type: ignore[import-untyped]

        table: CartoTableInfo = layer.data
        options = self.options
        gpio_carto.convert_carto_to_geoparquet(
            url=source,
            table_name=table.name,
            output_file=str(dest),
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
        if not dest.exists():
            return 0
        import pyarrow.parquet as pq

        return int(pq.read_metadata(dest).num_rows)

    def layer_metadata(self, source: str, layer: LayerRef) -> LayerMetadata:
        """Seed table provenance. Carto has no per-table description."""
        return LayerMetadata(source_url=_build_table_query_url(source, layer.name))

    def styles(self, source: str, layer: LayerRef, collection_dir: Path) -> None:
        """Carto publishes no styles."""

    def source_link(self, source: str, layer: LayerRef) -> SourceLink:
        """Link to a SQL API query that selects the table."""
        return SourceLink(
            href=_build_table_query_url(source, layer.name),
            title=f"Source Carto table: {layer.name}",
        )


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
        MissingLicenseError: If no license is given (issue #686).
    """
    options = options or ExtractionOptions()
    # gpio enforces the per-table timeout, so the driver sets no deadline.
    driver = LayerSourceOptions(
        layer_filter=layer_filter,
        layer_exclude=layer_exclude,
        resume=options.resume,
        dry_run=options.dry_run,
        raw=options.raw,
        retries=options.retries,
        workers=options.workers,
        catalog_id=options.catalog_id,
        license=options.license,
        license_url=options.license_url,
    )
    return run_layer_extraction(
        CartoLayerSource(options), url, output_dir, driver, on_progress=on_progress
    )
