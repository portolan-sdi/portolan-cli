"""Extraction orchestrator for ArcGIS services.

This module ties together all the extraction components:
- URL parsing → Discovery → Filtering → Extraction → Report generation

The orchestrator is the main entry point for `portolan extract arcgis`.
It handles both single-service (FeatureServer) and multi-service (services root)
extraction with resume capability.

Typical usage:
    from portolan_cli.extract.arcgis.orchestrator import extract_arcgis_catalog

    result = extract_arcgis_catalog(
        url="https://services.arcgis.com/.../FeatureServer",
        output_dir=Path("./output"),
        layer_filter=["Census*"],
        workers=3,
    )
    print(f"Extracted {result.summary.succeeded}/{result.summary.total_layers} layers")
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from portolan_cli.extract.arcgis.discovery import (
    FolderTraversal,
    LayerInfo,
    ServiceDiscoveryResult,
    ServiceInfo,
    discover_layers,
    discover_services,
    discover_services_recursive,
)
from portolan_cli.extract.arcgis.metadata import extract_arcgis_metadata
from portolan_cli.extract.arcgis.url_parser import (
    ArcGISURLType,
    ParsedArcGISURL,
    parse_arcgis_url,
)
from portolan_cli.extract.common.filters import filter_layers
from portolan_cli.extract.common.report import (
    ExtractionReport,
    ExtractionSummary,
    FolderCoverage,
    LayerResult,
    MetadataExtracted,
    load_report,
    save_report,
)
from portolan_cli.extract.common.resume import ResumeState, get_resume_state, should_process_layer
from portolan_cli.extract.common.retry import RetryConfig, retry_with_backoff
from portolan_cli.extract.common.styles import extract_esri_style

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from portolan_cli.extract.arcgis.metadata import ArcGISMetadata

logger = logging.getLogger(__name__)


def _coverage_from_traversal(traversal: FolderTraversal) -> FolderCoverage:
    """Map a discovery FolderTraversal to a serializable FolderCoverage."""
    return FolderCoverage(
        folders_visited=traversal.visited,
        folders_skipped=traversal.skipped,
        services_found=traversal.service_count,
    )


@dataclass
class ServicesRootDiscoveryResult:
    """Result of listing services from a services root URL.

    Used for --list-services mode and JSON output.

    Attributes:
        services: List of discovered services.
        folders: List of folder names in the services root.
        base_url: The services root URL that was queried.
        coverage: Optional folder traversal coverage when recursion was used.
    """

    services: list[ServiceInfo]
    folders: list[str]
    base_url: str
    coverage: FolderCoverage | None = None

    def to_dict(self) -> dict[str, object]:
        """Convert to JSON-serializable dict."""
        result: dict[str, object] = {
            "base_url": self.base_url,
            "services": [
                {
                    "name": s.name,
                    "type": s.service_type,
                    "url": s.get_url(self.base_url),
                }
                for s in self.services
            ],
            "folders": self.folders,
            "total_services": len(self.services),
        }
        if self.coverage is not None:
            result["folder_coverage"] = self.coverage.to_dict()
        return result


def list_services(
    url: str,
    *,
    service_types: Sequence[str] | None = None,
    service_filter: list[str] | None = None,
    token: str | None = None,
    recurse: bool = True,
    timeout: float = 60.0,
) -> ServicesRootDiscoveryResult:
    """List services from an ArcGIS services root or folder URL.

    Recurses into folders by default. Folders that error are skipped and
    recorded in the returned coverage.

    Args:
        url: ArcGIS services root or folder URL.
        service_types: Filter by service types (e.g., ["FeatureServer"]).
        service_filter: Glob patterns to filter service names.
        token: Optional ArcGIS token for authenticated endpoints.
        recurse: Whether to recurse into sub-folders (default True).
        timeout: Request timeout in seconds.

    Returns:
        ServicesRootDiscoveryResult with services, folders, and optional coverage.

    Raises:
        ValueError: If URL is not a services root or folder URL.
    """
    from portolan_cli.extract.arcgis.filters import filter_services

    parsed = parse_arcgis_url(url)
    if parsed.url_type not in (ArcGISURLType.SERVICES_ROOT, ArcGISURLType.SERVICES_FOLDER):
        msg = f"URL is not a services root or folder URL: {url}"
        raise ValueError(msg)

    if recurse:
        services, traversal = discover_services_recursive(
            url,
            service_types=list(service_types) if service_types else None,
            token=token,
            timeout=timeout,
        )
        coverage: FolderCoverage | None = _coverage_from_traversal(traversal)
        folders = traversal.visited
    else:
        services, folders = discover_services(
            url,
            service_types=list(service_types) if service_types else None,
            return_folders=True,
            timeout=timeout,
            token=token,
        )
        coverage = None

    if service_filter:
        service_names = [s.name for s in services]
        filtered_names = filter_services(
            service_names,
            include=service_filter,
            case_sensitive=False,
        )
        services = [s for s in services if s.name in filtered_names]

    return ServicesRootDiscoveryResult(
        services=services,
        folders=folders,
        base_url=parsed.base_url,
        coverage=coverage,
    )


def _emit_progress(
    on_progress: Callable[[ExtractionProgress], None] | None,
    layer_index: int,
    total_layers: int,
    layer_name: str,
    status: str,
) -> None:
    """Emit a progress event if callback is provided."""
    if on_progress:
        on_progress(
            ExtractionProgress(
                layer_index=layer_index,
                total_layers=total_layers,
                layer_name=layer_name,
                status=status,
            )
        )


def _slugify(name: str) -> str:
    """Convert a name to a filesystem-safe slug.

    Args:
        name: Original name (e.g., "Census Block Groups")

    Returns:
        Slugified name (e.g., "census_block_groups")
    """
    # Lowercase first
    slug = name.lower()
    # Replace all non-alphanumeric chars with underscore
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    # Collapse multiple underscores
    slug = re.sub(r"_+", "_", slug)
    # Strip leading/trailing underscores
    slug = slug.strip("_")
    return slug or "unnamed"


def _service_output_dir(output_dir: Path, service_name: str) -> Path:
    """Map a (possibly folder-qualified) service name to a nested directory.

    "ecml/active_faults" -> output_dir/ecml/active_faults
    "Top"                -> output_dir/top
    Each path segment is slugified independently so the folder hierarchy is
    preserved as nested subcatalogs (ADR-0032, ADR-0054).
    """
    parts = [_slugify(p) for p in service_name.split("/") if p]
    result = output_dir
    for part in parts:
        result = result / part
    return result


@dataclass
class ExtractionOptions:
    """Options for the extraction process.

    Attributes:
        workers: Number of parallel page requests per layer (gpio max_workers)
        retries: Number of retry attempts per failed layer
        timeout: Per-request timeout in seconds
        resume: Whether to resume from existing extraction report
        dry_run: If True, list layers without extracting
        sort_hilbert: Whether to apply Hilbert spatial sorting
        raw: If True, skip auto-init (only create extraction files, no STAC catalog)
        no_styles: If True, skip style extraction from ESRI drawingInfo
        token: Optional ArcGIS token for authenticated endpoints
        recurse: Whether to recurse into sub-folders during discovery (default True)
    """

    workers: int = 3
    retries: int = 3
    timeout: float = 60.0
    resume: bool = False
    raw: bool = False
    dry_run: bool = False
    sort_hilbert: bool = True
    no_styles: bool = False
    token: str | None = None
    recurse: bool = True


@dataclass
class ExtractionProgress:
    """Progress callback data for extraction.

    Attributes:
        layer_index: Current layer index (0-based)
        total_layers: Total number of layers to extract
        layer_name: Name of current layer
        status: Current status ("starting", "extracting", "success", "failed", "skipped")
        error: Error message when status is "failed" (Issue #504).
    """

    layer_index: int
    total_layers: int
    layer_name: str
    status: str
    error: str | None = None


def _extract_single_layer(
    service_url: str,
    layer: LayerInfo,
    output_path: Path,
    options: ExtractionOptions,
) -> tuple[int, int, float]:
    """Extract a single layer using gpio.

    Args:
        service_url: Base service URL (without layer ID)
        layer: Layer info
        output_path: Path to write parquet file
        options: Extraction options

    Returns:
        Tuple of (feature_count, file_size_bytes, duration_seconds)

    Raises:
        Exception: If extraction fails after retries
    """
    import inspect

    import geoparquet_io as gpio  # type: ignore[import-untyped]

    layer_url = f"{service_url.rstrip('/')}/{layer.id}"
    start_time = time.monotonic()

    # Check which optional parameters gpio.extract_arcgis supports (feature-detected
    # so older gpio versions continue to work without modification).
    sig = inspect.signature(gpio.extract_arcgis)
    kwargs: dict[str, object] = {}
    if "max_workers" in sig.parameters:
        kwargs["max_workers"] = options.workers
    if options.token and "token" in sig.parameters:
        kwargs["token"] = options.token
    table = gpio.extract_arcgis(layer_url, **kwargs)

    # Apply Hilbert sorting if requested
    if options.sort_hilbert:
        table = table.sort_hilbert()

    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write to parquet
    table.write(str(output_path))

    duration = time.monotonic() - start_time
    # gpio.Table uses num_rows property instead of __len__
    feature_count = table.num_rows
    file_size = output_path.stat().st_size if output_path.exists() else 0

    return feature_count, file_size, duration


def _filter_discovered_layers(
    layers: list[LayerInfo],
    layer_filter: list[str] | None,
    layer_exclude: list[str] | None,
) -> list[LayerInfo]:
    """Apply include/exclude filters to discovered layers."""
    if not layer_filter and not layer_exclude:
        return layers

    layers_dicts: list[dict[str, int | str]] = [
        {"id": layer.id, "name": layer.name} for layer in layers
    ]
    filtered_dicts = filter_layers(layers_dicts, include=layer_filter, exclude=layer_exclude)
    filtered_ids = {d["id"] for d in filtered_dicts}
    return [layer for layer in layers if layer.id in filtered_ids]


def _get_resume_context(
    options: ExtractionOptions,
    report_path: Path,
) -> tuple[ResumeState | None, dict[int, LayerResult]]:
    """Get resume state and existing results if resuming.

    Returns:
        Tuple of (resume_state, existing_results). resume_state is a ResumeState
        object from get_resume_state() or None if not resuming.
    """
    if not options.resume or not report_path.exists():
        return None, {}

    existing_report = load_report(report_path)
    resume_state = get_resume_state(existing_report)
    existing_results = {r.id: r for r in existing_report.layers}
    return resume_state, existing_results


def _build_dry_run_report(
    url: str,
    discovery_result: ServiceDiscoveryResult,
    layers: list[LayerInfo],
) -> ExtractionReport:
    """Build a report for dry-run mode."""
    dry_run_results = [
        LayerResult(
            id=layer.id,
            name=layer.name,
            status="pending",
            features=0,
            size_bytes=0,
            duration_seconds=0.0,
            output_path="",
            warnings=[],
            error=None,
            attempts=0,
        )
        for layer in layers
    ]
    return _build_report(url=url, discovery_result=discovery_result, layer_results=dry_run_results)


def _extract_layers(
    url: str,
    output_dir: Path,
    layers: list[LayerInfo],
    options: ExtractionOptions,
    resume_state: ResumeState | None,
    existing_results: dict[int, LayerResult],
    on_progress: Callable[[ExtractionProgress], None] | None,
) -> list[LayerResult]:
    """Extract all layers and return results."""
    layer_results: list[LayerResult] = []
    retry_config = RetryConfig(max_attempts=options.retries)
    total = len(layers)

    for i, layer in enumerate(layers):
        result = _extract_one_layer(
            url,
            output_dir,
            layer,
            i,
            total,
            options,
            retry_config,
            resume_state,
            existing_results,
            on_progress,
        )
        layer_results.append(result)

    return layer_results


def _extract_one_layer(
    url: str,
    output_dir: Path,
    layer: LayerInfo,
    index: int,
    total: int,
    options: ExtractionOptions,
    retry_config: RetryConfig,
    resume_state: ResumeState | None,
    existing_results: dict[int, LayerResult],
    on_progress: Callable[[ExtractionProgress], None] | None,
) -> LayerResult:
    """Extract a single layer and return its result."""
    layer_slug = _slugify(layer.name)
    _emit_progress(on_progress, index, total, layer.name, "starting")

    # Check resume state - skip if already succeeded
    if resume_state and not should_process_layer(layer.id, resume_state):
        if layer.id in existing_results:
            _emit_progress(on_progress, index, total, layer.name, "skipped")
            return existing_results[layer.id]
        # Resume state says skip, but we have no cached result - re-extract with warning
        logger.warning(
            "Layer '%s' (id=%d) marked complete in resume state but result missing; re-extracting",
            layer.name,
            layer.id,
        )

    # Build output path: collection_name/collection_name.parquet
    collection_dir = output_dir / layer_slug
    output_path = collection_dir / f"{layer_slug}.parquet"

    # Extract with retry
    _emit_progress(on_progress, index, total, layer.name, "extracting")

    result = retry_with_backoff(
        _extract_single_layer,
        retry_config,
        url,
        layer,
        output_path,
        options,
        on_retry=lambda attempt, err: None,
    )

    if result.success:
        features, size_bytes, duration = result.value  # type: ignore[misc]
        _emit_progress(on_progress, index, total, layer.name, "success")

        # Extract style from ESRI layer (Issue #490)
        if not options.no_styles:
            layer_url = f"{url.rstrip('/')}/{layer.id}"
            style_result = extract_esri_style(
                layer_url=layer_url,
                collection_path=collection_dir,
                source_layer=layer_slug,
            )
            if style_result:
                logger.debug("Extracted style for %s: %s", layer.name, style_result.path)

        return LayerResult(
            id=layer.id,
            name=layer.name,
            status="success",
            features=features,
            size_bytes=size_bytes,
            duration_seconds=duration,
            output_path=str(output_path.relative_to(output_dir)),
            warnings=[],
            error=None,
            attempts=result.attempts,
        )

    _emit_progress(on_progress, index, total, layer.name, "failed")
    return LayerResult(
        id=layer.id,
        name=layer.name,
        status="failed",
        features=0,
        size_bytes=0,
        duration_seconds=0.0,
        output_path="",
        warnings=[],
        error=str(result.error) if result.error else "Unknown error",
        attempts=result.attempts,
    )


def extract_arcgis_catalog(
    url: str,
    output_dir: Path,
    *,
    layer_filter: list[str] | None = None,
    layer_exclude: list[str] | None = None,
    service_filter: list[str] | None = None,
    service_exclude: list[str] | None = None,
    options: ExtractionOptions | None = None,
    on_progress: Callable[[ExtractionProgress], None] | None = None,
) -> ExtractionReport:
    """Extract layers from an ArcGIS service to a Portolan catalog.

    This is the main orchestration function that:
    1. Parses the URL to determine service type
    2. Discovers available layers (or services for services root)
    3. Applies filters
    4. Handles resume logic
    5. Extracts each layer with retry
    6. Generates extraction report

    Args:
        url: ArcGIS FeatureServer, MapServer, or services root URL
        output_dir: Directory to write extracted data
        layer_filter: Glob patterns to include layers (if None, include all)
        layer_exclude: Glob patterns to exclude layers
        service_filter: Glob patterns to include services (for services root URLs)
        service_exclude: Glob patterns to exclude services (for services root URLs)
        options: Extraction options (defaults to ExtractionOptions())
        on_progress: Callback for progress updates

    Returns:
        ExtractionReport with results for all layers

    Raises:
        ValueError: If URL is invalid
        ArcGISDiscoveryError: If service discovery fails
    """
    if options is None:
        options = ExtractionOptions()

    # Parse URL
    parsed = parse_arcgis_url(url)

    # Handle services root and folder URLs differently
    if parsed.url_type in (ArcGISURLType.SERVICES_ROOT, ArcGISURLType.SERVICES_FOLDER):
        return _extract_services_root(
            url=url,
            parsed=parsed,
            output_dir=output_dir,
            layer_filter=layer_filter,
            layer_exclude=layer_exclude,
            service_filter=service_filter,
            service_exclude=service_exclude,
            options=options,
            on_progress=on_progress,
        )

    # Single service extraction (FeatureServer or MapServer)
    discovery_result = discover_layers(url, timeout=options.timeout)

    # Apply layer filters
    layers = _filter_discovered_layers(discovery_result.layers, layer_filter, layer_exclude)

    # Handle resume state
    report_path = output_dir / ".portolan" / "extraction-report.json"
    resume_state, existing_results = _get_resume_context(options, report_path)

    # Dry run - just return what would be extracted
    if options.dry_run:
        return _build_dry_run_report(url, discovery_result, layers)

    # Create output directory structure
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / ".portolan").mkdir(exist_ok=True)

    # Extract each layer
    layer_results = _extract_layers(
        url, output_dir, layers, options, resume_state, existing_results, on_progress
    )

    # Build and save report
    report = _build_report(
        url=url,
        discovery_result=discovery_result,
        layer_results=layer_results,
    )
    save_report(report, report_path)

    # Auto-init catalog unless raw mode
    if not options.raw:
        _auto_init_catalog(output_dir, report)

    return report


def _auto_init_catalog(output_dir: Path, report: ExtractionReport) -> None:
    """Initialize a Portolan catalog and add extracted files.

    Called automatically after extraction unless raw=True.
    Creates catalog.json, config.yaml, and collection.json for each layer.
    Also seeds metadata.yaml from extracted service metadata and adds
    provenance links (Issue #353).

    Per Issue #369: Propagates rich metadata from ArcGIS service to STAC files,
    avoiding generic placeholders.
    """
    from portolan_cli.catalog import init_catalog
    from portolan_cli.dataset import add_files
    from portolan_cli.stac import update_stac_metadata

    # Get list of successfully extracted parquet files
    parquet_files = [
        output_dir / result.output_path
        for result in report.layers
        if result.status == "success" and result.output_path
    ]

    if not parquet_files:
        return  # Nothing to add

    # Initialize the catalog
    # Extract title from service metadata if available (Issue #369)
    title = None
    description = None
    if report.metadata_extracted:
        # Use description from service metadata
        description = report.metadata_extracted.description

        # Use service name from URL as title
        if report.metadata_extracted.source_url:
            from portolan_cli.extract.arcgis.url_parser import parse_arcgis_url

            try:
                parsed = parse_arcgis_url(report.metadata_extracted.source_url)
                title = parsed.service_name
            except ValueError:
                pass

    # Filter technical names BEFORE init_catalog to avoid writing them
    # (update_stac_metadata can't overwrite if init_catalog already wrote them)
    from portolan_cli.stac import is_technical_name

    filtered_title = None if is_technical_name(title) else title
    filtered_description = None if is_technical_name(description) else description

    init_catalog(output_dir, title=filtered_title, description=filtered_description)

    # Per Issue #369: Update catalog.json with rich metadata
    # This handles cases where init_catalog used defaults
    catalog_path = output_dir / "catalog.json"
    update_stac_metadata(catalog_path, title=title, description=description)

    # Add all extracted parquet files
    add_files(
        paths=parquet_files,
        catalog_root=output_dir,
    )

    # Register extracted styles as STAC assets (Issue #490)
    from portolan_cli.style import discover_styles, register_style_assets

    for result in report.layers:
        if result.status == "success" and result.output_path:
            collection_dir = output_dir / Path(result.output_path).parent
            styles = discover_styles(collection_dir)
            if styles:
                register_style_assets(collection_dir, styles)
                logger.debug("Registered %d style(s) for %s", len(styles), result.name)

    # Seed metadata.yaml from extracted service metadata
    _seed_metadata_from_extraction(output_dir, report)

    # Add via links for provenance tracking (Issue #353)
    _add_via_links_to_collections(output_dir, report)

    # Seed collection-level metadata.yaml with layer details
    # and update collection.json with rich metadata (Issue #369)
    _seed_collection_metadata_arcgis(output_dir, report)


def _seed_metadata_from_extraction(output_dir: Path, report: ExtractionReport) -> None:
    """Seed metadata.yaml from extracted service metadata.

    Called after catalog initialization to pre-populate metadata.yaml with
    values extracted from the ArcGIS service. Fields that couldn't be
    extracted are marked with TODO placeholders.

    Args:
        output_dir: The catalog output directory.
        report: The extraction report containing metadata.
    """
    from portolan_cli.metadata_seeding import seed_metadata_yaml
    from portolan_cli.output import info

    if not report.metadata_extracted:
        return

    # Convert MetadataExtracted from report to ArcGISMetadata, then to ExtractedMetadata
    # We need to reconstruct ArcGISMetadata from the report data
    arcgis_metadata = _report_metadata_to_arcgis_metadata(report.metadata_extracted)
    extracted = arcgis_metadata.to_extracted()

    metadata_path = output_dir / ".portolan" / "metadata.yaml"
    if seed_metadata_yaml(extracted, metadata_path):
        info(f"Seeded metadata.yaml from {extracted.source_type}")


def _report_metadata_to_arcgis_metadata(
    report_metadata: MetadataExtracted,
) -> ArcGISMetadata:
    """Convert report MetadataExtracted to ArcGISMetadata.

    The extraction report stores a flattened version of the metadata.
    This function reconstructs the ArcGISMetadata object for conversion
    to ExtractedMetadata.

    Args:
        report_metadata: MetadataExtracted from the extraction report.

    Returns:
        ArcGISMetadata instance with the same data.
    """
    from portolan_cli.extract.arcgis.metadata import ArcGISMetadata

    return ArcGISMetadata(
        source_url=report_metadata.source_url,
        attribution=report_metadata.attribution,
        description=report_metadata.description,
        processing_notes=report_metadata.processing_notes,
        contact_name=report_metadata.contact_name,
        keywords=report_metadata.keywords,
        known_issues=report_metadata.known_issues,
        license_info_raw=report_metadata.license_info_raw,
    )


def _add_via_links_to_collections(output_dir: Path, report: ExtractionReport) -> None:
    """Add via provenance links to each extracted collection.

    Per Issue #353: Each collection should have a `via` link pointing to
    the original data source (ArcGIS layer URL).

    Args:
        output_dir: The catalog output directory.
        report: The extraction report with layer info and source URL.
    """
    from portolan_cli.stac import add_via_link

    source_url = report.source_url

    for layer in report.layers:
        if layer.status != "success" or not layer.output_path:
            continue

        # Derive collection directory from output_path's parent
        # Handles nested paths like "service/layer/layer.parquet"
        collection_dir = output_dir / Path(layer.output_path).parent
        collection_path = collection_dir / "collection.json"

        if not collection_path.exists():
            continue

        # Build layer-specific URL: service_url + "/" + layer_id
        # This gives more specific provenance than just the service URL
        layer_url = f"{source_url.rstrip('/')}/{layer.id}"

        add_via_link(
            collection_path,
            layer_url,
            title=f"Source ArcGIS layer: {layer.name}",
        )


def _seed_collection_metadata_arcgis(
    output_dir: Path,
    report: ExtractionReport,
    timeout: float = 60.0,
) -> None:
    """Seed metadata.yaml for each collection with ArcGIS layer-specific info.

    Fetches layer details from ArcGIS API to get description for each layer.
    Falls back gracefully if layer details fetch fails.

    Args:
        output_dir: The catalog output directory.
        report: The extraction report with layer results.
        timeout: Request timeout for layer detail fetches.
    """
    from portolan_cli.extract.arcgis.discovery import fetch_layer_details
    from portolan_cli.extract.common.metadata_seeding import seed_collection_metadata

    source_url = report.source_url

    for layer_result in report.layers:
        if layer_result.status != "success" or not layer_result.output_path:
            continue

        # Derive collection directory from output_path's parent
        # Handles nested paths like "service/layer/layer.parquet"
        collection_dir = output_dir / Path(layer_result.output_path).parent

        # Fetch layer details to get description
        layer_description = None
        layer_name = layer_result.name
        try:
            layer_details = fetch_layer_details(source_url, layer_result.id, timeout=timeout)
            layer_description = layer_details.get("description")
            layer_name = layer_details.get("name", layer_result.name)
        except Exception as e:
            logger.debug(
                "Failed to fetch layer details for %s (id=%s) from %s: %s",
                layer_result.name,
                layer_result.id,
                source_url,
                e,
            )

        layer_url = f"{source_url.rstrip('/')}/{layer_result.id}"

        seed_collection_metadata(
            collection_dir,
            source_type="arcgis_featureserver",
            source_url=layer_url,
            layer_name=layer_name,
            title=layer_name,
            description=layer_description,
        )


def _discover_and_filter_services(
    url: str,
    service_filter: list[str] | None,
    service_exclude: list[str] | None,
    timeout: float,
    *,
    token: str | None = None,
    folder: str | None = None,
    recurse: bool = True,
) -> tuple[list[ServiceInfo], FolderCoverage | None]:
    """Discover services (recursively by default), scope to a folder, and apply filters.

    Returns (services, coverage). When recurse is False the flat (non-recursive)
    discovery is used and coverage is None. When folder is set (SERVICES_FOLDER
    URL) only services under that folder prefix are kept.
    """
    from portolan_cli.extract.arcgis.filters import filter_services

    coverage: FolderCoverage | None
    if recurse:
        services, traversal = discover_services_recursive(
            url,
            service_types=["FeatureServer", "MapServer"],
            token=token,
            timeout=timeout,
        )
        coverage = _coverage_from_traversal(traversal)
    else:
        services, _folders = discover_services(
            url,
            service_types=["FeatureServer", "MapServer"],
            return_folders=True,
            timeout=timeout,
            token=token,
        )
        coverage = None

    if folder:
        prefix = f"{folder.rstrip('/')}/"
        services = [s for s in services if s.name.startswith(prefix)]

    if service_filter or service_exclude:
        service_names = [s.name for s in services]
        filtered_names = filter_services(
            service_names,
            include=service_filter,
            exclude=service_exclude,
            case_sensitive=False,
        )
        services = [s for s in services if s.name in filtered_names]

    return services, coverage


def _collect_layers_from_services(
    services: list[ServiceInfo],
    base_url: str,
    timeout: float,
) -> tuple[list[LayerInfo], dict[int, ServiceInfo], dict[str, int], list[tuple[str, str]]]:
    """Collect all layers from multiple services.

    Returns:
        Tuple of (all_layers, service_for_layer, layer_count_per_service, discovery_errors).
        - all_layers: Flat list of all discovered layers
        - service_for_layer: Maps layer index to its source service
        - layer_count_per_service: Maps service name to total layer count (for flatten logic)
        - discovery_errors: List of (service_name, error_message) tuples
    """
    all_layers: list[LayerInfo] = []
    service_for_layer: dict[int, ServiceInfo] = {}
    layer_count_per_service: dict[str, int] = {}
    discovery_errors: list[tuple[str, str]] = []

    for service in services:
        service_url = service.get_url(base_url)
        try:
            service_discovery = discover_layers(service_url, timeout=timeout)
            # Track layer count for this service (for flatten logic)
            layer_count_per_service[service.name] = len(service_discovery.layers)
            for layer in service_discovery.layers:
                layer_idx = len(all_layers)
                all_layers.append(layer)
                service_for_layer[layer_idx] = service
        except Exception as e:
            error_msg = str(e)
            discovery_errors.append((service.name, error_msg))
            logger.warning(
                "Failed to discover layers from service '%s': %s",
                service.name,
                error_msg,
            )
            continue  # Skip services that fail to discover

    return all_layers, service_for_layer, layer_count_per_service, discovery_errors


def _filter_layers_by_index(
    all_layers: list[LayerInfo],
    layer_filter: list[str] | None,
    layer_exclude: list[str] | None,
) -> list[tuple[int, LayerInfo]]:
    """Filter layers and return (index, layer) tuples."""
    if not layer_filter and not layer_exclude:
        return list(enumerate(all_layers))

    layers_dicts: list[dict[str, int | str]] = [
        {"id": i, "name": layer.name} for i, layer in enumerate(all_layers)
    ]
    filtered_dicts = filter_layers(
        layers_dicts,
        include=layer_filter,
        exclude=layer_exclude,
    )
    filtered_indices = {d["id"] for d in filtered_dicts}
    return [(i, layer) for i, layer in enumerate(all_layers) if i in filtered_indices]


def _get_services_root_resume_context(
    options: ExtractionOptions,
    report_path: Path,
) -> dict[str, LayerResult]:
    """Get resume context for services root extraction.

    For services root, we use output_path as the unique identifier since
    layer IDs are not unique across services (multiple services can have layer 0).

    Returns:
        Dict mapping output_path to LayerResult for succeeded layers.
    """
    if not options.resume or not report_path.exists():
        return {}

    existing_report = load_report(report_path)
    # Map by output_path (unique across services) for succeeded/skipped layers
    return {
        r.output_path: r
        for r in existing_report.layers
        if r.status in ("success", "skipped") and r.output_path
    }


def _extract_services_root(
    url: str,
    parsed: ParsedArcGISURL,
    output_dir: Path,
    *,
    layer_filter: list[str] | None = None,
    layer_exclude: list[str] | None = None,
    service_filter: list[str] | None = None,
    service_exclude: list[str] | None = None,
    options: ExtractionOptions | None = None,
    on_progress: Callable[[ExtractionProgress], None] | None = None,
) -> ExtractionReport:
    """Extract from a services root URL.

    Services root URLs create a nested catalog structure:
    - Root = Catalog
    - Services = Sub-catalogs
    - Layers = Collections

    Supports resume by tracking output_path (unique across services) rather than
    layer ID (which can repeat across services).
    """
    if options is None:
        options = ExtractionOptions()

    # Discover and filter services
    services, coverage = _discover_and_filter_services(
        url,
        service_filter,
        service_exclude,
        options.timeout,
        token=options.token,
        folder=parsed.folder,
        recurse=options.recurse,
    )

    # Collect layers from all services
    all_layers, service_for_layer, layer_count_per_service, _discovery_errors = (
        _collect_layers_from_services(services, parsed.base_url, options.timeout)
    )

    # Apply layer filters
    filtered_layers = _filter_layers_by_index(all_layers, layer_filter, layer_exclude)

    # Dry run - just return what would be extracted
    if options.dry_run:
        dry_run_results = [
            LayerResult(
                id=layer.id,
                name=layer.name,
                status="pending",
                features=0,
                size_bytes=0,
                duration_seconds=0.0,
                output_path="",
                warnings=[],
                error=None,
                attempts=0,
            )
            for _idx, layer in filtered_layers
        ]
        # Create a minimal discovery result for the report
        combined_discovery = ServiceDiscoveryResult(
            layers=[layer for _, layer in filtered_layers],
        )
        dry_report = _build_report(
            url=url,
            discovery_result=combined_discovery,
            layer_results=dry_run_results,
        )
        dry_report.folder_coverage = coverage
        return dry_report

    # Create output directory structure
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / ".portolan").mkdir(exist_ok=True)

    # Handle resume state for services root
    report_path = output_dir / ".portolan" / "extraction-report.json"
    existing_results_by_path = _get_services_root_resume_context(options, report_path)

    # Extract each layer
    layer_results: list[LayerResult] = []
    retry_config = RetryConfig(max_attempts=options.retries)
    total = len(filtered_layers)

    for progress_idx, (layer_idx, layer) in enumerate(filtered_layers):
        service = service_for_layer[layer_idx]
        service_url = service.get_url(parsed.base_url)
        layer_slug = _slugify(layer.name)
        service_dir = _service_output_dir(output_dir, service.name)
        service_leaf_slug = service_dir.name

        # Determine output path based on layer count:
        # - Single-layer service: service_name/service_name.parquet (flattened - no subcatalog)
        # - Multi-layer service: service_name/layer_name/layer_name.parquet (nested)
        is_single_layer = layer_count_per_service.get(service.name, 0) == 1

        if is_single_layer:
            # Flatten: service becomes collection directly
            output_path = service_dir / f"{service_leaf_slug}.parquet"
        else:
            # Nested: service is subcatalog, layer is collection
            collection_dir = service_dir / layer_slug
            output_path = collection_dir / f"{layer_slug}.parquet"

        relative_output_path = output_path.relative_to(output_dir).as_posix()

        _emit_progress(on_progress, progress_idx, total, layer.name, "starting")

        # Check resume state - skip if already succeeded
        if relative_output_path in existing_results_by_path:
            existing_result = existing_results_by_path[relative_output_path]
            layer_results.append(existing_result)
            _emit_progress(on_progress, progress_idx, total, layer.name, "skipped")
            logger.debug(
                "Skipping already-completed layer: %s/%s",
                service.name,
                layer.name,
            )
            continue

        # Extract with retry
        _emit_progress(on_progress, progress_idx, total, layer.name, "extracting")

        result = retry_with_backoff(
            _extract_single_layer,
            retry_config,
            service_url,
            layer,
            output_path,
            options,
            on_retry=lambda attempt, err: None,  # Silent retries
        )

        if result.success:
            features, size_bytes, duration = result.value  # type: ignore[misc]

            # Extract style from ESRI layer (Issue #490)
            if not options.no_styles:
                layer_url = f"{service_url}/{layer.id}"
                # collection_dir is service_dir for single-layer, or nested dir for multi
                coll_dir = service_dir if is_single_layer else service_dir / layer_slug
                style_result = extract_esri_style(
                    layer_url=layer_url,
                    collection_path=coll_dir,
                    source_layer=layer_slug,
                )
                if style_result:
                    logger.debug("Extracted style for %s: %s", layer.name, style_result.path)

            layer_results.append(
                LayerResult(
                    id=layer.id,
                    name=layer.name,
                    status="success",
                    features=features,
                    size_bytes=size_bytes,
                    duration_seconds=duration,
                    output_path=relative_output_path,
                    warnings=[],
                    error=None,
                    attempts=result.attempts,
                )
            )
            _emit_progress(on_progress, progress_idx, total, layer.name, "success")
        else:
            layer_results.append(
                LayerResult(
                    id=layer.id,
                    name=layer.name,
                    status="failed",
                    features=0,
                    size_bytes=0,
                    duration_seconds=0.0,
                    output_path="",
                    warnings=[],
                    error=str(result.error) if result.error else "Unknown error",
                    attempts=result.attempts,
                )
            )
            _emit_progress(on_progress, progress_idx, total, layer.name, "failed")

    # Build and save report
    combined_discovery = ServiceDiscoveryResult(
        layers=[layer for _, layer in filtered_layers],
    )
    report = _build_report(
        url=url,
        discovery_result=combined_discovery,
        layer_results=layer_results,
    )
    report.folder_coverage = coverage
    report_path = output_dir / ".portolan" / "extraction-report.json"
    save_report(report, report_path)

    # Auto-init catalog unless raw mode
    if not options.raw:
        _auto_init_catalog(output_dir, report)

    return report


def _build_report(
    url: str,
    discovery_result: ServiceDiscoveryResult,
    layer_results: list[LayerResult],
) -> ExtractionReport:
    """Build an ExtractionReport from extraction results."""
    # Get versions
    try:
        from importlib.metadata import version

        portolan_version = version("portolan-cli")
    except Exception:
        portolan_version = "unknown"

    try:
        from importlib.metadata import version

        gpio_version = version("geoparquet-io")
    except Exception:
        gpio_version = "unknown"

    # Extract metadata
    arcgis_metadata = extract_arcgis_metadata(
        {
            "copyrightText": discovery_result.copyright_text,
            "description": discovery_result.description,
            "serviceDescription": discovery_result.service_description,
            "documentInfo": {
                "Author": discovery_result.author,
                "Keywords": discovery_result.keywords,
            },
            "accessInformation": discovery_result.access_information,
            "licenseInfo": discovery_result.license_info,
        },
        source_url=url,
    )

    metadata_extracted = MetadataExtracted(
        source_url=url,
        description=arcgis_metadata.description,
        attribution=arcgis_metadata.attribution,
        keywords=arcgis_metadata.keywords,
        contact_name=arcgis_metadata.contact_name,
        processing_notes=arcgis_metadata.processing_notes,
        known_issues=arcgis_metadata.known_issues,
        license_info_raw=arcgis_metadata.license_info_raw,
    )

    # Calculate summary
    succeeded = sum(1 for r in layer_results if r.status == "success")
    failed = sum(1 for r in layer_results if r.status == "failed")
    skipped = sum(1 for r in layer_results if r.status == "skipped")
    empty = sum(1 for r in layer_results if r.status == "empty")

    summary = ExtractionSummary(
        total_layers=len(layer_results),
        succeeded=succeeded,
        failed=failed,
        skipped=skipped,
        empty=empty,
        total_features=sum(r.features or 0 for r in layer_results),
        total_size_bytes=sum(r.size_bytes or 0 for r in layer_results),
        total_duration_seconds=sum(r.duration_seconds or 0.0 for r in layer_results),
    )

    return ExtractionReport(
        extraction_date=datetime.now(timezone.utc).isoformat(),
        source_url=url,
        portolan_version=portolan_version,
        gpio_version=gpio_version,
        metadata_extracted=metadata_extracted,
        layers=layer_results,
        summary=summary,
    )
