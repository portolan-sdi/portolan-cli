"""WFS extraction orchestrator.

This module ties together WFS extraction components:
- Discovery → Filtering → Extraction → Report generation

The orchestrator is the main entry point for `portolan extract wfs`.
Unlike ArcGIS, WFS is always a single service endpoint (no services root).

Typical usage:
    from portolan_cli.extract.wfs.orchestrator import extract_wfs_catalog

    result = extract_wfs_catalog(
        url="https://example.com/wfs",
        output_dir=Path("./output"),
        layer_filter=["buildings*"],
    )
    print(f"Extracted {result.summary.succeeded}/{result.summary.total_layers} layers")
"""

from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

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
from portolan_cli.extract.wfs.discovery import LayerInfo, WFSDiscoveryResult, discover_layers

if TYPE_CHECKING:
    from collections.abc import Callable

    from portolan_cli.extract.csw.models import ISOMetadata
    from portolan_cli.extract.wfs.metadata import WFSMetadata

logger = logging.getLogger(__name__)


@dataclass
class ExtractionOptions:
    """Options for WFS extraction.

    Attributes:
        workers: Number of parallel workers for layer extraction.
            Each layer is extracted independently; this controls how many
            layers are processed concurrently.
        retries: Number of retry attempts per failed layer.
        timeout: Per-layer timeout in seconds. Note: geoparquet-io uses a
            10-minute internal HTTP timeout for large datasets; this timeout
            wraps the entire layer extraction including retries.
        resume: Whether to resume from existing extraction report.
        raw: If True, skip auto-init (only create extraction files, no STAC catalog).
        dry_run: If True, list layers without extracting.
        wfs_version: WFS version ("1.0.0", "1.1.0", "2.0.0", or "auto").
            When "auto", the version is negotiated with the server once and
            used consistently for both discovery and extraction.
        output_crs: Target CRS for output (e.g., "EPSG:4326"). None keeps source CRS.
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in output CRS.
        limit: Maximum features per layer (None for unlimited).
        page_size: Features per page when gpio uses parallel pagination for
            very large layers (1M+ features). Default: 10000.
    """

    workers: int = 1
    retries: int = 3
    timeout: float = 300.0  # 5 minutes per layer (gpio has 10min internal timeout)
    resume: bool = False
    raw: bool = False
    dry_run: bool = False
    wfs_version: str = "auto"
    output_crs: str | None = None
    bbox: tuple[float, float, float, float] | None = None
    limit: int | None = None
    page_size: int = 10000


@dataclass
class ExtractionProgress:
    """Progress callback data for extraction.

    Attributes:
        layer_index: Current layer index (0-based).
        total_layers: Total number of layers to extract.
        layer_name: Name of current layer.
        status: Current status ("starting", "extracting", "success", "failed", "skipped").
    """

    layer_index: int
    total_layers: int
    layer_name: str
    status: str


def _slugify(name: str, disambiguate: bool = False, unique_id: int | None = None) -> str:
    """Convert a name to a filesystem-safe slug.

    Args:
        name: Original name (e.g., "ns:FeatureType")
        disambiguate: If True, append a short hash to prevent collisions.
        unique_id: Unique identifier to include in hash (required when disambiguating
            identical names that would otherwise produce identical hashes).

    Returns:
        Slugified name (e.g., "ns_featuretype" or "ns_featuretype_a1b2c3")
    """
    import hashlib

    slug = name.lower()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    slug = re.sub(r"_+", "_", slug)
    slug = slug.strip("_")
    slug = slug or "unnamed"

    if disambiguate:
        hash_input = f"{name}:{unique_id}" if unique_id is not None else name
        name_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:6]
        slug = f"{slug}_{name_hash}"

    return slug


def _assign_slugs(layers: list[LayerInfo]) -> dict[int, str]:
    """Pre-compute slugs for all layers, disambiguating only on collision.

    Args:
        layers: List of layers to assign slugs to.

    Returns:
        Dict mapping layer id to assigned slug.
    """
    from collections import Counter

    base_slugs = {layer.id: _slugify(layer.name, disambiguate=False) for layer in layers}
    slug_counts = Counter(base_slugs.values())

    result: dict[int, str] = {}
    for layer in layers:
        base_slug = base_slugs[layer.id]
        if slug_counts[base_slug] > 1:
            result[layer.id] = _slugify(layer.name, disambiguate=True, unique_id=layer.id)
        else:
            result[layer.id] = base_slug

    return result


def _build_wfs_url(base_url: str, params: dict[str, str]) -> str:
    """Build a WFS URL by merging params into base_url.

    Properly handles existing query params and URL-encodes values.

    Args:
        base_url: Base WFS service URL (may already have query params).
        params: Parameters to add/override (e.g., service, request, typename).

    Returns:
        Complete URL with merged and encoded parameters.
    """
    parsed = urlparse(base_url)
    existing_params = dict(parse_qsl(parsed.query))
    existing_params.update(params)
    new_query = urlencode(existing_params)
    return urlunparse(parsed._replace(query=new_query))


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


def _extract_single_layer(
    service_url: str,
    layer: LayerInfo,
    output_path: Path,
    options: ExtractionOptions,
    negotiated_version: str,
) -> tuple[int, int, float]:
    """Extract a single WFS layer using geoparquet-io.

    Args:
        service_url: WFS service URL.
        layer: Layer info (typename used for extraction).
        output_path: Path to write parquet file.
        options: Extraction options.
        negotiated_version: WFS version to use (already negotiated if "auto").

    Returns:
        Tuple of (feature_count, file_size_bytes, duration_seconds).

    Raises:
        Exception: If extraction fails.
    """
    from geoparquet_io.core.wfs import convert_wfs_to_geoparquet  # type: ignore[import-untyped]

    start_time = time.monotonic()

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Use gpio's convert_wfs_to_geoparquet which handles everything:
    # - HTTP streaming via DuckDB
    # - CRS negotiation
    # - Hilbert sorting and bbox column
    convert_wfs_to_geoparquet(
        service_url=service_url,
        typename=layer.typename,
        output_file=str(output_path),
        version=negotiated_version,
        bbox=options.bbox,
        output_crs=options.output_crs,
        limit=options.limit,
        page_size=options.page_size,
        overwrite=True,  # We control overwrites via resume logic
    )

    duration = time.monotonic() - start_time

    # Read back file stats
    if output_path.exists():
        import pyarrow.parquet as pq

        metadata = pq.read_metadata(output_path)
        feature_count = metadata.num_rows
        file_size = output_path.stat().st_size
    else:
        feature_count = 0
        file_size = 0

    return feature_count, file_size, duration


def _build_dry_run_report(
    url: str,
    layers: list[LayerInfo],
    discovery_result: WFSDiscoveryResult | None = None,
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
    return _build_report(url=url, layer_results=dry_run_results, discovery_result=discovery_result)


def _get_package_version(package_name: str) -> str:
    """Get installed version of a package."""
    try:
        from importlib.metadata import version

        return version(package_name)
    except Exception:
        return "unknown"


def _build_metadata(url: str, discovery_result: WFSDiscoveryResult | None) -> MetadataExtracted:
    """Build MetadataExtracted from discovery result."""
    if not discovery_result:
        return MetadataExtracted(
            source_url=url,
            description=None,
            attribution=None,
            keywords=None,
            contact_name=None,
            processing_notes=None,
            known_issues=None,
            license_info_raw=None,
        )

    processing_notes = None
    if discovery_result.service_title:
        processing_notes = f"WFS service: {discovery_result.service_title}"

    return MetadataExtracted(
        source_url=url,
        description=discovery_result.service_abstract,
        attribution=discovery_result.provider,
        keywords=discovery_result.keywords,
        contact_name=discovery_result.contact_name,
        processing_notes=processing_notes,
        known_issues=None,
        license_info_raw=discovery_result.access_constraints,
    )


def _build_summary(layer_results: list[LayerResult]) -> ExtractionSummary:
    """Compute extraction summary from layer results."""
    return ExtractionSummary(
        total_layers=len(layer_results),
        succeeded=sum(1 for r in layer_results if r.status == "success"),
        failed=sum(1 for r in layer_results if r.status == "failed"),
        skipped=sum(1 for r in layer_results if r.status == "skipped"),
        total_features=sum(r.features or 0 for r in layer_results),
        total_size_bytes=sum(r.size_bytes or 0 for r in layer_results),
        total_duration_seconds=sum(r.duration_seconds or 0.0 for r in layer_results),
    )


def _build_report(
    url: str,
    layer_results: list[LayerResult],
    discovery_result: WFSDiscoveryResult | None = None,
) -> ExtractionReport:
    """Build an ExtractionReport from extraction results."""
    return ExtractionReport(
        extraction_date=datetime.now(timezone.utc).isoformat(),
        source_url=url,
        portolan_version=_get_package_version("portolan-cli"),
        gpio_version=_get_package_version("geoparquet-io"),
        metadata_extracted=_build_metadata(url, discovery_result),
        layers=layer_results,
        summary=_build_summary(layer_results),
    )


def _negotiate_version(url: str, wfs_version: str) -> str:
    """Negotiate WFS version with the server.

    Args:
        url: WFS service URL.
        wfs_version: User-requested version ("auto" or specific version).

    Returns:
        Negotiated version string (e.g., "1.1.0", "2.0.0").
    """
    if wfs_version != "auto":
        return wfs_version

    try:
        from geoparquet_io.core.wfs import negotiate_wfs_version

        version, _ = negotiate_wfs_version(url, preferred_version="auto")
        return str(version)
    except ImportError:
        # Fallback if gpio doesn't have negotiate_wfs_version
        return "1.1.0"
    except Exception as e:
        logger.warning("Version negotiation failed, using 1.1.0: %s", e)
        return "1.1.0"


def _extract_layers_parallel(
    url: str,
    layers_to_extract: list[tuple[int, LayerInfo]],
    output_dir: Path,
    options: ExtractionOptions,
    negotiated_version: str,
    total: int,
    layer_slugs: dict[int, str],
    on_progress: Callable[[ExtractionProgress], None] | None,
) -> list[LayerResult]:
    """Extract multiple layers in parallel using ThreadPoolExecutor."""
    actual_workers = min(options.workers, len(layers_to_extract))
    logger.info("Extracting %d layers with %d workers", len(layers_to_extract), actual_workers)
    results: list[LayerResult] = []

    with ThreadPoolExecutor(max_workers=actual_workers) as executor:
        future_to_layer = {
            executor.submit(
                _extract_layer_task,
                url,
                layer,
                output_dir,
                options,
                negotiated_version,
                i,
                total,
                layer_slugs[layer.id],
            ): (i, layer)
            for i, layer in layers_to_extract
        }

        for future in as_completed(future_to_layer):
            i, layer = future_to_layer[future]
            try:
                result = future.result(timeout=options.timeout)
                results.append(result)
                status = "success" if result.status == "success" else "failed"
                _emit_progress(on_progress, i, total, layer.name, status)
            except TimeoutError:
                logger.error("Layer %s timed out after %ds", layer.name, options.timeout)
                _emit_progress(on_progress, i, total, layer.name, "failed")
                results.append(
                    LayerResult(
                        id=layer.id,
                        name=layer.name,
                        status="failed",
                        features=0,
                        size_bytes=0,
                        duration_seconds=options.timeout,
                        output_path="",
                        warnings=[],
                        error=f"Timeout after {options.timeout}s",
                        attempts=1,
                    )
                )
            except Exception as e:
                logger.error("Layer %s failed: %s", layer.name, e)
                _emit_progress(on_progress, i, total, layer.name, "failed")
                results.append(
                    LayerResult(
                        id=layer.id,
                        name=layer.name,
                        status="failed",
                        features=0,
                        size_bytes=0,
                        duration_seconds=0.0,
                        output_path="",
                        warnings=[],
                        error=str(e),
                        attempts=1,
                    )
                )
    return results


def _extract_layer_task(
    url: str,
    layer: LayerInfo,
    output_dir: Path,
    options: ExtractionOptions,
    negotiated_version: str,
    layer_index: int,
    total_layers: int,
    layer_slug: str,
) -> LayerResult:
    """Extract a single layer (task for parallel execution).

    Returns LayerResult with success or failure status.
    """
    collection_dir = output_dir / layer_slug
    output_path = collection_dir / f"{layer_slug}.parquet"

    retry_config = RetryConfig(max_attempts=options.retries)

    result = retry_with_backoff(
        _extract_single_layer,
        retry_config,
        url,
        layer,
        output_path,
        options,
        negotiated_version,
        on_retry=lambda attempt, err: logger.debug(
            "Retry %d for layer %s: %s", attempt, layer.name, err
        ),
    )

    if result.success:
        features, size_bytes, duration = result.value  # type: ignore[misc]
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
    else:
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


def extract_wfs_catalog(
    url: str,
    output_dir: Path,
    *,
    layer_filter: list[str] | None = None,
    layer_exclude: list[str] | None = None,
    options: ExtractionOptions | None = None,
    on_progress: Callable[[ExtractionProgress], None] | None = None,
) -> ExtractionReport:
    """Extract layers from a WFS service to a Portolan catalog.

    This is the main orchestration function that:
    1. Negotiates WFS version (if "auto")
    2. Discovers available layers via GetCapabilities
    3. Applies filters
    4. Handles resume logic
    5. Extracts layers in parallel with retry
    6. Generates extraction report

    Args:
        url: WFS service endpoint URL.
        output_dir: Directory to write extracted data.
        layer_filter: Glob patterns to include layers (if None, include all).
        layer_exclude: Glob patterns to exclude layers.
        options: Extraction options (defaults to ExtractionOptions()).
        on_progress: Callback for progress updates.

    Returns:
        ExtractionReport with results for all layers.

    Raises:
        WFSDiscoveryError: If service discovery fails.
    """
    if options is None:
        options = ExtractionOptions()

    # Negotiate WFS version ONCE - use same version for discovery AND extraction
    negotiated_version = _negotiate_version(url, options.wfs_version)
    logger.debug("Using WFS version: %s", negotiated_version)

    # Discover layers and service metadata
    discovery_result = discover_layers(url, version=negotiated_version)
    layers = discovery_result.layers

    # Apply layer filters
    layers = _filter_discovered_layers(layers, layer_filter, layer_exclude)

    # Dry run - just return what would be extracted
    if options.dry_run:
        return _build_dry_run_report(url, layers, discovery_result)

    # Create output directory structure
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / ".portolan").mkdir(exist_ok=True)

    # Handle resume state
    report_path = output_dir / ".portolan" / "extraction-report.json"
    resume_state: ResumeState | None = None
    existing_results: dict[str, LayerResult] = {}

    if options.resume and report_path.exists():
        from portolan_cli.extract.common.report import load_report

        existing_report = load_report(report_path)
        resume_state = get_resume_state(existing_report)
        existing_results = {r.name: r for r in existing_report.layers}

    # Separate layers into skip (already done) and extract (need processing)
    layers_to_extract: list[tuple[int, LayerInfo]] = []
    skipped_results: list[LayerResult] = []
    total = len(layers)

    for i, layer in enumerate(layers):
        if resume_state and not should_process_layer(layer.id, resume_state, layer_name=layer.name):
            if layer.name in existing_results:
                _emit_progress(on_progress, i, total, layer.name, "skipped")
                skipped_results.append(existing_results[layer.name])
                continue
            logger.warning(
                "Layer '%s' marked complete in resume state but result missing; re-extracting",
                layer.name,
            )
        layers_to_extract.append((i, layer))

    # Pre-compute slugs: only disambiguate on collision (Issue #379)
    layer_slugs = _assign_slugs([layer for _, layer in layers_to_extract])

    # Extract layers - parallel if workers > 1, sequential otherwise
    extracted_results: list[LayerResult] = []
    if options.workers > 1 and len(layers_to_extract) > 1:
        extracted_results = _extract_layers_parallel(
            url,
            layers_to_extract,
            output_dir,
            options,
            negotiated_version,
            total,
            layer_slugs,
            on_progress,
        )
    else:
        # Sequential extraction
        for i, layer in layers_to_extract:
            _emit_progress(on_progress, i, total, layer.name, "starting")
            _emit_progress(on_progress, i, total, layer.name, "extracting")

            result = _extract_layer_task(
                url,
                layer,
                output_dir,
                options,
                negotiated_version,
                i,
                total,
                layer_slugs[layer.id],
            )
            extracted_results.append(result)

            status = "success" if result.status == "success" else "failed"
            _emit_progress(on_progress, i, total, layer.name, status)

    # Combine results in original order
    all_results = skipped_results + extracted_results
    # Sort by layer id to maintain original discovery order
    all_results.sort(key=lambda r: r.id)

    # Build and save report
    report = _build_report(url=url, layer_results=all_results, discovery_result=discovery_result)
    save_report(report, report_path)

    # Auto-init catalog unless raw mode
    if not options.raw:
        _auto_init_catalog(output_dir, report, discovery_result)

    return report


def _auto_init_catalog(
    output_dir: Path,
    report: ExtractionReport,
    discovery_result: WFSDiscoveryResult | None = None,
) -> None:
    """Initialize a Portolan catalog and add extracted files.

    Called automatically after extraction unless raw=True.
    Creates catalog.json, config.yaml, and collection.json for each layer.
    Also adds provenance via links, seeds metadata.yaml from service metadata,
    and seeds collection-level metadata.yaml with layer info.

    Per Issue #369: Propagates rich metadata from WFS service to STAC files,
    avoiding generic placeholders.
    """
    from portolan_cli.catalog import add_files, init_catalog
    from portolan_cli.stac import update_stac_metadata

    parquet_files = [
        output_dir / result.output_path
        for result in report.layers
        if result.status == "success" and result.output_path
    ]

    if not parquet_files:
        return

    # Extract service title and description from discovery result (Issue #369)
    service_title = discovery_result.service_title if discovery_result else None
    service_description = discovery_result.service_abstract if discovery_result else None

    # Filter technical names AND boilerplate (Issue #376)
    from portolan_cli.extract.wfs.metadata import is_boilerplate_description
    from portolan_cli.stac import is_technical_name

    def _should_filter(text: str | None) -> bool:
        return is_technical_name(text) or is_boilerplate_description(text)

    filtered_title = None if _should_filter(service_title) else service_title
    filtered_description = None if _should_filter(service_description) else service_description

    init_catalog(output_dir, title=filtered_title, description=filtered_description)

    # Per Issue #369: Update catalog.json with rich metadata
    # Per Issue #376: Use filtered values to avoid overwriting with boilerplate
    catalog_path = output_dir / "catalog.json"
    update_stac_metadata(catalog_path, title=filtered_title, description=filtered_description)

    add_files(
        paths=parquet_files,
        catalog_root=output_dir,
    )

    # Add via links for provenance tracking
    _add_via_links_to_collections(output_dir, report)

    # Seed catalog-level metadata.yaml from extracted service metadata
    _seed_metadata_from_extraction(output_dir, report)

    # Seed collection-level metadata.yaml with layer-specific info
    # and update collection.json with rich metadata (Issue #369)
    if discovery_result:
        _seed_collection_metadata_wfs(output_dir, report, discovery_result)


def _add_via_links_to_collections(output_dir: Path, report: ExtractionReport) -> None:
    """Add via provenance links to each extracted collection.

    Each collection gets a `via` link pointing to a GetFeature-style URL
    for the original WFS layer.
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

        # Build GetFeature-style URL for provenance
        # WFS GetFeature URL pattern: service_url?service=WFS&request=GetFeature&typename=X
        layer_url = _build_wfs_url(
            source_url, {"service": "WFS", "request": "GetFeature", "typename": layer.name}
        )

        add_via_link(
            collection_path,
            layer_url,
            title=f"Source WFS layer: {layer.name}",
        )


def _seed_metadata_from_extraction(output_dir: Path, report: ExtractionReport) -> None:
    """Seed catalog-level metadata.yaml from extracted WFS service metadata.

    Called after catalog initialization to pre-populate metadata.yaml with
    values extracted from the WFS service. Fields that couldn't be
    extracted are marked with TODO placeholders.

    Args:
        output_dir: The catalog output directory.
        report: The extraction report containing metadata.
    """
    from portolan_cli.metadata_seeding import seed_metadata_yaml
    from portolan_cli.output import info

    if not report.metadata_extracted:
        return

    wfs_metadata = _report_metadata_to_wfs_metadata(report.metadata_extracted)
    extracted = wfs_metadata.to_extracted()

    metadata_path = output_dir / ".portolan" / "metadata.yaml"
    if seed_metadata_yaml(extracted, metadata_path):
        info(f"Seeded metadata.yaml from {extracted.source_type}")


def _report_metadata_to_wfs_metadata(
    report_metadata: MetadataExtracted,
) -> WFSMetadata:
    """Convert report MetadataExtracted back to WFSMetadata.

    The extraction report stores a flattened version of the metadata.
    This function reconstructs the WFSMetadata object for conversion
    to ExtractedMetadata.

    Args:
        report_metadata: MetadataExtracted from the extraction report.

    Returns:
        WFSMetadata instance with the same data.
    """
    from portolan_cli.extract.wfs.metadata import WFSMetadata

    return WFSMetadata(
        source_url=report_metadata.source_url,
        service_abstract=report_metadata.description,
        provider_name=report_metadata.attribution,
        keywords=report_metadata.keywords,
        access_constraints=report_metadata.license_info_raw,
    )


def _seed_collection_metadata_wfs(
    output_dir: Path,
    report: ExtractionReport,
    discovery_result: WFSDiscoveryResult,
    max_workers: int = 4,
) -> None:
    """Seed metadata.yaml for each collection with WFS layer-specific info.

    For layers with MetadataURL (e.g., INSPIRE services), fetches rich ISO 19139
    metadata from CSW in parallel. Falls back to GetCapabilities metadata if CSW
    fetch fails.

    Args:
        output_dir: The catalog output directory.
        report: The extraction report with layer results.
        discovery_result: WFS discovery result with layer metadata.
        max_workers: Maximum parallel CSW fetches (default: 4).
    """
    from portolan_cli.extract.common.metadata_seeding import seed_collection_metadata
    from portolan_cli.output import detail

    layer_info_by_name = {layer.name: layer for layer in discovery_result.layers}

    # Collect layers to process
    layers_to_process: list[tuple[LayerResult, LayerInfo]] = []
    for layer_result in report.layers:
        if layer_result.status != "success" or not layer_result.output_path:
            continue
        layer_info = layer_info_by_name.get(layer_result.name)
        if layer_info:
            layers_to_process.append((layer_result, layer_info))

    if not layers_to_process:
        return

    # Fetch ISO metadata in parallel
    iso_metadata_map: dict[str, ISOMetadata | None] = {}

    def fetch_iso(layer_info: LayerInfo) -> tuple[str, ISOMetadata | None]:
        return (layer_info.name, _try_fetch_iso_metadata(layer_info.metadata_urls))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_iso, layer_info): layer_info.name
            for _, layer_info in layers_to_process
        }
        for future in as_completed(futures):
            layer_name, iso_metadata = future.result()
            iso_metadata_map[layer_name] = iso_metadata

    # Seed metadata for each layer
    for layer_result, layer_info in layers_to_process:
        # Derive collection directory from output_path's parent
        # output_path is guaranteed non-None by the filter above, but check defensively
        if not layer_result.output_path:
            continue
        collection_dir = output_dir / Path(layer_result.output_path).parent

        # Build layer-specific URL for provenance
        layer_url = _build_wfs_url(
            report.source_url,
            {"service": "WFS", "request": "GetFeature", "typename": layer_info.name},
        )

        iso_metadata = iso_metadata_map.get(layer_info.name)

        if iso_metadata and iso_metadata.has_useful_metadata():
            # Use rich ISO metadata
            detail(f"Using ISO metadata for {layer_info.name}")
            _seed_collection_from_iso(
                collection_dir,
                iso_metadata,
                layer_url,
                layer_info.name,
            )
        else:
            # Fall back to sparse WFS GetCapabilities metadata
            seed_collection_metadata(
                collection_dir,
                source_type="wfs",
                source_url=layer_url,
                layer_name=layer_info.name,
                title=layer_info.title,
                description=layer_info.abstract,
                keywords=layer_info.keywords,
            )


def _try_fetch_iso_metadata(
    metadata_urls: list[dict[str, Any]] | None,
) -> ISOMetadata | None:
    """Try to fetch ISO 19139 metadata from MetadataURLs.

    Args:
        metadata_urls: List of metadata URL dicts from layer.

    Returns:
        Parsed ISOMetadata if successful, None otherwise.
    """
    if not metadata_urls:
        return None

    from portolan_cli.extract.csw import fetch_metadata_for_layer

    return fetch_metadata_for_layer(metadata_urls, timeout=30.0)


def _seed_collection_from_iso(
    collection_dir: Path,
    iso_metadata: ISOMetadata,
    source_url: str,
    layer_name: str,
) -> bool:
    """Seed collection metadata.yaml from ISO 19139 metadata.

    Also updates collection.json with title/description from ISO metadata
    per Issue #369 (propagate rich metadata to STAC, not just metadata.yaml).

    Args:
        collection_dir: Path to the collection directory.
        iso_metadata: Parsed ISO metadata.
        source_url: WFS layer URL for provenance.
        layer_name: Layer name for processing notes.

    Returns:
        True if metadata.yaml was created, False if skipped.
    """
    from dataclasses import replace

    from portolan_cli.metadata_seeding import seed_metadata_yaml
    from portolan_cli.stac import update_stac_metadata

    extracted = iso_metadata.to_extracted_metadata(source_url)

    # Override processing notes with layer-specific info
    extracted = replace(
        extracted,
        processing_notes=f"Extracted from WFS layer: {layer_name}. "
        f"Metadata from ISO 19139 record: {iso_metadata.file_identifier}",
    )

    metadata_path = collection_dir / ".portolan" / "metadata.yaml"
    seeded = seed_metadata_yaml(extracted, metadata_path)

    # Per Issue #369: Also update collection.json with title/description
    # ISO metadata has rich title and abstract that should appear in STAC
    collection_path = collection_dir / "collection.json"
    update_stac_metadata(
        collection_path,
        title=iso_metadata.title,
        description=iso_metadata.abstract,
    )

    return seeded
