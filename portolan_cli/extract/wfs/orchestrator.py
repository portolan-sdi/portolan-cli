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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

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
from portolan_cli.extract.wfs.discovery import LayerInfo, list_layers

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)


@dataclass
class ExtractionOptions:
    """Options for WFS extraction.

    Attributes:
        workers: Number of parallel workers for extraction.
        retries: Number of retry attempts per failed layer.
        timeout: Per-request timeout in seconds.
        resume: Whether to resume from existing extraction report.
        raw: If True, skip auto-init (only create extraction files, no STAC catalog).
        dry_run: If True, list layers without extracting.
        wfs_version: WFS version ("1.0.0", "1.1.0", "2.0.0", or "auto").
        output_crs: Target CRS for output (e.g., "EPSG:4326"). None keeps source CRS.
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in output CRS.
        limit: Maximum features per layer (None for unlimited).
    """

    workers: int = 1
    retries: int = 3
    timeout: float = 60.0
    resume: bool = False
    raw: bool = False
    dry_run: bool = False
    wfs_version: str = "auto"
    output_crs: str | None = None
    bbox: tuple[float, float, float, float] | None = None
    limit: int | None = None


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


def _slugify(name: str) -> str:
    """Convert a name to a filesystem-safe slug.

    Args:
        name: Original name (e.g., "ns:FeatureType")

    Returns:
        Slugified name (e.g., "ns_featuretype")
    """
    slug = name.lower()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    slug = re.sub(r"_+", "_", slug)
    slug = slug.strip("_")
    return slug or "unnamed"


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
) -> tuple[int, int, float]:
    """Extract a single WFS layer using gpio.

    Args:
        service_url: WFS service URL.
        layer: Layer info (typename used for extraction).
        output_path: Path to write parquet file.
        options: Extraction options.

    Returns:
        Tuple of (feature_count, file_size_bytes, duration_seconds).

    Raises:
        Exception: If extraction fails.
    """
    import geoparquet_io as gpio  # type: ignore[import-untyped]

    start_time = time.monotonic()

    # Determine WFS version (auto = let gpio decide)
    version = None if options.wfs_version == "auto" else options.wfs_version

    # Build extraction kwargs
    kwargs: dict[str, object] = {
        "typename": layer.typename,
    }
    if version:
        kwargs["version"] = version
    if options.output_crs:
        kwargs["output_crs"] = options.output_crs
    if options.bbox:
        kwargs["bbox"] = options.bbox
    if options.limit:
        kwargs["max_features"] = options.limit

    table = gpio.extract_wfs(service_url, **kwargs)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    table.write(str(output_path))

    duration = time.monotonic() - start_time
    feature_count = table.num_rows
    file_size = output_path.stat().st_size if output_path.exists() else 0

    return feature_count, file_size, duration


def _build_dry_run_report(
    url: str,
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
    return _build_report(url=url, layer_results=dry_run_results)


def _build_report(
    url: str,
    layer_results: list[LayerResult],
) -> ExtractionReport:
    """Build an ExtractionReport from extraction results."""
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

    # WFS metadata extraction happens separately via GetCapabilities
    metadata_extracted = MetadataExtracted(
        source_url=url,
        description=None,
        attribution=None,
        keywords=None,
        contact_name=None,
        processing_notes=None,
        known_issues=None,
        license_info_raw=None,
    )

    succeeded = sum(1 for r in layer_results if r.status == "success")
    failed = sum(1 for r in layer_results if r.status == "failed")
    skipped = sum(1 for r in layer_results if r.status == "skipped")

    summary = ExtractionSummary(
        total_layers=len(layer_results),
        succeeded=succeeded,
        failed=failed,
        skipped=skipped,
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
    1. Discovers available layers via GetCapabilities
    2. Applies filters
    3. Handles resume logic
    4. Extracts each layer with retry
    5. Generates extraction report

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

    # Determine WFS version for discovery
    version = "1.1.0" if options.wfs_version == "auto" else options.wfs_version

    # Discover layers
    layers = list_layers(url, version=version)

    # Apply layer filters
    layers = _filter_discovered_layers(layers, layer_filter, layer_exclude)

    # Dry run - just return what would be extracted
    if options.dry_run:
        return _build_dry_run_report(url, layers)

    # Create output directory structure
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / ".portolan").mkdir(exist_ok=True)

    # Handle resume state
    report_path = output_dir / ".portolan" / "extraction-report.json"
    resume_state: ResumeState | None = None
    existing_results: dict[int, LayerResult] = {}

    if options.resume and report_path.exists():
        from portolan_cli.extract.common.report import load_report

        existing_report = load_report(report_path)
        resume_state = get_resume_state(existing_report)
        existing_results = {r.id: r for r in existing_report.layers}

    # Extract each layer
    layer_results: list[LayerResult] = []
    retry_config = RetryConfig(max_attempts=options.retries)
    total = len(layers)

    for i, layer in enumerate(layers):
        layer_slug = _slugify(layer.name)
        _emit_progress(on_progress, i, total, layer.name, "starting")

        # Check resume state - skip if already succeeded
        if resume_state and not should_process_layer(layer.id, resume_state):
            if layer.id in existing_results:
                _emit_progress(on_progress, i, total, layer.name, "skipped")
                layer_results.append(existing_results[layer.id])
                continue
            logger.warning(
                "Layer '%s' (id=%d) marked complete in resume state but result missing; re-extracting",
                layer.name,
                layer.id,
            )

        # Build output path: layer_name/layer_name.parquet
        collection_dir = output_dir / layer_slug
        output_path = collection_dir / f"{layer_slug}.parquet"

        # Extract with retry
        _emit_progress(on_progress, i, total, layer.name, "extracting")

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
            _emit_progress(on_progress, i, total, layer.name, "success")
            layer_results.append(
                LayerResult(
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
            )
        else:
            _emit_progress(on_progress, i, total, layer.name, "failed")
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

    # Build and save report
    report = _build_report(url=url, layer_results=layer_results)
    save_report(report, report_path)

    # Auto-init catalog unless raw mode
    if not options.raw:
        _auto_init_catalog(output_dir, report)

    return report


def _auto_init_catalog(output_dir: Path, report: ExtractionReport) -> None:
    """Initialize a Portolan catalog and add extracted files.

    Called automatically after extraction unless raw=True.
    Creates catalog.json, config.yaml, and collection.json for each layer.
    Also adds provenance via links (GetFeature-style URLs).
    """
    from portolan_cli.catalog import init_catalog
    from portolan_cli.dataset import add_files

    parquet_files = [
        output_dir / result.output_path
        for result in report.layers
        if result.status == "success" and result.output_path
    ]

    if not parquet_files:
        return

    init_catalog(output_dir, title=None)

    add_files(
        paths=parquet_files,
        catalog_root=output_dir,
    )

    # Add via links for provenance tracking
    _add_via_links_to_collections(output_dir, report)


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

        output_parts = Path(layer.output_path).parts
        if not output_parts:
            continue

        collection_dir = output_dir / output_parts[0]
        collection_path = collection_dir / "collection.json"

        if not collection_path.exists():
            continue

        # Build GetFeature-style URL for provenance
        # WFS GetFeature URL pattern: service_url?service=WFS&request=GetFeature&typename=X
        layer_url = f"{source_url}?service=WFS&request=GetFeature&typename={layer.name}"

        add_via_link(
            collection_path,
            layer_url,
            title=f"Source WFS layer: {layer.name}",
        )
