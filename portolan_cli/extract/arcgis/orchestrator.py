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

import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from portolan_cli.extract.arcgis.discovery import (
    LayerInfo,
    ServiceDiscoveryResult,
    discover_layers,
)
from portolan_cli.extract.arcgis.filters import filter_layers
from portolan_cli.extract.arcgis.metadata import extract_arcgis_metadata
from portolan_cli.extract.arcgis.report import (
    ExtractionReport,
    ExtractionSummary,
    LayerResult,
    MetadataExtracted,
    load_report,
    save_report,
)
from portolan_cli.extract.arcgis.resume import get_resume_state, should_process_layer
from portolan_cli.extract.arcgis.retry import RetryConfig, retry_with_backoff
from portolan_cli.extract.arcgis.url_parser import ArcGISURLType, parse_arcgis_url

if TYPE_CHECKING:
    from collections.abc import Callable


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
    # Lowercase and replace spaces/special chars with underscores
    slug = name.lower()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    slug = slug.strip("_")
    return slug or "unnamed"


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
    """

    workers: int = 3
    retries: int = 3
    timeout: float = 60.0
    resume: bool = False
    dry_run: bool = False
    sort_hilbert: bool = True


@dataclass
class ExtractionProgress:
    """Progress callback data for extraction.

    Attributes:
        layer_index: Current layer index (0-based)
        total_layers: Total number of layers to extract
        layer_name: Name of current layer
        status: Current status ("starting", "extracting", "success", "failed", "skipped")
    """

    layer_index: int
    total_layers: int
    layer_name: str
    status: str


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

    # Check if gpio.extract_arcgis supports max_workers (added in gpio 0.10.0+)
    sig = inspect.signature(gpio.extract_arcgis)
    if "max_workers" in sig.parameters:
        table = gpio.extract_arcgis(layer_url, max_workers=options.workers)
    else:
        # Fallback for gpio < 0.10.0
        table = gpio.extract_arcgis(layer_url)

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


def extract_arcgis_catalog(
    url: str,
    output_dir: Path,
    *,
    layer_filter: list[str] | None = None,
    layer_exclude: list[str] | None = None,
    options: ExtractionOptions | None = None,
    on_progress: Callable[[ExtractionProgress], None] | None = None,
) -> ExtractionReport:
    """Extract layers from an ArcGIS service to a Portolan catalog.

    This is the main orchestration function that:
    1. Parses the URL to determine service type
    2. Discovers available layers
    3. Applies filters
    4. Handles resume logic
    5. Extracts each layer with retry
    6. Generates extraction report

    Args:
        url: ArcGIS FeatureServer, MapServer, or services root URL
        output_dir: Directory to write extracted data
        layer_filter: Glob patterns to include layers (if None, include all)
        layer_exclude: Glob patterns to exclude layers
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
    if parsed.url_type == ArcGISURLType.SERVICES_ROOT:
        msg = (
            "Services root URLs not yet supported. Please provide a FeatureServer or MapServer URL."
        )
        raise NotImplementedError(msg)

    # Discover layers
    discovery_result = discover_layers(url, timeout=options.timeout)

    # Convert LayerInfo to dict format for filtering
    layers_dicts: list[dict[str, int | str]] = [
        {"id": layer.id, "name": layer.name} for layer in discovery_result.layers
    ]

    # Apply filters
    if layer_filter or layer_exclude:
        filtered_dicts = filter_layers(
            layers_dicts,
            include=layer_filter,
            exclude=layer_exclude,
        )
        filtered_ids = {d["id"] for d in filtered_dicts}
        layers = [layer for layer in discovery_result.layers if layer.id in filtered_ids]
    else:
        layers = discovery_result.layers

    # Handle resume
    resume_state = None
    existing_results: dict[int, LayerResult] = {}
    report_path = output_dir / ".portolan" / "extraction-report.json"

    if options.resume and report_path.exists():
        existing_report = load_report(report_path)
        resume_state = get_resume_state(existing_report)
        existing_results = {r.id: r for r in existing_report.layers}

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
            for layer in layers
        ]
        return _build_report(
            url=url,
            discovery_result=discovery_result,
            layer_results=dry_run_results,
        )

    # Create output directory structure
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / ".portolan").mkdir(exist_ok=True)

    # Extract each layer
    layer_results: list[LayerResult] = []
    retry_config = RetryConfig(max_attempts=options.retries)

    for i, layer in enumerate(layers):
        layer_slug = _slugify(layer.name)

        total = len(layers)
        _emit_progress(on_progress, i, total, layer.name, "starting")

        # Check resume state
        if resume_state and not should_process_layer(layer.id, resume_state):
            # Already succeeded - use existing result
            if layer.id in existing_results:
                layer_results.append(existing_results[layer.id])
                _emit_progress(on_progress, i, total, layer.name, "skipped")
                continue

        # Build output path: collection_name/item_name/item_name.parquet
        collection_dir = output_dir / layer_slug
        item_dir = collection_dir / layer_slug
        output_path = item_dir / f"{layer_slug}.parquet"

        # Extract with retry
        _emit_progress(on_progress, i, total, layer.name, "extracting")

        result = retry_with_backoff(
            _extract_single_layer,
            retry_config,
            url,
            layer,
            output_path,
            options,
            on_retry=lambda attempt, err: None,  # Silent retries
        )

        if result.success:
            features, size_bytes, duration = result.value  # type: ignore[misc]
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
            _emit_progress(on_progress, i, total, layer.name, "success")
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
            _emit_progress(on_progress, i, total, layer.name, "failed")

    # Build and save report
    report = _build_report(
        url=url,
        discovery_result=discovery_result,
        layer_results=layer_results,
    )
    save_report(report, report_path)

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
