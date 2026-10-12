"""LayerSource: a data source made of layers, and the driver that extracts it (#947).

A service that serves named layers implements ``LayerSource`` with six
methods. ``run_layer_extraction`` gives it the extraction lifecycle that
the built-in extractors use: filters, resume, retry, the extraction report,
the license check, catalog setup, styles, provenance links, and metadata
seeding. ``extractor_from`` wraps a ``LayerSource`` as an ``Extractor``, so it
becomes ``portolan extract <name>``.

The built-in ArcGIS, Carto, and WFS extractors keep their own drivers. Each
one adds source-specific work, such as WFS version negotiation, ArcGIS
services-root traversal, and parallel layer downloads.
"""

from __future__ import annotations

import importlib
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    import click

    from portolan_cli.extract.common.progress import ExtractionProgress
    from portolan_cli.extract.common.report import ExtractionReport, LayerResult
    from portolan_cli.licensing import ResolvedLicense
    from portolan_cli.metadata_extraction import ExtractedMetadata

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LayerRef:
    """One layer that a source serves.

    Attributes:
        id: Stable integer id. The report and resume state use it.
        name: Layer name. The collection directory is a slug of it.
    """

    id: int
    name: str


@dataclass(frozen=True)
class LayerMetadata:
    """Descriptive metadata for one layer's collection.

    Attributes:
        title: Collection title.
        description: Collection description.
        keywords: Discovery keywords.
    """

    title: str | None = None
    description: str | None = None
    keywords: list[str] | None = None


@dataclass(frozen=True)
class LayerSourceOptions:
    """Options for ``run_layer_extraction``.

    Attributes:
        layer_filter: Glob patterns of layer names to include. None includes all.
        layer_exclude: Glob patterns of layer names to exclude.
        resume: Skip layers that succeeded in the previous report.
        dry_run: Report the layers without writing anything.
        raw: Write the layer files and the report, but no catalog.
        retries: Attempts per layer.
        retry_delay: Seconds before the first retry. Later retries double it.
        catalog_id: Catalog id. None derives it from the output directory.
        license: SPDX license identifier, or ``other`` with ``license_url``.
        license_url: URL of the license text.
    """

    layer_filter: list[str] | None = None
    layer_exclude: list[str] | None = None
    resume: bool = False
    dry_run: bool = False
    raw: bool = False
    retries: int = 3
    retry_delay: float = 1.0
    catalog_id: str | None = None
    license: str | None = None
    license_url: str | None = None


@runtime_checkable
class LayerSource(Protocol):
    """A data source that serves named layers.

    Attributes:
        name: The subcommand name, ``portolan extract <name>``. It is also
            the ``source_type`` in the seeded metadata.
        summary: One line that describes the source.
    """

    name: str
    summary: str

    def discover(self, source: str) -> list[LayerRef]:
        """List the layers at ``source``."""
        ...

    def fetch(self, source: str, layer: LayerRef, dest: Path) -> int:
        """Write ``layer`` as GeoParquet to ``dest``. Return the feature count.

        Raise an exception on failure. The driver retries it.
        """
        ...

    def service_metadata(self, source: str) -> ExtractedMetadata | None:
        """Return catalog-level metadata, or None when the source has none.

        A ``license_url`` in it lets the license check pass without
        ``--license``.
        """
        ...

    def layer_metadata(self, source: str, layer: LayerRef) -> LayerMetadata | None:
        """Return the metadata for the layer's collection, or None."""
        ...

    def styles(self, source: str, layer: LayerRef, collection_dir: Path) -> None:
        """Write Mapbox GL style files into ``collection_dir/styles/``, or do nothing."""
        ...

    def source_link(self, source: str, layer: LayerRef) -> str | None:
        """Return the URL of the original layer for a ``via`` link, or None."""
        ...


def _slug(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return slug or "layer"


def _package_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return "unknown"


def _build_report(
    source: str, metadata: ExtractedMetadata | None, results: list[LayerResult]
) -> ExtractionReport:
    from portolan_cli.extract.common.report import (
        ExtractionReport,
        ExtractionSummary,
        MetadataExtracted,
    )

    summary = ExtractionSummary(
        total_layers=len(results),
        succeeded=sum(1 for r in results if r.status == "success"),
        failed=sum(1 for r in results if r.status == "failed"),
        skipped=sum(1 for r in results if r.status == "skipped"),
        empty=sum(1 for r in results if r.status == "empty"),
        total_features=sum(r.features or 0 for r in results),
        total_size_bytes=sum(r.size_bytes or 0 for r in results),
        total_duration_seconds=sum(r.duration_seconds or 0.0 for r in results),
    )
    extracted = MetadataExtracted(
        source_url=source,
        description=metadata.description if metadata else None,
        attribution=metadata.attribution if metadata else None,
        keywords=metadata.keywords if metadata else None,
        contact_name=metadata.contact_name if metadata else None,
        processing_notes=metadata.processing_notes if metadata else None,
        known_issues=metadata.known_issues if metadata else None,
        license_info_raw=metadata.license_raw if metadata else None,
    )
    return ExtractionReport(
        extraction_date=datetime.now(timezone.utc).isoformat(),
        source_url=source,
        portolan_version=_package_version("portolan-cli"),
        gpio_version=_package_version("geoparquet-io"),
        metadata_extracted=extracted,
        layers=results,
        summary=summary,
    )


def _select_layers(layers: list[LayerRef], options: LayerSourceOptions) -> list[LayerRef]:
    from portolan_cli.extract.common.filters import filter_layers

    if not options.layer_filter and not options.layer_exclude:
        return layers
    kept = filter_layers(
        [{"id": layer.id, "name": layer.name} for layer in layers],
        include=options.layer_filter,
        exclude=options.layer_exclude,
    )
    kept_ids = {item["id"] for item in kept}
    return [layer for layer in layers if layer.id in kept_ids]


def _result(layer: LayerRef, status: str, **fields: Any) -> LayerResult:
    from portolan_cli.extract.common.report import LayerResult

    values: dict[str, Any] = {
        "features": None,
        "size_bytes": None,
        "duration_seconds": None,
        "output_path": None,
        "warnings": [],
        "error": None,
        "attempts": 0,
    }
    values.update(fields)
    return LayerResult(id=layer.id, name=layer.name, status=status, **values)


def _fetch_layer(
    layer_source: LayerSource,
    source: str,
    layer: LayerRef,
    output_dir: Path,
    options: LayerSourceOptions,
) -> LayerResult:
    from portolan_cli.extract.common.retry import RetryConfig, retry_with_backoff

    slug = _slug(layer.name)
    dest = output_dir / slug / f"{slug}.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    outcome = retry_with_backoff(
        layer_source.fetch,
        RetryConfig(max_attempts=max(options.retries, 1), initial_delay=options.retry_delay),
        source,
        layer,
        dest,
    )
    duration = time.monotonic() - start
    if not outcome.success:
        return _result(layer, "failed", error=str(outcome.error), attempts=outcome.attempts)
    features = int(outcome.value or 0)
    if features == 0 or not dest.exists():
        return _result(
            layer,
            "empty",
            features=0,
            size_bytes=0,
            duration_seconds=duration,
            error="The layer has no features.",
            attempts=outcome.attempts,
        )
    layer_source.styles(source, layer, dest.parent)
    return _result(
        layer,
        "success",
        features=features,
        size_bytes=dest.stat().st_size,
        duration_seconds=duration,
        output_path=dest.relative_to(output_dir).as_posix(),
        attempts=outcome.attempts,
    )


def _resolve_license(
    metadata: ExtractedMetadata | None, options: LayerSourceOptions
) -> ResolvedLicense | None:
    from portolan_cli.licensing import resolve_harvest_license

    if options.raw:
        return None
    return resolve_harvest_license(
        cli_license=options.license,
        cli_license_url=options.license_url,
        harvested_license_url=metadata.license_url if metadata else None,
    )


def _build_catalog(
    layer_source: LayerSource,
    source: str,
    output_dir: Path,
    report: ExtractionReport,
    layers: dict[str, LayerRef],
    metadata: ExtractedMetadata | None,
    resolved_license: ResolvedLicense | None,
    options: LayerSourceOptions,
) -> None:
    from portolan_cli.extract.common.metadata_seeding import seed_collection_metadata
    from portolan_cli.extract.common.orchestrator_base import (
        add_source_links,
        init_extracted_catalog,
        register_collection_styles,
        seed_catalog_metadata,
    )

    added = init_extracted_catalog(
        output_dir,
        report,
        catalog_id=options.catalog_id,
        title=None,
        description=metadata.description if metadata else None,
        post_init=lambda out, _files, _fresh: seed_catalog_metadata(
            out, metadata, resolved_license
        ),
    )
    if added is None:
        return
    register_collection_styles(output_dir, report)
    add_source_links(
        output_dir,
        report,
        url_builder=lambda src, result: layer_source.source_link(src, layers[result.name]) or src,
        title_builder=lambda result: f"Source layer: {result.name}",
    )
    for result in report.layers:
        if result.status != "success" or not result.output_path:
            continue
        layer = layers[result.name]
        layer_meta = layer_source.layer_metadata(source, layer) or LayerMetadata()
        seed_collection_metadata(
            output_dir / result.output_path.rsplit("/", 1)[0],
            source_type=layer_source.name,
            source_url=layer_source.source_link(source, layer) or source,
            layer_name=layer.name,
            title=layer_meta.title,
            description=layer_meta.description,
            keywords=layer_meta.keywords,
        )


def run_layer_extraction(
    layer_source: LayerSource,
    source: str,
    output_dir: Path,
    options: LayerSourceOptions | None = None,
    *,
    on_progress: Callable[[ExtractionProgress], None] | None = None,
) -> ExtractionReport:
    """Extract the layers of ``source`` into a Portolan catalog.

    The steps are those of the built-in extractors: discover, filter, check
    the license, resume, fetch with retry, save the report to
    ``.portolan/extraction-report.json``, then build the catalog unless
    ``options.raw`` is set.

    Args:
        layer_source: The source implementation.
        source: Service URL or other source locator.
        output_dir: Catalog directory.
        options: Extraction options. None uses the defaults.
        on_progress: Called with progress for each layer.

    Returns:
        The extraction report.

    Raises:
        MissingLicenseError: Before any download, when no license can be
            resolved and ``options.raw`` is not set.
    """
    from portolan_cli.extract.common.progress import emit_progress
    from portolan_cli.extract.common.report import load_report, save_report
    from portolan_cli.extract.common.resume import get_resume_state, should_process_layer

    options = options or LayerSourceOptions()
    layers = _select_layers(layer_source.discover(source), options)
    metadata = layer_source.service_metadata(source)

    if options.dry_run:
        return _build_report(
            source,
            metadata,
            [_result(layer, "pending", features=0, size_bytes=0) for layer in layers],
        )

    resolved_license = _resolve_license(metadata, options)
    report_path = output_dir / ".portolan" / "extraction-report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    previous: dict[str, LayerResult] = {}
    resume_state = None
    if options.resume and report_path.exists():
        existing = load_report(report_path)
        resume_state = get_resume_state(existing)
        previous = {r.name: r for r in existing.layers}

    results: list[LayerResult] = []
    total = len(layers)
    for index, layer in enumerate(layers):
        if (
            resume_state
            and layer.name in previous
            and not should_process_layer(layer.id, resume_state, layer_name=layer.name)
        ):
            emit_progress(on_progress, index, total, layer.name, "skipped")
            results.append(previous[layer.name])
            continue
        emit_progress(on_progress, index, total, layer.name, "starting")
        result = _fetch_layer(layer_source, source, layer, output_dir, options)
        emit_progress(on_progress, index, total, layer.name, result.status, error=result.error)
        results.append(result)

    report = _build_report(source, metadata, results)
    save_report(report, report_path)
    if not options.raw:
        _build_catalog(
            layer_source,
            source,
            output_dir,
            report,
            {layer.name: layer for layer in layers},
            metadata,
            resolved_license,
            options,
        )
    return report


class _LayerSourceExtractor:
    """An ``Extractor`` that runs a ``LayerSource`` through the shared driver."""

    def __init__(self, layer_source: LayerSource) -> None:
        self.layer_source = layer_source
        self.name = layer_source.name
        self.summary = layer_source.summary

    def extract(self, source: str, output_dir: Path, **options: Any) -> ExtractionReport:
        """Run ``run_layer_extraction``. ``options`` holds LayerSourceOptions fields."""
        on_progress = options.pop("on_progress", None)
        return run_layer_extraction(
            self.layer_source,
            source,
            output_dir,
            LayerSourceOptions(**options),
            on_progress=on_progress,
        )

    def cli_command(self) -> click.Command:
        """Build ``portolan extract <name>`` for the source.

        The command module is part of the CLI layer. It loads here, when the
        user runs the command.
        """
        layer_cli = importlib.import_module("portolan_cli.extract.common.layer_cli")
        command: click.Command = layer_cli.build_command(self.layer_source)
        return command


def extractor_from(layer_source: LayerSource) -> _LayerSourceExtractor:
    """Wrap a ``LayerSource`` as an ``Extractor`` for ``portolan_extractors``.

    Args:
        layer_source: The source implementation.

    Returns:
        An extractor with the source's name and summary.
    """
    return _LayerSourceExtractor(layer_source)
