"""WFS extraction through the shared LayerSource driver.

``WFSLayerSource`` implements ``LayerSource`` for a WFS endpoint, and
``extract_wfs_catalog`` runs it with ``run_layer_extraction``. Unlike ArcGIS,
WFS is always a single service endpoint (no services root).

- ``discover`` negotiates the WFS version once. Discovery and every fetch use
  that version, so ``LayerRef.data`` carries it with the layer.
- ``fetch`` calls geoparquet-io's ``convert_wfs_to_geoparquet``.
- ``styles`` reads the WMS style and legend of the layer.
- ``layer_metadata`` prefers an ISO 19139 record from the layer's MetadataURL.

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

import hashlib
import logging
from collections import Counter
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from portolan_cli.extract.common.orchestrator_base import parquet_row_count
from portolan_cli.extract.common.report import MetadataExtracted
from portolan_cli.extract.common.source import (
    Discovery,
    EmptyLayerError,
    LayerMetadata,
    LayerRef,
    LayerSourceOptions,
    SourceLink,
    filter_refs,
    run_layer_extraction,
    slugify,
)
from portolan_cli.extract.common.styles import extract_wms_legend, extract_wms_style
from portolan_cli.extract.common.urls import merge_query, redact_credentials
from portolan_cli.extract.wfs.discovery import LayerInfo, WFSDiscoveryResult, discover_layers
from portolan_cli.licensing import license_url_from_text

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_cli.extract.common.progress import ExtractionProgress
    from portolan_cli.extract.common.report import ExtractionReport
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
            10-minute internal HTTP timeout for large responses; this timeout
            wraps the entire layer extraction including retries.
        resume: Whether to resume from existing extraction report.
        raw: If True, skip auto-init (only create extraction files, no STAC catalog).
        dry_run: If True, list layers without extracting.
        no_styles: If True, skip style extraction from WMS GetStyles.
        wfs_version: WFS version ("1.0.0", "1.1.0", "2.0.0", or "auto").
            When "auto", the version is negotiated with the server once and
            used consistently for both discovery and extraction.
        output_crs: Target CRS for output (e.g., "EPSG:4326"). None keeps source CRS.
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in output CRS.
        limit: Maximum features per layer (None for unlimited).
        page_size: Features per page when gpio uses parallel pagination for
            very large layers (1M+ features). Default: 100000 (gpio 1.3+ default,
            ~10x fewer HTTP roundtrips than the old 10000).
        auto_tile: When True (default), gpio subdivides the bbox and retries when
            a server caps maxFeatures below the requested page, so capped layers
            extract completely instead of silently truncating (gpio 1.3+).
        license: SPDX identifier for the harvested data, or "other" with
            license_url. Overrides any license URL found in the service's own
            license text (issue #686).
        license_url: URL of the license text.
        catalog_id: Catalog id for the created catalog. None derives it from
            the output directory name, which is the behavior before issue #821.
    """

    workers: int = 1
    retries: int = 3
    timeout: float = 300.0  # 5 minutes per layer (gpio has 10min internal timeout)
    resume: bool = False
    raw: bool = False
    dry_run: bool = False
    no_styles: bool = False
    wfs_version: str = "auto"
    output_crs: str | None = None
    bbox: tuple[float, float, float, float] | None = None
    limit: int | None = None
    page_size: int = 100000
    auto_tile: bool = True
    license: str | None = None
    license_url: str | None = None
    catalog_id: str | None = None


def _assign_slugs(layers: list[LayerInfo]) -> dict[int, str]:
    """Assign a collection slug to each layer, with a hash suffix only on collision.

    Two typenames can slug to the same name (``ns:Roads`` and ``ns_roads``).
    Each colliding layer then gets a short hash of its name and id (issue #379).

    Args:
        layers: The layers that this run extracts.

    Returns:
        Slug by layer id.
    """
    base = {layer.id: slugify(layer.name) for layer in layers}
    counts = Counter(base.values())
    slugs: dict[int, str] = {}
    for layer in layers:
        slug = base[layer.id]
        if counts[slug] > 1:
            digest = hashlib.sha256(f"{layer.name}:{layer.id}".encode()).hexdigest()[:6]
            slug = f"{slug}_{digest}"
        slugs[layer.id] = slug
    return slugs


@dataclass(frozen=True)
class WFSLayer:
    """What ``fetch`` and the metadata methods need for one WFS layer."""

    info: LayerInfo
    version: str


class WFSLayerSource:
    """A WFS endpoint as a ``LayerSource``."""

    name = "wfs"
    summary = "OGC Web Feature Service layers."

    def __init__(self, options: ExtractionOptions | None = None) -> None:
        self.options = options or ExtractionOptions()

    def discover(self, source: str, options: LayerSourceOptions) -> Discovery:
        """Negotiate the version, list the layers, and read the service metadata.

        The slugs come from the filtered layers, so a collision with a layer
        that the filter drops does not add a hash suffix.
        """
        version = _negotiate_version(source, self.options.wfs_version)
        logger.debug("Using WFS version: %s", version)
        return self.discovery_from(
            source, discover_layers(source, version=version), version, options
        )

    def discovery_from(
        self,
        source: str,
        found: WFSDiscoveryResult,
        version: str,
        options: LayerSourceOptions,
    ) -> Discovery:
        """Turn a GetCapabilities result into the driver's ``Discovery``.

        Args:
            source: WFS service URL.
            found: The parsed GetCapabilities result.
            version: The negotiated WFS version.
            options: The driver options, for the layer filter.

        Returns:
            The filtered layers with their slugs, and the catalog metadata.
        """
        refs = filter_refs(
            [
                LayerRef(id=layer.id, name=layer.name, data=WFSLayer(layer, version))
                for layer in found.layers
            ],
            options.layer_filter,
            options.layer_exclude,
        )
        slugs = _assign_slugs([ref.data.info for ref in refs])
        metadata = _build_metadata(source, found)
        title, description = _derive_catalog_titles(found)
        # GetCapabilities carries the licence in Fees and AccessConstraints (issue #686).
        license_text = "; ".join(part for part in (found.fees, found.access_constraints) if part)
        return Discovery(
            source_url=source,
            layers=[replace(ref, path=slugs[ref.id]) for ref in refs],
            metadata=metadata,
            catalog_seed=_report_metadata_to_wfs_metadata(metadata).to_extracted(),
            license_url=license_url_from_text(license_text),
            catalog_title=title,
            catalog_description=description,
            root_metadata=(title, description),
            filtered=True,
        )

    def fetch(self, source: str, layer: LayerRef, dest: Path) -> int:
        """Write the layer with geoparquet-io's ``convert_wfs_to_geoparquet``.

        geoparquet-io raises a WFS error for a layer with no features. That
        error becomes ``EmptyLayerError`` (issue #450).
        """
        from geoparquet_io.core.wfs import convert_wfs_to_geoparquet  # type: ignore[import-untyped]

        wfs_layer: WFSLayer = layer.data
        options = self.options
        try:
            convert_wfs_to_geoparquet(
                service_url=source,
                typename=wfs_layer.info.typename,
                output_file=str(dest),
                version=wfs_layer.version,
                bbox=options.bbox,
                output_crs=options.output_crs,
                limit=options.limit,
                page_size=options.page_size,
                auto_tile=options.auto_tile,
                overwrite=True,  # Resume decides what to fetch again
            )
        except Exception as err:
            if _is_empty_layer_error(str(err)):
                raise EmptyLayerError(str(err)) from err
            raise
        return parquet_row_count(dest)

    def layer_metadata(self, source: str, layer: LayerRef) -> LayerMetadata:
        """Prefer the layer's ISO 19139 record. Fall back to GetCapabilities."""
        info: LayerInfo = layer.data.info
        layer_url = self._layer_url(source, info.name)
        iso = _try_fetch_iso_metadata(info.metadata_urls)
        if iso is not None and iso.has_useful_metadata():
            record = replace(
                iso.to_extracted_metadata(layer_url),
                processing_notes=f"Extracted from WFS layer: {info.name}. "
                f"Metadata from ISO 19139 record: {iso.file_identifier}",
            )
            return LayerMetadata(
                title=iso.title,
                description=iso.abstract,
                record=record,
                record_label="ISO metadata",
            )
        return LayerMetadata(
            title=info.title,
            description=info.abstract,
            keywords=info.keywords,
            source_url=layer_url,
        )

    def styles(self, source: str, layer: LayerRef, collection_dir: Path) -> None:
        """Write the WMS style (issue #490) and legend (issue #498) of the layer."""
        if self.options.no_styles:
            return
        style = extract_wms_style(
            wfs_url=source,
            layer_name=layer.name,
            collection_path=collection_dir,
            source_layer=collection_dir.name,
        )
        if style:
            logger.debug("Extracted style for %s: %s", layer.name, style.path)
        legend = extract_wms_legend(
            wfs_url=source, layer_name=layer.name, collection_path=collection_dir
        )
        if legend:
            logger.debug("Extracted legend for %s: %s", layer.name, legend.path)

    def source_link(self, source: str, layer: LayerRef) -> SourceLink:
        """Link to a GetFeature request for the layer."""
        return SourceLink(
            href=self._layer_url(source, layer.name), title=f"Source WFS layer: {layer.name}"
        )

    @staticmethod
    def _layer_url(source: str, typename: str) -> str:
        return merge_query(
            source, {"service": "WFS", "request": "GetFeature", "typename": typename}
        )


def driver_options(
    options: ExtractionOptions,
    layer_filter: list[str] | None,
    layer_exclude: list[str] | None,
) -> LayerSourceOptions:
    """Map WFS options to the driver's options.

    ``timeout`` is the per-layer deadline of a parallel run (issues #508, #526).
    """
    return LayerSourceOptions(
        layer_filter=layer_filter,
        layer_exclude=layer_exclude,
        resume=options.resume,
        dry_run=options.dry_run,
        raw=options.raw,
        retries=options.retries,
        workers=options.workers,
        layer_timeout=options.timeout,
        catalog_id=options.catalog_id,
        license=options.license,
        license_url=options.license_url,
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
        MissingLicenseError: If no license can be resolved (issue #686).
    """
    options = options or ExtractionOptions()
    return run_layer_extraction(
        WFSLayerSource(options),
        url,
        output_dir,
        driver_options(options, layer_filter, layer_exclude),
        on_progress=on_progress,
    )


def _build_metadata(url: str, discovery_result: WFSDiscoveryResult | None) -> MetadataExtracted:
    """Build MetadataExtracted from discovery result."""
    if not discovery_result:
        return MetadataExtracted(
            source_url=url,
            description=None,
            attribution=None,
            keywords=None,
            producer=None,
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
        # GetCapabilities names a point of contact, never an author.
        producer=None,
        processing_notes=processing_notes,
        known_issues=None,
        license_info_raw=discovery_result.access_constraints,
    )


def _is_empty_layer_error(error_msg: str | None) -> bool:
    """Check if an error message indicates an empty WFS layer.

    gpio raises WFSError with specific messages when a layer has 0 features:
    - "No features returned from WFS service for layer '{typename}'."
    - "No features returned from any spatial tile."

    This detection is string-based and coupled to gpio's error messages.
    See: https://github.com/portolan-sdi/portolan-cli/issues/450

    Upstream: https://github.com/geoparquet/geoparquet-io/issues/448
    (EmptyLayerError subclass requested)

    Args:
        error_msg: The error message from a failed extraction.

    Returns:
        True if the error indicates an empty layer (0 features).
    """
    if not error_msg:
        return False
    return "No features returned" in error_msg


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
        logger.warning("Version negotiation failed, using 1.1.0: %s", redact_credentials(str(e)))
        return "1.1.0"


def _derive_catalog_titles(
    discovery_result: WFSDiscoveryResult | None,
) -> tuple[str | None, str | None]:
    """Derive filtered catalog title/description from the WFS discovery result.

    Technical names and boilerplate descriptions are dropped (Issue #376) so the
    catalog is not populated with service-generated placeholders.
    """
    from portolan_cli.extract.wfs.metadata import is_boilerplate_description
    from portolan_cli.stac import is_technical_name

    service_title = discovery_result.service_title if discovery_result else None
    service_description = discovery_result.service_abstract if discovery_result else None

    def _should_filter(text: str | None) -> bool:
        return is_technical_name(text) or is_boilerplate_description(text)

    filtered_title = None if _should_filter(service_title) else service_title
    filtered_description = None if _should_filter(service_description) else service_description
    return filtered_title, filtered_description


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
