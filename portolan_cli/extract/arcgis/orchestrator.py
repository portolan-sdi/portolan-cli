"""ArcGIS extraction through the shared LayerSource driver.

``ArcGISLayerSource`` implements ``LayerSource`` for ArcGIS FeatureServer and
MapServer services, and ``extract_arcgis_catalog`` runs it with
``run_layer_extraction``. ImageServer rasters take their own path in
``imageserver/``.

- ``discover`` handles a single service or a services root. For a root it
  walks the folders, applies the service filters, and lists each service's
  layers. A single-layer service becomes one collection at the service
  directory. A multi-layer service nests one collection per layer.
- ``fetch`` calls ``gpio.extract_arcgis``, adds the bbox covering column,
  sorts, writes, and restores the source CRS (issues #802, #805).
- ``styles`` converts the layer's ESRI drawingInfo (issue #490).

Typical usage:
    from portolan_cli.extract.arcgis.orchestrator import extract_arcgis_catalog

    result = extract_arcgis_catalog(
        url="https://services.arcgis.com/.../FeatureServer",
        output_dir=Path("./output"),
        layer_filter=["Census*"],
    )
    print(f"Extracted {result.summary.succeeded}/{result.summary.total_layers} layers")
"""

from __future__ import annotations

import contextlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from portolan_cli.convert import has_geometry
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
from portolan_cli.extract.arcgis.url_parser import ArcGISURLType, parse_arcgis_url
from portolan_cli.extract.common.report import FolderCoverage, MetadataExtracted
from portolan_cli.extract.common.source import (
    Discovery,
    LayerMetadata,
    LayerRef,
    LayerSourceOptions,
    SourceLink,
    run_layer_extraction,
    slugify,
)
from portolan_cli.extract.common.styles import extract_esri_style
from portolan_cli.licensing import license_url_from_text

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from portolan_cli.extract.arcgis.metadata import ArcGISMetadata
    from portolan_cli.extract.common.progress import ExtractionProgress
    from portolan_cli.extract.common.report import ExtractionReport

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


def _service_output_dir(output_dir: Path, service_name: str) -> Path:
    """Map a (possibly folder-qualified) service name to a nested directory.

    "ecml/active_faults" -> output_dir/ecml/active_faults
    "Top" -> output_dir/top
    Each path segment is slugified independently so the folder hierarchy is
    preserved as nested subcatalogs.

    Slugification can collide when two service names differ only in characters
    it strips, ``turkiye`` and ``turkiye-alt`` for example. The second extraction
    writes into the first one's directory. This is a known limitation and is not
    mitigated; no uniquifying suffix is appended.
    """
    parts = [slugify(p) for p in service_name.split("/") if p]
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
        timeout: Per-request timeout in seconds, for layer discovery and for
            the feature-page requests gpio makes (issue #898)
        resume: Whether to resume from existing extraction report
        dry_run: If True, list layers without extracting
        sort_hilbert: Whether to apply Hilbert spatial sorting
        add_bbox: Whether to add a bbox covering column. rashid reads both
            PTL-DAT-006 and PTL-DAT-007 through this column (issue #805).
        raw: If True, skip auto-init (only create extraction files, no STAC catalog)
        no_styles: If True, skip style extraction from ESRI drawingInfo
        token: Optional ArcGIS token for authenticated endpoints
        recurse: Whether to recurse into sub-folders during discovery (default True)
        license: SPDX identifier for the harvested data, or "other" with
            license_url. Overrides any license URL found in the source's own
            license text (issue #686).
        license_url: URL of the license text.
        output_crs: CRS for the extracted GeoParquet. "native" keeps the
            service's source CRS (default). An EPSG code (e.g. "EPSG:4326")
            reprojects the coordinates to that CRS (issue #802).
        catalog_id: Catalog id for the created catalog. None derives it from
            the output directory name, which is the behavior before issue #821.
    """

    workers: int = 3
    retries: int = 3
    timeout: float = 60.0
    resume: bool = False
    raw: bool = False
    dry_run: bool = False
    sort_hilbert: bool = True
    add_bbox: bool = True
    no_styles: bool = False
    token: str | None = None
    recurse: bool = True
    license: str | None = None
    license_url: str | None = None
    output_crs: str = "native"
    catalog_id: str | None = None


def _is_wgs84_crs(crs: dict[str, object]) -> bool:
    """Return True when the PROJJSON CRS is WGS84 (EPSG:4326 or OGC:CRS84).

    A null CRS in GeoParquet already means CRS84, so a WGS84 output needs no
    explicit CRS tag (issue #802). gpio tags a native-4326 GeoJSON extraction as
    OGC:CRS84, whose ``to_epsg()`` is None, so this matches CRS84 as well.
    """
    from pyproj import CRS
    from pyproj.exceptions import CRSError

    try:
        parsed = CRS.from_json_dict(crs)
    except (CRSError, KeyError, TypeError, ValueError):
        # An unparsable CRS is not WGS84, so tag it rather than assume CRS84.
        return False
    return parsed.to_epsg() == 4326 or parsed == CRS("OGC:CRS84")


def _ensure_geoparquet_crs(output_path: Path, source_crs: dict[str, object] | None) -> None:
    """Write the source CRS into the GeoParquet metadata when the writer drops it.

    gpio.Table.write() emits ``crs: null`` even for a native extraction, so a file
    with projected coordinates would claim WGS84 (issue #802). This restamps the
    primary geometry column with ``source_crs`` and preserves the row groups.

    Args:
        output_path: The written GeoParquet file.
        source_crs: PROJJSON CRS the extractor detected, or None when unknown.
    """
    if source_crs is None or _is_wgs84_crs(source_crs):
        # No CRS to preserve, or WGS84 where the null tag is already correct.
        return

    import json

    import pyarrow.parquet as pq

    temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    with pq.ParquetFile(output_path) as parquet_file:
        schema = parquet_file.schema_arrow
        metadata = schema.metadata or {}
        geo_bytes = metadata.get(b"geo")
        if geo_bytes is None:
            return

        geo = json.loads(geo_bytes)
        primary = geo.get("primary_column")
        column = geo.get("columns", {}).get(primary)
        if column is None or column.get("crs") == source_crs:
            # Already tagged with the source CRS; nothing to rewrite.
            return

        column["crs"] = source_crs
        new_schema = schema.with_metadata({**metadata, b"geo": json.dumps(geo).encode("utf-8")})

        # Parquet keeps its CRS in the footer, so a rewrite is the only way to
        # change it. Copy the row groups verbatim to keep the covering
        # statistics intact.
        with pq.ParquetWriter(temp_path, new_schema, compression="zstd") as writer:
            for row_group in range(parquet_file.num_row_groups):
                writer.write_table(parquet_file.read_row_group(row_group))

    # Windows refuses to replace a file while a reader still holds it open. The
    # rename runs after the ParquetFile closes.
    temp_path.replace(output_path)


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
    skipped_out: list[tuple[ServiceInfo, LayerInfo, str]] | None = None,
) -> tuple[list[LayerInfo], dict[int, ServiceInfo], dict[str, int], list[tuple[str, str]]]:
    """Collect all layers from multiple services.

    Returns:
        Tuple of (all_layers, service_for_layer, layer_count_per_service, discovery_errors).
        - all_layers: Flat list of all discovered layers
        - service_for_layer: Maps layer index to its source service
        - layer_count_per_service: Maps service name to total layer count (for flatten logic)
        - discovery_errors: List of (service_name, error_message) tuples

    Layers of services without Query capability are not returned as extractable.
    When skipped_out is given, they are appended to it as (service, layer, reason).
    """
    all_layers: list[LayerInfo] = []
    service_for_layer: dict[int, ServiceInfo] = {}
    layer_count_per_service: dict[str, int] = {}
    discovery_errors: list[tuple[str, str]] = []

    for service in services:
        service_url = service.get_url(base_url)
        try:
            service_discovery = discover_layers(service_url, timeout=timeout)
            if skipped_out is not None and service_discovery.skip_reason:
                skipped_out.extend(
                    (service, layer, service_discovery.skip_reason)
                    for layer in service_discovery.skipped_layers
                )
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


def _service_metadata(url: str, found: ServiceDiscoveryResult) -> MetadataExtracted:
    """Read the service metadata that the extraction report records."""
    arcgis_metadata = extract_arcgis_metadata(
        {
            "copyrightText": found.copyright_text,
            "description": found.description,
            "serviceDescription": found.service_description,
            "documentInfo": {"Author": found.author, "Keywords": found.keywords},
            "accessInformation": found.access_information,
            "licenseInfo": found.license_info,
        },
        source_url=url,
    )
    return MetadataExtracted(
        source_url=url,
        description=arcgis_metadata.description,
        attribution=arcgis_metadata.attribution,
        keywords=arcgis_metadata.keywords,
        contact_name=arcgis_metadata.contact_name,
        processing_notes=arcgis_metadata.processing_notes,
        known_issues=arcgis_metadata.known_issues,
        license_info_raw=arcgis_metadata.license_info_raw,
    )


@dataclass(frozen=True)
class ArcGISLayer:
    """What ``fetch`` and the metadata methods need for one ArcGIS layer."""

    info: LayerInfo
    service_url: str

    @property
    def url(self) -> str:
        """The layer's REST URL: service URL plus layer id."""
        return f"{self.service_url.rstrip('/')}/{self.info.id}"


class ArcGISLayerSource:
    """ArcGIS FeatureServer and MapServer layers as a ``LayerSource``."""

    name = "arcgis"
    summary = "ArcGIS FeatureServer, MapServer, and ImageServer services."

    def __init__(
        self,
        options: ExtractionOptions | None = None,
        *,
        service_filter: list[str] | None = None,
        service_exclude: list[str] | None = None,
    ) -> None:
        self.options = options or ExtractionOptions()
        self.service_filter = service_filter
        self.service_exclude = service_exclude

    def discover(self, source: str, options: LayerSourceOptions) -> Discovery:  # noqa: ARG002 - LayerSource signature
        """List the layers of a service, or of every service under a root."""
        parsed = parse_arcgis_url(source)
        if parsed.url_type in (ArcGISURLType.SERVICES_ROOT, ArcGISURLType.SERVICES_FOLDER):
            return self._discover_root(source, parsed.base_url, parsed.folder)
        found = discover_layers(source, timeout=self.options.timeout)
        layers = [
            LayerRef(id=layer.id, name=layer.name, data=ArcGISLayer(layer, source))
            for layer in found.layers
        ]
        layers += [
            LayerRef(
                id=layer.id,
                name=layer.name,
                data=ArcGISLayer(layer, source),
                skip_reason=found.skip_reason,
            )
            for layer in found.skipped_layers
        ]
        return self.discovery_from(
            source, _service_metadata(source, found), layers, license_info=found.license_info
        )

    def _discover_root(self, source: str, base_url: str, folder: str | None) -> Discovery:
        """Walk a services root. Each layer keeps the URL of its own service.

        A services root spans services with different licenses and the
        catalog carries one metadata.yaml, so no harvested license applies
        (issue #686).
        """
        services, coverage = _discover_and_filter_services(
            source,
            self.service_filter,
            self.service_exclude,
            self.options.timeout,
            token=self.options.token,
            folder=folder,
            recurse=self.options.recurse,
        )
        skipped: list[tuple[ServiceInfo, LayerInfo, str]] = []
        all_layers, service_for_layer, layer_counts, _errors = _collect_layers_from_services(
            services, base_url, self.options.timeout, skipped_out=skipped
        )
        layers = []
        for index, layer in enumerate(all_layers):
            service = service_for_layer[index]
            service_dir = _service_output_dir(Path(), service.name).as_posix()
            # A single-layer service is one collection at the service directory.
            # A multi-layer service is a subcatalog with one collection per layer.
            single = layer_counts.get(service.name, 0) == 1
            path = service_dir if single else f"{service_dir}/{slugify(layer.name)}"
            layers.append(
                LayerRef(
                    id=layer.id,
                    name=layer.name,
                    path=path,
                    data=ArcGISLayer(layer, service.get_url(base_url)),
                )
            )
        for service, layer, reason in skipped:
            service_dir = _service_output_dir(Path(), service.name).as_posix()
            layers.append(
                LayerRef(
                    id=layer.id,
                    name=layer.name,
                    path=f"{service_dir}/{slugify(layer.name)}",
                    data=ArcGISLayer(layer, service.get_url(base_url)),
                    skip_reason=f"{service.name}: {reason}",
                )
            )
        combined = ServiceDiscoveryResult(layers=list(all_layers))
        found = self.discovery_from(source, _service_metadata(source, combined), layers)
        found.folder_coverage = coverage
        return found

    @staticmethod
    def discovery_from(
        source: str,
        metadata: MetadataExtracted | None,
        layers: list[LayerRef],
        *,
        license_info: str | None = None,
    ) -> Discovery:
        """Add the catalog metadata to the layers (issue #369).

        The service name titles the catalog and the service description
        describes it. A technical name is kept out of ``init_catalog`` and
        written to ``catalog.json`` only after a new catalog exists.
        """
        from portolan_cli.stac import is_technical_name

        title = None
        with contextlib.suppress(ValueError):
            title = parse_arcgis_url(source).service_name
        description = metadata.description if metadata else None
        seed = _report_metadata_to_arcgis_metadata(metadata).to_extracted() if metadata else None
        return Discovery(
            source_url=source,
            layers=layers,
            metadata=metadata,
            catalog_seed=seed,
            license_url=license_url_from_text(license_info),
            catalog_title=None if is_technical_name(title) else title,
            catalog_description=None if is_technical_name(description) else description,
            root_metadata=(title, description),
        )

    def fetch(self, source: str, layer: LayerRef, dest: Path) -> int:  # noqa: ARG002 - LayerSource signature
        """Write the layer with ``gpio.extract_arcgis``, then restore its CRS."""
        arcgis_layer: ArcGISLayer = layer.data
        options = self.options
        import inspect

        import geoparquet_io as gpio  # type: ignore[import-untyped]

        # Check which optional parameters gpio.extract_arcgis supports (feature-detected
        # so older gpio versions continue to work without modification).
        sig = inspect.signature(gpio.extract_arcgis)
        kwargs: dict[str, object] = {}
        if "max_workers" in sig.parameters:
            kwargs["max_workers"] = options.workers
        if options.token and "token" in sig.parameters:
            kwargs["token"] = options.token
        # Apply the timeout to the feature-page requests too, not only to discovery.
        # Slow layers with complex geometry time out at gpio's 60s default otherwise,
        # and --timeout appears to do nothing (issue #898).
        if "timeout" in sig.parameters:
            kwargs["timeout"] = options.timeout
        # Keep the service's source CRS instead of the silent WGS84 reprojection that
        # the GeoJSON path forces (issue #802). Older gpio versions without output_crs
        # cannot preserve the CRS, so warn that the output stays WGS84.
        if "output_crs" in sig.parameters:
            kwargs["output_crs"] = options.output_crs
        else:
            from portolan_cli.output import warn

            warn(
                "Installed geoparquet-io does not support output CRS selection. "
                f"Output uses WGS84 (EPSG:4326), not '{options.output_crs}'."
            )
        table = gpio.extract_arcgis(arcgis_layer.url, **kwargs)

        # Capture the CRS the extractor detected before the write drops it. gpio.Table
        # tags the native SR on the table but its writer emits crs: null, so a native
        # extraction would declare WGS84 over projected coordinates (issue #802).
        # gpio.Table.crs may return a dict, a string, or None. Only a PROJJSON dict
        # restamps a valid CRS, so ignore the other forms.
        captured_crs = getattr(table, "crs", None)
        source_crs = captured_crs if isinstance(captured_crs, dict) else None

        # Add the bbox covering column, then sort. Same order as
        # convert.apply_vector_settings, so both writers produce the same layout.
        # Portolan needs row-group spatial statistics for efficient remote reads.
        # ArcGIS extraction writes GeoParquet directly, so the covering column is
        # added here rather than by a later conversion pass.
        #
        # An ArcGIS service publishes non-spatial Table layers beside its Feature
        # Layers, and both operations raise a DuckDB binder error on a table with
        # no geometry column, so the same guard applies here (issue #805).
        if has_geometry(table):
            if options.add_bbox:
                table = table.add_bbox()
            if options.sort_hilbert:
                table = table.sort_hilbert()

        # Write to parquet
        table.write(str(dest))

        # Restore the source CRS that gpio's writer discarded (issue #802).
        _ensure_geoparquet_crs(dest, source_crs)

        # gpio.Table uses num_rows property instead of __len__
        return int(table.num_rows)

    def layer_metadata(self, source: str, layer: LayerRef) -> LayerMetadata:  # noqa: ARG002 - LayerSource signature
        """Read the layer's name and description from its REST details."""
        from portolan_cli.extract.arcgis.discovery import fetch_layer_details

        arcgis_layer: ArcGISLayer = layer.data
        name = layer.name
        description = None
        try:
            details = fetch_layer_details(
                arcgis_layer.service_url, layer.id, timeout=self.options.timeout
            )
            description = details.get("description")
            name = details.get("name", layer.name)
        except Exception as err:  # noqa: BLE001 - a missing description must not fail the catalog
            logger.debug(
                "Failed to fetch layer details for %s (id=%s) from %s: %s",
                layer.name,
                layer.id,
                arcgis_layer.service_url,
                err,
            )
        return LayerMetadata(
            title=name,
            description=description,
            layer_name=name,
            source_type="arcgis_featureserver",
            source_url=arcgis_layer.url,
        )

    def styles(self, source: str, layer: LayerRef, collection_dir: Path) -> None:  # noqa: ARG002 - LayerSource signature
        """Convert the layer's ESRI drawingInfo to a style (issue #490)."""
        if self.options.no_styles:
            return
        arcgis_layer: ArcGISLayer = layer.data
        style = extract_esri_style(
            layer_url=arcgis_layer.url,
            collection_path=collection_dir,
            source_layer=slugify(layer.name),
        )
        if style:
            logger.debug("Extracted style for %s: %s", layer.name, style.path)

    def source_link(self, source: str, layer: LayerRef) -> SourceLink:  # noqa: ARG002 - LayerSource signature
        """Link to the layer's REST endpoint (issue #353)."""
        arcgis_layer: ArcGISLayer = layer.data
        return SourceLink(href=arcgis_layer.url, title=f"Source ArcGIS layer: {layer.name}")


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
        MissingLicenseError: If no license can be resolved (issue #686).
    """
    options = options or ExtractionOptions()
    # ``workers`` is gpio's page concurrency inside one layer. Layers run one
    # at a time.
    driver = LayerSourceOptions(
        layer_filter=layer_filter,
        layer_exclude=layer_exclude,
        resume=options.resume,
        dry_run=options.dry_run,
        raw=options.raw,
        retries=options.retries,
        catalog_id=options.catalog_id,
        license=options.license,
        license_url=options.license_url,
    )
    layer_source = ArcGISLayerSource(
        options, service_filter=service_filter, service_exclude=service_exclude
    )
    return run_layer_extraction(layer_source, url, output_dir, driver, on_progress=on_progress)
